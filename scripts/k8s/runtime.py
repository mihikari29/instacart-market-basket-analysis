"""Explicit-target Kubernetes command and host-identity helpers.

No function in this module is called during import. Callers must opt into
commands and always provide the generated Minikube profile identity.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
from typing import Any, Protocol, Sequence

from .ownership import OwnershipError, RunIdentity


class RuntimePreflightError(RuntimeError):
    """Host, tool, or dedicated-target preflight failed safely."""


@dataclass(frozen=True, slots=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str = ""


class CommandExecutor(Protocol):
    def run(
        self,
        argv: Sequence[str],
        *,
        timeout: int,
        input_text: str | None = None,
    ) -> CommandResult: ...


class SubprocessExecutor:
    """Shell-free executor; output is returned to the caller and never logged."""

    def run(self, argv, *, timeout: int, input_text: str | None = None) -> CommandResult:
        if not isinstance(argv, Sequence) or isinstance(argv, (str, bytes)) or not argv:
            raise RuntimePreflightError("command must be a non-empty argument sequence")
        if any(not isinstance(part, str) or "\x00" in part for part in argv):
            raise RuntimePreflightError("command arguments are invalid")
        # Pipeline Jobs can legitimately run for up to two hours; the wrapper
        # must honor those reviewed per-stage bounds instead of rejecting them
        # at the subprocess boundary.
        if type(timeout) is not int or not 1 <= timeout <= 10800:
            raise RuntimePreflightError("command timeout must be between 1 and 10800 seconds")
        try:
            result = subprocess.run(
                list(argv),
                input=input_text,
                check=False,
                capture_output=True,
                text=True,
                timeout=timeout,
                shell=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise RuntimePreflightError(f"command could not complete ({type(exc).__name__})") from exc
        return CommandResult(result.returncode, result.stdout, result.stderr)


def _checked(executor: CommandExecutor, argv: Sequence[str], *, timeout: int = 30) -> CommandResult:
    result = executor.run(argv, timeout=timeout)
    if result.returncode != 0:
        executable = Path(argv[0]).name
        raise RuntimePreflightError(f"{executable} exited with status {result.returncode}")
    return result


def _json_output(result: CommandResult, description: str) -> Any:
    try:
        return json.loads(result.stdout)
    except (TypeError, json.JSONDecodeError) as exc:
        raise RuntimePreflightError(f"{description} did not return valid JSON") from exc


def kubectl(identity: RunIdentity, *arguments: str) -> list[str]:
    """Build a kubectl command with an explicit run-owned context."""
    if not isinstance(identity, RunIdentity):
        raise RuntimePreflightError("a validated RunIdentity is required")
    if not arguments or any(not isinstance(value, str) or not value or "\x00" in value for value in arguments):
        raise RuntimePreflightError("kubectl arguments must be explicit non-empty strings")
    return ["kubectl", "--context", identity.profile, *arguments]


def _profile_names(raw: str) -> set[str]:
    return set(_profile_entries(raw))


def _profile_entries(raw: str) -> dict[str, dict[str, Any]]:
    try:
        value = json.loads(raw)
    except (TypeError, json.JSONDecodeError) as exc:
        raise RuntimePreflightError("Minikube profile listing is not valid JSON") from exc
    profiles = None
    if isinstance(value, dict):
        profiles = value.get("profiles")
        if profiles is None:
            profiles = value.get("valid")
    if not isinstance(profiles, list):
        raise RuntimePreflightError("Minikube profile listing has an unsupported shape")
    entries = {}
    for item in profiles:
        if not isinstance(item, dict) or not isinstance(item.get("Name"), str):
            raise RuntimePreflightError("Minikube profile listing contains an invalid entry")
        name = item["Name"]
        if name in entries:
            raise RuntimePreflightError("Minikube profile listing contains a duplicate entry")
        config = item.get("Config", {})
        entries[name] = config if isinstance(config, dict) else {}
    return entries


def preflight_dedicated_target(executor: CommandExecutor, identity: RunIdentity) -> dict[str, Any]:
    """Reject profile/context collisions before any cluster creation or apply."""
    profile_output = _checked(executor, ["minikube", "profile", "list", "-o", "json"])
    context_output = _checked(executor, ["kubectl", "config", "get-contexts", "-o", "name"])
    profiles = _profile_names(profile_output.stdout)
    contexts = {line.strip() for line in context_output.stdout.splitlines() if line.strip()}
    if identity.profile in profiles:
        raise RuntimePreflightError("dedicated Minikube profile already exists; choose a fresh run ID")
    if identity.profile in contexts:
        raise RuntimePreflightError("dedicated Kubernetes context already exists; choose a fresh run ID")
    return {
        "status": "clear",
        "profile": identity.profile,
        "context": identity.profile,
        "namespace": identity.namespace,
        "checked_profiles": len(profiles),
        "checked_contexts": len(contexts),
    }


def _physical_cpu_count(cpuinfo: str) -> int | None:
    pairs = set()
    current: dict[str, str] = {}
    for line in cpuinfo.splitlines() + [""]:
        if not line.strip():
            if "physical id" in current and "core id" in current:
                pairs.add((current["physical id"], current["core id"]))
            current = {}
            continue
        key, separator, value = line.partition(":")
        if separator:
            current[key.strip()] = value.strip()
    return len(pairs) if pairs else None


def measure_host_capacity(*, disk_path: Path | str = "/") -> dict[str, Any]:
    """Measure guest-visible resources without contacting Docker or Kubernetes."""
    memory_total = memory_available = None
    meminfo = Path("/proc/meminfo")
    if meminfo.is_file():
        values = {}
        for line in meminfo.read_text(encoding="ascii", errors="replace").splitlines():
            key, separator, rest = line.partition(":")
            if separator:
                match = re.match(r"\s*(\d+)\s+kB\s*", rest)
                if match:
                    values[key] = int(match.group(1)) * 1024
        memory_total = values.get("MemTotal")
        memory_available = values.get("MemAvailable")
    try:
        cpuinfo = Path("/proc/cpuinfo").read_text(encoding="ascii", errors="replace")
    except OSError:
        cpuinfo = ""
    try:
        disk = shutil.disk_usage(disk_path)
    except OSError as exc:
        raise RuntimePreflightError("guest disk capacity could not be measured") from exc
    total_cores = _physical_cpu_count(cpuinfo)
    logical_threads = os.cpu_count()
    is_wsl = "microsoft" in platform.release().lower() or "wsl" in platform.release().lower()
    return {
        "cpu_cores": total_cores,
        "logical_cpu_threads": logical_threads,
        "memory_gib": round((memory_available or 0) / (1024**3), 3) if memory_available else None,
        "memory_total_gib": round((memory_total or 0) / (1024**3), 3) if memory_total else None,
        "free_disk_gib": round(disk.free / (1024**3), 3),
        "host_os": platform.platform(),
        "architecture": platform.machine().lower(),
        "wsl_guest": is_wsl,
        "windows_backing_disk_gib": None if is_wsl else round(disk.free / (1024**3), 3),
        "measured_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }


def validate_host_capacity(
    capacity: dict[str, Any], *, phase: str = "b2", require_backing_disk: bool = True
) -> None:
    """Fail closed against documented B2/B3 reserves and unknown WSL backing space."""
    if phase not in {"b2", "b3"}:
        raise RuntimePreflightError("phase must be b2 or b3")
    minimums = {"b2": (4, 6.0, 20.0), "b3": (6, 12.0, 40.0)}
    minimum_cpu, minimum_memory, minimum_disk = minimums[phase]
    for field, minimum in (
        ("cpu_cores", minimum_cpu),
        ("memory_gib", minimum_memory),
        ("free_disk_gib", minimum_disk),
    ):
        value = capacity.get(field)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise RuntimePreflightError(f"host {field} is unknown; runtime preflight cannot pass")
        if value < minimum:
            raise RuntimePreflightError(f"host {field} is below the documented {phase.upper()} reserve")
    if require_backing_disk and capacity.get("wsl_guest") is True:
        backing = capacity.get("windows_backing_disk_gib")
        if (
            not isinstance(backing, (int, float))
            or isinstance(backing, bool)
            or not math.isfinite(backing)
            or backing < minimum_disk
        ):
            raise RuntimePreflightError("Windows backing-disk capacity is not verified for this WSL guest")


def collect_cluster_identity(
    executor: CommandExecutor, identity: RunIdentity, *, execute: bool = False
) -> dict[str, Any]:
    """Collect a running profile's cluster marker before creating a namespace."""
    if execute is not True:
        raise RuntimePreflightError("cluster identity discovery requires explicit execute=True authorization")
    profile_result = _checked(executor, ["minikube", "status", "-p", identity.profile, "--output=json"])
    profile_status = _json_output(profile_result, "Minikube status")
    if not isinstance(profile_status, dict) or profile_status.get("Host") != "Running":
        raise RuntimePreflightError("dedicated Minikube profile is not running")
    profile_listing = _checked(executor, ["minikube", "profile", "list", "-o", "json"])
    profile_config = _profile_entries(profile_listing.stdout).get(identity.profile)
    if not isinstance(profile_config, dict):
        raise RuntimePreflightError("dedicated Minikube profile configuration could not be found")
    kube_config = profile_config.get("KubernetesConfig", {})
    kube_config = kube_config if isinstance(kube_config, dict) else {}
    driver = profile_config.get("Driver")
    configured_runtime = kube_config.get("ContainerRuntime") or profile_config.get("ContainerRuntime")
    cni = kube_config.get("CNI") or profile_config.get("CNI")
    configured_kubernetes_version = kube_config.get("KubernetesVersion")
    if not all(isinstance(value, str) and value for value in (driver, configured_runtime, cni, configured_kubernetes_version)):
        raise RuntimePreflightError("dedicated profile configuration is incomplete")
    kube_system = _json_output(
        _checked(executor, kubectl(identity, "get", "namespace", "kube-system", "-o", "json")),
        "kube-system identity",
    )
    version = _json_output(_checked(executor, kubectl(identity, "version", "-o", "json")), "Kubernetes version")
    if not all(isinstance(item, dict) for item in (kube_system, version)):
        raise RuntimePreflightError("Kubernetes identity response is invalid")
    cluster_uid = kube_system.get("metadata", {}).get("uid")
    git_version = version.get("serverVersion", {}).get("gitVersion")
    if not all(isinstance(value, str) and value for value in (cluster_uid, git_version)):
        raise RuntimePreflightError("Kubernetes identity is incomplete")
    nodes = _json_output(
        _checked(executor, kubectl(identity, "get", "nodes", "-o", "json")), "Kubernetes node inventory"
    )
    node_items = nodes.get("items") if isinstance(nodes, dict) else None
    if not isinstance(node_items, list) or len(node_items) != 1 or not isinstance(node_items[0], dict):
        raise RuntimePreflightError("dedicated profile must contain exactly one node")
    node_info = node_items[0].get("status", {}).get("nodeInfo", {})
    architecture = node_info.get("architecture") if isinstance(node_info, dict) else None
    node_runtime = node_info.get("containerRuntimeVersion") if isinstance(node_info, dict) else None
    node_count = len(node_items)
    if not isinstance(architecture, str) or not re.fullmatch(r"linux/(?:amd64|arm64|ppc64le|s390x)", f"linux/{architecture}"):
        raise RuntimePreflightError("Kubernetes node architecture is unsupported")
    if not isinstance(node_runtime, str) or not node_runtime.startswith("containerd://"):
        raise RuntimePreflightError("Kubernetes node is not using containerd")
    if configured_kubernetes_version != git_version:
        raise RuntimePreflightError("Minikube profile and Kubernetes API versions do not match")
    return {
        "profile": identity.profile,
        "context": identity.profile,
        "cluster_uid": cluster_uid,
        "kubernetes_version": git_version,
        "driver": driver.lower(),
        "container_runtime": "containerd",
        "cni": cni.lower(),
        "architecture": f"linux/{architecture}",
        "node_count": node_count,
        "minikube_status": profile_status,
    }


def collect_namespace_identity(
    executor: CommandExecutor, identity: RunIdentity, *, execute: bool = False
) -> dict[str, Any]:
    """Read back the run namespace after creation and require all ownership labels."""
    if execute is not True:
        raise RuntimePreflightError("namespace identity discovery requires explicit execute=True authorization")
    namespace = _json_output(
        _checked(executor, kubectl(identity, "get", "namespace", identity.namespace, "-o", "json")),
        "run namespace identity",
    )
    if not isinstance(namespace, dict):
        raise RuntimePreflightError("run namespace identity is invalid")
    metadata = namespace.get("metadata", {})
    if metadata.get("name") != identity.namespace:
        raise RuntimePreflightError("observed namespace does not match the run identity")
    labels = metadata.get("labels", {})
    uid = metadata.get("uid")
    if not isinstance(labels, dict) or any(labels.get(key) != value for key, value in identity.labels.items()):
        raise RuntimePreflightError("run namespace ownership labels do not match")
    if not isinstance(uid, str) or not uid:
        raise RuntimePreflightError("run namespace UID is missing")
    return {"namespace": identity.namespace, "namespace_uid": uid, "namespace_labels": labels}


def collect_toolchain_identity(executor: CommandExecutor, *, execute: bool = False) -> dict[str, Any]:
    """Inspect local tool versions and Docker Engine without changing their state."""
    if execute is not True:
        raise RuntimePreflightError("Docker Engine identity discovery requires explicit execute=True authorization")
    minikube_path = shutil.which("minikube")
    if not minikube_path:
        raise RuntimePreflightError("Minikube is not installed")
    try:
        with Path(minikube_path).open("rb") as handle:
            minikube_sha = hashlib.sha256(handle.read()).hexdigest()
    except OSError as exc:
        raise RuntimePreflightError("Minikube binary checksum could not be measured") from exc
    minikube = _json_output(
        _checked(executor, ["minikube", "version", "--output=json"]), "Minikube version"
    )
    kubectl_version = _json_output(
        _checked(executor, ["kubectl", "version", "--client", "-o", "json"]), "kubectl client version"
    )
    docker = _checked(executor, ["docker", "version", "--format", "{{.Server.Version}}"])
    minikube_version = minikube.get("minikubeVersion") if isinstance(minikube, dict) else None
    client = kubectl_version.get("clientVersion") if isinstance(kubectl_version, dict) else None
    kubectl_git_version = client.get("gitVersion") if isinstance(client, dict) else None
    docker_version = docker.stdout.strip()
    if not all(isinstance(value, str) and value for value in (minikube_version, kubectl_git_version, docker_version)):
        raise RuntimePreflightError("toolchain version identities are incomplete")
    return {
        "minikube_version": minikube_version,
        "minikube_binary_sha256": minikube_sha,
        "kubectl_version": kubectl_git_version,
        "docker_engine_version": docker_version,
    }


def collect_storage_class_identity(
    executor: CommandExecutor,
    identity: RunIdentity,
    *,
    execute: bool = False,
    expected_provisioner: str = "k8s.io/minikube-hostpath",
) -> dict[str, Any]:
    """Read and validate the unique default profile-local StorageClass."""
    if execute is not True:
        raise RuntimePreflightError("StorageClass discovery requires explicit execute=True authorization")
    payload = _json_output(
        _checked(executor, kubectl(identity, "get", "storageclass", "-o", "json")),
        "StorageClass inventory",
    )
    items = payload.get("items") if isinstance(payload, dict) else None
    if not isinstance(items, list):
        raise RuntimePreflightError("StorageClass inventory is invalid")
    defaults = [
        item for item in items
        if isinstance(item, dict)
        and item.get("metadata", {}).get("annotations", {}).get("storageclass.kubernetes.io/is-default-class") == "true"
    ]
    if len(defaults) != 1:
        raise RuntimePreflightError("exactly one default StorageClass is required")
    storage_class = defaults[0]
    metadata = storage_class.get("metadata", {})
    provisioner = storage_class.get("provisioner")
    uid = metadata.get("uid")
    name = metadata.get("name")
    if (
        not all(isinstance(value, str) and value for value in (uid, name, provisioner))
        or provisioner != expected_provisioner
    ):
        raise RuntimePreflightError("default StorageClass is not the expected profile-local provisioner")
    binding = storage_class.get("volumeBindingMode")
    if binding not in {"Immediate", "WaitForFirstConsumer"}:
        raise RuntimePreflightError("default StorageClass has an unsupported binding mode")
    return {
        "default_storage_class": name,
        "storage_class_uid": uid,
        "provisioner": provisioner,
        "volume_binding_mode": binding,
        "profile_local": True,
    }


def containerd_image_inventory_command(identity: RunIdentity) -> list[str]:
    """Return a read-only inspection command scoped to the named Minikube profile."""
    return ["minikube", "-p", identity.profile, "ssh", "--", "sudo", "crictl", "images", "-o", "json"]


def cluster_creation_plan(identity: RunIdentity, *, kubernetes_version: str, cpus: int, memory_mib: int, disk_gib: int) -> list[str]:
    """Return a reviewable command for a new dedicated profile; does not run it."""
    if not re.fullmatch(r"v\d+\.\d+\.\d+", kubernetes_version):
        raise RuntimePreflightError("an exact Kubernetes version is required")
    if type(cpus) is not int or cpus < 1 or type(memory_mib) is not int or memory_mib < 1024:
        raise RuntimePreflightError("CPU and memory reservations must be explicit positive integers")
    if type(disk_gib) is not int or disk_gib < 10:
        raise RuntimePreflightError("Minikube disk reservation must be at least 10 GiB")
    return [
        "minikube", "start", "--profile", identity.profile, "--driver=docker",
        f"--kubernetes-version={kubernetes_version}", "--container-runtime=containerd", "--nodes=1",
        "--cni=calico", f"--cpus={cpus}", f"--memory={memory_mib}", f"--disk-size={disk_gib}g",
    ]


def create_dedicated_cluster(
    executor: CommandExecutor,
    identity: RunIdentity,
    *,
    host_capacity: dict[str, Any],
    kubernetes_version: str,
    cpus: int,
    memory_mib: int,
    disk_gib: int,
    execute: bool = False,
) -> dict[str, Any]:
    """Create only a fresh run-named Minikube profile after explicit authorization."""
    if execute is not True:
        raise RuntimePreflightError("cluster creation requires explicit execute=True authorization")
    validate_host_capacity(host_capacity, phase="b2")
    if cpus > host_capacity["cpu_cores"] or memory_mib > host_capacity["memory_gib"] * 1024:
        raise RuntimePreflightError("Minikube resource reservations exceed measured host capacity")
    if disk_gib > host_capacity["free_disk_gib"]:
        raise RuntimePreflightError("Minikube disk reservation exceeds measured free disk")
    collision = preflight_dedicated_target(executor, identity)
    if collision.get("status") != "clear":
        raise RuntimePreflightError("dedicated profile/context collision preflight did not pass")
    command = cluster_creation_plan(
        identity,
        kubernetes_version=kubernetes_version,
        cpus=cpus,
        memory_mib=memory_mib,
        disk_gib=disk_gib,
    )
    try:
        result = executor.run(command, timeout=1800)
    except Exception as exc:
        raise RuntimePreflightError(f"dedicated Minikube creation failed ({type(exc).__name__})") from exc
    if result.returncode != 0:
        raise RuntimePreflightError(f"Minikube exited with status {result.returncode} during profile creation")
    return {
        "status": "created_pending_identity_discovery",
        "profile": identity.profile,
        "context": identity.profile,
        "namespace": identity.namespace,
        "reserved_cpus": cpus,
        "reserved_memory_mib": memory_mib,
        "reserved_disk_gib": disk_gib,
    }
