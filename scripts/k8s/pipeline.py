"""Offline B3 pipeline plans, private manifest rendering, and gated Job execution.

All commands target the run's explicit Minikube context. This module never
selects the current kubectl context and never deletes a resource.
"""

from __future__ import annotations

import copy
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Mapping, Protocol

import yaml

from .common import (
    PIPELINE_STORAGE_BUDGET,
    claim_names,
    demo_database,
    expected_claim_requests,
    pipeline_job_name,
)
from .ownership import IMAGE_DIGEST_PATTERN, RunIdentity, validate_version_lock
from .runtime import containerd_image_inventory_command, kubectl
from .config import get_infrastructure_timeout, get_step_timeout, get_capacity_reserve


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_MANIFESTS = REPOSITORY_ROOT / "deploy" / "k8s" / "pipeline"
PIPELINE_IMAGE_ROLES = {
    "pipeline_application_image": "__PIPELINE_IMAGE__",
    "kafka": "__KAFKA_IMAGE__",
    "hdfs_namenode": "__HDFS_NAMENODE_IMAGE__",
    "hdfs_datanode": "__HDFS_DATANODE_IMAGE__",
}
PIPELINE_COMPONENT_IMAGE_ROLES = ("kafka", "hdfs_namenode", "hdfs_datanode")
PIPELINE_POLICY_NAMES = frozenset({
    "allow-pipeline-egress", "allow-pipeline-to-hdfs", "allow-pipeline-to-datanode",
    "allow-datanode-to-namenode", "allow-datanode-egress-to-namenode",
    "allow-kafka-controller-self", "allow-pipeline-to-kafka", "allow-pipeline-to-mongo",
    "allow-acceptance-api-egress", "allow-acceptance-to-api",
})
TREND_SOURCE_NAMESPACE = "instacart-k8s-demo-v1"
PIPELINE_STEPS = ("input", "module1", "module2", "module4", "replay", "hybrid")
FIXTURE_MAX_USERS = 50
FIXTURE_MAX_EVENTS = 5_000
FULL_DATA_RESERVE = get_capacity_reserve()
FULL_PIPELINE_CLAIM_BYTES = 16 * 1024**3
FULL_PIPELINE_RESERVE_BYTES = 2 * 1024**3
FULL_PIPELINE_WORKING_SET_MULTIPLIER = 3
FULL_INPUT_MAX_BYTES = 4 * 1024**3
_IMAGE_REFERENCE = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9._/:@+-]{0,254}\Z")


class PipelineError(RuntimeError):
    """Pipeline plan, identity, manifest, or execution failure."""


def validate_full_data_storage_budget(input_bytes: int) -> int:
    """Bound the run PVC peak as input + cleaner output + event-feed output."""
    if type(input_bytes) is not int or input_bytes <= 0:
        raise PipelineError("full-data input byte count must be a positive integer")
    projected = input_bytes * FULL_PIPELINE_WORKING_SET_MULTIPLIER + FULL_PIPELINE_RESERVE_BYTES
    if input_bytes > FULL_INPUT_MAX_BYTES or projected > FULL_PIPELINE_CLAIM_BYTES:
        maximum = min(
            FULL_INPUT_MAX_BYTES,
            (FULL_PIPELINE_CLAIM_BYTES - FULL_PIPELINE_RESERVE_BYTES)
            // FULL_PIPELINE_WORKING_SET_MULTIPLIER,
        )
        raise PipelineError(
            f"full-data inputs exceed the run-PVC budget; maximum raw input is {maximum // 1024**3} GiB "
            "with three working-set copies and 2 GiB reserve"
        )
    return projected


def validate_pipeline_policy_inventory(policies: list[Mapping[str, Any]]) -> None:
    """Require the exact narrow B3 service paths and deny all other paths."""
    if not isinstance(policies, list) or any(not isinstance(item, Mapping) for item in policies):
        raise PipelineError("B3 NetworkPolicy inventory must be a list of objects")
    named = {item.get("metadata", {}).get("name"): item for item in policies}
    if len(named) != len(policies) or set(named) != PIPELINE_POLICY_NAMES:
        raise PipelineError("B3 NetworkPolicy inventory is incomplete or contains an unapproved policy")
    for name, policy in named.items():
        spec = policy.get("spec")
        if not isinstance(spec, Mapping) or not spec.get("policyTypes"):
            raise PipelineError(f"B3 NetworkPolicy {name} must declare its isolation direction")
        for direction in ("ingress", "egress"):
            rules = spec.get(direction, [])
            if not isinstance(rules, list):
                raise PipelineError(f"B3 NetworkPolicy {name} has malformed {direction} rules")
            for rule in rules:
                ports = rule.get("ports") if isinstance(rule, Mapping) else None
                if not isinstance(ports, list) or not ports:
                    raise PipelineError(f"B3 NetworkPolicy {name} contains a broad {direction} rule")
                for port in ports:
                    value = port.get("port") if isinstance(port, Mapping) else None
                    protocol = port.get("protocol") if isinstance(port, Mapping) else None
                    if protocol != "TCP" or type(value) is not int or not 1 <= value <= 65535:
                        raise PipelineError(f"B3 NetworkPolicy {name} has an invalid port rule")

    expected = {
        "allow-pipeline-egress": (
            {"it4931.instacart.edu/role": "pipeline"}, "Egress",
            {
                ("app.kubernetes.io/name", "hdfs-namenode", (8020, 9870)),
                ("app.kubernetes.io/name", "hdfs-datanode", (9864, 9866)),
                ("app.kubernetes.io/name", "kafka", (29092,)),
                ("app.kubernetes.io/name", "module4d-mongo", (27017,)),
            },
        ),
        "allow-pipeline-to-hdfs": (
            {"app.kubernetes.io/name": "hdfs-namenode"}, "Both",
            {
                ("it4931.instacart.edu/role", "pipeline", (8020, 9870)),
                ("app.kubernetes.io/name", "hdfs-namenode", (8020, 9870)),
            },
        ),
        "allow-pipeline-to-datanode": (
            {"app.kubernetes.io/name": "hdfs-datanode"}, "Ingress",
            {("it4931.instacart.edu/role", "pipeline", (9864, 9866))},
        ),
        "allow-datanode-to-namenode": (
            {"app.kubernetes.io/name": "hdfs-namenode"}, "Ingress",
            {("app.kubernetes.io/name", "hdfs-datanode", (8020, 9870))},
        ),
        "allow-datanode-egress-to-namenode": (
            {"app.kubernetes.io/name": "hdfs-datanode"}, "Egress",
            {("app.kubernetes.io/name", "hdfs-namenode", (8020, 9870))},
        ),
        "allow-kafka-controller-self": (
            {"app.kubernetes.io/name": "kafka"}, "Both",
            {("app.kubernetes.io/name", "kafka", (9093,))},
        ),
        "allow-pipeline-to-kafka": (
            {"app.kubernetes.io/name": "kafka"}, "Ingress",
            {("it4931.instacart.edu/role", "pipeline", (29092,))},
        ),
        "allow-pipeline-to-mongo": (
            {"app.kubernetes.io/name": "module4d-mongo"}, "Ingress",
            {("it4931.instacart.edu/role", "pipeline", (27017,))},
        ),
        "allow-acceptance-api-egress": (
            {"it4931.instacart.edu/role": "pipeline", "it4931.instacart.edu/step": "acceptance"},
            "Egress",
            {("app.kubernetes.io/name", "module4d-api", (8000,))},
        ),
        "allow-acceptance-to-api": (
            {"app.kubernetes.io/name": "module4d-api"},
            "Ingress",
            {("it4931.instacart.edu/step", "acceptance", (8000,))},
        ),
    }
    for name, (selector, direction, flows) in expected.items():
        spec = named[name]["spec"]
        if spec.get("podSelector", {}).get("matchLabels", {}) != selector:
            raise PipelineError(f"B3 NetworkPolicy {name} selects the wrong Pods")
        required_types = {"Ingress", "Egress"} if direction == "Both" else {direction}
        if set(spec.get("policyTypes", [])) != required_types:
            raise PipelineError(f"B3 NetworkPolicy {name} has an unexpected isolation direction")
        actual_flows = set()
        for traffic_direction in ("ingress", "egress"):
            raw_rules = spec.get(traffic_direction, [])
            if traffic_direction.title() not in required_types:
                if raw_rules:
                    raise PipelineError(f"B3 NetworkPolicy {name} contains an unapproved {traffic_direction} rule")
                continue
            endpoint_field = "from" if traffic_direction == "ingress" else "to"
            for rule in raw_rules:
                if not isinstance(rule, Mapping) or set(rule) != {endpoint_field, "ports"}:
                    raise PipelineError(f"B3 NetworkPolicy {name} contains an unapproved endpoint rule")
                endpoints = rule[endpoint_field]
                if not isinstance(endpoints, list) or len(endpoints) != 1:
                    raise PipelineError(f"B3 NetworkPolicy {name} must select one peer per rule")
                peer = endpoints[0]
                peer_selector = peer.get("podSelector") if isinstance(peer, Mapping) else None
                if not isinstance(peer_selector, Mapping) or set(peer) != {"podSelector"}:
                    raise PipelineError(f"B3 NetworkPolicy {name} may only select same-namespace Pods")
                peer_labels = peer_selector.get("matchLabels", {})
                if not isinstance(peer_labels, Mapping) or len(peer_labels) != 1:
                    raise PipelineError(f"B3 NetworkPolicy {name} peer selector is invalid")
                label, value = next(iter(peer_labels.items()))
                ports = tuple(sorted(port["port"] for port in rule["ports"]))
                actual_flows.add((label, value, ports))
        if actual_flows != flows:
            raise PipelineError(f"B3 NetworkPolicy {name} does not express its exact approved service flow")


class PipelineCommandExecutor(Protocol):
    def run(self, argv: list[str], *, timeout: int, input_text: str | None = None): ...


def pipeline_image_build_plan(
    source_root: Path | str,
    *,
    identity: RunIdentity,
    source_revision: str,
    metadata_directory: Path | str,
    platform: str,
) -> list[str]:
    """Prepare a clean, single-platform BuildKit plan for the B3 pipeline image."""
    if not re.fullmatch(r"[0-9a-f]{40}", source_revision or ""):
        raise PipelineError("pipeline image build requires a full lowercase source Git SHA")
    if not re.fullmatch(r"linux/(?:amd64|arm64|ppc64le|s390x)", platform or ""):
        raise PipelineError("pipeline image build requires an exact supported Linux platform")
    root = Path(source_root).resolve(strict=True)
    metadata_root = Path(metadata_directory).resolve(strict=True)
    if not root.is_dir() or not metadata_root.is_dir() or root == metadata_root or root in metadata_root.parents:
        raise PipelineError("BuildKit metadata must be stored outside the image context")
    from .image_provenance import source_revision_evidence

    evidence = source_revision_evidence(root)
    if evidence["source_dirty"] is not False or evidence["source_revision"] != source_revision:
        raise PipelineError("pipeline image builds require the exact clean source revision")
    return [
        "docker", "buildx", "build", "--file", "Dockerfile", "--target", "pipeline",
        "--build-arg", f"MODULE4_COMMIT={source_revision}", "--platform", platform,
        "--tag", f"instacart-pipeline-k8s:{identity.run_id}",
        "--metadata-file", str(metadata_root / "pipeline-build-metadata.json"),
        "--load", str(root),
    ]


def execute_pipeline_image_build(
    executor: PipelineCommandExecutor,
    command: list[str],
    *,
    execute: bool = False,
    timeout: int = 3600,
) -> Path:
    """Run only a validated, run-tagged BuildKit command after explicit opt-in."""
    if execute is not True:
        raise PipelineError("B3 pipeline image build requires explicit execute=True authorization")
    if (
        not isinstance(command, list)
        or command[:7] != ["docker", "buildx", "build", "--file", "Dockerfile", "--target", "pipeline"]
        or "--load" not in command
        or "--metadata-file" not in command
        or not any(value.startswith("instacart-pipeline-k8s:") for value in command)
    ):
        raise PipelineError("only a validated B3 pipeline image build plan can be executed")
    metadata_path = Path(command[command.index("--metadata-file") + 1])
    try:
        result = executor.run(command, timeout=timeout)
    except Exception as exc:
        raise PipelineError(f"pipeline image build failed ({type(exc).__name__})") from exc
    if result.returncode != 0 or not metadata_path.is_file():
        raise PipelineError("pipeline image build did not produce BuildKit metadata")
    return metadata_path


def discover_pipeline_component_images(
    executor: PipelineCommandExecutor,
    lock_template: Mapping[str, Any],
    *,
    execute: bool = False,
    timeout: int = 900,
) -> dict[str, dict[str, str]]:
    """Pull the selected non-latest component tags and inspect their actual digests."""
    if execute is not True:
        raise PipelineError("B3 component image discovery requires explicit execute=True authorization")
    images = lock_template.get("images")
    if not isinstance(images, Mapping):
        raise PipelineError("B3 lock template has no image identities")
    result: dict[str, dict[str, str]] = {}
    for role in PIPELINE_COMPONENT_IMAGE_ROLES:
        item = images.get(role)
        reference = item.get("reference") if isinstance(item, Mapping) else None
        if not isinstance(reference, str) or "@" in reference or reference.endswith(":latest"):
            raise PipelineError(f"B3 source image reference for {role} must be one exact non-latest tag")
        try:
            pulled = executor.run(["docker", "pull", reference], timeout=timeout)
        except Exception as exc:
            raise PipelineError(f"B3 component image {role} could not be pulled ({type(exc).__name__})") from exc
        if pulled.returncode != 0:
            raise PipelineError(f"B3 component image {role} pull failed")
        try:
            inspected = executor.run(["docker", "image", "inspect", reference], timeout=60)
            payload = json.loads(inspected.stdout)
        except Exception as exc:
            raise PipelineError(f"B3 component image {role} identity could not be inspected") from exc
        if inspected.returncode != 0 or not isinstance(payload, list) or len(payload) != 1:
            raise PipelineError(f"B3 component image {role} inspection is ambiguous")
        image = payload[0]
        repo_digests = image.get("RepoDigests") or []
        repository = reference.split(":", 1)[0]
        digests = {
            value.rsplit("@", 1)[1]
            for value in repo_digests
            if isinstance(value, str) and value.rsplit("@", 1)[0].endswith(repository)
            and "@" in value
        }
        manifest_digests = {value for value in digests if IMAGE_DIGEST_PATTERN.fullmatch(value)}
        config_digest = image.get("Id")
        architecture = image.get("Architecture")
        os_name = image.get("Os")
        if len(manifest_digests) != 1 or not isinstance(config_digest, str) or not IMAGE_DIGEST_PATTERN.fullmatch(config_digest):
            raise PipelineError(f"B3 component image {role} has no unique OCI manifest/config digest")
        platform = f"{os_name}/{architecture}"
        if platform not in {"linux/amd64", "linux/arm64", "linux/ppc64le", "linux/s390x"}:
            raise PipelineError(f"B3 component image {role} platform is unsupported")
        result[role] = {
            "reference": reference,
            "immutable_digest": next(iter(manifest_digests)),
            "config_digest": config_digest,
            "platform": platform,
        }
    return result


def pipeline_build_provenance(
    executor: PipelineCommandExecutor,
    *,
    source_root: Path | str,
    reference: str,
    metadata_path: Path | str,
    platform: str,
    execute: bool = False,
) -> dict[str, Any]:
    """Join BuildKit metadata and the stopped image's package inventory into provenance."""
    if execute is not True:
        raise PipelineError("B3 image provenance collection requires explicit execute=True authorization")
    from .image_provenance import build_provenance_record, collect_image_environment, source_revision_evidence

    source = source_revision_evidence(source_root)
    try:
        metadata = json.loads(Path(metadata_path).read_text(encoding="utf-8"))
        build = executor.run(["docker", "image", "inspect", "--format", "{{.Id}}", reference], timeout=60)
        if build.returncode != 0:
            raise PipelineError("B3 pipeline image is missing from the local image store")
        # collect_image_environment also verifies the OCI config digest and source-revision label.
        raw_build = json.loads(Path(metadata_path).read_text(encoding="utf-8"))
        expected_config = raw_build.get("containerimage.config.digest")
        environment = collect_image_environment(
            executor, reference, execute=True,
            expected_config_digest=expected_config,
            expected_source_revision=source["source_revision"],
        )
        provenance = build_provenance_record(
            source=source, metadata_path=metadata_path, metadata=metadata,
            python_version=environment["python_version"],
            python_environment_sha256=environment["python_environment_sha256"],
            platform=platform,
        )
    except Exception as exc:
        if isinstance(exc, PipelineError):
            raise
        raise PipelineError("B3 pipeline BuildKit provenance could not be verified") from exc
    provenance["reference"] = reference
    return provenance


def load_pipeline_images(
    executor: PipelineCommandExecutor,
    identity: RunIdentity,
    lock: Mapping[str, Any],
    *,
    execute: bool = False,
    timeout: int = 900,
) -> dict[str, dict[str, str]]:
    """Load four digest-locked B3 images only into the explicitly named profile."""
    if execute is not True:
        raise PipelineError("B3 image loading requires explicit execute=True authorization")
    validate_pipeline_lock(lock)
    if lock["cluster"].get("profile") != identity.profile or lock["cluster"].get("context") != identity.profile:
        raise PipelineError("B3 version lock does not identify the requested profile")
    expected = {}
    for role in PIPELINE_IMAGE_ROLES:
        reference = locked_image(lock, role)
        image_digest = lock["images"][role]["immutable_digest"]
        provenance = lock.get("pipeline_image_provenance", {}) if role == "pipeline_application_image" else {}
        config_digest = (
            provenance.get("config_digest")
            if role == "pipeline_application_image"
            else lock["pipeline_component_image_provenance"][role]["config_digest"]
        )
        expected[role] = {"reference": reference.split("@", 1)[0], "image_digest": image_digest, "config_digest": config_digest}
        tag = reference.split("@", 1)[0]
        try:
            result = executor.run(["minikube", "image", "load", "-p", identity.profile, tag], timeout=timeout)
        except Exception as exc:
            raise PipelineError(f"B3 {role} image load failed ({type(exc).__name__})") from exc
        if result.returncode != 0:
            raise PipelineError(f"B3 {role} image load failed")
    app_image = lock["images"]["pipeline_application_image"]
    app_tag = app_image["reference"].split("@", 1)[0]
    app_digest = app_image["immutable_digest"]
    source_ref = f"docker.io/library/{app_tag}"
    repo_prefix = app_tag.split(":", 1)[0]
    for t_ref in (
        f"{app_tag}@{app_digest}",
        f"docker.io/library/{app_tag}@{app_digest}",
        f"{repo_prefix}@{app_digest}",
        f"docker.io/library/{repo_prefix}@{app_digest}",
    ):
        tag_cmd = [
            "minikube", "-p", identity.profile, "ssh", "--",
            "sudo", "ctr", "-n", "k8s.io", "images", "tag", source_ref, t_ref,
        ]
        executor.run(tag_cmd, timeout=120)

    from .image_provenance import verify_loaded_image_inventory

    try:
        inventory = _checked(executor, containerd_image_inventory_command(identity), "pipeline image inventory", timeout).stdout
        return verify_loaded_image_inventory(inventory, expected)
    except Exception as exc:
        raise PipelineError("B3 loaded image identities do not match the finalized lock") from exc


@dataclass(frozen=True, slots=True)
class PipelineStep:
    name: str
    job_name: str
    timeout_seconds: int
    backoff_limit: int
    retry_semantics: str


@dataclass(frozen=True, slots=True)
class PipelinePlan:
    run_id: str
    profile: str
    namespace: str
    mode: str
    hdfs_root: str
    topic: str
    trend_source_namespace: str
    fixture_max_users: int | None
    fixture_max_events: int | None
    steps: tuple[PipelineStep, ...]
    execution_model: str = "Spark local[2] inside Kubernetes-managed Jobs; not native distributed Spark-on-Kubernetes"


def _step_specs(identity: RunIdentity, mode: str = "fixture") -> tuple[PipelineStep, ...]:
    return tuple(
        PipelineStep(name, f"it4931-{name}-{identity.run_id}", get_step_timeout(name, mode), retry, retry_text)
        for name, retry, retry_text in (
            ("input", 0, "fixture is deterministic; full input transfer is verified by the six-file manifest hash"),
            ("module1", 0, "no automatic retry; inspect partial staging before a new run identity"),
            ("module2", 0, "no automatic retry; includes bounded local[1]/local[2] executor benchmark"),
            ("module4", 0, "no automatic retry; stable model versions require a reviewed new attempt"),
            ("replay", 0, "no automatic retry; resume requires verified Kafka offsets and checkpoint receipt"),
            ("hybrid", 0, "insert-only source identity; no automatic conflict repair"),
        )
    )


def plan_pipeline(
    identity: RunIdentity,
    *,
    mode: str,
    host_capacity: Mapping[str, Any] | None = None,
    source_receipt_sha256: str | None = None,
) -> PipelinePlan:
    """Return a deterministic dependency order after validating fixture/full bounds."""
    if mode not in {"fixture", "full"}:
        raise PipelineError("mode must be fixture or full")
    if mode == "fixture":
        if source_receipt_sha256 is not None:
            raise PipelineError("fixture mode uses the image's bounded fixture and takes no external receipt")
        caps = (FIXTURE_MAX_USERS, FIXTURE_MAX_EVENTS)
    else:
        if not isinstance(source_receipt_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", source_receipt_sha256):
            raise PipelineError("full mode requires a verified SHA-256 receipt for the six input CSV files")
        if not isinstance(host_capacity, Mapping):
            raise PipelineError("full mode requires measured host capacity")
        for field, minimum in get_capacity_reserve().items():
            value = host_capacity.get(field)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or value < minimum:
                raise PipelineError(f"full mode requires at least {minimum} {field.replace('_', ' ')}")
        caps = (None, None)
    steps = _step_specs(identity, mode)
    return PipelinePlan(
        run_id=identity.run_id,
        profile=identity.profile,
        namespace=identity.namespace,
        mode=mode,
        hdfs_root=f"/instacart/{identity.run_id}",
        topic=f"instacart-purchase-events-{identity.run_id}",
        trend_source_namespace=TREND_SOURCE_NAMESPACE,
        fixture_max_users=caps[0],
        fixture_max_events=caps[1],
        steps=steps,
    )


def locked_image(lock: Mapping[str, Any], role: str) -> str:
    images = lock.get("images")
    item = images.get(role) if isinstance(images, Mapping) else None
    if not isinstance(item, Mapping):
        raise PipelineError(f"complete B3 lock is missing images.{role}")
    reference = item.get("reference")
    digest = item.get("immutable_digest")
    if not isinstance(reference, str) or not _IMAGE_REFERENCE.fullmatch(reference):
        raise PipelineError(f"images.{role}.reference is malformed")
    if not isinstance(digest, str) or not IMAGE_DIGEST_PATTERN.fullmatch(digest):
        raise PipelineError(f"images.{role}.immutable_digest is not a SHA-256 digest")
    if "@" in reference and reference.rsplit("@", 1)[1] != digest:
        raise PipelineError(f"images.{role} reference and digest disagree")
    base = reference.rsplit("@", 1)[0]
    if base.endswith(":latest"):
        raise PipelineError(f"images.{role} must not use latest")
    return f"{base}@{digest}"


def validate_pipeline_lock(lock: Mapping[str, Any]) -> None:
    try:
        validate_version_lock(lock)
    except Exception as exc:
        raise PipelineError("B3 requires a finalized, complete B2 runtime lock") from exc
    for role in PIPELINE_IMAGE_ROLES:
        locked_image(lock, role)
    component_provenance = lock.get("pipeline_component_image_provenance")
    if not isinstance(component_provenance, Mapping) or set(component_provenance) != set(PIPELINE_COMPONENT_IMAGE_ROLES):
        raise PipelineError("B3 lock is missing component image digest, config, and platform provenance")
    for role in PIPELINE_COMPONENT_IMAGE_ROLES:
        record = component_provenance[role]
        image = lock["images"][role]
        if (
            not isinstance(record, Mapping)
            or record.get("reference") != image.get("reference")
            or record.get("immutable_digest") != image.get("immutable_digest")
            or record.get("platform") != lock["cluster"]["architecture"]
            or not isinstance(record.get("config_digest"), str)
            or not IMAGE_DIGEST_PATTERN.fullmatch(record["config_digest"])
        ):
            raise PipelineError(f"B3 component image provenance is invalid for {role}")
    provenance = lock.get("pipeline_image_provenance")
    if not isinstance(provenance, Mapping):
        raise PipelineError("B3 lock is missing pipeline_image_provenance")
    image = lock["images"]["pipeline_application_image"]
    if provenance.get("image_digest") != image.get("immutable_digest"):
        raise PipelineError("B3 pipeline image provenance does not match its locked digest")
    if provenance.get("source_revision") != lock.get("image_provenance", {}).get("api_application_image", {}).get("source_revision"):
        raise PipelineError("B3 and serving images must share one clean source revision")
    if (
        provenance.get("reference") != str(image.get("reference", "")).split("@", 1)[0]
        or provenance.get("source_tree_sha256") != lock.get("image_provenance", {}).get("api_application_image", {}).get("source_tree_sha256")
    ):
        raise PipelineError("B3 pipeline image tag or source tree differs from the serving build")
    for field in ("image_digest", "config_digest"):
        if not isinstance(provenance.get(field), str) or not IMAGE_DIGEST_PATTERN.fullmatch(provenance[field]):
            raise PipelineError(f"B3 pipeline image provenance.{field} is invalid")
    for field in ("source_revision",):
        if not isinstance(provenance.get(field), str) or not re.fullmatch(r"[0-9a-f]{40}", provenance[field]):
            raise PipelineError(f"B3 pipeline image provenance.{field} is invalid")
    for field in ("source_tree_sha256", "build_metadata_sha256", "python_environment_sha256"):
        if not isinstance(provenance.get(field), str) or not re.fullmatch(r"[0-9a-f]{64}", provenance[field]):
            raise PipelineError(f"B3 pipeline image provenance.{field} is invalid")
    if provenance.get("source_dirty") is not False or provenance.get("platform") != lock["cluster"]["architecture"]:
        raise PipelineError("B3 image must be built cleanly for the locked cluster platform")
    if not re.fullmatch(r"\d+\.\d+\.\d+", str(provenance.get("python_version", ""))):
        raise PipelineError("B3 pipeline image Python version is not exact")
    python_dependencies = lock.get("python_dependencies", {})
    environments = python_dependencies.get("resolved_environment_digests", {}) if isinstance(python_dependencies, Mapping) else {}
    if (
        provenance.get("python_version") != python_dependencies.get("spark_parent_python")
        or provenance.get("python_environment_sha256") != environments.get("spark_parent", "").removeprefix("sha256:")
    ):
        raise PipelineError("B3 image Python environment does not match the locked Spark parent")


def _load_yaml(path: Path) -> list[dict[str, Any]]:
    try:
        value = list(yaml.safe_load_all(path.read_text(encoding="utf-8")))
    except (OSError, UnicodeError, yaml.YAMLError) as exc:
        raise PipelineError(f"pipeline manifest {path.name} is unavailable or invalid") from exc
    documents = [item for item in value if item is not None]
    if not documents or any(not isinstance(item, dict) for item in documents):
        raise PipelineError(f"pipeline manifest {path.name} must contain Kubernetes objects")
    return documents


def validate_pipeline_manifest_sources(manifest_directory: Path | str = PIPELINE_MANIFESTS) -> dict[str, Any]:
    """Static validation of checked-in B3 resources without reading kubeconfig."""
    root = Path(manifest_directory)
    platform = _load_yaml(root / "platform.yaml")
    policies = _load_yaml(root / "network-policies.yaml")
    validate_pipeline_policy_inventory(policies)
    forbidden = {"hostPath", "hostNetwork", "hostPID", "hostIPC", "privileged"}
    for document in platform + policies + [_load_yaml(root / "job.yaml")[0]]:
        encoded = json.dumps(document)
        if "/var/run/docker.sock" in encoded:
            raise PipelineError("B3 resources must not reference host Docker sockets or Compose storage")
        if "latest" in encoded.lower():
            raise PipelineError("B3 resources must use immutable locked images")
        def inspect(value):
            if isinstance(value, Mapping):
                for key, item in value.items():
                    if key in forbidden and item not in (None, False):
                        raise PipelineError(f"B3 resource uses forbidden host capability {key}")
                    inspect(item)
            elif isinstance(value, list):
                for item in value:
                    inspect(item)
        inspect(document)
    claims = {
        item.get("metadata", {}).get("name")
        for item in platform if item.get("kind") == "PersistentVolumeClaim"
    }
    expected_claims = {
        *claim_names(),
    }
    if claims != expected_claims:
        raise PipelineError("B3 pipeline storage claims do not match the isolated storage contract")
    claim_requests = {
        item.get("metadata", {}).get("name"): item.get("spec", {}).get("resources", {}).get("requests", {}).get("storage")
        for item in platform if item.get("kind") == "PersistentVolumeClaim"
    }
    if claim_requests != expected_claim_requests():
        raise PipelineError(f"B3 PVC requests do not match the {PIPELINE_STORAGE_BUDGET} run storage budget")
    kafka = next(
        item for item in platform
        if item.get("kind") == "StatefulSet" and item.get("metadata", {}).get("name") == "kafka"
    )
    kafka_containers = kafka.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
    kafka_container = next((item for item in kafka_containers if item.get("name") == "kafka"), None)
    kafka_env = kafka_container.get("env", []) if isinstance(kafka_container, Mapping) else None
    if not isinstance(kafka_env, list):
        raise PipelineError("Kafka environment must be an explicit list")
    kafka_values = {}
    for item in kafka_env:
        if (
            not isinstance(item, Mapping)
            or not isinstance(item.get("name"), str)
            or not isinstance(item.get("value"), str)
            or item["name"] in kafka_values
        ):
            raise PipelineError("Kafka environment names and values must be unique strings")
        kafka_values[item["name"]] = item["value"]
    expected_kafka_values = {
        "KAFKA_NODE_ID": "1",
        "KAFKA_PROCESS_ROLES": "controller,broker",
        "KAFKA_LISTENERS": "INTERNAL://:29092,CONTROLLER://:9093",
        "KAFKA_ADVERTISED_LISTENERS": "INTERNAL://kafka:29092",
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT",
        "KAFKA_INTER_BROKER_LISTENER_NAME": "INTERNAL",
        "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
        "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@127.0.0.1:9093",
        "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
        "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
        "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
        "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",
        "KAFKA_AUTO_CREATE_TOPICS_ENABLE": "true",
        "KAFKA_LOG_DIRS": "/var/lib/kafka/data",
        "KAFKA_LOG_RETENTION_HOURS": "24",
        "KAFKA_LOG_RETENTION_BYTES": "1073741824",
        "KAFKA_LOG_SEGMENT_BYTES": "134217728",
        "KAFKA_NUM_NETWORK_THREADS": "2",
        "KAFKA_NUM_IO_THREADS": "2",
        "KAFKA_HEAP_OPTS": "-Xms512m -Xmx512m",
    }
    if kafka_values != expected_kafka_values:
        raise PipelineError("Kafka environment does not match the one-broker KRaft contract")
    services = {item.get("metadata", {}).get("name"): item for item in platform if item.get("kind") == "Service"}
    expected_ports = {
        "namenode": {"rpc": (8020, 8020), "webhdfs": (9870, 9870)},
        "datanode": {"web": (9864, 9864), "transfer": (9866, 9866)},
        "kafka": {"broker": (29092, 29092), "controller": (9093, 9093)},
    }
    if set(services) != set(expected_ports):
        raise PipelineError("B3 services do not match the isolated HDFS/Kafka endpoints")
    for name, ports in expected_ports.items():
        actual = {
            item.get("name"): (item.get("port"), item.get("targetPort"))
            for item in services[name].get("spec", {}).get("ports", [])
        }
        if actual != ports:
            raise PipelineError(f"B3 {name} service ports do not match the data-service contract")
    statefulsets = {item.get("metadata", {}).get("name"): item for item in platform if item.get("kind") == "StatefulSet"}
    datanode_spec = statefulsets.get("hdfs-datanode", {}).get("spec", {}).get("template", {}).get("spec", {}).get("containers", [{}])[0]
    if datanode_spec.get("readinessProbe", {}).get("httpGet", {}).get("port") != 9864:
        raise PipelineError("B3 DataNode readiness must probe the DataNode WebHDFS HTTP port 9864")
    kafka_ports = {
        item.get("containerPort") for item in statefulsets.get("kafka", {}).get("spec", {}).get("template", {}).get("spec", {}).get("containers", [{}])[0].get("ports", [])
    }
    if kafka_ports != {29092, 9093}:
        raise PipelineError("B3 Kafka broker and KRaft controller listeners are incomplete")
    return {"status": "static-valid", "platform_objects": len(platform), "pipeline_policies": sorted(PIPELINE_POLICY_NAMES), "pipeline_claims": sorted(claims)}


def _dump_yaml(path: Path, documents: list[dict[str, Any]]) -> None:
    path.write_text(yaml.safe_dump_all(documents, sort_keys=False), encoding="utf-8")
    path.chmod(0o600)


def _validate_render_root(render_root: Path, identity: RunIdentity) -> None:
    root = render_root.resolve(strict=True)
    if not root.is_dir() or not (root / ".it4931-k8s-render-root").is_file():
        raise PipelineError("B3 assets must be rendered beneath an existing private B2-owned output root")
    namespace = root / "run" / "namespace.json"
    try:
        doc = json.loads(namespace.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise PipelineError("private B2 render does not contain the matching run namespace") from exc
    if doc.get("metadata", {}).get("name") != identity.namespace:
        raise PipelineError("private B2 render namespace does not match the B3 run identity")


def render_pipeline_assets(
    identity: RunIdentity,
    render_root: Path | str,
    lock: Mapping[str, Any],
    *,
    mode: str,
    host_capacity: Mapping[str, Any] | None = None,
    source_receipt_sha256: str | None = None,
    input_directory: Path | str | None = None,
) -> Path:
    """Write digest-pinned B3 manifests beneath an existing private B2 render."""
    validate_pipeline_manifest_sources()
    validate_pipeline_lock(lock)
    plan = plan_pipeline(
        identity, mode=mode, host_capacity=host_capacity,
        source_receipt_sha256=source_receipt_sha256,
    )
    as_of_event_time = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    root = Path(render_root)
    _validate_render_root(root, identity)
    images = {role: locked_image(lock, role) for role in PIPELINE_IMAGE_ROLES}
    output = root / "pipeline"
    output.mkdir(mode=0o700, exist_ok=False)
    for source_name in ("platform.yaml", "network-policies.yaml"):
        documents = _load_yaml(PIPELINE_MANIFESTS / source_name)
        replacements = {
            "__K8S_RENDER_REQUIRED__": identity.namespace,
            "__RUN_ID__": identity.run_id,
            "__MODE__": mode,
            "__SOURCE_RECEIPT_SHA256__": source_receipt_sha256 or "fixture-generated-in-image",
            "__AS_OF_EVENT_TIME__": as_of_event_time,
            **{placeholder: images[role] for role, placeholder in PIPELINE_IMAGE_ROLES.items()},
        }
        documents = _replace_manifest_tokens(documents, replacements)
        content = yaml.safe_dump_all(documents, sort_keys=False)
        unresolved = re.findall(r"__[A-Z0-9_]+__", content)
        if unresolved:
            raise PipelineError(f"B3 manifest has unresolved render fields: {', '.join(sorted(set(unresolved)))}")
        (output / source_name).write_text(content, encoding="utf-8")
        (output / source_name).chmod(0o600)

    owner_labels = {
        "it4931.instacart.edu/owner": "instacart-market-basket-analysis",
        "it4931.instacart.edu/run-id": identity.run_id,
        "app.kubernetes.io/managed-by": "it4931-k8s-demo",
    }
    for name in ("platform.yaml", "network-policies.yaml"):
        documents = _load_yaml(output / name)
        for document in documents:
            metadata = document.setdefault("metadata", {})
            labels = metadata.setdefault("labels", {})
            labels.update(owner_labels)
            labels.setdefault("it4931.instacart.edu/component", "pipeline")
            pod_template = document.get("spec", {}).get("template", {}).get("metadata")
            if isinstance(pod_template, dict):
                pod_labels = pod_template.setdefault("labels", {})
                pod_labels.update(owner_labels)
        _dump_yaml(output / name, documents)

    database = demo_database(identity.run_id)

    template = _load_yaml(PIPELINE_MANIFESTS / "job.yaml")[0]
    for step in plan.steps:
        document = json.loads(json.dumps(template))
        substitutions = {
            "__K8S_RENDER_REQUIRED__": identity.namespace,
            "__RUN_ID__": identity.run_id,
            "__STEP__": step.name,
            "__BACKOFF_LIMIT__": step.backoff_limit,
            "__DEADLINE_SECONDS__": step.timeout_seconds,
            "__PIPELINE_IMAGE__": images["pipeline_application_image"],
            "__DATABASE__": database,
        }
        document = _replace_manifest_tokens(document, substitutions)
        if step.name in {"input", "module1"}:
            document["spec"]["template"]["spec"]["containers"][0]["env"] = [
                variable for variable in document["spec"]["template"]["spec"]["containers"][0]["env"]
                if variable.get("name") != "MONGODB_URI"
            ]
        if step.name != "replay":
            container = document["spec"]["template"]["spec"]["containers"][0]
            container["volumeMounts"] = [mount for mount in container["volumeMounts"] if mount.get("name") != "pipeline-checkpoints"]
            pod_spec = document["spec"]["template"]["spec"]
            pod_spec["volumes"] = [volume for volume in pod_spec["volumes"] if volume.get("name") != "pipeline-checkpoints"]
        if step.name == "input":
            container = document["spec"]["template"]["spec"]["containers"][0]
            container["resources"] = {
                "requests": {"cpu": "100m", "memory": "256Mi", "ephemeral-storage": "128Mi"},
                "limits": {"cpu": "500m", "memory": "512Mi", "ephemeral-storage": "1Gi"},
            }
        document["metadata"].setdefault("labels", {}).update(owner_labels)
        document["spec"]["template"]["metadata"].setdefault("labels", {}).update(owner_labels)
        _dump_yaml(output / f"job-{step.name}.yaml", [document])

    image_records = {role: lock["images"][role] for role in PIPELINE_IMAGE_ROLES}
    image_records["pipeline_image_provenance"] = lock["pipeline_image_provenance"]
    image_records["pipeline_component_image_provenance"] = lock["pipeline_component_image_provenance"]
    manifest_digests = {
        name: hashlib.sha256((output / name).read_bytes()).hexdigest()
        for name in ("platform.yaml", "network-policies.yaml", *(f"job-{step.name}.yaml" for step in plan.steps))
    }
    manifest = {
        "schema_version": 1,
        "run_id": identity.run_id,
        "profile": identity.profile,
        "namespace": identity.namespace,
        "mode": mode,
        "hdfs_root": plan.hdfs_root,
        "topic": plan.topic,
        "trend_source_namespace": plan.trend_source_namespace,
        "fixture_max_users": plan.fixture_max_users,
        "fixture_max_events": plan.fixture_max_events,
        "as_of_event_time": as_of_event_time,
        "execution_model": plan.execution_model,
        "source_receipt_sha256": source_receipt_sha256,
        "pipeline_lock_sha256": hashlib.sha256(json.dumps(image_records, sort_keys=True).encode()).hexdigest(),
        "manifest_sha256": manifest_digests,
        "steps": [asdict(step) for step in plan.steps],
    }
    (output / "plan.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (output / "plan.json").chmod(0o600)
    validate_pipeline_plan(
        output,
        identity,
        lock,
        host_capacity=host_capacity,
        source_receipt_sha256=source_receipt_sha256,
        input_directory=input_directory,
    )
    return output


def _replace_manifest_tokens(value: Any, substitutions: Mapping[str, Any]) -> Any:
    if isinstance(value, str):
        if value in substitutions:
            return substitutions[value]
        replaced = value
        for token, replacement in substitutions.items():
            if token in replaced:
                if not isinstance(replacement, str):
                    raise PipelineError(f"typed manifest field {token} must occupy a complete YAML scalar")
                replaced = replaced.replace(token, replacement)
        if re.search(r"__[A-Z0-9_]+__", replaced):
            raise PipelineError("B3 Job manifest contains an unresolved render field")
        return replaced
    if isinstance(value, list):
        return [_replace_manifest_tokens(item, substitutions) for item in value]
    if isinstance(value, dict):
        return {key: _replace_manifest_tokens(item, substitutions) for key, item in value.items()}
    return value


def _checked(executor: PipelineCommandExecutor, command: list[str], description: str, timeout: int) -> Any:
    try:
        result = executor.run(command, timeout=timeout)
    except Exception as exc:
        raise PipelineError(f"{description} failed ({type(exc).__name__}); resources are retained") from exc
    if result.returncode != 0:
        raise PipelineError(f"{description} failed with exit status {result.returncode}; resources are retained")
    return result


def _require_pipeline_namespace_clear(executor: PipelineCommandExecutor, identity: RunIdentity) -> None:
    """Reject stale or colliding B3 names before applying any pipeline resource."""
    expected = _direct_pipeline_resources(identity)
    observed = set()
    for resource_type in ("configmaps", "serviceaccounts", "services", "persistentvolumeclaims", "statefulsets", "networkpolicies", "jobs", "pods"):
        try:
            result = executor.run(
                kubectl(identity, "--namespace", identity.namespace, "get", resource_type, "-o", "json"),
                timeout=60,
            )
            payload = json.loads(result.stdout)
        except Exception as exc:
            raise PipelineError(f"B3 {resource_type} preflight failed ({type(exc).__name__}); no resources were applied") from exc
        if result.returncode != 0 or not isinstance(payload, Mapping) or not isinstance(payload.get("items"), list):
            raise PipelineError(f"B3 {resource_type} preflight returned an invalid inventory; no resources were applied")
        for item in payload["items"]:
            if not isinstance(item, Mapping) or not isinstance(item.get("metadata"), Mapping):
                raise PipelineError("B3 namespace contains a malformed object; no resources were applied")
            metadata = item["metadata"]
            key = (item.get("kind"), metadata.get("name"))
            if not all(isinstance(value, str) and value for value in key) or metadata.get("namespace") != identity.namespace:
                raise PipelineError("B3 namespace inventory contains an invalid object identity; no resources were applied")
            labels = metadata.get("labels", {})
            if key in expected or (
                isinstance(labels, Mapping)
                and labels.get("it4931.instacart.edu/owner") == "instacart-market-basket-analysis"
                and labels.get("it4931.instacart.edu/run-id") == identity.run_id
                and labels.get("it4931.instacart.edu/component") == "pipeline"
            ):
                observed.add(key)
    if observed:
        raise PipelineError("B3 reserved resource names already exist in the run namespace; no resources were applied")


def validate_pipeline_plan(
    pipeline_directory: Path | str,
    identity: RunIdentity,
    lock: Mapping[str, Any],
    *,
    host_capacity: Mapping[str, Any] | None,
    source_receipt_sha256: str | None,
    input_directory: Path | str | None,
) -> dict[str, Any]:
    """Validate the private rendered plan using the same contract as execution."""
    validate_pipeline_lock(lock)
    directory = Path(pipeline_directory).resolve(strict=True)
    render_root = directory.parent
    _validate_render_root(render_root, identity)
    if directory != render_root / "pipeline":
        raise PipelineError("B3 plan must be beneath the matching private B2 render")
    try:
        plan = json.loads((directory / "plan.json").read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise PipelineError("private B3 pipeline plan is missing or invalid") from exc
    if (
        plan.get("run_id") != identity.run_id
        or plan.get("namespace") != identity.namespace
        or plan.get("profile") != identity.profile
    ):
        raise PipelineError("private B3 plan does not match the requested profile, context, and namespace")
    if plan.get("mode") not in {"fixture", "full"}:
        raise PipelineError("private B3 plan has an unsupported mode")
    as_of_event_time = plan.get("as_of_event_time")
    if not isinstance(as_of_event_time, str):
        raise PipelineError("private B3 plan publication time must remain a YAML/JSON string")
    try:
        parsed_as_of = datetime.fromisoformat(as_of_event_time.replace("Z", "+00:00"))
    except ValueError as exc:
        raise PipelineError("private B3 plan has no valid stable UTC publication time") from exc
    if parsed_as_of.tzinfo is None or parsed_as_of.utcoffset() != timezone.utc.utcoffset(parsed_as_of):
        raise PipelineError("private B3 publication time must be a stable UTC timestamp")
    platform_docs = _load_yaml(directory / "platform.yaml")
    pipeline_config = next(
        (
            item for item in platform_docs
            if item.get("kind") == "ConfigMap"
            and item.get("metadata", {}).get("name") == "it4931-pipeline-config"
        ),
        None,
    )
    if pipeline_config is None or pipeline_config.get("data", {}).get("PIPELINE_AS_OF_EVENT_TIME") != as_of_event_time:
        raise PipelineError("B3 task configuration does not match the stable reviewed publication time")
    if plan["mode"] == "full" and source_receipt_sha256 != plan.get("source_receipt_sha256"):
        raise PipelineError("full-data execution must repeat the preflight source receipt SHA-256")
    if plan["mode"] == "full":
        if input_directory is None:
            raise PipelineError("full-data plan validation requires the exact six-file input directory")
        from .data_receipts import InputReceiptError, inspect_input_directory

        try:
            input_receipt = inspect_input_directory(input_directory)
        except InputReceiptError as exc:
            raise PipelineError("full-data source directory failed its six-file preflight") from exc
        if input_receipt["manifest_sha256"] != source_receipt_sha256:
            raise PipelineError("full-data input bytes differ from the reviewed source receipt SHA-256")
        validate_full_data_storage_budget(input_receipt["total_bytes"])
    elif input_directory is not None:
        raise PipelineError("fixture mode creates its deterministic input inside the run-scoped PVC")
    expected_plan = plan_pipeline(
        identity,
        mode=plan["mode"],
        host_capacity=host_capacity or lock.get("host_capacity"),
        source_receipt_sha256=(source_receipt_sha256 if plan["mode"] == "full" else None),
    )
    expected_plan_dict = asdict(expected_plan)
    for field in (
        "hdfs_root", "topic", "trend_source_namespace", "fixture_max_users",
        "fixture_max_events", "execution_model",
    ):
        if plan.get(field) != expected_plan_dict.get(field):
            raise PipelineError(f"private B3 {field} does not match the validated run plan")
    expected_steps = [asdict(step) for step in _step_specs(identity, plan["mode"])]
    if plan.get("steps") != expected_steps:
        raise PipelineError("private B3 dependency order or retry policy differs from the reviewed plan")
    locked_images = {role: lock["images"][role] for role in PIPELINE_IMAGE_ROLES}
    locked_images["pipeline_image_provenance"] = lock["pipeline_image_provenance"]
    locked_images["pipeline_component_image_provenance"] = lock["pipeline_component_image_provenance"]
    lock_digest = hashlib.sha256(json.dumps(locked_images, sort_keys=True).encode()).hexdigest()
    if plan.get("pipeline_lock_sha256") != lock_digest:
        raise PipelineError("private B3 image references do not match the finalized lock")
    digests = plan.get("manifest_sha256")
    expected_names = {
        "platform.yaml", "network-policies.yaml",
        *(f"job-{step}.yaml" for step in PIPELINE_STEPS),
    }
    if not isinstance(digests, Mapping) or set(digests) != expected_names:
        raise PipelineError("private B3 plan does not contain the exact manifest inventory")
    for name, expected in digests.items():
        path = directory / name
        try:
            actual = hashlib.sha256(path.read_bytes()).hexdigest()
        except OSError as exc:
            raise PipelineError("private B3 manifest is missing") from exc
        if actual != expected:
            raise PipelineError(f"private B3 manifest {name} differs from its reviewed digest")
    return plan


def _direct_pipeline_resources(identity: RunIdentity) -> set[tuple[str, str]]:
    return {
        *{("ConfigMap", "it4931-pipeline-config"), ("ServiceAccount", "module4d-pipeline")},
        *{("PersistentVolumeClaim", name) for name in claim_names()},
        *{("Service", name) for name in ("namenode", "datanode", "kafka")},
        *{("StatefulSet", name) for name in ("hdfs-namenode", "hdfs-datanode", "kafka")},
        *{("NetworkPolicy", name) for name in PIPELINE_POLICY_NAMES},
        *{("Job", pipeline_job_name(step, identity.run_id)) for step in PIPELINE_STEPS},
        *{("Pod", f"{name}-0") for name in ("hdfs-namenode", "hdfs-datanode", "kafka")},
    }


def execute_pipeline(
    executor: PipelineCommandExecutor,
    identity: RunIdentity,
    pipeline_directory: Path | str,
    *,
    lock: Mapping[str, Any],
    serving_receipt_path: Path | str,
    host_capacity: Mapping[str, Any] | None = None,
    source_receipt_sha256: str | None = None,
    input_directory: Path | str | None = None,
    execute: bool = False,
) -> dict[str, Any]:
    """Apply isolated platform resources and run Jobs in their strict dependency order."""
    if execute is not True:
        raise PipelineError("B3 execution requires explicit execute=True authorization")
    validate_pipeline_lock(lock)
    from .runtime import RuntimePreflightError, validate_host_capacity

    try:
        validate_host_capacity(dict(host_capacity or {}), phase="b3")
    except RuntimePreflightError as exc:
        raise PipelineError("B3 deployment requires fresh measured 6 CPU / 12 GiB RAM / 40 GiB disk capacity") from exc
    directory = Path(pipeline_directory).resolve(strict=True)
    render_root = directory.parent
    _validate_render_root(render_root, identity)
    if directory != render_root / "pipeline":
        raise PipelineError("B3 execution must use the pipeline directory in the private B2 render")
    plan = validate_pipeline_plan(
        directory,
        identity,
        lock,
        host_capacity=host_capacity,
        source_receipt_sha256=source_receipt_sha256,
        input_directory=input_directory,
    )
    serving_path = Path(serving_receipt_path)
    if serving_path.is_symlink() or serving_path.resolve() != render_root / "b2-ownership-receipt.json":
        raise PipelineError("B3 requires the matching private B2 ownership receipt")
    try:
        recorded_document = json.loads(serving_path.read_text(encoding="utf-8"))
        from .receipts import (
            assert_receipt_matches_lock_and_live,
            collect_runtime_ownership_receipt,
            receipt_from_document,
            validate_collected_receipt,
        )

        recorded_serving = receipt_from_document(recorded_document)
        validate_collected_receipt(recorded_serving, lock)
        live_serving = collect_runtime_ownership_receipt(executor, identity, lock, execute=True)
        assert_receipt_matches_lock_and_live(recorded_serving, live_serving, lock, identity)
    except Exception as exc:
        raise PipelineError("B2 serving ownership receipt does not match the live run; B3 was not applied") from exc
    from .network_policy import collect_network_policy_inventory

    try:
        collect_network_policy_inventory(executor, identity, execute=True)
    except Exception as exc:
        raise PipelineError("B2 deny-first policy inventory failed before B3; no pipeline resources were applied") from exc
    execution_receipt_path = directory / "execution-receipt.json"
    completed_steps: list[str] = []
    if execution_receipt_path.is_file():
        try:
            cached_receipt = json.loads(execution_receipt_path.read_text(encoding="utf-8"))
            if cached_receipt.get("run_id") == identity.run_id and isinstance(cached_receipt.get("completed_steps"), list):
                completed_steps = list(cached_receipt["completed_steps"])
        except Exception:
            pass

    if not completed_steps:
        _require_pipeline_namespace_clear(executor, identity)
    try:
        load_pipeline_images(executor, identity, lock, execute=True)
    except PipelineError as exc:
        raise PipelineError("B3 pipeline images failed identity verification before workload apply") from exc

    receipt: dict[str, Any] = {
        "schema_version": 1, "run_id": identity.run_id, "profile": identity.profile,
        "namespace": identity.namespace, "mode": plan["mode"], "status": "running",
        "started_at_utc": datetime.now(timezone.utc).isoformat(), "completed_steps": list(completed_steps),
    }

    if "platform" not in completed_steps:
        _checked(executor, kubectl(identity, "apply", "-f", str(directory / "platform.yaml")), "pipeline platform apply", 120)
        _checked(executor, kubectl(identity, "apply", "-f", str(directory / "network-policies.yaml")), "pipeline network-policy apply", 120)
        for claim in claim_names():
            _checked(
                executor,
                kubectl(identity, "--namespace", identity.namespace, "wait", "--for=jsonpath={.status.phase}=Bound", f"pvc/{claim}", "--timeout=900s"),
                f"{claim} binding",
                930,
            )
        for resource, timeout_seconds in (("statefulset/hdfs-namenode", 900), ("statefulset/hdfs-datanode", 900), ("statefulset/kafka", 900)):
            _checked(executor, kubectl(identity, "--namespace", identity.namespace, "rollout", "status", resource, f"--timeout={timeout_seconds}s"), f"{resource} readiness", timeout_seconds + 30)
        from .hdfs_lifecycle import bootstrap_hdfs

        receipt["hdfs_bootstrap"] = bootstrap_hdfs(executor, identity, execute=True)
        receipt["completed_steps"].append("platform")
        execution_receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        execution_receipt_path.chmod(0o600)

    for step in plan["steps"]:
        if step["name"] in completed_steps:
            continue
        manifest = directory / f"job-{step['name']}.yaml"
        expected_name = step["job_name"]
        if not manifest.is_file():
            raise PipelineError(f"private Job manifest is missing for {step['name']}; resources are retained")
        _checked(executor, kubectl(identity, "apply", "-f", str(manifest)), f"{step['name']} Job apply", 120)
        if step["name"] == "input" and plan["mode"] == "full":
            selector = f"it4931.instacart.edu/component=pipeline,it4931.instacart.edu/run-id={identity.run_id},it4931.instacart.edu/step=input"
            _checked(
                executor,
                kubectl(identity, "--namespace", identity.namespace, "wait", "--for=condition=Ready", "pod", "-l", selector, "--timeout=300s"),
                "full-data input stager readiness",
                330,
            )
            try:
                pod_result = executor.run(
                    kubectl(identity, "--namespace", identity.namespace, "get", "pods", "-l", selector, "-o", "jsonpath={.items[0].metadata.name}"),
                    timeout=60,
                )
            except Exception as exc:
                raise PipelineError(f"full-data input stager Pod lookup failed ({type(exc).__name__}); resources are retained") from exc
            pod_name = pod_result.stdout.strip()
            if pod_result.returncode != 0 or not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", pod_name):
                raise PipelineError("full-data input stager did not resolve to exactly one valid run-scoped Pod")
            from .data_receipts import REQUIRED_HEADERS

            input_root = Path(input_directory)
            for filename in REQUIRED_HEADERS:
                _checked(
                    executor,
                    kubectl(identity, "--namespace", identity.namespace, "cp", str(input_root / filename), f"{pod_name}:/run/pipeline/input/{filename}"),
                    f"full-data transfer for {filename}",
                    900,
                )
            _checked(
                executor,
                kubectl(identity, "--namespace", identity.namespace, "exec", pod_name, "--", "python3", "-c", "from pathlib import Path; Path('/run/pipeline/input/.upload-complete').touch()"),
                "full-data input transfer completion marker",
                60,
            )
        try:
            _checked(
                executor,
                kubectl(identity, "--namespace", identity.namespace, "wait", "--for=condition=complete", f"job/{expected_name}", f"--timeout={step['timeout_seconds']}s"),
                f"{step['name']} Job completion",
                step["timeout_seconds"] + 30,
            )
        except PipelineError as exc:
            receipt.update(status="failed", failed_step=step["name"], failure_type=type(exc).__name__, completed_at_utc=datetime.now(timezone.utc).isoformat())
            execution_receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            raise
        receipt["completed_steps"].append(step["name"])
        receipt["last_completed_job"] = expected_name
        execution_receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    from .network_policy import collect_network_policy_inventory
    from .pipeline_receipts import collect_pipeline_receipt, pipeline_receipt_json, validate_pipeline_receipt

    try:
        policy_receipt = collect_network_policy_inventory(executor, identity, execute=True, include_pipeline=True)
        ownership_receipt = collect_pipeline_receipt(executor, identity, lock, execute=True)
        validate_pipeline_receipt(identity, ownership_receipt, lock)
    except Exception as exc:
        receipt.update(status="receipt_failed", failure_type=type(exc).__name__, completed_at_utc=datetime.now(timezone.utc).isoformat())
        execution_receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        raise PipelineError("B3 Jobs completed, but live ownership or storage receipt validation failed; all resources are retained") from exc
    ownership_path = directory / "pipeline-ownership-receipt.json"
    ownership_path.write_text(pipeline_receipt_json(ownership_receipt), encoding="utf-8")
    ownership_path.chmod(0o600)
    receipt["ownership_receipt"] = str(ownership_path)
    receipt["network_policy_inventory"] = policy_receipt
    receipt["b2_receipt_sha256"] = hashlib.sha256(serving_path.read_bytes()).hexdigest()
    receipt.update(status="completed", completed_at_utc=datetime.now(timezone.utc).isoformat())
    execution_receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return receipt


def finalize_pipeline_lock(
    complete_b2_lock: Mapping[str, Any],
    *,
    component_images: Mapping[str, Mapping[str, str]],
    pipeline_image: Mapping[str, Any],
) -> dict[str, Any]:
    """Attach measured B3 identities to an already finalized serving lock."""
    try:
        validate_version_lock(complete_b2_lock)
    except Exception as exc:
        raise PipelineError("B3 lock finalization requires a valid complete B2 lock") from exc
    if set(component_images) != set(PIPELINE_COMPONENT_IMAGE_ROLES):
        raise PipelineError("B3 image evidence must include Kafka, HDFS NameNode, and HDFS DataNode")
    result = copy.deepcopy(dict(complete_b2_lock))
    for role in PIPELINE_COMPONENT_IMAGE_ROLES:
        record = component_images[role]
        expected_reference = result["images"][role]["reference"].split("@", 1)[0]
        if record.get("reference") != expected_reference or record.get("platform") != result["cluster"]["architecture"]:
            raise PipelineError(f"B3 component image evidence for {role} does not match the selected release/platform")
        digest = record.get("immutable_digest")
        if not isinstance(digest, str) or not IMAGE_DIGEST_PATTERN.fullmatch(digest):
            raise PipelineError(f"B3 component image evidence for {role} is not immutable")
        if not isinstance(record.get("config_digest"), str) or not IMAGE_DIGEST_PATTERN.fullmatch(record["config_digest"]):
            raise PipelineError(f"B3 component image config identity for {role} is missing")
        result["images"][role]["immutable_digest"] = digest
        result["images"][role]["reference"] = f"{result['images'][role]['reference'].split('@', 1)[0]}@{digest}"
    digest = pipeline_image.get("image_digest")
    config_digest = pipeline_image.get("config_digest")
    if not isinstance(digest, str) or not IMAGE_DIGEST_PATTERN.fullmatch(digest):
        raise PipelineError("B3 BuildKit manifest digest is missing")
    if not isinstance(config_digest, str) or not IMAGE_DIGEST_PATTERN.fullmatch(config_digest):
        raise PipelineError("B3 BuildKit config digest is missing")
    if pipeline_image.get("source_dirty") is not False:
        raise PipelineError("B3 application image must come from a clean source tree")
    expected_revision = result["image_provenance"]["api_application_image"]["source_revision"]
    if pipeline_image.get("source_revision") != expected_revision:
        raise PipelineError("B3 and serving images do not share the locked source SHA")
    reference = pipeline_image.get("reference")
    if not isinstance(reference, str) or not _IMAGE_REFERENCE.fullmatch(reference) or ":latest" in reference:
        raise PipelineError("B3 pipeline image reference must be an exact run-specific immutable tag")
    result["images"]["pipeline_application_image"]["immutable_digest"] = digest
    result["images"]["pipeline_application_image"]["reference"] = f"{reference}@{digest}"
    result["pipeline_image_provenance"] = dict(pipeline_image)
    result["pipeline_component_image_provenance"] = {}
    for role, record in component_images.items():
        result["pipeline_component_image_provenance"][role] = {
            "reference": result["images"][role]["reference"],
            "immutable_digest": record["immutable_digest"],
            "config_digest": record["config_digest"],
            "platform": record["platform"],
        }
    try:
        from .lock_lifecycle import validate_static_lock

        validate_static_lock(result)
    except Exception as exc:
        raise PipelineError("B3 lock evidence does not satisfy the static lock contract") from exc
    validate_pipeline_lock(result)
    return result
