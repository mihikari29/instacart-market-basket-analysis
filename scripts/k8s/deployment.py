"""Opt-in B2 serving deployment against one strict, run-scoped version lock."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

import yaml

from .image_operations import ImageOperationError, load_and_verify_application_images
from .image_provenance import collect_cluster_component_images
from .lock_lifecycle import validate_static_lock
from .mongo_secrets import MongoSecretBundle, apply_mongo_secret_bundle
from .network_policy import collect_network_policy_inventory, validate_network_policy_inventory
from .ownership import OwnershipError, RunIdentity, validate_version_lock
from .receipts import collect_runtime_ownership_receipt, validate_collected_receipt
from .render import validate_locked_overlay
from .runtime import (
    CommandExecutor,
    RuntimePreflightError,
    _checked,
    collect_cluster_identity,
    collect_storage_class_identity,
    collect_toolchain_identity,
    kubectl,
    validate_host_capacity,
)


class DeploymentError(RuntimeError):
    """B2 deployment stopped before or during its explicit run-scoped workflow."""


def _check_cluster_contract(cluster: Mapping[str, Any], lock: Mapping[str, Any]) -> None:
    expected = lock["cluster"]
    for field in (
        "profile", "context", "cluster_uid", "kubernetes_version", "driver",
        "container_runtime", "cni", "architecture", "node_count",
    ):
        if cluster.get(field) != expected.get(field):
            raise DeploymentError(f"live cluster {field} does not match the finalized version lock")


def _check_toolchain_contract(observed: Mapping[str, Any], lock: Mapping[str, Any]) -> None:
    expected = lock["toolchain"]
    for field in ("minikube_version", "minikube_binary_sha256", "kubectl_version", "docker_engine_version"):
        if observed.get(field) != expected.get(field):
            raise DeploymentError(f"live toolchain {field} does not match the finalized version lock")


def _check_storage_class_contract(observed: Mapping[str, Any], lock: Mapping[str, Any]) -> None:
    expected = lock["storage"]
    for field in (
        "default_storage_class", "storage_class_uid", "provisioner", "volume_binding_mode", "profile_local"
    ):
        if observed.get(field) != expected.get(field):
            raise DeploymentError(f"live storage {field} does not match the finalized version lock")


def _run(executor: CommandExecutor, command: list[str], *, description: str, timeout: int) -> None:
    try:
        result = executor.run(command, timeout=timeout)
    except Exception as exc:
        raise DeploymentError(f"{description} failed ({type(exc).__name__})") from exc
    if result.returncode != 0:
        executable = Path(command[0]).name
        raise DeploymentError(f"{description} failed: {executable} exited with status {result.returncode}")


def _validate_base_network_policies(run_directory: Path) -> None:
    path = run_directory.parent / "base" / "network-policies.yaml"
    try:
        policies = list(yaml.safe_load_all(path.read_text(encoding="utf-8")))
    except (OSError, UnicodeError, yaml.YAMLError) as exc:
        raise DeploymentError("rendered NetworkPolicy manifests are unavailable") from exc
    validate_network_policy_inventory([item for item in policies if item])


def deploy_serving(
    executor: CommandExecutor,
    identity: RunIdentity,
    lock: Mapping[str, Any],
    *,
    host_capacity: Mapping[str, Any],
    run_directory: Path | str,
    secret_bundle: MongoSecretBundle,
    trend_source_namespace: str | None = None,
    execute: bool = False,
) -> dict[str, Any]:
    """Load locked images, apply namespaced serving resources, and record live proof.

    Failures leave the run-scoped resources in place for inspection. This helper
    never deletes a namespace, profile, PVC, or volume.
    """
    if execute is not True:
        raise DeploymentError("B2 deployment requires explicit execute=True authorization")
    validate_static_lock(lock)
    try:
        validate_version_lock(lock)
    except OwnershipError as exc:
        raise DeploymentError("B2 deployment requires a complete runtime version lock") from exc
    if not isinstance(secret_bundle, MongoSecretBundle) or secret_bundle.run_id != identity.run_id:
        raise DeploymentError("Mongo Secret bundle does not match the run identity")
    try:
        validate_host_capacity(dict(host_capacity), phase="b2")
    except RuntimePreflightError as exc:
        raise DeploymentError("measured host capacity does not pass the B2 reserve") from exc
    run_path = Path(run_directory)
    try:
        validate_locked_overlay(
            identity.run_id,
            run_path,
            dict(lock),
            trend_source_namespace=trend_source_namespace,
        )
        _validate_base_network_policies(run_path)
    except (OSError, ValueError) as exc:
        raise DeploymentError("private render does not match the complete lock and run identity") from exc

    try:
        cluster = collect_cluster_identity(executor, identity, execute=True)
        _check_cluster_contract(cluster, lock)
        toolchain = collect_toolchain_identity(executor, execute=True)
        _check_toolchain_contract(toolchain, lock)
        component_images = collect_cluster_component_images(executor, identity, execute=True)
        if (
            component_images["kubernetes_control_plane_images"]
            != lock["toolchain"].get("kubernetes_control_plane_images")
            or component_images["calico_images"] != lock["toolchain"].get("calico_images")
            or component_images["cni_version"] != lock["cluster"].get("cni_version")
        ):
            raise DeploymentError("live Kubernetes or Calico images do not match the finalized version lock")
        storage_class = collect_storage_class_identity(executor, identity, execute=True)
        _check_storage_class_contract(storage_class, lock)
        loaded_images = load_and_verify_application_images(executor, identity, lock, execute=True)
    except (RuntimePreflightError, ImageOperationError, DeploymentError, ValueError) as exc:
        raise DeploymentError("strict B2 pre-deployment identity checks failed") from exc

    namespace_file = run_path / "namespace.json"
    _run(
        executor,
        kubectl(identity, "apply", "-f", str(namespace_file)),
        description="run namespace creation",
        timeout=60,
    )
    apply_mongo_secret_bundle(executor, identity, secret_bundle, execute=True)
    _run(
        executor,
        kubectl(identity, "--namespace", identity.namespace, "apply", "-k", str(run_path)),
        description="serving manifest apply",
        timeout=120,
    )
    for resource, timeout_seconds in (
        ("statefulset/module4d-mongo", 900),
        ("deployment/module4d-api", 600),
        ("deployment/module4d-ui", 600),
    ):
        _run(
            executor,
            kubectl(
                identity,
                "--namespace",
                identity.namespace,
                "rollout",
                "status",
                resource,
                f"--timeout={timeout_seconds}s",
            ),
            description=f"{resource} readiness",
            timeout=timeout_seconds + 30,
        )
    _run(
        executor,
        kubectl(
            identity,
            "--namespace",
            identity.namespace,
            "exec",
            "deployment/module4d-api",
            "--",
            "python3",
            "/workspace/scripts/k8s/api_readiness.py",
        ),
        description="read-only API MongoDB/index readiness",
        timeout=30,
    )
    try:
        policy_receipt = collect_network_policy_inventory(executor, identity, execute=True)
        receipt = collect_runtime_ownership_receipt(executor, identity, lock, execute=True)
        validate_collected_receipt(receipt, lock)
    except Exception as exc:
        if isinstance(exc, DeploymentError):
            raise
        raise DeploymentError("post-deployment ownership, storage, policy, or image verification failed") from exc
    return {
        "status": "serving_ready",
        "run_id": identity.run_id,
        "profile": identity.profile,
        "namespace": identity.namespace,
        "loaded_application_images": loaded_images,
        "network_policy_inventory": policy_receipt,
        "ownership_receipt": receipt,
        "runtime_acceptance": "pending; B4 scenario checks have not run",
    }
