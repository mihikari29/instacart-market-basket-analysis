"""Staged validation and evidence-backed finalization for the K8s version lock."""

from __future__ import annotations

from copy import deepcopy
import hashlib
import os
from pathlib import Path
import re
import tempfile
from typing import Any, Mapping

import yaml

from .ownership import (
    HEX_SHA256_PATTERN,
    IMAGE_DIGEST_PATTERN,
    IMAGE_NAME_TAG_PATTERN,
    OwnershipError,
    REQUIRED_IMAGE_ROLES,
    validate_version_lock,
)


REQUIRED_BUILD_PROVENANCE = frozenset({"api_application_image", "ui_application_image"})
FULL_GIT_SHA = re.compile(r"[0-9a-f]{40}\Z")


class LockLifecycleError(ValueError):
    """Static lock configuration or runtime discovery evidence is incomplete."""


def load_version_lock(path: Path | str) -> dict[str, Any]:
    """Load a YAML lock as a mapping without echoing parser details or file contents."""
    try:
        value = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, yaml.YAMLError) as exc:
        raise LockLifecycleError("version lock could not be read as YAML") from exc
    if not isinstance(value, dict):
        raise LockLifecycleError("version lock must contain a YAML mapping")
    return value


def validate_static_lock(lock: Mapping[str, Any]) -> None:
    """Validate committed choices while permitting only explicitly unresolved runtime fields."""
    if not isinstance(lock, Mapping) or type(lock.get("schema_version")) is not int or lock["schema_version"] != 1:
        raise LockLifecycleError("version lock schema_version must be 1")
    if lock.get("status") not in {"incomplete", "complete"}:
        raise LockLifecycleError("version lock status must be incomplete or complete")
    unresolved = lock.get("unresolved")
    if not isinstance(unresolved, list) or any(not isinstance(item, str) or not item.strip() for item in unresolved):
        raise LockLifecycleError("version lock unresolved must be a list of explicit runtime requirements")
    cluster = lock.get("cluster")
    if not isinstance(cluster, Mapping):
        raise LockLifecycleError("version lock is missing cluster choices")
    for field in ("kubernetes_version", "minikube_version", "cni_version"):
        value = cluster.get(field)
        if not isinstance(value, str) or not re.fullmatch(r"v\d+\.\d+\.\d+", value):
            raise LockLifecycleError(f"cluster.{field} must be an exact release version")
    if cluster.get("driver") != "docker" or cluster.get("container_runtime") != "containerd" or cluster.get("cni") != "calico":
        raise LockLifecycleError("cluster driver, runtime, and CNI choices do not match the deployment contract")
    images = lock.get("images")
    if not isinstance(images, Mapping) or not REQUIRED_IMAGE_ROLES.issubset(images):
        raise LockLifecycleError("version lock is missing required serving image roles")
    for role, image in images.items():
        if not isinstance(role, str) or not isinstance(image, Mapping):
            raise LockLifecycleError("image lock entries must be named mappings")
        reference = image.get("reference")
        if not isinstance(reference, str):
            raise LockLifecycleError(f"images.{role}.reference must be a source tag or a digest-pinned reference")
        parts = reference.split("@")
        if len(parts) > 2:
            raise LockLifecycleError(f"images.{role}.reference is malformed")
        image_name = parts[0]
        reference_digest = parts[1] if len(parts) == 2 else None
        if not IMAGE_NAME_TAG_PATTERN.fullmatch(image_name):
            raise LockLifecycleError(f"images.{role}.reference is malformed")
        if reference_digest is not None and not IMAGE_DIGEST_PATTERN.fullmatch(reference_digest):
            raise LockLifecycleError(f"images.{role}.reference digest is malformed")
        if image_name.endswith(":latest"):
            raise LockLifecycleError(f"images.{role}.reference must not use latest")
        digest = image.get("immutable_digest")
        if digest is not None:
            if not isinstance(digest, str):
                raise LockLifecycleError(f"images.{role}.immutable_digest is malformed")
            if IMAGE_DIGEST_PATTERN.fullmatch(digest):
                if reference_digest is not None and reference_digest != digest:
                    raise LockLifecycleError(f"images.{role} reference and immutable digest conflict")
            elif lock["status"] != "incomplete" or not any(
                marker in digest.lower() for marker in ("unresolved", "pending")
            ):
                raise LockLifecycleError(f"images.{role}.immutable_digest is malformed")
        elif reference_digest is None and lock["status"] == "complete":
            raise LockLifecycleError(f"images.{role} must have an immutable digest")
        source = image.get("source")
        if not isinstance(source, str) or not source.strip():
            raise LockLifecycleError(f"images.{role}.source is required")
    storage = lock.get("storage")
    if not isinstance(storage, Mapping) or storage.get("access_mode") != "ReadWriteOnce" or storage.get("capacity") != "2Gi":
        raise LockLifecycleError("static Mongo storage contract must be 2Gi ReadWriteOnce")
    if lock["status"] == "incomplete" and not unresolved:
        raise LockLifecycleError("an incomplete lock must list its unresolved runtime evidence")
    if lock["status"] == "complete":
        try:
            validate_version_lock(lock)
        except OwnershipError as exc:
            raise LockLifecycleError(str(exc)) from exc


def _merge_mapping(target: dict[str, Any], updates: Mapping[str, Any], section: str) -> None:
    if not isinstance(updates, Mapping):
        raise LockLifecycleError(f"runtime evidence section {section} must be a mapping")
    for key, value in updates.items():
        if isinstance(target.get(key), dict) and isinstance(value, Mapping):
            _merge_mapping(target[key], value, f"{section}.{key}")
        else:
            target[key] = deepcopy(value)


def validate_build_provenance(lock: Mapping[str, Any]) -> None:
    provenance = lock.get("image_provenance")
    if not isinstance(provenance, Mapping) or not REQUIRED_BUILD_PROVENANCE.issubset(provenance):
        raise LockLifecycleError("runtime lock is missing application image build provenance")
    images = lock.get("images", {})
    for role in REQUIRED_BUILD_PROVENANCE:
        record = provenance.get(role)
        if not isinstance(record, Mapping):
            raise LockLifecycleError(f"image_provenance.{role} must be a mapping")
        revision = record.get("source_revision")
        if not isinstance(revision, str) or not FULL_GIT_SHA.fullmatch(revision):
            raise LockLifecycleError(f"image_provenance.{role}.source_revision must be a full Git SHA")
        if record.get("source_dirty") is not False:
            raise LockLifecycleError(f"image_provenance.{role}.source_dirty must be false")
        for field in ("build_metadata_sha256", "source_tree_sha256"):
            value = record.get(field)
            if not isinstance(value, str) or not HEX_SHA256_PATTERN.fullmatch(value):
                raise LockLifecycleError(f"image_provenance.{role}.{field} must be a SHA-256")
        digest = record.get("image_digest")
        if not isinstance(digest, str) or not IMAGE_DIGEST_PATTERN.fullmatch(digest):
            raise LockLifecycleError(f"image_provenance.{role}.image_digest must be an OCI SHA-256")
        config_digest = record.get("config_digest")
        if not isinstance(config_digest, str) or not IMAGE_DIGEST_PATTERN.fullmatch(config_digest):
            raise LockLifecycleError(f"image_provenance.{role}.config_digest must be an OCI SHA-256")
        image = images.get(role)
        if not isinstance(image, Mapping) or image.get("immutable_digest") != digest:
            raise LockLifecycleError(f"image_provenance.{role} digest does not match the image lock")
        python_version = record.get("python_version")
        if not isinstance(python_version, str) or not re.fullmatch(r"\d+\.\d+\.\d+", python_version):
            raise LockLifecycleError(f"image_provenance.{role}.python_version must be exact")
        environment_digest = record.get("python_environment_sha256")
        if not isinstance(environment_digest, str) or not HEX_SHA256_PATTERN.fullmatch(environment_digest):
            raise LockLifecycleError(f"image_provenance.{role}.python_environment_sha256 must be a SHA-256")
        platform = record.get("platform")
        if not isinstance(platform, str) or not re.fullmatch(r"linux/(?:amd64|arm64|ppc64le|s390x)", platform):
            raise LockLifecycleError(f"image_provenance.{role}.platform must identify one supported Linux architecture")


def finalize_version_lock(
    template: Mapping[str, Any], runtime_evidence: Mapping[str, Any]
) -> dict[str, Any]:
    """Create a complete lock only from concrete, separately collected evidence."""
    validate_static_lock(template)
    if not isinstance(runtime_evidence, Mapping):
        raise LockLifecycleError("runtime evidence must be a mapping")
    required_sections = {
        "cluster", "host_capacity", "images", "python_dependencies", "toolchain", "storage", "image_provenance"
    }
    missing = sorted(required_sections.difference(runtime_evidence))
    if missing:
        raise LockLifecycleError("runtime evidence is missing sections: " + ", ".join(missing))
    observed_cluster = runtime_evidence.get("cluster")
    if not isinstance(observed_cluster, Mapping):
        raise LockLifecycleError("runtime cluster identity must be a mapping")
    for field in (
        "kubernetes_version", "driver", "container_runtime", "cni", "cni_version", "node_count"
    ):
        if field not in observed_cluster or observed_cluster[field] != template["cluster"].get(field):
            raise LockLifecycleError(f"observed cluster.{field} does not match the selected static lock")
    if not all(isinstance(observed_cluster.get(field), str) and observed_cluster[field] for field in (
        "profile", "context", "cluster_uid", "architecture"
    )):
        raise LockLifecycleError("observed dedicated cluster identity is incomplete")
    if observed_cluster["profile"] != observed_cluster["context"]:
        raise LockLifecycleError("observed profile and context do not match")
    observed_toolchain = runtime_evidence.get("toolchain")
    if not isinstance(observed_toolchain, Mapping) or observed_toolchain.get("minikube_version") != template["cluster"]["minikube_version"]:
        raise LockLifecycleError("observed Minikube version does not match the selected static lock")
    observed_images = runtime_evidence.get("images")
    if not isinstance(observed_images, Mapping):
        raise LockLifecycleError("runtime image identities must be a mapping")
    for role in ("mongo", "spark_base", "ui_base"):
        if role in observed_images and observed_images[role] != template["images"][role]:
            raise LockLifecycleError(f"observed base image {role} does not match the selected static lock")
    result = deepcopy(dict(template))
    for section in required_sections:
        if section == "image_provenance":
            result[section] = deepcopy(runtime_evidence[section])
        else:
            _merge_mapping(result.setdefault(section, {}), runtime_evidence[section], section)
    result["status"] = "complete"
    result["unresolved"] = []
    validate_build_provenance(result)
    try:
        validate_version_lock(result)
    except OwnershipError as exc:
        raise LockLifecycleError(str(exc)) from exc
    return result


def write_finalized_lock(path: Path | str, lock: Mapping[str, Any]) -> Path:
    """Atomically write only a fully validated lock; never mutate the template in place."""
    validate_static_lock(lock)
    validate_build_provenance(lock)
    destination = Path(path)
    if not destination.parent.is_dir():
        raise LockLifecycleError("lock output parent must already exist")
    if destination.exists() or destination.is_symlink():
        raise LockLifecycleError("finalized lock output must be a new file outside the committed template")
    text = yaml.safe_dump(dict(lock), sort_keys=False, allow_unicode=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", dir=destination.parent, prefix=f".{destination.name}.", delete=False
        ) as handle:
            temporary = Path(handle.name)
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(destination)
    except OSError as exc:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
        raise LockLifecycleError("finalized lock could not be published") from exc
    return destination


def sha256_file(path: Path | str) -> str:
    digest = hashlib.sha256()
    try:
        with Path(path).open("rb") as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(block)
    except OSError as exc:
        raise LockLifecycleError("build metadata file could not be hashed") from exc
    return digest.hexdigest()
