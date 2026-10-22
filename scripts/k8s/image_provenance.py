"""Pure build metadata and loaded-image identity contracts for a single-platform run."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import subprocess
import tempfile
import time
from typing import Any, Mapping

from .ownership import (
    IMAGE_DIGEST_PATTERN,
    OwnershipError,
    RunIdentity,
)
from .runtime import CommandExecutor, containerd_image_inventory_command


class ImageProvenanceError(ValueError):
    """Image build, source, or runtime identity evidence is incomplete."""


CONTROL_PLANE_REPOSITORIES = {
    "kube-apiserver": "registry.k8s.io/kube-apiserver",
    "kube-controller-manager": "registry.k8s.io/kube-controller-manager",
    "kube-scheduler": "registry.k8s.io/kube-scheduler",
    "kube-proxy": "registry.k8s.io/kube-proxy",
    "etcd": "registry.k8s.io/etcd",
    "pause": "registry.k8s.io/pause",
    "coredns": "registry.k8s.io/coredns/coredns",
}
CALICO_REPOSITORIES = {
    "cni": "docker.io/calico/cni",
    "node": "docker.io/calico/node",
    "kube-controllers": "docker.io/calico/kube-controllers",
}


def source_revision_evidence(repository: Path | str) -> dict[str, Any]:
    """Return committed tree identity and cleanliness without exposing file names."""
    root = Path(repository)
    try:
        revision = subprocess.run(
            ["git", "rev-parse", "--verify", "HEAD"], cwd=root, check=False,
            capture_output=True, text=True, timeout=30, shell=False,
        )
        status = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=all"], cwd=root,
            check=False, capture_output=True, text=True, timeout=60, shell=False,
        )
        tree = subprocess.run(
            ["git", "ls-tree", "-r", "-z", "--full-tree", "HEAD"], cwd=root,
            check=False, capture_output=True, timeout=60, shell=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ImageProvenanceError("Git source identity could not be inspected") from exc
    if revision.returncode or status.returncode or tree.returncode:
        raise ImageProvenanceError("Git source identity could not be inspected")
    source_revision = revision.stdout.strip()
    if not re.fullmatch(r"[0-9a-f]{40}", source_revision):
        raise ImageProvenanceError("source revision must be a full lowercase Git SHA")
    return {
        "source_revision": source_revision,
        "source_dirty": bool(status.stdout),
        "source_tree_sha256": hashlib.sha256(tree.stdout).hexdigest(),
    }


def parse_build_metadata(metadata: Mapping[str, Any] | str | bytes) -> dict[str, str]:
    """Extract the platform manifest and config digests from BuildKit metadata."""
    if isinstance(metadata, bytes):
        try:
            metadata = metadata.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ImageProvenanceError("BuildKit metadata is not UTF-8 JSON") from exc
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except json.JSONDecodeError as exc:
            raise ImageProvenanceError("BuildKit metadata is not valid JSON") from exc
    if not isinstance(metadata, Mapping):
        raise ImageProvenanceError("BuildKit metadata must be a JSON mapping")
    image_digest = metadata.get("containerimage.digest")
    config_digest = metadata.get("containerimage.config.digest")
    if not isinstance(image_digest, str) or not IMAGE_DIGEST_PATTERN.fullmatch(image_digest):
        raise ImageProvenanceError("BuildKit manifest digest is missing or malformed")
    if not isinstance(config_digest, str) or not IMAGE_DIGEST_PATTERN.fullmatch(config_digest):
        raise ImageProvenanceError("BuildKit config digest is missing or malformed")
    return {"image_digest": image_digest, "config_digest": config_digest}


def build_provenance_record(
    *,
    source: Mapping[str, Any],
    metadata_path: Path | str,
    metadata: Mapping[str, Any] | str | bytes,
    python_version: str,
    python_environment_sha256: str,
    platform: str,
) -> dict[str, Any]:
    """Bind one clean source revision, BuildKit metadata, and exact runtime version."""
    if source.get("source_dirty") is not False:
        raise ImageProvenanceError("application images must be built from a clean source tree")
    revision = source.get("source_revision")
    tree_digest = source.get("source_tree_sha256")
    if not isinstance(revision, str) or not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ImageProvenanceError("source revision must be a full lowercase Git SHA")
    if not isinstance(tree_digest, str) or not re.fullmatch(r"[0-9a-f]{64}", tree_digest):
        raise ImageProvenanceError("source tree identity must be a SHA-256")
    if not isinstance(python_version, str) or not re.fullmatch(r"\d+\.\d+\.\d+", python_version):
        raise ImageProvenanceError("exact Python runtime version is required")
    if not isinstance(python_environment_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", python_environment_sha256):
        raise ImageProvenanceError("resolved Python environment identity must be a SHA-256")
    if not isinstance(platform, str) or not re.fullmatch(r"linux/(?:amd64|arm64|ppc64le|s390x)", platform):
        raise ImageProvenanceError("single-platform Linux image architecture is required")
    try:
        metadata_hash = hashlib.sha256(Path(metadata_path).read_bytes()).hexdigest()
    except OSError as exc:
        raise ImageProvenanceError("BuildKit metadata file could not be hashed") from exc
    return {
        **parse_build_metadata(metadata),
        "source_revision": revision,
        "source_dirty": False,
        "source_tree_sha256": tree_digest,
        "build_metadata_sha256": metadata_hash,
        "python_version": python_version,
        "python_environment_sha256": python_environment_sha256,
        "platform": platform,
    }


def verify_loaded_image_inventory(
    raw_inventory: str | bytes,
    expected: Mapping[str, Mapping[str, str]],
) -> dict[str, dict[str, str]]:
    """Verify minikube's CRI inventory by manifest repo digest or config digest."""
    if isinstance(raw_inventory, bytes):
        try:
            raw_inventory = raw_inventory.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ImageProvenanceError("container image inventory is not UTF-8") from exc
    try:
        payload = json.loads(raw_inventory)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ImageProvenanceError("container image inventory is not valid JSON") from exc
    images = payload.get("images") if isinstance(payload, Mapping) else None
    if not isinstance(images, list):
        raise ImageProvenanceError("container image inventory has an unsupported shape")
    if not isinstance(expected, Mapping) or not expected:
        raise ImageProvenanceError("expected immutable image identities are required")
    verified: dict[str, dict[str, str]] = {}
    for role, identity in expected.items():
        if not isinstance(identity, Mapping):
            raise ImageProvenanceError(f"expected identity for {role} is invalid")
        reference = identity.get("reference")
        manifest = identity.get("image_digest")
        config = identity.get("config_digest")
        if (
            not isinstance(reference, str)
            or not isinstance(manifest, str)
            or not IMAGE_DIGEST_PATTERN.fullmatch(manifest)
            or not isinstance(config, str)
            or not IMAGE_DIGEST_PATTERN.fullmatch(config)
        ):
            raise ImageProvenanceError(f"expected immutable identity for {role} is incomplete")
        repo_digest = f"{reference.split('@', 1)[0]}@{manifest}"
        matches = []
        for image in images:
            if not isinstance(image, Mapping):
                continue
            repo_digests = image.get("repoDigests") or []
            image_id = image.get("id")
            repo_tags = image.get("repoTags") or []
            has_manifest = isinstance(repo_digests, list) and repo_digest in repo_digests
            has_config = isinstance(image_id, str) and image_id.removeprefix("sha256:") == config.removeprefix("sha256:")
            has_tag = isinstance(repo_tags, list) and reference.split("@", 1)[0] in repo_tags
            if has_manifest or (has_config and has_tag):
                matches.append(image)
        if len(matches) != 1:
            raise ImageProvenanceError(f"loaded image identity for {role} is missing or ambiguous")
        verified[role] = {"reference": reference, "image_digest": manifest, "config_digest": config}
    return verified


def collect_component_image_digests(
    executor: CommandExecutor,
    identity: RunIdentity,
    repositories: Mapping[str, str],
    *,
    execute: bool = False,
    timeout: int = 60,
) -> dict[str, str]:
    """Discover exact control-plane/CNI repository digests inside one profile."""
    if execute is not True:
        raise ImageProvenanceError("cluster image discovery requires explicit execute=True authorization")
    if not isinstance(repositories, Mapping) or not repositories:
        raise ImageProvenanceError("component image repositories are required")
    try:
        result = executor.run(containerd_image_inventory_command(identity), timeout=timeout)
    except Exception as exc:
        raise ImageProvenanceError("profile image inventory could not be read") from exc
    if result.returncode != 0:
        raise ImageProvenanceError("profile image inventory query failed")
    try:
        payload = json.loads(result.stdout)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ImageProvenanceError("profile image inventory is not valid JSON") from exc
    images = payload.get("images") if isinstance(payload, Mapping) else None
    if not isinstance(images, list):
        raise ImageProvenanceError("profile image inventory has an unsupported shape")
    return _component_references_from_inventory(images, repositories)


def collect_cluster_component_images(
    executor: CommandExecutor,
    identity: RunIdentity,
    *,
    execute: bool = False,
    timeout: int = 60,
) -> dict[str, Any]:
    """Collect every locked Kubernetes/Calico node image and the installed CNI version."""
    if execute is not True:
        raise ImageProvenanceError("cluster image discovery requires explicit execute=True authorization")
    deadline = time.time() + max(timeout, 120)
    last_error: ImageProvenanceError | None = None
    while True:
        try:
            result = executor.run(containerd_image_inventory_command(identity), timeout=30)
            if result.returncode != 0:
                raise ImageProvenanceError("profile image inventory query failed")
            try:
                payload = json.loads(result.stdout)
            except (TypeError, json.JSONDecodeError) as exc:
                raise ImageProvenanceError("profile image inventory is not valid JSON") from exc
            images = payload.get("images") if isinstance(payload, Mapping) else None
            if not isinstance(images, list):
                raise ImageProvenanceError("profile image inventory has an unsupported shape")
            if set(CONTROL_PLANE_REPOSITORIES) != {
                "kube-apiserver", "kube-controller-manager", "kube-scheduler", "kube-proxy", "etcd", "pause", "coredns"
            } or set(CALICO_REPOSITORIES) != {"cni", "node", "kube-controllers"}:
                raise ImageProvenanceError("cluster image role inventory is inconsistent")
            control_plane = _component_references_from_inventory(images, CONTROL_PLANE_REPOSITORIES)
            calico = _component_references_from_inventory(images, CALICO_REPOSITORIES)
            versions = set()
            for reference in calico.values():
                name, _digest = reference.rsplit("@", 1)
                tag = name.rsplit(":", 1)[-1] if ":" in name.rsplit("/", 1)[-1] else ""
                if not re.fullmatch(r"v\d+\.\d+\.\d+", tag):
                    raise ImageProvenanceError("Calico image version tags are missing or not exact")
                versions.add(tag)
            if len(versions) != 1:
                raise ImageProvenanceError("Calico component images do not share one exact release")
            return {
                "kubernetes_control_plane_images": control_plane,
                "calico_images": calico,
                "cni_version": next(iter(versions)),
            }
        except ImageProvenanceError as exc:
            last_error = exc
            if time.time() >= deadline:
                raise last_error
            time.sleep(3)
        except Exception as exc:
            if time.time() >= deadline:
                raise ImageProvenanceError("profile image inventory could not be read") from exc
            time.sleep(3)


def _matches_repo(candidate_name: str, expected_repo: str) -> bool:
    if candidate_name == expected_repo:
        return True
    prefixes = ("quay.io/calico/", "docker.io/calico/")
    for p1 in prefixes:
        for p2 in prefixes:
            if candidate_name.startswith(p1) and expected_repo.startswith(p2):
                if candidate_name[len(p1):] == expected_repo[len(p2):]:
                    return True
    return False


def _component_references_from_inventory(
    images: list[Any], repositories: Mapping[str, str]
) -> dict[str, str]:
    found: dict[str, str] = {}
    for role, repository in repositories.items():
        digests = set()
        tags = set()
        matched_repo_name = repository
        for image in images:
            if not isinstance(image, Mapping):
                continue
            repo_digests = image.get("repoDigests")
            if not isinstance(repo_digests, list):
                continue
            repo_tags = image.get("repoTags")
            for candidate in repo_digests:
                if not isinstance(candidate, str) or "@" not in candidate:
                    continue
                repo_name, digest = candidate.rsplit("@", 1)
                if not _matches_repo(repo_name, repository) or not IMAGE_DIGEST_PATTERN.fullmatch(digest):
                    continue
                digests.add(digest)
                matched_repo_name = repo_name
                if isinstance(repo_tags, list):
                    for tag in repo_tags:
                        if isinstance(tag, str) and _matches_repo(tag.rsplit(":", 1)[0], repository):
                            tags.add(tag.rsplit(":", 1)[1])
        if len(digests) != 1:
            raise ImageProvenanceError(f"profile component image digest is missing or ambiguous for {role}")
        exact_tags = {tag for tag in tags if re.fullmatch(r"v\d+\.\d+\.\d+", tag)}
        if len(exact_tags) > 1:
            raise ImageProvenanceError(f"profile component image tags are ambiguous for {role}")
        selected_tag = next(iter(exact_tags)) if exact_tags else (next(iter(tags)) if len(tags) == 1 else None)
        image_name = f"{matched_repo_name}:{selected_tag}" if selected_tag else matched_repo_name
        found[role] = f"{image_name}@{next(iter(digests))}"
    return found


def verify_running_image_id(
    *, runtime_image_id: str, declared_reference: str, image_digest: str, config_digest: str
) -> None:
    """Bind a Pod's reported image ID to the locked manifest or BuildKit config digest."""
    if not isinstance(runtime_image_id, str) or not runtime_image_id:
        raise OwnershipError("running container image ID is missing")
    if not isinstance(declared_reference, str) or "@" not in declared_reference:
        raise OwnershipError("running workload must declare an immutable image reference")
    if declared_reference.rsplit("@", 1)[-1] != image_digest:
        raise OwnershipError("running workload digest does not match the lock")
    if f"@{image_digest}" not in runtime_image_id and not runtime_image_id.rstrip("/").endswith(config_digest):
        raise OwnershipError("running container image ID matches neither locked manifest nor config digest")


def collect_image_environment(
    executor: Any,
    reference: str,
    *,
    execute: bool = False,
    expected_config_digest: str,
    expected_source_revision: str,
    timeout: int = 60,
) -> dict[str, str]:
    """Read the build-emitted package inventory from a stopped local container.

    The helper never starts the image. It requires the exact local config digest,
    creates one stopped container, copies a read-only inventory file, and removes
    only that container by its returned ID.
    """
    if execute is not True:
        raise ImageProvenanceError("local image inspection requires explicit execute=True authorization")
    if not isinstance(reference, str) or not reference or "\x00" in reference:
        raise ImageProvenanceError("a local image reference is required")
    if not isinstance(expected_config_digest, str) or not IMAGE_DIGEST_PATTERN.fullmatch(expected_config_digest):
        raise ImageProvenanceError("expected image config digest is malformed")
    if not isinstance(expected_source_revision, str) or not re.fullmatch(r"[0-9a-f]{40}", expected_source_revision):
        raise ImageProvenanceError("expected source revision is malformed")
    try:
        inspected = executor.run(
            ["docker", "image", "inspect", "--format", "{{.Id}}", reference], timeout=timeout
        )
    except Exception as exc:
        raise ImageProvenanceError("local image identity inspection failed") from exc
    if inspected.returncode != 0 or inspected.stdout.strip() != expected_config_digest:
        raise ImageProvenanceError("local image config digest does not match build provenance")
    try:
        revision_result = executor.run(
            [
                "docker", "image", "inspect", "--format",
                '{{ index .Config.Labels "org.opencontainers.image.revision" }}', reference,
            ],
            timeout=timeout,
        )
    except Exception as exc:
        raise ImageProvenanceError("local image source revision could not be verified") from exc
    if revision_result.returncode != 0 or revision_result.stdout.strip() != expected_source_revision:
        raise ImageProvenanceError("local image source revision does not match build provenance")
    try:
        created = executor.run(["docker", "create", "--pull=never", reference], timeout=timeout)
    except Exception as exc:
        raise ImageProvenanceError("stopped image inspection container could not be created") from exc
    container_id = created.stdout.strip()
    if created.returncode != 0 or not re.fullmatch(r"[0-9a-f]{64}", container_id):
        raise ImageProvenanceError("stopped image inspection container identity is invalid")
    result: dict[str, str] | None = None
    primary_error: ImageProvenanceError | None = None
    try:
        with tempfile.TemporaryDirectory(prefix="it4931-image-env-") as directory:
            destination = Path(directory) / "python-environment.txt"
            copied = executor.run(
                ["docker", "cp", f"{container_id}:/opt/it4931/python-environment.txt", str(destination)],
                timeout=timeout,
            )
            if copied.returncode != 0:
                raise ImageProvenanceError("image Python environment inventory could not be copied")
            try:
                content = destination.read_bytes()
                text = content.decode("utf-8")
            except (OSError, UnicodeDecodeError) as exc:
                raise ImageProvenanceError("image Python environment inventory is invalid") from exc
            lines = text.splitlines()
            if not lines or not re.fullmatch(r"python_version=\d+\.\d+\.\d+", lines[0]):
                raise ImageProvenanceError("image Python runtime version is missing")
            package_lines = lines[1:]
            package_names = [line.split("==", 1)[0] for line in package_lines]
            if (package_lines != sorted(package_lines) and package_names != sorted(package_names)) or any(
                not re.fullmatch(r"[a-z0-9][a-z0-9.+-]*==[A-Za-z0-9_.+-]+", line)
                for line in package_lines
            ):
                raise ImageProvenanceError("image Python package inventory is not canonical")
            if len({line.split("==", 1)[0] for line in package_lines}) != len(package_lines):
                raise ImageProvenanceError("image Python package inventory contains duplicates")
            result = {
                "python_version": lines[0].split("=", 1)[1],
                "python_environment_sha256": hashlib.sha256(content).hexdigest(),
            }
    except ImageProvenanceError as exc:
        primary_error = exc
    finally:
        try:
            removed = executor.run(["docker", "rm", "--force", container_id], timeout=timeout)
        except Exception as exc:
            raise ImageProvenanceError("stopped image inspection container cleanup failed") from exc
        if removed.returncode != 0:
            raise ImageProvenanceError("stopped image inspection container cleanup failed")
    if primary_error is not None:
        raise primary_error
    if result is None:
        raise ImageProvenanceError("image Python environment inventory is incomplete")
    return result
