"""Explicit image build/load operations used only by an opted-in run coordinator."""

from __future__ import annotations

from pathlib import Path
import re
from typing import Any, Mapping

from .image_provenance import verify_loaded_image_inventory
from .lock_lifecycle import validate_static_lock
from .ownership import OwnershipError, RunIdentity, validate_version_lock
from .runtime import CommandExecutor, RuntimePreflightError, _checked, containerd_image_inventory_command


class ImageOperationError(RuntimeError):
    """An explicitly requested local image operation failed safely."""


APPLICATION_IMAGE_ROLES = ("api_application_image", "ui_application_image")


def image_build_plan(
    source_root: Path | str,
    *,
    identity: RunIdentity,
    source_revision: str,
    metadata_directory: Path | str,
    platform: str,
) -> tuple[list[str], list[str]]:
    """Prepare shell-free buildx commands and require a clean full Git revision."""
    if not isinstance(source_revision, str) or not re.fullmatch(r"[0-9a-f]{40}", source_revision):
        raise ImageOperationError("a full lowercase source Git SHA is required")
    if not isinstance(identity, RunIdentity):
        raise ImageOperationError("a validated run identity is required")
    if not isinstance(platform, str) or not re.fullmatch(r"linux/(?:amd64|arm64|ppc64le|s390x)", platform):
        raise ImageOperationError("an exact single-platform Linux architecture is required")
    root = Path(source_root).resolve()
    metadata_root = Path(metadata_directory).resolve()
    if not root.is_dir() or not metadata_root.is_dir():
        raise ImageOperationError("source and metadata directories must already exist")
    if root == metadata_root or root in metadata_root.parents:
        raise ImageOperationError("build metadata output must be outside the image build context")
    from .image_provenance import source_revision_evidence

    evidence = source_revision_evidence(root)
    if evidence["source_dirty"] or evidence["source_revision"] != source_revision:
        raise ImageOperationError("image build requires the exact clean source revision")
    api = [
        "docker", "buildx", "build", "--file", "Dockerfile", "--target", "k8s-runner",
        "--build-arg", f"MODULE4_COMMIT={source_revision}",
        "--platform", platform,
        "--tag", f"instacart-module4d-k8s-api:{identity.run_id}",
        "--metadata-file", str(metadata_root / "api-build-metadata.json"), "--load", str(root),
    ]
    ui = [
        "docker", "buildx", "build", "--file", "Dockerfile", "--target", "ui",
        "--build-arg", f"MODULE4_COMMIT={source_revision}",
        "--platform", platform,
        "--tag", f"instacart-module4d-k8s-ui:{identity.run_id}",
        "--metadata-file", str(metadata_root / "ui-build-metadata.json"), "--load", str(root),
    ]
    return api, ui


def execute_image_build_plan(
    executor: CommandExecutor,
    commands: tuple[list[str], list[str]],
    *,
    execute: bool = False,
    timeout: int = 1800,
) -> dict[str, str]:
    """Execute exactly the two commands returned by image_build_plan."""
    if execute is not True:
        raise ImageOperationError("image builds require explicit execute=True authorization")
    if (
        not isinstance(commands, tuple)
        or len(commands) != 2
        or any(not isinstance(command, list) or command[:3] != ["docker", "buildx", "build"] for command in commands)
    ):
        raise ImageOperationError("only a validated API/UI image build plan can be executed")
    metadata_paths = {}
    for role, command in zip(APPLICATION_IMAGE_ROLES, commands):
        index = command.index("--metadata-file") if "--metadata-file" in command else -1
        if index < 0 or index + 1 >= len(command):
            raise ImageOperationError("image build plan is missing its provenance metadata destination")
        metadata_paths[role] = command[index + 1]
        try:
            result = executor.run(command, timeout=timeout)
        except Exception as exc:
            raise ImageOperationError(f"{role} image build failed ({type(exc).__name__})") from exc
        if result.returncode != 0:
            raise ImageOperationError(f"{role} image build failed with status {result.returncode}")
    return metadata_paths


def load_and_verify_application_images(
    executor: CommandExecutor,
    identity: RunIdentity,
    lock: Mapping[str, Any],
    *,
    execute: bool = False,
    timeout: int = 600,
) -> dict[str, dict[str, str]]:
    """Load locked API/UI images into one explicit profile and verify the CRI IDs."""
    if execute is not True:
        raise ImageOperationError("image loading requires explicit execute=True authorization")
    validate_static_lock(lock)
    try:
        validate_version_lock(lock)
    except OwnershipError as exc:
        raise ImageOperationError("image loading requires a complete runtime version lock") from exc
    if lock["cluster"].get("profile") != identity.profile or lock["cluster"].get("context") != identity.profile:
        raise ImageOperationError("version lock does not identify the requested Minikube profile")
    expected: dict[str, dict[str, str]] = {}
    for role in APPLICATION_IMAGE_ROLES:
        image = lock["images"].get(role)
        provenance = lock["image_provenance"].get(role)
        if not isinstance(image, Mapping) or not isinstance(provenance, Mapping):
            raise ImageOperationError(f"version lock is missing {role} identity")
        reference = image.get("reference")
        if not isinstance(reference, str):
            raise ImageOperationError(f"version lock has no image reference for {role}")
        tag = reference.split("@", 1)[0]
        for field in ("image_digest", "config_digest"):
            if not isinstance(provenance.get(field), str):
                raise ImageOperationError(f"version lock has no {field} for {role}")
        expected[role] = {
            "reference": f"{tag}@{provenance['image_digest']}",
            "image_digest": provenance["image_digest"],
            "config_digest": provenance["config_digest"],
        }
        command = ["minikube", "image", "load", "-p", identity.profile, tag]
        try:
            result = executor.run(command, timeout=timeout)
        except Exception as exc:
            raise ImageOperationError(f"{role} image load failed ({type(exc).__name__})") from exc
        if result.returncode != 0:
            raise ImageOperationError(f"{role} image load failed with status {result.returncode}")
    try:
        inventory = _checked(executor, containerd_image_inventory_command(identity), timeout=timeout).stdout
        return verify_loaded_image_inventory(inventory, expected)
    except (RuntimePreflightError, ValueError) as exc:
        raise ImageOperationError("loaded images did not match the locked immutable identities") from exc
