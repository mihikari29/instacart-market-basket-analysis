"""Generate a private, per-run Kustomize overlay without contacting a cluster."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess

import yaml

from scripts.k8s.common import demo_database
from scripts.k8s.lock_lifecycle import load_version_lock, validate_static_lock
from scripts.k8s.ownership import (
    OWNER_LABEL,
    OWNER_VALUE,
    RUN_LABEL,
    OwnershipError,
    RunIdentity,
    validate_version_lock,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
BASE_DIRECTORY = REPOSITORY_ROOT / "deploy" / "k8s" / "base"
MANAGED_MARKER = ".it4931-k8s-render-root"


def _absolute_path_without_symlinks(output: Path) -> Path:
    """Normalize a path only after rejecting every existing symlink component."""
    candidate = Path(output).expanduser()
    if not candidate.is_absolute():
        candidate = Path.cwd() / candidate

    current = Path(candidate.anchor)
    for component in candidate.parts[1:]:
        if component == ".":
            continue
        if component == "..":
            current = current.parent
            continue
        current /= component
        try:
            mode = current.lstat().st_mode
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(mode):
            raise ValueError("render output path cannot pass through a symlink")
    return Path(os.path.abspath(candidate))


def _protected_roots() -> set[Path]:
    """Find this repository, its Git metadata, and every registered worktree."""
    try:
        git_directory = subprocess.check_output(
            ["git", "rev-parse", "--git-common-dir"],
            cwd=REPOSITORY_ROOT,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        worktrees = subprocess.check_output(
            ["git", "worktree", "list", "--porcelain"],
            cwd=REPOSITORY_ROOT,
            text=True,
            stderr=subprocess.DEVNULL,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise ValueError("cannot verify protected Git worktrees") from exc

    roots = {REPOSITORY_ROOT.resolve(), (REPOSITORY_ROOT / git_directory).resolve()}
    for line in worktrees.splitlines():
        if line.startswith("worktree "):
            roots.add(Path(line.removeprefix("worktree ")).resolve())
    return roots


def _validate_output_path(destination: Path) -> None:
    if destination == Path(destination.anchor):
        raise ValueError("render output cannot be a filesystem root")
    if destination.parent == Path(destination.anchor):
        raise ValueError("render output cannot be a direct child of a filesystem root")
    if not destination.parent.is_dir():
        raise FileNotFoundError("render output parent must be an existing directory")
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(f"render output already exists: {destination}")

    protected = _protected_roots()
    for root in protected:
        if destination == root or destination in root.parents or root in destination.parents:
            raise ValueError("render output cannot overlap a protected repository or worktree")
    if ".git" in destination.parts:
        raise ValueError("render output cannot overlap Git metadata")

    ancestor = destination.parent
    while True:
        if (ancestor / MANAGED_MARKER).exists():
            raise ValueError("render output cannot be nested under a managed render")
        if ancestor == ancestor.parent:
            break
        ancestor = ancestor.parent


def validate_render_output_path(output: Path) -> Path:
    """Validate and normalize a future render root without creating it."""
    destination = _absolute_path_without_symlinks(Path(output))
    _validate_output_path(destination)
    return destination


def _locked_image_reference(lock: dict, role: str) -> str:
    image = lock.get("images", {}).get(role)
    if not isinstance(image, dict):
        raise ValueError(f"complete version lock is missing images.{role}")
    reference = image.get("reference")
    digest = image.get("immutable_digest")
    if isinstance(reference, str) and "@" in reference:
        reference_name, reference_digest = reference.rsplit("@", 1)
        if digest is not None and digest != reference_digest:
            raise ValueError(f"version lock image identity conflicts for {role}")
        digest = reference_digest
    else:
        reference_name = reference
    if not isinstance(reference_name, str) or not isinstance(digest, str) or not digest.startswith("sha256:"):
        raise ValueError(f"complete version lock has no immutable digest for {role}")
    return f"{reference_name}@{digest}"


def _pin_serving_images(base_directory: Path, lock: dict) -> None:
    targets = {
        ("StatefulSet", "module4d-mongo"): _locked_image_reference(lock, "mongo"),
        ("Deployment", "module4d-api"): _locked_image_reference(lock, "api_application_image"),
        ("Deployment", "module4d-ui"): _locked_image_reference(lock, "ui_application_image"),
    }
    for path in base_directory.glob("*.yaml"):
        documents = list(yaml.safe_load_all(path.read_text(encoding="utf-8")))
        changed = False
        for document in documents:
            if not isinstance(document, dict):
                continue
            key = (document.get("kind"), document.get("metadata", {}).get("name"))
            if key not in targets:
                continue
            pod_spec = document.get("spec", {}).get("template", {}).get("spec", {})
            for container_group in ("initContainers", "containers"):
                containers = pod_spec.get(container_group, [])
                if not isinstance(containers, list) or (container_group == "containers" and not containers):
                    raise ValueError(f"serving workload {key[1]} has no {container_group}")
                for container in containers:
                    container["image"] = targets[key]
            changed = True
        if changed:
            path.write_text(yaml.safe_dump_all(documents, sort_keys=False), encoding="utf-8")


def render_overlay(
    run_id: str,
    output: Path,
    *,
    version_lock: dict | None = None,
    trend_source_namespace: str | None = None,
) -> Path:
    """Copy immutable base files and write unique namespace/config overlays.

    The output root is claimed atomically with private permissions. Repository,
    worktree, symlink, and prior managed-output paths are rejected before any
    files are copied. Secrets are never generated by this function.
    """
    identity = RunIdentity(run_id)
    if version_lock is not None:
        validate_static_lock(version_lock)
        try:
            validate_version_lock(version_lock)
        except OwnershipError as exc:
            raise ValueError("deployment rendering requires a complete version lock") from exc
        cluster = version_lock.get("cluster", {})
        if cluster.get("profile") != identity.profile or cluster.get("context") != identity.profile:
            raise ValueError("complete version lock profile does not match the requested run ID")
    destination = validate_render_output_path(Path(output))
    if not BASE_DIRECTORY.is_dir():
        raise FileNotFoundError("Kubernetes base manifests are missing")

    destination.mkdir(mode=0o700, exist_ok=False)
    (destination / MANAGED_MARKER).touch(mode=0o600, exist_ok=False)
    shutil.copytree(BASE_DIRECTORY, destination / "base")
    if version_lock is not None:
        _pin_serving_images(destination / "base", version_lock)
    base_resources = sorted(path.name for path in (destination / "base").glob("*.yaml"))
    base_kustomization = {
        "apiVersion": "kustomize.config.k8s.io/v1beta1",
        "kind": "Kustomization",
        "resources": base_resources,
    }
    (destination / "base" / "kustomization.yaml").write_text(
        json.dumps(base_kustomization, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )
    run_directory = destination / "run"
    run_directory.mkdir()

    labels = {
        OWNER_LABEL: OWNER_VALUE,
        RUN_LABEL: identity.run_id,
        "app.kubernetes.io/managed-by": "it4931-k8s-demo",
    }
    namespace = {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {"name": identity.namespace, "labels": labels},
    }
    overlay = {
        "apiVersion": "kustomize.config.k8s.io/v1beta1",
        "kind": "Kustomization",
        "namespace": identity.namespace,
        "resources": ["../base", "namespace.json"],
        "labels": [
            {
                "pairs": labels,
                "includeSelectors": False,
                "includeTemplates": True,
            }
        ],
        "patches": [
            {
                "path": "api-config-patch.json",
                "target": {"version": "v1", "kind": "ConfigMap", "name": "module4d-api-config"},
            },
            {
                "path": "ui-config-patch.json",
                "target": {"version": "v1", "kind": "ConfigMap", "name": "module4d-ui-config"},
            },
            {
                "path": "mongo-pvc-patch.json",
                "target": {"group": "apps", "version": "v1", "kind": "StatefulSet", "name": "module4d-mongo"},
            },
        ],
    }
    selected_trend_namespace = trend_source_namespace or f"module4d-demo-{identity.run_id}"
    if not selected_trend_namespace or selected_trend_namespace != selected_trend_namespace.strip():
        raise ValueError("trend source namespace must be a non-empty exact value")
    trend_namespaces = [selected_trend_namespace]
    if trend_source_namespace is not None:
        trend_namespaces.append(f"{selected_trend_namespace}-b4-{identity.run_id}")
    configured_trend_namespaces = ",".join(trend_namespaces)
    api_config_patch = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "module4d-api-config"},
        "data": {
            "MODULE4D_MONGO_DATABASE": demo_database(identity.run_id),
            "MODULE4D_TREND_SOURCE_NAMESPACES": configured_trend_namespaces,
        },
    }
    ui_config_patch = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "module4d-ui-config"},
        "data": {"MODULE4D_TREND_SOURCE_NAMESPACES": configured_trend_namespaces},
    }
    mongo_pvc_patch = {
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {"name": "module4d-mongo"},
        "spec": {
            "volumeClaimTemplates": [
                {
                    "metadata": {
                        "name": "data",
                        "labels": {
                            **labels,
                            "app.kubernetes.io/name": "module4d-mongo",
                            "app.kubernetes.io/component": "database",
                        },
                    },
                    "spec": {
                        "accessModes": ["ReadWriteOnce"],
                        "resources": {"requests": {"storage": "2Gi"}},
                    },
                }
            ]
        },
    }
    for filename, value in (
        ("kustomization.yaml", overlay),
        ("namespace.json", namespace),
        ("api-config-patch.json", api_config_patch),
        ("ui-config-patch.json", ui_config_patch),
        ("mongo-pvc-patch.json", mongo_pvc_patch),
    ):
        (run_directory / filename).write_text(
            json.dumps(value, sort_keys=True, indent=2) + "\n", encoding="utf-8"
        )
    return run_directory


def validate_locked_overlay(
    run_id: str,
    run_directory: Path,
    version_lock: dict,
    *,
    trend_source_namespace: str | None = None,
) -> None:
    """Reject a modified or unpinned render before it can be sent to Kubernetes."""
    identity = RunIdentity(run_id)
    validate_static_lock(version_lock)
    try:
        validate_version_lock(version_lock)
    except OwnershipError as exc:
        raise ValueError("deployment requires a complete version lock") from exc
    if version_lock.get("cluster", {}).get("profile") != identity.profile:
        raise ValueError("version lock profile does not match the rendered run")
    run_path = Path(run_directory)
    if run_path.is_symlink() or not run_path.is_dir() or run_path.name != "run":
        raise ValueError("locked render directory is invalid")
    output_root = run_path.parent
    if not (output_root / MANAGED_MARKER).is_file() or (output_root / MANAGED_MARKER).is_symlink():
        raise ValueError("locked render root marker is missing")
    try:
        overlay = json.loads((run_path / "kustomization.yaml").read_text(encoding="utf-8"))
        namespace = json.loads((run_path / "namespace.json").read_text(encoding="utf-8"))
        api_config_patch = json.loads((run_path / "api-config-patch.json").read_text(encoding="utf-8"))
        ui_config_patch = json.loads((run_path / "ui-config-patch.json").read_text(encoding="utf-8"))
        mongo_pvc_patch = json.loads((run_path / "mongo-pvc-patch.json").read_text(encoding="utf-8"))
        base_kustomization = json.loads((output_root / "base" / "kustomization.yaml").read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("locked render metadata could not be read") from exc
    expected_namespace = trend_source_namespace or f"module4d-demo-{identity.run_id}"
    configured_namespaces = [expected_namespace]
    if trend_source_namespace is not None:
        configured_namespaces.append(f"{expected_namespace}-b4-{identity.run_id}")
    configured_value = ",".join(configured_namespaces)
    if (
        overlay.get("namespace") != identity.namespace
        or namespace.get("kind") != "Namespace"
        or namespace.get("metadata", {}).get("name") != identity.namespace
        or namespace.get("metadata", {}).get("labels") != identity.labels
    ):
        raise ValueError("locked render namespace identity does not match the run")
    if (
        api_config_patch.get("data", {}).get("MODULE4D_MONGO_DATABASE") != demo_database(identity.run_id)
        or api_config_patch.get("data", {}).get("MODULE4D_TREND_SOURCE_NAMESPACES") != configured_value
        or ui_config_patch.get("data", {}).get("MODULE4D_TREND_SOURCE_NAMESPACES") != configured_value
    ):
        raise ValueError("locked render serving configuration differs from the run's B3 source contract")
    claim_templates = mongo_pvc_patch.get("spec", {}).get("volumeClaimTemplates", [])
    if (
        len(claim_templates) != 1
        or claim_templates[0].get("metadata", {}).get("name") != "data"
        or claim_templates[0].get("metadata", {}).get("labels") != identity.labels
    ):
        raise ValueError("locked render Mongo PVC ownership labels do not match the run")
    expected_resources = sorted(path.name for path in (output_root / "base").glob("*.yaml") if path.name != "kustomization.yaml")
    if base_kustomization.get("resources") != expected_resources or "../base" not in overlay.get("resources", []):
        raise ValueError("locked render resource inventory was changed")

    expected_images = {
        ("StatefulSet", "module4d-mongo"): _locked_image_reference(version_lock, "mongo"),
        ("Deployment", "module4d-api"): _locked_image_reference(version_lock, "api_application_image"),
        ("Deployment", "module4d-ui"): _locked_image_reference(version_lock, "ui_application_image"),
    }
    observed = set()
    for path in (output_root / "base").glob("*.yaml"):
        try:
            documents = list(yaml.safe_load_all(path.read_text(encoding="utf-8")))
        except (OSError, UnicodeError, yaml.YAMLError) as exc:
            raise ValueError("locked render base YAML could not be read") from exc
        for document in documents:
            if not isinstance(document, dict):
                continue
            key = (document.get("kind"), document.get("metadata", {}).get("name"))
            if key not in expected_images:
                continue
            pod_spec = document.get("spec", {}).get("template", {}).get("spec", {})
            containers = pod_spec.get("containers", []) + pod_spec.get("initContainers", [])
            if not containers or any(container.get("image") != expected_images[key] for container in containers):
                raise ValueError(f"locked image reference changed for {key[1]}")
            observed.add(key)
    if observed != set(expected_images):
        raise ValueError("locked render is missing a required serving workload")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True, help="12 lowercase hexadecimal characters")
    parser.add_argument("--output", required=True, type=Path, help="new private output directory")
    parser.add_argument(
        "--version-lock",
        type=Path,
        help="complete runtime lock; required by deployment execution, optional for offline base rendering",
    )
    args = parser.parse_args()
    try:
        version_lock = load_version_lock(args.version_lock) if args.version_lock else None
        print(render_overlay(args.run_id, args.output, version_lock=version_lock))
    except (FileExistsError, FileNotFoundError, ValueError) as exc:
        parser.error(str(exc))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
