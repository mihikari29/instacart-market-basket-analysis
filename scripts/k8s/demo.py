"""Controlled one-shot Kubernetes demo runner with offline-first commands.

The CLI's preflight, plan, and local verify/collect commands do not contact
runtime services. ``run --execute`` either prepares a fresh, run-specific
Minikube profile and finalizes its lock from collected evidence, or consumes a
supplied complete run-specific lock. It then calls the existing B2/B3/B4
execution gates in order. It never deletes a cluster/profile; teardown is a
separate command with its own explicit execution gate.
"""

from __future__ import annotations

import argparse
from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile
from typing import Any, Callable, Mapping, Sequence

from .acceptance import ACCEPTANCE_SCENARIOS, AcceptanceError, collect_failure_evidence, run_acceptance
from .data_receipts import InputReceiptError, inspect_input_directory
from .deployment import DeploymentError, deploy_serving
from .lock_lifecycle import LockLifecycleError, load_version_lock, validate_static_lock
from .lock_lifecycle import finalize_version_lock, write_finalized_lock
from .mongo_secrets import build_mongo_secret_bundle
from .ownership import OwnershipError, RunIdentity, validate_version_lock
from .pipeline import (
    PipelineError,
    discover_pipeline_component_images,
    execute_pipeline,
    execute_pipeline_image_build,
    finalize_pipeline_lock,
    pipeline_build_provenance,
    pipeline_image_build_plan,
    plan_pipeline,
    render_pipeline_assets,
)
from .pipeline_receipts import pipeline_receipt_from_document
from .pipeline_receipts import validate_pipeline_receipt
from .receipts import receipt_json, receipt_from_document, validate_collected_receipt
from .render import render_overlay, validate_render_output_path
from .image_operations import execute_image_build_plan, image_build_plan
from .image_provenance import (
    build_provenance_record,
    collect_cluster_component_images,
    collect_image_environment,
    parse_build_metadata,
    source_revision_evidence,
)
from .runtime import (
    CommandExecutor,
    RuntimePreflightError,
    SubprocessExecutor,
    collect_cluster_identity,
    collect_storage_class_identity,
    collect_toolchain_identity,
    create_dedicated_cluster,
    kubectl,
    measure_host_capacity,
    validate_host_capacity,
)
from .teardown import (
    TeardownError,
    _read_receipt as _read_teardown_receipt,
    _validate_acceptance_checks,
    _verify_acceptance_artifacts,
    teardown_run,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_LOCK = REPOSITORY_ROOT / "deploy" / "k8s" / "versions.lock.yaml"
SESSION_RECEIPT = "demo-session.json"
PREPARATION_RECEIPT = "preparation-state.json"
_RUN_PROFILE = re.compile(r"it4931-k8s-([0-9a-f]{12})\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_FULL_INPUT_MAX_BYTES = 12 * 1024**3
_STAGE_NAMES = (
    "local_identity", "profile_creation", "application_images", "pipeline_images",
    "lock_finalization", "private_render", "namespace_preflight", "serving", "pipeline", "acceptance",
)


class DemoError(RuntimeError):
    """The demo plan or a guarded stage could not complete."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _write_private_json(path: Path, payload: Mapping[str, Any], *, allow_replace: bool = False) -> None:
    if path.is_symlink() or (path.exists() and not allow_replace):
        raise DemoError("run evidence output already exists or is a symbolic link")
    if not path.parent.is_dir() or path.parent.stat().st_mode & 0o077:
        raise DemoError("run evidence parent must be a private directory")
    temporary = path.with_name(f".{path.name}.tmp")
    if temporary.exists() or temporary.is_symlink():
        raise DemoError("temporary evidence path already exists")
    try:
        with temporary.open("x", encoding="utf-8") as stream:
            json.dump(payload, stream, sort_keys=True, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        temporary.chmod(0o600)
        if allow_replace:
            temporary.replace(path)
        else:
            temporary.rename(path)
    except OSError as exc:
        temporary.unlink(missing_ok=True)
        raise DemoError("run evidence could not be written") from exc


def _read_json(path: Path) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise DemoError("run evidence is missing or is a symbolic link")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise DemoError("run evidence is invalid JSON") from exc
    if not isinstance(value, dict):
        raise DemoError("run evidence must be a JSON object")
    return value


def _identity_from_lock(lock: Mapping[str, Any], requested_run_id: str | None = None) -> RunIdentity:
    cluster = lock.get("cluster")
    profile = cluster.get("profile") if isinstance(cluster, Mapping) else None
    match = _RUN_PROFILE.fullmatch(profile) if isinstance(profile, str) else None
    if not match:
        raise DemoError("complete version lock must name one run-scoped profile")
    identity = RunIdentity(match.group(1))
    if requested_run_id is not None and identity.run_id != requested_run_id:
        raise DemoError("requested run ID does not match the finalized version lock")
    if cluster.get("context") != identity.profile:
        raise DemoError("version lock context does not match its run-scoped profile")
    return identity


def _journal_preparation(directory: Path, stage: str, status: str, **details: Any) -> None:
    path = directory / PREPARATION_RECEIPT
    prior = _read_json(path) if path.exists() else {
        "schema_version": 1,
        "status": "preparing",
        "completed_stages": [],
    }
    prior["stage"] = stage
    prior["stage_status"] = status
    prior["updated_at_utc"] = _utc_now()
    prior.update(details)
    if status == "completed" and stage not in prior["completed_stages"]:
        prior["completed_stages"].append(stage)
    _write_private_json(path, prior, allow_replace=path.exists())


def _image_reference(image: Mapping[str, Any], role: str, digest: str) -> dict[str, Any]:
    reference = image.get("reference")
    if not isinstance(reference, str) or "@" in reference or reference.endswith(":latest"):
        raise DemoError(f"selected image reference for {role} is not an exact immutable source tag")
    return {**dict(image), "reference": f"{reference}@{digest}", "immutable_digest": digest}


def prepare_run_lock(
    executor: CommandExecutor,
    identity: RunIdentity,
    *,
    template_path: Path | str,
    host_capacity: Mapping[str, Any],
    preparation_directory: Path | str,
) -> Path:
    """Create the dedicated profile and derive a complete B3 lock from evidence.

    All Docker, image-pull, Minikube, and Kubernetes calls occur only beneath
    this helper's caller-side ``execute=True`` gate. Partial resources and
    evidence are retained on failure; no cleanup or resume is attempted.
    """
    capacity = dict(host_capacity)
    validate_host_capacity(capacity, phase="b3", require_backing_disk=True)
    template = load_version_lock(template_path)
    validate_static_lock(template)
    if template.get("status") != "incomplete":
        raise DemoError("new profile preparation requires the committed incomplete version-lock template")
    directory = Path(preparation_directory)
    if directory.exists() or directory.is_symlink() or not directory.parent.is_dir():
        raise DemoError("preparation evidence path must be new beneath an existing directory")
    directory.mkdir(mode=0o700, exist_ok=False)
    _journal_preparation(directory, "local_identity", "running", run_id=identity.run_id)
    source = source_revision_evidence(REPOSITORY_ROOT)
    if source.get("source_dirty") is not False:
        raise DemoError("image builds require a clean committed source tree")
    toolchain = collect_toolchain_identity(executor, execute=True)
    if toolchain.get("minikube_version") != template["cluster"].get("minikube_version"):
        raise DemoError("installed Minikube version does not match the selected lock template")
    _journal_preparation(directory, "local_identity", "completed", source_revision=source["source_revision"])

    _journal_preparation(directory, "profile_creation", "running")
    try:
        create_dedicated_cluster(
            executor,
            identity,
            host_capacity=capacity,
            kubernetes_version=template["cluster"]["kubernetes_version"],
            cpus=6,
            memory_mib=12 * 1024,
            disk_gib=40,
            execute=True,
        )
    except RuntimePreflightError as exc:
        raise DemoError(f"dedicated profile creation stopped safely: {exc}") from exc
    cluster = collect_cluster_identity(executor, identity, execute=True)
    cluster_components = collect_cluster_component_images(executor, identity, execute=True)
    cluster["cni_version"] = cluster_components["cni_version"]
    storage = collect_storage_class_identity(executor, identity, execute=True)
    _journal_preparation(directory, "profile_creation", "completed", cluster_uid=cluster["cluster_uid"])

    architecture = cluster["architecture"]
    metadata_directory = directory / "build-metadata"
    metadata_directory.mkdir(mode=0o700)
    _journal_preparation(directory, "application_images", "running", platform=architecture)
    try:
        app_commands = image_build_plan(
            REPOSITORY_ROOT,
            identity=identity,
            source_revision=source["source_revision"],
            metadata_directory=metadata_directory,
            platform=architecture,
        )
        app_metadata_paths = execute_image_build_plan(executor, app_commands, execute=True, timeout=1800)
        app_images: dict[str, dict[str, Any]] = {}
        app_provenance: dict[str, dict[str, Any]] = {}
        app_environments: dict[str, dict[str, str]] = {}
        for role, command in zip(("api_application_image", "ui_application_image"), app_commands):
            tag = command[command.index("--tag") + 1]
            metadata_path = Path(app_metadata_paths[role])
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            parsed = parse_build_metadata(metadata)
            environment = collect_image_environment(
                executor,
                tag,
                execute=True,
                expected_config_digest=parsed["config_digest"],
                expected_source_revision=source["source_revision"],
            )
            provenance = build_provenance_record(
                source=source,
                metadata_path=metadata_path,
                metadata=metadata,
                python_version=environment["python_version"],
                python_environment_sha256=environment["python_environment_sha256"],
                platform=architecture,
            )
            app_images[role] = _image_reference(template["images"][role], role, provenance["image_digest"])
            app_images[role]["reference"] = f"{tag}@{provenance['image_digest']}"
            app_provenance[role] = provenance
            app_environments[role] = environment
    except Exception as exc:
        if isinstance(exc, DemoError):
            raise
        raise DemoError(f"API/UI image build or provenance collection stopped ({type(exc).__name__})") from exc
    _journal_preparation(directory, "application_images", "completed")

    _journal_preparation(directory, "pipeline_images", "running")
    try:
        component_images = discover_pipeline_component_images(executor, template, execute=True)
        pipeline_command = pipeline_image_build_plan(
            REPOSITORY_ROOT,
            identity=identity,
            source_revision=source["source_revision"],
            metadata_directory=metadata_directory,
            platform=architecture,
        )
        pipeline_metadata_path = execute_pipeline_image_build(executor, pipeline_command, execute=True, timeout=3600)
        pipeline_tag = pipeline_command[pipeline_command.index("--tag") + 1]
        pipeline_image = pipeline_build_provenance(
            executor,
            source_root=REPOSITORY_ROOT,
            reference=pipeline_tag,
            metadata_path=pipeline_metadata_path,
            platform=architecture,
            execute=True,
        )
    except Exception as exc:
        if isinstance(exc, DemoError):
            raise
        raise DemoError(f"pipeline image identity collection stopped ({type(exc).__name__})") from exc
    _journal_preparation(directory, "pipeline_images", "completed")

    images: dict[str, Any] = {}
    for role in ("api_application_image", "ui_application_image"):
        images[role] = app_images[role]
    for role, record in component_images.items():
        images[role] = _image_reference(template["images"][role], role, record["immutable_digest"])
        images[role]["reference"] = f"{record['reference']}@{record['immutable_digest']}"
    images["pipeline_application_image"] = _image_reference(
        template["images"]["pipeline_application_image"],
        "pipeline_application_image",
        pipeline_image["image_digest"],
    )
    images["pipeline_application_image"]["reference"] = f"{pipeline_tag}@{pipeline_image['image_digest']}"
    provenance_digests = {
        "api": "sha256:" + app_provenance["api_application_image"]["python_environment_sha256"],
        "ui": "sha256:" + app_provenance["ui_application_image"]["python_environment_sha256"],
        "spark_parent": "sha256:" + app_provenance["api_application_image"]["python_environment_sha256"],
    }
    python_dependencies = deepcopy(template["python_dependencies"])
    python_dependencies.update(
        api_python=app_environments["api_application_image"]["python_version"],
        ui_python=app_environments["ui_application_image"]["python_version"],
        spark_parent_python=app_environments["api_application_image"]["python_version"],
        resolved_environment_digests=provenance_digests,
    )
    toolchain = {
        **toolchain,
        "kubernetes_control_plane_images": cluster_components["kubernetes_control_plane_images"],
        "calico_images": cluster_components["calico_images"],
    }
    runtime_evidence = {
        "cluster": cluster,
        "host_capacity": capacity,
        "images": images,
        "python_dependencies": python_dependencies,
        "toolchain": toolchain,
        "storage": storage,
        "image_provenance": app_provenance,
    }
    try:
        b2_lock = finalize_version_lock(template, runtime_evidence)
        b3_lock = finalize_pipeline_lock(
            b2_lock,
            component_images=component_images,
            pipeline_image=pipeline_image,
        )
        b2_path = write_finalized_lock(directory / "b2-version-lock.yaml", b2_lock)
        b3_path = write_finalized_lock(directory / "b3-version-lock.yaml", b3_lock)
    except Exception as exc:
        if isinstance(exc, DemoError):
            raise
        raise DemoError(f"runtime version lock finalization failed ({type(exc).__name__})") from exc
    _journal_preparation(directory, "lock_finalization", "completed", b2_lock=str(b2_path.name), b3_lock=str(b3_path.name))
    return b3_path


def _validate_mode_inputs(
    mode: str,
    *,
    input_directory: Path | str | None,
    capacity: Mapping[str, Any],
) -> dict[str, Any] | None:
    if mode not in {"fixture", "full"}:
        raise DemoError("mode must be fixture or full")
    if mode == "fixture":
        if input_directory is not None:
            raise DemoError("fixture mode does not accept an external data directory")
        return None
    if input_directory is None:
        raise DemoError("full mode requires --input-dir with the six source CSV files")
    try:
        validate_host_capacity(dict(capacity), phase="b3", require_backing_disk=True)
        receipt = inspect_input_directory(input_directory)
    except (RuntimePreflightError, InputReceiptError) as exc:
        raise DemoError(f"full-data preflight stopped: {exc}") from exc
    if receipt.get("total_bytes", _FULL_INPUT_MAX_BYTES + 1) > _FULL_INPUT_MAX_BYTES:
        raise DemoError("full-data inputs exceed the 12 GiB source-storage guard for the 16 GiB HDFS DataNode claim")
    return receipt


def _plan_document(
    identity: RunIdentity,
    mode: str,
    *,
    host_capacity: Mapping[str, Any],
    source_receipt_sha256: str | None = None,
) -> dict[str, Any]:
    try:
        pipeline_plan = plan_pipeline(
            identity,
            mode=mode,
            host_capacity=host_capacity,
            source_receipt_sha256=source_receipt_sha256,
        )
    except PipelineError as exc:
        raise DemoError(str(exc)) from exc
    return {
        "schema_version": 1,
        "run_id": identity.run_id,
        "profile": identity.profile,
        "context": identity.profile,
        "namespace": identity.namespace,
        "mode": mode,
        "stages": list(_STAGE_NAMES),
        "preparation": "create a fresh dedicated profile, build and inspect locked images, finalize a run-specific B3 lock",
        "pipeline_steps": [asdict(item) for item in pipeline_plan.steps],
        "execution_model": pipeline_plan.execution_model,
        "authorized_execution_effects": [
            "create one fresh Minikube profile using the Docker driver",
            "build three run-tagged application images with BuildKit",
            "pull and inspect the selected Kafka and HDFS image tags",
            "apply only the run namespace, serving resources, pipeline resources, and acceptance Job",
            "retain all run-scoped resources after completion or failure",
        ],
        "host_capacity": dict(host_capacity),
        "source_receipt_sha256": source_receipt_sha256,
        "resume_policy": "disabled; every execution requires a fresh run ID and empty output path",
        "teardown_policy": "manual and separate; no teardown stage is part of this runner",
    }


def preflight_report(
    *,
    mode: str = "fixture",
    lock_path: Path | str = DEFAULT_LOCK,
    input_directory: Path | str | None = None,
    disk_path: Path | str = "/",
    windows_backing_disk_gib: float | None = None,
) -> dict[str, Any]:
    """Perform local-only checks; this function never starts external tools."""
    capacity = measure_host_capacity(disk_path=disk_path)
    if windows_backing_disk_gib is not None:
        if isinstance(windows_backing_disk_gib, bool) or windows_backing_disk_gib <= 0:
            raise DemoError("Windows backing-disk evidence must be a positive measured GiB value")
        capacity["windows_backing_disk_gib"] = windows_backing_disk_gib
        capacity["windows_backing_disk_source"] = "operator-supplied measured value"
    lock = load_version_lock(lock_path)
    validate_static_lock(lock)
    source = _validate_mode_inputs(mode, input_directory=input_directory, capacity=capacity)
    required_tools = {name: shutil.which(name) is not None for name in ("docker", "minikube", "kubectl")}
    source_identity = source_revision_evidence(REPOSITORY_ROOT)
    complete = lock.get("status") == "complete" and not lock.get("unresolved")
    if complete:
        try:
            validate_version_lock(lock)
        except OwnershipError as exc:
            raise DemoError("version lock claims completeness but fails runtime identity validation") from exc
    reserve_ok = True
    reserve_error = None
    try:
        validate_host_capacity(dict(capacity), phase="b3", require_backing_disk=True)
    except RuntimePreflightError as exc:
        reserve_ok = False
        reserve_error = str(exc)
    return {
        "schema_version": 1,
        "mode": mode,
        "checks": {
            "static_lock": "passed",
            "runtime_lock_complete": complete,
            "lock_use": "reuse complete lock" if complete else "valid incomplete template; run will collect runtime identities",
            "b3_capacity_reserve": "passed" if reserve_ok else "blocked",
            "required_executables_present": required_tools,
            "source_input": "six-file receipt verified" if source is not None else "bounded image fixture",
            "source_tree_clean": source_identity["source_dirty"] is False,
        },
        "source_identity": source_identity,
        "host_capacity": capacity,
        "source_receipt": source,
        "runtime_execution_ready": (
            reserve_ok and all(required_tools.values()) and source_identity["source_dirty"] is False
        ),
        "runtime_execution_blocker": reserve_error or (
            "required local executables are missing" if not all(required_tools.values()) else (
                "source tree must be committed and clean before image builds" if source_identity["source_dirty"] else None
            )
        ),
        "external_commands_run": False,
    }


def _require_absent_namespace(executor: CommandExecutor, identity: RunIdentity) -> None:
    """Fail closed if this new one-shot run would adopt an existing namespace."""
    try:
        result = executor.run(kubectl(identity, "get", "namespaces", "-o", "json"), timeout=30)
        payload = json.loads(result.stdout)
    except Exception as exc:
        raise DemoError(f"run namespace preflight failed ({type(exc).__name__}); no workload was applied") from exc
    if result.returncode != 0 or not isinstance(payload, Mapping) or not isinstance(payload.get("items"), list):
        raise DemoError("run namespace preflight returned an invalid inventory; no workload was applied")
    for item in payload["items"]:
        if not isinstance(item, Mapping) or not isinstance(item.get("metadata"), Mapping):
            raise DemoError("run namespace inventory is malformed; no workload was applied")
        metadata = item["metadata"]
        if metadata.get("name") == identity.namespace:
            raise DemoError("run namespace already exists; refusing to adopt or overwrite prior resources")


def _acceptance_passed(mode: str, receipt: Mapping[str, Any]) -> bool:
    expected = "fixture_runtime_pass" if mode == "fixture" else "full_data_runtime_pass"
    return receipt.get("status") == expected and receipt.get("scope") == f"{mode}_runtime"


def run_demo(
    *,
    mode: str,
    lock_path: Path | str | None = None,
    output_directory: Path | str | None = None,
    input_directory: Path | str | None = None,
    run_id: str | None = None,
    executor: CommandExecutor | None = None,
    execute: bool = False,
    capacity: Mapping[str, Any] | None = None,
    windows_backing_disk_gib: float | None = None,
) -> dict[str, Any]:
    """Execute B2 serving, B3 pipeline, and B4 acceptance in dependency order."""
    if execute is not True:
        raise DemoError("demo execution requires explicit --execute authorization")
    active_executor = executor or SubprocessExecutor()
    host_capacity = dict(capacity) if capacity is not None else measure_host_capacity()
    if windows_backing_disk_gib is not None:
        if isinstance(windows_backing_disk_gib, bool) or windows_backing_disk_gib <= 0:
            raise DemoError("Windows backing-disk evidence must be a positive measured GiB value")
        host_capacity["windows_backing_disk_gib"] = windows_backing_disk_gib
        host_capacity["windows_backing_disk_source"] = "operator-supplied measured value"
    try:
        validate_host_capacity(host_capacity, phase="b3", require_backing_disk=True)
    except RuntimePreflightError as exc:
        raise DemoError(f"runtime capacity preflight stopped: {exc}") from exc
    source = _validate_mode_inputs(mode, input_directory=input_directory, capacity=host_capacity)
    source_sha = source.get("manifest_sha256") if source is not None else None

    lock: dict[str, Any] | None = None
    if lock_path is not None:
        lock = load_version_lock(lock_path)
        validate_static_lock(lock)
        try:
            validate_version_lock(lock)
        except OwnershipError as exc:
            raise DemoError("supplied lock is incomplete; omit --lock to prepare a fresh profile and lock") from exc
        identity = _identity_from_lock(lock, run_id)
    else:
        identity = RunIdentity(run_id) if run_id is not None else RunIdentity.generate()
    root = Path(output_directory or (Path(tempfile.gettempdir()) / f"it4931-k8s-{identity.run_id}")).expanduser()
    try:
        root = validate_render_output_path(root)
    except (FileExistsError, FileNotFoundError, ValueError) as exc:
        raise DemoError(f"unsafe run output path: {exc}") from exc

    preparation_directory: Path | None = None
    preparation_stages: list[str] = []
    if lock is None:
        preparation_directory = root.with_name(root.name + ".preparation")
        try:
            lock_path = prepare_run_lock(
                active_executor,
                identity,
                template_path=DEFAULT_LOCK,
                host_capacity=host_capacity,
                preparation_directory=preparation_directory,
            )
            lock = load_version_lock(lock_path)
            validate_static_lock(lock)
            validate_version_lock(lock)
            preparation_state = _read_json(preparation_directory / PREPARATION_RECEIPT)
            preparation_stages = list(preparation_state.get("completed_stages", []))
        except Exception as exc:
            if preparation_directory.is_dir():
                try:
                    _journal_preparation(
                        preparation_directory,
                        "failed",
                        "failed",
                        failure_type=type(exc).__name__,
                        runtime_resources="retained; no cleanup or teardown attempted",
                    )
                except Exception:
                    pass
            if isinstance(exc, DemoError):
                raise DemoError(f"{exc}; preparation evidence: {preparation_directory}") from exc
            raise DemoError(
                f"run preparation stopped safely ({type(exc).__name__}); resources were retained; "
                f"preparation evidence: {preparation_directory}"
            ) from exc
    assert lock is not None
    plan = _plan_document(identity, mode, host_capacity=host_capacity, source_receipt_sha256=source_sha)

    try:
        render_overlay(identity.run_id, root, version_lock=dict(lock))
        session_path = root / SESSION_RECEIPT
        session: dict[str, Any] = {
            "schema_version": 1,
            "status": "running",
            "run_id": identity.run_id,
            "profile": identity.profile,
            "context": identity.profile,
            "namespace": identity.namespace,
            "mode": mode,
            "started_at_utc": _utc_now(),
            "plan": plan,
            "completed_stages": [*preparation_stages, "private_render"],
            "stage": "namespace_preflight",
            "runtime_acceptance": "pending",
            "teardown": "not requested; resources retained",
            "preparation_directory": (
                str(preparation_directory) if preparation_directory is not None else None
            ),
        }
        _write_private_json(root / "version-lock.json", lock)
        _write_private_json(session_path, session)
        _require_absent_namespace(active_executor, identity)
        session["completed_stages"].append("namespace_preflight")
        session["stage"] = "serving"
        _write_private_json(session_path, session, allow_replace=True)

        secret_bundle = build_mongo_secret_bundle(identity)
        serving_result = deploy_serving(
            active_executor,
            identity,
            lock,
            host_capacity=host_capacity,
            run_directory=root / "run",
            secret_bundle=secret_bundle,
            execute=True,
        )
        serving_receipt = serving_result.get("ownership_receipt")
        b2_receipt_path = root / "b2-ownership-receipt.json"
        b2_document = json.loads(receipt_json(serving_receipt))
        _write_private_json(b2_receipt_path, b2_document)
        session["completed_stages"].append("serving")
        session["stage"] = "pipeline_render"
        _write_private_json(session_path, session, allow_replace=True)

        pipeline_directory = render_pipeline_assets(
            identity,
            root,
            lock,
            mode=mode,
            host_capacity=host_capacity,
            source_receipt_sha256=source_sha,
        )
        session["stage"] = "pipeline"
        _write_private_json(session_path, session, allow_replace=True)
        pipeline_result = execute_pipeline(
            active_executor,
            identity,
            pipeline_directory,
            lock=lock,
            serving_receipt_path=b2_receipt_path,
            host_capacity=host_capacity,
            source_receipt_sha256=source_sha,
            input_directory=input_directory,
            execute=True,
        )
        b3_receipt_path = Path(str(pipeline_result.get("ownership_receipt", "")))
        if not b3_receipt_path.is_file():
            raise DemoError("B3 completed without its live ownership receipt; runtime PASS is unavailable")
        pipeline_receipt_from_document(_read_json(b3_receipt_path))
        session["completed_stages"].append("pipeline")
        session["stage"] = "acceptance"
        _write_private_json(session_path, session, allow_replace=True)

        acceptance_path = pipeline_directory / "acceptance-receipt.json"
        acceptance_result = run_acceptance(
            active_executor,
            identity,
            lock,
            mode=mode,
            b2_receipt=b2_receipt_path,
            b3_receipt=b3_receipt_path,
            pipeline_directory=pipeline_directory,
            acceptance_receipt_path=acceptance_path,
            execute=True,
        )
        if not _acceptance_passed(mode, acceptance_result):
            raise DemoError("B4 did not produce a complete passing runtime receipt")
        session.update(
            status="runtime_pass",
            stage="complete",
            completed_stages=[*session["completed_stages"], "acceptance"],
            runtime_acceptance=acceptance_result["status"],
            completed_at_utc=_utc_now(),
            artifacts={
                "b2_receipt": {"path": str(b2_receipt_path.relative_to(root)), "sha256": _sha256(b2_receipt_path)},
                "b3_receipt": {"path": str(b3_receipt_path.relative_to(root)), "sha256": _sha256(b3_receipt_path)},
                "acceptance_receipt": {"path": str(acceptance_path.relative_to(root)), "sha256": _sha256(acceptance_path)},
            },
            dashboard_access={
                "service": f"http://module4d-ui.{identity.namespace}.svc.cluster.local:8501",
                "port_forward_command": [
                    "kubectl", "--context", identity.profile, "--namespace", identity.namespace,
                    "port-forward", "service/module4d-ui", "8501:8501",
                ],
                "command_executed": False,
            },
        )
        _write_private_json(session_path, session, allow_replace=True)
        return session
    except Exception as exc:
        if "session" in locals() and "session_path" in locals() and session_path.parent.is_dir():
            session.update(
                status="failed",
                failed_stage=session.get("stage", "private_render"),
                failure_type=type(exc).__name__,
                failure_summary="See the sanitized CLI error. No command output or credential value is stored in this receipt.",
                runtime_acceptance="not passed",
                resources="retained for inspection; no automatic teardown",
                completed_at_utc=_utc_now(),
            )
            try:
                _write_private_json(session_path, session, allow_replace=True)
            except DemoError:
                pass
        if isinstance(exc, DemoError):
            raise
        if isinstance(exc, (DeploymentError, PipelineError, AcceptanceError)):
            raise DemoError(str(exc)) from exc
        raise DemoError(f"demo stopped safely ({type(exc).__name__}); run-scoped resources were retained") from exc


def verify_run(run_id: str, output_directory: Path | str) -> dict[str, Any]:
    """Verify private local receipts only; this command does not contact a cluster."""
    identity = RunIdentity(run_id)
    requested_root = Path(output_directory)
    if requested_root.is_symlink():
        raise DemoError("run output must be a private non-symlink directory")
    root = requested_root.resolve(strict=True)
    if not root.is_dir() or root.stat().st_mode & 0o077:
        raise DemoError("run output must be a private non-symlink directory")
    session = _read_json(root / SESSION_RECEIPT)
    if any(session.get(field) != value for field, value in (
        ("run_id", identity.run_id), ("profile", identity.profile), ("context", identity.profile),
        ("namespace", identity.namespace),
    )):
        raise DemoError("session receipt belongs to a different run identity")
    artifacts = session.get("artifacts", {})
    verified = {}
    if session.get("status") == "runtime_pass":
        for name in ("b2_receipt", "b3_receipt", "acceptance_receipt"):
            item = artifacts.get(name) if isinstance(artifacts, Mapping) else None
            if not isinstance(item, Mapping) or not isinstance(item.get("path"), str):
                raise DemoError("runtime-pass session is missing a bound receipt")
            path = root / item["path"]
            if path.is_symlink() or root not in path.resolve(strict=True).parents:
                raise DemoError("receipt path escapes the private run directory")
            if item.get("sha256") != _sha256(path):
                raise DemoError("run receipt changed after collection")
            verified[name] = item["sha256"]
        lock = _read_json(root / "version-lock.json")
        validate_static_lock(lock)
        validate_version_lock(lock)
        if _identity_from_lock(lock, identity.run_id) != identity:
            raise DemoError("saved version lock belongs to a different run identity")
        try:
            b2_document = _read_json(root / artifacts["b2_receipt"]["path"])
            b2 = receipt_from_document(b2_document)
            validate_collected_receipt(b2, lock)
            b3_document = _read_json(root / artifacts["b3_receipt"]["path"])
            b3 = pipeline_receipt_from_document(b3_document)
            validate_pipeline_receipt(identity, b3, lock)
        except DemoError:
            raise
        except Exception as exc:
            raise DemoError("saved B2/B3 ownership receipts are malformed or inconsistent") from exc
        acceptance = _read_json(root / artifacts["acceptance_receipt"]["path"])
        expected_status = "fixture_runtime_pass" if session.get("mode") == "fixture" else "full_data_runtime_pass"
        if (
            acceptance.get("status") != expected_status
            or acceptance.get("scope") != f"{session.get('mode')}_runtime"
            or acceptance.get("run_id") != identity.run_id
            or acceptance.get("profile") != identity.profile
            or acceptance.get("namespace") != identity.namespace
            or acceptance.get("cluster_uid") != b3.cluster_uid
            or acceptance.get("namespace_uid") != b3.namespace_uid
            or acceptance.get("lock_sha256") != _sha256_json(lock)
            or acceptance.get("b2_receipt_sha256") != verified["b2_receipt"]
            or acceptance.get("b3_receipt_sha256") != verified["b3_receipt"]
        ):
            raise DemoError("session runtime status does not match its B4 acceptance receipt")
        checks = acceptance.get("checks")
        if (
            not isinstance(checks, list)
            or {item.get("name") for item in checks if isinstance(item, Mapping)} != ACCEPTANCE_SCENARIOS
            or len(checks) != len(ACCEPTANCE_SCENARIOS)
            or any(
                not isinstance(item, Mapping)
                or item.get("status") != "passed"
                or not isinstance(item.get("evidence_sha256"), str)
                or not _SHA256.fullmatch(item["evidence_sha256"])
                for item in checks
            )
        ):
            raise DemoError("B4 acceptance receipt is incomplete or contains a failed scenario")
        for path_field, digest_field in (
            ("task_result_path", "task_result_file_sha256"),
            ("job_log_path", "job_log_file_sha256"),
        ):
            candidate = Path(str(acceptance.get(path_field, "")))
            if candidate.is_symlink():
                raise DemoError("B4 artifact path is a symbolic link")
            resolved = candidate.resolve(strict=True)
            if root not in resolved.parents or resolved.stat().st_mode & 0o077:
                raise DemoError("B4 artifact is outside the private run directory")
            if acceptance.get(digest_field) != _sha256(resolved):
                raise DemoError("B4 sanitized artifact changed after collection")
        acceptance_status = acceptance["status"]
    else:
        acceptance_status = "not passed"
    return {
        "run_id": identity.run_id,
        "session_status": session.get("status"),
        "runtime_acceptance": acceptance_status,
        "verified_receipt_sha256": verified,
        "cluster_contacted": False,
        "teardown_performed": False,
    }


def collect_run(
    run_id: str,
    output_directory: Path | str,
    *,
    executor: CommandExecutor | None = None,
    failure: bool = False,
    execute: bool = False,
) -> dict[str, Any]:
    """List sanitized run evidence, optionally collecting live failure details."""
    if failure and execute is not True:
        raise DemoError("live failure evidence collection requires explicit --execute authorization")
    identity = RunIdentity(run_id)
    requested_root = Path(output_directory)
    if requested_root.is_symlink():
        raise DemoError("run output must not be a symbolic link")
    root = requested_root.resolve(strict=True)
    if not root.is_dir() or root.stat().st_mode & 0o077:
        raise DemoError("run output must be a private directory")
    session = _read_json(root / SESSION_RECEIPT)
    if session.get("run_id") != identity.run_id or session.get("profile") != identity.profile:
        raise DemoError("session receipt belongs to a different run identity")
    if failure:
        evidence_path = root / "failure-evidence.json"
        receipt_path = root / "failure-evidence-receipt.json"
        try:
            captured = collect_failure_evidence(
                executor or SubprocessExecutor(), identity, evidence_path, execute=True
            )
        except AcceptanceError as exc:
            raise DemoError(str(exc)) from exc
        receipt = {
            "schema_version": 1,
            "run_id": identity.run_id,
            "profile": identity.profile,
            "namespace": identity.namespace,
            "status": captured["status"],
            "path": str(evidence_path),
            "sha256": captured["sha256"],
            "captured_at_utc": _utc_now(),
        }
        _write_private_json(receipt_path, receipt)
    artifacts = {}
    for path in sorted(root.rglob("*")):
        if path.is_symlink() or not path.is_file() or path.name.endswith(".tmp"):
            continue
        # Exclude credential and raw dataset paths by construction; inventory only
        # regular files and expose their basename, size, and digest.
        artifacts[str(path.relative_to(root))] = {"bytes": path.stat().st_size, "sha256": _sha256(path)}
    return {
        "run_id": identity.run_id,
        "session_status": session.get("status"),
        "runtime_acceptance": session.get("runtime_acceptance", "not passed"),
        "failure_evidence_collected": failure,
        "artifacts": artifacts,
        "resources_retained": True,
    }


def teardown_demo(
    run_id: str,
    output_directory: Path | str,
    *,
    executor: CommandExecutor | None = None,
    execute: bool = False,
) -> dict[str, Any]:
    """Show or execute only the exact run-scoped teardown after offline checks."""
    identity = RunIdentity(run_id)
    requested_root = Path(output_directory)
    if requested_root.is_symlink():
        raise DemoError("run output must not be a symbolic link")
    root = requested_root.resolve(strict=True)
    if not root.is_dir() or root.stat().st_mode & 0o077:
        raise DemoError("run output must be a private directory")
    session = _read_json(root / SESSION_RECEIPT)
    if session.get("run_id") != identity.run_id or session.get("profile") != identity.profile:
        raise DemoError("session receipt belongs to a different run identity")
    lock = load_version_lock(root / "version-lock.json")
    validate_static_lock(lock)
    validate_version_lock(lock)
    if _identity_from_lock(lock, run_id) != identity:
        raise DemoError("saved version lock belongs to a different run identity")
    b2_path = root / "b2-ownership-receipt.json"
    b3_path = root / "pipeline" / "pipeline-ownership-receipt.json"
    acceptance_path = root / "pipeline" / "acceptance-receipt.json"
    failure_pointer_path = root / "failure-evidence-receipt.json"
    failure_evidence = None
    try:
        b2 = receipt_from_document(_read_json(b2_path))
        validate_collected_receipt(b2, lock)
        b3 = pipeline_receipt_from_document(_read_json(b3_path))
        validate_pipeline_receipt(identity, b3, lock)
        if failure_pointer_path.is_file():
            pointer = _read_json(failure_pointer_path)
            evidence_path = Path(str(pointer.get("path", "")))
            if evidence_path.is_symlink():
                raise DemoError("failure evidence path is a symbolic link")
            resolved = evidence_path.resolve(strict=True)
            if root not in resolved.parents or pointer.get("sha256") != _sha256(resolved):
                raise DemoError("failure evidence receipt does not match private run evidence")
            failure_evidence = {"path": str(resolved), "sha256": pointer["sha256"]}
    except DemoError:
        raise
    except Exception as exc:
        raise DemoError("local ownership or failure evidence receipts are invalid") from exc
    if failure_evidence is None:
        raise DemoError("collect and review --failure evidence before teardown")
    if acceptance_path.is_file():
        acceptance, _acceptance_digest = _read_teardown_receipt(acceptance_path, "B4")
        if (
            acceptance.get("schema_version") != 1
            or acceptance.get("run_id") != identity.run_id
            or acceptance.get("profile") != identity.profile
            or acceptance.get("namespace") != identity.namespace
            or acceptance.get("b2_receipt_sha256") != _sha256(b2_path)
            or acceptance.get("b3_receipt_sha256") != _sha256(b3_path)
            or acceptance.get("lock_sha256") != _sha256_json(lock)
        ):
            raise DemoError("acceptance receipt does not bind this run and its B2/B3 locks")
        try:
            _validate_acceptance_checks(acceptance)
            if acceptance.get("status") in {"fixture_runtime_pass", "full_data_runtime_pass"}:
                _verify_acceptance_artifacts(acceptance)
        except TeardownError as exc:
            raise DemoError(f"acceptance receipt cannot authorize teardown: {exc}") from exc
    plan = {
        "run_id": identity.run_id,
        "profile": identity.profile,
        "namespace": identity.namespace,
        "actions": [
            ["kubectl", "--context", identity.profile, "delete", "namespace", identity.namespace,
             "--wait=true", "--timeout=180s"],
            ["minikube", "-p", identity.profile, "delete"],
        ],
        "evidence_sha256": {
            "b2": _sha256(b2_path),
            "b3": _sha256(b3_path),
            "failure": failure_evidence["sha256"],
            "acceptance": _sha256(acceptance_path) if acceptance_path.is_file() else None,
        },
        "command_executed": False,
    }
    if not execute:
        return {"status": "plan_only", **plan}
    try:
        result = teardown_run(
            executor or SubprocessExecutor(),
            identity,
            lock,
            b2_receipt=b2_path,
            b3_receipt=b3_path,
            acceptance_receipt=acceptance_path if acceptance_path.is_file() else None,
            failure_evidence=failure_evidence,
            teardown_receipt_path=root / "teardown-receipt.json",
            execute=True,
        )
    except TeardownError as exc:
        raise DemoError(str(exc)) from exc
    return {"status": result.get("status"), **plan, "command_executed": True}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Offline-first IT4931 Kubernetes demonstration runner")
    commands = parser.add_subparsers(dest="command", required=True)

    preflight = commands.add_parser("preflight", help="check local capacity, inputs, lock, and executable presence")
    preflight.add_argument("--mode", choices=("fixture", "full"), default="fixture")
    preflight.add_argument("--lock", type=Path, default=DEFAULT_LOCK)
    preflight.add_argument("--input-dir", type=Path)
    preflight.add_argument("--windows-backing-disk-gib", type=float)

    plan = commands.add_parser("plan", help="print a bounded run plan without creating files or contacting services")
    plan.add_argument("--mode", choices=("fixture", "full"), default="fixture")
    plan.add_argument("--run-id", required=True)
    plan.add_argument("--input-dir", type=Path)

    run = commands.add_parser("run", help="prepare a fresh dedicated profile and run B2, B3, then B4")
    run.add_argument("--mode", choices=("fixture", "full"), default="fixture")
    run.add_argument("--run-id", help="optional 12-hex run ID; generated when omitted")
    run.add_argument("--lock", type=Path, help="reuse a fully finalized B3 lock instead of preparing a new profile")
    run.add_argument("--output", type=Path, help="new private output directory; defaults under the system temp directory")
    run.add_argument("--input-dir", type=Path)
    run.add_argument("--windows-backing-disk-gib", type=float)
    run.add_argument("--execute", action="store_true", help="authorize live Kubernetes and workload operations")

    verify = commands.add_parser("verify", help="verify saved local receipt hashes; no cluster access")
    verify.add_argument("--run-id", required=True)
    verify.add_argument("--output", type=Path, required=True)

    collect = commands.add_parser("collect", help="summarize private run artifacts or collect live failure evidence")
    collect.add_argument("--run-id", required=True)
    collect.add_argument("--output", type=Path, required=True)
    collect.add_argument("--failure", action="store_true")
    collect.add_argument("--execute", action="store_true", help="authorize read-only live failure evidence collection")

    teardown = commands.add_parser("teardown", help="review or explicitly execute verified run-scoped teardown")
    teardown.add_argument("--run-id", required=True)
    teardown.add_argument("--output", type=Path, required=True)
    teardown.add_argument("--execute", action="store_true", help="authorize the namespace/profile deletion after live identity checks")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "preflight":
            report = preflight_report(
                mode=args.mode,
                lock_path=args.lock,
                input_directory=args.input_dir,
                windows_backing_disk_gib=args.windows_backing_disk_gib,
            )
        elif args.command == "plan":
            identity = RunIdentity(args.run_id)
            capacity = measure_host_capacity()
            source = _validate_mode_inputs(args.mode, input_directory=args.input_dir, capacity=capacity)
            report = _plan_document(
                identity, args.mode, host_capacity=capacity,
                source_receipt_sha256=source.get("manifest_sha256") if source else None,
            )
        elif args.command == "run":
            report = run_demo(
                mode=args.mode,
                lock_path=args.lock,
                output_directory=args.output,
                input_directory=args.input_dir,
                run_id=args.run_id,
                windows_backing_disk_gib=args.windows_backing_disk_gib,
                execute=args.execute,
            )
        elif args.command == "verify":
            report = verify_run(args.run_id, args.output)
        elif args.command == "collect":
            report = collect_run(
                args.run_id, args.output, failure=args.failure, execute=args.execute
            )
        else:
            report = teardown_demo(args.run_id, args.output, execute=args.execute)
        print(json.dumps(report, sort_keys=True, indent=2))
        return 0
    except (DemoError, LockLifecycleError, OwnershipError, InputReceiptError, RuntimePreflightError, PipelineError) as exc:
        # Infrastructure command output and secret payloads are never printed.
        print(f"demo stopped: {type(exc).__name__}: {str(exc)[:500]}", file=sys.stderr)
        return 2
    except Exception as exc:
        # Keep unexpected parser, filesystem, and tool errors from emitting a
        # traceback that might contain command arguments or environment data.
        print(f"demo stopped safely: {type(exc).__name__}; inspect the private run evidence", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
