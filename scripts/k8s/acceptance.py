"""B4 acceptance planning, runtime collection, and failure-evidence capture.

Runtime operations require ``execute=True`` and fresh comparisons against both
the B2 and B3 ownership receipts. This module never tears resources down.
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Mapping, Protocol

import yaml

from .ownership import RunIdentity
from .pipeline import PIPELINE_MANIFESTS, PipelineError, _locked_image, _validate_pipeline_lock
from .pipeline_receipts import (
    assert_pipeline_receipt_matches_live,
    collect_pipeline_receipt,
    pipeline_receipt_from_document,
)
from .receipts import (
    assert_receipt_matches_lock_and_live,
    collect_runtime_ownership_receipt,
    receipt_from_document,
)
from .runtime import CommandExecutor, _checked, _json_output, kubectl


ACCEPTANCE_SCENARIOS = frozenset({
    "input_provenance",
    "hdfs_readback",
    "kafka_delivery",
    "spark_batch_outputs",
    "spark_query_experiments",
    "spark_executor_benchmark",
    "event_time_watermark_late_data",
    "checkpoint_recovery",
    "mongo_idempotent_publication",
    "als_graph_readback",
    "hybrid_trend_change",
    "api_ui_smoke",
    "network_policy_denied_api_path",
    "storage_provenance",
})
RUNTIME_SCOPES = frozenset({"fixture_runtime", "full_data_runtime"})
SCOPE_VALUES = frozenset({"static", "offline", "ci", *RUNTIME_SCOPES})
_HEX_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MONGO_URI = re.compile(r"(?i)mongodb(?:\+srv)?://[^\s\"'<>]+")
_SECRET_ASSIGNMENT = re.compile(r"(?i)\b(password|passwd|pwd|token|secret)(\s*[:=]\s*)([^\s,;&]+)")


class AcceptanceError(RuntimeError):
    """B4 acceptance plan, runtime proof, or evidence collection failed."""


class AcceptanceExecutor(Protocol):
    def run(self, argv: list[str], *, timeout: int, input_text: str | None = None): ...


def _safe_text(value: str) -> str:
    return _SECRET_ASSIGNMENT.sub(r"\1\2<redacted>", _MONGO_URI.sub("mongodb://<redacted>", value))


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_json(value: Any) -> str:
    return _sha256_bytes(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode())


def classify_acceptance(scope: str, checks: list[Mapping[str, Any]], *, runtime_identity_complete: bool) -> str:
    """Keep offline, fixture-runtime, and full-data-runtime results distinct."""
    if scope not in SCOPE_VALUES:
        raise AcceptanceError("acceptance scope is not recognized")
    names = [item.get("name") for item in checks if isinstance(item, Mapping)]
    if set(names) != ACCEPTANCE_SCENARIOS or len(names) != len(ACCEPTANCE_SCENARIOS):
        raise AcceptanceError("acceptance scenario inventory is incomplete or duplicated")
    statuses = [item.get("status") for item in checks]
    if any(status == "failed" for status in statuses):
        return "failed"
    if scope not in RUNTIME_SCOPES:
        return "not_runtime_acceptance"
    if not all(status == "passed" for status in statuses) or not runtime_identity_complete:
        return "incomplete_runtime_acceptance"
    return "fixture_runtime_pass" if scope == "fixture_runtime" else "full_data_runtime_pass"


def build_acceptance_receipt(
    *,
    identity: RunIdentity,
    scope: str,
    checks: list[Mapping[str, Any]],
    cluster_uid: str | None = None,
    namespace_uid: str | None = None,
    acceptance_job_uid: str | None = None,
    acceptance_pod_uid: str | None = None,
    lock_sha256: str | None = None,
    b2_receipt_sha256: str | None = None,
    b3_receipt_sha256: str | None = None,
) -> dict[str, Any]:
    """Build a sanitized receipt; runtime PASS requires complete live identity."""
    if not isinstance(identity, RunIdentity):
        raise AcceptanceError("acceptance receipt requires a validated run identity")
    if scope not in SCOPE_VALUES:
        raise AcceptanceError("acceptance scope is not recognized")
    validated_checks = []
    for item in checks:
        if not isinstance(item, Mapping):
            raise AcceptanceError("acceptance check evidence must be an object")
        name, status, digest = item.get("name"), item.get("status"), item.get("evidence_sha256")
        if name not in ACCEPTANCE_SCENARIOS or status not in {"passed", "failed", "not_run"}:
            raise AcceptanceError("acceptance check identity or status is invalid")
        if not isinstance(digest, str) or not _HEX_SHA256.fullmatch(digest):
            raise AcceptanceError(f"acceptance check {name} is missing a SHA-256 evidence identity")
        validated_checks.append({"name": name, "status": status, "evidence_sha256": digest})
    runtime_identity_complete = (
        scope in RUNTIME_SCOPES
        and all(isinstance(value, str) and value.strip() for value in (
            cluster_uid, namespace_uid, acceptance_job_uid, acceptance_pod_uid,
            lock_sha256, b2_receipt_sha256, b3_receipt_sha256,
        ))
        and all(_HEX_SHA256.fullmatch(value) for value in (lock_sha256, b2_receipt_sha256, b3_receipt_sha256))
    )
    overall = classify_acceptance(scope, validated_checks, runtime_identity_complete=bool(runtime_identity_complete))
    return {
        "schema_version": 1,
        "scope": scope,
        "status": overall,
        "run_id": identity.run_id,
        "profile": identity.profile,
        "namespace": identity.namespace,
        "cluster_uid": cluster_uid if runtime_identity_complete else None,
        "namespace_uid": namespace_uid if runtime_identity_complete else None,
        "acceptance_job_uid": acceptance_job_uid if runtime_identity_complete else None,
        "acceptance_pod_uid": acceptance_pod_uid if runtime_identity_complete else None,
        "lock_sha256": lock_sha256 if runtime_identity_complete else None,
        "b2_receipt_sha256": b2_receipt_sha256 if runtime_identity_complete else None,
        "b3_receipt_sha256": b3_receipt_sha256 if runtime_identity_complete else None,
        "checks": sorted(validated_checks, key=lambda item: item["name"]),
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def render_acceptance_job(
    identity: RunIdentity,
    lock: Mapping[str, Any],
    pipeline_directory: Path | str,
) -> Path:
    """Render the B4 Job into the private B2/B3 run tree with immutable images."""
    _validate_pipeline_lock(lock)
    root = Path(pipeline_directory).resolve(strict=True)
    if root.name != "pipeline" or not (root / "plan.json").is_file():
        raise AcceptanceError("B4 Job must be rendered beneath the private completed B3 plan")
    try:
        plan = json.loads((root / "plan.json").read_text(encoding="utf-8"))
        template = yaml.safe_load((PIPELINE_MANIFESTS / "acceptance-job.yaml").read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError, yaml.YAMLError) as exc:
        raise AcceptanceError("private B3 plan or B4 Job template is unavailable") from exc
    if plan.get("run_id") != identity.run_id or plan.get("profile") != identity.profile or plan.get("namespace") != identity.namespace:
        raise AcceptanceError("B4 Job run identity differs from the private B3 plan")
    if (root / "job-acceptance.yaml").exists():
        raise AcceptanceError("B4 Job manifest already exists for this run")
    if not isinstance(template, dict) or template.get("kind") != "Job":
        raise AcceptanceError("B4 Job template has an unsupported structure")
    database = f"module4d_demo_{identity.run_id}"
    replacements = {
        "__K8S_RENDER_REQUIRED__": identity.namespace,
        "__RUN_ID__": identity.run_id,
        "__PIPELINE_IMAGE__": _locked_image(lock, "pipeline_application_image"),
        "__DATABASE__": database,
    }
    document = json.loads(json.dumps(template))
    encoded = json.dumps(document)
    for token, replacement in replacements.items():
        encoded = encoded.replace(token, replacement)
    document = json.loads(encoded)
    owner_labels = identity.labels
    for metadata in (document["metadata"], document["spec"]["template"]["metadata"]):
        metadata.setdefault("labels", {}).update(owner_labels)
    output = root / "job-acceptance.yaml"
    output.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    output.chmod(0o600)
    return output


def _read_receipt(source: Path | str | Mapping[str, Any], description: str) -> tuple[dict[str, Any], str]:
    try:
        if isinstance(source, Mapping):
            value = dict(source)
            encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
        else:
            path = Path(source).resolve(strict=True)
            encoded = path.read_bytes()
            value = json.loads(encoded)
    except (OSError, UnicodeError, json.JSONDecodeError, TypeError) as exc:
        raise AcceptanceError(f"{description} receipt could not be read") from exc
    if not isinstance(value, dict):
        raise AcceptanceError(f"{description} receipt is malformed")
    return value, _sha256_bytes(encoded)


def _get_json(executor: AcceptanceExecutor, identity: RunIdentity, command: list[str], description: str) -> dict[str, Any]:
    try:
        result = _checked(executor, command)
        value = _json_output(result, description)
    except Exception as exc:
        raise AcceptanceError(f"{description} could not be collected") from exc
    if not isinstance(value, dict):
        raise AcceptanceError(f"{description} response is malformed")
    return value


def _checked_acceptance_command(
    executor: AcceptanceExecutor, command: list[str], description: str, timeout: int,
) -> str:
    try:
        result = executor.run(command, timeout=timeout)
    except Exception as exc:
        raise AcceptanceError(f"{description} failed ({type(exc).__name__})") from exc
    if result.returncode != 0:
        raise AcceptanceError(f"{description} failed with exit status {result.returncode}")
    return _safe_text(result.stdout or "")


def _probe_api_ui(executor: AcceptanceExecutor, identity: RunIdentity) -> str:
    api_output = _checked_acceptance_command(
        executor,
        kubectl(identity, "--namespace", identity.namespace, "exec", "deployment/module4d-api", "--",
                "python3", "/workspace/scripts/k8s/api_readiness.py"),
        "B4 API readiness probe", 60,
    )
    ui_probe = (
        "import json,urllib.request;"
        "o=urllib.request.urlopen('http://module4d-api:8000/openapi.json',timeout=10);"
        "openapi=json.load(o);"
        "r=urllib.request.urlopen('http://module4d-api:8000/api/v1/recommendations/1',timeout=10);"
        "recommendations=json.load(r);"
        "u=urllib.request.urlopen('http://127.0.0.1:8501',timeout=10);"
        "print(json.dumps({'api_status':o.status,'recommendation_status':r.status,'recommendation_user_id':recommendations.get('user_id'),"
        "'recommendation_status_field':recommendations.get('status'),'recommendation_count':recommendations.get('recommendation_count'),"
        "'actual_recommendations':len(recommendations.get('recommendations',[])),'ui_status':u.status,'api_paths':len(openapi.get('paths',{}))}))"
    )
    ui_output = _checked_acceptance_command(
        executor,
        kubectl(identity, "--namespace", identity.namespace, "exec", "deployment/module4d-ui", "--",
                "python3", "-c", ui_probe),
        "B4 UI-to-API and UI HTTP probe", 60,
    )
    try:
        payload = json.loads(ui_output.strip().splitlines()[-1])
    except (json.JSONDecodeError, IndexError) as exc:
        raise AcceptanceError("B4 UI/API probe did not return structured evidence") from exc
    if (
        payload.get("api_status") != 200 or payload.get("recommendation_status") != 200
        or payload.get("recommendation_user_id") != 1
        or payload.get("recommendation_status_field") != "ready"
        or not isinstance(payload.get("recommendation_count"), int)
        or payload.get("recommendation_count", 0) != payload.get("actual_recommendations")
        or payload.get("actual_recommendations", 21) > 20
        or payload.get("ui_status") != 200 or payload.get("api_paths", 0) < 1
    ):
        raise AcceptanceError("B4 API/UI probe returned an unexpected HTTP contract")
    return _sha256_json({"api_readiness": api_output, "ui_api_probe": payload})


def _validate_task_result(value: Any, identity: RunIdentity, mode: str) -> list[dict[str, Any]]:
    expected_scope = "fixture_runtime" if mode == "fixture" else "full_data_runtime"
    if not isinstance(value, Mapping) or value.get("run_id") != identity.run_id or value.get("scope") != expected_scope or value.get("status") != "completed":
        raise AcceptanceError("B4 Job output does not match the requested run and scope")
    checks = value.get("checks")
    if not isinstance(checks, list):
        raise AcceptanceError("B4 Job output is missing its scenario evidence")
    normalized = []
    for item in checks:
        if not isinstance(item, Mapping):
            raise AcceptanceError("B4 Job output contains a malformed scenario")
        name = item.get("name")
        digest = item.get("evidence_sha256")
        if name not in ACCEPTANCE_SCENARIOS or name in {entry["name"] for entry in normalized}:
            raise AcceptanceError("B4 Job output has an unexpected or duplicate scenario")
        if item.get("status") != "passed" or not isinstance(digest, str) or not _HEX_SHA256.fullmatch(digest):
            raise AcceptanceError(f"B4 Job scenario {name} did not provide a passing hashed result")
        normalized.append({"name": name, "status": "passed", "evidence_sha256": digest})
    required_from_job = ACCEPTANCE_SCENARIOS - {"api_ui_smoke", "storage_provenance"}
    if {item["name"] for item in normalized} != required_from_job:
        raise AcceptanceError("B4 Job did not cover the exact in-cluster scenario inventory")
    return normalized


def run_acceptance(
    executor: AcceptanceExecutor,
    identity: RunIdentity,
    lock: Mapping[str, Any],
    *,
    mode: str,
    b2_receipt: Path | str | Mapping[str, Any],
    b3_receipt: Path | str | Mapping[str, Any],
    pipeline_directory: Path | str,
    acceptance_receipt_path: Path | str,
    execute: bool = False,
) -> dict[str, Any]:
    """Verify prior live receipts, run B4 once, and write its sanitized receipt."""
    if execute is not True:
        raise AcceptanceError("B4 runtime acceptance requires explicit execute=True authorization")
    if mode not in {"fixture", "full"}:
        raise AcceptanceError("B4 mode must be fixture or full")
    try:
        _validate_pipeline_lock(lock)
    except (PipelineError, KeyError, TypeError) as exc:
        raise AcceptanceError("B4 requires a complete B3 lock") from exc
    b2_document, b2_sha = _read_receipt(b2_receipt, "B2")
    b3_document, b3_sha = _read_receipt(b3_receipt, "B3")
    try:
        recorded_b2 = receipt_from_document(b2_document)
        live_b2 = collect_runtime_ownership_receipt(executor, identity, lock, execute=True)
        assert_receipt_matches_lock_and_live(recorded_b2, live_b2, lock, identity)
        recorded_b3 = pipeline_receipt_from_document(b3_document)
        live_b3 = collect_pipeline_receipt(executor, identity, lock, execute=True)
        assert_pipeline_receipt_matches_live(recorded_b3, live_b3, identity, lock)
    except Exception as exc:
        raise AcceptanceError("recorded B2/B3 receipts do not match live run identities; acceptance stopped") from exc

    pipeline_root = Path(pipeline_directory).resolve(strict=True)
    output = Path(acceptance_receipt_path).resolve()
    if (
        output.exists() or output.is_symlink() or not output.parent.is_dir()
        or output.parent != pipeline_root or pipeline_root.stat().st_mode & 0o077
        or output.name in {"job-acceptance.yaml", "acceptance-task-result.json", "acceptance-job.log"}
    ):
        raise AcceptanceError("B4 receipt output must be a new file inside the private B3 run directory")
    try:
        plan = json.loads((pipeline_root / "plan.json").read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise AcceptanceError("private B3 run plan could not be read") from exc
    locked_images = {role: lock["images"][role] for role in (
        "pipeline_application_image", "kafka", "hdfs_namenode", "hdfs_datanode",
    )}
    locked_images["pipeline_image_provenance"] = lock["pipeline_image_provenance"]
    locked_images["pipeline_component_image_provenance"] = lock["pipeline_component_image_provenance"]
    expected_lock_digest = _sha256_bytes(json.dumps(locked_images, sort_keys=True).encode())
    if (
        plan.get("mode") != mode or plan.get("run_id") != identity.run_id
        or plan.get("profile") != identity.profile or plan.get("namespace") != identity.namespace
        or plan.get("pipeline_lock_sha256") != expected_lock_digest
    ):
        raise AcceptanceError("B4 mode or image lock differs from the private B3 run plan")
    job_path = render_acceptance_job(identity, lock, pipeline_root)
    job_name = f"it4931-acceptance-{identity.run_id}"
    task_result_path = pipeline_root / "acceptance-task-result.json"
    job_log_path = pipeline_root / "acceptance-job.log"
    if any(path.exists() or path.is_symlink() for path in (task_result_path, job_log_path)):
        raise AcceptanceError("B4 evidence output already exists for this run")
    try:
        jobs = _get_json(
            executor, identity,
            kubectl(identity, "--namespace", identity.namespace, "get", "jobs", "-o", "json"),
            "run Job inventory",
        ).get("items")
        if not isinstance(jobs, list) or any(
            isinstance(item, Mapping) and item.get("metadata", {}).get("name") == job_name for item in jobs
        ):
            raise AcceptanceError("B4 Job already exists for this run; refusing to overwrite acceptance evidence")
        _checked_acceptance_command(executor, kubectl(identity, "--namespace", identity.namespace, "apply", "-f", str(job_path)), "B4 Job apply", 120)
        _checked_acceptance_command(
            executor,
            kubectl(identity, "--namespace", identity.namespace, "wait", "--for=condition=complete", f"job/{job_name}", "--timeout=1500s"),
            "B4 scenario Job completion", 1530,
        )
        job_doc = _get_json(executor, identity, kubectl(identity, "--namespace", identity.namespace, "get", "job", job_name, "-o", "json"), "B4 Job identity")
        pod_payload = _get_json(executor, identity, kubectl(identity, "--namespace", identity.namespace, "get", "pods", "-l", f"job-name={job_name}", "-o", "json"), "B4 Job Pod identity")
        pods = pod_payload.get("items")
        if not isinstance(pods, list) or len(pods) != 1:
            raise AcceptanceError("B4 Job must own exactly one Pod")
        pod = pods[0]
        job_meta, pod_meta = job_doc.get("metadata", {}), pod.get("metadata", {})
        job_labels = job_meta.get("labels", {})
        pod_labels = pod_meta.get("labels", {})
        owners = pod_meta.get("ownerReferences", [])
        if (
            not job_meta.get("uid") or not pod_meta.get("uid")
            or job_meta.get("name") != job_name or job_meta.get("namespace") != identity.namespace
            or not isinstance(job_labels, Mapping)
            or any(job_labels.get(key) != value for key, value in identity.labels.items())
            or job_labels.get("it4931.instacart.edu/component") != "pipeline"
            or job_labels.get("it4931.instacart.edu/step") != "acceptance"
            or not isinstance(pod_labels, Mapping)
            or any(pod_labels.get(key) != value for key, value in identity.labels.items())
            or pod_labels.get("it4931.instacart.edu/step") != "acceptance"
            or len(owners) != 1 or owners[0].get("kind") != "Job"
            or owners[0].get("name") != job_name or owners[0].get("uid") != job_meta["uid"]
            or owners[0].get("controller") is not True
        ):
            raise AcceptanceError("B4 Job Pod owner identity does not match the run-scoped Job")
        job_status = job_doc.get("status", {})
        pod_status = pod.get("status", {})
        container = next(
            (item for item in pod.get("spec", {}).get("containers", []) if item.get("name") == "acceptance"), None,
        )
        container_status = next(
            (item for item in pod_status.get("containerStatuses", []) if item.get("name") == "acceptance"), None,
        )
        expected_image = _locked_image(lock, "pipeline_application_image")
        if (
            job_status.get("succeeded") != 1 or job_status.get("failed", 0) != 0
            or job_status.get("active", 0) != 0 or pod_status.get("phase") != "Succeeded"
            or container is None or container.get("image") != expected_image
            or container_status is None
            or container_status.get("state", {}).get("terminated", {}).get("exitCode") != 0
        ):
            raise AcceptanceError("B4 Job status, container exit, or declared image differs from the run contract")
        from .image_provenance import verify_running_image_id
        try:
            verify_running_image_id(
                runtime_image_id=container_status.get("imageID", ""),
                declared_reference=expected_image,
                image_digest=lock["images"]["pipeline_application_image"]["immutable_digest"],
                config_digest=lock["pipeline_image_provenance"]["config_digest"],
            )
        except Exception as exc:
            raise AcceptanceError("B4 Job image identity differs from the locked pipeline image") from exc
        log_output = _checked_acceptance_command(
            executor, kubectl(identity, "--namespace", identity.namespace, "logs", f"job/{job_name}", "-c", "acceptance"),
            "B4 sanitized scenario log collection", 120,
        )
        prefix = "IT4931_ACCEPTANCE_JSON="
        lines = [line[len(prefix):] for line in log_output.splitlines() if line.startswith(prefix)]
        if len(lines) != 1:
            raise AcceptanceError("B4 Job did not emit exactly one structured acceptance result")
        task_result = json.loads(lines[0])
        checks = _validate_task_result(task_result, identity, mode)
        api_digest = _probe_api_ui(executor, identity)
        checks.append({"name": "api_ui_smoke", "status": "passed", "evidence_sha256": api_digest})
        storage_identity = {
            "b2": _sha256_json(b2_document),
            "b3": _sha256_json(b3_document),
            "b2_matches_live": True,
            "b3_matches_live": True,
        }
        checks.append({"name": "storage_provenance", "status": "passed", "evidence_sha256": _sha256_json(storage_identity)})
        cluster_uid = live_b3.cluster_uid
        namespace_uid = live_b3.namespace_uid
        lock_sha = _sha256_json(lock)
        receipt = build_acceptance_receipt(
            identity=identity, scope=task_result["scope"], checks=checks,
            cluster_uid=cluster_uid, namespace_uid=namespace_uid,
            acceptance_job_uid=job_meta["uid"], acceptance_pod_uid=pod_meta["uid"],
            lock_sha256=lock_sha, b2_receipt_sha256=b2_sha, b3_receipt_sha256=b3_sha,
        )
        receipt["task_result_sha256"] = _sha256_json(task_result)
        receipt["sanitized_job_log_sha256"] = _sha256_bytes(log_output.encode())
        task_result_path.write_text(json.dumps(task_result, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        task_result_path.chmod(0o600)
        job_log_path.write_text(log_output, encoding="utf-8")
        job_log_path.chmod(0o600)
        receipt["task_result_path"] = str(task_result_path)
        receipt["task_result_file_sha256"] = _sha256_bytes(task_result_path.read_bytes())
        receipt["job_log_path"] = str(job_log_path)
        receipt["job_log_file_sha256"] = _sha256_bytes(job_log_path.read_bytes())
        receipt["runtime_status_note"] = (
            "fixture runtime evidence only; it is not full-data acceptance"
            if mode == "fixture" else "full-data runtime scope"
        )
        output.write_text(json.dumps(receipt, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        output.chmod(0o600)
        return receipt
    except Exception as exc:
        failure_path = output.parent / f"{output.stem}-failure-evidence.json"
        failure_record = None
        try:
            failure_record = collect_failure_evidence(executor, identity, failure_path, execute=True)
        except Exception:
            failure_path = None
        if failure_record is not None:
            pointer_path = output.parent / f"{output.stem}-failure-evidence-receipt.json"
            pointer_path.write_text(json.dumps(failure_record, sort_keys=True, indent=2) + "\n", encoding="utf-8")
            pointer_path.chmod(0o600)
            detail = f"; failure evidence: {failure_path} (sha256={failure_record['sha256']}); receipt: {pointer_path}"
        else:
            detail = "; failure evidence collection also failed"
        if isinstance(exc, AcceptanceError):
            raise AcceptanceError(f"{exc}{detail}; run-scoped resources are retained") from exc
        raise AcceptanceError(f"B4 acceptance stopped ({type(exc).__name__}){detail}; run-scoped resources are retained") from exc


def collect_failure_evidence(
    executor: AcceptanceExecutor,
    identity: RunIdentity,
    output_path: Path | str,
    *,
    execute: bool = False,
) -> dict[str, Any]:
    """Capture sanitized Events, resource state, describes, and Pod logs before teardown."""
    if execute is not True:
        raise AcceptanceError("failure evidence collection requires explicit execute=True authorization")
    destination = Path(output_path).resolve()
    if destination.exists() or destination.is_symlink() or not destination.parent.is_dir():
        raise AcceptanceError("failure evidence output must be a new file in an existing private directory")
    evidence: dict[str, Any] = {
        "schema_version": 1, "run_id": identity.run_id, "profile": identity.profile,
        "namespace": identity.namespace, "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        "commands": {},
    }
    queries = {
        "events": kubectl(identity, "--namespace", identity.namespace, "get", "events", "-o", "json"),
        "jobs": kubectl(identity, "--namespace", identity.namespace, "get", "jobs", "-o", "json"),
        "pods": kubectl(identity, "--namespace", identity.namespace, "get", "pods", "-o", "json"),
        "statefulsets": kubectl(identity, "--namespace", identity.namespace, "get", "statefulsets", "-o", "json"),
        "claims": kubectl(identity, "--namespace", identity.namespace, "get", "pvc", "-o", "json"),
    }
    pod_names: list[str] = []
    for name, command in queries.items():
        try:
            result = executor.run(command, timeout=60)
            stdout = _safe_text(result.stdout or "")
            stderr = _safe_text(result.stderr or "")
            evidence["commands"][name] = {"exit_code": result.returncode, "stdout": stdout, "stderr": stderr}
            if name == "pods" and result.returncode == 0:
                payload = json.loads(stdout)
                pod_names = [
                    item.get("metadata", {}).get("name") for item in payload.get("items", [])
                    if isinstance(item, Mapping) and isinstance(item.get("metadata", {}).get("name"), str)
                ]
        except Exception as exc:
            evidence["commands"][name] = {"exit_code": None, "error_type": type(exc).__name__}
    evidence["pod_details"] = {}
    for pod_name in sorted(set(pod_names)):
        details = {}
        for key, command in (
            ("describe", kubectl(identity, "--namespace", identity.namespace, "describe", "pod", pod_name)),
            ("logs", kubectl(identity, "--namespace", identity.namespace, "logs", pod_name, "--all-containers=true")),
            ("previous_logs", kubectl(identity, "--namespace", identity.namespace, "logs", pod_name, "--all-containers=true", "--previous")),
        ):
            try:
                result = executor.run(command, timeout=90)
                details[key] = {
                    "exit_code": result.returncode,
                    "stdout": _safe_text(result.stdout or ""),
                    "stderr": _safe_text(result.stderr or ""),
                }
            except Exception as exc:
                details[key] = {"exit_code": None, "error_type": type(exc).__name__}
        evidence["pod_details"][pod_name] = details
    encoded = json.dumps(evidence, sort_keys=True, indent=2).encode() + b"\n"
    destination.write_bytes(encoded)
    destination.chmod(0o600)
    return {"path": str(destination), "sha256": _sha256_bytes(encoded), "pods": len(pod_names), "status": "captured"}
