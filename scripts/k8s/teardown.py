"""Explicit B4 teardown guarded by fresh B2/B3 identity comparisons."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import re
from typing import Any, Mapping

from .acceptance import ACCEPTANCE_SCENARIOS, _read_receipt, _sha256_bytes, _sha256_json
from .ownership import RunIdentity
from .pipeline import _validate_pipeline_lock
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


class TeardownError(RuntimeError):
    """Teardown was blocked because exact run ownership was not proven."""


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


def _failure_evidence(
    value: Path | str | Mapping[str, Any] | None,
    *,
    identity: RunIdentity,
) -> tuple[dict[str, Any], str] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise TeardownError("failure evidence must be supplied with its collected SHA-256 receipt")
    receipt = dict(value)
    path_value, expected_digest = receipt.get("path"), receipt.get("sha256")
    if not isinstance(path_value, (str, Path)):
        raise TeardownError("failure evidence receipt path is missing")
    path = Path(path_value).resolve(strict=True)
    try:
        content = path.read_bytes()
        document = json.loads(content)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise TeardownError("failure evidence could not be read") from exc
    digest = _sha256_bytes(content)
    if not isinstance(expected_digest, str) or not _SHA256.fullmatch(expected_digest) or expected_digest != digest:
        raise TeardownError("failure evidence SHA-256 does not match the captured bytes")
    if (
        not isinstance(document, Mapping)
        or document.get("run_id") != identity.run_id
        or document.get("profile") != identity.profile
        or document.get("namespace") != identity.namespace
    ):
        raise TeardownError("failure evidence belongs to a different run identity")
    return dict(document), digest


def _acceptance_job_identity_from_failure(document: Mapping[str, Any], identity: RunIdentity) -> tuple[str | None, str | None]:
    commands = document.get("commands", {})
    try:
        jobs = json.loads(commands["jobs"]["stdout"]).get("items", [])
        pods = json.loads(commands["pods"]["stdout"]).get("items", [])
    except (KeyError, TypeError, json.JSONDecodeError):
        raise TeardownError("failure evidence lacks readable Job and Pod inventories")
    job_name = f"it4931-acceptance-{identity.run_id}"
    job_matches = [item for item in jobs if isinstance(item, Mapping) and item.get("metadata", {}).get("name") == job_name]
    pod_matches = []
    for item in pods:
        if not isinstance(item, Mapping):
            continue
        metadata = item.get("metadata", {})
        if any(
            isinstance(owner, Mapping) and owner.get("kind") == "Job" and owner.get("name") == job_name
            for owner in metadata.get("ownerReferences", [])
        ):
            pod_matches.append(item)
    if len(job_matches) > 1 or len(pod_matches) > 1:
        raise TeardownError("failure evidence has duplicate B4 Job or Pod identities")
    job_uid = job_matches[0].get("metadata", {}).get("uid") if job_matches else None
    pod_uid = pod_matches[0].get("metadata", {}).get("uid") if pod_matches else None
    return job_uid, pod_uid


def _live_acceptance_job(executor: CommandExecutor, identity: RunIdentity) -> tuple[str | None, str | None]:
    name = f"it4931-acceptance-{identity.run_id}"
    try:
        job_result = executor.run(kubectl(identity, "--namespace", identity.namespace, "get", "job", name, "-o", "json"), timeout=30)
    except Exception as exc:
        raise TeardownError(f"B4 Job identity query failed ({type(exc).__name__})") from exc
    if job_result.returncode != 0:
        # Kubernetes NotFound is a valid state only when the scoped object was not created.
        if "NotFound" in (job_result.stderr or "") or "not found" in (job_result.stderr or "").lower():
            return None, None
        raise TeardownError("B4 Job identity could not be verified")
    try:
        job = json.loads(job_result.stdout)
    except json.JSONDecodeError as exc:
        raise TeardownError("live B4 Job identity is malformed") from exc
    job_meta = job.get("metadata", {})
    pods_payload = _json_output(
        _checked(executor, kubectl(identity, "--namespace", identity.namespace, "get", "pods", "-l", f"job-name={name}", "-o", "json")),
        "B4 child Pod inventory",
    )
    pods = pods_payload.get("items")
    if not isinstance(pods, list) or len(pods) > 1:
        raise TeardownError("live B4 Job has an invalid child Pod inventory")
    if not pods:
        return job_meta.get("uid"), None
    pod_meta = pods[0].get("metadata", {})
    owners = pod_meta.get("ownerReferences", [])
    if (
        not job_meta.get("uid") or not pod_meta.get("uid") or len(owners) != 1
        or owners[0].get("kind") != "Job" or owners[0].get("name") != name
        or owners[0].get("uid") != job_meta["uid"] or owners[0].get("controller") is not True
    ):
        raise TeardownError("live B4 Job owner chain does not match the run identity")
    return job_meta["uid"], pod_meta["uid"]


def _verify_acceptance_artifacts(document: Mapping[str, Any]) -> None:
    for path_field, digest_field in (
        ("task_result_path", "task_result_file_sha256"),
        ("job_log_path", "job_log_file_sha256"),
    ):
        path_value, expected = document.get(path_field), document.get(digest_field)
        if not isinstance(path_value, str) or not isinstance(expected, str) or not _SHA256.fullmatch(expected):
            raise TeardownError("B4 receipt is missing a sanitized output artifact identity")
        source = Path(path_value)
        if source.is_symlink():
            raise TeardownError("B4 acceptance evidence artifact must not be a symbolic link")
        try:
            path = source.resolve(strict=True)
            actual = _sha256_bytes(path.read_bytes())
        except OSError as exc:
            raise TeardownError("B4 acceptance evidence artifact is unavailable") from exc
        if actual != expected or path.stat().st_mode & 0o077:
            raise TeardownError("B4 acceptance evidence artifact changed or is not private")


def _validate_acceptance_checks(document: Mapping[str, Any]) -> None:
    checks = document.get("checks")
    if not isinstance(checks, list):
        raise TeardownError("B4 receipt has no acceptance scenario inventory")
    names = [item.get("name") for item in checks if isinstance(item, Mapping)]
    if len(names) != len(ACCEPTANCE_SCENARIOS) or set(names) != ACCEPTANCE_SCENARIOS:
        raise TeardownError("B4 receipt scenario inventory is incomplete or duplicated")
    if document.get("status") in {"fixture_runtime_pass", "full_data_runtime_pass"} and any(
        item.get("status") != "passed" or not isinstance(item.get("evidence_sha256"), str)
        or not _SHA256.fullmatch(item["evidence_sha256"])
        for item in checks
    ):
        raise TeardownError("B4 passing receipt contains a missing or invalid runtime scenario")


def teardown_run(
    executor: CommandExecutor,
    identity: RunIdentity,
    lock: Mapping[str, Any],
    *,
    b2_receipt: Path | str | Mapping[str, Any],
    b3_receipt: Path | str | Mapping[str, Any],
    acceptance_receipt: Path | str | Mapping[str, Any] | None = None,
    failure_evidence: Path | str | Mapping[str, Any] | None = None,
    teardown_receipt_path: Path | str,
    execute: bool = False,
) -> dict[str, Any]:
    """Delete only the exact run namespace and dedicated Minikube profile.

    The function is intentionally never called by B2, B3, or B4 execution. A
    caller must explicitly opt in after reviewing the concrete teardown plan.
    """
    if execute is not True:
        raise TeardownError("teardown requires explicit execute=True authorization")
    try:
        _validate_pipeline_lock(lock)
    except Exception as exc:
        raise TeardownError("teardown requires a complete B3 runtime lock") from exc
    b2_document, b2_sha = _read_receipt(b2_receipt, "B2")
    b3_document, b3_sha = _read_receipt(b3_receipt, "B3")
    acceptance_document = None
    if acceptance_receipt is not None:
        acceptance_document, acceptance_sha = _read_receipt(acceptance_receipt, "B4")
        if (
            acceptance_document.get("schema_version") != 1
            or acceptance_document.get("run_id") != identity.run_id
            or acceptance_document.get("profile") != identity.profile
            or acceptance_document.get("namespace") != identity.namespace
            or acceptance_document.get("b2_receipt_sha256") != b2_sha
            or acceptance_document.get("b3_receipt_sha256") != b3_sha
            or acceptance_document.get("lock_sha256") != _sha256_json(lock)
        ):
            raise TeardownError("B4 acceptance receipt does not bind the supplied run and B2/B3 receipts")
        if acceptance_document.get("status") not in {"fixture_runtime_pass", "full_data_runtime_pass", "failed", "incomplete_runtime_acceptance"}:
            raise TeardownError("B4 receipt scope cannot authorize teardown")
        if (
            acceptance_document.get("scope") not in {"fixture_runtime", "full_data_runtime"}
            or (acceptance_document.get("status") == "fixture_runtime_pass" and acceptance_document.get("scope") != "fixture_runtime")
            or (acceptance_document.get("status") == "full_data_runtime_pass" and acceptance_document.get("scope") != "full_data_runtime")
        ):
            raise TeardownError("B4 runtime scope and status do not match")
        _validate_acceptance_checks(acceptance_document)
        if acceptance_document.get("status") in {"fixture_runtime_pass", "full_data_runtime_pass"}:
            _verify_acceptance_artifacts(acceptance_document)
    captured_failure = _failure_evidence(failure_evidence, identity=identity)
    if captured_failure is None:
        raise TeardownError("failure evidence must be captured and reviewed before any teardown")

    output = Path(teardown_receipt_path).resolve()
    if output.exists() or output.is_symlink() or not output.parent.is_dir():
        raise TeardownError("teardown receipt output must be a new file in an existing private directory")
    try:
        recorded_b2 = receipt_from_document(b2_document)
        live_b2 = collect_runtime_ownership_receipt(executor, identity, lock, execute=True)
        assert_receipt_matches_lock_and_live(recorded_b2, live_b2, lock, identity)
        recorded_b3 = pipeline_receipt_from_document(b3_document)
        live_b3 = collect_pipeline_receipt(executor, identity, lock, execute=True, include_b4_acceptance=True)
        assert_pipeline_receipt_matches_live(recorded_b3, live_b3, identity, lock)
    except Exception as exc:
        raise TeardownError("fresh B2/B3 receipts do not match live ownership; no resources were deleted") from exc
    if acceptance_document is not None and (
        acceptance_document.get("cluster_uid") != live_b3.cluster_uid
        or acceptance_document.get("namespace_uid") != live_b3.namespace_uid
    ):
        raise TeardownError("B4 acceptance receipt belongs to a different live cluster or namespace")

    live_job_uid, live_pod_uid = _live_acceptance_job(executor, identity)
    if live_job_uid is not None:
        if acceptance_document is not None:
            expected = (acceptance_document.get("acceptance_job_uid"), acceptance_document.get("acceptance_pod_uid"))
        else:
            assert captured_failure is not None
            expected = _acceptance_job_identity_from_failure(captured_failure[0], identity)
        if expected != (live_job_uid, live_pod_uid):
            raise TeardownError("B4 Job or Pod UID differs from its recorded acceptance/failure evidence")
    elif acceptance_document is not None and acceptance_document.get("acceptance_job_uid"):
        raise TeardownError("recorded B4 Job is missing from the live run namespace")

    actions = [
        ["kubectl", "--context", identity.profile, "delete", "namespace", identity.namespace, "--wait=true", "--timeout=180s"],
        ["minikube", "-p", identity.profile, "delete"],
    ]
    started = datetime.now(timezone.utc).isoformat()
    completed = []
    try:
        for index, command in enumerate(actions, start=1):
            result = executor.run(command, timeout=240)
            if result.returncode != 0:
                raise TeardownError(f"scoped teardown action {index} failed ({Path(command[0]).name})")
            completed.append({"action": index, "executable": Path(command[0]).name, "exit_code": result.returncode})
    except Exception as exc:
        partial = {
            "schema_version": 1, "status": "partial_failure", "run_id": identity.run_id,
            "profile": identity.profile, "namespace": identity.namespace,
            "completed_actions": completed, "failure_type": type(exc).__name__,
            "b2_receipt_sha256": b2_sha, "b3_receipt_sha256": b3_sha,
            "acceptance_receipt_sha256": acceptance_sha if acceptance_document is not None else None,
            "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        output.write_text(json.dumps(partial, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        output.chmod(0o600)
        if isinstance(exc, TeardownError):
            raise
        raise TeardownError(f"scoped teardown stopped after {len(completed)} action(s) ({type(exc).__name__})") from exc

    receipt = {
        "schema_version": 1, "status": "deleted", "run_id": identity.run_id,
        "profile": identity.profile, "namespace": identity.namespace,
        "started_at_utc": started, "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "completed_actions": completed, "b2_receipt_sha256": b2_sha, "b3_receipt_sha256": b3_sha,
        "acceptance_receipt_sha256": acceptance_sha if acceptance_document is not None else None,
        "failure_evidence_sha256": captured_failure[1] if captured_failure is not None else None,
    }
    output.write_text(json.dumps(receipt, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    output.chmod(0o600)
    return receipt
