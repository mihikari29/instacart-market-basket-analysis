"""Explicit B4 teardown guarded by fresh B2/B3 identity comparisons."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import re
from typing import Any, Mapping

from .acceptance import ACCEPTANCE_SCENARIOS, _read_receipt, _sha256_bytes, _sha256_json
from .ownership import PROTECTED_NAMESPACES, RunIdentity
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
_NAMESPACE_SYSTEM_OBJECTS = {
    ("ServiceAccount", "default"),
    ("ConfigMap", "kube-root-ca.crt"),
}


def _receipt_object_map(*receipts) -> dict[tuple[str, str], tuple[str, dict[str, str], tuple]]:
    expected: dict[tuple[str, str], tuple[str, dict[str, str], tuple]] = {}
    for receipt in receipts:
        for item in receipt.objects:
            key = (item.kind, item.name)
            owners = tuple(
                (owner.kind, owner.name, owner.uid, owner.controller)
                for owner in item.owner_references
            )
            identity = (item.uid, dict(item.labels), owners)
            previous = expected.setdefault(key, identity)
            if previous != identity:
                raise TeardownError(f"B2/B3 receipts disagree about {item.kind}/{item.name}")
    return expected


def _json_inventory(executor: CommandExecutor, identity: RunIdentity, *args: str, description: str) -> dict[str, Any]:
    try:
        payload = _json_output(_checked(executor, kubectl(identity, *args)), description)
    except Exception as exc:
        raise TeardownError(f"{description} could not be collected") from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
        raise TeardownError(f"{description} is malformed")
    return payload


def _validate_complete_profile_inventory(
    executor: CommandExecutor,
    identity: RunIdentity,
    b2_receipt,
    b3_receipt,
    *,
    acceptance_job_uid: str | None,
    acceptance_pod_uid: str | None,
) -> None:
    """Compare every discoverable run-namespace object and bound volume before deletion."""
    namespaces = _json_inventory(executor, identity, "get", "namespaces", "-o", "json", description="profile namespace inventory")
    namespace_names = {
        item.get("metadata", {}).get("name")
        for item in namespaces["items"]
        if isinstance(item, Mapping) and isinstance(item.get("metadata"), Mapping)
    }
    allowed_namespaces = set(PROTECTED_NAMESPACES) | {identity.namespace}
    if namespace_names != allowed_namespaces:
        raise TeardownError("dedicated profile contains an unexpected or missing namespace")
    run_namespace = [
        item for item in namespaces["items"]
        if isinstance(item, Mapping)
        and isinstance(item.get("metadata"), Mapping)
        and item["metadata"].get("name") == identity.namespace
    ]
    if len(run_namespace) != 1:
        raise TeardownError("run namespace identity is missing or duplicated")
    namespace_metadata = run_namespace[0]["metadata"]
    if not isinstance(namespace_metadata.get("uid"), str) or not namespace_metadata.get("uid"):
        raise TeardownError("run namespace UID is missing")
    if namespace_metadata["uid"] != b2_receipt.namespace_uid or namespace_metadata["uid"] != b3_receipt.namespace_uid:
        raise TeardownError("live run namespace UID differs from the B2/B3 receipts")
    labels = namespace_metadata.get("labels", {})
    if not isinstance(labels, Mapping) or any(labels.get(key) != value for key, value in identity.labels.items()):
        raise TeardownError("run namespace ownership labels do not match")

    expected = _receipt_object_map(b2_receipt, b3_receipt)
    acceptance_job_name = f"it4931-acceptance-{identity.run_id}"
    acceptance_job_labels = {
        **identity.labels,
        "app.kubernetes.io/name": "pipeline-task",
        "app.kubernetes.io/part-of": "instacart-k8s-demo",
        "it4931.instacart.edu/component": "pipeline",
        "it4931.instacart.edu/step": "acceptance",
    }
    if acceptance_job_uid is not None:
        expected[("Job", acceptance_job_name)] = (
            acceptance_job_uid,
            acceptance_job_labels,
            (),
        )

    # Discovery keeps this inventory exhaustive when a new namespaced API kind
    # is introduced. A failed discovery or list request blocks teardown.
    try:
        discovered = _checked(
            executor,
            kubectl(identity, "api-resources", "--verbs=list", "--namespaced=true", "-o", "name"),
        ).stdout
    except Exception as exc:
        raise TeardownError("namespaced API resource discovery failed") from exc
    resource_names = sorted({line.strip() for line in discovered.splitlines() if line.strip()})
    if not resource_names or any(any(char.isspace() for char in name) or "/" in name for name in resource_names):
        raise TeardownError("namespaced API resource discovery returned an invalid inventory")

    live: dict[tuple[str, str], Mapping[str, Any]] = {}
    for resource_name in resource_names:
        payload = _json_inventory(
            executor,
            identity,
            "--namespace", identity.namespace, "get", resource_name, "-o", "json",
            description=f"unfiltered {resource_name} inventory",
        )
        for item in payload["items"]:
            if not isinstance(item, Mapping) or not isinstance(item.get("metadata"), Mapping):
                raise TeardownError("unfiltered run namespace inventory contains a malformed object")
            metadata = item["metadata"]
            kind, name = item.get("kind"), metadata.get("name")
            uid, object_namespace = metadata.get("uid"), metadata.get("namespace")
            if not all(isinstance(value, str) and value for value in (kind, name, uid)):
                raise TeardownError("unfiltered run namespace inventory is missing kind, name, or UID")
            if object_namespace not in (None, identity.namespace):
                raise TeardownError("unfiltered run namespace inventory returned an object from another namespace")
            if (kind, name) in _NAMESPACE_SYSTEM_OBJECTS:
                continue
            key = (kind, name)
            previous = live.get(key)
            if previous is not None:
                left = (previous["metadata"].get("uid"), previous["metadata"].get("labels", {}), previous["metadata"].get("ownerReferences", []))
                right = (uid, metadata.get("labels", {}), metadata.get("ownerReferences", []))
                if left != right:
                    raise TeardownError(f"API aliases disagree about {kind}/{name}")
                continue
            live[key] = item

    derived: set[tuple[str, str]] = set()
    approved_services = {
        name: record for (kind, name), record in expected.items() if kind == "Service"
    }
    authorized_uids = {record[0] for record in expected.values()} | {
        namespace_metadata["uid"],
        *(storage.locality_node_uid for storage in (b2_receipt.storage,)),
        *(storage.node_uid for storage in b3_receipt.storage),
    }
    for key, item in live.items():
        kind, name = key
        metadata = item["metadata"]
        if kind == "Event":
            reference = item.get("regarding") or item.get("involvedObject") or {}
            if not isinstance(reference, Mapping) or reference.get("uid") not in authorized_uids:
                raise TeardownError("run namespace contains an event for an unowned object")
            derived.add(key)
            continue
        if kind == "ControllerRevision":
            owners = metadata.get("ownerReferences", [])
            if not isinstance(owners, list) or len(owners) != 1 or not isinstance(owners[0], Mapping):
                raise TeardownError("ControllerRevision has no unique StatefulSet owner")
            owner = owners[0]
            owner_name = owner.get("name")
            owner_identity = expected.get(("StatefulSet", owner_name)) if isinstance(owner_name, str) else None
            labels = metadata.get("labels", {})
            if (
                owner.get("kind") != "StatefulSet"
                or owner.get("uid") != (owner_identity[0] if owner_identity else None)
                or owner.get("controller") is not True
                or not isinstance(labels, Mapping)
                or any(labels.get(label) != value for label, value in identity.labels.items())
            ):
                raise TeardownError("ControllerRevision owner chain differs from an authorized StatefulSet")
            derived.add(key)
            continue
        if kind in {"Endpoints", "EndpointSlice"}:
            object_labels = metadata.get("labels", {})
            service_name = (
                name if kind == "Endpoints"
                else object_labels.get("kubernetes.io/service-name")
                if isinstance(object_labels, Mapping)
                else None
            )
            service_identity = approved_services.get(service_name) if isinstance(service_name, str) else None
            service_item = live.get(("Service", service_name)) if isinstance(service_name, str) else None
            if (
                service_identity is None
                or service_item is None
                or service_item["metadata"].get("uid") != service_identity[0]
                or service_item["metadata"].get("labels") != service_identity[1]
            ):
                raise TeardownError(f"{kind}/{name} does not resolve to an approved live Service")
            owners = metadata.get("ownerReferences", [])
            if not isinstance(owners, list) or any(not isinstance(owner, Mapping) for owner in owners):
                raise TeardownError(f"{kind}/{name} has malformed Service owner references")
            if kind == "EndpointSlice" and (
                not isinstance(object_labels, Mapping)
                or object_labels.get("kubernetes.io/service-name") != service_name
            ):
                raise TeardownError(f"EndpointSlice/{name} has no verifiable Service label")
            # Core Endpoints may legitimately have no ownerReferences; its
            # same-name Service in the exact namespace is the API identity
            # association. EndpointSlices are always controller-owned.
            if owners:
                if (
                    len(owners) != 1
                    or owners[0].get("kind") != "Service"
                    or owners[0].get("name") != service_name
                    or owners[0].get("uid") != service_identity[0]
                    or owners[0].get("controller") is not True
                ):
                    raise TeardownError(f"{kind}/{name} owner chain differs from its approved Service")
            elif kind == "EndpointSlice":
                raise TeardownError(f"EndpointSlice/{name} has no controller Service owner")
            derived.add(key)

    if acceptance_pod_uid is not None:
        matches = [
            item for (kind, _), item in live.items()
            if kind == "Pod" and item["metadata"].get("uid") == acceptance_pod_uid
        ]
        if len(matches) != 1:
            raise TeardownError("B4 Pod UID is missing or duplicated in the complete inventory")
        owners = matches[0]["metadata"].get("ownerReferences", [])
        if (
            not isinstance(owners, list) or len(owners) != 1 or not isinstance(owners[0], Mapping)
            or owners[0].get("kind") != "Job" or owners[0].get("name") != acceptance_job_name
            or owners[0].get("uid") != acceptance_job_uid or owners[0].get("controller") is not True
        ):
            raise TeardownError("B4 Pod owner chain differs from its recorded Job")
        pod = matches[0]
        pod_labels = pod["metadata"].get("labels", {})
        if not isinstance(pod_labels, Mapping) or any(
            pod_labels.get(key) != value for key, value in {
                **identity.labels,
                "app.kubernetes.io/name": "pipeline-task",
                "it4931.instacart.edu/component": "pipeline",
                "it4931.instacart.edu/role": "pipeline",
                "it4931.instacart.edu/step": "acceptance",
            }.items()
        ):
            raise TeardownError("B4 Pod ownership labels do not match")
        pod_name = pod["metadata"].get("name")
        if not isinstance(pod_name, str) or not pod_name.startswith(acceptance_job_name + "-"):
            raise TeardownError("B4 Pod name is outside its run-scoped Job")
        expected[("Pod", pod_name)] = (
            acceptance_pod_uid,
            {
                **identity.labels,
                "app.kubernetes.io/name": "pipeline-task",
                "it4931.instacart.edu/component": "pipeline",
                "it4931.instacart.edu/role": "pipeline",
                "it4931.instacart.edu/step": "acceptance",
            },
            (("Job", acceptance_job_name, acceptance_job_uid, True),),
        )

    observed_expected = set(live).difference(derived)
    if observed_expected != set(expected):
        raise TeardownError("unfiltered run namespace inventory is incomplete or contains a foreign resource")
    for key, (expected_uid, expected_labels, expected_owners) in expected.items():
        item = live.get(key)
        if item is None:
            raise TeardownError(f"authorized resource {key[0]}/{key[1]} is missing")
        metadata = item["metadata"]
        labels = metadata.get("labels", {})
        owners = metadata.get("ownerReferences", [])
        owner_identities = tuple(
            (owner.get("kind"), owner.get("name"), owner.get("uid"), owner.get("controller") is True)
            for owner in owners
            if isinstance(owner, Mapping)
        ) if isinstance(owners, list) else ()
        if metadata.get("uid") != expected_uid or labels != expected_labels or owner_identities != expected_owners:
            raise TeardownError(f"live identity for {key[0]}/{key[1]} differs from its receipt")

    expected_storage = {}
    for storage in (b2_receipt.storage, *b3_receipt.storage):
        identity_value = (storage.volume_uid, storage.claim_name, storage.claim_uid)
        if storage.volume_name in expected_storage or identity_value[0] in {
            value[0] for value in expected_storage.values()
        }:
            raise TeardownError("B2/B3 storage receipts contain duplicate PV identities")
        expected_storage[storage.volume_name] = identity_value
    pv_payload = _json_inventory(executor, identity, "get", "pv", "-o", "json", description="profile PV inventory")
    pv_items = pv_payload["items"]
    observed_pvs = {}
    for volume in pv_items:
        if not isinstance(volume, Mapping) or not isinstance(volume.get("metadata"), Mapping):
            raise TeardownError("profile PV inventory contains a malformed volume")
        metadata = volume["metadata"]
        spec = volume.get("spec", {})
        claim_ref = spec.get("claimRef", {}) if isinstance(spec, Mapping) else {}
        name, uid = metadata.get("name"), metadata.get("uid")
        if not isinstance(name, str) or not isinstance(uid, str):
            raise TeardownError("profile PV inventory is missing name or UID")
        if name in observed_pvs:
            raise TeardownError("profile PV inventory contains duplicate names")
        observed_pvs[name] = (uid, claim_ref.get("namespace"), claim_ref.get("name"), claim_ref.get("uid"))
    if set(observed_pvs) != set(expected_storage):
        raise TeardownError("profile contains an unexpected or missing PersistentVolume")
    for name, (expected_uid, claim_name, claim_uid) in expected_storage.items():
        if observed_pvs[name] != (expected_uid, identity.namespace, claim_name, claim_uid):
            raise TeardownError(f"PV binding for {name} differs from its claim receipt")
    expected_classes = {
        storage.storage_class: storage.storage_class_uid
        for storage in (b2_receipt.storage, *b3_receipt.storage)
    }
    if len(expected_classes) != 1:
        raise TeardownError("B2/B3 storage receipts disagree about the profile StorageClass")
    class_payload = _json_inventory(
        executor, identity, "get", "storageclasses", "-o", "json", description="profile StorageClass inventory"
    )
    observed_classes = {}
    for storage_class in class_payload["items"]:
        if not isinstance(storage_class, Mapping) or not isinstance(storage_class.get("metadata"), Mapping):
            raise TeardownError("profile StorageClass inventory contains a malformed object")
        metadata = storage_class["metadata"]
        name, uid = metadata.get("name"), metadata.get("uid")
        if not isinstance(name, str) or not isinstance(uid, str):
            raise TeardownError("profile StorageClass inventory is missing name or UID")
        observed_classes[name] = uid
    if observed_classes != expected_classes:
        raise TeardownError("profile contains an unexpected or stale StorageClass")


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
    if acceptance_document is not None:
        expected_acceptance = (
            acceptance_document.get("acceptance_job_uid"),
            acceptance_document.get("acceptance_pod_uid"),
        )
    else:
        assert captured_failure is not None
        expected_acceptance = _acceptance_job_identity_from_failure(captured_failure[0], identity)
    if expected_acceptance != (live_job_uid, live_pod_uid):
        raise TeardownError("B4 Job or Pod UID differs from its recorded acceptance/failure evidence")
    try:
        _validate_complete_profile_inventory(
            executor,
            identity,
            recorded_b2,
            recorded_b3,
            acceptance_job_uid=live_job_uid,
            acceptance_pod_uid=live_pod_uid,
        )
    except Exception as exc:
        raise TeardownError("complete profile inventory is not exclusively owned; no resources were deleted") from exc

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
