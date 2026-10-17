"""Offline contracts for B4 acceptance evidence and destructive-action gates."""

import json
import hashlib
from dataclasses import replace
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import yaml

from scripts.k8s.acceptance import (
    ACCEPTANCE_SCENARIOS,
    AcceptanceError,
    build_acceptance_receipt,
    collect_failure_evidence,
    render_acceptance_job,
    run_acceptance,
)
from scripts.k8s.ownership import RunIdentity
from scripts.k8s.runtime import CommandResult
from scripts.k8s.receipts import assert_receipt_matches_lock_and_live, validate_collected_receipt
from scripts.k8s.pipeline_receipts import assert_pipeline_receipt_matches_live, validate_pipeline_receipt
from scripts.k8s.teardown import (
    TeardownError,
    _failure_evidence,
    _verify_acceptance_artifacts,
    _validate_complete_profile_inventory,
    teardown_run,
)
from tests.test_k8s_ownership import _complete_lock, _valid_runtime_receipt
from tests.test_k8s_pipeline import _complete_b3_lock, _valid_pipeline_receipt


class InertExecutor:
    def __init__(self):
        self.calls = []

    def run(self, argv, *, timeout, input_text=None):
        self.calls.append(list(argv))
        raise AssertionError("offline acceptance tests must not contact Kubernetes")


class FakeProfileInventoryExecutor:
    def __init__(self, identity, *, foreign=None, extra_namespace=False, stale_uid=False):
        self.identity = identity
        self.foreign = foreign
        self.extra_namespace = extra_namespace
        self.stale_uid = stale_uid
        self.calls = []

    def run(self, argv, *, timeout, input_text=None):
        self.calls.append(list(argv))
        if argv[3:5] == ["get", "namespaces"]:
            names = ["default", "kube-system", "kube-public", "kube-node-lease", self.identity.namespace]
            if self.extra_namespace:
                names.append("foreign-namespace")
            items = [{"metadata": {"name": name}} for name in names if name != self.identity.namespace]
            items.append({"metadata": {
                "name": self.identity.namespace,
                "uid": "namespace-uid",
                "labels": self.identity.labels,
            }})
            return CommandResult(0, json.dumps({"items": items}))
        if "api-resources" in argv:
            return CommandResult(0, "configmaps\n")
        if "get" in argv and "configmaps" in argv:
            owned = {
                "apiVersion": "v1", "kind": "ConfigMap",
                "metadata": {
                    "name": "owned", "namespace": self.identity.namespace,
                    "uid": "stale-uid" if self.stale_uid else "owned-uid",
                    "labels": self.identity.labels,
                },
            }
            items = [owned]
            if self.foreign is not None:
                items.append(self.foreign)
            return CommandResult(0, json.dumps({"items": items}))
        if argv[3:6] == ["get", "pv", "-o"]:
            return CommandResult(0, json.dumps({"items": [{
                "metadata": {"name": "pv-owned", "uid": "pv-uid"},
                "spec": {"claimRef": {
                    "namespace": self.identity.namespace, "name": "claim-owned", "uid": "claim-uid",
                }},
            }]}))
        if argv[3:6] == ["get", "storageclasses", "-o"]:
            return CommandResult(0, json.dumps({"items": [{
                "metadata": {"name": "standard", "uid": "sc-uid"},
            }]}))
        raise AssertionError(f"unexpected fake inventory command: {argv}")


class ReceiptInventoryExecutor:
    """Unfiltered Kubernetes API fake populated from genuine B2/B3 receipts."""

    RESOURCE_KINDS = {
        "configmaps": "ConfigMap",
        "controllerrevisions.apps": "ControllerRevision",
        "endpoints": "Endpoints",
        "endpointslices.discovery.k8s.io": "EndpointSlice",
        "jobs.batch": "Job",
        "limitranges": "LimitRange",
        "networkpolicies.networking.k8s.io": "NetworkPolicy",
        "persistentvolumeclaims": "PersistentVolumeClaim",
        "pods": "Pod",
        "replicasets.apps": "ReplicaSet",
        "resourcequotas": "ResourceQuota",
        "secrets": "Secret",
        "serviceaccounts": "ServiceAccount",
        "services": "Service",
        "statefulsets.apps": "StatefulSet",
        "deployments.apps": "Deployment",
    }

    def __init__(
        self, identity, b2, b3, *, derived_mutation=None, live_mutation=None,
        extra_namespace=False, acceptance_job_uid=None, acceptance_pod_uid=None,
        controller_revision_owner_uid=None, pv_mutation=None,
    ):
        self.identity = identity
        self.calls = []
        self.extra_namespace = extra_namespace
        self.resource_names = list(self.RESOURCE_KINDS)
        self.resources = {name: [] for name in self.resource_names}
        self.pvs = []
        self.storage_classes = {}
        self.service_uids = {}
        for receipt in (b2, b3):
            for record in receipt.objects:
                owners = [
                    {"kind": owner.kind, "name": owner.name, "uid": owner.uid, "controller": owner.controller}
                    for owner in record.owner_references
                ]
                item = {"kind": record.kind, "metadata": {
                    "name": record.name, "namespace": record.namespace, "uid": record.uid,
                    "labels": dict(record.labels), "ownerReferences": owners,
                }}
                resource = next(
                    name for name, kind in self.RESOURCE_KINDS.items() if kind == record.kind
                )
                self.resources[resource].append(item)
                if record.kind == "Service":
                    self.service_uids[record.name] = record.uid
            storage_records = (receipt.storage,) if hasattr(receipt.storage, "volume_name") else receipt.storage
            for storage in storage_records:
                self.pvs.append({
                    "metadata": {"name": storage.volume_name, "uid": storage.volume_uid},
                    "spec": {"claimRef": {
                        "namespace": identity.namespace, "name": storage.claim_name, "uid": storage.claim_uid,
                    }},
                })
                self.storage_classes[storage.storage_class] = storage.storage_class_uid

        mongo_statefulset_uid = next(
            record.uid for record in b2.objects
            if (record.kind, record.name) == ("StatefulSet", "module4d-mongo")
        )
        self.resources["controllerrevisions.apps"].append({"kind": "ControllerRevision", "metadata": {
            "name": "module4d-mongo-abc123", "namespace": identity.namespace,
            "uid": "mongo-controller-revision-uid", "labels": identity.labels,
            "ownerReferences": [{
                "kind": "StatefulSet", "name": "module4d-mongo",
                "uid": controller_revision_owner_uid or mongo_statefulset_uid, "controller": True,
            }],
        }})

        if acceptance_job_uid is not None:
            job_name = f"it4931-acceptance-{identity.run_id}"
            job_labels = {
                **identity.labels,
                "app.kubernetes.io/name": "pipeline-task",
                "app.kubernetes.io/part-of": "instacart-k8s-demo",
                "it4931.instacart.edu/component": "pipeline",
                "it4931.instacart.edu/step": "acceptance",
            }
            self.resources["jobs.batch"].append({"kind": "Job", "metadata": {
                "name": job_name, "namespace": identity.namespace, "uid": acceptance_job_uid,
                "labels": job_labels, "ownerReferences": [],
            }})
            if acceptance_pod_uid is not None:
                self.resources["pods"].append({"kind": "Pod", "metadata": {
                    "name": job_name + "-pod", "namespace": identity.namespace, "uid": acceptance_pod_uid,
                    "labels": {
                        **identity.labels,
                        "app.kubernetes.io/name": "pipeline-task",
                        "it4931.instacart.edu/component": "pipeline",
                        "it4931.instacart.edu/role": "pipeline",
                        "it4931.instacart.edu/step": "acceptance",
                    },
                    "ownerReferences": [{
                        "kind": "Job", "name": job_name, "uid": acceptance_job_uid, "controller": True,
                    }],
                }})

        service_name = "module4d-api"
        service_uid = self.service_uids[service_name]
        endpoint_owners = []
        slice_owners = [{"kind": "Service", "name": service_name, "uid": service_uid, "controller": True}]
        if derived_mutation == "foreign_endpoints_uid":
            endpoint_owners = [{"kind": "Service", "name": service_name, "uid": "foreign-service-uid", "controller": True}]
        elif derived_mutation == "foreign_endpoints_kind":
            endpoint_owners = [{"kind": "Deployment", "name": service_name, "uid": service_uid, "controller": True}]
        if derived_mutation == "foreign_slice_uid":
            slice_owners = [{"kind": "Service", "name": service_name, "uid": "foreign-service-uid", "controller": True}]
        elif derived_mutation == "slice_without_owner":
            slice_owners = []
        self.resources["endpoints"].append({"kind": "Endpoints", "metadata": {
            "name": service_name, "namespace": identity.namespace, "uid": "api-endpoints-uid",
            "labels": {}, "ownerReferences": endpoint_owners,
        }})
        slice_name = "module4d-api-abcde"
        if derived_mutation == "unverifiable_slice_label":
            slice_labels = {"kubernetes.io/service-name": "missing-service"}
        else:
            slice_labels = {"kubernetes.io/service-name": service_name}
        self.resources["endpointslices.discovery.k8s.io"].append({"kind": "EndpointSlice", "metadata": {
            "name": slice_name, "namespace": identity.namespace, "uid": "api-slice-uid",
            "labels": slice_labels, "ownerReferences": slice_owners,
        }})
        if derived_mutation == "unverifiable_endpoints":
            self.resources["endpoints"].append({"kind": "Endpoints", "metadata": {
                "name": "foreign-endpoints", "namespace": identity.namespace, "uid": "foreign-endpoints-uid",
                "labels": {}, "ownerReferences": [],
            }})
        if live_mutation is not None:
            kind, name, field, value = live_mutation
            for items in self.resources.values():
                for item in items:
                    metadata = item["metadata"]
                    if (item["kind"], metadata["name"]) == (kind, name):
                        metadata[field] = value
        if pv_mutation is not None:
            self.pvs[0]["spec"]["claimRef"][pv_mutation[0]] = pv_mutation[1]
        self.resource_names = [name for name in self.resource_names if self.resources[name]]

    def run(self, argv, *, timeout, input_text=None):
        self.calls.append(list(argv))
        if "api-resources" in argv:
            return CommandResult(0, "\n".join(self.resource_names) + "\n")
        if argv[3:5] == ["get", "namespaces"]:
            names = ["default", "kube-system", "kube-public", "kube-node-lease"]
            if self.extra_namespace:
                names.append("foreign-namespace")
            items = [{"metadata": {"name": name}} for name in names]
            items.append({"metadata": {
                "name": self.identity.namespace, "uid": "b2-namespace-uid", "labels": self.identity.labels,
            }})
            return CommandResult(0, json.dumps({"items": items}))
        if "get" in argv and "-o" in argv:
            resource_name = argv[argv.index("get") + 1]
            if resource_name == "pv":
                items = self.pvs
            elif resource_name == "storageclasses":
                items = [
                    {"metadata": {"name": name, "uid": uid}}
                    for name, uid in self.storage_classes.items()
                ]
            else:
                items = self.resources.get(resource_name)
                if items is None:
                    raise AssertionError(f"unexpected fake resource query: {resource_name}")
            return CommandResult(0, json.dumps({"items": items}))
        raise AssertionError(f"unexpected fake inventory command: {argv}")


def _checks(status="passed"):
    return [
        {"name": name, "status": status, "evidence_sha256": "a" * 64}
        for name in sorted(ACCEPTANCE_SCENARIOS)
    ]


class AcceptanceContractTests(unittest.TestCase):
    def setUp(self):
        self.identity = RunIdentity("0123456789ab")

    def test_offline_scope_never_reports_runtime_pass(self):
        receipt = build_acceptance_receipt(identity=self.identity, scope="offline", checks=_checks())
        self.assertEqual(receipt["status"], "not_runtime_acceptance")
        self.assertIsNone(receipt["cluster_uid"])

    def test_fixture_and_full_data_runtime_scopes_remain_distinct(self):
        identity = {
            "cluster_uid": "cluster-uid", "namespace_uid": "namespace-uid",
            "acceptance_job_uid": "job-uid", "acceptance_pod_uid": "pod-uid",
            "lock_sha256": "b" * 64, "b2_receipt_sha256": "c" * 64,
            "b3_receipt_sha256": "d" * 64,
        }
        fixture = build_acceptance_receipt(
            identity=self.identity, scope="fixture_runtime", checks=_checks(), **identity,
        )
        full = build_acceptance_receipt(
            identity=self.identity, scope="full_data_runtime", checks=_checks(), **identity,
        )
        self.assertEqual(fixture["status"], "fixture_runtime_pass")
        self.assertEqual(full["status"], "full_data_runtime_pass")

    def test_missing_runtime_identity_cannot_turn_fixture_results_into_pass(self):
        receipt = build_acceptance_receipt(identity=self.identity, scope="fixture_runtime", checks=_checks())
        self.assertEqual(receipt["status"], "incomplete_runtime_acceptance")

    def test_failed_scenario_prevents_runtime_pass(self):
        checks = _checks()
        checks[0]["status"] = "failed"
        identity = {
            "cluster_uid": "cluster-uid", "namespace_uid": "namespace-uid",
            "acceptance_job_uid": "job-uid", "acceptance_pod_uid": "pod-uid",
            "lock_sha256": "b" * 64, "b2_receipt_sha256": "c" * 64,
            "b3_receipt_sha256": "d" * 64,
        }
        receipt = build_acceptance_receipt(
            identity=self.identity, scope="full_data_runtime", checks=checks, **identity,
        )
        self.assertEqual(receipt["status"], "failed")

    def test_acceptance_manifest_uses_locked_pipeline_image_and_run_identity(self):
        lock = _complete_b3_lock()
        identity = self.identity
        with tempfile.TemporaryDirectory() as temporary:
            pipeline = Path(temporary) / "pipeline"
            pipeline.mkdir(mode=0o700)
            (pipeline / "plan.json").write_text(json.dumps({
                "run_id": identity.run_id, "profile": identity.profile, "namespace": identity.namespace,
            }))
            path = render_acceptance_job(identity, lock, pipeline)
            job = yaml.safe_load(path.read_text(encoding="utf-8"))
        self.assertEqual(job["metadata"]["name"], f"it4931-acceptance-{identity.run_id}")
        self.assertEqual(job["metadata"]["namespace"], identity.namespace)
        self.assertEqual(
            job["spec"]["template"]["spec"]["containers"][0]["image"],
            lock["images"]["pipeline_application_image"]["reference"],
        )
        self.assertEqual(job["spec"]["template"]["spec"]["restartPolicy"], "Never")
        self.assertFalse(job["spec"]["template"]["spec"]["automountServiceAccountToken"])
        self.assertIn("module4d-mongo-publisher", json.dumps(job))
        self.assertNotIn("mongodb://", json.dumps(job))

    def test_runtime_acceptance_requires_execute_true_without_invoking_commands(self):
        executor = InertExecutor()
        with self.assertRaisesRegex(AcceptanceError, "execute=True"):
            run_acceptance(
                executor, self.identity, {}, mode="fixture", b2_receipt={}, b3_receipt={},
                pipeline_directory="/does/not/exist", acceptance_receipt_path="/does/not/exist.json",
            )
        self.assertEqual(executor.calls, [])

    def test_teardown_requires_execute_true_without_invoking_commands(self):
        executor = InertExecutor()
        with self.assertRaisesRegex(TeardownError, "execute=True"):
            teardown_run(
                executor, self.identity, {}, b2_receipt={}, b3_receipt={},
                teardown_receipt_path="/does/not/exist.json",
            )
        self.assertEqual(executor.calls, [])

    def test_failure_evidence_redacts_mongo_uri_and_records_only_run_scope(self):
        class EvidenceExecutor:
            def run(self, argv, *, timeout, input_text=None):
                self.assert_context = "--context" in argv and RunIdentity("0123456789ab").profile in argv
                return CommandResult(0, json.dumps({"items": [], "diagnostic": "mongodb://user:secret@mongo/db"}))

        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "failure.json"
            result = collect_failure_evidence(EvidenceExecutor(), self.identity, output, execute=True)
            encoded = output.read_text(encoding="utf-8")
        self.assertEqual(result["status"], "captured")
        self.assertEqual(result["pods"], 0)
        self.assertNotIn("user:secret", encoded)
        self.assertIn("mongodb://<redacted>", encoded)

    def test_teardown_rejects_unhashed_failure_evidence_and_changed_acceptance_artifacts(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            evidence = root / "failure.json"
            evidence.write_text(json.dumps({
                "run_id": self.identity.run_id,
                "profile": self.identity.profile,
                "namespace": self.identity.namespace,
            }))
            with self.assertRaisesRegex(TeardownError, "SHA-256"):
                _failure_evidence({"path": str(evidence), "sha256": "0" * 64}, identity=self.identity)

            task = root / "task.json"
            log = root / "job.log"
            task.write_text("{}\n")
            log.write_text("ok\n")
            task.chmod(0o600)
            log.chmod(0o600)
            receipt = {
                "task_result_path": str(task),
                "task_result_file_sha256": hashlib.sha256(task.read_bytes()).hexdigest(),
                "job_log_path": str(log),
                "job_log_file_sha256": hashlib.sha256(log.read_bytes()).hexdigest(),
            }
            _verify_acceptance_artifacts(receipt)
            log.write_text("changed\n")
            with self.assertRaisesRegex(TeardownError, "changed"):
                _verify_acceptance_artifacts(receipt)

    def _inventory_receipts(self):
        owned = SimpleNamespace(
            kind="ConfigMap", name="owned", uid="owned-uid", labels=self.identity.labels,
            owner_references=(),
        )
        storage = SimpleNamespace(
            volume_name="pv-owned", volume_uid="pv-uid", claim_name="claim-owned", claim_uid="claim-uid",
            storage_class="standard", storage_class_uid="sc-uid", locality_node_uid="node-uid",
        )
        return (
            SimpleNamespace(namespace_uid="namespace-uid", objects=(owned,), storage=storage),
            SimpleNamespace(namespace_uid="namespace-uid", objects=(), storage=()),
        )

    def test_unfiltered_teardown_inventory_accepts_exact_offline_fixture(self):
        b2, b3 = self._inventory_receipts()
        executor = FakeProfileInventoryExecutor(self.identity)

        _validate_complete_profile_inventory(
            executor, self.identity, b2, b3, acceptance_job_uid=None, acceptance_pod_uid=None,
        )

        self.assertTrue(executor.calls)
        self.assertFalse(any("delete" in call for call in executor.calls))

    def test_unfiltered_teardown_inventory_accepts_only_owned_statefulset_revisions(self):
        statefulset = SimpleNamespace(
            kind="StatefulSet", name="module4d-mongo", uid="mongo-sts-uid",
            labels=self.identity.labels, owner_references=(),
        )
        b2, b3 = self._inventory_receipts()
        b2.objects = (*b2.objects, statefulset)

        class RevisionExecutor(FakeProfileInventoryExecutor):
            def __init__(self, owner_uid):
                super().__init__(self_identity)
                self.owner_uid = owner_uid

            def run(self, argv, *, timeout, input_text=None):
                self.calls.append(list(argv))
                if "api-resources" in argv:
                    return CommandResult(0, "configmaps\nstatefulsets.apps\ncontrollerrevisions.apps\n")
                if "statefulsets.apps" in argv:
                    return CommandResult(0, json.dumps({"items": [{
                        "kind": "StatefulSet",
                        "metadata": {
                            "name": "module4d-mongo", "namespace": self.identity.namespace,
                            "uid": "mongo-sts-uid", "labels": self.identity.labels,
                        },
                    }]}))
                if "controllerrevisions.apps" in argv:
                    return CommandResult(0, json.dumps({"items": [{
                        "kind": "ControllerRevision",
                        "metadata": {
                            "name": "module4d-mongo-abc123", "namespace": self.identity.namespace,
                            "uid": "mongo-revision-uid", "labels": self.identity.labels,
                            "ownerReferences": [{
                                "kind": "StatefulSet", "name": "module4d-mongo",
                                "uid": self.owner_uid, "controller": True,
                            }],
                        },
                    }]}))
                return super().run(argv, timeout=timeout, input_text=input_text)

        self_identity = self.identity
        valid = RevisionExecutor("mongo-sts-uid")
        _validate_complete_profile_inventory(
            valid, self.identity, b2, b3, acceptance_job_uid=None, acceptance_pod_uid=None,
        )
        self.assertFalse(any("delete" in call for call in valid.calls))

        foreign = RevisionExecutor("foreign-sts-uid")
        with self.assertRaisesRegex(TeardownError, "owner chain"):
            _validate_complete_profile_inventory(
                foreign, self.identity, b2, b3, acceptance_job_uid=None, acceptance_pod_uid=None,
            )
        self.assertFalse(any("delete" in call for call in foreign.calls))

    def test_unfiltered_teardown_inventory_rejects_foreign_pipeline_or_unlabelled_resources(self):
        b2, b3 = self._inventory_receipts()
        for labels in (
            {"it4931.instacart.edu/component": "pipeline"},
            {},
        ):
            foreign = {
                "apiVersion": "v1", "kind": "ConfigMap",
                "metadata": {
                    "name": "foreign", "namespace": self.identity.namespace,
                    "uid": "foreign-uid", "labels": labels,
                },
            }
            executor = FakeProfileInventoryExecutor(self.identity, foreign=foreign)
            with self.subTest(labels=labels), self.assertRaisesRegex(TeardownError, "foreign resource"):
                _validate_complete_profile_inventory(
                    executor, self.identity, b2, b3, acceptance_job_uid=None, acceptance_pod_uid=None,
                )
            self.assertFalse(any("delete" in call for call in executor.calls))

    def test_unfiltered_teardown_inventory_rejects_extra_namespace_and_stale_uid(self):
        b2, b3 = self._inventory_receipts()
        for executor, message in (
            (FakeProfileInventoryExecutor(self.identity, extra_namespace=True), "unexpected or missing namespace"),
            (FakeProfileInventoryExecutor(self.identity, stale_uid=True), "differs from its receipt"),
        ):
            with self.subTest(message=message), self.assertRaisesRegex(TeardownError, message):
                _validate_complete_profile_inventory(
                    executor, self.identity, b2, b3, acceptance_job_uid=None, acceptance_pod_uid=None,
                )
            self.assertFalse(any("delete" in call for call in executor.calls))

    def test_teardown_orchestration_stops_at_foreign_inventory_before_delete(self):
        executor = InertExecutor()
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "teardown.json"
            with (
                patch("scripts.k8s.teardown._validate_pipeline_lock"),
                patch("scripts.k8s.teardown._read_receipt", side_effect=[({}, "a" * 64), ({}, "b" * 64)]),
                patch("scripts.k8s.teardown._failure_evidence", return_value=({}, "c" * 64)),
                patch("scripts.k8s.teardown.receipt_from_document", return_value=object()),
                patch("scripts.k8s.teardown.collect_runtime_ownership_receipt", return_value=object()),
                patch("scripts.k8s.teardown.assert_receipt_matches_lock_and_live"),
                patch("scripts.k8s.teardown.pipeline_receipt_from_document", return_value=object()),
                patch(
                    "scripts.k8s.teardown.collect_pipeline_receipt",
                    return_value=SimpleNamespace(cluster_uid="cluster-uid", namespace_uid="namespace-uid"),
                ),
                patch("scripts.k8s.teardown.assert_pipeline_receipt_matches_live"),
                patch("scripts.k8s.teardown._live_acceptance_job", return_value=(None, None)),
                patch("scripts.k8s.teardown._acceptance_job_identity_from_failure", return_value=(None, None)),
                patch(
                    "scripts.k8s.teardown._validate_complete_profile_inventory",
                    side_effect=TeardownError("foreign resource detected"),
                ) as validate_inventory,
            ):
                with self.assertRaisesRegex(TeardownError, "no resources were deleted"):
                    teardown_run(
                        executor,
                        self.identity,
                        {},
                        b2_receipt={},
                        b3_receipt={},
                        failure_evidence={},
                        teardown_receipt_path=output,
                        execute=True,
                    )

        validate_inventory.assert_called_once()
        self.assertEqual(executor.calls, [])

    def _genuine_b2_b3_receipts(self):
        b2_lock = _complete_lock()
        b3_lock = _complete_b3_lock()
        b2 = _valid_runtime_receipt(self.identity)
        b3 = _valid_pipeline_receipt(self.identity, b3_lock)
        b3 = replace(b3, namespace_uid=b2.namespace_uid)
        validate_collected_receipt(b2, b2_lock)
        assert_receipt_matches_lock_and_live(b2, b2, b2_lock, self.identity)
        validate_pipeline_receipt(self.identity, b3, b3_lock)
        assert_pipeline_receipt_matches_live(b3, b3, self.identity, b3_lock)
        return b2_lock, b3_lock, b2, b3

    def test_genuine_b2_to_b3_to_b4_inventory_preflight_passes(self):
        from scripts.k8s.pipeline_receipts import pipeline_receipt_document
        from scripts.k8s.receipts import receipt_document

        b2_lock, _b3_lock, b2, b3 = self._genuine_b2_b3_receipts()
        self.assertEqual(b2.namespace_uid, b3.namespace_uid)
        self.assertTrue(b3.storage)
        self.assertTrue(all(storage.node_uid for storage in b3.storage))
        b2_api_identities = {
            (item.kind, item.name): item.uid for item in b2.objects
            if item.kind in {"Deployment", "ReplicaSet", "Pod"}
            and (item.name == "module4d-api" or item.name.startswith("module4d-api-"))
        }
        self.assertEqual(set(b2_api_identities), {
            ("Deployment", "module4d-api"),
            ("ReplicaSet", "module4d-api-hash"),
            ("Pod", "module4d-api-hash-pod"),
        })
        self.assertFalse(any(
            item.kind in {"Deployment", "ReplicaSet", "Pod"}
            and (item.name == "module4d-api" or item.name.startswith("module4d-api-"))
            for item in b3.objects
        ))

        b2_doc = receipt_document(b2)
        b3_doc = pipeline_receipt_document(b3)
        canonical_sha = lambda value: hashlib.sha256(
            json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        job_uid, pod_uid = "b4-acceptance-job-uid", "b4-acceptance-pod-uid"
        b4 = build_acceptance_receipt(
            identity=self.identity,
            scope="fixture_runtime",
            checks=_checks(),
            cluster_uid=b2.cluster_uid,
            namespace_uid=b2.namespace_uid,
            acceptance_job_uid=job_uid,
            acceptance_pod_uid=pod_uid,
            lock_sha256=canonical_sha(_b3_lock),
            b2_receipt_sha256=canonical_sha(b2_doc),
            b3_receipt_sha256=canonical_sha(b3_doc),
        )
        self.assertEqual(b4["status"], "fixture_runtime_pass")
        executor = ReceiptInventoryExecutor(
            self.identity, b2, b3, acceptance_job_uid=job_uid, acceptance_pod_uid=pod_uid,
        )

        _validate_complete_profile_inventory(
            executor, self.identity, b2, b3,
            acceptance_job_uid=job_uid, acceptance_pod_uid=pod_uid,
        )

        self.assertFalse(any("delete" in call for call in executor.calls))
        self.assertFalse(any("-l" in call for call in executor.calls))

    def test_derived_network_inventory_requires_service_uid_ownership(self):
        _b2_lock, _b3_lock, b2, b3 = self._genuine_b2_b3_receipts()
        for mutation, expected in (
            ("foreign_slice_uid", "owner chain"),
            ("slice_without_owner", "no controller Service owner"),
            ("unverifiable_slice_label", "approved live Service"),
            ("foreign_endpoints_uid", "owner chain"),
            ("foreign_endpoints_kind", "owner chain"),
            ("unverifiable_endpoints", "approved live Service"),
        ):
            executor = ReceiptInventoryExecutor(
                self.identity, b2, b3, derived_mutation=mutation,
            )
            with self.subTest(mutation=mutation), self.assertRaisesRegex(TeardownError, expected):
                _validate_complete_profile_inventory(
                    executor, self.identity, b2, b3,
                    acceptance_job_uid=None, acceptance_pod_uid=None,
                )
            self.assertFalse(any("delete" in call for call in executor.calls))

    def test_complete_inventory_rejects_stale_and_foreign_resource_identities(self):
        _b2_lock, _b3_lock, b2, b3 = self._genuine_b2_b3_receipts()
        foreign_workload = {
            "kind": "Deployment", "metadata": {
                "name": "foreign-workload", "namespace": self.identity.namespace,
                "uid": "foreign-workload-uid", "labels": self.identity.labels,
            },
        }
        cases = (
            (ReceiptInventoryExecutor(self.identity, b2, b3, extra_namespace=True), "unexpected or missing namespace"),
            (ReceiptInventoryExecutor(
                self.identity, b2, b3,
                live_mutation=("Pod", "module4d-api-hash-pod", "uid", "stale-api-pod-uid"),
            ), "differs from its receipt"),
            (ReceiptInventoryExecutor(
                self.identity, b2, b3,
                live_mutation=("ReplicaSet", "module4d-api-hash", "uid", "stale-api-rs-uid"),
            ), "differs from its receipt"),
            (ReceiptInventoryExecutor(
                self.identity, b2, b3,
                live_mutation=("Deployment", "module4d-api", "uid", "stale-api-deployment-uid"),
            ), "differs from its receipt"),
            (ReceiptInventoryExecutor(
                self.identity, b2, b3,
                controller_revision_owner_uid="foreign-statefulset-uid",
            ), "ControllerRevision owner chain"),
            (ReceiptInventoryExecutor(
                self.identity, b2, b3,
                live_mutation=("PersistentVolumeClaim", "data-module4d-mongo-0", "uid", "stale-pvc-uid"),
            ), "differs from its receipt"),
            (ReceiptInventoryExecutor(
                self.identity, b2, b3, pv_mutation=("uid", "foreign-claim-uid"),
            ), "PV binding"),
        )
        for executor, message in cases:
            with self.subTest(message=message), self.assertRaisesRegex(TeardownError, message):
                _validate_complete_profile_inventory(
                    executor, self.identity, b2, b3,
                    acceptance_job_uid=None, acceptance_pod_uid=None,
                )
            self.assertFalse(any("delete" in call for call in executor.calls))

        executor = ReceiptInventoryExecutor(self.identity, b2, b3)
        executor.resources["deployments.apps"].append(foreign_workload)
        with self.assertRaisesRegex(TeardownError, "foreign resource"):
            _validate_complete_profile_inventory(
                executor, self.identity, b2, b3,
                acceptance_job_uid=None, acceptance_pod_uid=None,
            )
        self.assertFalse(any("delete" in call for call in executor.calls))

    def test_teardown_run_uses_real_receipt_validators_and_refuses_before_delete(self):
        from scripts.k8s.pipeline_receipts import pipeline_receipt_document
        from scripts.k8s.receipts import receipt_document

        _b2_lock, b3_lock, b2, b3 = self._genuine_b2_b3_receipts()
        b2_doc, b3_doc = receipt_document(b2), pipeline_receipt_document(b3)
        canonical_sha = lambda value: hashlib.sha256(
            json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        job_uid, pod_uid = "b4-acceptance-job-uid", "b4-acceptance-pod-uid"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            task_result, job_log = root / "task-result.json", root / "job.log"
            task_result.write_text("{}\n", encoding="utf-8")
            job_log.write_text("synthetic bounded acceptance log\n", encoding="utf-8")
            task_result.chmod(0o600)
            job_log.chmod(0o600)
            b4 = build_acceptance_receipt(
                identity=self.identity, scope="fixture_runtime", checks=_checks(),
                cluster_uid=b2.cluster_uid, namespace_uid=b2.namespace_uid,
                acceptance_job_uid=job_uid, acceptance_pod_uid=pod_uid,
                lock_sha256=canonical_sha(b3_lock),
                b2_receipt_sha256=canonical_sha(b2_doc),
                b3_receipt_sha256=canonical_sha(b3_doc),
            )
            b4.update({
                "task_result_path": str(task_result),
                "task_result_file_sha256": hashlib.sha256(task_result.read_bytes()).hexdigest(),
                "job_log_path": str(job_log),
                "job_log_file_sha256": hashlib.sha256(job_log.read_bytes()).hexdigest(),
            })
            evidence_path = root / "failure-evidence.json"
            evidence_path.write_text(json.dumps({
                "run_id": self.identity.run_id, "profile": self.identity.profile,
                "namespace": self.identity.namespace,
            }), encoding="utf-8")
            evidence = {
                "path": str(evidence_path),
                "sha256": hashlib.sha256(evidence_path.read_bytes()).hexdigest(),
            }
            executor = ReceiptInventoryExecutor(
                self.identity, b2, b3, derived_mutation="foreign_slice_uid",
                acceptance_job_uid=job_uid, acceptance_pod_uid=pod_uid,
            )
            with (
                patch("scripts.k8s.teardown.collect_runtime_ownership_receipt", return_value=b2),
                patch("scripts.k8s.teardown.collect_pipeline_receipt", return_value=b3),
                patch("scripts.k8s.teardown._live_acceptance_job", return_value=(job_uid, pod_uid)),
            ):
                with self.assertRaisesRegex(TeardownError, "no resources were deleted"):
                    teardown_run(
                        executor, self.identity, b3_lock,
                        b2_receipt=b2_doc, b3_receipt=b3_doc,
                        acceptance_receipt=b4, failure_evidence=evidence,
                        teardown_receipt_path=root / "teardown.json", execute=True,
                    )
            self.assertFalse(any("delete" in call for call in executor.calls))


if __name__ == "__main__":
    unittest.main()
