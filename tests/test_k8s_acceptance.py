"""Offline contracts for B4 acceptance evidence and destructive-action gates."""

import json
import hashlib
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

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
from scripts.k8s.teardown import (
    TeardownError,
    _failure_evidence,
    _validate_acceptance_checks,
    _verify_acceptance_artifacts,
    _validate_complete_profile_inventory,
    teardown_run,
)
from tests.test_k8s_pipeline import _complete_b3_lock


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


if __name__ == "__main__":
    unittest.main()
