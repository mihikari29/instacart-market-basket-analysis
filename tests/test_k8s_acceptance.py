"""Offline contracts for B4 acceptance evidence and destructive-action gates."""

import json
import hashlib
from pathlib import Path
import tempfile
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
    teardown_run,
)
from tests.test_k8s_pipeline import _complete_b3_lock


class InertExecutor:
    def __init__(self):
        self.calls = []

    def run(self, argv, *, timeout, input_text=None):
        self.calls.append(list(argv))
        raise AssertionError("offline acceptance tests must not contact Kubernetes")


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


if __name__ == "__main__":
    unittest.main()
