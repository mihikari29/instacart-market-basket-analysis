"""Offline state-machine and safety contracts for the one-session demo CLI."""

import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from contextlib import redirect_stdout

from scripts.k8s.demo import (
    DemoError,
    REPOSITORY_ROOT,
    plan_document,
    require_absent_namespace,
    acceptance_passed,
    _validate_mode_inputs,
    main,
    run_demo,
    verify_run,
)
from scripts.k8s.ownership import RunIdentity
from scripts.k8s.runtime import CommandResult
from tests.test_k8s_pipeline import complete_b3_lock
from scripts.k8s.fixture_data import write_fixture


class FakeCommandExecutor:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def run(self, argv, *, timeout, input_text=None):
        self.calls.append((list(argv), timeout, input_text))
        return self.response


class DemoRunnerOfflineTests(unittest.TestCase):
    def test_plan_records_stable_dependency_order_and_fixture_bounds(self):
        identity = RunIdentity("0123456789ab")
        plan = plan_document(identity, "fixture", host_capacity={"cpu_cores": 8})
        self.assertEqual(plan["stages"][:5], [
            "local_identity", "profile_creation", "application_images", "pipeline_images", "lock_finalization",
        ])
        self.assertEqual(plan["stages"][-5:], [
            "private_render", "namespace_preflight", "serving", "pipeline", "acceptance",
        ])
        self.assertEqual(
            [item["name"] for item in plan["pipeline_steps"]],
            ["input", "module1", "module2", "module4", "replay", "hybrid"],
        )
        self.assertEqual(plan["pipeline_steps"][0]["backoff_limit"], 0)
        self.assertIn("not native distributed", plan["execution_model"])
        self.assertEqual(plan["resume_policy"].split(";")[0], "disabled")

    def test_full_data_plan_accepts_measured_wsl_backing_disk_evidence(self):
        with tempfile.TemporaryDirectory() as temporary:
            input_directory = Path(temporary) / "input"
            write_fixture(input_directory)
            capacity = {
                "cpu_cores": 8,
                "memory_gib": 16,
                "free_disk_gib": 50,
                "wsl_guest": True,
            }
            output = io.StringIO()
            with patch("scripts.k8s.demo.measure_host_capacity", side_effect=lambda: dict(capacity)), redirect_stdout(output):
                status = main([
                    "plan", "--mode", "full", "--run-id", "0123456789ab",
                    "--input-dir", str(input_directory),
                ])
            self.assertEqual(status, 0)
            plan = json.loads(output.getvalue())
            self.assertEqual(plan["host_capacity"]["free_disk_gib"], 50.0)

    def test_full_data_acceptance_scope_matches_runtime_receipt(self):
        self.assertTrue(acceptance_passed("full", {
            "status": "full_data_runtime_pass", "scope": "full_data_runtime",
        }))
        self.assertTrue(acceptance_passed("fixture", {
            "status": "fixture_runtime_pass", "scope": "fixture_runtime",
        }))
        self.assertFalse(acceptance_passed("full", {
            "status": "full_data_runtime_pass", "scope": "full_runtime",
        }))

    def test_namespace_preflight_uses_only_explicit_profile_and_rejects_collisions(self):
        identity = RunIdentity("0123456789ab")
        executor = FakeCommandExecutor(CommandResult(0, '{"items":[]}'))
        require_absent_namespace(executor, identity)
        command = executor.calls[0][0]
        self.assertEqual(command[:3], ["kubectl", "--context", identity.profile])
        self.assertNotIn("current-context", command)

        collision = FakeCommandExecutor(CommandResult(0, json.dumps({"items": [
            {"metadata": {"name": identity.namespace}},
        ]})))
        with self.assertRaisesRegex(DemoError, "already exists"):
            require_absent_namespace(collision, identity)

    def test_run_requires_explicit_authorization_before_any_external_call(self):
        executor = FakeCommandExecutor(CommandResult(0, '{"items":[]}'))
        with self.assertRaisesRegex(DemoError, "explicit --execute"):
            run_demo(
                mode="fixture", lock_path="missing.yaml", output_directory="/tmp/demo-out",
                run_id="0123456789ab", executor=executor,
            )
        self.assertEqual(executor.calls, [])

    def test_repository_output_path_is_rejected_before_profile_preparation(self):
        run_id = "abcdef012345"
        output = REPOSITORY_ROOT / "tests" / ".forbidden-demo-output"
        self.assertFalse(output.exists())
        executor = FakeCommandExecutor(CommandResult(0, "{}"))
        capacity = {
            "cpu_cores": 6,
            "memory_gib": 12.0,
            "free_disk_gib": 50.0,
            "wsl_guest": False,
        }
        with patch("scripts.k8s.demo.prepare_run_lock") as prepare:
            with self.assertRaisesRegex(DemoError, "unsafe run output path"):
                run_demo(
                    mode="fixture",
                    output_directory=output,
                    run_id=run_id,
                    executor=executor,
                    execute=True,
                    capacity=capacity,
                )
        prepare.assert_not_called()
        self.assertEqual(executor.calls, [])
        self.assertFalse(output.exists())

    def test_full_mode_checks_capacity_and_run_pvc_working_set_budget(self):
        capacity = {"cpu_cores": 6, "memory_gib": 12.0, "free_disk_gib": 50.0, "wsl_guest": False}
        with tempfile.TemporaryDirectory() as temporary:
            with patch(
                "scripts.k8s.demo.inspect_input_directory",
                return_value={"total_bytes": 4 * 1024**3 + 1, "manifest_sha256": "a" * 64},
            ):
                with self.assertRaisesRegex(DemoError, "three working-set copies and 2 GiB reserve"):
                    _validate_mode_inputs("full", input_directory=temporary, capacity=capacity)

        with self.assertRaisesRegex(DemoError, "memory_gib"):
            _validate_mode_inputs(
                "full",
                input_directory="unused",
                capacity={**capacity, "memory_gib": 8.0},
            )

    def test_synthetic_runtime_pass_without_real_shaped_receipts_is_rejected(self):
        identity = RunIdentity("0123456789ab")
        lock = complete_b3_lock()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "private-run"
            root.mkdir(mode=0o700)
            (root / "version-lock.json").write_text(json.dumps(lock))
            (root / "fake-b2_receipt.json").write_text("{}\n")
            (root / "fake-b3_receipt.json").write_text("{}\n")
            (root / "fake-acceptance_receipt.json").write_text(json.dumps({
                "run_id": identity.run_id,
                "status": "fixture_runtime_pass",
                "scope": "fixture_runtime",
            }))
            artifacts = {}
            for name in ("b2_receipt", "b3_receipt", "acceptance_receipt"):
                filename = f"fake-{name}.json"
                path = root / filename
                artifacts[name] = {
                    "path": filename,
                    "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                }
            (root / "demo-session.json").write_text(json.dumps({
                "status": "runtime_pass", "run_id": identity.run_id,
                "profile": identity.profile, "context": identity.profile,
                "namespace": identity.namespace, "mode": "fixture",
                "artifacts": artifacts,
            }))
            with self.assertRaises(DemoError):
                verify_run(identity.run_id, root)

    def test_verify_failed_session_never_claims_runtime_acceptance(self):
        identity = RunIdentity("0123456789ab")
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "failed-run"
            root.mkdir(mode=0o700)
            (root / "demo-session.json").write_text(json.dumps({
                "status": "failed", "run_id": identity.run_id,
                "profile": identity.profile, "context": identity.profile,
                "namespace": identity.namespace, "mode": "fixture",
            }))
            report = verify_run(identity.run_id, root)
        self.assertEqual(report["runtime_acceptance"], "not passed")
        self.assertFalse(report["cluster_contacted"])
        self.assertFalse(report["teardown_performed"])


if __name__ == "__main__":
    unittest.main()
