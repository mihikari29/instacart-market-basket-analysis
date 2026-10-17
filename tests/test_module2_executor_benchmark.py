"""Offline contract tests for the bounded Spark execution comparison."""

import json
from pathlib import Path
import tempfile
import unittest

from src.module2.executor_benchmark import build_plan, execute_plan, validate_arms


class ExecutorBenchmarkTests(unittest.TestCase):
    def test_only_supported_local_thread_arms_are_accepted(self):
        self.assertEqual(validate_arms(("local[1]", "local[2]")), ("local[1]", "local[2]"))
        for arms in (("local[1]",), ("local[1]", "local[1]"), ("spark://host:7077", "local[2]")):
            with self.subTest(arms=arms), self.assertRaises(ValueError):
                validate_arms(arms)

    def test_plan_is_offline_and_disclaims_executor_scaling(self):
        plan = build_plan(root="hdfs://namenode:8020/instacart", output="results/bench", run_id="offline")

        self.assertFalse(plan["executor_scaling_claimed"])
        self.assertEqual(plan["arms"], ["local[1]", "local[2]"])
        self.assertTrue(all("worker" in arm["command"] for arm in plan["arms_plan"]))
        self.assertTrue(all(arm["native_executor_scaling"] is False for arm in plan["arms_plan"]))

    def test_execution_requires_matching_source_and_workload_checksums(self):
        with tempfile.TemporaryDirectory() as tmp:
            plan = build_plan(root="hdfs://namenode:8020/instacart", output=tmp, run_id="offline")

            def fake_runner(command, check, timeout):
                self.assertTrue(check)
                self.assertEqual(timeout, 1200)
                output = Path(command[command.index("--output") + 1])
                run_id = command[command.index("--run-id") + 1]
                arm = command[command.index("--master") + 1]
                arm_dir = output / run_id
                arm_dir.mkdir()
                (arm_dir / "summary.json").write_text(
                    json.dumps(
                        {
                            "status": "passed",
                            "validation": {
                                "source_receipt": {"source_sha256": "a" * 64},
                                "counts": {"interactions": 15},
                            },
                            "workload": {"checksum": "b" * 64, "master": arm},
                        }
                    ),
                    encoding="utf-8",
                )

            result = execute_plan(plan, runner=fake_runner)

        self.assertEqual(result["status"], "passed")
        self.assertTrue(result["same_source_and_output_verified"])
        self.assertFalse(result["executor_scaling_claimed"])
        self.assertEqual(len(result["arms"]), 2)

    def test_execution_timeout_is_bounded_and_recorded(self):
        plan = build_plan(root="hdfs://namenode:8020/instacart", output="results/bench", run_id="timeout")
        self.assertEqual(plan["timeout_seconds"], 1200)
        with self.assertRaisesRegex(ValueError, "between 1 and 7200"):
            build_plan(
                root="hdfs://namenode:8020/instacart", output="results/bench", run_id="timeout",
                timeout_seconds=7201,
            )


if __name__ == "__main__":
    unittest.main()
