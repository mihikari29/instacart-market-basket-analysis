"""Offline contract tests for the bounded Spark execution comparison."""

import json
from pathlib import Path
import tempfile
import unittest

from src.module2.config import Config
from src.module2.executor_benchmark import build_plan, execute_plan, observed_executors, validate_arms


class ExecutorBenchmarkTests(unittest.TestCase):
    def test_standalone_plan_has_distinct_explicit_executor_layouts(self):
        plan = build_plan(root="file:///fixture", output="results", run_id="standalone",
                          execution_mode="standalone", master="spark://spark-master:7077")
        configs = [arm["executor_config"] for arm in plan["arms_plan"]]
        self.assertEqual([item["executor_cores"] for item in configs], [1, 2])
        self.assertEqual([item["total_cores"] for item in configs], [2, 2])
        self.assertTrue(all("--executor-memory" in arm["command"] for arm in plan["arms_plan"]))
        self.assertTrue(all(arm["master"] == "spark://spark-master:7077" for arm in plan["arms_plan"]))

    def test_standalone_requires_remote_master_and_distinct_valid_configs(self):
        for options in ({"master": "local[2]"}, {"master": "spark://host:7077", "executor_configs": [{"executor_cores": 1}] * 2},
                        {"master": "spark://host:7077", "executor_configs": [dict(executor_memory="1g", executor_cores=1, total_cores=2)] * 2}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                build_plan(root="file:///fixture", output="results", run_id="bad", execution_mode="standalone", **options)

    def test_observed_executor_gate_rejects_driver_only_or_incorrect_allocation(self):
        config = Config(root="file:///fixture", executor_cores=1, total_cores=2)
        rows = [dict(id="1", isActive=True, totalCores=1, maxMemory=100),
                dict(id="2", isActive=True, totalCores=1, maxMemory=100)]
        self.assertEqual(len(observed_executors(None, config, fetch=lambda: rows)), 2)
        for invalid in ([], rows[:1], [dict(id="driver", isActive=True, totalCores=2, maxMemory=100)]):
            with self.subTest(invalid=invalid), self.assertRaises(RuntimeError):
                observed_executors(None, config, fetch=lambda: invalid)

    def test_standalone_execution_checks_actual_executor_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            plan = build_plan(root="file:///fixture", output=tmp, run_id="standalone",
                              execution_mode="standalone", master="spark://host:7077")
            def fake_runner(command, **kwargs):
                arm_id = command[command.index("--run-id") + 1]
                arm = next(item for item in plan["arms_plan"] if item["command"] == command)
                config = arm["executor_config"]
                output = Path(plan["output_directory"]) / arm_id
                output.mkdir()
                (output / "summary.json").write_text(json.dumps({
                    "status": "passed", "executor_config": config,
                    "environment": {"actual_master": arm["master"], "observed_executors": [
                        dict(id=str(i), isActive=True, totalCores=config["executor_cores"], maxMemory=100)
                        for i in range(config["total_cores"] // config["executor_cores"])]},
                    "validation": {"source_receipt": {"source_sha256": "a" * 64}, "counts": {"interactions": 15}},
                    "workload": {"checksum": "b" * 64},
                }))
            self.assertTrue(execute_plan(plan, runner=fake_runner)["executor_configuration_comparison_verified"])

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
