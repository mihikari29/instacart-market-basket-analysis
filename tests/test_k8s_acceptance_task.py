"""Offline acceptance checks for the in-pipeline Spark executor comparison."""

import unittest

from scripts.k8s.acceptance_task import TaskError, _check_executor_benchmark


def _module2_receipt(source_sha="a" * 64):
    arms = []
    for master in ("local[1]", "local[2]"):
        arms.append({
            "status": "passed",
            "environment": {
                "actual_master": master,
                "execution_model": "Spark local mode",
                "native_executor_scaling": False,
            },
            "validation": {"source_receipt": {"source_sha256": source_sha}},
            "workload": {
                "input_rows": 100,
                "input_bytes": 4096,
                "median_seconds": 1.25,
                "trials": [
                    {"task_metrics": {"tasks": 2, "shuffle_read_bytes": 0, "shuffle_write_bytes": 0}}
                    for _ in range(3)
                ],
            },
        })
    return {"executor_benchmark": {
        "status": "passed",
        "same_source_and_output_verified": True,
        "executor_scaling_claimed": False,
        "execution_scope": "single-host Spark local thread pool comparison",
        "arms": arms,
    }}


class ExecutorBenchmarkAcceptanceTests(unittest.TestCase):
    def test_accepts_two_measured_same_source_local_arms(self):
        result = _check_executor_benchmark(_module2_receipt())
        self.assertEqual(set(result["arms"]), {"local[1]", "local[2]"})
        self.assertFalse(result["executor_scaling_claimed"])

    def test_rejects_mixed_source_or_missing_shuffle_evidence(self):
        receipt = _module2_receipt()
        receipt["executor_benchmark"]["arms"][1]["validation"]["source_receipt"]["source_sha256"] = "b" * 64
        with self.assertRaisesRegex(TaskError, "same measured source"):
            _check_executor_benchmark(receipt)

        receipt = _module2_receipt()
        del receipt["executor_benchmark"]["arms"][0]["workload"]["trials"][0]["task_metrics"]["shuffle_write_bytes"]
        with self.assertRaisesRegex(TaskError, "shuffle/task metrics"):
            _check_executor_benchmark(receipt)


if __name__ == "__main__":
    unittest.main()
