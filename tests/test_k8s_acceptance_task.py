"""Offline acceptance checks for the in-pipeline Spark executor comparison."""

import ast
import hashlib
import json
import os
import sys
import types
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts.k8s.acceptance_task import (
    TaskError,
    controlled_trend_rows,
    check_executor_benchmark,
    check_kafka_round_trip,
    check_module2_experiments,
)


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
        result = check_executor_benchmark(_module2_receipt())
        self.assertEqual(set(result["arms"]), {"local[1]", "local[2]"})
        self.assertFalse(result["executor_scaling_claimed"])

    def test_rejects_mixed_source_or_missing_shuffle_evidence(self):
        receipt = _module2_receipt()
        receipt["executor_benchmark"]["arms"][1]["validation"]["source_receipt"]["source_sha256"] = "b" * 64
        with self.assertRaisesRegex(TaskError, "same measured source"):
            check_executor_benchmark(receipt)

    def test_module2_performance_receipt_requires_all_three_source_bound_experiments(self):
        source_sha = "a" * 64
        summary = {
            "status": "passed",
            "validation": {"source_receipt": {"source_sha256": source_sha}},
            "join_benchmark": {
                "sort_merge": {"runs_seconds": [1.0, 1.1, 1.2], "median_seconds": 1.1},
                "broadcast_hash": {"runs_seconds": [0.8, 0.9, 1.0], "median_seconds": 0.9},
                "outputs_equal": True, "operators_verified": True,
            },
            "partition_pruning": {
                "full_scan": {"runs_seconds": [1.0, 1.1, 1.2], "median_seconds": 1.1},
                "date_filter": {"runs_seconds": [0.8, 0.9, 1.0], "median_seconds": 0.9},
                "partition_filter": {"runs_seconds": [0.5, 0.6, 0.7], "median_seconds": 0.6},
                "partition_filters_verified": True, "equivalent_filters_equal": True,
            },
            "cache_benchmark": {
                "uncached": {"runs_seconds": [1.0, 1.1, 1.2], "median_seconds": 1.1},
                "cached_reuse": {"runs_seconds": [0.5, 0.6, 0.7], "median_seconds": 0.6},
                "outputs_equal": True,
            },
        }
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root / "results/module2/summary.json"
            path.parent.mkdir(parents=True)
            raw = json.dumps(summary).encode()
            path.write_bytes(raw)
            receipt = {"module2_experiments": {
                "status": "passed", "summary_path": str(path),
                "summary_sha256": hashlib.sha256(raw).hexdigest(),
                "experiments": ["cache_benchmark", "join_benchmark", "partition_pruning"],
                "source_sha256": source_sha,
            }}
            with patch("scripts.k8s.acceptance_task.RUNTIME_ROOT", root):
                result = check_module2_experiments(receipt)
                self.assertEqual(set(result["join_medians_seconds"]), {"sort_merge", "broadcast_hash"})
                path.write_text("{}", encoding="utf-8")
                with self.assertRaisesRegex(TaskError, "does not match"):
                    check_module2_experiments(receipt)

    def test_kafka_round_trip_decodes_the_wire_json_bytes(self):
        state = {"raw": None, "consumer_options": None}

        class Future:
            def get(self, timeout):
                return None

        class Admin:
            def __init__(self, **kwargs):
                pass

            def list_topics(self):
                return set()

            def create_topics(self, topics):
                return None

            def close(self):
                return None

        class Producer:
            def __init__(self, *, value_serializer, **kwargs):
                self.serialize = value_serializer

            def send(self, topic, *, key, value):
                state["raw"] = self.serialize(value)
                return Future()

            def flush(self, timeout):
                return None

            def close(self, timeout):
                return None

        class Consumer:
            def __init__(self, **kwargs):
                state["consumer_options"] = kwargs
                self.deserialize = kwargs["value_deserializer"]

            def assign(self, partitions):
                self.partition = partitions[0]

            def seek(self, partition, offset):
                self.offset = offset

            def poll(self, timeout_ms):
                raw = state["raw"]
                if not isinstance(raw, bytes):
                    raise AssertionError("Kafka consumer did not receive wire bytes")
                record = types.SimpleNamespace(value=self.deserialize(raw))
                return {self.partition: [record]}

            def close(self):
                return None

        class TopicPartition:
            def __init__(self, topic, partition):
                self.topic = topic
                self.partition = partition

        class NewTopic:
            def __init__(self, topic, num_partitions, replication_factor):
                self.topic = topic

        class TopicAlreadyExistsError(Exception):
            pass

        kafka = types.ModuleType("kafka")
        kafka.KafkaConsumer = Consumer
        kafka.KafkaProducer = Producer
        kafka.TopicPartition = TopicPartition
        admin = types.ModuleType("kafka.admin")
        admin.KafkaAdminClient = Admin
        admin.NewTopic = NewTopic
        errors = types.ModuleType("kafka.errors")
        errors.TopicAlreadyExistsError = TopicAlreadyExistsError
        with patch.dict(os.environ, {"PIPELINE_KAFKA_BOOTSTRAP": "kafka:29092"}), patch.dict(
            sys.modules, {"kafka": kafka, "kafka.admin": admin, "kafka.errors": errors}
        ):
            result = check_kafka_round_trip("0123456789ab")
        self.assertEqual(json.loads(state["raw"].decode("utf-8"))["probe"], "kafka-round-trip")
        self.assertIn("value_deserializer", state["consumer_options"])
        self.assertEqual(result["topic"], "instacart-b4-0123456789ab")

    def test_hybrid_acceptance_probe_uses_same_namespace_and_later_event_time(self):
        source_path = Path(__file__).resolve().parents[1] / "scripts/k8s/acceptance_task.py"
        tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
        function = next(
            node for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name == "_check_hybrid_trend_sensitivity"
        )
        calls = [node for node in ast.walk(function) if isinstance(node, ast.Call)]

        def keyword(call, name):
            item = next((item for item in call.keywords if item.arg == name), None)
            return ast.unparse(item.value) if item is not None else None

        publishers = [
            call for call in calls
            if isinstance(call.func, ast.Name) and call.func.id == "serve_hybrid_user"
        ]
        self.assertEqual(len(publishers), 2)
        self.assertEqual([keyword(call, "source_namespace") for call in publishers], ["source_namespace"] * 2)
        self.assertEqual(
            {keyword(call, "as_of_event_time") for call in publishers},
            {"as_of", "changed_as_of"},
        )
        trend_writers = [
            call for call in calls
            if isinstance(call.func, ast.Name) and call.func.id == "write_trend_bundles"
        ]
        self.assertEqual(len(trend_writers), 1)
        self.assertEqual(keyword(trend_writers[0], "source_namespace"), "source_namespace")
        hybrid_configs = [
            call for call in calls
            if isinstance(call.func, ast.Name) and call.func.id == "HybridRankingConfig"
        ]
        self.assertEqual(len(hybrid_configs), 1)
        self.assertEqual(hybrid_configs[0].args, [])
        self.assertNotIn("changed_namespace", {
            node.id for node in ast.walk(function) if isinstance(node, ast.Name)
        })

        changed_assignment = next(
            node for node in ast.walk(function)
            if isinstance(node, ast.Assign)
            and any(isinstance(target, ast.Name) and target.id == "changed_as_of" for target in node.targets)
        )
        self.assertEqual(
            ast.unparse(changed_assignment.value),
            "max(as_of, trend_result.window_end) + timedelta(milliseconds=1)",
        )
        leader_assignment = next(
            node for node in ast.walk(function)
            if isinstance(node, ast.Assign)
            and any(isinstance(target, ast.Name) and target.id == "trend_leader_index" for target in node.targets)
        )
        self.assertEqual(
            ast.unparse(leader_assignment.value),
            "1 if float(baseline_trend_score) > 0.5 else 0",
        )
        controlled_rows = [
            call for call in calls
            if isinstance(call.func, ast.Name) and call.func.id == "controlled_trend_rows"
        ]
        self.assertEqual(len(controlled_rows), 1)
        self.assertEqual(keyword(controlled_rows[0], "trend_weight_short"), "stream_config.weight_short")

    def test_controlled_trend_rows_match_module3_short_window_score(self):
        start = datetime(2026, 10, 17, 8, 0, tzinfo=timezone.utc)
        end = datetime(2026, 10, 17, 10, 0, tzinfo=timezone.utc)
        first_leads = controlled_trend_rows(
            [101, 202], start, end, trend_leader_index=0, trend_weight_short=0.7,
        )
        second_leads = controlled_trend_rows(
            [101, 202], start, end, trend_leader_index=1, trend_weight_short=0.7,
        )
        self.assertEqual(
            [(row["purchase_count_30m"], row["trend_score"], row["trend_rank"]) for row in first_leads],
            [(1, 0.7, 1), (0, 0.0, 2)],
        )
        self.assertEqual(
            [(row["purchase_count_30m"], row["trend_score"], row["trend_rank"]) for row in second_leads],
            [(0, 0.0, 2), (1, 0.7, 1)],
        )

    def test_executor_benchmark_rejects_missing_shuffle_metrics(self):
        receipt = _module2_receipt()
        del receipt["executor_benchmark"]["arms"][0]["workload"]["trials"][0]["task_metrics"]["shuffle_write_bytes"]
        with self.assertRaisesRegex(TaskError, "shuffle/task metrics"):
            check_executor_benchmark(receipt)


if __name__ == "__main__":
    unittest.main()
