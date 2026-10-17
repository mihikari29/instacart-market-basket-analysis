"""Offline acceptance checks for the in-pipeline Spark executor comparison."""

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
    _check_executor_benchmark,
    _check_hybrid_trend_sensitivity,
    _check_kafka_round_trip,
    _check_module2_experiments,
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
        result = _check_executor_benchmark(_module2_receipt())
        self.assertEqual(set(result["arms"]), {"local[1]", "local[2]"})
        self.assertFalse(result["executor_scaling_claimed"])

    def test_rejects_mixed_source_or_missing_shuffle_evidence(self):
        receipt = _module2_receipt()
        receipt["executor_benchmark"]["arms"][1]["validation"]["source_receipt"]["source_sha256"] = "b" * 64
        with self.assertRaisesRegex(TaskError, "same measured source"):
            _check_executor_benchmark(receipt)

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
                result = _check_module2_experiments(receipt)
                self.assertEqual(set(result["join_medians_seconds"]), {"sort_merge", "broadcast_hash"})
                path.write_text("{}", encoding="utf-8")
                with self.assertRaisesRegex(TaskError, "does not match"):
                    _check_module2_experiments(receipt)

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
            result = _check_kafka_round_trip("0123456789ab")
        self.assertEqual(json.loads(state["raw"].decode("utf-8"))["probe"], "kafka-round-trip")
        self.assertIn("value_deserializer", state["consumer_options"])
        self.assertEqual(result["topic"], "instacart-b4-0123456789ab")

    def test_hybrid_trend_sensitivity_publishes_reads_back_and_checks_api_change(self):
        state = {"api_reads": [], "trend_reads": [], "published_rows": None, "hybrid_versions": []}
        run_id = "0123456789ab"
        source_namespace = "instacart-k8s-demo-v1"
        as_of = datetime(2026, 10, 17, 10, 0, tzinfo=timezone.utc)

        class FakeMongoClient:
            def __init__(self, uri, **kwargs):
                self.uri = uri

            def __getitem__(self, name):
                return {"module3_trend_bundles": object()}

            def close(self):
                return None

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

        class FakeConfig:
            def __init__(self, **kwargs):
                self.values = kwargs

        class FakeSparkTypes:
            def StructField(self, *args):
                return args

            def StructType(self, fields):
                return fields

            def TimestampType(self):
                return "timestamp"

            def LongType(self):
                return "long"

            def DoubleType(self):
                return "double"

            def IntegerType(self):
                return "integer"

        class Response:
            def __init__(self, payload):
                self.payload = payload

            def raise_for_status(self):
                return None

            def json(self):
                return self.payload

        def api_get(url, **kwargs):
            state["api_reads"].append((url, kwargs))
            payloads = (
                {
                    "user_id": 1, "serving_version": "baseline-version",
                    "source": {"trend_source_namespace": source_namespace},
                    "recommendations": [{"product_id": 101, "final_score": 0.4}],
                },
                {
                    "user_id": 1, "serving_version": "changed-version",
                    "source": {"trend_source_namespace": f"{source_namespace}-b4-{run_id}"},
                    "recommendations": [{"product_id": 202, "final_score": 0.8}],
                },
            )
            return Response(payloads[len(state["api_reads"]) - 1])

        initial_snapshot = types.SimpleNamespace(
            available=True, content_sha256="a" * 64,
            window_start=as_of.replace(minute=0), window_end=as_of,
            trend_recommendations=(types.SimpleNamespace(product_id=999),),
        )
        changed_snapshot = types.SimpleNamespace(available=True, content_sha256="b" * 64)

        def read_trend(_collection, *, source_namespace, **kwargs):
            state["trend_reads"].append(source_namespace)
            return initial_snapshot if source_namespace == source_namespace_base else changed_snapshot

        source_namespace_base = source_namespace

        def write_trends(dataframe, _collection, _config, **kwargs):
            state["published_rows"] = dataframe.rows
            state["trend_write"] = kwargs
            return types.SimpleNamespace(bundles_created=1, trend_rows_represented=2)

        def capture_sources(_config, _user_id, *, prospective_trend_product_ids):
            state["prospective_trend_product_ids"] = tuple(prospective_trend_product_ids)
            return types.SimpleNamespace(
                als_recommendations=(types.SimpleNamespace(product_id=101), types.SimpleNamespace(product_id=202)),
                product_catalog=(types.SimpleNamespace(product_id=101), types.SimpleNamespace(product_id=202)),
                model_version="als-run", graph_version="graph-run",
            )

        def serve(_config, **kwargs):
            version = "baseline-version" if not state["hybrid_versions"] else "changed-version"
            state["hybrid_versions"].append((version, kwargs["source_namespace"]))
            return types.SimpleNamespace(publication_status="published", serving_version=version)

        fake_modules = {
            "pymongo": types.ModuleType("pymongo"),
            "src.module3.config": types.ModuleType("src.module3.config"),
            "src.module3.trend_bundle": types.ModuleType("src.module3.trend_bundle"),
            "src.module4.config": types.ModuleType("src.module4.config"),
            "src.module4.hybrid.config": types.ModuleType("src.module4.hybrid.config"),
            "src.module4.hybrid.data": types.ModuleType("src.module4.hybrid.data"),
            "src.module4.hybrid.service": types.ModuleType("src.module4.hybrid.service"),
            "src.module4.hybrid.trend_reader": types.ModuleType("src.module4.hybrid.trend_reader"),
            "pyspark": types.ModuleType("pyspark"),
            "pyspark.sql": types.ModuleType("pyspark.sql"),
            "pyspark.sql.types": types.ModuleType("pyspark.sql.types"),
            "requests": types.ModuleType("requests"),
        }
        fake_modules["pymongo"].MongoClient = FakeMongoClient
        fake_modules["src.module3.config"].Config = FakeConfig
        fake_modules["src.module3.trend_bundle"].BUNDLE_COLLECTION_NAME = "module3_trend_bundles"
        fake_modules["src.module3.trend_bundle"].write_trend_bundles = write_trends
        fake_modules["src.module4.config"].Config = FakeConfig
        fake_modules["src.module4.hybrid.config"].HybridRankingConfig = FakeConfig
        fake_modules["src.module4.hybrid.data"].capture_verified_user_sources = capture_sources
        fake_modules["src.module4.hybrid.service"].serve_hybrid_user = serve
        fake_modules["src.module4.hybrid.trend_reader"].read_verified_trend_snapshot = read_trend
        fake_modules["pyspark"].__path__ = []
        fake_modules["pyspark.sql"].__path__ = []
        for name in ("StructField", "StructType", "TimestampType", "LongType", "DoubleType", "IntegerType"):
            setattr(fake_modules["pyspark.sql.types"], name, getattr(FakeSparkTypes(), name))
        fake_modules["requests"].get = api_get

        class FakeSpark:
            def createDataFrame(self, rows, schema):
                return types.SimpleNamespace(rows=rows, schema=schema)

        environment = {
            "PIPELINE_RUN_ID": run_id,
            "PIPELINE_MODE": "fixture",
            "PIPELINE_HDFS_ROOT": f"/instacart/{run_id}",
            "PIPELINE_HDFS_URI": f"hdfs://namenode:8020/instacart/{run_id}",
            "PIPELINE_TOPIC": f"instacart-purchase-events-{run_id}",
            "PIPELINE_KAFKA_BOOTSTRAP": "kafka:29092",
            "PIPELINE_TREND_NAMESPACE": source_namespace,
            "PIPELINE_AS_OF_EVENT_TIME": as_of.isoformat().replace("+00:00", "Z"),
            "MODULE4D_MONGO_DATABASE": f"module4d_demo_{run_id}",
            "MONGODB_URI": "mongodb://publisher:secret@module4d-mongo:27017/admin",
        }
        with patch.dict(os.environ, environment, clear=True), patch.dict(sys.modules, fake_modules):
            result = _check_hybrid_trend_sensitivity(FakeSpark())

        self.assertEqual(state["trend_reads"], [source_namespace, f"{source_namespace}-b4-{run_id}"])
        self.assertEqual(state["prospective_trend_product_ids"], (999,))
        self.assertEqual([row["product_id"] for row in state["published_rows"]], [101, 202])
        self.assertEqual(state["trend_write"]["source_scope"], "b4_controlled_trend_sensitivity")
        self.assertEqual(len(state["api_reads"]), 2)
        self.assertEqual(result["source_namespace"], f"{source_namespace}-b4-{run_id}")
        self.assertTrue(result["api_readback_verified"])
        self.assertEqual(result["before_product_id"], 101)
        self.assertEqual(result["after_product_id"], 202)
        self.assertEqual(result["changed_serving_version"], "changed-version")

        receipt = _module2_receipt()
        del receipt["executor_benchmark"]["arms"][0]["workload"]["trials"][0]["task_metrics"]["shuffle_write_bytes"]
        with self.assertRaisesRegex(TaskError, "shuffle/task metrics"):
            _check_executor_benchmark(receipt)


if __name__ == "__main__":
    unittest.main()
