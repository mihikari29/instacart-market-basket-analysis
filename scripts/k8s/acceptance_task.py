"""In-cluster B4 acceptance probes for one already completed B3 run.

The task uses only run-scoped services and receipts. It never exports Mongo
credentials and does not delete a topic, collection, checkpoint, or workload.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import time
from typing import Any

from .data_receipts import REQUIRED_HEADERS, source_manifest_digest
from .pipeline_task import RUNTIME_ROOT, TaskError, _runtime_values, _write_json


def _canonical_digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


def _check(name: str, evidence: Any) -> dict[str, Any]:
    return {"name": name, "status": "passed", "evidence_sha256": _canonical_digest(evidence)}


def _load_step(name: str) -> dict[str, Any]:
    path = RUNTIME_ROOT / "evidence" / "steps" / f"{name}.json"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise TaskError(f"required B3 {name} step receipt is unavailable") from exc
    if value.get("step") != name or value.get("status") != "completed":
        raise TaskError(f"required B3 {name} step did not complete")
    return value


def _check_input_provenance(run_id: str, mode: str) -> dict[str, Any]:
    path = RUNTIME_ROOT / "evidence" / "source-receipt.json"
    try:
        source = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise TaskError("B4 source provenance receipt is unavailable") from exc
    input_step = _load_step("input").get("receipt", {})
    module1_step = _load_step("module1").get("receipt", {})
    files = source.get("files")
    digest = source.get("manifest_sha256")
    expected_scope = "synthetic_fixture" if mode == "fixture" else "full_data_input"
    if (
        source.get("run_id") != run_id or source.get("scope") != expected_scope
        or not isinstance(files, dict) or set(files) != set(REQUIRED_HEADERS)
        or not isinstance(digest, str) or digest != source_manifest_digest(files)
        or input_step.get("source_manifest_sha256") != digest
        or module1_step.get("source_manifest_sha256") != digest
    ):
        raise TaskError("B4 six-file source receipt does not reconcile with the completed Module 1 run")
    if mode == "full" and digest != os.environ.get("PIPELINE_SOURCE_RECEIPT_SHA256"):
        raise TaskError("B4 full-data source receipt differs from reviewed preflight evidence")
    fixture = source.get("fixture")
    if mode == "fixture" and (
        not isinstance(fixture, dict) or fixture.get("scope") != "synthetic_fixture"
        or type(fixture.get("users")) is not int or fixture["users"] > 50
        or type(module1_step.get("generated_events")) is not int
        or module1_step["generated_events"] > 5000
    ):
        raise TaskError("B4 deterministic fixture exceeds its declared user/event bound")
    return {"scope": expected_scope, "manifest_sha256": digest, "file_count": len(files), "fixture": fixture}


def _check_hdfs_readback(run_id: str) -> dict[str, Any]:
    import requests

    url = f"http://namenode:9870/webhdfs/v1/instacart/{run_id}/curated/_sources.json"
    try:
        response = requests.get(url, params={"op": "OPEN", "user.name": "hadoop"}, timeout=30)
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:
        raise TaskError(f"HDFS WebHDFS source-manifest readback failed ({type(exc).__name__})") from exc
    if not isinstance(payload, dict) or not payload:
        raise TaskError("HDFS curated source manifest is empty or malformed")
    for table, record in payload.items():
        if not isinstance(table, str) or not isinstance(record, dict):
            raise TaskError("HDFS curated source manifest has an invalid entry")
        if type(record.get("rows")) is not int or record["rows"] < 0:
            raise TaskError("HDFS curated source manifest has an invalid row count")
        if not isinstance(record.get("sha256"), str) or len(record["sha256"]) != 64:
            raise TaskError("HDFS curated source manifest has an invalid source hash")
    return {"tables": len(payload), "manifest_sha256": _canonical_digest(payload)}


def _check_executor_benchmark(module2_receipt: dict[str, Any]) -> dict[str, Any]:
    """Require same-source local[1]/local[2] measurements and shuffle evidence."""
    benchmark = module2_receipt.get("executor_benchmark")
    if (
        not isinstance(benchmark, dict)
        or benchmark.get("status") != "passed"
        or benchmark.get("same_source_and_output_verified") is not True
        or benchmark.get("executor_scaling_claimed") is not False
        or benchmark.get("execution_scope") != "single-host Spark local thread pool comparison"
    ):
        raise TaskError("Module 2 executor benchmark is missing or failed its source/output gate")
    arms = benchmark.get("arms")
    if not isinstance(arms, list) or len(arms) != 2:
        raise TaskError("Module 2 executor benchmark must contain exactly two configurations")
    summaries = {}
    for arm in arms:
        if not isinstance(arm, dict):
            raise TaskError("Module 2 executor benchmark arm is malformed")
        environment = arm.get("environment")
        workload = arm.get("workload")
        validation = arm.get("validation")
        source = validation.get("source_receipt") if isinstance(validation, dict) else None
        master = environment.get("actual_master") if isinstance(environment, dict) else None
        if (
            master not in {"local[1]", "local[2]"}
            or environment.get("execution_model") != "Spark local mode"
            or environment.get("native_executor_scaling") is not False
            or arm.get("status") != "passed"
            or not isinstance(source, dict)
            or not isinstance(source.get("source_sha256"), str)
            or len(source["source_sha256"]) != 64
            or any(char not in "0123456789abcdef" for char in source["source_sha256"])
            or not isinstance(workload, dict)
            or type(workload.get("input_rows")) is not int
            or workload["input_rows"] <= 0
            or type(workload.get("input_bytes")) is not int
            or workload["input_bytes"] <= 0
            or type(workload.get("median_seconds")) not in (int, float)
            or workload["median_seconds"] <= 0
        ):
            raise TaskError("Module 2 executor arm lacks actual source, runtime, or workload measurements")
        trials = workload.get("trials")
        if not isinstance(trials, list) or len(trials) < 3:
            raise TaskError("Module 2 executor benchmark requires at least three measured trials per arm")
        metrics = []
        for trial in trials:
            task = trial.get("task_metrics") if isinstance(trial, dict) else None
            if not isinstance(task, dict) or any(
                type(task.get(field)) is not int or task[field] < 0
                for field in ("tasks", "shuffle_read_bytes", "shuffle_write_bytes")
            ):
                raise TaskError("Module 2 executor trial is missing Spark shuffle/task metrics")
            metrics.append({
                "tasks": task["tasks"],
                "shuffle_read_bytes": task["shuffle_read_bytes"],
                "shuffle_write_bytes": task["shuffle_write_bytes"],
            })
        summaries[master] = {
            "source_sha256": source["source_sha256"],
            "input_rows": workload["input_rows"],
            "input_bytes": workload["input_bytes"],
            "median_seconds": workload["median_seconds"],
            "shuffle_metrics": metrics,
        }
    if set(summaries) != {"local[1]", "local[2]"}:
        raise TaskError("Module 2 executor benchmark did not compare local[1] and local[2]")
    if len({item["source_sha256"] for item in summaries.values()}) != 1 or len(
        {(item["input_rows"], item["input_bytes"]) for item in summaries.values()}
    ) != 1:
        raise TaskError("Module 2 executor benchmark arms did not use the same measured source")
    return {"scope": benchmark["execution_scope"], "arms": summaries, "executor_scaling_claimed": False}


def _check_module2_experiments(module2_receipt: dict[str, Any]) -> dict[str, Any]:
    """Reconcile all three Spark query experiments to their saved performance receipt."""
    experiment = module2_receipt.get("module2_experiments")
    if not isinstance(experiment, dict) or experiment.get("status") != "passed":
        raise TaskError("Module 2 join, partition, and cache experiment receipt is missing")
    path = Path(str(experiment.get("summary_path", "")))
    root = RUNTIME_ROOT.resolve()
    if path.is_symlink() or not path.is_file():
        raise TaskError("Module 2 performance summary is missing or unsafe")
    try:
        path.resolve(strict=True).relative_to(root)
        raw = path.read_bytes()
        summary = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as exc:
        raise TaskError("Module 2 performance summary failed its path or JSON check") from exc
    digest = hashlib.sha256(raw).hexdigest()
    required = {"join_benchmark", "partition_pruning", "cache_benchmark"}
    if (
        digest != experiment.get("summary_sha256")
        or experiment.get("experiments") != sorted(required)
        or summary.get("status") != "passed"
        or experiment.get("source_sha256") != summary.get("validation", {}).get("source_receipt", {}).get("source_sha256")
    ):
        raise TaskError("Module 2 performance summary does not match the successful source-bound receipt")

    def measured_arms(key: str, arm_names: set[str]) -> dict[str, float]:
        value = summary.get(key)
        if not isinstance(value, dict) or set(value).intersection(arm_names) != arm_names:
            raise TaskError(f"Module 2 {key} is missing required experimental arms")
        result = {}
        for name in sorted(arm_names):
            arm = value[name]
            runs = arm.get("runs_seconds") if isinstance(arm, dict) else None
            median = arm.get("median_seconds") if isinstance(arm, dict) else None
            if (
                not isinstance(runs, list) or len(runs) < 3
                or any(type(item) not in (int, float) or not math.isfinite(item) or item <= 0 for item in runs)
                or type(median) not in (int, float) or not math.isfinite(median) or median <= 0
            ):
                raise TaskError(f"Module 2 {key} arm {name} lacks three finite measured timings")
            result[name] = float(median)
        return result

    joins = measured_arms("join_benchmark", {"sort_merge", "broadcast_hash"})
    partitions = measured_arms("partition_pruning", {"full_scan", "date_filter", "partition_filter"})
    cache = measured_arms("cache_benchmark", {"uncached", "cached_reuse"})
    if (
        summary["join_benchmark"].get("outputs_equal") is not True
        or summary["join_benchmark"].get("operators_verified") is not True
        or summary["partition_pruning"].get("partition_filters_verified") is not True
        or summary["partition_pruning"].get("equivalent_filters_equal") is not True
        or summary["cache_benchmark"].get("outputs_equal") is not True
    ):
        raise TaskError("Module 2 experiment output, plan, or partition-pruning verification failed")
    return {
        "summary_sha256": digest,
        "source_sha256": experiment["source_sha256"],
        "join_medians_seconds": joins,
        "partition_medians_seconds": partitions,
        "cache_medians_seconds": cache,
    }


def _check_kafka_round_trip(run_id: str) -> dict[str, Any]:
    from kafka import KafkaConsumer, KafkaProducer, TopicPartition
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    bootstrap = os.environ["PIPELINE_KAFKA_BOOTSTRAP"]
    topic = f"instacart-b4-{run_id}"
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=5000)
    try:
        if topic in set(admin.list_topics()):
            raise TaskError("B4 Kafka probe topic already exists for this run; refusing to reuse stale offsets")
        try:
            admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
        except TopicAlreadyExistsError as exc:
            raise TaskError("B4 Kafka probe topic collided during creation") from exc
    finally:
        admin.close()

    marker = {"run_id": run_id, "probe": "kafka-round-trip", "created_at_utc": datetime.now(timezone.utc).isoformat()}
    producer = KafkaProducer(bootstrap_servers=bootstrap, value_serializer=lambda value: json.dumps(value).encode())
    try:
        producer.send(topic, key=run_id.encode(), value=marker).get(timeout=15)
        producer.flush(timeout=15)
    finally:
        producer.close(timeout=10)
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap,
        enable_auto_commit=False,
        request_timeout_ms=5000,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
    )
    try:
        partition = TopicPartition(topic, 0)
        consumer.assign([partition])
        consumer.seek(partition, 0)
        deadline = time.monotonic() + 20
        observed = None
        while time.monotonic() < deadline and observed is None:
            for records in consumer.poll(timeout_ms=1000).values():
                for record in records:
                    observed = record.value
                    break
                if observed is not None:
                    break
    finally:
        consumer.close()
    if observed != marker:
        raise TaskError("B4 Kafka marker did not round-trip through the run-scoped broker")
    return {"topic": topic, "partition": 0, "marker_sha256": _canonical_digest(marker)}


def _check_checkpoint_recovery(run_id: str, config, spark) -> dict[str, Any]:
    from kafka import KafkaAdminClient
    from src.module3.demo import _event
    from src.module3.kafka_utils import create_topic, produce_json_events, safe_topic_name
    from src.module3.stream import run_stream

    topic = safe_topic_name(f"{config.topic}-b4-checkpoint")
    checkpoint_root = Path("/run/checkpoints") / "module3" / "b4-recovery"
    if checkpoint_root.exists():
        raise TaskError("B4 checkpoint path already exists for this run; refusing stale recovery evidence")
    admin = KafkaAdminClient(bootstrap_servers=config.bootstrap_servers, request_timeout_ms=5000)
    try:
        if topic in set(admin.list_topics()):
            raise TaskError("B4 checkpoint topic already exists for this run; refusing stale offsets")
    finally:
        admin.close()
    create_topic(config.bootstrap_servers, topic, partitions=1, reset=False)
    recovery_config = replace(config, topic=topic, trigger_interval="2 seconds", starting_offsets="earliest")
    checkpoint_id = f"b4-checkpoint-{run_id}"
    output_root = RUNTIME_ROOT / "results" / os.environ.get("POD_NAME", "acceptance") / "checkpoint-recovery"
    receipts = []
    for sequence in (1, 2):
        event_time = datetime(2030, 1, 1, 0, sequence * 5, tzinfo=timezone.utc)
        event = _event(f"b4-checkpoint-{run_id}-{sequence}", 970000 + sequence, event_time)

        def publish_after_start(_query, current=event):
            return {"published_events": produce_json_events(recovery_config.bootstrap_servers, topic, [current])}

        phase_output = output_root / f"phase-{sequence}"
        phase_output.mkdir(parents=True, exist_ok=False)
        receipt = run_stream(
            spark, recovery_config, phase_output, f"b4-checkpoint-{run_id}",
            duration_seconds=12, checkpoint_id=checkpoint_id,
            checkpoint_root=checkpoint_root, on_query_started=publish_after_start,
        )
        progress_path = phase_output / "streaming_progress.jsonl"
        try:
            progress_records = [json.loads(line) for line in progress_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise TaskError(f"checkpoint recovery phase {sequence} progress log is unavailable") from exc
        observed_rows = sum(int(item.get("numInputRows") or 0) for item in progress_records)
        if observed_rows < 1:
            raise TaskError(f"checkpoint recovery phase {sequence} did not consume its Kafka marker")
        receipts.append({
            "query_id": str(receipt.get("query_id")),
            "checkpoint_id": receipt.get("checkpoint_id"),
            "checkpoint_location": receipt.get("checkpoint_location"),
            "num_input_rows": observed_rows,
            "progress_batches": len(progress_records),
        })
    if receipts[0]["query_id"] != receipts[1]["query_id"]:
        raise TaskError("Structured Streaming query identity changed across checkpoint recovery")
    if receipts[0]["checkpoint_location"] != receipts[1]["checkpoint_location"]:
        raise TaskError("Structured Streaming checkpoint location changed across recovery")
    return {"phases": receipts, "checkpoint_id": checkpoint_id}


def _check_late_data_and_sink(run_id: str, config, spark) -> dict[str, Any]:
    from kafka import KafkaAdminClient
    from src.module3.demo import run_late_data_demo
    from src.module3.kafka_utils import safe_topic_name

    output = RUNTIME_ROOT / "results" / os.environ.get("POD_NAME", "acceptance") / "late-data"
    topic = safe_topic_name(f"{config.topic}-late-demo")
    admin = KafkaAdminClient(bootstrap_servers=config.bootstrap_servers, request_timeout_ms=5000)
    try:
        if topic in set(admin.list_topics()):
            raise TaskError("B4 late-data topic already exists for this run; refusing stale evidence")
    finally:
        admin.close()
    output.mkdir(parents=True, exist_ok=False)
    evidence = run_late_data_demo(
        spark, config, output, f"b4-late-{run_id}", duration_seconds=1,
        checkpoint_id=f"b4-late-{run_id}", reset_topic=False,
        checkpoint_root=Path("/run/checkpoints") / "module3" / "late-demo",
    )
    conclusion = evidence.get("conclusion", {})
    if conclusion != {
        "within_watermark_accepted": True,
        "beyond_watermark_dropped": True,
        "duplicate_event_id_dropped": True,
        "poison_event_counted_invalid": True,
        "accepted_count": 2,
    }:
        raise TaskError("Module 3 late-data evidence did not satisfy the exact watermark and deduplication contract")
    return {
        "evidence_sha256": hashlib.sha256((output / "late_data_evidence.json").read_bytes()).hexdigest(),
        "conclusion": conclusion,
    }


def _check_mongo_idempotent(run_id: str) -> dict[str, Any]:
    from pymongo import MongoClient

    uri = os.environ.get("MONGODB_URI")
    database = os.environ.get("MODULE4D_MONGO_DATABASE")
    if not uri or not database:
        raise TaskError("B4 MongoDB acceptance credentials are unavailable")
    identity = f"b4-idempotency-{run_id}"
    document = {"_id": identity, "run_id": run_id, "probe": "idempotent-upsert"}
    client = MongoClient(uri, serverSelectionTimeoutMS=10000)
    try:
        collection = client[database]["b4_acceptance_idempotency"]
        collection.replace_one({"_id": identity}, document, upsert=True)
        collection.replace_one({"_id": identity}, document, upsert=True)
        count = collection.count_documents({"_id": identity})
        observed = collection.find_one({"_id": identity})
    finally:
        client.close()
    if count != 1 or observed != document:
        raise TaskError("MongoDB repeated publication did not converge to one run-scoped document")
    return {"collection": "b4_acceptance_idempotency", "documents_after_repeat": count, "document_sha256": _canonical_digest(document)}


def _check_als_graph_readback(run_id: str) -> dict[str, Any]:
    from pymongo import MongoClient

    uri = os.environ.get("MONGODB_URI")
    database = os.environ.get("MODULE4D_MONGO_DATABASE")
    model = f"als-k8s-{run_id}"
    graph = f"graph-k8s-{run_id}"
    client = MongoClient(uri, serverSelectionTimeoutMS=10000)
    try:
        db = client[database]
        recommendation = db[os.environ.get("MODULE4_RECOMMENDATION_COLLECTION", "user_recommendations")]
        graph_collection = db[os.environ.get("MODULE4_GRAPH_COLLECTION", "product_graph_metrics")]
        recommendation_count = recommendation.count_documents({"model_version": model})
        graph_count = graph_collection.count_documents({"graph_version": graph})
    finally:
        client.close()
    if recommendation_count <= 0 or graph_count <= 0:
        raise TaskError("run-scoped ALS recommendation or graph snapshot is missing from MongoDB")
    return {"model_version": model, "recommendation_documents": recommendation_count, "graph_version": graph, "graph_documents": graph_count}


def _check_hybrid_trend_sensitivity(spark) -> dict[str, Any]:
    from pymongo import MongoClient
    from src.module3.config import Config as StreamConfig
    from src.module3.trend_bundle import BUNDLE_COLLECTION_NAME, write_trend_bundles
    from src.module4.config import Config as Module4Config
    from src.module4.hybrid.config import HybridRankingConfig
    from src.module4.hybrid.data import capture_verified_user_sources
    from src.module4.hybrid.service import serve_hybrid_user
    from src.module4.hybrid.trend_reader import read_verified_trend_snapshot
    from pyspark.sql.types import DoubleType, IntegerType, LongType, StructField, StructType, TimestampType
    import requests

    values = _runtime_values()
    user_id = int(os.environ.get("PIPELINE_DEMO_USER_ID", "1"))
    uri = os.environ["MONGODB_URI"]
    database = os.environ["MODULE4D_MONGO_DATABASE"]
    source_namespace = os.environ.get("PIPELINE_TREND_NAMESPACE", "instacart-k8s-demo-v1")
    as_of = datetime.fromisoformat(os.environ["PIPELINE_AS_OF_EVENT_TIME"].replace("Z", "+00:00"))
    module4_config = Module4Config(
        root=os.environ["PIPELINE_HDFS_URI"], master="local[2]", mongodb_uri=uri,
        mongo_database=database, recommendation_k=20,
    )
    client = MongoClient(uri, serverSelectionTimeoutMS=10000)
    try:
        trend_result = read_verified_trend_snapshot(
            client[database][BUNDLE_COLLECTION_NAME], source_namespace=source_namespace,
            as_of_event_time=as_of, processing_time_utc=datetime.now(timezone.utc),
        )
    finally:
        client.close()
    if not trend_result.available:
        raise TaskError("B4 Hybrid test cannot read a verified trend snapshot for the reviewed run")
    captured = capture_verified_user_sources(
        module4_config, user_id,
        prospective_trend_product_ids=[row.product_id for row in trend_result.trend_recommendations],
    )
    if len(captured.als_recommendations) < 2:
        raise TaskError("B4 Hybrid trend sensitivity needs at least two real run-scoped ALS candidates")
    als = captured.als_recommendations
    catalog = captured.product_catalog
    selected = [row.product_id for row in als[:2]]
    if any(product_id not in catalog for product_id in selected):
        raise TaskError("B4 Hybrid test candidates are missing from the verified graph catalog")
    ranking_config = HybridRankingConfig(lambda_weight=0.5, output_top_k=20)
    processing_time = datetime.now(timezone.utc).replace(microsecond=(datetime.now(timezone.utc).microsecond // 1000) * 1000)
    as_of = as_of.replace(microsecond=(as_of.microsecond // 1000) * 1000)

    def read_api_snapshot() -> dict[str, Any]:
        try:
            response = requests.get(
                f"http://module4d-api:8000/api/v1/recommendations/{user_id}",
                headers={"Cache-Control": "no-cache"}, timeout=15,
            )
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:
            raise TaskError(f"B4 API recommendation readback failed ({type(exc).__name__})") from exc
        if not isinstance(payload, dict) or payload.get("user_id") != user_id or not payload.get("recommendations"):
            raise TaskError("B4 API did not return the ready run-scoped user recommendation snapshot")
        return payload

    baseline_publication = serve_hybrid_user(
        module4_config,
        user_id=user_id,
        source_namespace=source_namespace,
        as_of_event_time=as_of,
        ranking_config=ranking_config,
        processing_time_utc=processing_time,
    )
    if baseline_publication.publication_status not in {"published", "already_active"}:
        raise TaskError("B4 baseline Hybrid publication did not reach a ready state")
    before = read_api_snapshot()

    changed_namespace = f"{source_namespace}-b4-{values['PIPELINE_RUN_ID']}"
    changed_rows = [
        {
            "window_start": trend_result.window_start,
            "window_end": trend_result.window_end,
            "product_id": selected[0],
            "purchase_count_30m": 0,
            "purchase_count_120m": 1,
            "trend_score": 0.0,
            "trend_rank": 2,
        },
        {
            "window_start": trend_result.window_start,
            "window_end": trend_result.window_end,
            "product_id": selected[1],
            "purchase_count_30m": 1,
            "purchase_count_120m": 1,
            "trend_score": 1.0,
            "trend_rank": 1,
        },
    ]
    schema = StructType([
        StructField("window_start", TimestampType(), False),
        StructField("window_end", TimestampType(), False),
        StructField("product_id", LongType(), False),
        StructField("purchase_count_30m", LongType(), False),
        StructField("purchase_count_120m", LongType(), False),
        StructField("trend_score", DoubleType(), False),
        StructField("trend_rank", IntegerType(), False),
    ])
    controlled = spark.createDataFrame(changed_rows, schema=schema)
    stream_config = StreamConfig(
        root=values["PIPELINE_HDFS_URI"], master="local[2]", mongodb_uri=uri,
        mongo_database=database, mongo_top_k_per_window=20,
        publish_trend_bundles=True, trend_source_namespace=changed_namespace,
        trend_source_scope="b4_controlled_trend_sensitivity",
    )
    with MongoClient(uri, serverSelectionTimeoutMS=10000) as writer:
        collection = writer[database][BUNDLE_COLLECTION_NAME]
        publication = write_trend_bundles(
            controlled,
            collection,
            stream_config,
            source_namespace=changed_namespace,
            source_scope="b4_controlled_trend_sensitivity",
            published_at=processing_time,
            retention_seconds=86_400,
        )
    if publication.bundles_created != 1 or publication.trend_rows_represented != 2:
        raise TaskError("B4 controlled trend bundle was not published as one two-product immutable snapshot")
    with MongoClient(uri, serverSelectionTimeoutMS=10000) as reader:
        changed = read_verified_trend_snapshot(
            reader[database][BUNDLE_COLLECTION_NAME],
            source_namespace=changed_namespace,
            as_of_event_time=as_of,
            processing_time_utc=processing_time,
        )
    if not changed.available or changed.content_sha256 is None:
        raise TaskError("B4 controlled trend publication failed verified readback")
    changed_publication = serve_hybrid_user(
        module4_config,
        user_id=user_id,
        source_namespace=changed_namespace,
        as_of_event_time=as_of,
        ranking_config=ranking_config,
        processing_time_utc=processing_time,
    )
    if changed_publication.publication_status not in {"published", "already_active"}:
        raise TaskError("B4 changed-trend Hybrid publication did not reach a ready state")
    after = read_api_snapshot()
    if (
        before.get("source", {}).get("trend_source_namespace") != source_namespace
        or after.get("source", {}).get("trend_source_namespace") != changed_namespace
        or after.get("serving_version") != changed_publication.serving_version
    ):
        raise TaskError("B4 API did not expose the controlled trend source and its new serving version")
    before_top = before["recommendations"][0]
    after_top = after["recommendations"][0]
    if (
        before_top["product_id"] == after_top["product_id"]
        and abs(before_top["final_score"] - after_top["final_score"]) <= 1e-12
    ):
        raise TaskError("published trend change did not alter the API-visible ranking or FinalScore")
    return {
        "before_product_id": before_top["product_id"],
        "before_final_score": before_top["final_score"],
        "after_product_id": after_top["product_id"],
        "after_final_score": after_top["final_score"],
        "verified_trend_content_sha256": trend_result.content_sha256,
        "controlled_trend_content_sha256": changed.content_sha256,
        "als_model_version": captured.model_version,
        "graph_version": captured.graph_version,
        "source_namespace": changed_namespace,
        "baseline_serving_version": baseline_publication.serving_version,
        "changed_serving_version": changed_publication.serving_version,
        "api_readback_verified": True,
        "scope": "published_immutable_trend_change_read_back_and_api_visible",
    }


def run() -> dict[str, Any]:
    values = _runtime_values()
    if os.environ.get("PIPELINE_MODE") not in {"fixture", "full"}:
        raise TaskError("B4 requires a known fixture or full-data mode")
    for step in ("input", "module1", "module2", "module4", "replay", "hybrid"):
        _load_step(step)

    from src.module3.config import Config as StreamConfig

    uri = os.environ.get("MONGODB_URI")
    database = os.environ.get("MODULE4D_MONGO_DATABASE", "")
    if not uri or not database:
        raise TaskError("B4 requires the run-scoped Mongo publisher Secret")
    base_config = StreamConfig(
        root=values["PIPELINE_HDFS_URI"], master="local[2]",
        bootstrap_servers=os.environ["PIPELINE_KAFKA_BOOTSTRAP"],
        topic=values["PIPELINE_TOPIC"], starting_offsets="earliest", fail_on_data_loss=True,
        mongodb_uri=uri, mongo_database=database, mongo_top_k_per_window=20,
        publish_trend_bundles=False, trigger_interval="3 seconds",
        window_short="30 minutes", window_long="120 minutes", slide="5 minutes", watermark="10 minutes",
    )

    module2_receipt = _load_step("module2")["receipt"]
    module4_receipt = _load_step("module4")["receipt"]
    for label, value in (("Module 2", module2_receipt), ("Module 4", module4_receipt)):
        if not isinstance(value.get("artifacts"), list) or not value["artifacts"]:
            raise TaskError(f"{label} did not leave a hashed output artifact inventory")
    module2_experiments = _check_module2_experiments(module2_receipt)
    executor_benchmark = _check_executor_benchmark(module2_receipt)
    replay_receipt = _load_step("replay")["receipt"]
    producer = replay_receipt.get("producer") if isinstance(replay_receipt, dict) else None
    if not isinstance(producer, dict) or producer.get("failed_events") != 0 or producer.get("acked_events") != producer.get("attempted_events"):
        raise TaskError("Module 3 replay receipt does not reconcile all Kafka acknowledgements")
    hybrid_receipt = _load_step("hybrid")["receipt"]
    if not isinstance(hybrid_receipt, dict) or hybrid_receipt.get("status") not in {"published", "already_active"}:
        raise TaskError("Module 4C Hybrid did not publish a ready run-scoped snapshot")
    denied_api = module2_receipt.get("network_policy_denied_api_path")
    if denied_api != {"target": "module4d-api:8000", "connection": "blocked"}:
        raise TaskError("Module 2 pipeline-role deny-path receipt is missing or invalid")

    from src.module3.spark import session

    acceptance_root = RUNTIME_ROOT / "results" / os.environ.get("POD_NAME", "acceptance") / "acceptance"
    acceptance_root.mkdir(parents=True, exist_ok=False)
    spark = session(base_config, acceptance_root / "eventlog")
    try:
        late_data_detail = _check_late_data_and_sink(values["PIPELINE_RUN_ID"], base_config, spark)
        checkpoint_detail = _check_checkpoint_recovery(values["PIPELINE_RUN_ID"], base_config, spark)
        hybrid_trend_detail = _check_hybrid_trend_sensitivity(spark)
    finally:
        spark.stop()

    details = {
        "input_provenance": _check_input_provenance(values["PIPELINE_RUN_ID"], values["PIPELINE_MODE"]),
        "hdfs_readback": _check_hdfs_readback(values["PIPELINE_RUN_ID"]),
        "kafka_delivery": _check_kafka_round_trip(values["PIPELINE_RUN_ID"]),
        "batch_outputs": {"module2": module2_receipt, "module4": module4_receipt},
        "executor_benchmark": executor_benchmark,
        "module2_experiments": module2_experiments,
        "late_data": late_data_detail,
        "checkpoint_recovery": checkpoint_detail,
        "mongo_idempotency": _check_mongo_idempotent(values["PIPELINE_RUN_ID"]),
        "als_graph_readback": _check_als_graph_readback(values["PIPELINE_RUN_ID"]),
        "hybrid_trend_sensitivity": hybrid_trend_detail,
    }

    checks = [
        _check("input_provenance", details["input_provenance"]),
        _check("hdfs_readback", details["hdfs_readback"]),
        _check("kafka_delivery", details["kafka_delivery"]),
        _check("spark_batch_outputs", details["batch_outputs"]),
        _check("spark_query_experiments", details["module2_experiments"]),
        _check("spark_executor_benchmark", details["executor_benchmark"]),
        _check("event_time_watermark_late_data", details["late_data"]),
        _check("checkpoint_recovery", details["checkpoint_recovery"]),
        _check("mongo_idempotent_publication", details["mongo_idempotency"]),
        _check("als_graph_readback", details["als_graph_readback"]),
        _check("hybrid_trend_change", details["hybrid_trend_sensitivity"]),
        _check("network_policy_denied_api_path", denied_api),
    ]
    return {
        "schema_version": 1,
        "run_id": values["PIPELINE_RUN_ID"],
        "mode": values["PIPELINE_MODE"],
        "scope": "fixture_runtime" if values["PIPELINE_MODE"] == "fixture" else "full_data_runtime",
        "status": "completed",
        "checks": checks,
        "details": details,
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def main() -> int:
    try:
        receipt = run()
        _write_json(RUNTIME_ROOT / "evidence" / "acceptance-task.json", receipt)
        print("IT4931_ACCEPTANCE_JSON=" + json.dumps(receipt, sort_keys=True, default=str), flush=True)
        return 0
    except Exception as exc:
        failure = {"schema_version": 1, "status": "failed", "failure_type": type(exc).__name__, "message": "B4 runtime acceptance task failed; see sanitized Job logs"}
        try:
            _write_json(RUNTIME_ROOT / "evidence" / "acceptance-task-failure.json", failure)
        except Exception:
            pass
        print(f"B4 acceptance task failed ({type(exc).__name__}); sensitive details redacted", flush=True)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
