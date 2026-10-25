#!/usr/bin/env python3
"""Bounded live Kafka → Spark → MongoDB → Module 4C integration demo.

The pure fixture, oracle, guards, and evidence validation are also imported by
``tests/test_hybrid_streaming_demo.py``.  Live execution is intended to run
inside the pinned Spark image on the dedicated internal Docker network.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from typing import Any, Callable, Iterable
from urllib.parse import urlsplit


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))


USER_ID = 9401
PRODUCT_IDS = tuple(range(1, 21))
SOURCE_SCOPE = "controlled_synthetic"
WINDOW_LONG = timedelta(hours=2)
WINDOW_SHORT = timedelta(minutes=30)
WINDOW_SLIDE = timedelta(minutes=5)
WATERMARK_DELAY = timedelta(minutes=10)
TARGET_A_START = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
TARGET_A_END = TARGET_A_START + WINDOW_LONG
TARGET_B_START = datetime(2024, 1, 1, 3, 0, tzinfo=timezone.utc)
TARGET_B_END = TARGET_B_START + WINDOW_LONG
FIXTURE_INGESTION_MS = 1_800_000_000_000
KAFKA_HOST = ("c8b-kafka", 9092)
MONGODB_HOST = ("c8b-mongo", 27017)
RESERVED_DATABASES = frozenset({"admin", "config", "instacart", "local"})
REQUIRED_EVENT_FIELDS = (
    "event_id",
    "order_id",
    "user_id",
    "product_id",
    "add_to_cart_order",
    "reordered",
    "aisle_id",
    "department_id",
    "order_dow",
    "order_hour_of_day",
    "event_time_epoch_ms",
    "event_time_iso",
    "ingestion_time_epoch_ms",
)
_RUN_ID_PATTERN = re.compile(r"c8b-[0-9]{8}t[0-9]{6}z-[a-f0-9]{8}\Z")
_DATABASE_PATTERN = re.compile(r"i3_c8b_[a-z0-9_]{8,100}\Z")
_URI_SECRET_PATTERN = re.compile(r"mongodb(?:\+srv)?://[^\s\"'<>]+", re.IGNORECASE)
_INLINE_SECRET_PATTERN = re.compile(
    r"(?i)\b(password|passwd|pwd|token|secret)(\s*[:=]\s*)([^\s,;&]+)"
)
_ANONYMOUS_VOLUME_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_DOCKER_TASK_LABEL = "codex.task"
_DOCKER_RUN_LABEL = "codex.run"
_DOCKER_TASK_VALUE = "c8b"
_CONTAINER_ROLES = (
    "spark_container",
    "kafka_container",
    "mongo_container",
    "owner_helper_container",
)


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
        default=_json_default,
    )


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return timestamp(value)
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"cannot encode {type(value).__name__} as JSON")


def sha256_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def timestamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must include a timezone")
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def parse_timestamp(value: str) -> datetime:
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must include a timezone")
    return parsed.astimezone(timezone.utc)


def _epoch_ms(value: datetime) -> int:
    utc_value = value.astimezone(timezone.utc)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = utc_value - epoch
    return ((delta.days * 86400 + delta.seconds) * 1000) + delta.microseconds // 1000


def _datetime_from_epoch_ms(value: int) -> datetime:
    return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=value)


def validate_run_id(run_id: str) -> None:
    if not isinstance(run_id, str) or _RUN_ID_PATTERN.fullmatch(run_id) is None:
        raise ValueError("run ID must be a unique C8B timestamp and hex suffix")


def resource_names(run_id: str) -> dict[str, str]:
    """Return names that are unique to one validated C8B run."""
    validate_run_id(run_id)
    safe = run_id.replace("-", "_")
    return {
        "network": f"i3-c8b-net-{run_id}",
        "kafka_container": f"i3-c8b-kafka-{run_id}",
        "mongo_container": f"i3-c8b-mongo-{run_id}",
        "spark_container": f"i3-c8b-spark-{run_id}",
        "owner_helper_container": f"i3-c8b-owner-helper-{run_id}",
        "topic": f"i3-c8b-{run_id}",
        "database": f"i3_c8b_{safe}",
        "namespace": f"c8b:controlled-synthetic:{run_id}",
        "checkpoint_id": run_id,
    }


def validate_disposable_database_name(database_name: str) -> None:
    if (
        not isinstance(database_name, str)
        or database_name.lower() in RESERVED_DATABASES
        or _DATABASE_PATTERN.fullmatch(database_name) is None
    ):
        raise ValueError("database must be a unique C8B disposable database name")


def validate_mongodb_target(uri: str) -> None:
    """Allow only credential-free MongoDB on the isolated C8B network."""
    try:
        parsed = urlsplit(uri)
        host = parsed.hostname
        port = parsed.port
    except (TypeError, ValueError) as exc:
        raise ValueError("MongoDB URI is invalid") from exc
    if (
        parsed.scheme != "mongodb"
        or (host, port) != MONGODB_HOST
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in ("", "/")
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("MongoDB URI must target only credential-free c8b-mongo:27017")


def redact_secrets(value: str) -> str:
    value = _URI_SECRET_PATTERN.sub("mongodb://<redacted>", value)
    return _INLINE_SECRET_PATTERN.sub(r"\1\2<redacted>", value)


def _make_event(
    event_id: str,
    order_id: int,
    product_id: int,
    event_time: datetime,
    ingestion_time_epoch_ms: int,
) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "order_id": order_id,
        "user_id": USER_ID,
        "product_id": product_id,
        "add_to_cart_order": 1,
        "reordered": False,
        "aisle_id": 1,
        "department_id": 1,
        "order_dow": event_time.weekday(),
        "order_hour_of_day": event_time.hour,
        "event_time_epoch_ms": _epoch_ms(event_time),
        "event_time_iso": timestamp(event_time),
        "ingestion_time_epoch_ms": ingestion_time_epoch_ms,
    }


def build_event_fixture(run_id: str, *, ingestion_time_epoch_ms: int = FIXTURE_INGESTION_MS) -> dict[str, list[dict[str, Any]]]:
    """Build 48 deterministic, valid synthetic events in four ordered phases."""
    validate_run_id(run_id)
    if type(ingestion_time_epoch_ms) is not int or ingestion_time_epoch_ms <= 0:
        raise ValueError("ingestion_time_epoch_ms must be a positive integer")

    sequence = 0

    def group(prefix: str, entries: Iterable[tuple[int, datetime]]) -> list[dict[str, Any]]:
        nonlocal sequence
        result = []
        for product_id, event_time in entries:
            sequence += 1
            event = _make_event(
                f"{run_id}:{prefix}:{sequence:03d}",
                order_id=sequence,
                product_id=product_id,
                event_time=event_time,
                ingestion_time_epoch_ms=ingestion_time_epoch_ms,
            )
            validate_event(event)
            result.append(event)
        return result

    a_base_time = TARGET_A_END - timedelta(minutes=10)
    b_base_time = TARGET_B_END - timedelta(minutes=10)
    a_entries = [(product_id, a_base_time) for product_id in PRODUCT_IDS]
    a_entries.extend(((1, a_base_time + timedelta(minutes=1)), (1, a_base_time + timedelta(minutes=2))))
    a_entries.sort(key=lambda item: item[1])
    b_entries = [(product_id, b_base_time) for product_id in PRODUCT_IDS]
    b_entries.extend(((2, b_base_time + timedelta(minutes=1)), (2, b_base_time + timedelta(minutes=2))))
    b_entries.sort(key=lambda item: item[1])

    phases = {
        "window_a": group("a", a_entries),
        "advance_a": group(
            "advance-a",
            (
                (20, TARGET_A_END + timedelta(minutes=20)),
                (20, TARGET_A_END + timedelta(minutes=25)),
            ),
        ),
        "window_b": group("b", b_entries),
        "advance_b": group(
            "advance-b",
            (
                (20, TARGET_B_END + timedelta(minutes=20)),
                (20, TARGET_B_END + timedelta(minutes=25)),
            ),
        ),
    }
    validate_fixture(phases)
    return phases


def validate_event(event: dict[str, Any]) -> None:
    if not isinstance(event, dict) or set(event) != set(REQUIRED_EVENT_FIELDS):
        raise ValueError("Kafka event fields do not match the Module 3 event schema")
    if not isinstance(event["event_id"], str) or not event["event_id"].strip():
        raise ValueError("event_id must be a non-empty string")
    for field in ("order_id", "user_id", "product_id", "add_to_cart_order", "aisle_id", "department_id", "event_time_epoch_ms", "ingestion_time_epoch_ms"):
        if type(event[field]) is not int or event[field] <= 0:
            raise ValueError(f"{field} must be a positive integer")
    if type(event["reordered"]) is not bool:
        raise ValueError("reordered must be a boolean")
    if event["product_id"] not in PRODUCT_IDS or event["user_id"] != USER_ID:
        raise ValueError("fixture product or user is outside the controlled catalog")
    if not 0 <= event["order_dow"] <= 6 or not 0 <= event["order_hour_of_day"] <= 23:
        raise ValueError("order calendar fields are outside their valid domain")
    event_time = _datetime_from_epoch_ms(event["event_time_epoch_ms"])
    if timestamp(event_time) != event["event_time_iso"]:
        raise ValueError("event_time_iso does not agree with event_time_epoch_ms")
    if event_time.weekday() != event["order_dow"] or event_time.hour != event["order_hour_of_day"]:
        raise ValueError("order calendar fields do not agree with event time")


def validate_fixture(phases: dict[str, list[dict[str, Any]]]) -> None:
    expected_names = ("window_a", "advance_a", "window_b", "advance_b")
    if tuple(phases) != expected_names:
        raise ValueError("fixture phases must be published in their declared chronological order")
    flattened = []
    for events in phases.values():
        for event in events:
            validate_event(event)
            flattened.append(event)
        times = [event["event_time_epoch_ms"] for event in events]
        if times != sorted(times):
            raise ValueError("events within a phase must be in event-time order")
    event_ids = [event["event_id"] for event in flattened]
    order_ids = [event["order_id"] for event in flattened]
    if len(set(event_ids)) != len(event_ids) or len(set(order_ids)) != len(order_ids):
        raise ValueError("fixture event and order IDs must be unique")
    max_a = max(item["event_time_epoch_ms"] for item in phases["advance_a"])
    max_b = max(item["event_time_epoch_ms"] for item in phases["advance_b"])
    if _datetime_from_epoch_ms(max_a) - WATERMARK_DELAY <= TARGET_A_END:
        raise ValueError("Window A watermark events do not advance strictly beyond its end")
    if _datetime_from_epoch_ms(max_b) - WATERMARK_DELAY <= TARGET_B_END:
        raise ValueError("Window B watermark events do not advance strictly beyond its end")
    if max(event["event_time_epoch_ms"] for event in phases["window_a"]) >= TARGET_A_END.timestamp() * 1000:
        raise ValueError("Window A target events must remain before the target window end")
    if max(event["event_time_epoch_ms"] for event in phases["advance_a"]) >= TARGET_B_START.timestamp() * 1000:
        raise ValueError("Window A watermark events must not enter target Window B")


def target_window_events(phases: dict[str, list[dict[str, Any]]], start: datetime, end: datetime) -> list[dict[str, Any]]:
    start_ms, end_ms = _epoch_ms(start), _epoch_ms(end)
    return [
        event
        for name in ("window_a", "window_b")
        for event in phases[name]
        if start_ms <= event["event_time_epoch_ms"] < end_ms
    ]


def independent_trend_oracle(
    events: Iterable[dict[str, Any]],
    *,
    start: datetime,
    end: datetime,
    top_k: int = 20,
) -> list[dict[str, Any]]:
    """Calculate Module 3 counts, min-max scores, and deterministic order."""
    if type(top_k) is not int or not 1 <= top_k <= len(PRODUCT_IDS):
        raise ValueError("top_k must be an integer from 1 through 20")
    rows = []
    for product_id in PRODUCT_IDS:
        selected = [
            event
            for event in events
            if event["product_id"] == product_id
            and _epoch_ms(start) <= event["event_time_epoch_ms"] < _epoch_ms(end)
        ]
        short_count = sum(
            event["event_time_epoch_ms"] >= _epoch_ms(end - WINDOW_SHORT)
            for event in selected
        )
        long_count = len(selected)
        rows.append(
            {
                "product_id": product_id,
                "purchase_count_30m": int(short_count),
                "purchase_count_120m": int(long_count),
            }
        )
    for field in ("purchase_count_30m", "purchase_count_120m"):
        low = min(row[field] for row in rows)
        high = max(row[field] for row in rows)
        for row in rows:
            row[f"norm_{field}"] = (row[field] - low) / (high - low) if high > low else 0.0
    for row in rows:
        row["trend_score"] = 0.7 * row["norm_purchase_count_30m"] + 0.3 * row["norm_purchase_count_120m"]
    rows.sort(key=lambda row: (-row["trend_score"], row["product_id"]))
    for rank, row in enumerate(rows, start=1):
        row["trend_rank"] = rank
    return rows[:top_k]


def independent_hybrid_oracle(
    trend_rows: Iterable[dict[str, Any]], *, lambda_weight: float = 0.5
) -> list[dict[str, Any]]:
    if isinstance(lambda_weight, bool) or not isinstance(lambda_weight, (int, float)) or not math.isfinite(lambda_weight) or not 0.0 <= lambda_weight <= 1.0:
        raise ValueError("lambda_weight must be finite and within [0, 1]")
    als = {product_id: 3.0 if product_id == 1 else 2.0 if product_id == 2 else 1.0 for product_id in PRODUCT_IDS}
    als_low, als_high = min(als.values()), max(als.values())
    epsilon = 1e-12
    max_trend = max((float(row["trend_score"]) for row in trend_rows), default=0.0)
    trends = {row["product_id"]: float(row["trend_score"]) for row in trend_rows}
    rows = []
    for product_id in PRODUCT_IDS:
        als_norm = (als[product_id] - als_low) / (als_high - als_low + epsilon)
        trend_norm = trends.get(product_id, 0.0) / (max_trend + epsilon) if max_trend else 0.0
        rows.append(
            {
                "product_id": product_id,
                "normalized_als_score": als_norm,
                "normalized_trend_score": trend_norm,
                "final_score": lambda_weight * als_norm + (1.0 - lambda_weight) * trend_norm,
            }
        )
    rows.sort(key=lambda row: (-row["final_score"], row["product_id"]))
    for rank, row in enumerate(rows, start=1):
        row["rank"] = rank
    return rows


def wait_bounded(
    predicate: Callable[[], Any],
    *,
    description: str,
    timeout_seconds: float,
    poll_seconds: float = 0.25,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> Any:
    if timeout_seconds <= 0 or poll_seconds <= 0:
        raise ValueError("wait timeout and poll interval must be positive")
    deadline = monotonic() + timeout_seconds
    last_observation: Any = None
    while monotonic() < deadline:
        last_observation = predicate()
        if last_observation:
            return last_observation
        sleep(min(poll_seconds, max(0.0, deadline - monotonic())))
    raise TimeoutError(f"Timed out waiting for {description}; last={last_observation!r}")


def validate_report(report: dict[str, Any]) -> None:
    required = {
        "experiment_id", "status", "git_commit", "git_dirty", "source_scope",
        "source_namespace", "dataset_type", "kafka_topic", "event_counts",
        "spark_version", "kafka_version", "mongodb_version", "window_configuration",
        "watermark_configuration", "checkpoint_identity", "target_windows",
        "spark_progress_summary", "bundle_identities", "bundle_content_hashes",
        "serving_requests", "serving_versions", "ranking_before", "ranking_after",
        "readiness_checks", "retry_checks", "timing_observations", "cleanup_status",
        "limitations", "recommendation_accuracy_measured", "production_latency_benchmark",
    }
    missing = required.difference(report)
    if missing:
        raise ValueError(f"report is missing required fields: {sorted(missing)}")

    def inspect(value: Any, path: str = "report") -> None:
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError(f"{path} contains a non-finite number")
        if isinstance(value, dict):
            for key, child in value.items():
                if "event_to_serving_latency" in key or "event_time_distance_wall_clock" in key:
                    raise ValueError("report must not fabricate event-to-serving latency")
                inspect(child, f"{path}.{key}")
        elif isinstance(value, (list, tuple)):
            for index, child in enumerate(value):
                inspect(child, f"{path}[{index}]")

    inspect(report)
    serialized = canonical_json(report)
    if re.search(r"mongodb(?:\+srv)?://[^/@\s:]+:[^/@\s]+@", serialized, re.IGNORECASE):
        raise ValueError("report contains a credential-bearing MongoDB URI")
    if report.get("source_scope") != SOURCE_SCOPE:
        raise ValueError("report must retain the controlled_synthetic source scope")
    if report.get("dataset_type") != "controlled synthetic Kafka fixture":
        raise ValueError("report must identify the controlled synthetic dataset")
    if report.get("recommendation_accuracy_measured") is not False:
        raise ValueError("report must state recommendation accuracy was not measured")
    if report.get("production_latency_benchmark") is not False:
        raise ValueError("report must state this is not a production latency benchmark")
    if "event_time_distance_is_wall_clock_latency" in serialized:
        raise ValueError("event-time distance cannot be described as wall-clock latency")


def _jsonl_append(path: Path, value: Any) -> None:
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(value, sort_keys=True, separators=(",", ":"), default=_json_default) + "\n")
        stream.flush()


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True, default=_json_default) + "\n", encoding="utf-8")


def _progress_watermark(progress: dict[str, Any]) -> datetime | None:
    value = (progress.get("eventTime") or {}).get("watermark")
    if not value:
        return None
    return parse_timestamp(value)


def _offset_from_progress(progress: dict[str, Any], topic: str) -> int | None:
    for source in progress.get("sources") or []:
        end_offset = source.get("endOffset")
        if isinstance(end_offset, str):
            try:
                end_offset = json.loads(end_offset)
            except json.JSONDecodeError:
                continue
        if not isinstance(end_offset, dict):
            continue
        topic_offsets = end_offset.get(topic)
        if isinstance(topic_offsets, dict) and "0" in topic_offsets:
            try:
                return int(topic_offsets["0"])
            except (TypeError, ValueError):
                continue
        if "0" in end_offset:
            try:
                return int(end_offset["0"])
            except (TypeError, ValueError):
                continue
    return None


def _progress_record(progress: dict[str, Any], phase: str) -> dict[str, Any]:
    return {
        "phase": phase,
        "batchId": progress.get("batchId"),
        "numInputRows": progress.get("numInputRows"),
        "durationMs": progress.get("durationMs") or {},
        "eventTime": progress.get("eventTime") or {},
        "observedMetrics": progress.get("observedMetrics") or {},
        "stateOperators": progress.get("stateOperators") or [],
        "sources": progress.get("sources") or [],
        "sink": progress.get("sink") or {},
        "timestamp": progress.get("timestamp"),
    }


def _send_phase(bootstrap_servers: str, topic: str, events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks="all",
        retries=3,
        max_in_flight_requests_per_connection=1,
        request_timeout_ms=10000,
        delivery_timeout_ms=15000,
        value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode("utf-8"),
        key_serializer=lambda key: str(key).encode("utf-8"),
    )
    acknowledgements = []
    try:
        for template in events:
            event = dict(template)
            event["ingestion_time_epoch_ms"] = int(time.time() * 1000)
            started = time.perf_counter()
            metadata = producer.send(topic, key=event["user_id"], value=event).get(timeout=15)
            acknowledgements.append(
                {
                    "event_id": event["event_id"],
                    "partition": metadata.partition,
                    "offset": metadata.offset,
                    "ack_wall_ms": round((time.perf_counter() - started) * 1000, 3),
                    "ingestion_time_epoch_ms": event["ingestion_time_epoch_ms"],
                }
            )
        producer.flush(timeout=15)
    finally:
        producer.close(timeout=10)
    return acknowledgements


def _topic_offsets(bootstrap_servers: str, topic: str) -> dict[str, Any]:
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers, request_timeout_ms=10000)
    try:
        partition = TopicPartition(topic, 0)
        return {
            "beginning": int(consumer.beginning_offsets([partition])[partition]),
            "end": int(consumer.end_offsets([partition])[partition]),
        }
    finally:
        consumer.close()


def _wait_progress(query: Any, topic: str, target_offset: int, *, timeout_seconds: float = 35.0) -> dict[str, Any]:
    def find() -> dict[str, Any] | None:
        for progress in reversed(list(query.recentProgress)):
            offset = _offset_from_progress(progress, topic)
            if offset is not None and offset >= target_offset:
                return progress
        return None

    progress = wait_bounded(find, description=f"Spark consumption through Kafka offset {target_offset}", timeout_seconds=timeout_seconds)
    quality = (progress.get("observedMetrics") or {}).get("record_quality") or {}
    invalid = int(quality.get("invalid_records") or 0)
    valid = int(quality.get("valid_records") or 0)
    if invalid:
        raise RuntimeError(f"Spark reported {invalid} invalid records")
    if int(progress.get("numInputRows") or 0) and valid != int(progress.get("numInputRows") or 0):
        raise RuntimeError("Spark record_quality valid count did not match input rows")
    return progress


def _wait_for_bundle(database: Any, namespace: str, window_end: datetime, *, timeout_seconds: float = 35.0) -> dict[str, Any]:
    from src.module3.trend_bundle import BUNDLE_COLLECTION_NAME

    collection = database[BUNDLE_COLLECTION_NAME]

    def find() -> dict[str, Any] | None:
        return collection.find_one(
            {"source_namespace": namespace, "window_end": window_end}
        )

    return wait_bounded(
        find,
        description=f"atomic trend bundle ending {timestamp(window_end)}",
        timeout_seconds=timeout_seconds,
    )


def _assert_trend_bundle(
    database: Any,
    bundle: dict[str, Any],
    expected: list[dict[str, Any]],
    *,
    namespace: str,
    window_start: datetime,
    window_end: datetime,
) -> dict[str, Any]:
    from src.module3.trend_bundle import BUNDLE_COLLECTION_NAME
    from src.module4.hybrid.trend_reader import read_verified_trend_snapshot

    if bundle.get("source_namespace") != namespace or bundle.get("source_scope") != SOURCE_SCOPE:
        raise RuntimeError("C4B bundle namespace or controlled source scope is incorrect")
    if bundle.get("window_start") != window_start or bundle.get("window_end") != window_end:
        raise RuntimeError("C4B published unexpected target window boundaries")
    items = bundle.get("items") or []
    if bundle.get("row_count") != 20 or bundle.get("top_k") != 20 or len(items) != 20:
        raise RuntimeError("C4B target bundle is not a complete Top-20")
    actual = sorted(items, key=lambda row: row.get("trend_rank", 0))
    if [row.get("trend_rank") for row in actual] != list(range(1, 21)):
        raise RuntimeError("C4B ranks are not contiguous and deterministic")
    for got, want in zip(actual, expected, strict=True):
        if got["product_id"] != want["product_id"]:
            raise RuntimeError("C4B product order differs from the independent event-count oracle")
        if got["purchase_count_30m"] != want["purchase_count_30m"] or got["purchase_count_120m"] != want["purchase_count_120m"]:
            raise RuntimeError("C4B stored counts differ from the independent event-count oracle")
        if not math.isclose(float(got["trend_score"]), want["trend_score"], rel_tol=0.0, abs_tol=1e-12):
            raise RuntimeError("C4B stored score differs from the independent trend oracle")

    c5 = read_verified_trend_snapshot(
        database[BUNDLE_COLLECTION_NAME],
        source_namespace=namespace,
        as_of_event_time=window_end,
    )
    if not c5.available or c5.status != "available" or c5.content_sha256 != bundle.get("content_sha256"):
        raise RuntimeError("C5 did not verify the finalized C4B bundle")
    if c5.window_start != window_start or c5.window_end != window_end or len(c5.trend_recommendations) != 20:
        raise RuntimeError("C5 returned incomplete or differently bounded trend rows")
    raw_rows = database["realtime_trending"].count_documents(
        {"window_start": window_start, "window_end": window_end}
    )
    if raw_rows != 20:
        raise RuntimeError("the existing mutable realtime_trending sink lacks the target Top-20 rows")
    return {
        "bundle_id": bundle.get("_id"),
        "content_sha256": bundle.get("content_sha256"),
        "config_hash": bundle.get("config_hash"),
        "row_count": len(items),
        "window_start": timestamp(window_start),
        "window_end": timestamp(window_end),
        "top1_product_id": actual[0]["product_id"],
        "c5_status": c5.status,
        "c5_rows": len(c5.trend_recommendations),
        "mutable_raw_rows": raw_rows,
    }


def _seed_als_graph(database: Any, *, run_id: str) -> dict[str, Any]:
    from src.module4.hybrid.data import READINESS_COLLECTION

    source_run_id = f"c8b-source-{run_id}"
    model_version = f"c8b-als-{run_id}"
    graph_version = f"c8b-graph-{run_id}"
    readiness = {
        "_id": "active",
        "status": "ready",
        "ready": True,
        "run_id": source_run_id,
        "model_version": model_version,
        "graph_version": graph_version,
        "recommendation_published": True,
        "graph_published": True,
        "single_writer_policy": "module4_publication_lock",
        "updated_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "c8b_fixture_marker": run_id,
    }
    database[READINESS_COLLECTION].insert_one(readiness)
    database["user_recommendations"].insert_many(
        [
            {
                "_id": f"c8b-als:{USER_ID}:{product_id}",
                "user_id": USER_ID,
                "product_id": product_id,
                "als_score": 3.0 if product_id == 1 else 2.0 if product_id == 2 else 1.0,
                "als_rank": rank,
                "model_version": model_version,
                "run_id": source_run_id,
            }
            for rank, product_id in enumerate(PRODUCT_IDS, start=1)
        ]
    )
    database["product_graph_metrics"].insert_many(
        [
            {
                "_id": f"c8b-graph:{product_id}",
                "product_id": product_id,
                "product_name": f"C8B Synthetic Product {product_id}",
                "graph_version": graph_version,
                "run_id": source_run_id,
            }
            for product_id in PRODUCT_IDS
        ]
    )
    return readiness


def expected_source_provenance(readiness: dict[str, Any]) -> dict[str, str]:
    """Translate the active Module 4 readiness names to CLI provenance fields."""
    return {
        "source_run_id": readiness["run_id"],
        "model_version": readiness["model_version"],
        "graph_version": readiness["graph_version"],
    }


def _read_active(database: Any) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    from src.module4.hybrid.data import READINESS_COLLECTION
    from src.module4.hybrid.sink_mongo import FINAL_RECOMMENDATIONS_COLLECTION

    pointer = database[READINESS_COLLECTION].find_one({"_id": f"hybrid:{USER_ID}"})
    if pointer is None or pointer.get("ready") is not True or pointer.get("status") != "ready":
        raise RuntimeError("Module 4C did not publish a ready per-user pointer")
    rows = list(
        database[FINAL_RECOMMENDATIONS_COLLECTION]
        .find({"user_id": USER_ID, "serving_version": pointer.get("serving_version")})
        .sort("rank", 1)
    )
    expected = pointer.get("expected_rows")
    if expected != 20 or len(rows) != expected or [row.get("rank") for row in rows] != list(range(1, expected + 1)):
        raise RuntimeError("ready pointer does not resolve to a complete Top-20 snapshot")
    if any(row.get("user_id") != USER_ID for row in rows):
        raise RuntimeError("recommendation snapshot contains another user's rows")
    return pointer, rows


def _invoke_cli(
    *,
    database_name: str,
    mongodb_uri: str,
    namespace: str,
    as_of: datetime,
    lambda_weight: float,
) -> tuple[dict[str, Any], float, list[str]]:
    command = [
        sys.executable,
        "-m",
        "src.module4.hybrid.cli",
        "serve-user",
        "--user-id",
        str(USER_ID),
        "--source-namespace",
        namespace,
        "--as-of-event-time",
        timestamp(as_of),
        "--mongodb-uri",
        mongodb_uri,
        "--mongo-database",
        database_name,
        "--lambda-weight",
        format(lambda_weight, ".17g"),
    ]
    safe_command = [redact_secrets(part) for part in command]
    process_env = os.environ.copy()
    process_env["PYTHONDONTWRITEBYTECODE"] = "1"
    started = time.perf_counter()
    try:
        completed = subprocess.run(
            command,
            cwd="/workspace",
            env=process_env,
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise TimeoutError(f"Module 4C CLI timed out after 60 seconds: {safe_command}") from exc
    duration_ms = round((time.perf_counter() - started) * 1000, 3)
    if completed.returncode != 0:
        raise RuntimeError(
            f"Module 4C CLI exited {completed.returncode}: {redact_secrets(completed.stderr.strip())}"
        )
    if completed.stderr.strip():
        raise RuntimeError(f"Module 4C CLI wrote stderr: {redact_secrets(completed.stderr.strip())}")
    try:
        result = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Module 4C CLI did not return a JSON success document") from exc
    if result.get("status") not in {"published", "already_active"}:
        raise RuntimeError("Module 4C CLI did not confirm a ready publication")
    return result, duration_ms, safe_command


def _expected_hybrid_rows(trend_rows: list[dict[str, Any]], lambda_weight: float) -> list[dict[str, Any]]:
    return independent_hybrid_oracle(trend_rows, lambda_weight=lambda_weight)


def _verify_hybrid_rows(actual_rows: list[dict[str, Any]], expected_rows: list[dict[str, Any]]) -> None:
    if [row.get("product_id") for row in actual_rows] != [row["product_id"] for row in expected_rows]:
        raise RuntimeError("Module 4C rank order differs from the independent hybrid oracle")
    for actual, expected in zip(actual_rows, expected_rows, strict=True):
        score = float(actual["final_score"])
        if not math.isclose(score, expected["final_score"], rel_tol=0.0, abs_tol=5e-14):
            raise RuntimeError("Module 4C final score differs from the independent hybrid oracle")


def _latest_offsets_for_acknowledgements(acks: list[dict[str, Any]]) -> int:
    return max((int(item["offset"]) for item in acks), default=-1) + 1


def _capture_state_metrics(progresses: list[dict[str, Any]]) -> dict[str, Any]:
    durations = []
    add_batch = []
    input_rows = 0
    quality_valid = 0
    quality_invalid = 0
    duplicate_drops = 0
    watermark_drops = 0
    watermarks = []
    batch_ids = []
    seen_batches = set()
    for progress in progresses:
        batch_id = progress.get("batchId")
        if batch_id is not None and batch_id in seen_batches:
            continue
        if batch_id is not None:
            seen_batches.add(batch_id)
        duration = progress.get("durationMs") or {}
        if isinstance(duration.get("triggerExecution"), (int, float)):
            durations.append(float(duration["triggerExecution"]))
        if isinstance(duration.get("addBatch"), (int, float)):
            add_batch.append(float(duration["addBatch"]))
        input_rows += int(progress.get("numInputRows") or 0)
        metrics = (progress.get("observedMetrics") or {}).get("record_quality") or {}
        quality_valid += int(metrics.get("valid_records") or 0)
        quality_invalid += int(metrics.get("invalid_records") or 0)
        for operator in progress.get("stateOperators") or []:
            custom = operator.get("customMetrics") or {}
            duplicate_drops += int(custom.get("numDroppedDuplicateRows") or 0)
            watermark_drops += int(operator.get("numRowsDroppedByWatermark") or 0)
        mark = _progress_watermark(progress)
        if mark is not None:
            watermarks.append(timestamp(mark))
        if progress.get("batchId") is not None:
            batch_ids.append(int(progress["batchId"]))
    return {
        "progress_records": len(progresses),
        "batch_ids": sorted(set(batch_ids)),
        "input_rows": input_rows,
        "quality_valid_records": quality_valid,
        "quality_invalid_records": quality_invalid,
        "duplicate_rows_dropped": duplicate_drops,
        "rows_dropped_by_watermark": watermark_drops,
        "event_time_watermarks": watermarks,
        "microbatch_trigger_execution_ms": durations,
        "microbatch_add_batch_ms": add_batch,
    }


def _empty_report(run_id: str, git_commit: str, dirty_status: list[str], names: dict[str, str]) -> dict[str, Any]:
    return {
        "experiment_id": run_id,
        "status": "failed",
        "git_commit": git_commit,
        "git_dirty": bool(dirty_status),
        "git_status_lines": dirty_status,
        "source_scope": SOURCE_SCOPE,
        "source_namespace": names["namespace"],
        "dataset_type": "controlled synthetic Kafka fixture",
        "recommendation_accuracy_measured": False,
        "production_latency_benchmark": False,
        "kafka_topic": names["topic"],
        "event_counts": {"planned": 48, "acknowledged": 0, "consumed": 0},
        "spark_version": None,
        "kafka_version": "3.7.0",
        "mongodb_version": None,
        "window_configuration": {
            "window_short": "30 minutes",
            "window_long": "120 minutes",
            "slide": "5 minutes",
            "trigger_interval": "2 seconds",
            "weights": {"short": 0.7, "long": 0.3},
            "top_k": 20,
        },
        "watermark_configuration": {"delay": "10 minutes", "semantics": "Spark Structured Streaming event-time watermark"},
        "checkpoint_identity": names["checkpoint_id"],
        "target_windows": {},
        "spark_progress_summary": {},
        "bundle_identities": [],
        "bundle_content_hashes": [],
        "serving_requests": [],
        "serving_versions": [],
        "ranking_before": {},
        "ranking_after": {},
        "readiness_checks": {},
        "retry_checks": {},
        "timing_observations": {
            "kafka_producer_ack_wall_ms": [],
            "spark_microbatch_duration_ms": {},
            "processing_time_to_finalized_publication_ms": {},
            "module4c_cli_wall_ms": [],
        },
        "cleanup_status": {
            "mongodb_database_dropped": False,
            "spark_started": False,
            "spark_query_started": False,
            "spark_query_stopped": False,
            "docker_resources_removed": False,
        },
        "limitations": [
            "Synthetic serving behavior does not measure recommendation accuracy.",
            "This is not a production Kafka or serving latency benchmark.",
            "Streaming checkpoint restart behavior was not exercised.",
            "Source-generation race coverage remains dependent on I2 C6 and is outside this seeded-source demonstration.",
        ],
    }


def run_live(args: argparse.Namespace) -> int:
    from kafka import KafkaAdminClient
    from kafka.admin import NewTopic
    from kafka.errors import TopicAlreadyExistsError
    from pymongo import MongoClient
    from pyspark.sql import SparkSession

    from src.module3.config import Config as Module3Config
    from src.module3.stream import run_stream
    from src.module4.hybrid.data import READINESS_COLLECTION
    from src.module4.hybrid.sink_mongo import FINAL_RECOMMENDATIONS_COLLECTION

    validate_run_id(args.run_id)
    validate_mongodb_target(args.mongodb_uri)
    names = resource_names(args.run_id)
    if args.topic != names["topic"] or args.database != names["database"]:
        raise ValueError("Kafka topic or MongoDB database does not match this isolated C8B run")
    validate_disposable_database_name(args.database)
    if str(Path(args.output_dir).resolve()) != "/demo-output":
        raise ValueError("live evidence output must be the dedicated /demo-output mount")
    git_commit = os.environ.get("C8B_GIT_COMMIT", "")
    try:
        git_status = json.loads(os.environ["C8B_GIT_STATUS_JSON"])
    except (KeyError, json.JSONDecodeError) as exc:
        raise RuntimeError("host-verified C8B Git status metadata is required") from exc
    if re.fullmatch(r"[0-9a-f]{40,64}", git_commit) is None or not isinstance(git_status, list) or any(not isinstance(line, str) for line in git_status):
        raise RuntimeError("invalid host-verified Git metadata")

    output_dir = Path("/demo-output")
    output_dir.mkdir(parents=True, exist_ok=True)
    report = _empty_report(args.run_id, git_commit, git_status, names)
    state: dict[str, Any] = {"phase": "preflight", "last_successful_phase": None}
    client = None
    spark = None
    database_owned = False
    failure: BaseException | None = None
    namespace = names["namespace"]
    topic = names["topic"]
    bootstrap = "c8b-kafka:9092"
    mongodb_uri = args.mongodb_uri
    acknowledgements: list[dict[str, Any]] = []
    progress_rows: list[dict[str, Any]] = []
    phase_counts: dict[str, int] = {}

    try:
        client = MongoClient(
            mongodb_uri,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
            tz_aware=True,
        )
        client.admin.command("ping")
        mongo_version = client.server_info()["version"]
        if mongo_version != "7.0.16":
            raise RuntimeError(f"expected MongoDB 7.0.16, received {mongo_version}")
        report["mongodb_version"] = mongo_version
        if args.database in client.list_database_names():
            raise RuntimeError("generated C8B MongoDB database already exists")
        database = client[args.database]
        database["_c8b_ownership"].insert_one({"_id": "owner", "run_id": args.run_id})
        database_owned = True
        state["last_successful_phase"] = "MongoDB readiness and disposable database ownership"

        admin = KafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=10000)
        try:
            existing_topics = set(admin.list_topics())
            if topic in existing_topics:
                raise RuntimeError("unique C8B Kafka topic unexpectedly already exists")
            try:
                admin.create_topics([NewTopic(name=topic, num_partitions=1, replication_factor=1)], timeout_ms=10000)
            except TopicAlreadyExistsError as exc:
                raise RuntimeError("unique C8B topic creation raced with another writer") from exc
        finally:
            admin.close()
        state["last_successful_phase"] = "unique one-partition Kafka topic creation"
        fixture = build_event_fixture(args.run_id)
        expected_a = independent_trend_oracle(
            target_window_events(fixture, TARGET_A_START, TARGET_A_END),
            start=TARGET_A_START,
            end=TARGET_A_END,
        )
        expected_b = independent_trend_oracle(
            target_window_events(fixture, TARGET_B_START, TARGET_B_END),
            start=TARGET_B_START,
            end=TARGET_B_END,
        )
        if expected_a[0]["product_id"] != 1 or expected_b[0]["product_id"] != 2:
            raise RuntimeError("controlled event distributions did not produce expected trend winners")
        readiness_before = _seed_als_graph(database, run_id=args.run_id)
        state["last_successful_phase"] = "synthetic ALS/graph source seeding"

        Path("/tmp/c8b/ivy").mkdir(parents=True, exist_ok=True)
        spark = (
            SparkSession.builder.master("local[2]")
            .appName(f"module4c-c8b-{args.run_id}")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.default.parallelism", "2")
            .config("spark.sql.warehouse.dir", "file:///tmp/c8b/warehouse")
            .config("spark.local.dir", "/tmp/c8b/spark")
            .config("spark.jars.ivy", "/tmp/c8b/ivy")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.memory", "1g")
            .config("spark.executor.memory", "1g")
            .getOrCreate()
        )
        report["spark_version"] = spark.version
        report["cleanup_status"]["spark_started"] = True
        spark.sparkContext.setLogLevel("WARN")
        if spark.version != "3.5.5":
            raise RuntimeError(f"expected Spark 3.5.5, received {spark.version}")
        state["last_successful_phase"] = "Spark 3.5.5 local runtime startup"
        config = Module3Config(
            root="file:///tmp/c8b-dataset",
            master="local[2]",
            shuffle_partitions=2,
            bootstrap_servers=bootstrap,
            topic=topic,
            starting_offsets="earliest",
            fail_on_data_loss=True,
            window_short="30 minutes",
            window_long="120 minutes",
            slide="5 minutes",
            watermark="10 minutes",
            weight_short=0.7,
            weight_long=0.3,
            trigger_interval="2 seconds",
            mongodb_uri=mongodb_uri,
            mongo_database=args.database,
            mongo_collection="realtime_trending",
            mongo_top_k_per_window=20,
            ttl_seconds=0,
            publish_trend_bundles=True,
            trend_source_namespace=namespace,
            trend_source_scope=SOURCE_SCOPE,
        )
        phase_evidence: dict[str, Any] = {}
        cli_requests: list[dict[str, Any]] = []
        publication_delays: dict[str, float] = {}

        def orchestrate(query: Any) -> dict[str, Any]:
            report["cleanup_status"]["spark_query_started"] = True
            phase_order = ("window_a", "advance_a", "window_b", "advance_b")
            offsets_by_phase: dict[str, int] = {}
            for phase in phase_order:
                state["phase"] = phase
                phase_started = time.perf_counter()
                phase_acks = _send_phase(bootstrap, topic, fixture[phase])
                last_watermark_ack_at = time.perf_counter()
                acknowledgements.extend({"phase": phase, **ack} for ack in phase_acks)
                report["timing_observations"]["kafka_producer_ack_wall_ms"].extend(
                    {
                        "phase": phase,
                        "event_id": item["event_id"],
                        "ack_wall_ms": item["ack_wall_ms"],
                    }
                    for item in phase_acks
                )
                phase_counts[phase] = len(phase_acks)
                report["event_counts"]["acknowledged"] = len(acknowledgements)
                report["event_counts"]["by_phase"] = dict(phase_counts)
                offsets_by_phase[phase] = _latest_offsets_for_acknowledgements(phase_acks)
                progress = _wait_progress(query, topic, offsets_by_phase[phase], timeout_seconds=45)
                record = _progress_record(progress, phase)
                progress_rows.append(record)
                current_progress = _capture_state_metrics(progress_rows)
                report["spark_progress_summary"] = current_progress
                report["event_counts"]["consumed"] = current_progress["quality_valid_records"]
                report["event_counts"]["invalid"] = current_progress["quality_invalid_records"]
                _jsonl_append(output_dir / "orchestration_progress.jsonl", record)
                phase_evidence[phase] = {
                    "acknowledged_count": len(phase_acks),
                    "offset_start": min(item["offset"] for item in phase_acks),
                    "offset_end_exclusive": offsets_by_phase[phase],
                    "spark_batch_id": progress.get("batchId"),
                    "spark_input_rows": progress.get("numInputRows"),
                    "spark_end_offset": _offset_from_progress(progress, topic),
                    "record_quality": (progress.get("observedMetrics") or {}).get("record_quality") or {},
                    "wall_ms_to_consumption": round((time.perf_counter() - phase_started) * 1000, 3),
                }
                if phase in {"advance_a", "advance_b"}:
                    window_label = "A" if phase == "advance_a" else "B"
                    target_end = TARGET_A_END if window_label == "A" else TARGET_B_END
                    target_start = TARGET_A_START if window_label == "A" else TARGET_B_START
                    watermark_deadline = time.monotonic() + 35
                    watermark_progress = None
                    while time.monotonic() < watermark_deadline:
                        recent = list(query.recentProgress)
                        for candidate in reversed(recent):
                            mark = _progress_watermark(candidate)
                            if mark is not None and mark > target_end:
                                watermark_progress = candidate
                                break
                        if watermark_progress is not None:
                            break
                        if not query.isActive:
                            raise RuntimeError("Spark query stopped before target watermark advancement")
                        time.sleep(0.2)
                    if watermark_progress is None:
                        raise TimeoutError(f"Spark watermark did not advance strictly beyond target Window {window_label}")
                    progress_record = _progress_record(watermark_progress, f"watermark_{window_label.lower()}")
                    progress_rows.append(progress_record)
                    report["spark_progress_summary"] = _capture_state_metrics(progress_rows)
                    _jsonl_append(output_dir / "orchestration_progress.jsonl", progress_record)
                    bundle = _wait_for_bundle(database, namespace, target_end, timeout_seconds=45)
                    publication_delays[window_label] = round((time.perf_counter() - last_watermark_ack_at) * 1000, 3)
                    expected = expected_a if window_label == "A" else expected_b
                    bundle_info = _assert_trend_bundle(
                        database,
                        bundle,
                        expected,
                        namespace=namespace,
                        window_start=target_start,
                        window_end=target_end,
                    )
                    report["bundle_identities"].append(bundle_info["bundle_id"])
                    report["bundle_content_hashes"].append(bundle_info["content_sha256"])
                    report["target_windows"][window_label] = {
                        "start": timestamp(target_start),
                        "end": timestamp(target_end),
                        **bundle_info,
                    }
                    phase_evidence[f"finalized_window_{window_label}"] = {
                        **bundle_info,
                        "spark_watermark": timestamp(_progress_watermark(watermark_progress)),
                        "progress_batch_id": watermark_progress.get("batchId"),
                        "processing_time_from_last_watermark_ack_to_bundle_visible_ms": publication_delays[window_label],
                    }
                    as_of = target_end
                    cli_result, cli_ms, safe_command = _invoke_cli(
                        database_name=args.database,
                        mongodb_uri=mongodb_uri,
                        namespace=namespace,
                        as_of=as_of,
                        lambda_weight=0.5,
                    )
                    pointer, rows = _read_active(database)
                    if cli_result.get("serving_version") != pointer.get("serving_version"):
                        raise RuntimeError("CLI output and active serving pointer disagree")
                    if cli_result.get("trend_content_sha256") != bundle_info["content_sha256"]:
                        raise RuntimeError("CLI did not serve from the verified atomic bundle")
                    if cli_result.get("trend_window_end") != timestamp(target_end):
                        raise RuntimeError("CLI selected an unexpected finalized trend window")
                    if cli_result.get("trend_source_namespace") != namespace:
                        raise RuntimeError("CLI returned a different trend namespace")
                    if cli_result.get("ranking_mode") != "hybrid":
                        raise RuntimeError("CLI did not serve the expected warm-user hybrid result")
                    provenance = expected_source_provenance(readiness_before)
                    if any(cli_result.get(key) != value for key, value in provenance.items()):
                        raise RuntimeError("CLI source provenance does not match the seeded active A/B snapshot")
                    if cli_result.get("readiness_pointer_id") != f"hybrid:{USER_ID}":
                        raise RuntimeError("CLI returned an unexpected per-user readiness pointer")
                    trend_oracle = expected_a if window_label == "A" else expected_b
                    expected_hybrid = _expected_hybrid_rows(trend_oracle, 0.5)
                    _verify_hybrid_rows(rows, expected_hybrid)
                    expected_top = 1 if window_label == "A" else 2
                    if rows[0]["product_id"] != expected_top:
                        raise RuntimeError(f"lambda=0.5 Window {window_label} Top-1 did not match the independent oracle")
                    cli_requests.append(
                        {
                            "window": window_label,
                            "as_of_event_time": timestamp(as_of),
                            "status": cli_result["status"],
                            "serving_version": cli_result["serving_version"],
                            "trend_window_end": cli_result["trend_window_end"],
                            "trend_content_sha256": cli_result["trend_content_sha256"],
                            "readiness_pointer_id": cli_result["readiness_pointer_id"],
                            "pointer_status": pointer.get("status"),
                            "pointer_ready": pointer.get("ready"),
                            "published_rows": len(rows),
                            "top_five": [
                                {"product_id": row["product_id"], "final_score": float(row["final_score"])}
                                for row in rows[:5]
                            ],
                            "lambda_weight": 0.5,
                            "ranking_mode": cli_result["ranking_mode"],
                            "source_provenance": provenance,
                            "command": safe_command,
                            "cli_wall_ms": cli_ms,
                        }
                    )
                    report["serving_requests"] = list(cli_requests)
                    report["serving_versions"] = [
                        item["serving_version"] for item in cli_requests
                    ]
                    report["timing_observations"]["module4c_cli_wall_ms"] = [
                        {"window": item["window"], "status": item["status"], "wall_ms": item["cli_wall_ms"]}
                        for item in cli_requests
                    ]
                    report["ranking_before" if window_label == "A" else "ranking_after"] = {
                        "window": window_label,
                        "top1_product_id": rows[0]["product_id"],
                        "top5": cli_requests[-1]["top_five"],
                        "expected_top1_product_id": expected_top,
                    }
                    phase_evidence[f"serving_{window_label}"] = {
                        "serving_version": pointer["serving_version"],
                        "as_of_event_time": timestamp(as_of),
                        "trend_window_end": cli_result["trend_window_end"],
                        "top1_product_id": rows[0]["product_id"],
                        "ready": pointer.get("ready"),
                        "expected_top1_product_id": expected_top,
                    }
                    if window_label == "A":
                        phase_evidence["_a_version"] = pointer["serving_version"]
                        phase_evidence["_a_rows"] = [dict(row) for row in rows]
                    else:
                        phase_evidence["_b_version"] = pointer["serving_version"]
                        phase_evidence["_b_rows"] = [dict(row) for row in rows]
                    if database[READINESS_COLLECTION].find_one({"_id": "active"}) != readiness_before:
                        raise RuntimeError("serving modified the accepted A/B readiness document")
                state["last_successful_phase"] = f"published and verified {phase} phase"
            # Same request retry must be idempotent; it does not restart the stream.
            retry_result, retry_ms, retry_command = _invoke_cli(
                database_name=args.database,
                mongodb_uri=mongodb_uri,
                namespace=namespace,
                as_of=TARGET_B_END,
                lambda_weight=0.5,
            )
            pointer_after_retry, retry_rows = _read_active(database)
            b_version = phase_evidence["_b_version"]
            if retry_result.get("status") != "already_active" or retry_result.get("serving_version") != b_version:
                raise RuntimeError("same-request retry did not report the existing active serving version")
            if retry_result.get("ranking_mode") != "hybrid":
                raise RuntimeError("same-request retry returned an unexpected ranking mode")
            if (
                retry_result.get("trend_window_end") != timestamp(TARGET_B_END)
                or retry_result.get("trend_content_sha256")
                != phase_evidence["finalized_window_B"]["content_sha256"]
                or retry_result.get("trend_source_namespace") != namespace
            ):
                raise RuntimeError("same-request retry selected different verified trend content")
            if any(
                retry_result.get(key) != value
                for key, value in expected_source_provenance(readiness_before).items()
            ):
                raise RuntimeError("same-request retry returned different source provenance")
            if pointer_after_retry.get("serving_version") != b_version or pointer_after_retry.get("as_of_event_time") != TARGET_B_END:
                raise RuntimeError("same-request retry regressed the active pointer")
            if len(retry_rows) != 20:
                raise RuntimeError("same-request retry changed the complete active Top-20")
            cli_requests.append(
                {
                    "window": "B",
                    "as_of_event_time": timestamp(TARGET_B_END),
                    "status": retry_result["status"],
                    "serving_version": retry_result["serving_version"],
                    "trend_window_end": retry_result["trend_window_end"],
                    "trend_content_sha256": retry_result["trend_content_sha256"],
                    "readiness_pointer_id": retry_result["readiness_pointer_id"],
                    "pointer_status": pointer_after_retry.get("status"),
                    "pointer_ready": pointer_after_retry.get("ready"),
                    "published_rows": len(retry_rows),
                    "top_five": [
                        {"product_id": row["product_id"], "final_score": float(row["final_score"])}
                        for row in retry_rows[:5]
                    ],
                    "lambda_weight": 0.5,
                    "ranking_mode": retry_result["ranking_mode"],
                    "source_provenance": {
                        "source_run_id": retry_result["source_run_id"],
                        "model_version": retry_result["model_version"],
                        "graph_version": retry_result["graph_version"],
                    },
                    "command": retry_command,
                    "cli_wall_ms": retry_ms,
                }
            )
            report["serving_requests"] = list(cli_requests)
            report["serving_versions"] = [item["serving_version"] for item in cli_requests]
            report["timing_observations"]["module4c_cli_wall_ms"] = [
                {"window": item["window"], "status": item["status"], "wall_ms": item["cli_wall_ms"]}
                for item in cli_requests
            ]
            query.stop()
            return {
                "query_id": str(query.id),
                "query_name": query.name,
                "checkpoint_location": "<dedicated-run-output>/checkpoint/" + names["checkpoint_id"],
                "checkpoint_id": names["checkpoint_id"],
                "topic": topic,
                "phase_evidence": phase_evidence,
                "cli_requests": cli_requests,
                "readiness_before": readiness_before,
                "readiness_after": database[READINESS_COLLECTION].find_one({"_id": "active"}),
                "final_pointer": pointer_after_retry,
                "final_rows": retry_rows,
                "final_recommendation_document_count": database[FINAL_RECOMMENDATIONS_COLLECTION].count_documents({}),
                "foreign_user_recommendation_count": database[FINAL_RECOMMENDATIONS_COLLECTION].count_documents({"user_id": {"$ne": USER_ID}}),
                "retry": {
                    "status": retry_result["status"],
                    "same_serving_version": retry_result["serving_version"] == b_version,
                    "same_final_rows": retry_rows == phase_evidence["_b_rows"],
                    "pointer_non_regressing": pointer_after_retry.get("serving_version") == b_version,
                    "final_row_count": database[FINAL_RECOMMENDATIONS_COLLECTION].count_documents({}),
                    "cli_wall_ms": retry_ms,
                },
            }

        stream_output = output_dir / "stream"
        stream_info = run_stream(
            spark,
            config,
            stream_output,
            args.run_id,
            checkpoint_id=names["checkpoint_id"],
            on_query_started=orchestrate,
        )
        state["last_successful_phase"] = "live Kafka → Spark → C4B → C5 → CLI flow"
        stream = stream_info.get("orchestration") or {}
        if stream.get("readiness_before") != stream.get("readiness_after"):
            raise RuntimeError("active A/B readiness changed during serving")
        if stream.get("foreign_user_recommendation_count") != 0:
            raise RuntimeError("another user's final recommendation rows were written")
        all_offsets = _topic_offsets(bootstrap, topic)
        if all_offsets["end"] != 48:
            raise RuntimeError(f"Kafka end offset {all_offsets['end']} differs from 48 acknowledged events")
        total_ack = len(acknowledgements)
        if total_ack != 48 or len({item["event_id"] for item in acknowledgements}) != total_ack:
            raise RuntimeError("Kafka acknowledgement count or event ID uniqueness assertion failed")
        spark_records = _capture_state_metrics(progress_rows)
        if spark_records["quality_invalid_records"] != 0 or spark_records["quality_valid_records"] != 48:
            raise RuntimeError("Spark quality metrics do not account for all 48 valid input records")
        if spark_records["duplicate_rows_dropped"] != 0 or spark_records["rows_dropped_by_watermark"] != 0:
            raise RuntimeError("Spark unexpectedly deduplicated or dropped controlled fixture records")
        target_results = {
            "A": stream["phase_evidence"]["finalized_window_A"],
            "B": stream["phase_evidence"]["finalized_window_B"],
        }
        if [item["top1_product_id"] for item in target_results.values()] != [1, 2]:
            raise RuntimeError("the actual streaming ranker did not produce the planned P1→P2 shift")
        a_request, b_request, retry_request = stream["cli_requests"]
        if a_request["serving_version"] == b_request["serving_version"]:
            raise RuntimeError("changed trend content did not create a new serving version")
        if not stream["retry"]["same_serving_version"] or not stream["retry"]["same_final_rows"]:
            raise RuntimeError("CLI retry was not idempotent")
        if stream["final_recommendation_document_count"] != 40 or stream["retry"]["final_row_count"] != 40:
            raise RuntimeError("versioned final rows were duplicated or incomplete")
        final_offsets = _topic_offsets(bootstrap, topic)

        report.update(
            {
                "status": "passed",
                "spark_version": spark.version,
                "mongodb_version": mongo_version,
                "event_counts": {
                    "planned": 48,
                    "acknowledged": total_ack,
                    "consumed": spark_records["quality_valid_records"],
                    "invalid": spark_records["quality_invalid_records"],
                    "unique_event_ids": len({item["event_id"] for item in acknowledgements}),
                    "by_phase": phase_counts,
                    "kafka_offsets": {"beginning": all_offsets["beginning"], "end": final_offsets["end"]},
                },
                "target_windows": {
                    "A": {
                        "start": timestamp(TARGET_A_START),
                        "end": timestamp(TARGET_A_END),
                        **target_results["A"],
                        "expected_trend_rows": expected_a,
                    },
                    "B": {
                        "start": timestamp(TARGET_B_START),
                        "end": timestamp(TARGET_B_END),
                        **target_results["B"],
                        "expected_trend_rows": expected_b,
                    },
                    "all_finalized_bundle_count": client[args.database]["realtime_trending_windows"].count_documents({"source_namespace": namespace}),
                },
                "spark_progress_summary": {
                    **spark_records,
                    "query_id": stream_info.get("query_id"),
                    "checkpoint_id": names["checkpoint_id"],
                    "checkpoint_location": "<dedicated-run-output>/checkpoint/" + names["checkpoint_id"],
                    "topic_identity": names["topic"],
                    "phase_progress": {key: value for key, value in stream["phase_evidence"].items() if not key.startswith("_")},
                },
                "bundle_identities": [target_results[key]["bundle_id"] for key in ("A", "B")],
                "bundle_content_hashes": [target_results[key]["content_sha256"] for key in ("A", "B")],
                "serving_requests": stream["cli_requests"],
                "serving_versions": [a_request["serving_version"], b_request["serving_version"]],
                "ranking_before": {
                    "window": "A",
                    "top1_product_id": stream["phase_evidence"]["serving_A"]["top1_product_id"],
                    "top5": a_request["top_five"],
                    "expected_top1_product_id": 1,
                },
                "ranking_after": {
                    "window": "B",
                    "top1_product_id": stream["phase_evidence"]["serving_B"]["top1_product_id"],
                    "top5": b_request["top_five"],
                    "expected_top1_product_id": 2,
                    "top1_changed": stream["phase_evidence"]["serving_A"]["top1_product_id"] != stream["phase_evidence"]["serving_B"]["top1_product_id"],
                },
                "readiness_checks": {
                    "ab_readiness_unchanged": stream["readiness_before"] == stream["readiness_after"],
                    "per_user_pointer_ready": stream["final_pointer"].get("ready") is True,
                    "pointer_advanced_to_window_b": stream["final_pointer"].get("serving_version") == b_request["serving_version"],
                    "no_foreign_user_rows": stream["foreign_user_recommendation_count"] == 0,
                    "complete_rows_per_version": 20,
                    "previous_version_rows_immutable": stream["phase_evidence"]["_a_rows"] == list(
                        client[args.database][FINAL_RECOMMENDATIONS_COLLECTION]
                        .find({"user_id": USER_ID, "serving_version": a_request["serving_version"]})
                        .sort("rank", 1)
                    ),
                },
                "retry_checks": stream["retry"],
                "timing_observations": {
                    "kafka_producer_ack_wall_ms": [
                        {"phase": item["phase"], "event_id": item["event_id"], "ack_wall_ms": item["ack_wall_ms"]}
                        for item in acknowledgements
                    ],
                    "kafka_ack_summary_ms": {
                        "mean": sum(item["ack_wall_ms"] for item in acknowledgements) / len(acknowledgements),
                        "max": max(item["ack_wall_ms"] for item in acknowledgements),
                    },
                    "spark_microbatch_duration_ms": {
                        "trigger_execution": spark_records["microbatch_trigger_execution_ms"],
                        "add_batch": spark_records["microbatch_add_batch_ms"],
                    },
                    "processing_time_to_finalized_publication_ms": publication_delays,
                    "module4c_cli_wall_ms": [
                        {"window": item["window"], "status": item["status"], "wall_ms": item["cli_wall_ms"]}
                        for item in stream["cli_requests"]
                    ],
                    "latency_definition": "Each duration is measured with a monotonic wall clock; event-time gaps are not latency measurements.",
                },
                "stream_restart": {"attempted": False, "status": "unverified", "reason": "The acceptance gate did not require a checkpoint restart; the stream was stopped normally and the checkpoint was preserved in run evidence."},
                "runtime": {
                    "kafka_image": "apache/kafka:3.7.0",
                    "kafka_version": "3.7.0",
                    "spark_image": "instacart-spark:3.5.5",
                    "spark_version": spark.version,
                    "mongodb_image": "mongo:7.0.16",
                    "mongodb_version": mongo_version,
                    "network": names["network"],
                    "mongo_target": "c8b-mongo:27017",
                    "mongo_database": args.database,
                    "host_ports_published": False,
                    "persistent_volumes": False,
                    "spark_mode": "local[2]",
                    "shuffle_partitions": 2,
                },
                "failure_diagnostics": None,
            }
        )
        state["last_successful_phase"] = "all acceptance assertions"
    except BaseException as exc:
        failure = exc
        report["failure_diagnostics"] = {
            "last_successful_phase": state.get("last_successful_phase"),
            "current_phase": state.get("phase"),
            "exception": redact_secrets(f"{type(exc).__name__}: {exc}"),
            "acknowledged_records": len(acknowledgements),
            "spark_progress_records": len(progress_rows),
            "last_spark_progress": _progress_record(progress_rows[-1], "last-known") if progress_rows else None,
            "last_mongo_publication": report.get("bundle_identities", []),
        }
        report["status"] = "failed"
        report["event_counts"]["acknowledged"] = len(acknowledgements)
        failure_metrics = _capture_state_metrics(progress_rows)
        report["event_counts"]["consumed"] = failure_metrics["quality_valid_records"]
        report["event_counts"]["invalid"] = failure_metrics["quality_invalid_records"]
        report["event_counts"]["by_phase"] = phase_counts
        report["spark_progress_summary"] = failure_metrics
    finally:
        if spark is not None:
            try:
                spark.stop()
                report["cleanup_status"]["spark_session_stopped"] = True
                report["cleanup_status"]["spark_query_stopped"] = True
            except Exception as exc:
                report["cleanup_status"]["spark_stop_error"] = redact_secrets(str(exc))
        else:
            report["cleanup_status"]["spark_query_stopped"] = True
            report["cleanup_status"]["spark_session_stopped"] = True
            report["cleanup_status"]["spark_started"] = False
        if client is not None:
            try:
                if database_owned:
                    owner = client[args.database]["_c8b_ownership"].find_one({"_id": "owner"})
                    if owner is not None and owner.get("run_id") == args.run_id:
                        client.drop_database(args.database)
                        report["cleanup_status"]["mongodb_database_dropped"] = args.database not in client.list_database_names()
                    else:
                        report["cleanup_status"]["mongodb_database_dropped"] = False
                        report["cleanup_status"]["mongodb_cleanup_error"] = "ownership marker did not verify"
                client.close()
                report["cleanup_status"]["mongodb_client_closed"] = True
            except Exception as exc:
                report["cleanup_status"]["mongodb_cleanup_error"] = redact_secrets(str(exc))
        _write_json(output_dir / "report.json", report)
        if progress_rows:
            _write_json(output_dir / "spark_progress_excerpt.json", progress_rows)
        if failure is not None:
            _write_json(output_dir / "failure.json", report.get("failure_diagnostics") or {})

    if failure is not None:
        print(f"C8B live demonstration failed: {redact_secrets(str(failure))}", file=sys.stderr)
        return 1
    try:
        validate_report(report)
    except ValueError as exc:
        report["status"] = "failed"
        report["failure_diagnostics"] = {"exception": str(exc)}
        _write_json(output_dir / "report.json", report)
        print(f"C8B report validation failed: {exc}", file=sys.stderr)
        return 1
    _write_json(output_dir / "report.json", report)
    digest = hashlib.sha256((output_dir / "report.json").read_bytes()).hexdigest()
    (output_dir / "report.sha256").write_text(digest + "  report.json\n", encoding="utf-8")
    print(json.dumps({"status": "passed", "report": "/demo-output/report.json", "experiment_id": args.run_id}, separators=(",", ":")))
    return 0


def _docker_cli(arguments: list[str], *, timeout_seconds: int = 30) -> subprocess.CompletedProcess[str]:
    """Run one bounded Docker CLI command without exposing command output."""
    try:
        return subprocess.run(
            ["docker", *arguments],
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError(f"Docker command failed: {redact_secrets(str(exc))}") from exc


def _docker_detail(result: subprocess.CompletedProcess[str]) -> str:
    detail = (result.stderr or result.stdout or "Docker returned no diagnostic").strip()
    return redact_secrets(detail[:1000])


def _docker_resource_missing(result: subprocess.CompletedProcess[str]) -> bool:
    detail = f"{result.stderr or ''} {result.stdout or ''}".lower()
    return any(token in detail for token in ("no such", "not found", "no such object"))


def _docker_inspect(resource_type: str, name: str) -> dict[str, Any] | None:
    result = _docker_cli([resource_type, "inspect", name])
    if result.returncode:
        if _docker_resource_missing(result):
            return None
        raise RuntimeError(
            f"Docker {resource_type} inspect failed for {name}: {_docker_detail(result)}"
        )
    try:
        decoded = json.loads(result.stdout)
        if not isinstance(decoded, list) or len(decoded) != 1 or not isinstance(decoded[0], dict):
            raise ValueError("inspect result was not a single JSON object")
        return decoded[0]
    except (json.JSONDecodeError, ValueError) as exc:
        raise RuntimeError(f"Docker {resource_type} inspect returned invalid JSON for {name}") from exc


def _container_volume_mounts(container: dict[str, Any]) -> tuple[list[dict[str, str]], list[str]]:
    """Identify image-created anonymous volumes; reject explicit or unknown volumes."""
    config = container.get("Config") or {}
    declared = set((config.get("Volumes") or {}).keys())
    host_config = container.get("HostConfig") or {}
    explicit_destinations = {
        str(mount.get("Target") or mount.get("Destination") or "")
        for mount in (host_config.get("Mounts") or [])
        if mount.get("Type") == "volume"
    }
    volumes_from = bool(host_config.get("VolumesFrom"))
    for bind in host_config.get("Binds") or []:
        parts = str(bind).split(":")
        if len(parts) >= 2:
            explicit_destinations.add(parts[1])

    anonymous: list[dict[str, str]] = []
    unsafe: list[str] = []
    for mount in container.get("Mounts") or []:
        if mount.get("Type") != "volume":
            continue
        name = str(mount.get("Name") or "")
        destination = str(mount.get("Destination") or "")
        if (
            destination not in declared
            or destination in explicit_destinations
            or volumes_from
            or _ANONYMOUS_VOLUME_PATTERN.fullmatch(name) is None
        ):
            unsafe.append(
                f"volume mount at {destination or '<unknown destination>'} is named, external, or unexpected"
            )
            continue
        anonymous.append({"name": name, "destination": destination})
    return anonymous, unsafe


def _is_task_owned(resource: dict[str, Any], *, name: str, run_id: str, network: bool = False) -> bool:
    actual_name = resource.get("Name")
    if not network:
        actual_name = str(actual_name or "").lstrip("/")
    labels = resource.get("Labels") if network else (resource.get("Config") or {}).get("Labels")
    return (
        actual_name == name
        and isinstance(labels, dict)
        and labels.get(_DOCKER_TASK_LABEL) == _DOCKER_TASK_VALUE
        and labels.get(_DOCKER_RUN_LABEL) == run_id
    )


def _filtered_task_resources(resource_type: str, run_id: str) -> list[str]:
    if resource_type == "container":
        command = [
            "ps", "-aq", "--filter", f"label={_DOCKER_TASK_LABEL}={_DOCKER_TASK_VALUE}",
            "--filter", f"label={_DOCKER_RUN_LABEL}={run_id}",
        ]
    else:
        command = [
            "network", "ls", "-q", "--filter", f"label={_DOCKER_TASK_LABEL}={_DOCKER_TASK_VALUE}",
            "--filter", f"label={_DOCKER_RUN_LABEL}={run_id}",
        ]
    result = _docker_cli(command)
    if result.returncode:
        raise RuntimeError(f"Could not verify remaining task {resource_type}s: {_docker_detail(result)}")
    return sorted(set(result.stdout.split()))


def _capture_container_diagnostics(
    container: dict[str, Any], *, role: str, evidence_dir: Path | None
) -> list[str]:
    warnings: list[str] = []
    if evidence_dir is None:
        return warnings
    evidence_dir.mkdir(parents=True, exist_ok=True)
    summary = {
        key: container.get(key)
        for key in ("Id", "Name", "Created", "State", "Image", "Mounts")
    }
    _write_json(evidence_dir / f"{role}-inspect.json", summary)
    container_id = str(container.get("Id") or "")
    logs = _docker_cli(["logs", container_id])
    if logs.returncode:
        warnings.append(f"Could not collect {role} logs: {_docker_detail(logs)}")
    else:
        (evidence_dir / f"{role}.log").write_text(
            redact_secrets(logs.stdout + logs.stderr), encoding="utf-8"
        )
    return warnings


def cleanup_task_docker_resources(run_id: str, *, evidence_dir: Path | None = None) -> dict[str, Any]:
    """Remove only verified C8B resources and independently inspect cleanup results."""
    validate_run_id(run_id)
    names = resource_names(run_id)
    receipt: dict[str, Any] = {
        "run_id": run_id,
        "status": "failed",
        "cleanup_complete": False,
        "containers": [],
        "network": {"name": names["network"], "ownership_verified": False, "removed": False},
        "captured_anonymous_volumes": [],
        "remaining": {"containers": [], "networks": [], "anonymous_volumes": []},
        "errors": [],
        "warnings": [],
    }
    errors: list[str] = receipt["errors"]
    receipt_path = evidence_dir / "docker-cleanup.json" if evidence_dir is not None else None
    if receipt_path is not None and receipt_path.exists():
        try:
            previous = json.loads(receipt_path.read_text(encoding="utf-8"))
            if previous.get("run_id") != run_id:
                errors.append("Existing cleanup receipt belongs to a different run ID; refusing to reuse its volumes")
            else:
                for volume in previous.get("captured_anonymous_volumes") or []:
                    if (
                        not isinstance(volume, dict)
                        or _ANONYMOUS_VOLUME_PATTERN.fullmatch(str(volume.get("name") or "")) is None
                        or not isinstance(volume.get("container_ids"), list)
                    ):
                        errors.append("Existing cleanup receipt contains an invalid captured volume identity")
                        continue
                    if not any(
                        item["name"] == volume["name"] for item in receipt["captured_anonymous_volumes"]
                    ):
                        receipt["captured_anonymous_volumes"].append(
                            {
                                "name": volume["name"],
                                "destination": volume.get("destination", "previous cleanup inspection"),
                                "container_ids": list(volume["container_ids"]),
                            }
                        )
        except (OSError, json.JSONDecodeError, AttributeError, TypeError) as exc:
            errors.append(f"Existing cleanup receipt could not be validated: {redact_secrets(str(exc))}")
    task_containers: list[dict[str, Any]] = []
    owned_ids: set[str] = set()

    # Inspect every exact run-specific name and capture its mount identities before
    # any task container is removed.
    for role in _CONTAINER_ROLES:
        name = names[role]
        try:
            container = _docker_inspect("container", name)
        except RuntimeError as exc:
            errors.append(str(exc))
            continue
        row: dict[str, Any] = {
            "role": role,
            "name": name,
            "present_before_cleanup": container is not None,
            "ownership_verified": False,
            "removed": False,
            "anonymous_volumes": [],
            "unsafe_mounts": [],
        }
        receipt["containers"].append(row)
        if container is None:
            continue
        if not _is_task_owned(container, name=name, run_id=run_id):
            errors.append(f"Refusing to remove {name}: exact name or C8B task/run labels do not match")
            continue
        container_id = str(container.get("Id") or "")
        if not container_id:
            errors.append(f"Refusing to remove {name}: Docker inspection omitted the container ID")
            continue
        row["container_id"] = container_id
        row["ownership_verified"] = True
        anonymous, unsafe = _container_volume_mounts(container)
        row["anonymous_volumes"] = anonymous
        row["unsafe_mounts"] = unsafe
        if unsafe:
            errors.extend(f"{name}: {message}" for message in unsafe)
        task_containers.append({"role": role, "row": row, "container": container, "id": container_id})
        owned_ids.add(container_id)
        for mount in anonymous:
            captured = next(
                (item for item in receipt["captured_anonymous_volumes"] if item["name"] == mount["name"]),
                None,
            )
            if captured is None:
                captured = {
                    "name": mount["name"],
                    "destination": mount["destination"],
                    "container_ids": [],
                }
                receipt["captured_anonymous_volumes"].append(captured)
            if container_id not in captured["container_ids"]:
                captured["container_ids"].append(container_id)
        receipt["warnings"].extend(
            _capture_container_diagnostics(container, role=role.removesuffix("_container"), evidence_dir=evidence_dir)
        )

    # Save the inspected identities before container removal. A repeated call
    # can then still detect a same-run volume if Docker left it behind.
    if receipt_path is not None:
        evidence_dir.mkdir(parents=True, exist_ok=True)
        _write_json(receipt_path, receipt)

    # A volume referenced by any non-task container is external to this experiment.
    externally_referenced: set[str] = set()
    for volume in receipt["captured_anonymous_volumes"]:
        result = _docker_cli(["ps", "-aq", "--filter", f"volume={volume['name']}"])
        if result.returncode:
            errors.append(
                f"Could not verify users of captured volume {volume['name']}: {_docker_detail(result)}"
            )
            externally_referenced.add(volume["name"])
            continue
        # `docker ps -q` returns abbreviated IDs by default; container inspect
        # returns full IDs. Compare by unique prefix rather than raw equality.
        referenced_ids = set(result.stdout.split())
        outside = {
            referenced
            for referenced in referenced_ids
            if not any(
                container_id == referenced
                or container_id.startswith(referenced)
                or referenced.startswith(container_id)
                for container_id in owned_ids
            )
        }
        if outside:
            errors.append(
                f"Captured volume {volume['name']} is also referenced outside this C8B run"
            )
            externally_referenced.add(volume["name"])
            volume["external_container_ids"] = sorted(outside)

    for item in task_containers:
        row = item["row"]
        container_id = item["id"]
        unsafe = bool(row["unsafe_mounts"])
        unsafe = unsafe or any(
            mount["name"] in externally_referenced for mount in row["anonymous_volumes"]
        )
        # -v removes only anonymous volumes. If inspection finds an unexpected
        # mount, use plain -f to remove the owned container but preserve that data.
        flags = ["rm", "-f"] if unsafe else ["rm", "-f", "-v"]
        result = _docker_cli([*flags, container_id])
        row["remove_command"] = "docker " + " ".join(flags + [container_id])
        row["removed"] = result.returncode == 0
        if result.returncode:
            errors.append(f"Could not remove verified C8B container {container_id}: {_docker_detail(result)}")

    network_name = names["network"]
    try:
        network = _docker_inspect("network", network_name)
    except RuntimeError as exc:
        errors.append(str(exc))
        network = None
    if network is not None:
        network_id = str(network.get("Id") or "")
        receipt["network"]["network_id"] = network_id
        if not _is_task_owned(network, name=network_name, run_id=run_id, network=True):
            errors.append(f"Refusing to remove {network_name}: exact name or C8B task/run labels do not match")
        else:
            receipt["network"]["ownership_verified"] = True
            result = _docker_cli(["network", "rm", network_id])
            receipt["network"]["removed"] = result.returncode == 0
            if result.returncode:
                errors.append(f"Could not remove verified C8B network {network_id}: {_docker_detail(result)}")

    # Verify exact resource names and each captured volume identity. Label filters
    # are an additional leak check, never the source of ownership for deletion.
    for item in receipt["containers"]:
        try:
            current = _docker_inspect("container", item["name"])
        except RuntimeError as exc:
            errors.append(str(exc))
            current = {"Id": "inspection-error"}
        if current is not None:
            receipt["remaining"]["containers"].append(str(current.get("Id") or item["name"]))
    try:
        current_network = _docker_inspect("network", network_name)
        if current_network is not None:
            receipt["remaining"]["networks"].append(str(current_network.get("Id") or network_name))
    except RuntimeError as exc:
        errors.append(str(exc))
        receipt["remaining"]["networks"].append(network_name)
    for volume in receipt["captured_anonymous_volumes"]:
        try:
            current_volume = _docker_inspect("volume", volume["name"])
        except RuntimeError as exc:
            errors.append(str(exc))
            current_volume = {"Name": volume["name"]}
        volume["absent_after_cleanup"] = current_volume is None
        if current_volume is not None:
            receipt["remaining"]["anonymous_volumes"].append(volume["name"])

    for resource_type, target in (("container", "containers"), ("network", "networks")):
        try:
            remaining = _filtered_task_resources(resource_type, run_id)
        except RuntimeError as exc:
            errors.append(str(exc))
            remaining = [f"{resource_type}-verification-error"]
        for identity in remaining:
            if identity not in receipt["remaining"][target]:
                receipt["remaining"][target].append(identity)

    receipt["cleanup_complete"] = not errors and not any(receipt["remaining"].values())
    receipt["status"] = "passed" if receipt["cleanup_complete"] else "failed"
    if evidence_dir is not None:
        evidence_dir.mkdir(parents=True, exist_ok=True)
        _write_json(receipt_path, receipt)
    return receipt


def cleanup_docker_command(args: argparse.Namespace) -> int:
    evidence_dir = Path(args.evidence_dir)
    receipt = cleanup_task_docker_resources(args.run_id, evidence_dir=evidence_dir)
    print(json.dumps({"status": receipt["status"], "receipt": str(evidence_dir / "docker-cleanup.json")}, sort_keys=True))
    return 0 if receipt["cleanup_complete"] else 1


def finalize_report(args: argparse.Namespace) -> int:
    """Record independently verified container, network, and volume cleanup."""
    path = Path(args.report)
    report = json.loads(path.read_text(encoding="utf-8"))
    if report.get("experiment_id") != args.run_id:
        raise ValueError("report run ID does not match cleanup run ID")
    cleanup_status = report.setdefault("cleanup_status", {})
    receipt_valid = False
    receipt_summary: dict[str, Any] = {}
    try:
        receipt = json.loads(Path(args.docker_cleanup_report).read_text(encoding="utf-8"))
        receipt_summary = {
            "status": receipt.get("status"),
            "cleanup_complete": receipt.get("cleanup_complete"),
            "captured_anonymous_volume_count": len(receipt.get("captured_anonymous_volumes") or []),
            "remaining": receipt.get("remaining"),
            "errors": receipt.get("errors"),
        }
        receipt_valid = (
            receipt.get("run_id") == args.run_id
            and receipt.get("status") == "passed"
            and receipt.get("cleanup_complete") is True
            and not receipt.get("errors")
            and not any((receipt.get("remaining") or {}).values())
            and all(
                volume.get("absent_after_cleanup") is True
                for volume in receipt.get("captured_anonymous_volumes") or []
            )
        )
    except (OSError, json.JSONDecodeError, AttributeError, TypeError) as exc:
        receipt_summary = {"status": "unavailable", "error": redact_secrets(str(exc))}
    cleanup_status["docker_cleanup_receipt"] = receipt_summary
    cleanup_status["docker_anonymous_volumes_removed"] = receipt_valid
    cleanup_status["docker_resources_removed"] = receipt_valid
    cleanup_status["docker_remaining_task_resources"] = (
        receipt_summary.get("remaining") or {"containers": [], "networks": [], "anonymous_volumes": []}
    )
    required_cleanup = (
        cleanup_status.get("mongodb_database_dropped") is True
        and cleanup_status.get("spark_query_stopped") is True
        and receipt_valid
        and args.cleaned == "yes"
        and args.experiment_exit_code == 0
    )
    if not required_cleanup:
        report["status"] = "failed"
        cleanup_status["cleanup_error"] = (
            "experiment failed, task resources or anonymous volumes remain, "
            "or cleanup could not be verified"
        )
    validate_report(report)
    _write_json(path, report)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    path.with_name("report.sha256").write_text(f"{digest}  report.json\n", encoding="utf-8")
    return 0 if report.get("status") == "passed" and required_cleanup else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    live = subparsers.add_parser("live", help="run the real bounded streaming demonstration")
    live.add_argument("--run-id", required=True)
    live.add_argument("--output-dir", default="/demo-output")
    live.add_argument("--mongodb-uri", required=True)
    live.add_argument("--database", required=True)
    live.add_argument("--topic", required=True)
    live.set_defaults(func=run_live)
    docker_cleanup = subparsers.add_parser(
        "cleanup-docker", help="remove and verify only one run's C8B Docker resources"
    )
    docker_cleanup.add_argument("--run-id", required=True)
    docker_cleanup.add_argument("--evidence-dir", required=True)
    docker_cleanup.set_defaults(func=cleanup_docker_command)
    finalize = subparsers.add_parser("finalize", help="record host-verified container cleanup")
    finalize.add_argument("--run-id", required=True)
    finalize.add_argument("--report", required=True)
    finalize.add_argument("--docker-cleanup-report", required=True)
    finalize.add_argument("--cleaned", choices=("yes", "no"), required=True)
    finalize.add_argument("--experiment-exit-code", type=int, required=True)
    finalize.set_defaults(func=finalize_report)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return int(args.func(args))
    except Exception as exc:
        print(f"C8B {args.command} failed: {redact_secrets(str(exc))}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
