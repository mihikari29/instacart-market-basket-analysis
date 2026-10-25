"""Bounded, versioned latest-batch telemetry shared by publisher and readers."""

from datetime import datetime, timezone
import json
import math
import re

COLLECTION = "streaming_metrics"
MAX_BSON_INT64 = (1 << 63) - 1
MIN_BSON_INT64 = -(1 << 63)
FIELDS = {"schema_version", "source_namespace", "source_scope", "run_id", "query_id", "batch_id",
          "captured_at", "num_input_rows", "input_rows_per_second", "processed_rows_per_second",
          "ingestion_to_output_latency"}


def _is_integer(value):
    """Accept Python/BSON integers without accepting booleans or floats."""
    return isinstance(value, int) and not isinstance(value, bool)


def _valid_int64(value, *, minimum=MIN_BSON_INT64, maximum=MAX_BSON_INT64):
    return _is_integer(value) and minimum <= value <= maximum


def _valid_finite_number(value):
    if _is_integer(value):
        return MIN_BSON_INT64 <= value <= MAX_BSON_INT64
    return type(value) is float and math.isfinite(value)


def validate_snapshot(document):
    """Reject malformed telemetry before storing or exposing it; no coercion."""
    if not isinstance(document, dict) or set(document) != FIELDS:
        raise ValueError("invalid streaming metrics fields")
    if len(json.dumps(document, allow_nan=False).encode()) > 16_384:
        raise ValueError("streaming metrics exceed the document bound")
    if not _valid_int64(document["schema_version"], minimum=1, maximum=1):
        raise ValueError("unsupported streaming metrics version")
    for key, maximum in (("source_namespace", 256), ("source_scope", 128), ("run_id", 256), ("query_id", 128)):
        value = document[key]
        if not isinstance(value, str) or not 1 <= len(value) <= maximum or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/-]*", value):
            raise ValueError("invalid streaming metrics identity")
    for key in ("batch_id", "num_input_rows"):
        if not _valid_int64(document[key], minimum=0):
            raise ValueError("invalid streaming metrics count")
    captured = datetime.fromisoformat(document["captured_at"])
    if captured.tzinfo is None:
        raise ValueError("streaming metrics timestamp requires timezone")
    for key in ("input_rows_per_second", "processed_rows_per_second"):
        value = document[key]
        if value is not None and (not _valid_finite_number(value) or value < 0):
            raise ValueError("invalid streaming rate")
    latency = document["ingestion_to_output_latency"]
    if not isinstance(latency, dict):
        raise ValueError("invalid streaming latency")
    if latency.get("status") == "unavailable":
        required = {"status", "reason", "sample_count"}
        if (
            set(latency) != required
            or latency["reason"] != "no_valid_ingestion_timestamps_in_finalized_output"
            or not _valid_int64(latency["sample_count"], minimum=0, maximum=0)
        ):
            raise ValueError("invalid unavailable latency")
    elif latency.get("status") == "measured":
        required = {"status", "sample_count", "source_timestamp_field", "output_completion_epoch_ms", "sample_boundary",
                    "latency_ms", "clock_skew_detected", "timestamp_provenance", "limitations"}
        if set(latency) != required or not _valid_int64(latency["sample_count"], minimum=1):
            raise ValueError("invalid measured latency")
        if not _valid_int64(latency["output_completion_epoch_ms"], minimum=1):
            raise ValueError("invalid latency completion time")
        values = latency["latency_ms"]
        if not isinstance(values, dict) or set(values) != {"minimum", "mean", "maximum"}:
            raise ValueError("invalid latency summary")
        if any(not _valid_finite_number(v) for v in values.values()):
            raise ValueError("invalid latency values")
        if not values["minimum"] <= values["mean"] <= values["maximum"]:
            raise ValueError("inconsistent latency values")
        if type(latency["clock_skew_detected"]) is not bool or latency["clock_skew_detected"] != (values["minimum"] < 0):
            raise ValueError("invalid latency clock skew flag")
        if latency["source_timestamp_field"] != "ingestion_time_epoch_ms":
            raise ValueError("invalid latency source")
        for key in ("sample_boundary", "timestamp_provenance"):
            if not isinstance(latency[key], str) or not 1 <= len(latency[key]) <= 1024:
                raise ValueError("invalid latency provenance")
        if not isinstance(latency["limitations"], list) or len(latency["limitations"]) > 10 or any(not isinstance(v, str) or len(v) > 1024 for v in latency["limitations"]):
            raise ValueError("invalid latency limitations")
    else:
        raise ValueError("invalid latency status")
    return document


def canonicalize_snapshot_for_json(document):
    """Validate a stored document and convert BSON integer subclasses at the API boundary."""
    validate_snapshot(document)
    canonical = dict(document)
    for key in ("schema_version", "batch_id", "num_input_rows"):
        canonical[key] = int(document[key])
    for key in ("input_rows_per_second", "processed_rows_per_second"):
        if _is_integer(document[key]):
            canonical[key] = int(document[key])
    latency = dict(document["ingestion_to_output_latency"])
    if latency.get("status") == "measured":
        latency["sample_count"] = int(latency["sample_count"])
        latency["output_completion_epoch_ms"] = int(latency["output_completion_epoch_ms"])
        latency_values = dict(latency["latency_ms"])
        for key, value in latency_values.items():
            if _is_integer(value):
                latency_values[key] = int(value)
        latency["latency_ms"] = latency_values
    else:
        latency["sample_count"] = int(latency["sample_count"])
    canonical["ingestion_to_output_latency"] = latency
    return canonical


def build_snapshot(progress, sink_result, *, source_namespace, source_scope, run_id):
    """Pair rates and latency from the same completed Spark batch."""
    if progress.get("batchId") != sink_result.get("batch_id"):
        raise ValueError("streaming metrics require matching batch identities")
    def rate(key):
        value = progress.get(key)
        return value if type(value) in (int, float) and math.isfinite(value) and value >= 0 else None
    return validate_snapshot({
        "schema_version": 1, "source_namespace": source_namespace, "source_scope": source_scope,
        "run_id": run_id, "query_id": str(progress["id"]), "batch_id": progress["batchId"],
        "captured_at": datetime.now(timezone.utc).isoformat(), "num_input_rows": progress["numInputRows"],
        "input_rows_per_second": rate("inputRowsPerSecond"), "processed_rows_per_second": rate("processedRowsPerSecond"),
        "ingestion_to_output_latency": sink_result["ingestion_to_output_latency"],
    })


def publish_snapshot(config, document):
    """Upsert one latest snapshot per namespace; API readers never call this."""
    from pymongo import MongoClient

    validate_snapshot(document)
    with MongoClient(config.mongodb_uri, timeoutMS=10_000, serverSelectionTimeoutMS=10_000,
                     connectTimeoutMS=5_000, socketTimeoutMS=5_000) as client:
        client[config.mongo_database][COLLECTION].replace_one(
            {"_id": document["source_namespace"]}, {"_id": document["source_namespace"], **document}, upsert=True
        )


def summarize_ingestion_to_output(stats: dict, output_epoch_ms: int) -> dict:
    """Summarize row-level max-ingestion timestamps at one output boundary.

    Each sample is the latest producer timestamp among the events contributing
    to one finalized (window, product) row. This is a bounded summary: callers
    accumulate count/sum/min/max while streaming rows to the sink.
    """
    if type(output_epoch_ms) is not int or output_epoch_ms <= 0:
        raise ValueError("output_epoch_ms must be a positive integer")
    required = {"count", "sum", "min", "max"}
    if not isinstance(stats, dict) or not required.issubset(stats):
        raise ValueError("timestamp stats must contain count, sum, min, and max")
    count = stats["count"]
    if type(count) is not int or count < 0:
        raise ValueError("timestamp stats count must be a non-negative integer")
    if count == 0:
        return {
            "status": "unavailable",
            "reason": "no_valid_ingestion_timestamps_in_finalized_output",
            "sample_count": 0,
        }
    minimum = stats["min"]
    maximum = stats["max"]
    total = stats["sum"]
    if any(type(value) is not int or value <= 0 for value in (minimum, maximum, total)):
        raise ValueError("timestamp stats values must be positive integers")
    if minimum > maximum or not (count * minimum <= total <= count * maximum):
        raise ValueError("timestamp stats are inconsistent")

    return {
        "status": "measured",
        "sample_count": count,
        "source_timestamp_field": "ingestion_time_epoch_ms",
        "output_completion_epoch_ms": output_epoch_ms,
        "sample_boundary": "latest contributing event per finalized product-window output row",
        "latency_ms": {
            "minimum": output_epoch_ms - maximum,
            "mean": output_epoch_ms - (total / count),
            "maximum": output_epoch_ms - minimum,
        },
        "clock_skew_detected": maximum > output_epoch_ms,
        "timestamp_provenance": (
            "producer wall clock immediately before Kafka send, compared with the "
            "output host wall clock after sink publication; clock synchronization "
            "is not independently verified"
        ),
        "limitations": [
            "not Kafka acknowledgement latency",
            "not micro-batch duration or Spark processing rate",
            "not Mongo sink duration",
            "uses the latest contributing event timestamp for each product-window row, not every event",
            "finalized event-time windows wait for the configured watermark before output",
        ],
    }
