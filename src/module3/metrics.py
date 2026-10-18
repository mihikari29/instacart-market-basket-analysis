"""Bounded, versioned latest-batch telemetry shared by publisher and readers."""

from datetime import datetime, timezone
import json
import math
import re

COLLECTION = "streaming_metrics"
FIELDS = {"schema_version", "source_namespace", "source_scope", "run_id", "query_id", "batch_id",
          "captured_at", "num_input_rows", "input_rows_per_second", "processed_rows_per_second",
          "ingestion_to_output_latency"}


def validate_snapshot(document):
    """Reject malformed telemetry before storing or exposing it; no coercion."""
    if not isinstance(document, dict) or set(document) != FIELDS:
        raise ValueError("invalid streaming metrics fields")
    if len(json.dumps(document, allow_nan=False).encode()) > 16_384:
        raise ValueError("streaming metrics exceed the document bound")
    if type(document["schema_version"]) is not int or document["schema_version"] != 1:
        raise ValueError("unsupported streaming metrics version")
    for key, maximum in (("source_namespace", 256), ("source_scope", 128), ("run_id", 256), ("query_id", 128)):
        value = document[key]
        if not isinstance(value, str) or not 1 <= len(value) <= maximum or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/-]*", value):
            raise ValueError("invalid streaming metrics identity")
    for key in ("batch_id", "num_input_rows"):
        if type(document[key]) is not int or document[key] < 0:
            raise ValueError("invalid streaming metrics count")
    captured = datetime.fromisoformat(document["captured_at"])
    if captured.tzinfo is None:
        raise ValueError("streaming metrics timestamp requires timezone")
    for key in ("input_rows_per_second", "processed_rows_per_second"):
        value = document[key]
        if value is not None and (type(value) not in (int, float) or not math.isfinite(value) or value < 0):
            raise ValueError("invalid streaming rate")
    latency = document["ingestion_to_output_latency"]
    if not isinstance(latency, dict):
        raise ValueError("invalid streaming latency")
    if latency.get("status") == "unavailable":
        if latency != {"status": "unavailable", "reason": "no_valid_ingestion_timestamps_in_finalized_output", "sample_count": 0}:
            raise ValueError("invalid unavailable latency")
    elif latency.get("status") == "measured":
        required = {"status", "sample_count", "source_timestamp_field", "output_completion_epoch_ms", "sample_boundary",
                    "latency_ms", "clock_skew_detected", "timestamp_provenance", "limitations"}
        if set(latency) != required or type(latency["sample_count"]) is not int or latency["sample_count"] <= 0:
            raise ValueError("invalid measured latency")
        if type(latency["output_completion_epoch_ms"]) is not int or latency["output_completion_epoch_ms"] <= 0:
            raise ValueError("invalid latency completion time")
        values = latency["latency_ms"]
        if not isinstance(values, dict) or set(values) != {"minimum", "mean", "maximum"}:
            raise ValueError("invalid latency summary")
        if any(type(v) not in (int, float) or not math.isfinite(v) for v in values.values()):
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
