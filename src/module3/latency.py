"""Bounded reducers for producer-ingestion to finalized-output latency."""

from __future__ import annotations

from typing import Any


def summarize_ingestion_to_output(stats: dict[str, Any], output_epoch_ms: int) -> dict[str, Any]:
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
