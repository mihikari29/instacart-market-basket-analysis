"""Deterministic schema-compatible fixtures for the speed layer.

Mirrors src.module2.fixtures.style: tiny schema-complete input, never presented
as real Instacart results. Builds an in-memory events DataFrame that triggers
the trending logic deterministically without needing a Kafka broker.
"""

from datetime import datetime, timezone

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import StructField, TimestampType

from .schemas import KAFKA_EVENT_SCHEMA


def _evt(product_id, event_time_epoch_ms):
    return {
        "event_id": f"e_{product_id}_{event_time_epoch_ms}",
        "order_id": event_time_epoch_ms,
        "user_id": 1,
        "product_id": product_id,
        "add_to_cart_order": 1,
        "reordered": True,
        "aisle_id": 1,
        "department_id": 1,
        "order_dow": 1,
        "order_hour_of_day": 0,
        "event_time_epoch_ms": event_time_epoch_ms,
        "event_time_iso": "2024-01-01T00:00:00Z",
        "ingestion_time_epoch_ms": event_time_epoch_ms,
    }


def make_events_dataframe(spark: SparkSession, rows=None):
    """Return a typed streaming-compatible DataFrame mirroring parse.from_kafka output.

    Default rows exercise the 30m/120m windows with a sparse, sparse-but-aligned cadence.
    """
    if rows is None:
        # Base time = 2024-01-01T00:00:00Z. Events clustered to test window boundaries.
        base_ms = int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
        rows = [
            # Three products in minute 0 (window 0-30m: products 1,2,3 appear once)
            _evt(1, base_ms + 0 * 60_000),
            _evt(2, base_ms + 1 * 60_000),
            _evt(3, base_ms + 2 * 60_000),
            # 25 minutes later: window 0-30m picks product 1 again
            _evt(1, base_ms + 25 * 60_000),
            # 35 minutes after start: opens window 5-35m (slides), closes 0-30m only at 35m
            _evt(2, base_ms + 35 * 60_000),
            # Far later: tests long-window (120m) coverage
            _evt(1, base_ms + 100 * 60_000),
            # Late event: event_time declared 5 minutes after start but emitted last;
            # watermark = 10m, so 5m late stays within watermark for static tests (no
            # watermark enforcement on batch).
            _evt(3, base_ms + 5 * 60_000),
        ]
    rows_with_time = [
        Row(**{**r, "event_time": datetime.fromtimestamp(r["event_time_epoch_ms"] / 1000, tz=timezone.utc)})
        for r in rows
    ]
    schema = KAFKA_EVENT_SCHEMA.add(StructField("event_time", TimestampType()))
    return spark.createDataFrame(rows_with_time, schema=schema)
