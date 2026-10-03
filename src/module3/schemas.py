"""Kafka event schema — the Module 1 producer contract (Proposal §9.4).

13 fields are emitted by src.module1.producer. The 14th, ingestion_time_epoch_ms,
is appended at send-time and is the wall-clock Kafka arrival time (used for
latency measurement, not for event-time watermarking — that role belongs to
event_time_epoch_ms).
"""

from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


KAFKA_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("order_id", LongType()),
        StructField("user_id", LongType()),
        StructField("product_id", LongType()),
        StructField("add_to_cart_order", IntegerType()),
        StructField("reordered", BooleanType()),
        StructField("aisle_id", IntegerType()),
        StructField("department_id", IntegerType()),
        StructField("order_dow", IntegerType()),
        StructField("order_hour_of_day", IntegerType()),
        StructField("event_time_epoch_ms", LongType()),
        StructField("event_time_iso", StringType()),
        StructField("ingestion_time_epoch_ms", LongType()),
    ]
)

# Columns downstream code can rely on after parse.py
PARSED_COLUMNS = (
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
    "event_time",  # timestamp column added by parse.from_kafka
)
