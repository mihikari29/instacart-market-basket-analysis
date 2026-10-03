"""Kafka JSON parsing, domain validation, and streaming quality metrics."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .config import Config
from .schemas import KAFKA_EVENT_SCHEMA


def parse_json_records(json_df: DataFrame) -> DataFrame:
    """Parse every payload and label invalid rows with explicit reasons."""
    parsed = json_df.select(
        F.col("json_str").alias("raw_payload"),
        F.from_json(F.col("json_str"), KAFKA_EVENT_SCHEMA).alias("event"),
    ).select("raw_payload", "event.*")
    parsed = parsed.withColumn("event_time", F.timestamp_millis(F.col("event_time_epoch_ms")))
    parsed = parsed.withColumn(
        "_event_time_iso", F.expr("try_cast(event_time_iso as timestamp)")
    )

    reasons = [
        F.when(F.col("event_id").isNull() | (F.length(F.trim("event_id")) == 0), "event_id"),
        F.when(F.col("user_id").isNull() | (F.col("user_id") <= 0), "user_id"),
        F.when(F.col("order_id").isNull() | (F.col("order_id") <= 0), "order_id"),
        F.when(F.col("product_id").isNull() | (F.col("product_id") <= 0), "product_id"),
        F.when(
            F.col("add_to_cart_order").isNull() | (F.col("add_to_cart_order") <= 0),
            "add_to_cart_order",
        ),
        F.when(
            F.col("order_dow").isNull() | ~F.col("order_dow").between(0, 6),
            "order_dow",
        ),
        F.when(
            F.col("order_hour_of_day").isNull()
            | ~F.col("order_hour_of_day").between(0, 23),
            "order_hour_of_day",
        ),
        F.when(
            F.col("event_time_epoch_ms").isNull()
            | (F.col("event_time_epoch_ms") <= 0)
            | F.col("event_time").isNull(),
            "event_time_epoch_ms",
        ),
        F.when(
            F.col("event_time_iso").isNull() | F.col("_event_time_iso").isNull(),
            "event_time_iso",
        ),
    ]
    invalid_reason = F.concat_ws(",", *reasons)
    return (
        parsed.withColumn("invalid_reason", invalid_reason)
        .withColumn("is_valid", F.length("invalid_reason") == 0)
        .drop("_event_time_iso")
    )


def parse_json_events(json_df: DataFrame) -> DataFrame:
    """Return only valid typed events; useful for static fixture tests."""
    return parse_json_records(json_df).where("is_valid").drop(
        "raw_payload", "invalid_reason", "is_valid"
    )


def read_kafka_source(spark, config: Config) -> DataFrame:
    """Build the Kafka source with explicit offset-loss semantics."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.bootstrap_servers)
        .option("subscribe", config.topic)
        .option("startingoffsets", config.starting_offsets)
        .option("failOnDataLoss", str(config.fail_on_data_loss).lower())
        .load()
    )


def from_kafka(spark, config: Config) -> DataFrame:
    """Read Kafka and expose input/valid/invalid counts in query progress."""
    raw = read_kafka_source(spark, config)
    parsed = parse_json_records(raw.selectExpr("CAST(value AS STRING) AS json_str"))
    observed = parsed.observe(
        "record_quality",
        F.count(F.lit(1)).alias("input_records"),
        F.sum(F.col("is_valid").cast("long")).alias("valid_records"),
        F.sum((~F.col("is_valid")).cast("long")).alias("invalid_records"),
    )
    return observed.where("is_valid").drop(
        "raw_payload", "invalid_reason", "is_valid"
    )
