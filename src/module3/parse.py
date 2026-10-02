"""Kafka source -> typed streaming DataFrame.

Poison-pill guard: src.module1.producer --inject poison:<n> corrupts product_id
to the string "MALFORMED". Applying the explicit JSON schema yields NULL for
invalid numeric fields, which are dropped before event-time processing.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from .config import Config
from .schemas import KAFKA_EVENT_SCHEMA


def parse_json_events(json_df: DataFrame) -> DataFrame:
    """Parse a DataFrame containing ``json_str`` into validated event rows.

    Keeping this separate from the Kafka source makes malformed payload
    behavior testable without a broker. ``from_json`` applies the frozen event
    schema, so non-numeric ids/timestamps become null and are safely rejected.
    """
    parsed = (
        json_df.select(F.from_json(F.col("json_str"), KAFKA_EVENT_SCHEMA).alias("event"))
        .select("event.*")
        .where(F.col("product_id").isNotNull() & F.col("event_time_epoch_ms").isNotNull())
        .withColumn("event_time", F.timestamp_millis(F.col("event_time_epoch_ms")))
    )
    return parsed


def from_kafka(spark, config: Config) -> DataFrame:
    """Read Kafka stream, parse JSON, return typed event rows with event_time.

    Poison-pill rows collapse to NULL under the explicit JSON schema and are
    filtered out before watermarking; they cannot pollute trending counts.
    """
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.bootstrap_servers)
        .option("subscribe", config.topic)
        .option("startingoffsets", config.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )
    return parse_json_events(raw.selectExpr("CAST(value AS STRING) AS json_str"))
