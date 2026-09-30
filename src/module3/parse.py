"""Kafka source -> typed streaming DataFrame.

Poison-pill guard: src.module1.producer --inject poison:<n> corrupts product_id
to the string "MALFORMED". Casting to LongType yields NULL, which we drop here.
Dropped counts per micro-batch are returned via the per-batch report dict
emitted by sink_mongo.write_mongo (printed into streaming_progress.jsonl).
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType

from .config import Config
from .schemas import KAFKA_EVENT_SCHEMA


def from_kafka(spark, config: Config) -> DataFrame:
    """Read Kafka stream, parse JSON, return typed event rows with event_time.

    Poison-pill rows (product_id corrupted to a non-numeric string by the
    producer --inject poison flag) collapse to NULL after the LongType cast and
    are filtered out before watermarking; they cannot pollute trending counts.
    """
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.bootstrap_servers)
        .option("subscribe", config.topic)
        .option("startingoffsets", config.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )
    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json_str")
        .select(F.from_json(F.col("json_str"), KAFKA_EVENT_SCHEMA).alias("e"))
        .select("e.*")
    )
    parsed = parsed.withColumn("product_id", F.col("product_id").cast(LongType()))
    parsed = parsed.where(
        F.col("product_id").isNotNull() & F.col("event_time_epoch_ms").isNotNull()
    )
    parsed = parsed.withColumn("event_time", F.timestamp_millis(F.col("event_time_epoch_ms")))
    return parsed
