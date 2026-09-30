"""SparkSession builder for the speed layer.

Same invariant block as module2.spark (AQE off, autoBroadcastJoin off, UTC,
ANSI on, event log enabled uncompressed) plus the streaming metrics flag so
recentProgress exposes the throughput counters required by Proposal §15.4.
"""

from pathlib import Path
from pyspark.sql import SparkSession


def session(config, event_dir: Path):
    event_dir.mkdir(parents=True, exist_ok=True)
    builder = (
        SparkSession.builder.appName("instacart-module3")
        .master(config.master)
        .config("spark.sql.shuffle.partitions", config.shuffle_partitions)
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.debug.maxToStringFields", "200")
        .config("spark.sql.maxMetadataStringLength", "10000")
        .config("spark.sql.streaming.metricsEnabled", "true")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.compress", "false")
        .config("spark.eventLog.rolling.enabled", "false")
        .config("spark.eventLog.dir", event_dir.resolve().as_uri())
        .config("spark.cores.max", "2")
        .config("spark.executor.memory", "1g")
        .config("spark.executor.cores", "2")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
