from pathlib import Path
from pyspark.sql import SparkSession


def session(config, event_dir: Path):
    event_dir.mkdir(parents=True, exist_ok=True)
    builder = (
        SparkSession.builder.appName("instacart-module2")
        .master(config.master)
        .config("spark.sql.shuffle.partitions", config.shuffle_partitions)
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.debug.maxToStringFields", "200")
        .config("spark.sql.maxMetadataStringLength", "10000")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.compress", "false")
        .config("spark.eventLog.rolling.enabled", "false")
        .config("spark.eventLog.dir", event_dir.resolve().as_uri())
        .config("spark.cores.max", str(config.total_cores))
        .config("spark.executor.memory", config.executor_memory)
        .config("spark.executor.cores", str(config.executor_cores))
        .config("spark.dynamicAllocation.enabled", "false")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
