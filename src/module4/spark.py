"""Spark runtime used by the Module 4A/4B batch jobs."""

from pathlib import Path


def clear_job_group(context):
    """Clear group properties through the public PySpark 3.5 API."""
    for name in ("spark.jobGroup.id", "spark.job.description", "spark.job.interruptOnCancel"):
        context.setLocalProperty(name, None)


def session(config, event_dir: Path | None = None):
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("instacart-module4")
        .master(config.master)
        .config("spark.sql.shuffle.partitions", config.shuffle_partitions)
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.debug.maxToStringFields", "200")
        .config("spark.sql.maxMetadataStringLength", "10000")
        .config("spark.eventLog.enabled", "true" if event_dir else "false")
        .config("spark.cores.max", config.total_cores)
        .config("spark.executor.memory", config.executor_memory)
        .config("spark.executor.cores", config.executor_cores)
    )
    if event_dir:
        event_dir.mkdir(parents=True, exist_ok=True)
        builder = builder.config("spark.eventLog.compress", "false").config(
            "spark.eventLog.rolling.enabled", "false"
        ).config("spark.eventLog.dir", event_dir.resolve().as_uri())
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
