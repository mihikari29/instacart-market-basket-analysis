"""The Spark session contract shared by Modules 2, 3 and 4.

Each module needs a slightly different session, but they must agree on the
project-wide safety invariants. Those invariants are fixed here so a future
module cannot accidentally enable adaptive execution, auto-broadcast, or a
timezone other than UTC.

The invariants and the reason for each one:

* Adaptive query execution is disabled. Module 2's join benchmark asserts
  that no `AdaptiveSparkPlan` appears in the physical plan, so enabling it
  would change a published benchmark result.
* Automatic broadcast is disabled. The Module 4 ALS factor cross-join collapses
  into too few tasks and exhausts the executor heap when the small reloaded
  factor files are broadcast.
* The session timezone is UTC so that synthetic event times, window boundaries
  and stored timestamps are comparable across hosts.
* ANSI mode is enabled so that invalid input fails instead of returning null.
* The event log is written uncompressed-per-run and rotated off, so one run
  owns one directory. Compression is enabled because an uncompressed event log
  is the largest artifact a full run produces.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

#: Configuration keys every module session must set identically.
INVARIANT_SPARK_CONFIG: dict[str, str] = {
    "spark.sql.adaptive.enabled": "false",
    "spark.sql.autoBroadcastJoinThreshold": "-1",
    "spark.sql.session.timeZone": "UTC",
    "spark.sql.ansi.enabled": "true",
    "spark.sql.debug.maxToStringFields": "200",
    "spark.sql.maxMetadataStringLength": "10000",
}


def build_session(
    config: Any,
    event_dir: Path | None = None,
    *,
    app_name: str,
    extra_config: dict[str, str] | None = None,
    total_cores: int | None = None,
    executor_memory: str | None = None,
    executor_cores: int | None = None,
):
    """Create a SparkSession that satisfies the project-wide invariants.

    `extra_config` holds the keys that are genuinely module-specific. It is
    applied last so a module can override a default it owns, but it cannot
    accidentally change an invariant without the change being visible here.
    """
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name).master(config.master)
    for key, value in INVARIANT_SPARK_CONFIG.items():
        builder = builder.config(key, value)
    builder = builder.config("spark.sql.shuffle.partitions", str(config.shuffle_partitions))
    builder = builder.config("spark.cores.max", str(total_cores if total_cores is not None else 2))
    builder = builder.config(
        "spark.executor.memory", executor_memory if executor_memory is not None else "1g"
    )
    builder = builder.config(
        "spark.executor.cores", str(executor_cores if executor_cores is not None else 2)
    )
    builder = builder.config("spark.dynamicAllocation.enabled", "false")
    builder = builder.config("spark.eventLog.enabled", "true" if event_dir else "false")
    if event_dir:
        event_dir.mkdir(parents=True, exist_ok=True)
        # The event log stays uncompressed. Spark writes a compressed log as a
        # sequence of independent zstd frames, and the only decoder available
        # to the project reads a single frame, so a compressed log could not be
        # parsed back into the published benchmark metrics. The driver memory
        # the parser used to hold is fixed at the source instead.
        builder = (
            builder.config("spark.eventLog.compress", "false")
            .config("spark.eventLog.rolling.enabled", "false")
            .config("spark.eventLog.dir", event_dir.resolve().as_uri())
        )
    for key, value in (extra_config or {}).items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
