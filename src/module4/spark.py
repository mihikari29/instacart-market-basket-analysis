"""Spark runtime used by the Module 4A/4B batch jobs.

The project-wide Spark invariants live in `src.common.spark_session`.
"""

from pathlib import Path

from ..common.spark_session import build_session


def clear_job_group(context):
    """Clear group properties through the public PySpark 3.5 API."""
    for name in ("spark.jobGroup.id", "spark.job.description", "spark.job.interruptOnCancel"):
        context.setLocalProperty(name, None)


def session(config, event_dir: Path | None = None):
    """Create the Module 4 Spark session.

    The event log is only enabled when an event directory is supplied, so a
    diagnostic run does not write a multi-hundred-megabyte log.
    """
    return build_session(
        config,
        event_dir,
        app_name="instacart-module4",
        total_cores=config.total_cores,
        executor_memory=config.executor_memory,
        executor_cores=config.executor_cores,
    )
