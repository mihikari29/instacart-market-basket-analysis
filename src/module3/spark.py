"""Spark session for the Module 3 speed layer.

The project-wide Spark invariants live in `src.common.spark`. The
streaming metrics flag is added so that `recentProgress` exposes the
throughput counters required by Proposal section 15.4.
"""

from pathlib import Path

from ..common.spark import build_session


def session(config, event_dir: Path):
    """Create the Module 3 Spark session."""
    return build_session(
        config,
        event_dir,
        app_name="instacart-module3",
        total_cores=2,
        executor_memory="1g",
        executor_cores=2,
        extra_config={"spark.sql.streaming.metricsEnabled": "true"},
    )
