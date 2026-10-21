"""Spark session for the Module 2 batch layer.

The project-wide Spark invariants live in `src.common.spark`; this
module only names the application and supplies the Module 2 settings.
"""

from pathlib import Path

from ..common.spark import build_session


def session(config, event_dir: Path):
    """Create the Module 2 Spark session."""
    return build_session(
        config,
        event_dir,
        app_name="instacart-module2",
        total_cores=config.total_cores,
        executor_memory=config.executor_memory,
        executor_cores=config.executor_cores,
    )
