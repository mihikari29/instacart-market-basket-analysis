"""Run identity, run directories and JSON report writing.

Every module writes a `results/<module>/<run-id>/` directory containing a JSON
summary. The run identifier, the directory creation rule and the report
encoding were previously re-implemented in each module, which let the three
copies drift.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import re
import uuid

RUN_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,79}")


def new_run_id() -> str:
    """Return a sortable, collision-resistant run identifier."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:8]


def validate_run_id(run_id: str) -> str:
    """Require a path-safe run identifier."""
    if not RUN_ID_PATTERN.fullmatch(run_id or "") or ".." in run_id:
        raise ValueError("run_id must be a 1-80 character path-safe identifier")
    return run_id


def create_run_directory(base: Path, run_id: str) -> Path:
    """Create and return the directory that owns one run's artifacts."""
    validate_run_id(run_id)
    directory = base / run_id
    directory.mkdir(parents=True, exist_ok=False)
    return directory


def save_json(path: Path, value) -> None:
    """Write a JSON report, creating the parent directory when needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8"
    )
