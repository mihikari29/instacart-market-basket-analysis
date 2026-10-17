"""Checkpoint location management for streaming queries.

Checkpoint identity is deliberately separate from evidence run identity so a
later process can resume Kafka offsets and aggregation state.
"""

import json
import re
from pathlib import Path


class CheckpointCompatibilityError(ValueError):
    """Raised when a checkpoint does not match the streaming state contract."""


CHECKPOINT_VERSION_MARKER = "_module3_checkpoint_version.json"


def ensure_checkpoint(
    output_dir: Path,
    checkpoint_id: str,
    *,
    checkpoint_root: Path | None = None,
    state_version: str | None = None,
) -> str:
    base = checkpoint_root if checkpoint_root is not None else output_dir / "checkpoint"
    identity_root = (base / checkpoint_id).resolve()
    if state_version is None:
        checkpoint = identity_root
    else:
        if (
            not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,79}", state_version)
            or ".." in state_version
        ):
            raise ValueError("state_version must be a path-safe identifier")
        checkpoint = identity_root / state_version
        marker = identity_root / CHECKPOINT_VERSION_MARKER
        if checkpoint.exists():
            try:
                recorded = json.loads(marker.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise CheckpointCompatibilityError(
                    f"checkpoint {checkpoint} is missing a valid state-version marker"
                ) from exc
            if recorded != {"checkpoint_id": checkpoint_id, "state_version": state_version}:
                raise CheckpointCompatibilityError("checkpoint state-version marker does not match")
        else:
            if identity_root.exists() and any(identity_root.iterdir()):
                raise CheckpointCompatibilityError(
                    "an unversioned checkpoint exists; refusing to resume it with a changed state schema"
                )
            checkpoint.mkdir(parents=True, exist_ok=True)
            marker.write_text(
                json.dumps(
                    {"checkpoint_id": checkpoint_id, "state_version": state_version},
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
    checkpoint.mkdir(parents=True, exist_ok=True)
    return checkpoint.as_uri()


def reset_checkpoint(output_dir: Path, run_id: str) -> None:
    checkpoint = (output_dir / "checkpoint" / run_id).resolve()
    if checkpoint.exists():
        import shutil

        shutil.rmtree(checkpoint)
    checkpoint.mkdir(parents=True, exist_ok=True)
