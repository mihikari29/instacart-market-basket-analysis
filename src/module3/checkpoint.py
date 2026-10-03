"""Checkpoint location management for streaming queries.

Checkpoint identity is deliberately separate from evidence run identity so a
later process can resume Kafka offsets and aggregation state.
"""

from pathlib import Path


def ensure_checkpoint(output_dir: Path, checkpoint_id: str) -> str:
    checkpoint = (output_dir / "checkpoint" / checkpoint_id).resolve()
    checkpoint.mkdir(parents=True, exist_ok=True)
    return checkpoint.as_uri()


def reset_checkpoint(output_dir: Path, run_id: str) -> None:
    checkpoint = (output_dir / "checkpoint" / run_id).resolve()
    if checkpoint.exists():
        import shutil

        shutil.rmtree(checkpoint)
    checkpoint.mkdir(parents=True, exist_ok=True)
