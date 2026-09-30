"""Checkpoint location management for streaming queries.

Each run_id gets its own checkpoint dir so concurrent runs and partition
sweep benchmarks don't trample state. Reset deletes the dir for clean re-runs.
"""

from pathlib import Path


def ensure_checkpoint(output_dir: Path, run_id: str) -> str:
    checkpoint = (output_dir / "checkpoint" / run_id).resolve()
    checkpoint.mkdir(parents=True, exist_ok=True)
    return checkpoint.as_uri()


def reset_checkpoint(output_dir: Path, run_id: str) -> None:
    checkpoint = (output_dir / "checkpoint" / run_id).resolve()
    if checkpoint.exists():
        import shutil

        shutil.rmtree(checkpoint)
    checkpoint.mkdir(parents=True, exist_ok=True)
