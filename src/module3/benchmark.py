"""Streaming throughput and Kafka partition sweep benchmarks (Proposal §15.4, §15.5).

Mirrors module2/benchmarks.py: warmup + measured reps with median summary.
Each arm writes its own progress log; the partition sweep resets checkpoint
between reps so state never leaks from a previous topic configuration.
"""

import json
import statistics
from pathlib import Path

from .config import Config
from .checkpoint import reset_checkpoint
from .stream import run_stream, PROGRESS_LOG_NAME


THROUGHPUT_RATES = (0, 50_000, 10_000)
PARTITION_COUNTS = (1, 2, 4, 8)


def _summarize_progress(progress_path: Path) -> dict:
    if not progress_path.exists():
        return {"rows_logged": 0}
    rows = []
    with progress_path.open(encoding="utf-8") as fh:
        for line in fh:
            try:
                rows.append(json.loads(line))
            except Exception:
                continue
    if not rows:
        return {"rows_logged": 0}
    rates = [r.get("processed_rows_per_second") for r in rows if r.get("processed_rows_per_second") is not None]
    durations = [r.get("trigger_duration_ms") for r in rows if r.get("trigger_duration_ms") is not None]
    return {
        "rows_logged": len(rows),
        "processed_rows_per_second_median": statistics.median(rates) if rates else None,
        "processed_rows_per_second_max": max(rates) if rates else None,
        "trigger_duration_ms_median": statistics.median(durations) if durations else None,
    }


def _run_one_stream(spark, config: Config, output_dir: Path, run_id: str, duration_seconds: int) -> dict:
    progress_path = output_dir / PROGRESS_LOG_NAME
    if progress_path.exists():
        progress_path.unlink()
    info = run_stream(spark, config, output_dir, run_id, duration_seconds=duration_seconds)
    info["progress_summary"] = _summarize_progress(progress_path)
    return info


def benchmark_throughput(spark, config: Config, output_dir: Path, duration_seconds: int = 60) -> dict:
    """(Proposal §15.4) Throughput vs producer rate; replay-speed is set externally.

    Driver for measurement; the producer is started by the user/cli separately
    with --replay-speed. Here we just collect query progress for the duration.
    """
    trials = {"warmups": [], "runs": []}
    for i in range(-config.warmups, config.repetitions):
        rid = f"{output_dir.name}__throughput__{i}"
        reset_checkpoint(output_dir.parent.parent, rid)
        measured = _run_one_stream(spark, config, output_dir, rid, duration_seconds)
        (trials["warmups"] if i < 0 else trials["runs"]).append(measured)
    runs = trials["runs"]
    rates = [r["progress_summary"].get("processed_rows_per_second_median") for r in runs if r.get("progress_summary")]
    return {
        "warmups": trials["warmups"],
        "runs": runs,
        "median_processed_rows_per_second": statistics.median(rates) if rates else None,
        "duration_seconds": duration_seconds,
        "producer_rates_used": THROUGHPUT_RATES,
        "note": "Producer must be started separately with --replay-speed for each rate; this benchmark measures the consumer side only.",
    }


def benchmark_kafka_partitions(spark, config: Config, output_dir: Path, duration_seconds: int = 60,
                               partition_counts: tuple = PARTITION_COUNTS) -> dict:
    """(Proposal §15.5) Sweep topic partition counts 1/2/4/8 and measure consumer parallelism.

    Caller (cli) is responsible for recreating the topic with each partition count
    via kafka-topics.sh before each arm. We only reset checkpoint and run.
    """
    arms = {}
    for n in partition_counts:
        arm_runs = []
        for i in range(config.repetitions):
            rid = f"{output_dir.name}__parts{n}__{i}"
            reset_checkpoint(output_dir.parent.parent, rid)
            run_info = _run_one_stream(spark, config, output_dir, rid, duration_seconds)
            run_info["topic_partitions"] = n
            arm_runs.append(run_info)
        rates = [r["progress_summary"].get("processed_rows_per_second_median") for r in arm_runs if r.get("progress_summary")]
        arms[f"partitions_{n}"] = {
            "runs": arm_runs,
            "median_processed_rows_per_second": statistics.median(rates) if rates else None,
        }
    return {
        "arms": arms,
        "duration_seconds": duration_seconds,
        "note": "Caller must recreate topic with --partitions n between arms (kafka-topics.sh --create --partitions n).",
    }
