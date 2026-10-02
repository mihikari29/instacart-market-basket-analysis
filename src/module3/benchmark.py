"""Measured streaming-load and real Kafka-partition benchmarks."""

from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path
import statistics
import subprocess
import sys

from .config import Config
from .kafka_utils import create_topic, safe_topic_name, topic_metadata
from .stream import PROGRESS_LOG_NAME, run_stream


THROUGHPUT_TARGET_RATES = (100.0, 500.0, 0.0)
PARTITION_COUNTS = (1, 2, 4, 8)


def _median(values):
    present = [value for value in values if value is not None]
    return statistics.median(present) if present else None


def _source_partition_count(row: dict) -> int | None:
    counts = []
    for source in row.get("sources") or []:
        for offsets in (source.get("endOffset") or {}).values():
            if isinstance(offsets, dict):
                counts.append(len(offsets))
    return max(counts) if counts else None


def summarize_progress(progress_path: Path) -> dict:
    """Summarize non-empty batches while retaining total trigger evidence."""
    rows = []
    if progress_path.exists():
        with progress_path.open(encoding="utf-8") as stream:
            for line in stream:
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    non_empty = [row for row in rows if (row.get("numInputRows") or 0) > 0]
    measured = non_empty or rows
    return {
        "batches_logged": len(rows),
        "non_empty_batches": len(non_empty),
        "total_input_rows": sum(int(row.get("numInputRows") or 0) for row in rows),
        "input_rows_per_second_median": _median(
            row.get("inputRowsPerSecond") for row in measured
        ),
        "processed_rows_per_second_median": _median(
            row.get("processedRowsPerSecond") for row in measured
        ),
        "processed_rows_per_second_max": max(
            (row.get("processedRowsPerSecond") or 0.0 for row in measured),
            default=None,
        ),
        "trigger_execution_ms_median": _median(
            (row.get("durationMs") or {}).get("triggerExecution") for row in measured
        ),
        "add_batch_ms_median": _median(
            (row.get("durationMs") or {}).get("addBatch") for row in measured
        ),
        "consumer_partitions_observed": max(
            (count for count in (_source_partition_count(row) for row in rows) if count),
            default=None,
        ),
    }


def _producer_command(
    config: Config,
    feed: Path,
    topic: str,
    event_count: int,
    target_rate: float,
    receipt_path: Path,
) -> list[str]:
    return [
        sys.executable,
        "-m",
        "src.module1.producer",
        "--feed",
        str(feed),
        "--bootstrap",
        config.bootstrap_servers,
        "--topic",
        topic,
        "--limit-events",
        str(event_count),
        "--replay-speed",
        "0",
        "--target-events-per-second",
        str(target_rate),
        "--start-delay-seconds",
        "5",
        "--receipt",
        str(receipt_path),
    ]


def _run_trial(
    spark,
    config: Config,
    trial_dir: Path,
    checkpoint_id: str,
    topic: str,
    partitions: int,
    feed: Path,
    event_count: int,
    target_rate: float,
    duration_seconds: int,
) -> dict:
    trial_dir.mkdir(parents=True, exist_ok=False)
    before = create_topic(config.bootstrap_servers, topic, partitions=partitions, reset=True)
    receipt_path = trial_dir / "producer_receipt.json"
    process = None

    trial_config = replace(
        config,
        topic=topic,
        starting_offsets="earliest",
        trigger_interval="3 seconds",
    )

    def start_producer(_query):
        nonlocal process
        process = subprocess.Popen(
            _producer_command(config, feed, topic, event_count, target_rate, receipt_path),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        return {
            "pid": process.pid,
            "requested_events": event_count,
            "target_events_per_second": target_rate,
        }

    stream_info = run_stream(
        spark,
        trial_config,
        trial_dir,
        checkpoint_id,
        duration_seconds=duration_seconds,
        checkpoint_id=checkpoint_id,
        on_query_started=start_producer,
    )

    if process is None:
        raise RuntimeError("Producer did not start")
    try:
        producer_output, _ = process.communicate(timeout=30)
    except subprocess.TimeoutExpired:
        process.terminate()
        producer_output, _ = process.communicate(timeout=10)
    (trial_dir / "producer.log").write_text(producer_output, encoding="utf-8")
    if process.returncode != 0:
        raise RuntimeError(f"Producer failed with exit {process.returncode}: {producer_output}")
    if not receipt_path.exists():
        raise RuntimeError(f"Producer receipt missing: {receipt_path}")

    producer_receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    after = topic_metadata(config.bootstrap_servers, topic)
    progress = summarize_progress(trial_dir / PROGRESS_LOG_NAME)
    kafka_delta = after["total_records"] - before["total_records"]
    if kafka_delta != producer_receipt["sent_events"]:
        raise RuntimeError(
            f"Kafka offset delta {kafka_delta} != producer sent {producer_receipt['sent_events']}"
        )
    if progress["total_input_rows"] <= 0:
        raise RuntimeError(f"Spark processed no input rows: {progress}")
    return {
        "topic_before": before,
        "topic_after": after,
        "kafka_records_added": kafka_delta,
        "producer": producer_receipt,
        "progress_summary": progress,
        "stream": stream_info,
    }


def _arm_summary(runs: list[dict]) -> dict:
    summaries = [run["progress_summary"] for run in runs]
    return {
        "median_input_rows_per_second": _median(
            item["input_rows_per_second_median"] for item in summaries
        ),
        "median_processed_rows_per_second": _median(
            item["processed_rows_per_second_median"] for item in summaries
        ),
        "median_trigger_execution_ms": _median(
            item["trigger_execution_ms_median"] for item in summaries
        ),
        "median_total_input_rows": _median(item["total_input_rows"] for item in summaries),
    }


def benchmark_throughput(
    spark,
    config: Config,
    output_dir: Path,
    duration_seconds: int = 30,
    feed: Path = Path("data/synthesized/module3_dev"),
    event_count: int = 500,
    target_rates: tuple[float, ...] = THROUGHPUT_TARGET_RATES,
) -> dict:
    """Vary an explicit producer send-rate limit and measure Spark progress."""
    if duration_seconds < 12:
        raise ValueError("Throughput trials require at least 12 seconds")
    arms = {}
    benchmark_root = output_dir / "throughput"
    for rate in target_rates:
        rate_label = "bulk" if rate == 0 else str(rate).replace(".", "_")
        arm_name = f"target_{rate_label}_eps"
        arm_root = benchmark_root / arm_name
        warmups = []
        runs = []
        for kind, count in (("warmup", config.warmups), ("run", config.repetitions)):
            for index in range(count):
                trial_name = f"{kind}_{index}"
                topic = safe_topic_name(f"m3-throughput-{output_dir.name}-{rate_label}-{kind}-{index}")
                result = _run_trial(
                    spark,
                    config,
                    arm_root / trial_name,
                    checkpoint_id=topic,
                    topic=topic,
                    partitions=4,
                    feed=feed,
                    event_count=event_count,
                    target_rate=rate,
                    duration_seconds=duration_seconds,
                )
                (warmups if kind == "warmup" else runs).append(result)
        arms[arm_name] = {
            "producer_configuration": {
                "event_count": event_count,
                "target_events_per_second": rate,
                "semantics": "explicit wall-clock send-rate limit; 0 means unbounded bulk",
            },
            "warmups": warmups,
            "runs": runs,
            "summary": _arm_summary(runs),
        }
    return {
        "arms": arms,
        "duration_seconds_per_trial": duration_seconds,
        "measured_repetitions_per_arm": config.repetitions,
        "warmups_per_arm": config.warmups,
    }


def benchmark_kafka_partitions(
    spark,
    config: Config,
    output_dir: Path,
    duration_seconds: int = 30,
    feed: Path = Path("data/synthesized/module3_dev"),
    event_count: int = 500,
    partition_counts: tuple[int, ...] = PARTITION_COUNTS,
) -> dict:
    """Measure topics physically created with 1, 2, 4, and 8 partitions."""
    if duration_seconds < 12:
        raise ValueError("Partition trials require at least 12 seconds")
    arms = {}
    benchmark_root = output_dir / "partitions"
    for partitions in partition_counts:
        arm_root = benchmark_root / f"partitions_{partitions}"
        warmups = []
        runs = []
        for kind, count in (("warmup", config.warmups), ("run", config.repetitions)):
            for index in range(count):
                topic = safe_topic_name(
                    f"m3-partitions-{output_dir.name}-p{partitions}-{kind}-{index}"
                )
                result = _run_trial(
                    spark,
                    config,
                    arm_root / f"{kind}_{index}",
                    checkpoint_id=topic,
                    topic=topic,
                    partitions=partitions,
                    feed=feed,
                    event_count=event_count,
                    target_rate=0,
                    duration_seconds=duration_seconds,
                )
                observed = result["topic_after"]["partitions"]
                if observed != partitions:
                    raise RuntimeError(f"Requested {partitions} partitions, broker reports {observed}")
                (warmups if kind == "warmup" else runs).append(result)
        arms[f"partitions_{partitions}"] = {
            "requested_partitions": partitions,
            "observed_partitions": sorted(
                {run["topic_after"]["partitions"] for run in warmups + runs}
            ),
            "warmups": warmups,
            "runs": runs,
            "summary": _arm_summary(runs),
        }
    return {
        "arms": arms,
        "duration_seconds_per_trial": duration_seconds,
        "event_count_per_trial": event_count,
        "measured_repetitions_per_arm": config.repetitions,
        "warmups_per_arm": config.warmups,
    }
