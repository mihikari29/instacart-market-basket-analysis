"""Stream runner: Kafka -> trending -> foreachBatch MongoDB upsert.

Designed for the `run` and `late-demo` CLI subcommands. Benchmarks use
benchmark.py which drives its own queries with explicit AvailableNow triggers.
"""

import json
import time
from pathlib import Path
from threading import Thread

from pyspark.sql.streaming import StreamingQuery

from .config import Config
from .parse import from_kafka
from .sink_mongo import write_mongo
from .trending import build_trending
from .checkpoint import ensure_checkpoint


PROGRESS_LOG_NAME = "streaming_progress.jsonl"


def _progress_logger(query: StreamingQuery, output_dir: Path, stop_event) -> Thread:
    """Background thread that polls recentProgress into a JSONL log."""
    log_path = output_dir / PROGRESS_LOG_NAME
    log_path.parent.mkdir(parents=True, exist_ok=True)

    def _loop():
        with log_path.open("a", encoding="utf-8") as fh:
            while not stop_event.is_set() and query.isActive:
                try:
                    for progress in query.recentProgress:
                        progress_record = {
                            "captured_at": time.time(),
                            "id": progress.get("id"),
                            "batch_id": progress.get("batchId"),
                            "input_rows_per_second": progress.get("inputRowsPerSecond"),
                            "processed_rows_per_second": progress.get("processedRowsPerSecond"),
                            "trigger_duration_ms": (progress.get("triggerDuration") or {}).get("computeExecutionMs"),
                            "num_input_sources": len(progress.get("sources", [])),
                            "timestamp": progress.get("timestamp"),
                        }
                        fh.write(json.dumps(progress_record, default=str) + "\n")
                        fh.flush()
                except Exception:
                    pass
                stop_event.wait(2.0)

    thread = Thread(target=_loop, daemon=True)
    return thread


def run_stream(spark, config: Config, output_dir: Path, run_id: str, duration_seconds: int | None = None) -> dict:
    """Start the streaming query. If duration_seconds given, block until done."""
    from pyspark.sql.streaming import Trigger

    events = from_kafka(spark, config)
    trend = build_trending(events, config)
    checkpoint = ensure_checkpoint(output_dir, run_id)

    trigger = Trigger.ProcessingTime(config.trigger_interval) if config.trigger_interval else Trigger.ProcessingTime()

    def _foreach(batch_df, batch_id):
        return write_mongo(batch_df, batch_id, config)

    query = (
        trend.writeStream
        .outputMode("update")
        .trigger(trigger)
        .option("checkpointLocation", checkpoint)
        .foreachBatch(_foreach)
        .queryName("instacart-realtime-trending")
        .start()
    )

    info = {
        "query_id": query.id,
        "query_name": query.name,
        "checkpoint_location": checkpoint,
        "trigger_interval": config.trigger_interval,
        "window_short": config.window_short,
        "window_long": config.window_long,
        "slide": config.slide,
        "watermark": config.watermark,
        "topic": config.topic,
        "progress_log": str(output_dir / PROGRESS_LOG_NAME),
    }

    if duration_seconds is None:
        return info

    stop_event = __import__("threading").Event()
    logger_thread = _progress_logger(query, output_dir, stop_event)
    logger_thread.start()
    try:
        deadline = time.time() + duration_seconds
        while query.isActive and time.time() < deadline:
            time.sleep(1.0)
    finally:
        query.awaitTermination(timeout=5)
        if query.isActive:
            query.stop()
        stop_event.set()
        logger_thread.join(timeout=5)

    info["last_progress"] = list(query.recentProgress)[-1] if query.recentProgress else None
    return info
