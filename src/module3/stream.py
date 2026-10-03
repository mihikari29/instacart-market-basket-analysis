"""Stream runner: Kafka -> trending -> foreachBatch MongoDB upsert.

Designed for the `run` and `late-demo` CLI subcommands and reused by benchmark
orchestration with finite durations.
"""

import json
import time
from pathlib import Path
from threading import Thread

from pyspark.sql.streaming import StreamingQuery

from .config import Config
from .parse import from_kafka
from .sink_mongo import write_mongo
from .trending import build_window_counts, rank_trending_batch, select_top_k
from .checkpoint import ensure_checkpoint


PROGRESS_LOG_NAME = "streaming_progress.jsonl"


def _progress_logger(query: StreamingQuery, output_dir: Path, stop_event) -> Thread:
    """Background thread that polls recentProgress into a JSONL log."""
    log_path = output_dir / PROGRESS_LOG_NAME
    log_path.parent.mkdir(parents=True, exist_ok=True)
    seen_batch_ids = set()

    def _record(progress):
        batch_id = progress.get("batchId")
        if batch_id in seen_batch_ids:
            return None
        seen_batch_ids.add(batch_id)
        duration = progress.get("durationMs") or {}
        event_time = progress.get("eventTime") or {}
        state_operators = []
        for operator in progress.get("stateOperators") or []:
            state_operators.append(
                {
                    "numRowsTotal": operator.get("numRowsTotal"),
                    "numRowsUpdated": operator.get("numRowsUpdated"),
                    "numRowsRemoved": operator.get("numRowsRemoved"),
                    "numRowsDroppedByWatermark": operator.get("numRowsDroppedByWatermark"),
                    "customMetrics": operator.get("customMetrics") or {},
                }
            )
        return {
            "capturedAtEpoch": time.time(),
            "id": progress.get("id"),
            "batchId": batch_id,
            "numInputRows": progress.get("numInputRows"),
            "inputRowsPerSecond": progress.get("inputRowsPerSecond"),
            "processedRowsPerSecond": progress.get("processedRowsPerSecond"),
            "durationMs": {
                "triggerExecution": duration.get("triggerExecution"),
                "addBatch": duration.get("addBatch"),
                "getBatch": duration.get("getBatch"),
            },
            "eventTime": event_time,
            "observedMetrics": progress.get("observedMetrics") or {},
            "stateOperators": state_operators,
            "sources": progress.get("sources") or [],
            "sink": progress.get("sink") or {},
            "timestamp": progress.get("timestamp"),
        }

    def _loop():
        with log_path.open("a", encoding="utf-8") as fh:
            while True:
                try:
                    for progress in query.recentProgress:
                        progress_record = _record(progress)
                        if progress_record is not None:
                            fh.write(json.dumps(progress_record, default=str) + "\n")
                            fh.flush()
                except Exception:
                    pass
                if stop_event.wait(2.0) or not query.isActive:
                    break

            try:
                for progress in query.recentProgress:
                    progress_record = _record(progress)
                    if progress_record is not None:
                        fh.write(json.dumps(progress_record, default=str) + "\n")
                fh.flush()
            except Exception:
                pass

    thread = Thread(target=_loop, daemon=True)
    return thread


def run_stream(
    spark,
    config: Config,
    output_dir: Path,
    run_id: str,
    duration_seconds: int | None = None,
    checkpoint_id: str | None = None,
    on_query_started=None,
) -> dict:
    """Run until the duration expires, or indefinitely until interrupted."""
    events = from_kafka(spark, config)
    counts = build_window_counts(events, config)
    checkpoint_identity = checkpoint_id or run_id
    checkpoint = ensure_checkpoint(output_dir.parent, checkpoint_identity)
    sink_log = output_dir / "sink_batches.jsonl"

    def _foreach(batch_df, batch_id):
        ranked = rank_trending_batch(batch_df, config)
        top_k = select_top_k(ranked, config)
        result = write_mongo(top_k, batch_id, config)
        with sink_log.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(result, default=str, sort_keys=True) + "\n")

    writer = (
        counts.writeStream
        # Append emits only finalized windows. This guarantees foreachBatch sees
        # every product for a window_end together for complete normalization.
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .foreachBatch(_foreach)
        .queryName("instacart-realtime-trending")
    )
    if config.trigger_interval:
        writer = writer.trigger(processingTime=config.trigger_interval)
    query = writer.start()
    started_monotonic = time.monotonic()

    info = {
        "query_id": query.id,
        "query_name": query.name,
        "checkpoint_location": checkpoint,
        "checkpoint_id": checkpoint_identity,
        "trigger_interval": config.trigger_interval,
        "window_short": config.window_short,
        "window_long": config.window_long,
        "slide": config.slide,
        "watermark": config.watermark,
        "topic": config.topic,
        "serving_semantics": "finalized-event-time-windows",
        "mongo_top_k_per_window": config.mongo_top_k_per_window,
        "ttl_seconds": config.ttl_seconds,
        "progress_log": str(output_dir / PROGRESS_LOG_NAME),
        "sink_log": str(sink_log),
    }

    stop_event = __import__("threading").Event()
    logger_thread = _progress_logger(query, output_dir, stop_event)
    logger_thread.start()
    try:
        if on_query_started is not None:
            info["orchestration"] = on_query_started(query)
        if duration_seconds is not None and duration_seconds > 0:
            elapsed = time.monotonic() - started_monotonic
            query.awaitTermination(max(0.0, duration_seconds - elapsed))
        else:
            # Do not block inside a Py4J call forever: PySpark's SIGINT handler
            # also uses the gateway and can otherwise re-enter it on Ctrl+C.
            while query.isActive:
                time.sleep(1.0)
            failure = query.exception()
            if failure is not None:
                raise failure
    except KeyboardInterrupt:
        info["interrupted"] = True
    finally:
        if query.isActive:
            query.stop()
        stop_event.set()
        logger_thread.join(timeout=5)

    info["last_progress"] = list(query.recentProgress)[-1] if query.recentProgress else None
    return info
