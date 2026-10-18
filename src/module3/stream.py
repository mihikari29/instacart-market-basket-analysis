"""Stream runner: Kafka -> trending -> foreachBatch MongoDB upsert.

Designed for the `run` and `late-demo` CLI subcommands and reused by benchmark
orchestration with finite durations.
"""

import json
import time
from dataclasses import asdict
from pathlib import Path
from threading import Event

from .config import Config
from .parse import from_kafka
from .sink_mongo import write_mongo
from .trend_bundle import BUNDLE_COLLECTION_NAME, write_trend_bundles
from .trending import build_window_counts, rank_trending_batch, select_top_k
from .checkpoint import ensure_checkpoint
from .latency import summarize_ingestion_to_output
from .replay import wait_for_replay
from .metrics import build_snapshot, publish_snapshot


from .progress_log import (
    PROGRESS_LOG_NAME,
    CompletedBatchPublisher,
    progress_logger as _progress_logger,
)
CHECKPOINT_STATE_VERSION = "ingestion-latency-v1"


def _publish_finalized_batch(batch_df, batch_id, config: Config) -> dict:
    """Publish finalized Top-K rows, then optional immutable window bundles."""
    ranked = rank_trending_batch(batch_df, config)
    top_k = select_top_k(ranked, config)

    # Keep the established row sink first. If it raises, bundles are not attempted.
    result = write_mongo(top_k, batch_id, config)
    result["sink_write_duration_ms"] = result.get("duration_ms")
    timestamp_stats = result.pop(
        "ingestion_timestamp_stats", {"count": 0, "sum": 0, "min": None, "max": None}
    )
    if not config.publish_trend_bundles:
        result["ingestion_to_output_latency"] = summarize_ingestion_to_output(
            timestamp_stats, time.time_ns() // 1_000_000
        )
        return result

    if not config.trend_source_namespace:
        # Config validates this invariant; retain a local guard for non-Config
        # callers and to prevent publication with an unstable implicit identity.
        raise ValueError(
            "trend_source_namespace is required when trend bundle publication is enabled"
        )

    started = time.perf_counter()
    from pymongo import MongoClient

    with MongoClient(config.mongodb_uri, serverSelectionTimeoutMS=10000) as client:
        collection = client[config.mongo_database][BUNDLE_COLLECTION_NAME]
        bundle_report = write_trend_bundles(
            top_k,
            collection,
            config,
            source_namespace=config.trend_source_namespace,
            source_scope=config.trend_source_scope,
            retention_seconds=config.ttl_seconds,
        )

    result = dict(result)
    result["trend_bundles"] = {
        "collection": BUNDLE_COLLECTION_NAME,
        "source_namespace": config.trend_source_namespace,
        "source_scope": config.trend_source_scope,
        **asdict(bundle_report),
        "publication_duration_ms": round((time.perf_counter() - started) * 1000, 3),
    }
    result["ingestion_to_output_latency"] = summarize_ingestion_to_output(
        timestamp_stats, time.time_ns() // 1_000_000
    )
    return result


def _foreach_batch(batch_df, batch_id, config: Config, sink_log: Path) -> dict:
    """Log only after every enabled publisher has completed successfully."""
    result = _publish_finalized_batch(batch_df, batch_id, config)
    with sink_log.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(result, default=str, sort_keys=True) + "\n")
    return result



def run_stream(
    spark,
    config: Config,
    output_dir: Path,
    run_id: str,
    duration_seconds: int | None = None,
    checkpoint_id: str | None = None,
    checkpoint_root: Path | None = None,
    on_query_started=None,
    until_complete=None,
) -> dict:
    """Run until the duration expires, or indefinitely until interrupted."""
    events = from_kafka(spark, config)
    counts = build_window_counts(events, config)
    checkpoint_identity = checkpoint_id or run_id
    checkpoint = ensure_checkpoint(
        output_dir.parent,
        checkpoint_identity,
        checkpoint_root=checkpoint_root,
        state_version=CHECKPOINT_STATE_VERSION,
    )
    sink_log = output_dir / "sink_batches.jsonl"
    metrics_errors = []
    stop_event = Event()

    def _publish_progress(progress, sink_result):
        document = build_snapshot(
            progress,
            sink_result,
            source_namespace=config.trend_source_namespace,
            source_scope=config.trend_source_scope,
            run_id=run_id,
        )
        publish_snapshot(config, document)

    progress_publisher = (
        CompletedBatchPublisher(_publish_progress)
        if config.publish_trend_bundles
        else None
    )

    def _foreach(batch_df, batch_id):
        result = _foreach_batch(batch_df, batch_id, config, sink_log)
        if progress_publisher is not None:
            progress_publisher.record_sink_result(
                batch_id,
                {
                    "batch_id": batch_id,
                    "ingestion_to_output_latency": result["ingestion_to_output_latency"],
                },
            )

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
        "checkpoint_state_version": CHECKPOINT_STATE_VERSION,
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

    logger_thread = _progress_logger(
        query,
        output_dir,
        stop_event,
        progress_publisher,
        metrics_errors,
        on_error=progress_publisher.fail if progress_publisher is not None else None,
    )
    logger_thread.start()

    def _raise_metrics_error():
        if metrics_errors:
            raise RuntimeError(
                f"stream progress/metrics publication failed: {metrics_errors[0]}"
            )

    try:
        if on_query_started is not None:
            info["orchestration"] = on_query_started(query)
        if until_complete is not None:
            if duration_seconds is None or duration_seconds <= 0:
                raise ValueError("completion-driven streaming requires a positive hard deadline")
            info["completion"] = wait_for_replay(
                query,
                until_complete,
                duration_seconds - (time.monotonic() - started_monotonic),
                check_progress=_raise_metrics_error,
            )
        elif duration_seconds is not None and duration_seconds > 0:
            deadline = started_monotonic + duration_seconds
            while query.isActive:
                _raise_metrics_error()
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                query.awaitTermination(min(1.0, remaining))
            _raise_metrics_error()
            if not query.isActive:
                failure = query.exception()
                if failure is not None:
                    raise failure
        else:
            # Do not block inside a Py4J call forever: PySpark's SIGINT handler
            # also uses the gateway and can otherwise re-enter it on Ctrl+C.
            while query.isActive:
                _raise_metrics_error()
                time.sleep(1.0)
            failure = query.exception()
            if failure is not None:
                raise failure
    except KeyboardInterrupt:
        info["interrupted"] = True
    finally:
        try:
            # Spark can report isActive=False before its execution thread has
            # published final progress. stop() waits for that thread, so only
            # signal the logger after this synchronous shutdown has completed.
            query.stop()
        finally:
            stop_event.set()
            logger_thread.join()

    if logger_thread.is_alive() or metrics_errors:
        detail = metrics_errors[0] if metrics_errors else "progress logger did not stop"
        raise RuntimeError(f"stream progress/metrics publication failed: {detail}")

    info["last_progress"] = list(query.recentProgress)[-1] if query.recentProgress else None
    return info
