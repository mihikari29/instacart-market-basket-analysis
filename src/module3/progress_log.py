"""Constant-memory background capture of completed Spark batch progress."""

import json
from pathlib import Path
from threading import Thread
import time

PROGRESS_LOG_NAME = "streaming_progress.jsonl"


def progress_logger(query, output_dir: Path, stop_event, on_progress=None, errors=None) -> Thread:
    """Background thread that polls recentProgress into a JSONL log."""
    log_path = output_dir / PROGRESS_LOG_NAME
    log_path.parent.mkdir(parents=True, exist_ok=True)
    last_batch_id = -1

    def _record(progress):
        nonlocal last_batch_id
        batch_id = progress.get("batchId")
        if batch_id is None or batch_id <= last_batch_id:
            return None
        last_batch_id = batch_id
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
                    latest = None
                    for progress in query.recentProgress:
                        progress_record = _record(progress)
                        if progress_record is not None:
                            fh.write(json.dumps(progress_record, default=str) + "\n")
                            fh.flush()
                            latest = progress
                    if latest is not None and on_progress is not None:
                        on_progress(latest)
                except Exception as exc:
                    if errors is not None:
                        errors[:] = [type(exc).__name__]
                        return
                if stop_event.wait(2.0) or not query.isActive:
                    break

            try:
                latest = None
                for progress in query.recentProgress:
                    progress_record = _record(progress)
                    if progress_record is not None:
                        fh.write(json.dumps(progress_record, default=str) + "\n")
                        latest = progress
                if latest is not None and on_progress is not None:
                    on_progress(latest)
                fh.flush()
            except Exception as exc:
                if errors is not None:
                    errors[:] = [type(exc).__name__]

    thread = Thread(target=_loop, daemon=True)
    return thread
