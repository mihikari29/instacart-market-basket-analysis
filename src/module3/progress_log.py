"""Bounded capture and same-batch pairing for completed Spark progress."""

import json
from collections import OrderedDict
from collections.abc import Mapping
from pathlib import Path
import re
from threading import Condition, Thread
import time


PROGRESS_LOG_NAME = "streaming_progress.jsonl"
MAX_PENDING_SINK_RESULTS = 16


def is_executed_batch(progress) -> bool:
    """Identify Spark 3.5.5 batches from the runBatch-only addBatch duration."""
    durations = progress.get("durationMs")
    # MicroBatchExecution records addBatch inside runBatch. finishTrigger also
    # publishes idle observations, but those did not execute runBatch and lack
    # this duration. A state-cleanup batch does execute runBatch, even with
    # numInputRows == 0, so row count is deliberately not used as the marker.
    return isinstance(durations, Mapping) and "addBatch" in durations


class CompletedBatchPublisher:
    """Pair every observed executed batch with its bounded foreachBatch receipt."""

    def __init__(self, publish, *, max_pending=MAX_PENDING_SINK_RESULTS):
        if type(max_pending) is not int or max_pending <= 0:
            raise ValueError("max_pending must be a positive integer")
        self._publish = publish
        self._max_pending = max_pending
        self._condition = Condition()
        self._sink_results = OrderedDict()
        self._last_batch_id = None
        self._failure = None

    def record_sink_result(self, batch_id, result):
        """Register a completed sink receipt, applying backpressure at the bound."""
        if isinstance(batch_id, bool) or not isinstance(batch_id, int) or batch_id < 0:
            raise ValueError("sink receipt has an invalid batch ID")
        with self._condition:
            while len(self._sink_results) >= self._max_pending and self._failure is None:
                self._condition.wait()
            if self._failure is not None:
                raise RuntimeError("stream metrics progress publisher failed")
            if batch_id in self._sink_results or (
                self._last_batch_id is not None and batch_id <= self._last_batch_id
            ):
                self._fail_locked("duplicate sink receipt for completed batch")
                raise RuntimeError(self._failure)
            self._sink_results[batch_id] = result

    def fail(self):
        """Wake a blocked sink callback if the progress logger itself fails."""
        with self._condition:
            self._fail_locked("stream progress logger failed")

    @property
    def failure_message(self):
        """Return a fixed, sanitized failure summary for the owning stream."""
        with self._condition:
            return self._failure

    def _fail_locked(self, message):
        if self._failure is None:
            self._failure = message
        self._condition.notify_all()

    def __call__(self, progress):
        """Publish one executed progress record; ignore idle observations."""
        if not is_executed_batch(progress):
            return False

        batch_id = progress.get("batchId")
        if isinstance(batch_id, bool) or not isinstance(batch_id, int) or batch_id < 0:
            with self._condition:
                self._fail_locked("executed progress has an invalid batch ID")
            raise RuntimeError("executed progress has an invalid batch ID")

        with self._condition:
            if self._failure is not None:
                raise RuntimeError("stream metrics progress publisher failed")
            if self._last_batch_id is not None and batch_id <= self._last_batch_id:
                return False
            if self._last_batch_id is not None and batch_id != self._last_batch_id + 1:
                self._fail_locked("completed Spark batch progress was lost")
                raise RuntimeError("completed Spark batch progress was lost")
            result = self._sink_results.get(batch_id)
            if result is None:
                self._fail_locked("completed progress has no matching bounded sink receipt")
                raise RuntimeError(self._failure)
            if result.get("batch_id") != batch_id:
                self._fail_locked("sink receipt batch ID does not match completed progress")
                raise RuntimeError(self._failure)

        try:
            self._publish(progress, result)
        except Exception:
            with self._condition:
                self._fail_locked("stream metrics publication failed")
            raise

        with self._condition:
            self._sink_results.pop(batch_id, None)
            self._last_batch_id = batch_id
            self._condition.notify_all()
        return True


def _progress_record(progress):
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
        "batchId": progress.get("batchId"),
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


def progress_logger(
    query,
    output_dir: Path,
    stop_event,
    on_progress=None,
    errors=None,
    *,
    on_error=None,
    poll_interval=2.0,
) -> Thread:
    """Capture executed batches until the owner confirms query shutdown."""
    log_path = output_dir / PROGRESS_LOG_NAME
    log_path.parent.mkdir(parents=True, exist_ok=True)
    last_batch_id = None

    def _drain(fh):
        nonlocal last_batch_id
        for progress in query.recentProgress:
            if not is_executed_batch(progress):
                continue
            batch_id = progress.get("batchId")
            if batch_id is None or (last_batch_id is not None and batch_id <= last_batch_id):
                continue
            if (
                on_progress is not None
                and last_batch_id is not None
                and batch_id != last_batch_id + 1
            ):
                raise RuntimeError("completed Spark batch progress was lost")
            record = _progress_record(progress)
            fh.write(json.dumps(record, default=str) + "\n")
            fh.flush()
            if on_progress is not None:
                on_progress(progress)
            last_batch_id = batch_id

    def _record_error(exc):
        if errors is not None:
            summary = getattr(on_progress, "failure_message", None)
            # Keep the exception type and its message, so a failed publisher
            # is diagnosable instead of only reporting a class name.
            errors[:] = [summary or f"{type(exc).__name__}: {exc}"]
        if on_error is not None:
            try:
                on_error()
            except Exception:
                pass

    def _loop():
        try:
            with log_path.open("a", encoding="utf-8") as fh:
                while not stop_event.is_set():
                    try:
                        _drain(fh)
                        if stop_event.wait(poll_interval):
                            break
                    except Exception as exc:
                        _record_error(exc)
                        return

                try:
                    _drain(fh)
                    fh.flush()
                except Exception as exc:
                    _record_error(exc)
        except Exception as exc:
            _record_error(exc)

    thread = Thread(target=_loop, daemon=False)
    return thread


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
