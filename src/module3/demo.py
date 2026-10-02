"""Deterministic live scenarios for watermark and late-data validation."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
import json
from pathlib import Path
import time

from .config import Config
from .kafka_utils import create_topic, produce_json_events, safe_topic_name
from .stream import run_stream


def _epoch_ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def _event(event_id: str, product_id: int, event_time: datetime) -> dict:
    epoch_ms = _epoch_ms(event_time)
    return {
        "event_id": event_id,
        "order_id": epoch_ms + product_id,
        "user_id": 900001,
        "product_id": product_id,
        "add_to_cart_order": 1,
        "reordered": False,
        "aisle_id": 1,
        "department_id": 1,
        "order_dow": event_time.weekday(),
        "order_hour_of_day": event_time.hour,
        "event_time_epoch_ms": epoch_ms,
        "event_time_iso": event_time.isoformat(),
        "ingestion_time_epoch_ms": int(time.time() * 1000),
    }


def _batch_id(progress: dict | None) -> int:
    return int((progress or {}).get("batchId", -1))


def _latest_batch(query) -> int:
    return max((_batch_id(item) for item in query.recentProgress), default=-1)


def _watermark(progress: dict) -> datetime | None:
    value = (progress.get("eventTime") or {}).get("watermark")
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _dropped(progress: dict) -> int:
    return sum(
        int(operator.get("numRowsDroppedByWatermark") or 0)
        for operator in progress.get("stateOperators") or []
    )


def _wait_for(query, predicate, description: str, after_batch: int = -1, timeout: float = 40) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for progress in reversed(list(query.recentProgress)):
            if _batch_id(progress) > after_batch and predicate(progress):
                return progress
        if not query.isActive:
            raise RuntimeError(f"Stream stopped while waiting for {description}")
        time.sleep(0.25)
    raise TimeoutError(f"Timed out waiting for {description}; lastProgress={query.lastProgress}")


def _progress_evidence(progress: dict) -> dict:
    return {
        "batchId": progress.get("batchId"),
        "numInputRows": progress.get("numInputRows"),
        "eventTime": progress.get("eventTime"),
        "stateOperators": [
            {
                key: operator.get(key)
                for key in (
                    "numRowsTotal",
                    "numRowsUpdated",
                    "numRowsRemoved",
                    "numRowsDroppedByWatermark",
                )
            }
            for operator in progress.get("stateOperators") or []
        ],
    }


def run_late_data_demo(
    spark,
    config: Config,
    output_dir: Path,
    run_id: str,
    duration_seconds: int,
    checkpoint_id: str | None = None,
) -> dict:
    """Exercise on-time, within-watermark, and dropped late events."""
    topic = safe_topic_name(f"{config.topic}-late-demo")
    metadata = create_topic(config.bootstrap_servers, topic, partitions=1, reset=True)
    demo_config = replace(
        config,
        topic=topic,
        starting_offsets="earliest",
        trigger_interval="3 seconds",
    )

    on_time_at = datetime(2030, 1, 1, 0, 20, tzinfo=timezone.utc)
    within_at = datetime(2030, 1, 1, 0, 15, tzinfo=timezone.utc)
    too_late_at = datetime(2030, 1, 1, 0, 18, tzinfo=timezone.utc)
    advance_at = datetime(2030, 1, 1, 3, 0, tzinfo=timezone.utc)
    expected_watermark = datetime(2030, 1, 1, 2, 50, tzinfo=timezone.utc)
    selected_window_end = datetime(2030, 1, 1, 0, 25, tzinfo=timezone.utc)
    started_at = datetime.now(timezone.utc)

    events = {
        "on_time": _event("late-demo-on-time", 4242, on_time_at),
        "within_watermark": _event("late-demo-within", 4242, within_at),
        "watermark_advance": _event("late-demo-advance", 9999, advance_at),
        "beyond_watermark": _event("late-demo-dropped", 4242, too_late_at),
    }

    def orchestrate(query):
        evidence = {}

        previous = _latest_batch(query)
        produce_json_events(config.bootstrap_servers, topic, [events["on_time"]])
        progress = _wait_for(
            query,
            lambda item: item.get("numInputRows", 0) >= 1,
            "on-time event batch",
            after_batch=previous,
        )
        evidence["on_time"] = _progress_evidence(progress)

        progress = _wait_for(
            query,
            lambda item: (_watermark(item) or datetime.min.replace(tzinfo=timezone.utc))
            >= datetime(2030, 1, 1, 0, 10, tzinfo=timezone.utc),
            "initial watermark advancement",
            after_batch=_batch_id(progress),
        )

        previous = _batch_id(progress)
        produce_json_events(config.bootstrap_servers, topic, [events["within_watermark"]])
        progress = _wait_for(
            query,
            lambda item: item.get("numInputRows", 0) >= 1 and _dropped(item) == 0,
            "within-watermark event batch",
            after_batch=previous,
        )
        evidence["within_watermark"] = _progress_evidence(progress)

        previous = _batch_id(progress)
        produce_json_events(config.bootstrap_servers, topic, [events["watermark_advance"]])
        progress = _wait_for(
            query,
            lambda item: item.get("numInputRows", 0) >= 1,
            "watermark advance event batch",
            after_batch=previous,
        )
        progress = _wait_for(
            query,
            lambda item: (_watermark(item) or datetime.min.replace(tzinfo=timezone.utc))
            >= expected_watermark,
            "watermark/state cleanup",
            after_batch=_batch_id(progress),
        )
        evidence["watermark_advanced"] = _progress_evidence(progress)

        previous = _batch_id(progress)
        produce_json_events(config.bootstrap_servers, topic, [events["beyond_watermark"]])
        progress = _wait_for(
            query,
            lambda item: item.get("numInputRows", 0) >= 1 and _dropped(item) >= 1,
            "beyond-watermark dropped event",
            after_batch=previous,
        )
        evidence["beyond_watermark"] = _progress_evidence(progress)
        return evidence

    stream_info = run_stream(
        spark,
        demo_config,
        output_dir,
        run_id,
        duration_seconds=duration_seconds,
        checkpoint_id=checkpoint_id or f"late-data-{run_id}",
        on_query_started=orchestrate,
    )

    from pymongo import MongoClient

    with MongoClient(config.mongodb_uri, serverSelectionTimeoutMS=10000) as client:
        collection = client[config.mongo_database][config.mongo_collection]
        document = collection.find_one(
            {"window_end": selected_window_end.replace(tzinfo=None), "product_id": 4242}
        )
        duplicate_groups = list(
            collection.aggregate(
                [
                    {"$match": {"window_end": selected_window_end.replace(tzinfo=None)}},
                    {"$group": {"_id": "$product_id", "count": {"$sum": 1}}},
                    {"$match": {"count": {"$gt": 1}}},
                ]
            )
        )

    if document is None or document.get("updated_at") < started_at.replace(tzinfo=None):
        raise RuntimeError("Late-data demo did not write the selected finalized window")
    if document["purchase_count_30m"] != 2 or document["purchase_count_120m"] != 2:
        raise RuntimeError(f"Late-data counts are incorrect; expected 2/2: {document}")
    if duplicate_groups:
        raise RuntimeError(f"Duplicate sink keys found: {duplicate_groups}")

    evidence = {
        "topic_metadata": metadata,
        "events": events,
        "selected_window_end": selected_window_end,
        "selected_document": document,
        "duplicate_groups": duplicate_groups,
        "stream": stream_info,
        "conclusion": {
            "within_watermark_accepted": True,
            "beyond_watermark_dropped": True,
            "accepted_count": 2,
        },
    }
    evidence_path = output_dir / "late_data_evidence.json"
    evidence_path.write_text(
        json.dumps(evidence, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    evidence["evidence_path"] = str(evidence_path)
    return evidence
