"""Completion contracts for a finite producer feeding a running Kafka query."""

from __future__ import annotations

import json
import math
from pathlib import Path
import time


FULL_REPLAY_SECONDS = 64_800
FIXTURE_REPLAY_SECONDS = 1_200


def wait_for_replay(query, completion, seconds, *, clock=time.monotonic, sleep=time.sleep):
    """Keep the consumer alive until reconciliation or a hard deadline."""
    if seconds <= 0:
        raise TimeoutError("stream replay deadline has expired")
    deadline = clock() + seconds
    while True:
        if not query.isActive:
            raise RuntimeError("stream stopped before replay completion") from query.exception()
        if clock() >= deadline:
            raise TimeoutError("stream replay did not drain before its hard deadline")
        result = completion(query)
        if result:
            return result
        sleep(min(0.5, max(0.0, deadline - clock())))


def validate_replay_budget(events: int, rate: float, seconds: int) -> None:
    if type(events) is not int or events <= 0:
        raise ValueError("replay requires a positive expected event count")
    if isinstance(rate, bool) or not isinstance(rate, (int, float)) or not math.isfinite(rate) or rate <= 0:
        raise ValueError("replay rate must be finite and positive")
    if type(seconds) is not int or not 1 <= seconds <= FULL_REPLAY_SECONDS:
        raise ValueError("replay deadline is outside its reviewed bound")
    if math.ceil(events / rate) + 60 > seconds:
        raise ValueError("replay pacing cannot finish within the deadline and drain reserve")


def kafka_end_offsets(bootstrap: str, topic: str) -> dict[str, int]:
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(bootstrap_servers=bootstrap, enable_auto_commit=False, request_timeout_ms=10000)
    try:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            raise RuntimeError("replay topic partition metadata is unavailable")
        values = consumer.end_offsets([TopicPartition(topic, p) for p in sorted(partitions)])
        return {str(key.partition): value for key, value in values.items()}
    finally:
        consumer.close()


def validate_replay_completion(receipt: dict) -> None:
    """Check saved completion evidence without contacting any service."""
    expected = receipt.get("expected_events")
    if type(expected) is not int or expected <= 0:
        raise ValueError("replay completion has no expected event count")
    for key in ("attempted_events", "acked_events", "consumed_records"):
        if type(receipt.get(key)) is not int or receipt[key] != expected:
            raise ValueError("replay delivery and consumption counts do not reconcile")
    if receipt.get("failed_events") != 0:
        raise ValueError("replay contains failed deliveries")
    start, end, consumed = (receipt.get(key) for key in ("start_offsets", "terminal_offsets", "consumed_offsets"))
    if not all(isinstance(value, dict) and value for value in (start, end, consumed)):
        raise ValueError("replay completion is missing offset evidence")
    if set(start) != set(end) or set(end) != set(consumed):
        raise ValueError("replay partition inventory changed")
    if any(type(value) is not int or value < 0 for values in (start, end, consumed) for value in values.values()):
        raise ValueError("replay offsets must be nonnegative integers")
    if any(start[key] != 0 or end[key] != consumed[key] for key in end):
        raise ValueError("replay must consume the complete fresh topic through terminal offsets")
    if sum(end.values()) != expected:
        raise ValueError("replay Kafka offsets do not reconcile with source events")


class ReplayCompletion:
    """Poll a producer and only complete after a committed query batch drains it."""

    def __init__(self, *, expected_events, topic, start_offsets, producer, receipt_path, end_offsets):
        self.expected = expected_events
        self.topic = topic
        self.start = start_offsets
        self.producer = producer
        self.receipt_path = Path(receipt_path)
        self.end_offsets = end_offsets
        self.delivery = None
        self.terminal = None

    def __call__(self, query):
        process = self.producer()
        if process is None:
            raise RuntimeError("replay producer was not started")
        code = process.poll()
        if code is None:
            return None
        if code != 0:
            raise RuntimeError("replay producer failed")
        if self.delivery is None:
            self.delivery = json.loads(self.receipt_path.read_text(encoding="utf-8"))
            if self.delivery.get("lifecycle_errors"):
                raise RuntimeError("replay producer did not complete cleanly")
            self.terminal = self.end_offsets()
        progress = query.lastProgress or {}
        consumed = None
        for source in progress.get("sources", []):
            raw = source.get("endOffset")
            offsets = json.loads(raw) if isinstance(raw, str) else raw
            if isinstance(offsets, dict) and self.topic in offsets:
                consumed = offsets[self.topic]
        if not isinstance(consumed, dict) or any(consumed.get(key, -1) < value for key, value in self.terminal.items()):
            return None
        receipt = {
            "expected_events": self.expected,
            **{key: self.delivery.get(key) for key in ("attempted_events", "acked_events", "failed_events")},
            "start_offsets": self.start,
            "terminal_offsets": self.terminal,
            "consumed_offsets": consumed,
            "consumed_records": sum(consumed.values()) - sum(self.start.values()),
            "window_state": "retained in checkpoint; event-time tail may remain unfinalized",
        }
        validate_replay_completion(receipt)
        return receipt
