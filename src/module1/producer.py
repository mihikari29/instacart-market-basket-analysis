"""Replay synthesized events to Kafka in chronological order with bounded RAM.

Unsorted Parquet feeds use a fixed-memory, disk-backed external merge. Kafka
delivery is successful only after broker acknowledgement (``acks=all``); the
receipt distinguishes attempted, acknowledged, and failed events.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
import json
from pathlib import Path
from threading import Lock
import time
import warnings

import numpy as np
import pandas as pd
from kafka import KafkaProducer

from .event_reader import iter_ordered_events, manifest_declares_sorted

warnings.filterwarnings("ignore", message=".*serializer does not implement.*")

try:
    from kafka.errors import NoBrokersAvailable
except ImportError:
    NoBrokersAvailable = Exception

DEFAULT_TOPIC = "instacart-purchase-events"
KAFKA_RETRIES = 5


@dataclass
class DeliveryAccounting:
    """Thread-safe broker-delivery counters populated by Kafka callbacks."""

    attempted_events: int = 0
    acked_events: int = 0
    failed_events: int = 0
    failure_samples: list[str] = field(default_factory=list)
    _lock: Lock = field(default_factory=Lock, repr=False)

    def attempt(self) -> None:
        with self._lock:
            self.attempted_events += 1

    def ack(self, *_args) -> None:
        with self._lock:
            self.acked_events += 1

    def fail(self, error=None) -> None:
        with self._lock:
            self.failed_events += 1
            if error is not None and len(self.failure_samples) < 10:
                self.failure_samples.append(f"{type(error).__name__}: {error}")

    def reconcile_unconfirmed(self, reason: str) -> None:
        """Account for futures left unresolved after flush/close failure."""
        with self._lock:
            missing = self.attempted_events - self.acked_events - self.failed_events
            if missing > 0:
                self.failed_events += missing
                if len(self.failure_samples) < 10:
                    self.failure_samples.append(f"UnconfirmedDelivery: {reason} ({missing} events)")


def build_producer(bootstrap_servers: str) -> KafkaProducer:
    try:
        return KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda val: json.dumps(val).encode("utf-8"),
            key_serializer=lambda key: str(key).encode("utf-8"),
            acks="all",
            retries=KAFKA_RETRIES,
            linger_ms=5,
            batch_size=262_144,
        )
    except NoBrokersAvailable as exc:
        raise RuntimeError(
            f"Cannot reach Kafka at {bootstrap_servers} (is docker compose up?)"
        ) from exc


def parse_injections(injection_spec: str | None) -> dict[str, float]:
    injections: dict[str, float] = {}
    if not injection_spec:
        return injections
    for item in injection_spec.split(","):
        key, _, value = item.partition(":")
        injections[key.strip()] = float(value or 1.0)
    return injections


def format_iso(epoch_ms: int) -> str:
    return pd.to_datetime(epoch_ms, unit="ms", utc=True).strftime("%Y-%m-%dT%H:%M:%S.%f")


def build_message(row: dict, event_time_ms: int, iso_string: str | None = None) -> dict:
    return {
        "event_id": str(row["event_id"]),
        "order_id": int(row["order_id"]),
        "user_id": int(row["user_id"]),
        "product_id": int(row["product_id"]),
        "add_to_cart_order": int(row["add_to_cart_order"]),
        "reordered": bool(row["reordered"]),
        "aisle_id": int(row["aisle_id"]),
        "department_id": int(row["department_id"]),
        "order_dow": int(row["order_dow"]),
        "order_hour_of_day": int(row["order_hour_of_day"]),
        "event_time_epoch_ms": event_time_ms,
        "event_time_iso": iso_string if iso_string is not None else str(row["event_time_iso"]),
        "ingestion_time_epoch_ms": int(time.time() * 1000),
    }


def send_tracked(producer, topic: str, message: dict, accounting: DeliveryAccounting) -> None:
    """Send once and attach acknowledgement/failure callbacks."""
    accounting.attempt()
    try:
        future = producer.send(topic, key=message["user_id"], value=message)
        future.add_callback(accounting.ack)
        future.add_errback(accounting.fail)
    except Exception as exc:
        accounting.fail(exc)


def publish_rows(
    producer,
    rows,
    *,
    topic: str,
    replay_speed: float,
    target_events_per_second: float | None,
    start_delay_seconds: float,
    injections: dict[str, float],
) -> tuple[dict, list[int]]:
    """Publish an ordered iterator and return delivery metrics plus latency samples."""
    probability_late = injections.get("late", 0.0)
    probability_duplicate = injections.get("dup", 0.0)
    burst_count = int(injections.get("burst", 0) or 0)
    poison_remaining = int(injections.get("poison", 0) or 0)
    rng = np.random.default_rng(1234)
    accounting = DeliveryAccounting()
    latencies: list[int] = []
    first_event_ms = None
    lifecycle_errors: list[str] = []

    if start_delay_seconds:
        time.sleep(start_delay_seconds)
    replay_start = time.monotonic()

    for index, row in enumerate(rows):
        event_time_ms = int(row["event_time_epoch_ms"])
        if first_event_ms is None:
            first_event_ms = event_time_ms
        if target_events_per_second is not None and target_events_per_second > 0:
            target_time = replay_start + accounting.attempted_events / target_events_per_second
            sleep_duration = target_time - time.monotonic()
            if sleep_duration > 0:
                time.sleep(sleep_duration)
        elif target_events_per_second is None and replay_speed > 0:
            target_time = replay_start + max(0, event_time_ms - first_event_ms) / (
                replay_speed * 1000.0
            )
            sleep_duration = target_time - time.monotonic()
            if sleep_duration > 0:
                time.sleep(sleep_duration)

        message = build_message(row, event_time_ms)
        if poison_remaining > 0:
            message["product_id"] = "MALFORMED"
            poison_remaining -= 1
        send_tracked(producer, topic, message, accounting)
        if accounting.attempted_events % 100 == 0:
            latencies.append(message["ingestion_time_epoch_ms"] - event_time_ms)
        if accounting.attempted_events % 50_000 == 0:
            rate = accounting.attempted_events / max(1e-9, time.monotonic() - replay_start)
            print(f"  attempted={accounting.attempted_events:,} rate={rate:,.0f}/s", flush=True)

        if burst_count and index % 500 == 0:
            for _ in range(burst_count):
                send_tracked(producer, topic, build_message(row, event_time_ms), accounting)
        if probability_duplicate and rng.random() < probability_duplicate:
            send_tracked(producer, topic, build_message(row, event_time_ms), accounting)
        if probability_late and rng.random() < probability_late:
            late_delta_ms = int(rng.uniform(4 * 60, 25 * 60) * 1000)
            late_epoch_ms = max(0, event_time_ms - late_delta_ms)
            late_message = build_message(row, late_epoch_ms, format_iso(late_epoch_ms))
            send_tracked(producer, topic, late_message, accounting)

    try:
        producer.flush(timeout=30)
    except Exception as exc:
        lifecycle_errors.append(f"flush: {type(exc).__name__}: {exc}")
    try:
        producer.close(timeout=10)
    except Exception as exc:
        lifecycle_errors.append(f"close: {type(exc).__name__}: {exc}")

    if lifecycle_errors:
        accounting.reconcile_unconfirmed("; ".join(lifecycle_errors))
    else:
        accounting.reconcile_unconfirmed("callback not completed after clean close")
    elapsed = time.monotonic() - replay_start
    receipt = {
        "attempted_events": accounting.attempted_events,
        "acked_events": accounting.acked_events,
        "failed_events": accounting.failed_events,
        "failure_samples": accounting.failure_samples,
        "lifecycle_errors": lifecycle_errors,
        "elapsed_seconds": elapsed,
        "achieved_events_per_second": accounting.acked_events / max(1e-9, elapsed),
        "configured_retries": KAFKA_RETRIES,
    }
    return receipt, latencies


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--feed", default=str(Path("data/synthesized/scatter_3m")))
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--replay-speed", type=float, default=1.0)
    parser.add_argument("--target-events-per-second", type=float, default=None)
    parser.add_argument("--limit-events", type=int, default=None)
    parser.add_argument("--start-delay-seconds", type=float, default=0.0)
    parser.add_argument("--receipt", type=Path, default=None)
    parser.add_argument("--inject", default=None)
    parser.add_argument(
        "--input-order",
        choices=("auto", "sorted", "unsorted"),
        default="auto",
        help="Use 'sorted' only when the feed is ordered by the documented event key",
    )
    parser.add_argument("--sort-chunk-rows", type=int, default=65_536)
    parser.add_argument("--sort-merge-fan-in", type=int, default=32)
    args = parser.parse_args(argv)
    if args.target_events_per_second is not None and args.target_events_per_second < 0:
        parser.error("--target-events-per-second must be non-negative")
    if args.start_delay_seconds < 0:
        parser.error("--start-delay-seconds must be non-negative")
    if args.limit_events is not None and args.limit_events < 1:
        parser.error("--limit-events must be positive")

    feed_path = Path(args.feed)
    event_path = feed_path if feed_path.name == "events.parquet" else feed_path / "events.parquet"
    declared_sorted = manifest_declares_sorted(event_path)
    input_sorted = args.input_order == "sorted" or (
        args.input_order == "auto" and declared_sorted
    )
    reader_stats: dict = {}
    rows = iter_ordered_events(
        event_path,
        limit=args.limit_events,
        input_sorted=input_sorted,
        chunk_rows=args.sort_chunk_rows,
        merge_fan_in=args.sort_merge_fan_in,
        stats=reader_stats,
    )
    injections = parse_injections(args.inject)

    try:
        producer = build_producer(args.bootstrap)
        delivery, latencies = publish_rows(
            producer,
            rows,
            topic=args.topic,
            replay_speed=args.replay_speed,
            target_events_per_second=args.target_events_per_second,
            start_delay_seconds=args.start_delay_seconds,
            injections=injections,
        )
    except Exception as exc:
        print(f"[error] {type(exc).__name__}: {exc}", flush=True)
        return 1

    receipt = {
        "topic": args.topic,
        "feed": str(event_path),
        "requested_events": args.limit_events,
        **delivery,
        "target_events_per_second": args.target_events_per_second,
        "replay_speed_multiplier": args.replay_speed,
        "injections": injections,
        "reader": reader_stats,
    }
    print(
        f"[done ] topic={args.topic} attempted={receipt['attempted_events']:,} "
        f"acked={receipt['acked_events']:,} failed={receipt['failed_events']:,} "
        f"elapsed={receipt['elapsed_seconds']:.1f}s "
        f"acked_rate={receipt['achieved_events_per_second']:,.0f}/s",
        flush=True,
    )
    if latencies:
        p50, p99 = np.percentile(np.asarray(latencies, dtype="int64"), [50, 99])
        print(f"  ingestion - event_time (ms): p50={p50:.0f} p99={p99:.0f}", flush=True)
    if args.receipt is not None:
        args.receipt.parent.mkdir(parents=True, exist_ok=True)
        args.receipt.write_text(
            json.dumps(receipt, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    return 1 if receipt["failed_events"] or receipt["lifecycle_errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
