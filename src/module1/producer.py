"""Module 1 - Kafka Producer: Replay synthesized purchase events onto a Kafka topic.

Emits one JSON message per row of the chosen feed in chronological order, keyed
by user_id (preserving per-user event ordering across partitions). Pacing is driven
by event_time_epoch_ms vs --replay-speed; each message receives an ingestion_time_epoch_ms
timestamp at send time.

Schema (13 fields):
  event_id, order_id, user_id, product_id, add_to_cart_order, reordered,
  aisle_id, department_id, order_dow, order_hour_of_day,
  event_time_epoch_ms, event_time_iso, ingestion_time_epoch_ms

Usage:
  python -m src.module1.producer --feed data/synthesized/scatter_3m --replay-speed 100000
  python -m src.module1.producer --limit-events 50000 --replay-speed 0     # Bulk ingestion
  python -m src.module1.producer --inject "late:0.05,dup:0.02,burst:200,poison:1"
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from kafka import KafkaProducer

warnings.filterwarnings("ignore", message=".*serializer does not implement.*")

try:
    from kafka.errors import NoBrokersAvailable
except ImportError:
    NoBrokersAvailable = Exception

SCHEMA_FIELDS = [
    "event_id",
    "order_id",
    "user_id",
    "product_id",
    "add_to_cart_order",
    "reordered",
    "aisle_id",
    "department_id",
    "order_dow",
    "order_hour_of_day",
    "event_time_epoch_ms",
    "event_time_iso",
]

DEFAULT_TOPIC = "instacart-purchase-events"


def load_sorted(path: Path, limit: int | None) -> dict[str, np.ndarray]:
    """Load an event feed sorted chronologically by event_time_epoch_ms."""
    table = pq.read_table(path, columns=SCHEMA_FIELDS)
    table = table.sort_by([("event_time_epoch_ms", "ascending")])
    if limit:
        table = table.slice(0, limit)
    return {col_name: table[col_name].to_numpy(zero_copy_only=False) for col_name in SCHEMA_FIELDS}


def build_producer(bootstrap_servers: str) -> KafkaProducer:
    try:
        return KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda val: json.dumps(val).encode("utf-8"),
            key_serializer=lambda key: str(key).encode("utf-8"),
            linger_ms=5,
            batch_size=262144,
        )
    except NoBrokersAvailable as exc:
        raise SystemExit(f"Cannot reach Kafka at {bootstrap_servers} (is docker compose up?)") from exc


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


def build_message(columns: dict[str, np.ndarray], index: int, event_time_ms: int, iso_string: str | None = None) -> dict:
    return {
        "event_id": str(columns["event_id"][index]),
        "order_id": int(columns["order_id"][index]),
        "user_id": int(columns["user_id"][index]),
        "product_id": int(columns["product_id"][index]),
        "add_to_cart_order": int(columns["add_to_cart_order"][index]),
        "reordered": bool(columns["reordered"][index]),
        "aisle_id": int(columns["aisle_id"][index]),
        "department_id": int(columns["department_id"][index]),
        "order_dow": int(columns["order_dow"][index]),
        "order_hour_of_day": int(columns["order_hour_of_day"][index]),
        "event_time_epoch_ms": event_time_ms,
        "event_time_iso": iso_string if iso_string is not None else str(columns["event_time_iso"][index]),
        "ingestion_time_epoch_ms": int(time.time() * 1000),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--feed", default=str(Path("data/synthesized/scatter_3m")))
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--replay-speed", type=float, default=1.0,
                        help="Wall-time multiplier (0 = bulk streaming without pacing)")
    parser.add_argument("--limit-events", type=int, default=None)
    parser.add_argument("--inject", default=None,
                        help="Fault injection spec: late:<p>,dup:<p>,burst:<n>,poison:<n>")
    args = parser.parse_args()

    feed_path = Path(args.feed)
    event_path = feed_path if feed_path.name == "events.parquet" else feed_path / "events.parquet"
    load_start = time.perf_counter()
    columns = load_sorted(event_path, args.limit_events)
    total_events = len(columns["event_id"])
    print(f"[load ] {event_path.name}: {total_events:,} rows in {time.perf_counter() - load_start:.1f}s", flush=True)

    injections = parse_injections(args.inject)
    prob_late = injections.get("late", 0.0)
    prob_dup = injections.get("dup", 0.0)
    burst_count = int(injections.get("burst", 0) or 0)
    poison_count = int(injections.get("poison", 0) or 0)

    rng = np.random.default_rng(1234)
    producer = build_producer(args.bootstrap)
    first_event_ms = columns["event_time_epoch_ms"][0]
    replay_start = time.monotonic()
    sent_count = 0
    last_report_count = 0
    latencies: list[int] = []

    for idx in range(total_events):
        event_time_ms = int(columns["event_time_epoch_ms"][idx])
        if args.replay_speed > 0:
            target_time = replay_start + max(0, event_time_ms - first_event_ms) / (args.replay_speed * 1000.0)
            sleep_duration = target_time - time.monotonic()
            if sleep_duration > 0:
                time.sleep(sleep_duration)

        message = build_message(columns, idx, event_time_ms)
        if poison_count and (sent_count % max(1, total_events // poison_count) == 0):
            message["product_id"] = "MALFORMED"

        producer.send(args.topic, key=message["user_id"], value=message)
        sent_count += 1
        if sent_count % 100 == 0:
            latencies.append(message["ingestion_time_epoch_ms"] - event_time_ms)

        if sent_count - last_report_count >= 50000:
            last_report_count = sent_count
            current_rate = sent_count / max(1e-9, time.monotonic() - replay_start)
            print(f"  sent={sent_count:,} rate={current_rate:,.0f}/s", flush=True)

        # Fault Injections
        if burst_count and idx % 500 == 0:
            for _ in range(burst_count):
                burst_msg = build_message(columns, idx, event_time_ms)
                producer.send(args.topic, key=burst_msg["user_id"], value=burst_msg)
                sent_count += 1

        if prob_dup and rng.random() < prob_dup:
            dup_msg = build_message(columns, idx, event_time_ms)
            producer.send(args.topic, key=dup_msg["user_id"], value=dup_msg)
            sent_count += 1

        if prob_late and rng.random() < prob_late:
            late_delta_ms = int(rng.uniform(4 * 60, 25 * 60) * 1000)
            late_epoch_ms = max(0, event_time_ms - late_delta_ms)
            late_msg = build_message(columns, idx, late_epoch_ms, format_iso(late_epoch_ms))
            producer.send(args.topic, key=late_msg["user_id"], value=late_msg)
            sent_count += 1

    producer.flush()
    try:
        producer.close(timeout=10)
    except Exception:
        pass

    elapsed = time.monotonic() - replay_start
    print(f"[done ] topic={args.topic} key=user_id sent={sent_count:,} "
          f"elapsed={elapsed:.1f}s rate={sent_count / max(1e-9, elapsed):,.0f}/s", flush=True)
    if latencies:
        latencies_arr = np.asarray(latencies, dtype="int64")
        p50, p99 = np.percentile(latencies_arr, [50, 99])
        print(f"  ingestion - event_time (ms): p50={p50:.0f} p99={p99:.0f}", flush=True)
    print("[exit ] hard-exit (kafka-python cleanup)", flush=True)
    sys.stdout.flush()
    os._exit(0)


if __name__ == "__main__":
    raise SystemExit(main())