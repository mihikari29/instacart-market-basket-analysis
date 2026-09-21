"""Module 1 - Kafka producer: replay events.parquet onto a Kafka topic.

Emits one JSON message per row of the chosen feed, in chronological order, keyed
by user_id (keeps every user's events ordered across the topic). Pacing is driven
by event_time_epoch_ms vs --replay-speed; each message gets an
ingestion_time_epoch_ms stamp at send time (per Proposal 9.4).

Schema (13 fields, Proposal 9.4 minus event_type):
  event_id, order_id, user_id, product_id, add_to_cart_order, reordered,
  aisle_id, department_id, order_dow, order_hour_of_day,
  event_time_epoch_ms, event_time_iso, ingestion_time_epoch_ms

Usage:
  python src/producer.py --feed data/synthesized/scatter_3m --replay-speed 100000
  python src/producer.py --limit-events 50000 --replay-speed 0     # bulk, no pacer
  python src/producer.py --inject "late:0.05,dup:0.02,burst:200,poison:1"
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
except ImportError:  # kafka-python 3.x renamed the connect-time exception
    NoBrokersAvailable = Exception

SCHEMA_FLDS = [
    "event_id", "order_id", "user_id", "product_id", "add_to_cart_order",
    "reordered", "aisle_id", "department_id", "order_dow", "order_hour_of_day",
    "event_time_epoch_ms", "event_time_iso",
]

TOPIC = "instacart-purchase-events"


def load_sorted(path: Path, limit: int | None) -> dict:
    """Load a feed (drop HDFS-only cols), sorted chronologically."""
    t = pq.read_table(path, columns=SCHEMA_FLDS)
    t = t.sort_by([("event_time_epoch_ms", "ascending")])
    if limit:
        t = t.slice(0, limit)
    return {c: t[c].to_numpy(zero_copy_only=False) for c in SCHEMA_FLDS}


def build_producer(bootstrap: str) -> KafkaProducer:
    try:
        return KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: str(k).encode("utf-8"),
            linger_ms=5,
            batch_size=262144,
        )
    except NoBrokersAvailable as exc:
        raise SystemExit(f"cannot reach Kafka at {bootstrap} (docker compose up -d?)") from exc


def parse_inject(spec: str | None) -> dict:
    out = {}
    if not spec:
        return out
    for item in spec.split(","):
        key, _, val = item.partition(":")
        out[key.strip()] = float(val or 1.0)
    return out


def format_iso(epoch_ms: int) -> str:
    return pd.to_datetime(epoch_ms, unit="ms", utc=True).strftime("%Y-%m-%dT%H:%M:%S.%f")


def build_message(col: dict, i: int, et: int, iso: str | None = None) -> dict:
    return {
        "event_id": str(col["event_id"][i]),
        "order_id": int(col["order_id"][i]),
        "user_id": int(col["user_id"][i]),
        "product_id": int(col["product_id"][i]),
        "add_to_cart_order": int(col["add_to_cart_order"][i]),
        "reordered": bool(col["reordered"][i]),
        "aisle_id": int(col["aisle_id"][i]),
        "department_id": int(col["department_id"][i]),
        "order_dow": int(col["order_dow"][i]),
        "order_hour_of_day": int(col["order_hour_of_day"][i]),
        "event_time_epoch_ms": et,
        "event_time_iso": iso if iso is not None else str(col["event_time_iso"][i]),
        "ingestion_time_epoch_ms": int(time.time() * 1000),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--feed", default=str(Path("data/synthesized/scatter_3m")))
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default=TOPIC)
    ap.add_argument("--replay-speed", type=float, default=1.0,
                    help="wall-time multiplier (0 = no pacing/bulk ingest)")
    ap.add_argument("--limit-events", type=int, default=None)
    ap.add_argument("--inject", default=None,
                    help="comma list: late:<p>,dup:<p>,burst:<n>,poison:<n>")
    args = ap.parse_args()

    feed = Path(args.feed)
    evpath = feed if feed.name == "events.parquet" else feed / "events.parquet"
    t0 = time.perf_counter()
    col = load_sorted(evpath, args.limit_events)
    n = len(col["event_id"])
    print(f"[load ] {evpath.name}: {n:,} rows in {time.perf_counter() - t0:.1f}s", flush=True)

    inj = parse_inject(args.inject)
    p_late = inj.get("late", 0.0)
    p_dup = inj.get("dup", 0.0)
    bursts = int(inj.get("burst", 0) or 0)
    poisons = int(inj.get("poison", 0) or 0)

    rng = np.random.default_rng(1234)
    prod = build_producer(args.bootstrap)
    src_min = col["event_time_epoch_ms"][0]
    start = time.monotonic()
    sent = 0
    last_report = 0
    lat: list[int] = []
    for i in range(n):
        et = int(col["event_time_epoch_ms"][i])
        if args.replay_speed > 0:
            target = start + max(0, et - src_min) / (args.replay_speed * 1000.0)
            sleep = target - time.monotonic()
            if sleep > 0:
                time.sleep(sleep)

        # Base message
        m = build_message(col, i, et)
        if poisons and (sent % max(1, n // poisons) == 0):
            m["product_id"] = "MALFORMED"

        prod.send(args.topic, key=m["user_id"], value=m)
        sent += 1
        if sent % 100 == 0:
            lat.append(m["ingestion_time_epoch_ms"] - et)

        if sent - last_report >= 50000:
            last_report = sent
            rate = sent / max(1e-9, time.monotonic() - start)
            print(f"  sent={sent:,} rate={rate:,.0f}/s", flush=True)

        # Fault injections
        if bursts and i % 500 == 0:
            for _ in range(bursts):
                mb = build_message(col, i, et)
                prod.send(args.topic, key=mb["user_id"], value=mb)
                sent += 1

        if p_dup and rng.random() < p_dup:
            m_dup = build_message(col, i, et)
            prod.send(args.topic, key=m_dup["user_id"], value=m_dup)
            sent += 1

        if p_late and rng.random() < p_late:
            late_ms = int(rng.uniform(4 * 60, 25 * 60) * 1000)
            et_late = max(0, et - late_ms)
            m_late = build_message(col, i, et_late, format_iso(et_late))
            prod.send(args.topic, key=m_late["user_id"], value=m_late)
            sent += 1

    prod.flush()
    try:
        prod.close(timeout=10)
    except Exception:
        pass

    elapsed = time.monotonic() - start
    print(f"[done ] topic={args.topic} key=user_id sent={sent:,} "
          f"elapsed={elapsed:.1f}s rate={sent / max(1e-9, elapsed):,.0f}/s", flush=True)
    if lat:
        a = np.asarray(lat, dtype="int64")
        p50, p99 = np.percentile(a, [50, 99])
        print(f"  ingestion - event_time (ms): p50={p50:.0f} p99={p99:.0f}", flush=True)
    print("[exit ] hard-exit (kafka-python leaves a non-daemon thread; force close)", flush=True)
    sys.stdout.flush()
    os._exit(0)


if __name__ == "__main__":
    raise SystemExit(main())