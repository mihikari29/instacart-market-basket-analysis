"""Publish deterministic event sets used by live recovery validation."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
import time

from .kafka_utils import create_topic, produce_json_events, topic_metadata


def _event(event_id: str, product_id: int, hour: int, minute: int) -> dict:
    event_time = datetime(2031, 1, 1, hour, minute, tzinfo=timezone.utc)
    epoch_ms = int(event_time.timestamp() * 1000)
    return {
        "event_id": event_id,
        "order_id": epoch_ms + product_id,
        "user_id": 910001,
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


SCENARIOS = {
    "recovery-first": [
        _event("recovery-first-a", 5101, 0, 20),
        _event("recovery-first-b", 5102, 0, 22),
        _event("recovery-first-advance", 5199, 3, 0),
    ],
    "recovery-second": [
        _event("recovery-second-a", 5101, 3, 5),
        _event("recovery-second-advance", 5199, 6, 0),
    ],
}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario", choices=sorted(SCENARIOS))
    parser.add_argument("--topic", required=True)
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
    )
    parser.add_argument("--partitions", type=int, default=2)
    parser.add_argument("--reset-topic", action="store_true")
    args = parser.parse_args(argv)

    before = create_topic(
        args.bootstrap_servers,
        args.topic,
        partitions=args.partitions,
        reset=args.reset_topic,
    )
    sent = produce_json_events(args.bootstrap_servers, args.topic, SCENARIOS[args.scenario])
    after = topic_metadata(args.bootstrap_servers, args.topic)
    print(
        json.dumps(
            {
                "scenario": args.scenario,
                "sent": sent,
                "before": before,
                "after": after,
            },
            sort_keys=True,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
