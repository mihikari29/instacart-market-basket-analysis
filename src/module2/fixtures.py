"""Deterministic schema-compatible fixtures; never presented as Instacart results."""

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from src.partition_events import partition_events, file_sha256
from .config import TABLES


def create_fixture(root):
    root = Path(root)
    root.mkdir(parents=True, exist_ok=False)
    orders, prior, train, events = [], [], [], []
    products = [
        {
            "product_id": p,
            "product_name": f"Product {p}",
            "aisle_id": (p % 3) + 1,
            "department_id": (p % 2) + 1,
        }
        for p in range(1, 7)
    ]
    for user in range(1, 5):
        for sequence in range(1, 5):
            order = user * 10 + sequence
            evaluation = "prior" if sequence < 3 else ("train" if sequence == 3 else "test")
            orders.append(
                {
                    "order_id": order,
                    "user_id": user,
                    "eval_set": evaluation,
                    "order_number": sequence,
                    "order_dow": sequence % 7,
                    "order_hour_of_day": user + 8,
                    "days_since_prior_order": float(sequence - 1),
                }
            )
            if evaluation == "test":
                continue
            # Same dates across user row groups; sparse dates test calendar windows.
            date = ["2024-01-01", "2024-01-04", "2024-02-01"][sequence - 1]
            for cart in range(1, 4):
                product_id = (user + cart) % 6 + 1
                fact = {
                    "order_id": order,
                    "product_id": product_id,
                    "add_to_cart_order": cart,
                    "reordered": sequence > 1,
                }
                (prior if evaluation == "prior" else train).append(fact)
                stamp = (
                    int(datetime.fromisoformat(date).replace(tzinfo=timezone.utc).timestamp() * 1000)
                    + cart * 1000
                )
                events.append(
                    {
                        **fact,
                        "event_id": f"{order}_{cart}",
                        "user_id": user,
                        "aisle_id": (product_id % 3) + 1,
                        "department_id": (product_id % 2) + 1,
                        "order_dow": sequence % 7,
                        "order_hour_of_day": user + 8,
                        "event_time_epoch_ms": stamp,
                        "event_time_iso": date + "T00:00:01.000000",
                        "synthetic_date": date,
                    }
                )
    data = {
        "orders": orders,
        "prior": prior,
        "train": train,
        "products": products,
        "aisles": [{"aisle_id": a, "aisle": f"Aisle {a}"} for a in range(1, 4)],
        "departments": [{"department_id": d, "department": f"Department {d}"} for d in range(1, 3)],
    }
    sources = {}
    for name, records in data.items():
        directory = root / "curated" / TABLES[name]
        directory.mkdir(parents=True)
        path = directory / "part.parquet"
        pq.write_table(pa.Table.from_pylist(records), path)
        sources[TABLES[name].split("/")[-1]] = {"rows": len(records), "sha256": file_sha256(path)}
    (root / "curated/_sources.json").write_text(json.dumps(sources), encoding="utf-8")
    feed = root / "source"
    feed.mkdir()
    pq.write_table(pa.Table.from_pylist(events), feed / "events.parquet", row_group_size=9)
    (feed / "manifest.json").write_text(
        json.dumps({"events": len(events), "config": {"limit_users": None}, "fixture": True}),
        encoding="utf-8",
    )
    partition_events(feed / "events.parquet", root / "curated/interactions")
    return root


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default="data/fixture")
    args = parser.parse_args()
    print(create_fixture(args.root).resolve())
