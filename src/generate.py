"""Module 1 - step 2: synthesize absolute timestamps per docs/generate.md.

Reads cleaned data (data/clean), applies the 3-step pipeline (absolute day,
cap-30 recovery/cumulative days, session seconds) controlled by SyntheticConfig,
and writes one events file (Kafka JSON event schema + a synthetic_date column).

Usage:
  python src/generate.py --scenario default --scatter-weeks 13
  python src/generate.py --scenario default --scatter-weeks 13 --out-dir data/synthesized/scatter_3m
  python src/generate.py --scenario deterministic --limit-users 5000 --format jsonl
  python src/generate.py --list-scenarios
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from config import SyntheticConfig, preset_summary

BASE = pd.Timestamp("2024-01-01")  # Monday; order_dow 0..6 aligns with +dow days
DAY_MS = 86_400_000
BASE_EPOCH_MS = int(BASE.value // 10**6)

SCHEMA_ORDER = [
    "event_id", "order_id", "user_id", "product_id", "add_to_cart_order",
    "reordered", "aisle_id", "department_id", "order_hour_of_day", "order_dow",
    "event_time_epoch_ms", "event_time_iso",
]


def load_clean(clean_dir: Path):
    return tuple(
        pd.read_parquet(clean_dir / name)
        for name in ("orders.parquet", "order_products__prior.parquet",
                     "order_products__train.parquet", "products.parquet")
    )


def order_minutes(rng, order_ids: pd.Series, mode: str) -> tuple["np.ndarray", "np.ndarray"]:
    if mode == "hash":
        minute = (order_ids * 10**6 + 1) % 60
        return minute.to_numpy(), np.zeros(len(order_ids), dtype="int64")
    n = len(order_ids)
    return rng.integers(0, 60, size=n), rng.integers(0, 60, size=n)


def prepare_orders(orders: pd.DataFrame, conf: SyntheticConfig, rng) -> pd.DataFrame:
    """Per-order absolute base moment: synthetic date + hour + minute + second."""
    stream = orders[orders["eval_set"].isin(["prior", "train"])].copy()
    if conf.limit_users:
        keep = np.sort(stream["user_id"].unique())[: conf.limit_users]
        stream = stream[stream["user_id"].isin(keep)]
    stream = stream.sort_values(["user_id", "order_number"]).reset_index(drop=True)

    fdow = stream.groupby("user_id", sort=True)["order_dow"].first()
    anchor_days = fdow + rng.integers(0, conf.scatter_window_weeks, size=len(fdow)) * 7

    stream["day_offset"] = (
        stream["user_id"].map(anchor_days)
        + stream.groupby("user_id")["days_since_prior_order"].cumsum().astype("int64")
    )
    minute, second = order_minutes(rng, stream["order_id"], conf.minute_mode)
    stream["base_ms"] = (
        BASE_EPOCH_MS
        + stream["day_offset"] * DAY_MS
        + stream["order_hour_of_day"] * 3_600_000
        + minute * 60_000
        + second * 1_000
    ).astype("int64")
    return stream.drop(columns=["day_offset"])


def enforce_monotonicity(ev: pd.DataFrame) -> pd.DataFrame:
    """Guarantee event_time strictly increases per user, in order sequence.

    Later orders on the same (day, hour) could otherwise get an earlier time.
    All items of an affected order are shifted forward by a constant so the
    within-session order is preserved.
    """
    orders = ev.groupby("order_id", sort=False).agg(
        user=("user_id", "first"),
        onum=("order_number", "first"),
        first=("event_ms", "min"),
        last=("event_ms", "max"),
    ).sort_values(["user", "onum"])

    users = orders["user"].to_numpy()
    first = orders["first"].to_numpy()
    last = orders["last"].to_numpy()
    shift_ms = np.zeros(len(orders), dtype="int64")

    prev_user, prev_last = None, 0
    for i in range(len(orders)):
        if users[i] != prev_user:
            prev_user, prev_last = users[i], 0
        needed = prev_last + 1000 - first[i]
        if needed > 0:
            shift_ms[i] = needed
        prev_last = max(prev_last, last[i] + shift_ms[i])

    shift_by_order = dict(zip(orders.index, shift_ms))
    ev = ev.copy()
    ev["event_ms"] += ev["order_id"].map(shift_by_order).fillna(0).astype("int64")
    return ev


def expand_events(
    stream: pd.DataFrame,
    ops: pd.DataFrame,
    products: pd.DataFrame,
    conf: SyntheticConfig,
    rng,
) -> pd.DataFrame:
    ev = (
        ops.merge(
            stream[["order_id", "user_id", "order_number", "order_hour_of_day",
                    "order_dow", "base_ms"]],
            on="order_id",
        )
        .merge(products[["product_id", "aisle_id", "department_id"]], on="product_id")
        .sort_values(["user_id", "order_number", "add_to_cart_order"])
        .reset_index(drop=True)
    )

    delta_s = conf.delta.sample(rng, len(ev))
    delta_s[ev["add_to_cart_order"].to_numpy() == 1] = 0.0
    ev["delta_s"] = delta_s

    elapsed = ev.groupby("order_id", sort=False)["delta_s"].cumsum()
    ev["event_ms"] = (ev["base_ms"] + (elapsed * 1000).round()).astype("int64")
    return enforce_monotonicity(ev.drop(columns=["delta_s", "base_ms"]))


def finalize(ev: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=ev.index)
    out["event_id"] = ev["order_id"].astype(str) + "_" + ev["add_to_cart_order"].astype(str)
    out["order_id"] = ev["order_id"].astype("int64")
    out["user_id"] = ev["user_id"].astype("int64")
    out["product_id"] = ev["product_id"].astype("int64")
    out["add_to_cart_order"] = ev["add_to_cart_order"].astype("int16")
    out["reordered"] = ev["reordered"].astype("bool")
    out["aisle_id"] = ev["aisle_id"].astype("int16")
    out["department_id"] = ev["department_id"].astype("int8")
    out["order_hour_of_day"] = ev["order_hour_of_day"].astype("int8")
    out["order_dow"] = ev["order_dow"].astype("int8")
    out["event_time_epoch_ms"] = ev["event_ms"].astype("int64")
    out["event_time_iso"] = pd.to_datetime(ev["event_ms"], unit="ms", utc=True).dt.strftime(
        "%Y-%m-%dT%H:%M:%S.%f"
    )
    out["synthetic_date"] = pd.to_datetime(ev["event_ms"], unit="ms", utc=True).dt.strftime(
        "%Y-%m-%d"
    )
    return out


def monotonic_violations(out: pd.DataFrame) -> int:
    s = out.sort_values(["user_id", "event_time_epoch_ms"])
    same_user = s["user_id"].to_numpy()[1:] == s["user_id"].to_numpy()[:-1]
    regressed = s["event_time_epoch_ms"].to_numpy()[1:] <= s["event_time_epoch_ms"].to_numpy()[:-1]
    return int((same_user & regressed).sum())


class EventWriter:
    """Writes finalized events, appending per user-batch. Tracks totals."""

    def __init__(self, out_dir: Path, fmt: str):
        out_dir.mkdir(parents=True, exist_ok=True)
        self.path = out_dir / ("events.parquet" if fmt == "parquet" else "events.json.gz")
        self.fmt = fmt
        self._writer = None
        self._gzip = None
        self.daily: dict[str, int] = {}
        self.events = self.orders = self.violations = 0

    def add(self, out: pd.DataFrame) -> None:
        self.events += len(out)
        self.orders += int(out["order_id"].nunique())
        self.violations += monotonic_violations(out)
        for date, count in out["synthetic_date"].value_counts().items():
            self.daily[str(date)] = self.daily.get(str(date), 0) + int(count)

        if self.fmt == "jsonl":
            if self._gzip is None:
                import gzip
                self._gzip = gzip.open(str(self.path), "wt", encoding="utf-8")
            self._gzip.write(out[SCHEMA_ORDER].to_json(orient="records", lines=True))
            self._gzip.write("\n")
            return

        import pyarrow as pa
        import pyarrow.parquet as pq
        if self._writer is None:
            self._writer = pq.ParquetWriter(self.path, pa.Table.from_pandas(out).schema)
        self._writer.write_table(pa.Table.from_pandas(out))

    def close(self) -> None:
        if self._writer is not None:
            self._writer.close()
        if self._gzip is not None:
            self._gzip.close()


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--data-dir", default=str(Path(__file__).resolve().parent.parent / "data" / "clean"))
    ap.add_argument("--out-dir", default=None)
    ap.add_argument("--scenario", default="default")
    ap.add_argument("--scatter-weeks", type=int, default=None, choices=(1, 4, 13, 26, 52))
    ap.add_argument("--minute-mode", choices=("uniform", "hash"), default=None)
    ap.add_argument("--limit-users", type=int, default=None)
    ap.add_argument("--batch-users", type=int, default=20000,
                    help="finalize+write in chunks of this many users to bound peak memory")
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--format", dest="fmt", default="parquet", choices=("parquet", "jsonl"))
    ap.add_argument("--list-scenarios", action="store_true")
    args = ap.parse_args()

    if args.list_scenarios:
        print(preset_summary())
        return 0

    conf = SyntheticConfig.from_scenario(
        args.scenario,
        seed=args.seed,
        scatter_window_weeks=args.scatter_weeks,
        minute_mode=args.minute_mode,
        limit_users=args.limit_users,
    )

    clean_dir = Path(args.data_dir)
    out_dir = Path(args.out_dir) if args.out_dir else clean_dir.parent / "synthesized"

    orders, prior, train, products = load_clean(clean_dir)
    ops = pd.concat([prior, train]).reset_index(drop=True)

    scope = orders[orders["eval_set"].isin(["prior", "train"])].copy()
    users = np.sort(scope["user_id"].to_numpy())
    user_ids = np.unique(users)
    if conf.limit_users:
        user_ids = user_ids[: conf.limit_users]

    writer = EventWriter(out_dir, args.fmt)
    print(f"[config] {json.dumps(conf.to_dict(), default=str)}", flush=True)
    n_batches = (len(user_ids) + args.batch_users - 1) // args.batch_users
    for b, start in enumerate(range(0, len(user_ids), args.batch_users)):
        brng = np.random.default_rng(np.random.SeedSequence([conf.seed, b]))
        ids = user_ids[start : start + args.batch_users]
        batch_stream = prepare_orders(scope[scope["user_id"].isin(ids)], conf, brng)
        batch_ops = ops.merge(batch_stream[["order_id"]], on="order_id")
        ev = expand_events(batch_stream, batch_ops, products, conf, brng)
        writer.add(finalize(ev))
        del ev, batch_stream, batch_ops
        print(f"  batch {b + 1}/{n_batches}: events so far={writer.events:,}", flush=True)
    writer.close()
    del orders, scope, ops

    daily_counts = pd.Series(writer.daily, dtype="int64").sort_index()
    stats = {
        "config": conf.to_dict(),
        "output": str(writer.path),
        "events": writer.events,
        "users": len(user_ids),
        "orders": writer.orders,
        "span": [str(daily_counts.index.min()), str(daily_counts.index.max())],
        "days": len(daily_counts),
        "peak_daily": int(daily_counts.max()),
        "mean_daily": round(float(daily_counts.mean()), 1),
        "monotonic_violations": writer.violations,
    }
    (out_dir / "manifest.json").write_text(json.dumps(stats, indent=2, default=str))

    print(f"[wrote ] {stats['output']}")
    print(f"  events={stats['events']:,} users={stats['users']:,} orders={stats['orders']:,}")
    print(f"  span={stats['span'][0]}..{stats['span'][1]} days={stats['days']} "
          f"peak/mean daily={stats['peak_daily']:,}/{stats['mean_daily']:,}")
    print(f"  monotonic violations={stats['monotonic_violations']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())