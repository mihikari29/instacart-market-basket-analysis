"""Module 1 - Step 2: Synthesize deterministic absolute timestamps.

Reads cleaned data (data/clean), applies the 3-step synthesis pipeline
(anchor date, minimum weekday-consistent synthetic gap reconstruction, session seconds),
enforces user monotonicity, and writes finalized event data.

Usage:
  python -m src.module1.generate --scenario default --scatter-weeks 13
  python -m src.module1.generate --scenario default --scatter-weeks 13 --output-dir data/synthesized/scatter_3m
  python -m src.module1.generate --scenario deterministic --limit-users 5000 --format jsonl
  python -m src.module1.generate --list-scenarios
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import numpy as np
import pandas as pd

from .config import SyntheticConfig, preset_summary

BASE_TIMESTAMP = pd.Timestamp("2024-01-01")  # Monday; order_dow 0..6 aligns with +dow days
DAY_MS = 86_400_000
BASE_EPOCH_MS = int(BASE_TIMESTAMP.value // 10**6)

SCHEMA_ORDER = [
    "event_id",
    "order_id",
    "user_id",
    "product_id",
    "add_to_cart_order",
    "reordered",
    "aisle_id",
    "department_id",
    "order_hour_of_day",
    "order_dow",
    "event_time_epoch_ms",
    "event_time_iso",
]


def load_clean(clean_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    return tuple(
        pd.read_parquet(clean_dir / name)
        for name in (
            "orders.parquet",
            "order_products__prior.parquet",
            "order_products__train.parquet",
            "products.parquet",
        )
    )


def _namespace_seed(seed: int, namespace: str) -> np.uint64:
    """Return a stable 64-bit namespace seed (never use Python's salted hash)."""
    digest = hashlib.blake2b(f"{seed}:{namespace}".encode(), digest_size=8).digest()
    return np.uint64(int.from_bytes(digest, "little"))


def stable_uniform(
    seed: int,
    namespace: str,
    primary_ids: pd.Series | np.ndarray,
    secondary_ids: pd.Series | np.ndarray | None = None,
) -> np.ndarray:
    """Vectorized SplitMix64 values in [0, 1), keyed by stable entity ids.

    Values depend only on the configured seed, a fixed namespace, and entity
    identifiers. They are therefore invariant to input order and user batching.
    """
    values = np.asarray(primary_ids, dtype="uint64") ^ _namespace_seed(seed, namespace)
    if secondary_ids is not None:
        secondary = np.asarray(secondary_ids, dtype="uint64")
        with np.errstate(over="ignore"):
            secondary = secondary + np.uint64(0x9E3779B97F4A7C15)
            secondary = (secondary ^ (secondary >> np.uint64(30))) * np.uint64(0xBF58476D1CE4E5B9)
            secondary = (secondary ^ (secondary >> np.uint64(27))) * np.uint64(0x94D049BB133111EB)
            values ^= secondary ^ (secondary >> np.uint64(31))
    with np.errstate(over="ignore"):
        values = values + np.uint64(0x9E3779B97F4A7C15)
        values = (values ^ (values >> np.uint64(30))) * np.uint64(0xBF58476D1CE4E5B9)
        values = (values ^ (values >> np.uint64(27))) * np.uint64(0x94D049BB133111EB)
        values ^= values >> np.uint64(31)
    return (values >> np.uint64(11)).astype("float64") * (1.0 / (1 << 53))


def order_time_parts(order_ids: pd.Series, mode: str, seed: int) -> tuple[np.ndarray, np.ndarray]:
    if mode == "hash":
        minute = (order_ids.astype("int64") * 10**6 + 1) % 60
        return minute.to_numpy(), np.zeros(len(order_ids), dtype="int64")
    minute = np.floor(stable_uniform(seed, "order-minute", order_ids) * 60).astype("int64")
    second = np.floor(stable_uniform(seed, "order-second", order_ids) * 60).astype("int64")
    return minute, second


def prepare_orders(orders: pd.DataFrame, config: SyntheticConfig) -> pd.DataFrame:
    """Compute per-order absolute base timestamp: synthetic date + hour + minute + second."""
    stream = orders[orders["eval_set"].isin(["prior", "train"])].copy()
    if config.limit_users:
        retained_users = np.sort(stream["user_id"].unique())[: config.limit_users]
        stream = stream[stream["user_id"].isin(retained_users)]
    stream = stream.sort_values(["user_id", "order_number"]).reset_index(drop=True)

    first_dow = stream.groupby("user_id", sort=True)["order_dow"].first()
    anchor_weeks = np.floor(
        stable_uniform(config.seed, "user-anchor-week", first_dow.index.to_numpy())
        * config.scatter_window_weeks
    ).astype("int64")
    anchor_days = first_dow + anchor_weeks * 7

    stream["day_offset"] = (
        stream["user_id"].map(anchor_days)
        + stream.groupby("user_id")["days_since_prior_order"].cumsum().astype("int64")
    )
    minute, second = order_time_parts(stream["order_id"], config.time_mode, config.seed)
    stream["base_ms"] = (
        BASE_EPOCH_MS
        + stream["day_offset"] * DAY_MS
        + stream["order_hour_of_day"].astype("int64") * 3_600_000
        + minute * 60_000
        + second * 1_000
    ).astype("int64")
    return stream.drop(columns=["day_offset"])


def enforce_monotonicity(events_df: pd.DataFrame) -> pd.DataFrame:
    """Guarantee event_time strictly increases per user in chronological order sequence."""
    orders = events_df.groupby("order_id", sort=False).agg(
        user=("user_id", "first"),
        order_num=("order_number", "first"),
        first_ms=("event_ms", "min"),
        last_ms=("event_ms", "max"),
    ).sort_values(["user", "order_num"])

    users = orders["user"].to_numpy()
    first_timestamps = orders["first_ms"].to_numpy()
    last_timestamps = orders["last_ms"].to_numpy()
    shift_ms = np.zeros(len(orders), dtype="int64")

    previous_user = None
    previous_last_ms = 0
    for i in range(len(orders)):
        if users[i] != previous_user:
            previous_user = users[i]
            previous_last_ms = 0
        needed_shift = previous_last_ms + 1000 - first_timestamps[i]
        if needed_shift > 0:
            shift_ms[i] = needed_shift
        previous_last_ms = max(previous_last_ms, last_timestamps[i] + shift_ms[i])

    shift_by_order = dict(zip(orders.index, shift_ms))
    result_df = events_df.copy()
    result_df["event_ms"] += result_df["order_id"].map(shift_by_order).fillna(0).astype("int64")
    return result_df


def expand_events(
    stream: pd.DataFrame,
    order_products: pd.DataFrame,
    products: pd.DataFrame,
    config: SyntheticConfig,
) -> pd.DataFrame:
    events = (
        order_products.merge(
            stream[["order_id", "user_id", "order_number", "order_hour_of_day", "order_dow", "base_ms"]],
            on="order_id",
        )
        .merge(products[["product_id", "aisle_id", "department_id"]], on="product_id")
        .sort_values(["user_id", "order_number", "add_to_cart_order"])
        .reset_index(drop=True)
    )

    uniform = stable_uniform(
        config.seed,
        "event-item-gap",
        events["order_id"],
        events["add_to_cart_order"],
    )
    if config.delta.kind == "uniform":
        delta_seconds = config.delta.lo_s + uniform * (config.delta.hi_s - config.delta.lo_s)
    else:
        delta_seconds = np.clip(
            -config.delta.mean_s * np.log1p(-uniform),
            config.delta.lo_s,
            config.delta.hi_s,
        )
    delta_seconds[events["add_to_cart_order"].to_numpy() == 1] = 0.0
    events["delta_s"] = delta_seconds

    elapsed = events.groupby("order_id", sort=False)["delta_s"].cumsum()
    events["event_ms"] = (events["base_ms"] + (elapsed * 1000).round()).astype("int64")
    return enforce_monotonicity(events.drop(columns=["delta_s", "base_ms"]))


def finalize(events_df: pd.DataFrame) -> pd.DataFrame:
    output_df = pd.DataFrame(index=events_df.index)
    output_df["event_id"] = events_df["order_id"].astype(str) + "_" + events_df["add_to_cart_order"].astype(str)
    output_df["order_id"] = events_df["order_id"].astype("int64")
    output_df["user_id"] = events_df["user_id"].astype("int64")
    output_df["order_number"] = events_df["order_number"].astype("int16")
    output_df["product_id"] = events_df["product_id"].astype("int64")
    output_df["add_to_cart_order"] = events_df["add_to_cart_order"].astype("int16")
    output_df["reordered"] = events_df["reordered"].astype("bool")
    output_df["aisle_id"] = events_df["aisle_id"].astype("int16")
    output_df["department_id"] = events_df["department_id"].astype("int8")
    output_df["order_hour_of_day"] = events_df["order_hour_of_day"].astype("int8")
    output_df["order_dow"] = events_df["order_dow"].astype("int8")
    output_df["event_time_epoch_ms"] = events_df["event_ms"].astype("int64")
    output_df["event_time_iso"] = pd.to_datetime(events_df["event_ms"], unit="ms", utc=True).dt.strftime(
        "%Y-%m-%dT%H:%M:%S.%f"
    )
    output_df["synthetic_date"] = pd.to_datetime(events_df["event_ms"], unit="ms", utc=True).dt.strftime(
        "%Y-%m-%d"
    )
    return output_df


def monotonic_violations(out: pd.DataFrame) -> int:
    """Count business-order timestamp inversions or collisions per user.

    Validates that timestamps strictly increase along the business sequence:
    user_id -> order_number -> add_to_cart_order.
    """
    if len(out) <= 1:
        return 0
    sorted_events = out.sort_values(["user_id", "order_number", "add_to_cart_order"])
    same_user = sorted_events["user_id"].to_numpy()[1:] == sorted_events["user_id"].to_numpy()[:-1]
    regressed = sorted_events["event_time_epoch_ms"].to_numpy()[1:] <= sorted_events["event_time_epoch_ms"].to_numpy()[:-1]
    return int((same_user & regressed).sum())


class EventWriter:
    """Writes finalized events, appending per user-batch. Tracks totals."""

    def __init__(self, output_dir: Path, output_format: str):
        output_dir.mkdir(parents=True, exist_ok=True)
        self.path = output_dir / ("events.parquet" if output_format == "parquet" else "events.json.gz")
        self.output_format = output_format
        self._parquet_writer = None
        self._gzip_file = None
        self.daily_counts: dict[str, int] = {}
        self.events_count = 0
        self.orders_count = 0
        self.violations_count = 0

    def add(self, batch_df: pd.DataFrame) -> None:
        self.events_count += len(batch_df)
        self.orders_count += int(batch_df["order_id"].nunique())
        self.violations_count += monotonic_violations(batch_df)
        for date_str, count in batch_df["synthetic_date"].value_counts().items():
            self.daily_counts[str(date_str)] = self.daily_counts.get(str(date_str), 0) + int(count)

        if self.output_format == "jsonl":
            if self._gzip_file is None:
                import gzip
                self._gzip_file = gzip.open(str(self.path), "wt", encoding="utf-8")
            self._gzip_file.write(batch_df[SCHEMA_ORDER].to_json(orient="records", lines=True))
            self._gzip_file.write("\n")
            return

        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.Table.from_pandas(batch_df)
        if self._parquet_writer is None:
            self._parquet_writer = pq.ParquetWriter(self.path, table.schema)
        self._parquet_writer.write_table(table)

    def close(self) -> None:
        if self._parquet_writer is not None:
            self._parquet_writer.close()
        if self._gzip_file is not None:
            self._gzip_file.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    default_data_dir = Path(__file__).resolve().parents[2] / "data" / "clean"
    parser.add_argument("--data-dir", default=str(default_data_dir))
    parser.add_argument("--output-dir", "--out-dir", dest="output_dir", default=None)
    parser.add_argument("--scenario", default="default")
    parser.add_argument("--scatter-weeks", type=int, default=None, choices=(1, 4, 13, 26, 52))
    parser.add_argument("--time-mode", choices=("uniform", "hash"), default=None)
    parser.add_argument("--limit-users", type=int, default=None)
    parser.add_argument("--batch-users", type=int, default=20000,
                        help="Finalize and write in chunks of this many users to bound peak memory")
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--format", dest="fmt", default="parquet", choices=("parquet", "jsonl"))
    parser.add_argument("--list-scenarios", action="store_true")
    args = parser.parse_args()

    if args.list_scenarios:
        print(preset_summary())
        return 0

    config = SyntheticConfig.from_scenario(
        args.scenario,
        seed=args.seed,
        scatter_window_weeks=args.scatter_weeks,
        time_mode=args.time_mode,
        limit_users=args.limit_users,
    )

    clean_dir = Path(args.data_dir)
    output_dir = Path(args.output_dir) if args.output_dir else clean_dir.parent / "synthesized"

    orders, prior, train, products = load_clean(clean_dir)
    order_products = pd.concat([prior, train]).reset_index(drop=True)

    scope = orders[orders["eval_set"].isin(["prior", "train"])].copy()
    user_ids = np.unique(np.sort(scope["user_id"].to_numpy()))
    if config.limit_users:
        user_ids = user_ids[: config.limit_users]

    writer = EventWriter(output_dir, args.fmt)
    print(f"[config] {json.dumps(config.to_dict(), default=str)}", flush=True)
    total_batches = (len(user_ids) + args.batch_users - 1) // args.batch_users
    for batch_idx, start_idx in enumerate(range(0, len(user_ids), args.batch_users)):
        current_user_ids = user_ids[start_idx : start_idx + args.batch_users]
        batch_stream = prepare_orders(scope[scope["user_id"].isin(current_user_ids)], config)
        batch_order_products = order_products.merge(batch_stream[["order_id"]], on="order_id")
        events_df = expand_events(batch_stream, batch_order_products, products, config)
        writer.add(finalize(events_df))
        del events_df, batch_stream, batch_order_products
        print(f"  batch {batch_idx + 1}/{total_batches}: events so far={writer.events_count:,}", flush=True)
    writer.close()
    del orders, scope, order_products

    daily_counts = pd.Series(writer.daily_counts, dtype="int64").sort_index()
    stats = {
        "config": config.to_dict(),
        "batch_users": args.batch_users,
        "determinism": {
            "strategy": "entity-keyed-splitmix64",
            "keys": {
                "anchor_week": "user_id",
                "order_clock": "order_id",
                "item_gap": "order_id+add_to_cart_order",
            },
            "batch_size_invariant": True,
        },
        "output": str(writer.path),
        "events": writer.events_count,
        "users": len(user_ids),
        "orders": writer.orders_count,
        "span": [str(daily_counts.index.min()), str(daily_counts.index.max())],
        "days": len(daily_counts),
        "peak_daily": int(daily_counts.max()) if len(daily_counts) else 0,
        "mean_daily": round(float(daily_counts.mean()), 1) if len(daily_counts) else 0.0,
        "monotonic_violations": writer.violations_count,
    }
    (output_dir / "manifest.json").write_text(json.dumps(stats, indent=2, default=str))

    print(f"[wrote ] {stats['output']}")
    print(f"  events={stats['events']:,} users={stats['users']:,} orders={stats['orders']:,}")
    print(f"  span={stats['span'][0]}..{stats['span'][1]} days={stats['days']} "
          f"peak/mean daily={stats['peak_daily']:,}/{stats['mean_daily']:,}")
    print(f"  monotonic violations={stats['monotonic_violations']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
