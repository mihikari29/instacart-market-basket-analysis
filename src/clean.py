"""Module 1 - step 1: clean raw Instacart CSVs per docs/clean.md.

Applies the 6 fixes from docs/clean.md and writes narrow-dtype parquet to
<data-dir>/clean so the timestamp generator (generate.py) can read fast.

Fixes:
  1. NaN days_since_prior_order (first-ever order, order_number=1) -> 0
  2. Cap-30 recovery: gap capped at 30 -> true gap in [30,36] from day-of-week
  3. Trailing non-breaking space (\\xa0) stripped from product_name
  4. eval_set='test' kept but flagged (excluded from train/stream logic)
  5. Narrow dtypes everywhere (memory)
  6. Never-purchased products kept as catalog (benign, no-op)

Usage:
  python src/clean.py [--data-dir data] [--out-dir data/clean] [--validate]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

RAW_DTYPES = {
    "orders": {
        "order_id": "int32", "user_id": "int32", "eval_set": "category",
        "order_number": "int16", "order_dow": "int16",
        "order_hour_of_day": "int16", "days_since_prior_order": "float32",
    },
    "order_products__prior": {
        "order_id": "int32", "product_id": "int32", "add_to_cart_order": "int16",
        "reordered": "int8",
    },
    "order_products__train": {
        "order_id": "int32", "product_id": "int32", "add_to_cart_order": "int16",
        "reordered": "int8",
    },
    "products": {
        "product_id": "int32", "aisle_id": "int16", "department_id": "int8",
        "product_name": "string",
    },
    "aisles": {"aisle_id": "int16", "aisle": "string"},
    "departments": {"department_id": "int8", "department": "string"},
}


def load_raw(data_dir: Path) -> dict[str, pd.DataFrame]:
    dfs: dict[str, pd.DataFrame] = {}
    for name, dtypes in RAW_DTYPES.items():
        path = data_dir / f"{name}.csv"
        if not path.exists():
            print(f"[skip] missing {path.name}")
            continue
        dfs[name] = pd.read_csv(path, dtype=dtypes)
        print(f"[load] {path.name}: {len(dfs[name]):,} rows")
    return dfs


def clean_orders(orders: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    stats: dict[str, int] = {}
    df = orders.copy()

    n_first = int((df["days_since_prior_order"].isna() & (df["order_number"] == 1)).sum())
    stats["first_order_nan_gap"] = n_first
    df["dspo_raw"] = df["days_since_prior_order"]
    df["days_since_prior_order"] = df["days_since_prior_order"].fillna(0.0)

    capped = (df["dspo_raw"] == 30) & df["dspo_raw"].notna()
    stats["cap30_rows"] = int(capped.sum())

    df = df.sort_values(["user_id", "order_number"], kind="mergesort").reset_index(drop=True)
    prev_dow = df.groupby("user_id", sort=False)["order_dow"].shift(1)
    df["prev_dow"] = prev_dow.astype("float64")

    df["cap_recovered"] = False
    df.loc[capped, "days_since_prior_order"] = 30 + ((df.loc[capped, "order_dow"] - df.loc[capped, "prev_dow"] - 2) % 7)
    df.loc[capped, "cap_recovered"] = True

    recovered = df.loc[capped, "days_since_prior_order"]
    stats["cap_recovered_range_ok"] = int(bool(((recovered >= 30) & (recovered <= 36)).all()))
    stats["cap_recovered_valid_dow"] = int(recovered.notna().sum())

    check = df
    valid = check["prev_dow"].notna() & check["dspo_raw"].notna()
    mismatches = int((((check.loc[valid, "order_dow"] - check.loc[valid, "prev_dow"]) % 7) != (check.loc[valid, "days_since_prior_order"] % 7)).sum())
    stats["dow_mismatch_after_recovery"] = mismatches

    stats["eval_set_counts"] = {
        str(k): int(v) for k, v in df["eval_set"].value_counts().items()
    }

    df = df.drop(columns=["prev_dow"])
    df["days_since_prior_order"] = df["days_since_prior_order"].astype("float32")
    return df, stats


def clean_products(products: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    stats: dict[str, int] = {}
    df = products.copy()
    has_nbsp = df["product_name"].str.contains("\xa0", regex=False)
    stats["nbsp_rows"] = int(has_nbsp.sum())
    df["product_name"] = (
        df["product_name"]
        .str.replace("\xa0", " ", regex=False)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )
    return df, stats


def clean_order_products(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["reordered"] = out["reordered"].astype("bool")
    out["add_to_cart_order"] = out["add_to_cart_order"].astype("int32")
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data-dir", default=str(Path(__file__).resolve().parent.parent / "data"))
    ap.add_argument("--out-dir", default=None)
    ap.add_argument("--validate", action="store_true", help="print audit checks")
    args = ap.parse_args()

    data_dir = Path(args.data_dir)
    out_dir = Path(args.out_dir) if args.out_dir else data_dir / "clean"
    out_dir.mkdir(parents=True, exist_ok=True)

    dfs = load_raw(data_dir)
    if not dfs:
        print("no raw CSVs found", file=sys.stderr)
        return 1

    orders, ostats = clean_orders(dfs["orders"])
    products, pstats = clean_products(dfs["products"])
    prior = clean_order_products(dfs["order_products__prior"])
    train = clean_order_products(dfs["order_products__train"])
    aisles = dfs.get("aisles")
    departments = dfs.get("departments")

    sizes = {"orders": len(orders), "prior": len(prior), "train": len(train)}
    orders.to_parquet(out_dir / "orders.parquet", index=False)
    prior.to_parquet(out_dir / "order_products__prior.parquet", index=False)
    train.to_parquet(out_dir / "order_products__train.parquet", index=False)
    products.to_parquet(out_dir / "products.parquet", index=False)
    if aisles is not None:
        aisles.to_parquet(out_dir / "aisles.parquet", index=False)
    if departments is not None:
        departments.to_parquet(out_dir / "departments.parquet", index=False)

    print("\n[clean] outputs written to", out_dir)
    for name, n in sizes.items():
        print(f"  {name}: {n:,}")
    if args.validate:
        print("\n[validate]")
        print(f"  first-order NaN gaps -> 0            : {ostats['first_order_nan_gap']:,}")
        print(f"  cap-30 rows recovered                : {ostats['cap30_rows']:,}")
        print(f"  recovered gap in [30,36]             : {ostats['cap_recovered_range_ok']}")
        print(f"  DOW mismatches after recovery        : {ostats['dow_mismatch_after_recovery']}")
        print(f"  \\xa0 product names cleaned           : {pstats['nbsp_rows']}")
        print(f"  eval_set counts                      : {ostats['eval_set_counts']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())