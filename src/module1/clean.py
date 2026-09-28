"""Module 1 - Step 1: Clean raw Instacart CSVs per specification.

Fixes applied:
  1. NaN days_since_prior_order (first-ever order, order_number=1) -> 0.0
  2. Cap-30 reconstruction: minimum weekday-consistent synthetic gap in [30, 36]
  3. Non-breaking space (\\xa0) stripped from product_name
  4. eval_set='test' preserved and flagged (excluded from train/streaming interactions)
  5. Narrow data types applied across all columns for memory efficiency
  6. Unpurchased catalog products retained

Usage:
  python -m src.module1.clean [--data-dir data] [--output-dir data/clean] [--validate]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
import pandas as pd

RAW_DATA_TYPES = {
    "orders": {
        "order_id": "int32",
        "user_id": "int32",
        "eval_set": "category",
        "order_number": "int16",
        "order_dow": "int16",
        "order_hour_of_day": "int16",
        "days_since_prior_order": "float32",
    },
    "order_products__prior": {
        "order_id": "int32",
        "product_id": "int32",
        "add_to_cart_order": "int16",
        "reordered": "int8",
    },
    "order_products__train": {
        "order_id": "int32",
        "product_id": "int32",
        "add_to_cart_order": "int16",
        "reordered": "int8",
    },
    "products": {
        "product_id": "int32",
        "aisle_id": "int16",
        "department_id": "int8",
        "product_name": "string",
    },
    "aisles": {"aisle_id": "int16", "aisle": "string"},
    "departments": {"department_id": "int8", "department": "string"},
}


def load_raw(data_dir: Path) -> dict[str, pd.DataFrame]:
    dataframes: dict[str, pd.DataFrame] = {}
    for table_name, dtypes in RAW_DATA_TYPES.items():
        csv_path = data_dir / "raw" / f"{table_name}.csv"
        if not csv_path.exists():
            print(f"[skip] missing {csv_path.name}")
            continue
        dataframes[table_name] = pd.read_csv(csv_path, dtype=dtypes)
        print(f"[load] {csv_path.name}: {len(dataframes[table_name]):,} rows")
    return dataframes


def clean_orders(orders: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    stats: dict[str, int] = {}
    df = orders.copy()

    first_order_nan_count = int((df["days_since_prior_order"].isna() & (df["order_number"] == 1)).sum())
    stats["first_order_nan_gap"] = first_order_nan_count
    df["dspo_raw"] = df["days_since_prior_order"]
    df["days_since_prior_order"] = df["days_since_prior_order"].fillna(0.0)

    df = df.sort_values(["user_id", "order_number"], kind="mergesort").reset_index(drop=True)
    capped_mask = df["dspo_raw"].eq(30)
    stats["cap30_rows"] = int(capped_mask.sum())
    previous_dow = df.groupby("user_id", sort=False)["order_dow"].shift(1)
    df["prev_dow"] = previous_dow.astype("float64")

    df["cap_recovered"] = False
    df.loc[capped_mask, "days_since_prior_order"] = 30 + (
        (df.loc[capped_mask, "order_dow"] - df.loc[capped_mask, "prev_dow"] - 2) % 7
    )
    df.loc[capped_mask, "cap_recovered"] = True

    recovered_gaps = df.loc[capped_mask, "days_since_prior_order"]
    stats["cap_recovered_range_ok"] = int(bool(((recovered_gaps >= 30) & (recovered_gaps <= 36)).all()))
    stats["cap_recovered_valid_dow"] = int(recovered_gaps.notna().sum())

    valid_mask = df["prev_dow"].notna() & df["dspo_raw"].notna()
    dow_diff = (df.loc[valid_mask, "order_dow"] - df.loc[valid_mask, "prev_dow"]) % 7
    gap_diff = df.loc[valid_mask, "days_since_prior_order"] % 7
    stats["dow_mismatch_after_recovery"] = int((dow_diff != gap_diff).sum())

    stats["eval_set_counts"] = {str(k): int(v) for k, v in df["eval_set"].value_counts().items()}
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


def clean_order_products(order_products: pd.DataFrame) -> pd.DataFrame:
    df = order_products.copy()
    df["reordered"] = df["reordered"].astype("bool")
    df["add_to_cart_order"] = df["add_to_cart_order"].astype("int32")
    return df


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    default_data_dir = Path(__file__).resolve().parents[2] / "data"
    parser.add_argument("--data-dir", default=str(default_data_dir))
    parser.add_argument("--output-dir", "--out-dir", dest="output_dir", default=None)
    parser.add_argument("--validate", action="store_true", help="Print data quality audit statistics")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    output_dir = Path(args.output_dir) if args.output_dir else data_dir / "clean"
    output_dir.mkdir(parents=True, exist_ok=True)

    dataframes = load_raw(data_dir)
    if not dataframes:
        print("no raw CSVs found in data directory", file=sys.stderr)
        return 1

    orders, order_stats = clean_orders(dataframes["orders"])
    products, product_stats = clean_products(dataframes["products"])
    prior = clean_order_products(dataframes["order_products__prior"])
    train = clean_order_products(dataframes["order_products__train"])
    aisles = dataframes.get("aisles")
    departments = dataframes.get("departments")

    row_counts = {"orders": len(orders), "prior": len(prior), "train": len(train)}
    orders.to_parquet(output_dir / "orders.parquet", index=False)
    prior.to_parquet(output_dir / "order_products__prior.parquet", index=False)
    train.to_parquet(output_dir / "order_products__train.parquet", index=False)
    products.to_parquet(output_dir / "products.parquet", index=False)
    if aisles is not None:
        aisles.to_parquet(output_dir / "aisles.parquet", index=False)
    if departments is not None:
        departments.to_parquet(output_dir / "departments.parquet", index=False)

    print("\n[clean] outputs written to", output_dir)
    for name, count in row_counts.items():
        print(f"  {name}: {count:,}")
    if args.validate:
        print("\n[validate]")
        print(f"  first-order NaN gaps -> 0            : {order_stats['first_order_nan_gap']:,}")
        print(f"  cap-30 rows reconstructed [30,36]    : {order_stats['cap30_rows']:,}")
        print(f"  reconstructed gap in [30,36]         : {order_stats['cap_recovered_range_ok']}")
        print(f"  DOW mismatches after reconstruction  : {order_stats['dow_mismatch_after_recovery']}")
        print(f"  \\xa0 product names cleaned           : {product_stats['nbsp_rows']}")
        print(f"  eval_set counts                      : {order_stats['eval_set_counts']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
