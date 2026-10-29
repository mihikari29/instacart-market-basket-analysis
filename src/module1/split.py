"""Module 1 - Chronological 80/20 Train/Test Partitioning.

Enforces strict temporal isolation for Next-Basket Grocery Recommendation:
  - For each user u with N_u orders, threshold_u = max(1, round(split_ratio * N_u)).
  - Orders 1..threshold_u -> Train partition (~80% volume, ~27.28M items).
  - Orders (threshold_u + 1)..N_u -> Test partition (~20% volume, ~6.53M items).
  - Zero leakage guarantee: max(train_order_number_u) < min(test_order_number_u) for all users.

Usage:
  python -m src.module1.split --clean-dir data/clean --output-dir data/clean/split_80_20 --split-ratio 0.8
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd

from .config import PARQUET_COMPRESSION


def temporal_split_orders(
    orders: pd.DataFrame,
    split_ratio: float = 0.8,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Partition orders table chronologically per user.

    Parameters:
        orders: DataFrame containing at least 'order_id', 'user_id', 'order_number'.
        split_ratio: Target fraction of earlier orders allocated to train (default 0.8).

    Returns:
        tuple of (train_orders, test_orders) DataFrames with 'temporal_split' column.
    """
    if not (0.0 < split_ratio < 1.0):
        raise ValueError(f"split_ratio must be strictly between 0 and 1, got {split_ratio}")
    for col in ("order_id", "user_id", "order_number"):
        if col not in orders.columns:
            raise ValueError(f"orders must contain '{col}' column")

    orders_df = orders.copy()
    max_orders = orders_df.groupby("user_id", sort=False)["order_number"].transform("max")
    train_threshold = np.maximum(1, np.round(split_ratio * max_orders)).astype("int16")

    is_train = orders_df["order_number"] <= train_threshold
    orders_df["temporal_split"] = np.where(is_train, "train", "test")

    train_orders = orders_df[is_train].copy().reset_index(drop=True)
    test_orders = orders_df[~is_train].copy().reset_index(drop=True)

    return train_orders, test_orders


def split_order_products(
    orders_with_split: pd.DataFrame,
    order_products: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Partition order_products into train and test sets using order split identities.

    Parameters:
        orders_with_split: DataFrame with 'order_id' and 'temporal_split'.
        order_products: DataFrame with 'order_id' and product purchase facts.

    Returns:
        tuple of (train_products, test_products).
    """
    merged = order_products.merge(
        orders_with_split[["order_id", "temporal_split"]],
        on="order_id",
        how="inner",
    )
    is_train = merged["temporal_split"] == "train"

    train_products = merged[is_train].drop(columns=["temporal_split"]).reset_index(drop=True)
    test_products = merged[~is_train].drop(columns=["temporal_split"]).reset_index(drop=True)

    return train_products, test_products


def verify_temporal_isolation(
    train_orders: pd.DataFrame,
    test_orders: pd.DataFrame,
) -> dict[str, int | float | bool]:
    """Verify that no future order exists in the train set for any user."""
    if test_orders.empty:
        return {
            "leakage_violations": 0,
            "strictly_isolated": True,
            "evaluated_users": int(train_orders["user_id"].nunique()),
        }

    max_train_order = train_orders.groupby("user_id")["order_number"].max()
    min_test_order = test_orders.groupby("user_id")["order_number"].min()

    comparison = pd.DataFrame({
        "max_train": max_train_order,
        "min_test": min_test_order,
    }).dropna()

    violations = int((comparison["max_train"] >= comparison["min_test"]).sum())
    return {
        "leakage_violations": violations,
        "strictly_isolated": violations == 0,
        "evaluated_users": int(len(comparison)),
    }


def execute_split(
    clean_dir: Path,
    output_dir: Path,
    split_ratio: float = 0.8,
) -> dict:
    """Read curated clean data, compute temporal 80/20 split, write partitioned Parquets."""
    orders_path = clean_dir / "orders.parquet"
    prior_path = clean_dir / "order_products__prior.parquet"
    train_path = clean_dir / "order_products__train.parquet"

    if not orders_path.exists():
        raise FileNotFoundError(f"Missing curated orders: {orders_path}")
    if not prior_path.exists():
        raise FileNotFoundError(f"Missing curated prior products: {prior_path}")

    orders = pd.read_parquet(orders_path)
    prior_products = pd.read_parquet(prior_path)
    train_products = pd.read_parquet(train_path) if train_path.exists() else pd.DataFrame()

    all_products = (
        pd.concat([prior_products, train_products], ignore_index=True)
        if not train_products.empty
        else prior_products
    )

    train_orders, test_orders = temporal_split_orders(orders, split_ratio=split_ratio)
    combined_orders_split = pd.concat([train_orders, test_orders], ignore_index=True)

    train_prods, test_prods = split_order_products(combined_orders_split, all_products)
    isolation_audit = verify_temporal_isolation(train_orders, test_orders)

    output_dir.mkdir(parents=True, exist_ok=True)

    train_orders.to_parquet(
        output_dir / "orders_train.parquet",
        compression=PARQUET_COMPRESSION,
        index=False,
    )
    test_orders.to_parquet(
        output_dir / "orders_test.parquet",
        compression=PARQUET_COMPRESSION,
        index=False,
    )
    train_prods.to_parquet(
        output_dir / "order_products_train.parquet",
        compression=PARQUET_COMPRESSION,
        index=False,
    )
    test_prods.to_parquet(
        output_dir / "order_products_test.parquet",
        compression=PARQUET_COMPRESSION,
        index=False,
    )

    manifest = {
        "strategy": "per_user_temporal_split",
        "split_ratio_target": split_ratio,
        "orders": {
            "total": len(orders),
            "train": len(train_orders),
            "test": len(test_orders),
            "train_ratio": round(len(train_orders) / len(orders), 4),
            "test_ratio": round(len(test_orders) / len(orders), 4),
        },
        "purchases": {
            "total": len(all_products),
            "train": len(train_prods),
            "test": len(test_prods),
            "train_ratio": round(len(train_prods) / len(all_products), 4),
            "test_ratio": round(len(test_prods) / len(all_products), 4),
        },
        "users": {
            "total_users": int(orders["user_id"].nunique()),
            "train_users": int(train_orders["user_id"].nunique()),
            "test_users": int(test_orders["user_id"].nunique()),
        },
        "isolation_audit": isolation_audit,
    }

    manifest_path = output_dir / "split_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    default_clean_dir = Path(__file__).resolve().parents[2] / "data" / "clean"
    parser.add_argument("--clean-dir", default=str(default_clean_dir))
    parser.add_argument(
        "--output-dir",
        default=str(default_clean_dir / "split_80_20"),
        help="Target directory for partitioned Parquet datasets",
    )
    parser.add_argument("--split-ratio", type=float, default=0.8)
    args = parser.parse_args(argv)

    try:
        manifest = execute_split(
            clean_dir=Path(args.clean_dir),
            output_dir=Path(args.output_dir),
            split_ratio=args.split_ratio,
        )
        print("\n[split] temporal 80/20 partitioning completed successfully:")
        print(f"  Train orders   : {manifest['orders']['train']:,} ({manifest['orders']['train_ratio'] * 100:.2f}%)")
        print(f"  Test orders    : {manifest['orders']['test']:,} ({manifest['orders']['test_ratio'] * 100:.2f}%)")
        print(f"  Train purchases: {manifest['purchases']['train']:,} ({manifest['purchases']['train_ratio'] * 100:.2f}%)")
        print(f"  Test purchases : {manifest['purchases']['test']:,} ({manifest['purchases']['test_ratio'] * 100:.2f}%)")
        print(f"  Zero-leakage   : {manifest['isolation_audit']['strictly_isolated']}")
        return 0
    except Exception as exc:
        print(f"[error] {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
