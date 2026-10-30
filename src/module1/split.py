"""Export an audited per-user temporal split of labeled curated purchases.

Official train baskets always remain evaluation truth. Unlabeled official test
orders are excluded. This export does not replace the Modules 2/4 input contract
or establish a common absolute-time cutoff across users.
"""

from __future__ import annotations

import argparse
import json
from numbers import Real
from pathlib import Path
import sys

import numpy as np
import pandas as pd

from ..common.runtime import config_section, file_sha256, read_yaml_mapping
from .config import INT32_MAX, PARQUET_COMPRESSION


def _validate_orders(orders: pd.DataFrame) -> None:
    for column in ("order_id", "user_id", "order_number", "eval_set"):
        if column not in orders.columns:
            raise ValueError(f"orders must contain '{column}' column")
    for column in ("order_id", "user_id", "order_number"):
        values = orders[column]
        if (
            not pd.api.types.is_numeric_dtype(values)
            or pd.api.types.is_bool_dtype(values)
            or not (values.notna() & values.between(1, INT32_MAX) & values.mod(1).eq(0)).all()
        ):
            raise ValueError(f"orders.{column} must contain positive int32 integers")
    if orders["order_id"].duplicated().any() or orders.duplicated(["user_id", "order_number"]).any():
        raise ValueError("orders contain duplicate order identities or user/order numbers")
    if not orders["eval_set"].isin(["prior", "train", "test"]).all():
        raise ValueError("orders contain invalid eval_set provenance")
    prior_max = orders.loc[orders["eval_set"].eq("prior")].groupby("user_id")["order_number"].max()
    target_min = orders.loc[orders["eval_set"].ne("prior")].groupby("user_id")["order_number"].min()
    common = prior_max.index.intersection(target_min.index)
    if (prior_max.loc[common] >= target_min.loc[common]).any():
        raise ValueError("official evaluation orders must follow prior history")


def temporal_split_orders(
    orders: pd.DataFrame, split_ratio: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split labeled orders by chronological rank, keeping official targets out of training.

    Round the labeled order count with NumPy's round-to-even rule; clamp to
    [1, N-1] for N > 1. A singleton prior order is training-only. Official train
    orders remain test even when the requested ratio would place them in train.
    """
    if isinstance(split_ratio, bool) or not isinstance(split_ratio, Real) or not 0 < split_ratio < 1:
        raise ValueError(f"split_ratio must be strictly between 0 and 1, got {split_ratio}")
    _validate_orders(orders)
    frame = orders.loc[orders["eval_set"].ne("test")].sort_values(
        ["user_id", "order_number", "order_id"], kind="stable",
    ).copy()
    counts = frame.groupby("user_id", sort=False)["order_id"].transform("size")
    thresholds = np.minimum(np.maximum(1, np.rint(split_ratio * counts)), np.maximum(1, counts - 1))
    ranks = frame.groupby("user_id", sort=False).cumcount() + 1
    is_train = ranks.le(thresholds) & frame["eval_set"].eq("prior")
    frame["temporal_split"] = np.where(is_train, "train", "test")
    return frame.loc[is_train].reset_index(drop=True), frame.loc[~is_train].reset_index(drop=True)


def split_order_products(
    orders_with_split: pd.DataFrame, order_products: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Partition facts without dropping unknown orders or multiplying duplicate identities."""
    if not {"order_id", "temporal_split"} <= set(orders_with_split.columns):
        raise ValueError("orders must contain order_id and temporal_split")
    if orders_with_split["order_id"].isna().any() or orders_with_split["order_id"].duplicated().any():
        raise ValueError("split orders must have unique non-null order_id values")
    if not orders_with_split["temporal_split"].isin(["train", "test"]).all():
        raise ValueError("invalid temporal_split label")
    required = {"order_id", "product_id", "add_to_cart_order", "reordered"}
    if not required <= set(order_products.columns) or "temporal_split" in order_products.columns:
        raise ValueError("purchase facts have missing columns or pre-existing split labels")
    for column in ("order_id", "product_id", "add_to_cart_order"):
        values = order_products[column]
        if (
            not pd.api.types.is_numeric_dtype(values)
            or pd.api.types.is_bool_dtype(values)
            or not (values.notna() & values.between(1, INT32_MAX) & values.mod(1).eq(0)).all()
        ):
            raise ValueError(f"purchase {column} must contain positive int32 integers")
    if not order_products["reordered"].isin([0, 1]).all():
        raise ValueError("invalid reordered purchase value")
    if order_products.duplicated(["order_id", "product_id"]).any() or order_products.duplicated(
        ["order_id", "add_to_cart_order"]
    ).any():
        raise ValueError("duplicate purchase or cart position")
    merged = order_products.merge(
        orders_with_split[["order_id", "temporal_split"]], on="order_id", how="left",
        validate="many_to_one", indicator=True,
    )
    if merged["_merge"].ne("both").any():
        raise ValueError("purchase facts reference unknown or unlabeled orders")
    merged = merged.drop(columns="_merge").sort_values(["order_id", "add_to_cart_order"], kind="stable")
    is_train = merged["temporal_split"].eq("train")
    return (
        merged.loc[is_train].drop(columns="temporal_split").reset_index(drop=True),
        merged.loc[~is_train].drop(columns="temporal_split").reset_index(drop=True),
    )


def verify_temporal_isolation(train_orders: pd.DataFrame, test_orders: pd.DataFrame) -> dict:
    """Audit both disjoint identities and chronology, including empty/cold-start cohorts."""
    _validate_orders(train_orders)
    _validate_orders(test_orders)
    combined = pd.concat([train_orders, test_orders], ignore_index=True)
    # Cross-partition duplicates are audit failures rather than silently dropped rows.
    overlapping = int(train_orders["order_id"].isin(test_orders["order_id"]).sum())
    _validate_orders(combined.drop_duplicates(["order_id", "user_id", "order_number"]))
    comparison = pd.concat([
        train_orders.groupby("user_id")["order_number"].max().rename("max_train"),
        test_orders.groupby("user_id")["order_number"].min().rename("min_test"),
    ], axis=1).dropna()
    violations = int(comparison["max_train"].ge(comparison["min_test"]).sum())
    official_targets = int(train_orders["eval_set"].ne("prior").sum())
    return {
        "leakage_violations": violations,
        "overlapping_orders": overlapping,
        "official_targets_in_training": official_targets,
        "strictly_isolated": violations == overlapping == official_targets == 0,
        "evaluated_users": int(len(comparison)),
        "test_only_users": int(test_orders.loc[~test_orders["user_id"].isin(train_orders["user_id"]), "user_id"].nunique()),
    }


def execute_split(clean_dir: Path, output_dir: Path, split_ratio: float) -> dict:
    """Export to a fresh directory, preserving source provenance and any existing outputs."""
    clean_dir, output_dir = clean_dir.resolve(), output_dir.absolute()
    if output_dir.exists() or output_dir.is_symlink():
        raise FileExistsError(f"Split output already exists: {output_dir}")
    paths = {name: clean_dir / f"{name}.parquet" for name in (
        "orders", "order_products__prior", "order_products__train",
    )}
    sources = {name: {"sha256": file_sha256(path)} for name, path in paths.items()}
    tables = {name: pd.read_parquet(path) for name, path in paths.items()}
    orders = tables["orders"]
    train_orders, test_orders = temporal_split_orders(orders, split_ratio)
    for source, expected in (("order_products__prior", "prior"), ("order_products__train", "train")):
        facts = tables[source]
        if "order_id" not in facts.columns or not facts["order_id"].isin(
            orders.loc[orders["eval_set"].eq(expected), "order_id"]
        ).all():
            raise ValueError(f"{source}: orphan order or wrong eval_set provenance")
    all_products = pd.concat([tables["order_products__prior"], tables["order_products__train"]], ignore_index=True)
    combined_orders = pd.concat([train_orders, test_orders], ignore_index=True)
    train_prods, test_prods = split_order_products(combined_orders, all_products)
    if not combined_orders["order_id"].isin(all_products["order_id"]).all():
        raise ValueError("labeled orders are missing purchase facts")
    audit = verify_temporal_isolation(train_orders, test_orders)
    if not audit["strictly_isolated"]:
        raise ValueError(f"Temporal isolation audit failed: {audit}")
    for name, path in paths.items():
        if file_sha256(path) != sources[name]["sha256"]:
            raise ValueError(f"Source changed during split: {name}")
        sources[name]["rows"] = len(tables[name])
    labeled_total = len(combined_orders)
    purchase_total = len(all_products)
    manifest = {
        "strategy": "per_user_temporal_split",
        "split_ratio_target": split_ratio,
        "official_train_policy": "evaluation_only",
        "unlabeled_test_orders_excluded": int(orders["eval_set"].eq("test").sum()),
        "sources": sources,
        "orders": {
            "total": labeled_total, "train": len(train_orders), "test": len(test_orders),
            "train_ratio": round(len(train_orders) / labeled_total, 4) if labeled_total else 0.0,
            "test_ratio": round(len(test_orders) / labeled_total, 4) if labeled_total else 0.0,
        },
        "purchases": {
            "total": purchase_total, "train": len(train_prods), "test": len(test_prods),
            "train_ratio": round(len(train_prods) / purchase_total, 4) if purchase_total else 0.0,
            "test_ratio": round(len(test_prods) / purchase_total, 4) if purchase_total else 0.0,
        },
        "users": {
            "total_users": int(combined_orders["user_id"].nunique()),
            "train_users": int(train_orders["user_id"].nunique()),
            "test_users": int(test_orders["user_id"].nunique()),
        },
        "isolation_audit": audit,
    }
    # Exclusive creation never replaces a previous publication. The manifest is
    # written last; its absence identifies incomplete exports after write failure.
    output_dir.mkdir(parents=True, exist_ok=False)
    outputs = {
        "orders_train": train_orders, "orders_test": test_orders,
        "order_products_train": train_prods, "order_products_test": test_prods,
    }
    for name, frame in outputs.items():
        path = output_dir / f"{name}.parquet"
        frame.to_parquet(path, compression=PARQUET_COMPRESSION, index=False)
    manifest["outputs"] = {name: {"rows": len(frame), "sha256": file_sha256(output_dir / f"{name}.parquet")}
                           for name, frame in outputs.items()}
    (output_dir / "split_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    root = Path(__file__).resolve().parents[2]
    parser.add_argument("--clean-dir", type=Path, default=root / "data" / "clean")
    parser.add_argument("--output-dir", type=Path, default=root / "data" / "clean" / "split_80_20")
    parser.add_argument("--pipeline-config", type=Path, default=root / "config" / "pipeline.yaml")
    args = parser.parse_args(argv)
    try:
        settings = config_section(read_yaml_mapping(args.pipeline_config), "temporal_split")
        if set(settings) != {"train_ratio"}:
            raise ValueError("temporal_split must define only train_ratio")
        manifest = execute_split(args.clean_dir, args.output_dir, settings["train_ratio"])
        print(json.dumps(manifest, indent=2))
        return 0
    except Exception as exc:
        print(f"[error] {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
