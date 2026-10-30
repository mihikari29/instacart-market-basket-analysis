"""Tests for chronological 80/20 train/test partitioning in Module 1."""

import pandas as pd
import pytest

from src.module1.split import (
    execute_split,
    split_order_products,
    temporal_split_orders,
    verify_temporal_isolation,
)


@pytest.fixture
def sample_orders_and_products():
    """Build synthetic orders and products for 10 users with 5 to 20 orders each."""
    order_rows = []
    product_rows = []
    order_id = 1

    for user_id in range(1, 11):
        num_orders = 5 + (user_id * 2)  # 7 to 25 orders
        for order_num in range(1, num_orders + 1):
            order_rows.append({
                "order_id": order_id,
                "user_id": user_id,
                "order_number": order_num,
                "eval_set": "prior",
                "order_dow": (order_num * 2) % 7,
                "order_hour_of_day": (order_num * 3) % 24,
                "days_since_prior_order": 7.0 if order_num > 1 else 0.0,
            })
            # 3 to 6 products per order
            for item_idx in range(1, 4):
                product_rows.append({
                    "order_id": order_id,
                    "product_id": (order_id * 10) + item_idx,
                    "add_to_cart_order": item_idx,
                    "reordered": item_idx > 1,
                })
            order_id += 1

    orders_df = pd.DataFrame(order_rows)
    products_df = pd.DataFrame(product_rows)
    return orders_df, products_df


def test_temporal_split_zero_leakage(sample_orders_and_products):
    orders, products = sample_orders_and_products
    train_orders, test_orders = temporal_split_orders(orders, split_ratio=0.8)

    # 1. Zero leakage: For every user, all train orders precede all test orders
    audit = verify_temporal_isolation(train_orders, test_orders)
    assert audit["strictly_isolated"] is True
    assert audit["leakage_violations"] == 0
    assert audit["evaluated_users"] == 10

    # 2. Every user has orders in both train and test (since num_orders >= 7)
    assert set(train_orders["user_id"]) == set(range(1, 11))
    assert set(test_orders["user_id"]) == set(range(1, 11))


def test_temporal_split_approximate_80_20_ratio(sample_orders_and_products):
    orders, products = sample_orders_and_products
    train_orders, test_orders = temporal_split_orders(orders, split_ratio=0.8)

    total_orders = len(orders)
    train_ratio = len(train_orders) / total_orders
    test_ratio = len(test_orders) / total_orders

    assert 0.75 <= train_ratio <= 0.85
    assert 0.15 <= test_ratio <= 0.25
    assert len(train_orders) + len(test_orders) == total_orders


def test_split_order_products_partitions_all_items_without_loss(sample_orders_and_products):
    orders, products = sample_orders_and_products
    train_orders, test_orders = temporal_split_orders(orders, split_ratio=0.8)
    combined_orders = pd.concat([train_orders, test_orders], ignore_index=True)

    train_products, test_products = split_order_products(combined_orders, products)

    assert len(train_products) + len(test_products) == len(products)
    assert len(train_products) > len(test_products)
    # Check that product order_ids belong strictly to the respective order partitions
    assert set(train_products["order_id"]).issubset(set(train_orders["order_id"]))
    assert set(test_products["order_id"]).issubset(set(test_orders["order_id"]))
    assert len(set(train_products["order_id"]) & set(test_products["order_id"])) == 0


def test_temporal_split_invalid_arguments(sample_orders_and_products):
    orders, _ = sample_orders_and_products
    with pytest.raises(ValueError, match="split_ratio must be strictly between 0 and 1"):
        temporal_split_orders(orders, split_ratio=0.0)

    with pytest.raises(ValueError, match="split_ratio must be strictly between 0 and 1"):
        temporal_split_orders(orders, split_ratio=1.0)

    with pytest.raises(ValueError, match="split_ratio must be strictly between 0 and 1"):
        temporal_split_orders(orders, split_ratio=-0.2)

    incomplete = orders.drop(columns=["order_number"])
    with pytest.raises(ValueError, match="orders must contain 'order_number'"):
        temporal_split_orders(incomplete, split_ratio=0.8)


def test_execute_split_end_to_end(tmp_path, sample_orders_and_products):
    orders, products = sample_orders_and_products
    clean_dir = tmp_path / "clean"
    clean_dir.mkdir()
    output_dir = tmp_path / "split_80_20"

    orders.to_parquet(clean_dir / "orders.parquet", index=False)
    products.to_parquet(clean_dir / "order_products__prior.parquet", index=False)
    products.iloc[:0].to_parquet(clean_dir / "order_products__train.parquet", index=False)

    manifest = execute_split(clean_dir, output_dir, split_ratio=0.8)

    assert manifest["strategy"] == "per_user_temporal_split"
    assert manifest["isolation_audit"]["strictly_isolated"] is True
    assert (output_dir / "orders_train.parquet").exists()
    assert (output_dir / "orders_test.parquet").exists()
    assert (output_dir / "order_products_train.parquet").exists()
    assert (output_dir / "order_products_test.parquet").exists()
    assert (output_dir / "split_manifest.json").exists()


def test_split_uses_observed_chronological_rank_and_preserves_official_targets():
    orders = pd.DataFrame({
        "order_id": [3, 1, 2, 4, 5, 6], "user_id": [7, 7, 7, 8, 8, 9],
        "order_number": [30, 10, 20, 1, 2, 1],
        "eval_set": ["train", "prior", "prior", "prior", "test", "train"],
    })
    train, test = temporal_split_orders(orders, 0.99)
    assert train.order_id.tolist() == [1, 2, 4]
    assert test.order_id.tolist() == [3, 6]
    audit = verify_temporal_isolation(train, test)
    assert audit["strictly_isolated"]
    assert audit["evaluated_users"] == 1
    assert audit["test_only_users"] == 1
    shuffled = temporal_split_orders(orders.sample(frac=1, random_state=9), 0.99)
    for expected, actual in zip((train, test), shuffled):
        pd.testing.assert_frame_equal(expected, actual)


def test_two_prior_orders_keep_a_holdout_even_at_high_ratio():
    orders = pd.DataFrame({"order_id": [1, 2], "user_id": [7, 7],
                           "order_number": [1, 2], "eval_set": ["prior", "prior"]})
    train, test = temporal_split_orders(orders, 0.99)
    assert train.order_id.tolist() == [1]
    assert test.order_id.tolist() == [2]


@pytest.mark.parametrize("column,value", [("user_id", None), ("order_number", 1.5),
                                         ("order_id", -1), ("eval_set", "unknown")])
def test_split_rejects_invalid_identity_and_provenance(sample_orders_and_products, column, value):
    orders, _ = sample_orders_and_products
    orders = orders.astype({column: "object"})
    orders.loc[0, column] = value
    with pytest.raises(ValueError):
        temporal_split_orders(orders, 0.8)


def test_split_rejects_duplicate_orders_and_nonchronological_targets(sample_orders_and_products):
    orders, _ = sample_orders_and_products
    with pytest.raises(ValueError, match="duplicate"):
        temporal_split_orders(pd.concat([orders, orders.iloc[:1]]), 0.8)
    orders.loc[0, "eval_set"] = "train"
    with pytest.raises(ValueError, match="must follow prior"):
        temporal_split_orders(orders, 0.8)


def test_fact_partition_fails_instead_of_dropping_or_multiplying_purchases(sample_orders_and_products):
    orders, products = sample_orders_and_products
    train, test = temporal_split_orders(orders, 0.8)
    labeled = pd.concat([train, test], ignore_index=True)
    with pytest.raises(ValueError, match="unique"):
        split_order_products(pd.concat([labeled, labeled.iloc[:1]]), products)
    orphan = products.copy()
    orphan.loc[0, "order_id"] = 999999
    with pytest.raises(ValueError, match="unknown"):
        split_order_products(labeled, orphan)
    with pytest.raises(ValueError, match="duplicate purchase"):
        split_order_products(labeled, pd.concat([products, products.iloc[:1]]))
    labeled.loc[0, "temporal_split"] = "validation"
    with pytest.raises(ValueError, match="invalid temporal_split"):
        split_order_products(labeled, products)


def test_audit_reports_overlap_and_future_history(sample_orders_and_products):
    orders, _ = sample_orders_and_products
    train, test = temporal_split_orders(orders, 0.8)
    audit = verify_temporal_isolation(pd.concat([train, test.iloc[:1]]), test)
    assert not audit["strictly_isolated"]
    assert audit["overlapping_orders"] == 1
    assert audit["leakage_violations"] == 1


def test_empty_export_and_existing_output_preservation(tmp_path, sample_orders_and_products):
    orders, products = sample_orders_and_products
    clean = tmp_path / "clean"
    clean.mkdir()
    orders.iloc[:0].to_parquet(clean / "orders.parquet", index=False)
    for name in ("prior", "train"):
        products.iloc[:0].to_parquet(clean / f"order_products__{name}.parquet", index=False)
    output = tmp_path / "split"
    manifest = execute_split(clean, output, 0.8)
    assert manifest["orders"]["total"] == manifest["purchases"]["total"] == 0
    assert manifest["isolation_audit"]["evaluated_users"] == 0
    assert manifest["isolation_audit"]["strictly_isolated"]
    before = {path.name: path.read_bytes() for path in output.iterdir()}
    with pytest.raises(FileExistsError):
        execute_split(clean, output, 0.8)
    assert {path.name: path.read_bytes() for path in output.iterdir()} == before


def test_export_rejects_wrong_source_provenance_and_missing_train(tmp_path, sample_orders_and_products):
    orders, products = sample_orders_and_products
    orders.to_parquet(tmp_path / "orders.parquet", index=False)
    products.to_parquet(tmp_path / "order_products__prior.parquet", index=False)
    output = tmp_path / "output"
    with pytest.raises(FileNotFoundError):
        execute_split(tmp_path, output, 0.8)
    products.iloc[:1].to_parquet(tmp_path / "order_products__train.parquet", index=False)
    with pytest.raises(ValueError, match="wrong eval_set"):
        execute_split(tmp_path, output, 0.8)
    assert not output.exists()


def test_export_audit_is_a_gate_and_outputs_have_provenance(tmp_path, sample_orders_and_products, monkeypatch):
    from src.module1 import split
    from src.common.runtime import file_sha256

    orders, products = sample_orders_and_products
    orders.to_parquet(tmp_path / "orders.parquet", index=False)
    products.to_parquet(tmp_path / "order_products__prior.parquet", index=False)
    products.iloc[:0].to_parquet(tmp_path / "order_products__train.parquet", index=False)
    manifest = execute_split(tmp_path, tmp_path / "first", 0.8)
    replay = execute_split(tmp_path, tmp_path / "replay", 0.8)
    assert replay == manifest
    assert manifest["sources"]["orders"]["sha256"] == file_sha256(tmp_path / "orders.parquet")
    assert all(len(value["sha256"]) == 64 for value in manifest["outputs"].values())
    monkeypatch.setattr(split, "verify_temporal_isolation", lambda *_: {"strictly_isolated": False})
    with pytest.raises(ValueError, match="audit failed"):
        execute_split(tmp_path, tmp_path / "rejected", 0.8)
    assert not (tmp_path / "rejected").exists()


def test_split_cli_reads_pipeline_ratio(tmp_path, sample_orders_and_products):
    from src.module1.split import main
    import json

    orders, products = sample_orders_and_products
    orders.to_parquet(tmp_path / "orders.parquet", index=False)
    products.to_parquet(tmp_path / "order_products__prior.parquet", index=False)
    products.iloc[:0].to_parquet(tmp_path / "order_products__train.parquet", index=False)
    config = tmp_path / "pipeline.yaml"
    config.write_text("temporal_split:\n  train_ratio: 0.6\n")
    output = tmp_path / "result"
    assert main(["--clean-dir", str(tmp_path), "--output-dir", str(output),
                 "--pipeline-config", str(config)]) == 0
    assert json.loads((output / "split_manifest.json").read_text())["split_ratio_target"] == 0.6
