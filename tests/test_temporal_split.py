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

    manifest = execute_split(clean_dir, output_dir, split_ratio=0.8)

    assert manifest["strategy"] == "per_user_temporal_split"
    assert manifest["isolation_audit"]["strictly_isolated"] is True
    assert (output_dir / "orders_train.parquet").exists()
    assert (output_dir / "orders_test.parquet").exists()
    assert (output_dir / "order_products_train.parquet").exists()
    assert (output_dir / "order_products_test.parquet").exists()
    assert (output_dir / "split_manifest.json").exists()
