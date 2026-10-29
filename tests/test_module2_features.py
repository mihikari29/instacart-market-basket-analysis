# ruff: noqa: E402 -- imports intentionally follow pytest.importorskip.

import pytest

pyspark = pytest.importorskip("pyspark")

from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.module2.analytics import (
    compute_product_features,
    compute_user_behavior_features,
    compute_user_product_features,
    tag_features,
)

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-module2-features")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def _build_test_data(spark):
    orders_schema = StructType([
        StructField("order_id", IntegerType(), False),
        StructField("user_id", IntegerType(), False),
        StructField("eval_set", StringType(), False),
        StructField("order_number", IntegerType(), False),
        StructField("order_dow", IntegerType(), False),
        StructField("order_hour_of_day", IntegerType(), False),
        StructField("days_since_prior_order", DoubleType(), True),
    ])
    orders_data = [
        (1, 101, "prior", 1, 2, 10, 0.0),
        (2, 101, "prior", 2, 3, 11, 7.0),
        (3, 101, "prior", 3, 4, 12, 14.0),
        (4, 101, "train", 4, 5, 13, 7.0),  # Holdout test order - must not contaminate prior
        (5, 102, "prior", 1, 1, 9, 0.0),
        (6, 102, "prior", 2, 2, 10, 5.0),
    ]
    orders_df = spark.createDataFrame(orders_data, orders_schema)

    prior_schema = StructType([
        StructField("order_id", IntegerType(), False),
        StructField("product_id", IntegerType(), False),
        StructField("add_to_cart_order", IntegerType(), False),
        StructField("reordered", IntegerType(), False),
    ])
    prior_data = [
        (1, 1, 1, 0),
        (1, 2, 4, 0),
        (2, 1, 2, 1),
        (2, 3, 1, 0),
        (3, 1, 1, 1),
        (3, 2, 2, 1),
        (5, 2, 1, 0),
        (6, 3, 2, 0),
    ]
    prior_df = spark.createDataFrame(prior_data, prior_schema)

    products_schema = StructType([
        StructField("product_id", IntegerType(), False),
        StructField("product_name", StringType(), False),
        StructField("aisle_id", IntegerType(), False),
        StructField("department_id", IntegerType(), False),
    ])
    products_data = [
        (1, "Organic Banana", 1, 1),
        (2, "Whole Milk", 2, 2),
        (3, "Sourdough Bread", 3, 3),
    ]
    products_df = spark.createDataFrame(products_data, products_schema)

    enriched = (
        prior_df
        .join(orders_df.select("order_id", "user_id", "order_number", "order_dow", "order_hour_of_day", "days_since_prior_order"), "order_id")
        .join(F.broadcast(products_df), "product_id")
    )
    return orders_df, prior_df, products_df, enriched


def test_compute_product_features(spark):
    orders_df, _, _, enriched = _build_test_data(spark)
    pf = compute_product_features(enriched)
    rows = {r.product_id: r for r in pf.collect()}

    p1 = rows[1]
    assert p1.purchase_count == 3
    assert p1.unique_users == 1
    assert p1.reorder_count == 2
    assert pytest.approx(p1.reorder_rate, 0.01) == 2 / 3
    assert pytest.approx(p1.avg_cart_position, 0.01) == (1 + 2 + 1) / 3
    assert p1.cart_priority_ratio == 1.0

    p2 = rows[2]
    assert p2.purchase_count == 3
    assert p2.unique_users == 2
    assert pytest.approx(p2.cart_priority_ratio, 0.01) == 2 / 3


def test_compute_user_behavior_features(spark):
    orders_df, _, _, enriched = _build_test_data(spark)
    ub = compute_user_behavior_features(enriched, orders_df)
    rows = {r.user_id: r for r in ub.collect()}

    u101 = rows[101]
    assert u101.total_prior_orders == 3
    assert u101.max_order_number == 3
    assert pytest.approx(u101.order_interval_mean, 0.01) == 7.0
    assert pytest.approx(u101.lifetime_basket_size_avg, 0.01) == 2.0
    assert pytest.approx(u101.basket_growth_slope, 0.01) == 0.0

    u102 = rows[102]
    assert u102.total_prior_orders == 2
    assert u102.max_order_number == 2
    assert pytest.approx(u102.order_interval_mean, 0.01) == 2.5


def test_compute_user_product_features_and_streaks(spark):
    orders_df, _, _, enriched = _build_test_data(spark)
    ub = compute_user_behavior_features(enriched, orders_df)
    up = compute_user_product_features(enriched, ub)
    records = {(r.user_id, r.product_id): r for r in up.collect()}

    u101_p1 = records[(101, 1)]
    assert u101_p1.user_product_purchases == 3
    assert u101_p1.orders_since_last_purchase == 0
    assert u101_p1.purchase_streak_active == 1

    u101_p2 = records[(101, 2)]
    assert u101_p2.user_product_purchases == 2
    assert u101_p2.orders_since_last_purchase == 0
    assert u101_p2.purchase_streak_active == 1

    u101_p3 = records[(101, 3)]
    assert u101_p3.user_product_purchases == 1
    assert u101_p3.orders_since_last_purchase == 1
    assert u101_p3.purchase_streak_active == 0


def test_tag_features_lineage_metadata(spark):
    _, _, _, enriched = _build_test_data(spark)
    pf = compute_product_features(enriched)
    now_utc = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z") + "+00:00"
    tagged = tag_features(pf, "test-run-123", now_utc, scope="prior_only")

    assert "module2_run_id" in tagged.columns
    assert "feature_scope" in tagged.columns
    assert "generated_at_utc" in tagged.columns

    first = tagged.first()
    assert first.module2_run_id == "test-run-123"
    assert first.feature_scope == "prior_only"
    assert first.generated_at_utc == now_utc
