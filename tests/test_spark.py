from datetime import date
import json
import pytest
from pyspark.sql import functions as F
from src.module2.fixtures import create_fixture
from src.module2.config import Config
from src.module2.spark import session
from src.module2.data import read_tables, validate
from src.module2.analytics import analyze
from src.module2.benchmarks import (
    join_query,
    trial,
    verify_join,
    scan_query,
    partition_predicate,
    verify_pruning,
)

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def context(tmp_path_factory):
    directory = tmp_path_factory.mktemp("spark")
    root = create_fixture(directory / "fixture")
    config = Config(root=root.as_uri(), shuffle_partitions=2)
    spark = session(config, directory / "events")
    yield spark, config, read_tables(spark, config), root
    spark.stop()


def test_handoff_and_analytics(context):
    spark, config, tables, root = context
    validation = validate(spark, config, tables)
    assert validation["counts"] == {
        "orders": 16,
        "prior": 24,
        "train": 12,
        "products": 6,
        "aisles": 3,
        "departments": 2,
        "interactions": 36,
        "users": 4,
    }
    summary, outputs = analyze(spark, tables, validation["counts"], min_support=1)
    assert summary["purchase_behavior"] == {"purchases": 24, "reorder_rate": 0.5, "average_basket_size": 3.0}
    assert outputs["user_features"].count() == 4
    assert {r.order_count for r in outputs["user_features"].collect()} == {2}
    trends = outputs["department_daily_trends"].collect()
    assert sum(r.rolling_7day_purchase_count for r in trends if r.synthetic_date == "2024-01-04") == 24
    assert not any(r.synthetic_date == "2024-02-01" for r in trends)  # train excluded


def test_both_physical_strategies_and_pruning(context):
    spark, config, tables, root = context
    smj = trial(
        spark, join_query(tables, "sort_merge"), "test/smj", lambda p: verify_join(p, "SortMergeJoin")
    )
    bhj = trial(
        spark, join_query(tables, "broadcast_hash"), "test/bhj", lambda p: verify_join(p, "BroadcastHashJoin")
    )
    assert smj["checksum"] == bhj["checksum"]
    events = tables["interactions"]
    full = trial(spark, scan_query(events), "test/full", lambda p: verify_pruning(p, False))
    subset = trial(
        spark,
        scan_query(events.where(partition_predicate(date(2024, 1, 1), date(2024, 1, 7)))),
        "test/pruned",
        lambda p: verify_pruning(p, True),
    )
    assert full["result"][0]["events"] == 36
    assert subset["result"][0]["events"] == 24


def test_validation_rejects_corruption(context):
    spark, config, tables, root = context
    # Preserve count, but duplicate one dimension key and lose another.
    corrupt = dict(tables)
    corrupt["products"] = tables["products"].withColumn(
        "product_id", F.when(F.col("product_id") == 6, 5).otherwise(F.col("product_id"))
    )
    with pytest.raises(ValueError, match="duplicate"):
        validate(spark, config, corrupt)
    receipt_path = root / "curated/interactions/_handoff.json"
    original = receipt_path.read_text()
    receipt = json.loads(original)
    receipt["source_rows"] += 1
    try:
        receipt_path.write_text(json.dumps(receipt))
        with pytest.raises(ValueError, match="count mismatch"):
            validate(spark, config, tables)
    finally:
        receipt_path.write_text(original)
