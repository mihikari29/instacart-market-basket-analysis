"""Exercise the actual Module 2 producer and bounded Module 4 Parquet reader."""

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession
from src.module2.analytics import metrics, tag_user_features
from src.module2.config import Config
from src.module4.data import read_user_features


def test_module2_features_roundtrip_to_descriptive_reader_without_train_leakage(tmp_path):
    spark = SparkSession.builder.master("local[1]").appName("feature-handoff").getOrCreate()
    try:
        orders = spark.createDataFrame([(1, 7, 0, 8), (2, 7, 1, 9)],
                                      "order_id int, user_id int, order_dow int, order_hour_of_day int")
        prior = spark.createDataFrame([(1, 10, False), (1, 11, True)], "order_id int, product_id int, reordered boolean")
        train = spark.createDataFrame([(2, 99, False)], "order_id int, product_id int, reordered boolean")
        products = spark.createDataFrame([(10, 1), (11, 1), (99, 2)], "product_id int, department_id int")
        _, _, _, features = metrics({"orders": orders, "prior": prior, "train": train, "products": products})
        config = Config(root=tmp_path.as_uri())
        tag_user_features(features, "producer-run", "2026-10-18T12:00:00+00:00").write.parquet(config.features())
        result = read_user_features(spark, config, 7)
        assert result["source"]["module2_run_id"] == "producer-run"
        assert result["features"]["purchase_count"] == 2
        assert result["features"]["order_count"] == 1
        assert result["features"]["reorder_rate"] == 0.5
        assert read_user_features(spark, config, 8)["status"] == "no_prior_activity"
    finally:
        spark.stop()
