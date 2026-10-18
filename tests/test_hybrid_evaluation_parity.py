"""Spark parity with the serving ranker, including zero-score endpoint ties."""

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession

from src.module4.evaluation import offline_hybrid_recommendations
from src.module4.hybrid.config import HybridRankingConfig, TrendSnapshot, VerifiedSourceContext
from src.module4.hybrid.ranking import rank_hybrid_recommendations


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").appName("hybrid-parity").getOrCreate()
    yield session
    session.stop()


@pytest.mark.parametrize("weight", [0.0, 0.5, 1.0])
@pytest.mark.parametrize("scores", [[(100, 1.0)], [(100, 0.0), (101, 0.0)], [(100, 0.2), (101, 0.9)]])
def test_offline_ranking_matches_serving_membership_and_order(spark, weight, scores):
    als_rows = [dict(user_id=1, product_id=product, als_score=score, als_rank=rank)
                for rank, (product, score) in enumerate(scores, 1)]
    trends = [dict(product_id=1, trend_score=0.0, trend_rank=1), dict(product_id=100, trend_score=0.0, trend_rank=2)]
    actual = offline_hybrid_recommendations(
        spark.createDataFrame(als_rows), spark.createDataFrame([(1,), (2,)], "user_id long"),
        spark.createDataFrame(trends), lambda_weight=weight,
    ).orderBy("user_id", "als_rank").collect()
    for user in (1, 2):
        result = rank_hybrid_recommendations(
            user, als_rows if user == 1 else [], TrendSnapshot(available=True, rows=tuple(trends)),
            {1: "Trend", 100: "ALS", 101: "Other"},
            VerifiedSourceContext(als_available=True, graph_available=True),
            HybridRankingConfig(lambda_weight=weight),
        )
        assert [row.product_id for row in actual if row.user_id == user] == [row.product_id for row in result.recommendations]
