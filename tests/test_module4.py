"""Module 4A/4B fixture tests for contracts, metrics, ALS and graph invariants."""

# ruff: noqa: E402 -- imports intentionally follow the optional Spark guard.

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession

from src.module4.als import ALSParams, fit_model, recommend_top_k
from src.module4.data import build_prior_interactions, validate_inputs
from src.module4.evaluation import ranking_metrics
from src.module4.graph import build_edges, build_metrics, build_vertices, validate_metrics
from src.module4.split import make_internal_holdout


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("module4-fixture-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    yield spark
    spark.stop()


@pytest.fixture
def tables(spark):
    orders = spark.createDataFrame(
        [
            (1, 1, "prior", 1),
            (2, 1, "prior", 2),
            (3, 1, "train", 3),
            (4, 2, "prior", 1),
            (5, 2, "train", 2),
            (6, 3, "train", 1),
        ],
        "order_id int, user_id int, eval_set string, order_number int",
    )
    prior = spark.createDataFrame(
        [(1, 1, 1, False), (1, 2, 2, False), (2, 1, 1, True), (2, 3, 2, False), (4, 2, 1, False), (4, 3, 2, False)],
        "order_id int, product_id int, add_to_cart_order int, reordered boolean",
    )
    train = spark.createDataFrame(
        [(3, 3, 1, True), (5, 1, 1, True), (6, 4, 1, False)],
        "order_id int, product_id int, add_to_cart_order int, reordered boolean",
    )
    products = spark.createDataFrame(
        [(1, "one", 1, 1), (2, "two", 1, 1), (3, "three", 1, 1), (4, "isolated", 1, 1)],
        "product_id int, product_name string, aisle_id int, department_id int",
    )
    return {"orders": orders, "prior": prior, "train": train, "products": products}


def test_validation_and_internal_split_are_leakage_safe(tables):
    report = validate_inputs(tables)
    assert report["counts"]["prior"] == 6
    assert report["official_train_used_for_fit"] is False
    prior = tables["prior"].join(tables["orders"], "order_id")
    prior = prior.select("order_id", "product_id", "user_id", "order_number")
    split = make_internal_holdout(prior)
    assert split["summary"]["eligible_users"] == 1
    assert split["summary"]["order_sets_disjoint"] is True
    assert split["validation_truth"].collect()[0].ground_truth == [1, 3]


def test_ranking_metrics_use_k_denominator_and_tie_break(spark):
    recommendations = spark.createDataFrame(
        [(1, 1, 0.9, 1), (1, 3, 0.8, 2), (2, 3, 0.9, 1)],
        "user_id int, product_id int, als_score double, als_rank int",
    )
    truth = spark.createDataFrame([(1, [1, 2]), (2, [3])], "user_id int, ground_truth array<int>")
    result = ranking_metrics(recommendations, truth, 2, catalog_size=4)
    assert result["precision_at_k"] == pytest.approx(0.5)
    assert result["recall_at_k"] == pytest.approx(0.75)
    assert result["map_at_k"] > 0
    assert result["catalog_coverage"] == 2


def test_als_fixture_fit_and_recommend(spark, tables):
    prior = tables["prior"].join(tables["orders"], "order_id")
    interactions = build_prior_interactions(prior.select("order_id", "product_id", "user_id"))
    model = fit_model(interactions, ALSParams(rank=2, reg_param=0.1, alpha=10, max_iter=2), seed=7)
    recommendations = recommend_top_k(model, interactions.select("user_id"), 2)
    assert recommendations.select("user_id").distinct().count() == 2
    assert recommendations.where("als_rank < 1 OR product_id IS NULL").count() == 0


def test_recommend_top_k_resolves_score_ties_before_cutting_membership(spark):
    class EqualScoreModel:
        itemFactors = spark.createDataFrame([(1, [0.0]), (2, [0.0]), (3, [0.0]), (4, [0.0])], "id int, features array<float>")

        def recommendForUserSubset(self, users, count):
            from pyspark.sql import functions as F

            ordered = spark.createDataFrame(
                [(1, float(product_id), product_id) for product_id in (4, 3, 2, 1)],
                "user_id int, rating double, product_id int",
            )
            recs = ordered.orderBy(F.desc("product_id")).limit(count).groupBy("user_id").agg(
                F.collect_list(F.struct("product_id", "rating")).alias("recommendations")
            )
            return users.select("user_id").join(recs, "user_id")

    result = recommend_top_k(EqualScoreModel(), spark.createDataFrame([(1,)], "user_id int"), 2)
    assert [row.product_id for row in result.orderBy("als_rank").collect()] == [1, 2]


def test_graph_invariants_and_pagerank_include_isolated_vertex(spark, tables):
    prior = tables["prior"].join(tables["orders"], "order_id")
    edges = build_edges(prior.select("order_id", "product_id"), min_edge_weight=1)
    vertices = build_vertices(prior.select("product_id"), tables["products"])
    metrics = build_metrics(vertices, edges, reset_probability=0.15, max_iter=3)
    report = validate_metrics(metrics, edges, vertices)
    assert report["edge_count"] == 3
    assert report["isolated_vertices"] == 1
    assert metrics.where("product_id = 4").select("pagerank").first()[0] is not None
