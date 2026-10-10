"""Module 4A/4B fixture tests for contracts, metrics, ALS and graph invariants."""

# ruff: noqa: E402 -- imports intentionally follow the optional Spark guard.

import pytest

pyspark = pytest.importorskip("pyspark")
from src.module4.als import ALSParams, fit_model, load_model, recommend_top_k, recommend_with_fallback, save_model
from src.module4.config import Config
from src.module4.data import build_prior_interactions, validate_inputs
from src.module4.evaluation import evaluate_at_k, personal_frequency, ranking_metrics
from src.module4.graph import build_edges, build_metrics, build_vertices, validate_edges, validate_metrics
from src.module4.split import make_internal_holdout
from src.module4.spark import session


@pytest.fixture(scope="module")
def spark():
    spark = session(Config(master="local[2]", shuffle_partitions=2))
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


def test_internal_holdout_preserves_exact_baskets_and_releases_owned_cache(spark):
    prior = spark.createDataFrame(
        [(1, 10, 1, 1), (1, 10, 1, 2), (1, 30, 3, 2), (1, 30, 3, 3),
         (1, 20, 2, 4), (2, 40, 1, 1), (3, 60, 2, 5), (3, 50, 1, 4)],
        "user_id int, order_id int, order_number int, product_id int",
    )
    cache_before = spark.sparkContext._jsc.getPersistentRDDs().size()
    split = make_internal_holdout(prior)
    assert split["summary"]["eligible_users"] == 2
    assert split["summary"]["excluded_users"] == 1
    assert split["summary"]["training_rows"] == 4
    assert split["summary"]["validation_rows"] == 3
    assert {row.order_id for row in split["training"].collect()} == {10, 20, 50}
    assert {row.order_id for row in split["validation"].collect()} == {30, 60}
    assert {row.user_id: row.ground_truth for row in split["validation_truth"].collect()} == {
        1: [2, 3], 3: [5],
    }
    assert spark.sparkContext._jsc.getPersistentRDDs().size() == cache_before
    with pytest.raises(ValueError, match="official train order leaked"):
        make_internal_holdout(prior, spark.createDataFrame([(30,)], "order_id int"))
    assert spark.sparkContext._jsc.getPersistentRDDs().size() == cache_before


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
                [(1, 1.0, product_id) for product_id in (4, 3, 2, 1)],
                "user_id int, rating double, product_id int",
            )
            recs = ordered.orderBy(F.desc("product_id")).limit(count).groupBy("user_id").agg(
                F.collect_list(F.struct("product_id", "rating")).alias("recommendations")
            )
            return users.select("user_id").join(recs, "user_id")

    cache_before = spark.sparkContext._jsc.getPersistentRDDs().size()
    result = recommend_top_k(EqualScoreModel(), spark.createDataFrame([(1,)], "user_id int"), 2)
    assert [row.product_id for row in result.orderBy("als_rank").collect()] == [1, 2]
    result.unpersist(blocking=True)
    assert spark.sparkContext._jsc.getPersistentRDDs().size() == cache_before


def test_graph_invariants_and_pagerank_include_isolated_vertex(spark, tables):
    prior = tables["prior"].join(tables["orders"], "order_id")
    edges = build_edges(prior.select("order_id", "product_id"), min_edge_weight=1)
    vertices = build_vertices(prior.select("product_id"), tables["products"])
    metrics = build_metrics(vertices, edges, reset_probability=0.15, max_iter=3)
    report = validate_metrics(metrics, edges, vertices)
    assert report["edge_count"] == 3
    assert report["isolated_vertices"] == 1
    assert metrics.where("product_id = 4").select("pagerank").first()[0] is not None


def test_split_rejects_distinct_orders_with_equal_chronology_and_official_leakage(spark, tables):
    prior = tables["prior"].join(tables["orders"], "order_id")
    from pyspark.sql import functions as F

    tied = prior.withColumn("order_number", F.lit(1))
    with pytest.raises(ValueError, match="chronology"):
        make_internal_holdout(tied)
    with pytest.raises(ValueError, match="official train"):
        make_internal_holdout(prior, spark.createDataFrame([(2,)], "order_id int"))


@pytest.mark.parametrize("value", [None, -1.0, 0.0, 1.5, float("nan"), float("inf")])
def test_graph_rejects_invalid_weights(spark, value):
    edges = spark.createDataFrame([(1, 2, value)], "src int, dst int, weight double")
    with pytest.raises(ValueError):
        validate_edges(edges)


def test_graphframes_empty_edges_preserve_all_isolated_vertices(spark):
    vertices = spark.createDataFrame([(1,), (2,)], "id int")
    edges = spark.createDataFrame([], "src int, dst int, weight long")
    metrics = build_metrics(vertices, edges, max_iter=2)
    report = validate_metrics(metrics, edges, vertices)
    assert report["isolated_vertices"] == 2
    assert report["weighted_degree_sum"] == 0
    assert report["pagerank_finite"]


def test_metrics_short_missing_empty_and_extra_users_share_correct_denominators(spark):
    predictions = spark.createDataFrame([(1, 1, 0.8, 1), (999, 9, 0.9, 1)],
                                       "user_id int, product_id int, als_score double, als_rank int")
    truth = spark.createDataFrame([(1, [1, 2, 3, 4, 5]), (2, [2]), (3, [])],
                                 "user_id int, ground_truth array<int>")
    result = evaluate_at_k(predictions, truth, (5, 10, 20), catalog_size=9)
    assert result["5"]["precision_at_k"] == pytest.approx(0.1)
    assert result["5"]["recall_at_k"] == pytest.approx(0.1)
    assert result["5"]["map_at_k"] == pytest.approx(0.1)
    assert result["5"]["ndcg_at_k"] == pytest.approx(0.3391602052736161 / 2)
    assert result["5"]["user_coverage"] == pytest.approx(0.5)
    assert result["5"]["catalog_coverage"] == 1
    assert result["5"]["users_with_no_recommendations"] == 1
    empty = spark.createDataFrame([], "user_id int, ground_truth array<int>")
    assert ranking_metrics(predictions, empty)["status"] == "not_applicable_empty_cohort"


@pytest.mark.parametrize("duplicate", ["rank", "product"])
def test_metrics_reject_duplicate_predictions(spark, duplicate):
    rows = [(1, 1, 1)] + ([(1, 2, 1)] if duplicate == "rank" else [(1, 1, 2)])
    predictions = spark.createDataFrame(rows, "user_id int, product_id int, als_rank int")
    truth = spark.createDataFrame([(1, [1])], "user_id int, ground_truth array<int>")
    with pytest.raises(ValueError, match="duplicate"):
        ranking_metrics(predictions, truth)


def test_als_save_reload_seed_repeatability_and_cold_user_fallback(spark, tables, tmp_path):
    from pyspark.sql import functions as F

    prior = tables["prior"].join(tables["orders"], "order_id")
    interactions = build_prior_interactions(prior)
    params = ALSParams(rank=2, max_iter=2)
    model = fit_model(interactions, params, seed=7, blocks=2)
    path = str(tmp_path / "model")
    save_model(model, path)
    restored = load_model(path)
    users = spark.createDataFrame([(1,), (2,), (99,)], "user_id int")
    # Small saved factor files must keep a distributed block cross join. A
    # broadcast nested-loop join caused heap exhaustion on the full-data run.
    plan = restored.recommendForUserSubset(users, 2)._jdf.queryExecution().executedPlan().toString()
    assert "CartesianProduct" in plan
    assert "BroadcastNestedLoopJoin" not in plan
    expected = recommend_top_k(model, users, 2).orderBy("user_id", "als_rank").collect()
    actual = recommend_top_k(restored, users, 2).orderBy("user_id", "als_rank").collect()
    repeated = fit_model(interactions, params, seed=7, blocks=2)
    replay = recommend_top_k(repeated, users, 2).orderBy("user_id", "als_rank").collect()
    assert [(r.user_id, r.product_id, r.als_rank) for r in actual] == [
        (r.user_id, r.product_id, r.als_rank) for r in expected
    ]
    assert [r.als_score for r in actual] == pytest.approx([r.als_score for r in expected], abs=1e-6)
    assert [(r.user_id, r.product_id) for r in replay] == [(r.user_id, r.product_id) for r in expected]
    fallback = recommend_with_fallback(restored, users, interactions, 2)
    assert fallback.where(F.col("user_id") == 99).count() == 2
    personal = personal_frequency(interactions, users, 3)
    assert personal.groupBy("user_id", "product_id").count().where("count > 1").count() == 0
    assert personal.where("user_id = 99").count() == 3
    for fitted in (model, restored, repeated):
        fitted.userFactors.unpersist()
        fitted.itemFactors.unpersist()


@pytest.mark.parametrize("corruption", ["duplicate_fact", "wrong_eval_set", "orphan_product", "chronology_tie"])
def test_data_contract_rejects_corruption_before_training(tables, corruption):
    from pyspark.sql import functions as F

    invalid = dict(tables)
    if corruption == "duplicate_fact":
        invalid["prior"] = invalid["prior"].unionByName(invalid["prior"].limit(1))
    elif corruption == "wrong_eval_set":
        invalid["orders"] = invalid["orders"].withColumn(
            "eval_set", F.when(F.col("order_id") == 4, "train").otherwise(F.col("eval_set")))
    elif corruption == "orphan_product":
        invalid["products"] = invalid["products"].where("product_id != 2")
    else:
        invalid["orders"] = invalid["orders"].withColumn(
            "order_number", F.when(F.col("order_id") == 2, 1).otherwise(F.col("order_number")))
    with pytest.raises(ValueError):
        validate_inputs(invalid)


def test_staged_receipt_checks_real_parquet_hash_and_count(spark, tmp_path):
    import json
    from src.module2.fixtures import create_fixture
    from src.module4.config import Config
    from src.module4.data import read_inputs, validate_source_receipts

    root = create_fixture(tmp_path / "source")
    config = Config(root=root.as_uri())
    frames = read_inputs(spark, config)
    receipt = validate_source_receipts(spark, config, frames)
    assert receipt["inputs"]["order_products_prior"]["rows"] == 24
    path = root / "curated/_sources.json"
    source = json.loads(path.read_text())
    source["orders"]["rows"] += 1
    path.write_text(json.dumps(source))
    with pytest.raises(ValueError, match="row count"):
        validate_source_receipts(spark, config, frames)
    source["orders"]["rows"] -= 1
    source["orders"]["sha256"] = "0" * 64
    path.write_text(json.dumps(source))
    with pytest.raises(ValueError, match="SHA-256"):
        validate_source_receipts(spark, config, frames)


def test_graphframes_failure_is_not_replaced_with_fallback(spark, monkeypatch):
    from src.module4.graph import pagerank
    import graphframes

    def broken_backend(*args, **kwargs):
        raise RuntimeError("injected missing GraphFrames JAR")

    monkeypatch.setattr(graphframes, "GraphFrame", broken_backend)
    with pytest.raises(RuntimeError, match="GraphFrames JAR"):
        pagerank(spark.createDataFrame([(1,)], "id int"),
                 spark.createDataFrame([], "src int, dst int"))
