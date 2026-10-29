"""Exact graph construction and validation contracts on bounded Spark fixtures."""

# ruff: noqa: E402 -- Spark is optional in the host environment.

from collections import Counter
from itertools import combinations

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import functions as F

from src.module4.config import Config, session
from src.module4.graph import build_edges, validate_metrics, weighted_degree


@pytest.fixture(scope="module")
def spark():
    runtime = session(Config(master="local[2]", shuffle_partitions=2))
    yield runtime
    runtime.stop()


@pytest.mark.parametrize("threshold", [1, 2, 3])
def test_edges_and_degrees_preserve_duplicate_singleton_and_skewed_baskets(spark, threshold):
    baskets = {100001: list(range(1, 34)), 100002: list(range(1, 7)), 100003: [1]}
    facts = [(order, product) for order, products in baskets.items() for product in products]
    facts.extend([(100001, 1), (100001, 1), (100002, 2)])
    prior = spark.createDataFrame(facts, "order_id int, product_id int")
    counts = Counter(pair for products in baskets.values() for pair in combinations(products, 2))
    expected_rows = sorted((src, dst, weight) for (src, dst), weight in counts.items() if weight >= threshold)
    edges = build_edges(prior, threshold).cache()
    try:
        assert edges.schema.simpleString() == "struct<src:int,dst:int,weight:bigint>"
        expected = spark.createDataFrame(expected_rows, edges.schema)
        differences = edges.exceptAll(expected).unionByName(expected.exceptAll(edges))
        assert differences.count() == 0
        assert [tuple(row) for row in edges.orderBy("src", "dst").collect()] == expected_rows
        vertices = spark.range(1, 35).select(F.col("id").cast("int").alias("id"))
        degrees = weighted_degree(edges, vertices)
        expected_degrees = {product: [0, 0] for product in range(1, 35)}
        for src, dst, weight in expected_rows:
            for endpoint in (src, dst):
                expected_degrees[endpoint][0] += 1
                expected_degrees[endpoint][1] += weight
        assert {
            row.product_id: [row.neighbor_count, row.weighted_degree] for row in degrees.collect()
        } == expected_degrees
    finally:
        edges.unpersist(blocking=True)


@pytest.mark.parametrize("case", ["empty", "isolated", "connected"])
def test_graph_validation_exact_summary_and_caller_cache_ownership(spark, case):
    metric_rows = {
        "empty": [],
        "isolated": [(1, 0, 0, 1.0)],
        "connected": [(1, 1, 3, 1.0), (2, 1, 3, 1.0), (3, 0, 0, 0.15)],
    }[case]
    metrics = spark.createDataFrame(
        metric_rows, "product_id int, neighbor_count long, weighted_degree long, pagerank double"
    ).cache()
    edges = spark.createDataFrame([(1, 2, 3)] if case == "connected" else [], "src int, dst int, weight long").cache()
    vertices = metrics.select(F.col("product_id").alias("id"))
    expected = {
        "vertex_count": len(metric_rows), "edge_count": int(case == "connected"),
        "total_edge_weight": 3 if case == "connected" else 0,
        "weighted_degree_sum": 6 if case == "connected" else 0,
        "isolated_vertices": int(case != "empty"), "pagerank_finite": True,
        "pagerank_backend_validation": "passed",
    }
    try:
        metrics.count()
        edges.count()
        persistent = spark.sparkContext._jsc.getPersistentRDDs().size()
        assert validate_metrics(metrics, edges, vertices) == expected
        assert validate_metrics(metrics, edges) == expected
        assert spark.sparkContext._jsc.getPersistentRDDs().size() == persistent
        assert [tuple(row) for row in metrics.orderBy("product_id").collect()] == metric_rows
    finally:
        metrics.unpersist(blocking=True)
        edges.unpersist(blocking=True)


@pytest.mark.parametrize("case,diagnostic", [
    ("duplicate", "graph metrics contain duplicate products"),
    ("nonfinite", "graph metrics contain invalid values"),
    ("conservation", "weighted-degree conservation failed"),
    ("neighbors", "neighbor_count exceeds the number of other vertices"),
    ("vertices", "graph metrics product set does not match the vertex set"),
    ("endpoints", "graph edge references a product outside the vertex set"),
])
def test_graph_validation_preserves_failure_diagnostics_and_owned_state(spark, case, diagnostic):
    rows = [(1, 1, 3, 1.0), (2, 1, 3, 1.0)]
    if case == "duplicate":
        rows.append(rows[0])
    elif case == "nonfinite":
        rows[0] = (1, 1, 3, float("nan"))
    elif case == "conservation":
        rows[0] = (1, 1, 4, 1.0)
    elif case == "neighbors":
        rows[0] = (1, 2, 3, 1.0)
    metrics = spark.createDataFrame(
        rows, "product_id int, neighbor_count long, weighted_degree long, pagerank double"
    ).cache()
    edges = spark.createDataFrame([(1, 3 if case == "endpoints" else 2, 3)], "src int, dst int, weight long")
    vertices = spark.createDataFrame([(1,), (2,), (3,)] if case == "vertices" else [(1,), (2,)], "id int")
    try:
        metrics.count()
        persistent = spark.sparkContext._jsc.getPersistentRDDs().size()
        with pytest.raises(ValueError) as error:
            validate_metrics(metrics, edges, vertices)
        assert str(error.value) == diagnostic
        assert spark.sparkContext._jsc.getPersistentRDDs().size() == persistent
    finally:
        metrics.unpersist(blocking=True)


def test_edge_builder_empty_inputs_and_invalid_arguments(spark):
    empty = spark.createDataFrame([], "order_id int, product_id int")
    assert build_edges(empty, 1).collect() == []
    with pytest.raises(ValueError, match="min_edge_weight must be positive"):
        build_edges(empty, 0)
    with pytest.raises(ValueError, match="missing required columns.*product_id"):
        build_edges(empty.select("order_id"), 1)
