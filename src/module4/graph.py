"""Co-purchase graph construction, invariants and centrality metrics."""

from __future__ import annotations


def build_edges(prior_enriched, min_edge_weight: int = 2):
    """Create canonical undirected co-purchase edges from prior baskets."""
    from pyspark.sql import functions as F

    if min_edge_weight < 1:
        raise ValueError("min_edge_weight must be positive")
    required = {"order_id", "product_id"}
    missing = required.difference(prior_enriched.columns)
    if missing:
        raise ValueError(f"prior_enriched: missing required columns {sorted(missing)}")

    basket = prior_enriched.select("order_id", "product_id").distinct()
    left = basket.alias("left")
    right = basket.alias("right")
    return (
        left.join(
            right,
            (F.col("left.order_id") == F.col("right.order_id"))
            & (F.col("left.product_id") < F.col("right.product_id")),
            "inner",
        )
        .select(
            F.col("left.product_id").alias("src"),
            F.col("right.product_id").alias("dst"),
        )
        .groupBy("src", "dst")
        .agg(F.count(F.lit(1)).cast("long").alias("weight"))
        .where(F.col("weight") >= min_edge_weight)
    )


def build_vertices(prior_enriched, products=None):
    """Return active prior products, or the full product catalog when supplied."""
    from pyspark.sql import functions as F

    active = prior_enriched.select("product_id").distinct()
    if products is None:
        return active.select(F.col("product_id").alias("id"))
    catalog = products.select(F.col("product_id").alias("id")).distinct()
    return catalog


def directed_edges(edges):
    """Expand canonical edges in both directions for GraphFrames algorithms."""
    from pyspark.sql import functions as F

    required = {"src", "dst"}
    missing = required.difference(edges.columns)
    if missing:
        raise ValueError(f"edges: missing required columns {sorted(missing)}")
    forward = edges.select("src", "dst", *(["weight"] if "weight" in edges.columns else []))
    reverse = edges.select(
        F.col("dst").alias("src"),
        F.col("src").alias("dst"),
        *(["weight"] if "weight" in edges.columns else []),
    )
    return forward.unionByName(reverse)


def weighted_degree(edges, vertices=None):
    """Compute neighbor count and weighted degree from undirected edges."""
    from pyspark.sql import functions as F

    incident = edges.select(
        F.col("src").alias("product_id"), "dst", "weight"
    ).unionByName(
        edges.select(F.col("dst").alias("product_id"), F.col("src").alias("dst"), "weight")
    )
    metrics = incident.groupBy("product_id").agg(
        F.countDistinct("dst").alias("neighbor_count"),
        F.sum("weight").cast("long").alias("weighted_degree"),
    )
    if vertices is None:
        return metrics
    return (
        vertices.select("id")
        .withColumnRenamed("id", "product_id")
        .join(metrics, "product_id", "left")
        .fillna({"neighbor_count": 0, "weighted_degree": 0})
    )


def pagerank(
    vertices,
    edges,
    reset_probability: float = 0.15,
    max_iter: int = 10,
    backend: str = "graphframes",
):
    """Run topology-only PageRank with an explicitly selected implementation."""
    from pyspark.sql import functions as F

    if not 0.0 < reset_probability < 1.0:
        raise ValueError("reset_probability must be between 0 and 1")
    if max_iter < 1:
        raise ValueError("max_iter must be positive")
    vertex_frame = vertices.select("id").distinct()
    edge_frame = edges.select("src", "dst").distinct()
    if backend == "graphframes":
        from graphframes import GraphFrame

        graph = GraphFrame(vertex_frame, edge_frame)
        return graph.pageRank(resetProbability=reset_probability, maxIter=max_iter).vertices.select(
            "id", "pagerank"
        )
    if backend != "dataframe":
        raise ValueError("backend must be graphframes or dataframe")

    # PageRank fallback using the same unweighted directed topology.
    count = vertex_frame.count()
    if count == 0:
        return vertex_frame.withColumn("pagerank", F.lit(0.0))
    out_degree = edge_frame.groupBy("src").count().withColumnRenamed("count", "out_degree")
    rank = vertex_frame.withColumn("pagerank", F.lit(1.0 / count))
    dangling = vertex_frame.join(out_degree, vertex_frame.id == out_degree.src, "left").where(
        F.col("out_degree").isNull()
    ).select("id")
    for _ in range(max_iter):
        dangling_mass = rank.join(dangling, "id", "inner").agg(F.sum("pagerank")).first()[0] or 0.0
        contribution = (
            edge_frame.join(rank.withColumnRenamed("id", "rank_src"), F.col("src") == F.col("rank_src"))
            .join(out_degree, "src")
            .select(
                F.col("dst").alias("id"),
                (F.col("pagerank") / F.col("out_degree")).alias("contribution"),
            )
            .groupBy("id")
            .agg(F.sum("contribution").alias("incoming"))
        )
        rank = (
            vertex_frame.join(contribution, "id", "left")
            .fillna({"incoming": 0.0})
            .withColumn(
                "pagerank",
                F.lit(reset_probability / count)
                + F.lit(1.0 - reset_probability)
                * (F.col("incoming") + F.lit(dangling_mass / count)),
            )
            .select("id", "pagerank")
        )
    return rank


def build_metrics(vertices, edges, reset_probability=0.15, max_iter=10, backend="graphframes"):
    """Join graph topology metrics with PageRank for every graph vertex."""
    topology = weighted_degree(edges, vertices)
    ranks = pagerank(
        vertices, directed_edges(edges), reset_probability, max_iter, backend
    ).withColumnRenamed("id", "product_id")
    return topology.join(ranks, "product_id", "left")


def validate_edges(edges) -> dict:
    """Run canonical-edge invariants and return compact counts."""
    from pyspark.sql import functions as F

    if not {"src", "dst", "weight"}.issubset(edges.columns):
        raise ValueError("edges must contain src, dst and weight")
    weight_value = F.col("weight").cast("double")
    if edges.where(
        (F.col("src") >= F.col("dst"))
        | (F.col("src") <= 0)
        | (F.col("dst") <= 0)
        | F.col("src").isNull()
        | F.col("dst").isNull()
        | F.col("weight").isNull()
        | (weight_value <= 0)
        | F.isnan(weight_value)
        | (F.abs(weight_value) == float("inf"))
        | (weight_value != weight_value.cast("long").cast("double"))
    ).limit(1).count():
        raise ValueError("edges contain invalid endpoints or non-positive/non-integer weights")
    if edges.groupBy("src", "dst").count().where("count > 1").limit(1).count():
        raise ValueError("edges contain duplicate canonical pairs")
    row = edges.agg(
        F.count("*").alias("edge_count"),
        F.sum("weight").alias("total_weight"),
    ).first()
    return {"edge_count": int(row.edge_count), "total_weight": int(row.total_weight or 0)}


def validate_metrics(metrics, edges, vertices=None) -> dict:
    """Validate degree conservation, vertex uniqueness and finite PageRank."""
    from pyspark.sql import functions as F

    required = {"product_id", "neighbor_count", "weighted_degree", "pagerank"}
    missing = required.difference(metrics.columns)
    if missing:
        raise ValueError(f"graph metrics: missing required columns {sorted(missing)}")
    validate_edges(edges)
    if metrics.groupBy("product_id").count().where("count > 1").limit(1).count():
        raise ValueError("graph metrics contain duplicate products")
    pagerank_value = F.col("pagerank").cast("double")
    if metrics.where(
        F.col("product_id").isNull()
        | (F.col("product_id") <= 0)
        | F.col("neighbor_count").isNull()
        | (F.col("neighbor_count") < 0)
        | (F.col("neighbor_count") != F.col("neighbor_count").cast("long"))
        | F.col("weighted_degree").isNull()
        | (F.col("weighted_degree") < 0)
        | (F.col("weighted_degree") != F.col("weighted_degree").cast("long"))
        | pagerank_value.isNull()
        | F.isnan(pagerank_value)
        | (F.abs(pagerank_value) == float("inf"))
        | (pagerank_value < 0)
    ).limit(1).count():
        raise ValueError("graph metrics contain invalid values")
    degree_total = metrics.agg(F.sum("weighted_degree")).first()[0] or 0
    edge_total = edges.agg(F.sum("weight")).first()[0] or 0
    if int(degree_total) != 2 * int(edge_total):
        raise ValueError("weighted-degree conservation failed")
    vertex_count = metrics.select("product_id").distinct().count()
    if metrics.where(F.col("neighbor_count") > max(0, vertex_count - 1)).limit(1).count():
        raise ValueError("neighbor_count exceeds the number of other vertices")
    if vertices is not None:
        expected_vertices = vertices.select(F.col("id").alias("product_id")).distinct()
        actual_vertices = metrics.select("product_id").distinct()
        if expected_vertices.join(actual_vertices, "product_id", "left_anti").limit(1).count() or actual_vertices.join(
            expected_vertices, "product_id", "left_anti"
        ).limit(1).count():
            raise ValueError("graph metrics product set does not match the vertex set")
        allowed = expected_vertices.select(F.col("product_id").alias("allowed_product_id"))
        endpoints = edges.select(F.col("src").alias("product_id")).unionByName(
            edges.select(F.col("dst").alias("product_id"))
        ).distinct()
        if endpoints.join(
            allowed, endpoints.product_id == allowed.allowed_product_id, "left_anti"
        ).limit(1).count():
            raise ValueError("graph edge references a product outside the vertex set")
    return {
        "vertex_count": metrics.select("product_id").distinct().count(),
        "edge_count": edges.count(),
        "total_edge_weight": int(edge_total),
        "weighted_degree_sum": int(degree_total),
        "isolated_vertices": metrics.where("neighbor_count = 0").count(),
        "pagerank_finite": True,
        "pagerank_backend_validation": "passed",
    }
