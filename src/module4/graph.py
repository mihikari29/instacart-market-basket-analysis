"""Co-purchase graph preparation and weighted-degree metrics."""

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
    """Return products with prior activity; optionally retain isolated catalog items."""
    from pyspark.sql import functions as F

    active = prior_enriched.select("product_id").distinct()
    if products is None:
        return active.select(F.col("product_id").alias("id"))
    catalog = products.select(F.col("product_id").alias("id"))
    return catalog.join(active.select(F.col("product_id").alias("id")), "id", "left")


def directed_edges(edges):
    """Expand canonical edges in both directions for GraphFrames algorithms."""
    from pyspark.sql import functions as F

    forward = edges.select("src", "dst", "weight")
    reverse = edges.select(F.col("dst").alias("src"), F.col("src").alias("dst"), "weight")
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
        vertices.select(F.col("id").alias("product_id"))
        .join(metrics, "product_id", "left")
        .fillna({"neighbor_count": 0, "weighted_degree": 0})
    )


def validate_edges(edges) -> dict:
    """Run graph invariants and return compact counts."""
    from pyspark.sql import functions as F

    if not {"src", "dst", "weight"}.issubset(edges.columns):
        raise ValueError("edges must contain src, dst and weight")
    if edges.where((F.col("src") >= F.col("dst")) | (F.col("weight") <= 0)).limit(1).count():
        raise ValueError("edges contain a self-loop, non-canonical pair or non-positive weight")
    if edges.groupBy("src", "dst").count().where("count > 1").limit(1).count():
        raise ValueError("edges contain duplicate canonical pairs")
    row = edges.agg(
        F.count("*").alias("edge_count"),
        F.sum("weight").alias("total_weight"),
    ).first()
    return {"edge_count": int(row.edge_count), "total_weight": int(row.total_weight or 0)}
