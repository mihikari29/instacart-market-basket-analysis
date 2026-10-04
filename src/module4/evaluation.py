"""Recommendation metrics and simple non-ML baselines."""

from __future__ import annotations


def _top_k(rows, k: int):
    return rows.where(rows.als_rank <= k)


def ranking_metrics(recommendations, truth, k: int = 10) -> dict:
    """Compute macro Precision@K/Recall@K and coverage from Spark DataFrames."""
    from pyspark.sql import functions as F

    if k < 1:
        raise ValueError("k must be positive")
    for name, frame, columns in (
        ("recommendations", recommendations, {"user_id", "product_id", "als_rank"}),
        ("truth", truth, {"user_id", "ground_truth"}),
    ):
        missing = columns.difference(frame.columns)
        if missing:
            raise ValueError(f"{name}: missing required columns {sorted(missing)}")

    predicted = (
        _top_k(recommendations, k)
        .groupBy("user_id")
        .agg(F.collect_set("product_id").alias("predicted"))
    )
    evaluated = (
        truth.select("user_id", "ground_truth")
        .where(F.size("ground_truth") > 0)
        .join(predicted, "user_id", "left")
        .withColumn("predicted", F.coalesce("predicted", F.array().cast("array<int>")))
        .withColumn("hits", F.size(F.array_intersect("predicted", "ground_truth")))
        .withColumn("precision_at_k", F.col("hits") / F.lit(float(k)))
        .withColumn("recall_at_k", F.col("hits") / F.size("ground_truth"))
    )
    summary = evaluated.agg(
        F.count("*").alias("evaluated_users"),
        F.avg("precision_at_k").alias("precision_at_k"),
        F.avg("recall_at_k").alias("recall_at_k"),
    ).first()
    truth_users = truth.where(F.size("ground_truth") > 0).select("user_id").distinct().count()
    predicted_users = predicted.select("user_id").distinct().count()
    predicted_products = predicted.select(F.explode("predicted").alias("product_id")).select(
        "product_id"
    ).distinct().count()
    return {
        "k": k,
        "evaluated_users": int(summary.evaluated_users or 0),
        "precision_at_k": float(summary.precision_at_k or 0.0),
        "recall_at_k": float(summary.recall_at_k or 0.0),
        "user_coverage": (predicted_users / truth_users) if truth_users else 0.0,
        "catalog_coverage": predicted_products,
    }


def personal_frequency(interactions, users, k: int = 20):
    """Recommend each user's most frequently purchased products."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    ranking = Window.partitionBy("user_id").orderBy(
        F.desc("purchase_count"), F.asc("product_id")
    )
    return (
        interactions.join(users.select("user_id").distinct(), "user_id", "inner")
        .withColumn("als_rank", F.row_number().over(ranking))
        .where(F.col("als_rank") <= k)
        .select(
            "user_id",
            "product_id",
            F.col("purchase_count").cast("double").alias("als_score"),
            "als_rank",
        )
    )


def global_popularity(interactions, users, k: int = 20):
    """Recommend the same prior-popular products to every requested user."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    products = (
        interactions.groupBy("product_id")
        .agg(F.sum("purchase_count").alias("purchase_count"))
        .withColumn(
            "als_rank",
            F.row_number().over(Window.orderBy(F.desc("purchase_count"), F.asc("product_id"))),
        )
        .where(F.col("als_rank") <= k)
        .select(
            "product_id",
            F.col("purchase_count").cast("double").alias("als_score"),
            "als_rank",
        )
    )
    return users.select("user_id").distinct().crossJoin(products)
