"""Leakage-safe ranking metrics and deterministic recommendation baselines."""

from __future__ import annotations


def _normalise_rank_column(recommendations):
    """Return the common ``als_rank`` schema used by ALS and baselines."""
    if "als_rank" in recommendations.columns:
        return recommendations
    if "rank" in recommendations.columns:
        return recommendations.withColumnRenamed("rank", "als_rank")
    raise ValueError("recommendations: missing required rank/als_rank column")


def _top_k(rows, k: int):
    return _normalise_rank_column(rows).where("als_rank <= {0}".format(int(k)))


def _ranking_detail():
    from pyspark.sql.types import DoubleType, StructField, StructType

    return StructType(
        [
            StructField("average_precision", DoubleType(), False),
            StructField("ndcg", DoubleType(), False),
        ]
    )


def _score_lists(predicted, truth, k: int = 20):
    """Compute AP/NDCG for one user without making the denominator ambiguous."""
    import math

    predicted = list(predicted or [])
    truth_set = set(truth or [])
    if k < 1:
        raise ValueError("k must be positive")
    predicted = predicted[:k]
    if not predicted or not truth_set:
        return (0.0, 0.0)
    hits = 0
    average_precision = 0.0
    dcg = 0.0
    for position, product_id in enumerate(predicted, 1):
        if product_id in truth_set:
            hits += 1
            average_precision += hits / position
            dcg += 1.0 / math.log2(position + 1)
    average_precision /= min(k, len(truth_set))
    ideal = min(k, len(truth_set))
    idcg = sum(1.0 / math.log2(position + 1) for position in range(1, ideal + 1))
    return (float(average_precision), float(dcg / idcg) if idcg else 0.0)


def _ranking_metrics_many(recommendations, truth, ks, catalog_size=None) -> dict:
    """Compute macro P/R/MAP/NDCG and coverage over non-empty truth users.

    Precision always uses ``K`` as its denominator, including when Spark
    returns fewer than K recommendations.  This is important for fair ALS vs
    baseline comparisons and is part of the Module 4 acceptance contract.
    """
    from pyspark.sql import functions as F

    ks = tuple(sorted(set(ks)))
    if not ks or any(not isinstance(k, int) or k < 1 for k in ks):
        raise ValueError("ks must contain positive integers")
    for name, frame, columns in (
        ("recommendations", recommendations, {"user_id", "product_id"}),
        ("truth", truth, {"user_id", "ground_truth"}),
    ):
        missing = columns.difference(frame.columns)
        if missing:
            raise ValueError(f"{name}: missing required columns {sorted(missing)}")

    recommendations = _normalise_rank_column(recommendations)
    if recommendations.groupBy("user_id", "als_rank").count().where("count > 1").limit(1).count():
        raise ValueError("recommendations contain duplicate user/rank keys")
    if recommendations.groupBy("user_id", "product_id").count().where("count > 1").limit(1).count():
        raise ValueError("recommendations contain duplicate user/product keys")
    invalid_recommendation = (
        F.col("user_id").isNull()
        | (F.col("user_id") <= 0)
        | F.col("product_id").isNull()
        | (F.col("product_id") <= 0)
        | F.col("als_rank").isNull()
        | (F.col("als_rank") < 1)
        | (F.col("als_rank") != F.col("als_rank").cast("long"))
    )
    if "als_score" in recommendations.columns:
        invalid_recommendation = invalid_recommendation | F.col("als_score").isNull() | F.isnan("als_score") | (
            F.abs("als_score") == float("inf")
        )
    if recommendations.where(invalid_recommendation).limit(1).count():
        raise ValueError("recommendations contain invalid IDs, ranks or scores")

    truth = truth.select(
        "user_id", F.col("ground_truth").cast("array<int>").alias("ground_truth")
    )
    if truth.groupBy("user_id").count().where("count > 1").limit(1).count():
        raise ValueError("truth contains duplicate users")
    if truth.where(
        F.col("user_id").isNull()
        | F.col("ground_truth").isNull()
        | (F.size("ground_truth") != F.size(F.array_distinct("ground_truth")))
        | F.exists("ground_truth", lambda value: value.isNull() | (value <= 0))
    ).limit(1).count():
        raise ValueError("truth contains invalid or duplicate product IDs")

    predicted = (
        _top_k(recommendations, max(ks))
        .where(F.col("product_id").isNotNull())
        .groupBy("user_id")
        .agg(
            F.sort_array(
                F.collect_list(F.struct(F.col("als_rank"), F.col("product_id")))
            ).alias("ranked")
        )
        .select("user_id", F.expr("transform(ranked, x -> x.product_id)").alias("predicted"))
    )
    spark = recommendations.sparkSession
    k_frame = spark.createDataFrame([(k,) for k in ks], "k int")
    evaluated = (
        truth.where(F.size("ground_truth") > 0)
        .join(predicted, "user_id", "left")
        .crossJoin(k_frame)
        .withColumn(
            "predicted",
            F.when(F.col("predicted").isNull(), F.array().cast("array<int>")).otherwise(
                F.slice(F.col("predicted").cast("array<int>"), F.lit(1), F.col("k"))
            ),
        )
        .withColumn("hits", F.size(F.array_intersect("predicted", "ground_truth")))
        .withColumn("precision_at_k", F.col("hits") / F.col("k"))
        .withColumn("recall_at_k", F.col("hits") / F.size("ground_truth"))
    )
    # Compute AP/DCG with Spark expressions so workers do not need a Python UDF.
    hits = F.transform("predicted", lambda product, index: F.struct(
        F.when(F.array_contains(F.col("ground_truth"), product), 1.0).otherwise(0.0).alias("hit"),
        (index + 1).cast("double").alias("position"),
    ))
    initial = F.struct(F.lit(0.0).alias("hits"), F.lit(0.0).alias("ap"), F.lit(0.0).alias("dcg"))
    details = F.aggregate(hits, initial, lambda acc, item: F.struct(
        (acc.hits + item.hit).alias("hits"),
        (acc.ap + item.hit * (acc.hits + item.hit) / item.position).alias("ap"),
        (acc.dcg + item.hit / F.log2(item.position + 1.0)).alias("dcg"),
    ))
    ideal_size = F.least(F.col("k"), F.size("ground_truth"))
    ideal_dcg = F.aggregate(F.sequence(F.lit(1), ideal_size), F.lit(0.0),
                            lambda acc, position: acc + 1.0 / F.log2(position + 1.0))
    evaluated = evaluated.withColumn("ranking", details).cache()
    summaries = evaluated.groupBy("k").agg(
        F.count("*").alias("evaluated_users"),
        F.avg("precision_at_k").alias("precision_at_k"),
        F.avg("recall_at_k").alias("recall_at_k"),
        F.avg(F.col("ranking.ap") / ideal_size).alias("map_at_k"),
        F.avg(F.col("ranking.dcg") / ideal_dcg).alias("ndcg_at_k"),
        F.avg(F.size("predicted")).alias("avg_count"),
        F.min(F.size("predicted")).alias("min_count"),
        F.max(F.size("predicted")).alias("max_count"),
        F.sum(F.when(F.size("predicted") > 0, 1).otherwise(0)).alias("predicted_users"),
        F.sum(F.when(F.size("predicted") == 0, 1).otherwise(0)).alias("missing_users"),
    )
    try:
        summary_by_k = {row.k: row for row in summaries.collect()}
        products_by_k = {row.k: row.products for row in evaluated.select(
            "k", F.explode("predicted").alias("product_id")
        ).groupBy("k").agg(F.countDistinct("product_id").alias("products")).collect()}
    finally:
        evaluated.unpersist(blocking=False)
    results = {}
    for k in ks:
        summary = summary_by_k.get(k)
        applicable = summary is not None and summary.evaluated_users > 0
        predicted_products = products_by_k.get(k, 0)
        if catalog_size is not None and predicted_products > catalog_size:
            raise ValueError("recommendation products exceed the declared catalog size")
        results[str(k)] = {
        "k": int(k),
        "evaluated_users": int(summary.evaluated_users) if applicable else 0,
        "status": "evaluated" if applicable else "not_applicable_empty_cohort",
        "precision_at_k": float(summary.precision_at_k) if applicable else None,
        "recall_at_k": float(summary.recall_at_k) if applicable else None,
        "map_at_k": float(summary.map_at_k) if applicable else None,
        "ndcg_at_k": float(summary.ndcg_at_k) if applicable else None,
        "user_coverage": summary.predicted_users / summary.evaluated_users if applicable else 0.0,
        "catalog_coverage": predicted_products,
        "catalog_coverage_rate": (
            predicted_products / catalog_size if catalog_size and catalog_size > 0 else 0.0
        ),
        "average_recommendations_per_user": float(summary.avg_count or 0.0) if applicable else None,
        "min_recommendations_per_user": int(summary.min_count or 0) if applicable else None,
        "max_recommendations_per_user": int(summary.max_count or 0) if applicable else None,
        "users_with_no_recommendations": int(summary.missing_users) if applicable else 0,
    }
    return results


def ranking_metrics(recommendations, truth, k: int = 10, catalog_size: int | None = None) -> dict:
    return _ranking_metrics_many(recommendations, truth, (k,), catalog_size)[str(k)]


def evaluate_at_k(recommendations, truth, ks=(5, 10, 20), catalog_size=None) -> dict:
    """Evaluate one recommendation frame at every requested K."""
    return _ranking_metrics_many(recommendations, truth, ks, catalog_size)


def personal_frequency(interactions, users, k: int = 20):
    """Recommend user-frequency products, filling short lists by popularity."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    if k < 1:
        raise ValueError("k must be positive")
    requested = users.select("user_id").distinct()
    ranking = Window.partitionBy("user_id").orderBy(
        F.desc("purchase_count"), F.asc("product_id")
    )
    personal = (
        interactions.join(requested, "user_id", "inner")
        .withColumn("als_rank", F.row_number().over(ranking))
        .where(F.col("als_rank") <= k)
        .select(
            "user_id",
            "product_id",
            F.col("purchase_count").cast("double").alias("als_score"),
            "als_rank",
        )
    )
    popularity = global_popularity(interactions, requested, 2 * k).select(
        "user_id", "product_id", "als_score", F.col("als_rank").alias("popularity_rank")
    )
    candidates = popularity.join(personal.select("user_id", "product_id"), ["user_id", "product_id"], "left_anti")
    personal_counts = personal.groupBy("user_id").agg(F.count("*").alias("personal_count"))
    fallback = (
        candidates.join(personal_counts, "user_id", "left")
        .fillna({"personal_count": 0})
        .withColumn(
            "fallback_rank",
            F.row_number().over(
                Window.partitionBy("user_id").orderBy(F.asc("popularity_rank"), F.asc("product_id"))
            ),
        )
        .withColumn("als_rank", F.col("personal_count") + F.col("fallback_rank"))
        .where(F.col("als_rank") <= k)
        .select("user_id", "product_id", "als_score", "als_rank")
    )
    return personal.unionByName(fallback)


def global_popularity(interactions, users, k: int = 20):
    """Recommend the same prior-popular products to every requested user."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    if k < 1:
        raise ValueError("k must be positive")
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


def split_truth_by_history(truth, interactions):
    """Annotate official truth with reordered and new-to-user product groups."""
    from pyspark.sql import functions as F

    history = interactions.select("user_id", "product_id").distinct()
    exploded = truth.select("user_id", F.explode("ground_truth").alias("product_id"))
    return (
        exploded.join(history.withColumn("seen", F.lit(True)), ["user_id", "product_id"], "left")
        .fillna({"seen": False})
        .groupBy("user_id")
        .agg(
            F.sort_array(F.collect_set(F.when(F.col("seen"), F.col("product_id")))).alias(
                "reordered_truth"
            ),
            F.sort_array(
                F.collect_set(F.when(~F.col("seen"), F.col("product_id")))
            ).alias("new_to_user_truth"),
        )
    )
