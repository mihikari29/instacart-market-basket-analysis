"""Leakage-safe ranking metrics and deterministic recommendation baselines."""

from __future__ import annotations

import math
import re


PRIOR_ONLY_HYBRID_LAMBDAS = (0.0, 0.5, 0.7, 0.9, 1.0)
PRIOR_ONLY_HYBRID_KS = (5, 10, 20)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def prior_only_hybrid_evaluation_provenance(
    source_receipt: dict,
    *,
    prior_eval_set: str,
    truth_eval_set: str,
    official_train_used_for_fit: bool,
    official_train_used_for_tuning: bool,
    mixed_module1_event_feed_used: bool,
) -> dict:
    """Validate and describe the split boundary for an offline Hybrid evaluation."""
    if prior_eval_set != "prior" or truth_eval_set != "train":
        raise ValueError("Hybrid evaluation requires prior-only features and official-train truth")
    if any(
        type(value) is not bool
        for value in (
            official_train_used_for_fit,
            official_train_used_for_tuning,
            mixed_module1_event_feed_used,
        )
    ):
        raise ValueError("evaluation provenance flags must be booleans")
    if official_train_used_for_fit or official_train_used_for_tuning:
        raise ValueError("official-train truth cannot be used for fitting or tuning")
    if mixed_module1_event_feed_used:
        raise ValueError("mixed Module 1 event feeds cannot provide leakage-safe trend provenance")
    inputs = source_receipt.get("inputs") if isinstance(source_receipt, dict) else None
    if not isinstance(inputs, dict):
        raise ValueError("source receipt is missing curated input identities")
    prior = inputs.get("order_products_prior")
    train = inputs.get("order_products_train")
    if not isinstance(prior, dict) or not isinstance(train, dict):
        raise ValueError("source receipt must identify prior and official-train facts")
    prior_sha = str(prior.get("sha256", "")).lower()
    train_sha = str(train.get("sha256", "")).lower()
    product_catalog = inputs.get("products")
    if not isinstance(product_catalog, dict):
        raise ValueError("source receipt must identify the product catalog")
    product_sha = str(product_catalog.get("sha256", "")).lower()
    receipt_sha = str(source_receipt.get("receipt_sha256", "")).lower()
    if not all(_SHA256.fullmatch(value) for value in (prior_sha, train_sha, product_sha, receipt_sha)):
        raise ValueError("source receipt has invalid curated input SHA-256 identity")
    return {
        "status": "provenance_validated",
        "training_features_source": "curated order_products_prior joined only to orders.eval_set='prior'",
        "trend_candidate_source": "global product frequency aggregated from prior-only interactions",
        "truth_source": "curated order_products_train joined only to orders.eval_set='train'",
        "prior_source_sha256": prior_sha,
        "official_train_source_sha256": train_sha,
        "product_catalog_source_sha256": product_sha,
        "source_receipt_sha256": receipt_sha,
        "official_train_used_for_fit": False,
        "official_train_used_for_tuning": False,
        "mixed_module1_event_feed_used": False,
        "temporal_cutoff": (
            "official Instacart eval_set partition; no synthetic event-time cutoff is inferred"
        ),
        "streaming_trend_equivalence": (
            "not claimed; this is a prior-only global-frequency trend proxy, not Module 3 temporal trends"
        ),
    }


def validate_prior_only_hybrid_evaluation(evaluation: dict, source_receipt: dict) -> None:
    """Acceptance gate for a completed, provenance-backed Hybrid evaluation."""
    if not isinstance(evaluation, dict) or evaluation.get("status") != "evaluated":
        raise ValueError("prior-only Hybrid evaluation is missing or incomplete")
    if (
        not isinstance(evaluation.get("run_id"), str)
        or not isinstance(evaluation.get("model_version"), str)
        or not isinstance(evaluation.get("source_code_sha256"), str)
        or not _SHA256.fullmatch(evaluation["source_code_sha256"])
        or not isinstance(evaluation.get("als_parameters"), dict)
        or type(evaluation.get("catalog_size")) is not int
        or evaluation["catalog_size"] < 1
        or not isinstance(evaluation.get("cold_start_summary"), dict)
    ):
        raise ValueError("Hybrid evaluation is missing reproducibility or cold-start identities")
    provenance = evaluation.get("provenance")
    if not isinstance(provenance, dict):
        raise ValueError("prior-only Hybrid evaluation provenance is missing")
    inputs = source_receipt.get("inputs") if isinstance(source_receipt, dict) else None
    if not isinstance(inputs, dict):
        raise ValueError("validated source receipt is missing curated input identities")
    prior_input = inputs.get("order_products_prior")
    train_input = inputs.get("order_products_train")
    product_input = inputs.get("products")
    if not all(isinstance(item, dict) for item in (prior_input, train_input, product_input)):
        raise ValueError("validated source receipt is missing prior, train, or catalog input identities")
    expected_hashes = {
        "prior_source_sha256": prior_input.get("sha256"),
        "official_train_source_sha256": train_input.get("sha256"),
        "product_catalog_source_sha256": product_input.get("sha256"),
        "source_receipt_sha256": source_receipt.get("receipt_sha256"),
    }
    for field, expected in expected_hashes.items():
        if (
            not isinstance(expected, str)
            or not _SHA256.fullmatch(expected.lower())
            or provenance.get(field) != expected.lower()
        ):
            raise ValueError(f"Hybrid evaluation {field} does not match the validated source receipt")
    if any(
        provenance.get(field) is not False
        for field in (
            "official_train_used_for_fit",
            "official_train_used_for_tuning",
            "mixed_module1_event_feed_used",
        )
    ):
        raise ValueError("Hybrid evaluation provenance declares leakage or a mixed event feed")
    policy = evaluation.get("candidate_policy")
    if not isinstance(policy, dict) or policy.get("lambda_values_preregistered") != list(
        PRIOR_ONLY_HYBRID_LAMBDAS
    ):
        raise ValueError("Hybrid evaluation does not contain the preregistered lambda sweep")
    if policy.get("lambda_selection") != "fixed sweep; official-train truth is not used to select a value":
        raise ValueError("Hybrid lambda selection used an unapproved policy")
    if (
        policy.get("output_top_k") != max(PRIOR_ONLY_HYBRID_KS)
        or policy.get("epsilon") != 1e-12
        or policy.get("output_order") != "descending unrounded hybrid score, then ascending product_id"
    ):
        raise ValueError("Hybrid candidate ranking configuration is incomplete")
    if "not Module 3 temporal trends" not in str(provenance.get("streaming_trend_equivalence", "")):
        raise ValueError("Hybrid evaluation must disclose that its trend proxy is not Module 3 temporal trends")
    required_methods = {
        "als",
        "trend_only_prior_frequency",
        "global_popularity",
        "personal_frequency",
        *(f"hybrid_lambda_{value:.1f}".replace(".", "_") for value in PRIOR_ONLY_HYBRID_LAMBDAS),
    }
    cohorts = evaluation.get("cohorts")
    for cohort_name in ("all_official_train_users", "warm_users_and_all_truth_items"):
        cohort = cohorts.get(cohort_name) if isinstance(cohorts, dict) else None
        metrics = cohort.get("metrics") if isinstance(cohort, dict) else None
        if not isinstance(metrics, dict) or not required_methods.issubset(metrics):
            raise ValueError(f"Hybrid evaluation cohort {cohort_name} is incomplete")
        for method in required_methods:
            method_metrics = metrics[method]
            if not isinstance(method_metrics, dict) or not {
                str(k) for k in PRIOR_ONLY_HYBRID_KS
            }.issubset(method_metrics):
                raise ValueError(f"Hybrid evaluation method {method} is missing K=5/10/20 metrics")
            for k in map(str, PRIOR_ONLY_HYBRID_KS):
                metric_row = method_metrics[k]
                if (
                    not isinstance(metric_row, dict)
                    or metric_row.get("status") != "evaluated"
                    or type(metric_row.get("evaluated_users")) is not int
                    or metric_row["evaluated_users"] < 1
                ):
                    raise ValueError(f"Hybrid evaluation method {method} K={k} has no evaluated users")
                for metric in ("precision_at_k", "recall_at_k", "map_at_k", "ndcg_at_k"):
                    value = metric_row.get(metric)
                    if (
                        isinstance(value, bool)
                        or not isinstance(value, (int, float))
                        or not math.isfinite(value)
                        or not 0.0 <= value <= 1.0
                    ):
                        raise ValueError(f"Hybrid evaluation method {method} K={k} has invalid {metric}")


def prior_frequency_trend_products(interactions, top_m: int = 20):
    """Build a deterministic Top-M trend proxy using prior-only interactions."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    if type(top_m) is not int or not 1 <= top_m <= 20:
        raise ValueError("top_m must be an integer from 1 through 20")
    required = {"product_id", "purchase_count"}
    missing = required.difference(interactions.columns)
    if missing:
        raise ValueError(f"prior interactions: missing required columns {sorted(missing)}")
    counts = interactions.groupBy("product_id").agg(
        F.sum("purchase_count").cast("double").alias("_prior_product_count")
    )
    if counts.where(
        F.col("product_id").isNull()
        | (F.col("product_id") <= 0)
        | F.col("_prior_product_count").isNull()
        | F.isnan("_prior_product_count")
        | (F.col("_prior_product_count") <= 0)
        | (F.abs("_prior_product_count") == float("inf"))
    ).limit(1).count():
        raise ValueError("prior interactions contain invalid product-frequency values")
    order = Window.orderBy(F.desc("_prior_product_count"), F.asc("product_id"))
    max_count = counts.agg(F.max("_prior_product_count").alias("max_count")).first().max_count
    if max_count is None or max_count <= 0:
        raise ValueError("prior interactions have no positive product frequency")
    return (
        counts.orderBy(F.desc("_prior_product_count"), F.asc("product_id"))
        .limit(top_m)
        .withColumn(
            "trend_score",
            F.col("_prior_product_count") / (F.lit(float(max_count)) + F.lit(1e-12)),
        )
        .withColumn("trend_rank", F.row_number().over(order))
        .where(F.col("trend_rank") <= top_m)
        .select("product_id", "trend_score", "trend_rank")
    )


def prior_trend_only_recommendations(users, trend_products):
    """Expand the fixed prior-only Top-M list to a cohort without reading truth labels."""
    from pyspark.sql import functions as F

    return users.select("user_id").distinct().crossJoin(trend_products).select(
        "user_id", "product_id", F.col("trend_rank").cast("long").alias("als_rank")
    )


def offline_hybrid_recommendations(
    als_recommendations,
    users,
    trend_products,
    *,
    lambda_weight: float,
    top_k: int = 20,
    epsilon: float = 1e-12,
):
    """Create one distributed, deterministic ALS plus prior-trend candidate ranking."""
    from numbers import Real
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    if isinstance(lambda_weight, bool) or not isinstance(lambda_weight, Real):
        raise ValueError("lambda_weight must be finite and within [0, 1]")
    lambda_weight = float(lambda_weight)
    if not 0.0 <= lambda_weight <= 1.0 or lambda_weight != lambda_weight:
        raise ValueError("lambda_weight must be finite and within [0, 1]")
    if type(top_k) is not int or not 1 <= top_k <= 20:
        raise ValueError("top_k must be an integer from 1 through 20")
    if (
        isinstance(epsilon, bool)
        or not isinstance(epsilon, Real)
        or not math.isfinite(float(epsilon))
        or epsilon <= 0.0
    ):
        raise ValueError("epsilon must be finite and positive")
    epsilon = float(epsilon)
    required = {"user_id", "product_id", "als_score"}
    missing = required.difference(als_recommendations.columns)
    if missing:
        raise ValueError(f"ALS recommendations: missing required columns {sorted(missing)}")

    user_window = Window.partitionBy("user_id")
    als = (
        als_recommendations.withColumn("_als_min", F.min("als_score").over(user_window))
        .withColumn("_als_max", F.max("als_score").over(user_window))
        .withColumn(
            "_normalized_als",
            F.when(
                F.col("_als_max") > F.col("_als_min"),
                (F.col("als_score") - F.col("_als_min"))
                / (F.col("_als_max") - F.col("_als_min") + F.lit(epsilon)),
            ).otherwise(F.lit(0.0)),
        )
        .select("user_id", "product_id", "_normalized_als")
        .withColumn("_normalized_trend", F.lit(0.0))
    )
    max_trend_score = trend_products.agg(F.max("trend_score").alias("max_score")).first().max_score
    trend_source = trend_products.withColumn(
        "_normalized_trend",
        F.col("trend_score") / (F.lit(float(max_trend_score)) + F.lit(epsilon))
        if max_trend_score is not None and max_trend_score > 0
        else F.lit(0.0),
    )
    trend = (
        users.select("user_id").distinct().crossJoin(
            trend_source.select("product_id", "_normalized_trend")
        )
        .select("user_id", "product_id", F.lit(0.0).alias("_normalized_als"), "_normalized_trend")
    )
    if lambda_weight == 0.0:
        selected = trend
    elif lambda_weight == 1.0:
        # Serving selects ALS-only for warm users, with trend-only fallback
        # for users without ALS rows. Zero-weight candidates must not enter ties.
        cold_trend = trend.join(als.select("user_id").distinct(), "user_id", "left_anti")
        selected = als.unionByName(cold_trend.withColumn("_normalized_als", F.col("_normalized_trend")))
    else:
        selected = als.unionByName(trend)
    candidates = (
        selected
        .groupBy("user_id", "product_id")
        .agg(
            F.max("_normalized_als").alias("_normalized_als"),
            F.max("_normalized_trend").alias("_normalized_trend"),
        )
        .withColumn(
            "_final_score",
            F.lit(lambda_weight) * F.col("_normalized_als")
            + F.lit(1.0 - lambda_weight) * F.col("_normalized_trend"),
        )
    )
    rank_window = Window.partitionBy("user_id").orderBy(
        F.desc("_final_score"), F.asc("product_id")
    )
    return (
        candidates.withColumn("als_rank", F.row_number().over(rank_window))
        .where(F.col("als_rank") <= top_k)
        .select("user_id", "product_id", "als_rank")
    )


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


def popularity_products(interactions, k: int = 20):
    """Return the `k` most popular prior products, ranked by popularity.

    The result has one row per catalog product, but producing it means
    aggregating every prior interaction. A caller that needs the table for
    several cohorts should compute it once and pass it to
    `global_popularity` and `personal_frequency` rather than letting each
    call re-aggregate the full interaction table.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    if k < 1:
        raise ValueError("k must be positive")
    return (
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


def global_popularity(interactions, users, k: int = 20, products=None):
    """Recommend the same prior-popular products to every requested user.

    `products` may be a precomputed popularity table from
    `popularity_products`. Passing it avoids re-aggregating the interaction
    table for every cohort that needs the same baseline.
    """

    if k < 1:
        raise ValueError("k must be positive")
    if products is None:
        products = popularity_products(interactions, k)
    return users.select("user_id").distinct().crossJoin(products)


def personal_frequency(interactions, users, k: int = 20, fallback_products=None):
    """Recommend user-frequency products, filling short lists by popularity.

    `fallback_products` may be a precomputed popularity table holding twice
    as many products as the requested list. It is independent of the
    requested users, so computing it once and reusing it across cohorts
    avoids repeating the full aggregation.
    """
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
    if fallback_products is None:
        fallback_products = popularity_products(interactions, 2 * k)
    popularity = requested.crossJoin(fallback_products).select(
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
