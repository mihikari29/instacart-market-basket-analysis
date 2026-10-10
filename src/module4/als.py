"""Spark MLlib ALS training and deterministic Top-K recommendations."""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any


@dataclass(frozen=True)
class ALSParams:
    rank: int = 32
    reg_param: float = 0.1
    alpha: float = 10.0
    max_iter: int = 10
    strength_col: str = "strength_raw"

    def __post_init__(self):
        if self.rank < 1 or self.max_iter < 1:
            raise ValueError("rank and max_iter must be positive")
        if not math.isfinite(self.reg_param) or not math.isfinite(self.alpha):
            raise ValueError("reg_param and alpha must be finite")
        if self.reg_param < 0 or self.alpha <= 0:
            raise ValueError("reg_param must be non-negative and alpha positive")
        if self.strength_col in {"raw", "log"}:
            object.__setattr__(self, "strength_col", f"strength_{self.strength_col}")
        if self.strength_col not in {"strength_raw", "strength_log"}:
            raise ValueError("strength_col must be raw/log or strength_raw/strength_log")


def _training_frame(interactions, strength_col: str):
    from pyspark.sql import functions as F

    required = {"user_id", "product_id", strength_col}
    missing = required.difference(interactions.columns)
    if missing:
        raise ValueError(f"interactions: missing required columns {sorted(missing)}")
    user_value = F.col("user_id").cast("double")
    product_value = F.col("product_id").cast("double")
    strength_value = F.col(strength_col).cast("double")
    frame = interactions.select(
        user_value.cast("int").alias("user_id"),
        product_value.cast("int").alias("product_id"),
        strength_value.cast("float").alias("rating"),
        user_value.alias("_user_value"),
        product_value.alias("_product_value"),
        strength_value.alias("_strength_value"),
    )
    if frame.where(
        F.col("user_id").isNull()
        | F.col("product_id").isNull()
        | F.col("rating").isNull()
        | (F.col("user_id") <= 0)
        | (F.col("product_id") <= 0)
        | (F.col("rating") <= 0)
        | (F.col("_user_value") != F.col("user_id").cast("double"))
        | (F.col("_product_value") != F.col("product_id").cast("double"))
        | F.isnan("_user_value")
        | F.isnan("_product_value")
        | F.isnan("_strength_value")
        | (F.abs("_strength_value") == float("inf"))
        | (F.abs("rating") == float("inf"))
        | (F.col("_user_value") > 2147483647)
        | (F.col("_product_value") > 2147483647)
    ).limit(1).count():
        raise ValueError("ALS interactions contain null, non-positive or invalid values")
    frame = frame.drop("_user_value", "_product_value", "_strength_value")
    if frame.groupBy("user_id", "product_id").count().where("count > 1").limit(1).count():
        raise ValueError("ALS interactions must contain one row per user/product")
    return frame


def build_estimator(params: ALSParams, seed: int = 42, blocks: int = 4):
    """Build an explicit, reproducible implicit-feedback ALS estimator."""
    from pyspark.ml.recommendation import ALS

    return (
        ALS(
            userCol="user_id",
            itemCol="product_id",
            ratingCol="rating",
            implicitPrefs=True,
            coldStartStrategy="drop",
            rank=params.rank,
            regParam=params.reg_param,
            alpha=params.alpha,
            maxIter=params.max_iter,
            seed=seed,
            numUserBlocks=blocks,
            numItemBlocks=blocks,
            blockSize=512,
        )
    )


def fit_model(interactions, params: ALSParams | None = None, seed: int = 42, blocks: int = 4):
    """Fit ALS from aggregated prior interactions only."""
    params = params or ALSParams()
    from pyspark import StorageLevel

    training = (
        _training_frame(interactions, params.strength_col)
        .repartition(blocks, "user_id")
        .sortWithinPartitions("user_id", "product_id")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    try:
        training.count()
        return build_estimator(params, seed=seed, blocks=blocks).fit(training)
    finally:
        training.unpersist(blocking=False)


def model_summary(model, params: ALSParams, seed: int, input_frame=None, blocks: int = 4) -> dict[str, Any]:
    """Return the model facts required by the Module 4 evidence contract."""
    summary = {
        "params": {
            "rank": params.rank,
            "reg_param": params.reg_param,
            "alpha": params.alpha,
            "max_iter": params.max_iter,
            "strength_col": params.strength_col,
        },
        "seed": seed,
        "user_factors": int(model.userFactors.count()),
        "product_factors": int(model.itemFactors.count()),
        "implicit_prefs": True,
        "cold_start_strategy": "drop",
        "num_user_blocks": blocks,
        "num_item_blocks": blocks,
        "checkpoint_interval": 10,
        "intermediate_storage_level": "MEMORY_AND_DISK",
        "final_storage_level": "MEMORY_AND_DISK",
        "recommendation_block_size": model.getBlockSize(),
    }
    from pyspark.sql import functions as F

    for factors in (model.userFactors, model.itemFactors):
        if factors.where(F.col("id").isNull() | F.col("features").isNull() | F.exists(
            "features", lambda value: value.isNull() | F.isnan(value) | (F.abs(value) == float("inf"))
        )).limit(1).count():
            raise ValueError("ALS model contains null or non-finite factors")
    if not summary["user_factors"] or not summary["product_factors"]:
        raise ValueError("ALS model has empty user/item factors")
    summary["factors_finite"] = True
    if input_frame is not None:
        summary["input_rows"] = int(input_frame.count())
        summary["input_users"] = int(input_frame.select("user_id").distinct().count())
        summary["input_products"] = int(input_frame.select("product_id").distinct().count())
    return summary


def recommend_top_k(model, users, k: int = 20):
    """Return materialized Top-K rows, expanding exact boundary ties as needed.

    The caller owns the returned DISK_ONLY cache and must unpersist it. Temporary
    candidate caches prevent repeating the full factor cross join for the tie
    check and are released before returning, including on failure.
    """
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    if k < 1:
        raise ValueError("k must be positive")
    required = {"user_id"}
    missing = required.difference(users.columns)
    if missing:
        raise ValueError(f"users: missing required columns {sorted(missing)}")

    requested = users.select(F.col("user_id").cast("int").alias("user_id")).distinct()
    available_items = int(model.itemFactors.select("id").distinct().count())
    if available_items == 0:
        return model.userFactors.sparkSession.createDataFrame(
            [], "user_id int, product_id int, als_score double, als_rank int"
        )

    def fetch(user_frame, count):
        return (
            model.recommendForUserSubset(user_frame, count)
            .select("user_id", F.explode("recommendations").alias("recommendation"))
            .select(
                "user_id",
                F.col("recommendation.product_id").cast("int").alias("product_id"),
                # Spark 3.5.5 exposes the nested ALS score as `rating`.
                F.col("recommendation.rating").cast("double").alias("als_score"),
            )
        )

    cached = []

    def retain(frame):
        cached.append(frame.persist(StorageLevel.DISK_ONLY))
        return frame

    try:
        return _materialize_top_k(fetch, requested, available_items, k, retain)
    finally:
        for frame in reversed(cached):
            frame.unpersist(blocking=False)


def _materialize_top_k(fetch, requested, available_items, k, retain):
    from pyspark import StorageLevel
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    limit = min(available_items, max(1, k + 1))
    candidates = retain(fetch(requested, limit))
    while limit < available_items:
        by_rank = Window.partitionBy("user_id").orderBy(
            F.desc("als_score"), F.asc("product_id")
        )
        ranked = candidates.withColumn("_candidate_rank", F.row_number().over(by_rank))
        boundary = ranked.where(F.col("_candidate_rank") == k).select(
            "user_id", F.col("als_score").alias("_boundary_score")
        )
        last = ranked.groupBy("user_id").agg(
            F.count("*").alias("_candidate_count"), F.min("als_score").alias("_last_score")
        )
        ambiguous = (
            boundary.join(last, "user_id")
            .where(
                (F.col("_candidate_count") >= limit)
                & (F.col("_last_score") == F.col("_boundary_score"))
            )
            .select("user_id")
        )
        if not ambiguous.limit(1).count():
            candidates = ranked.drop("_candidate_rank")
            break
        stable = candidates.join(ambiguous, "user_id", "left_anti")
        limit = min(available_items, limit * 2)
        candidates = retain(stable.unionByName(fetch(ambiguous, limit)))

    ranking = Window.partitionBy("user_id").orderBy(
        F.desc("als_score"), F.asc("product_id")
    )
    result = candidates.where(
        F.col("product_id").isNotNull()
        & F.col("als_score").isNotNull()
        & ~F.isnan("als_score")
        & (F.abs("als_score") != float("inf"))
    ).withColumn("als_rank", F.row_number().over(ranking)).select(
        "user_id", "product_id", "als_score", "als_rank"
    ).where(F.col("als_rank") <= k).persist(StorageLevel.DISK_ONLY)
    try:
        result.count()
        return result
    except BaseException:
        result.unpersist(blocking=False)
        raise


def recommend_with_fallback(model, users, interactions, k: int = 20):
    """Recommend for every requested user, filling unknown users globally.

    Official publication intentionally uses :func:`recommend_top_k` for warm
    users only.  This helper is used solely for the all-test-user evaluation
    cohort and makes that fallback explicit in the report.
    """
    warm = recommend_top_k(model, users, k)
    requested = users.select("user_id").distinct()
    missing = requested.join(warm.select("user_id").distinct(), "user_id", "left_anti")
    if not missing.limit(1).count():
        return warm
    from .evaluation import global_popularity

    from pyspark import StorageLevel

    result = warm.unionByName(global_popularity(interactions, missing, k)).persist(StorageLevel.DISK_ONLY)
    try:
        result.count()
        return result
    except BaseException:
        result.unpersist(blocking=False)
        raise
    finally:
        warm.unpersist(blocking=False)


def save_model(model, path: str) -> None:
    """Persist a model to a new versioned path without overwriting prior output."""
    model.write().save(path)


def load_model(path: str):
    """Load a saved ALS model for the required save/load verification."""
    from pyspark.ml.recommendation import ALSModel

    return ALSModel.load(path)
