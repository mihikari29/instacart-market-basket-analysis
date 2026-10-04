"""Spark MLlib ALS training and deterministic Top-K recommendations."""

from __future__ import annotations

from dataclasses import dataclass


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
        if self.reg_param < 0 or self.alpha <= 0:
            raise ValueError("reg_param must be non-negative and alpha positive")
        if self.strength_col not in {"strength_raw", "strength_log"}:
            raise ValueError("strength_col must be strength_raw or strength_log")


def _training_frame(interactions, strength_col: str):
    from pyspark.sql import functions as F

    required = {"user_id", "product_id", strength_col}
    missing = required.difference(interactions.columns)
    if missing:
        raise ValueError(f"interactions: missing required columns {sorted(missing)}")
    return interactions.select(
        F.col("user_id").cast("int").alias("user_id"),
        F.col("product_id").cast("int").alias("product_id"),
        F.col(strength_col).cast("float").alias("rating"),
    )


def build_estimator(params: ALSParams, seed: int = 42):
    """Build an explicit, reproducible implicit-feedback ALS estimator."""
    from pyspark.ml.recommendation import ALS

    return (
        ALS(
            userCol="user_id",
            itemCol="product_id",
            ratingCol="rating",
            predictionCol="als_score",
            implicitPrefs=True,
            coldStartStrategy="drop",
            rank=params.rank,
            regParam=params.reg_param,
            alpha=params.alpha,
            maxIter=params.max_iter,
            seed=seed,
        )
    )


def fit_model(interactions, params: ALSParams | None = None, seed: int = 42):
    """Fit ALS from aggregated prior interactions only."""
    params = params or ALSParams()
    return build_estimator(params, seed=seed).fit(
        _training_frame(interactions, params.strength_col)
    )


def recommend_top_k(model, users, k: int = 20):
    """Return one deterministic row per user/rank/product."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    if k < 1:
        raise ValueError("k must be positive")
    required = {"user_id"}
    missing = required.difference(users.columns)
    if missing:
        raise ValueError(f"users: missing required columns {sorted(missing)}")

    exploded = (
        model.recommendForUserSubset(users.select("user_id").distinct(), k)
        .select("user_id", F.explode("recommendations").alias("recommendation"))
        .select(
            "user_id",
            F.col("recommendation.product_id").cast("int").alias("product_id"),
            F.col("recommendation.als_score").cast("double").alias("als_score"),
        )
    )
    ranking = Window.partitionBy("user_id").orderBy(
        F.desc("als_score"), F.asc("product_id")
    )
    return exploded.withColumn("als_rank", F.row_number().over(ranking)).select(
        "user_id", "product_id", "als_score", "als_rank"
    )


def save_model(model, path: str) -> None:
    """Persist a model to a versioned path; callers own version naming."""
    model.write().overwrite().save(path)
