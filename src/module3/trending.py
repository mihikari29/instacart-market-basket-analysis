"""Streaming-safe aggregation and static micro-batch ranking (Proposal §18).

    Trend(p,t) = w_short * N(C_30m(p,t)) + w_long * N(C_120m(p,t))

The streaming plan has one stateful 120-minute sliding aggregation. Its short
count is conditional on the event falling in the final 30 minutes of that same
window. Min/max normalization and row-number ranking run only on the static
DataFrame supplied by ``foreachBatch``.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window

from .config import Config


def _window_agg(df: DataFrame, window_dur: str, slide: str, count_alias: str) -> DataFrame:
    """Group by product within a tumbling-sliding window of event_time."""
    return (
        df.groupBy(F.window("event_time", window_dur, slide).alias("w"), "product_id")
        .agg(F.count(F.lit(1)).alias(count_alias))
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "product_id",
            count_alias,
        )
    )


def _normalize(df: DataFrame, count_col: str) -> DataFrame:
    """Min-max normalize per (window_end) so trend scores lie in [0, 1]."""
    w = Window.partitionBy("window_end")
    norm_col = "norm_" + count_col
    return (
        df.withColumn("_max", F.max(count_col).over(w))
        .withColumn("_min", F.min(count_col).over(w))
        .withColumn(
            norm_col,
            F.when(F.col("_max") > F.col("_min"), (F.col(count_col) - F.col("_min")) / (F.col("_max") - F.col("_min")))
            .otherwise(F.lit(0.0)),
        )
        .drop("_max", "_min")
    )


def build_window_counts(events_df: DataFrame, config: Config) -> DataFrame:
    """Deduplicate by event_id, then build the production stateful aggregation.

    One event-time watermark bounds both deduplication state and window state.
    Static fixtures use equivalent deterministic event-id deduplication.
    """
    watermarked = events_df.withWatermark("event_time", config.watermark)
    if "event_id" not in events_df.columns:
        raise ValueError("event_id is required for source-event deduplication")
    deduplicated = (
        watermarked.dropDuplicatesWithinWatermark(["event_id"])
        if events_df.isStreaming
        else watermarked.dropDuplicates(["event_id"])
    )
    windowed = deduplicated.withColumn(
        "_window", F.window("event_time", config.window_long, config.slide)
    )
    return (
        windowed.groupBy("_window", "product_id")
        .agg(
            F.sum(
                F.when(
                    F.col("event_time")
                    >= F.col("_window.end") - F.expr(f"INTERVAL {config.window_short}"),
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).cast("long").alias("purchase_count_30m"),
            F.count(F.lit(1)).cast("long").alias("purchase_count_120m"),
        )
        .select(
            F.col("_window.start").alias("window_start"),
            F.col("_window.end").alias("window_end"),
            "product_id",
            "purchase_count_30m",
            "purchase_count_120m",
        )
    )


def rank_trending_batch(counts_df: DataFrame, config: Config) -> DataFrame:
    """Normalize and rank one static, finalized micro-batch."""
    if counts_df.isStreaming:
        raise ValueError("rank_trending_batch requires a static foreachBatch DataFrame")

    normalized = _normalize(counts_df, "purchase_count_30m")
    normalized = _normalize(normalized, "purchase_count_120m")

    trend = normalized.withColumn(
        "trend_score",
        F.col("norm_purchase_count_30m") * F.lit(config.weight_short)
        + F.col("norm_purchase_count_120m") * F.lit(config.weight_long),
    )

    rank_window = Window.partitionBy("window_end").orderBy(F.desc("trend_score"), "product_id")
    ranked = trend.withColumn("trend_rank", F.row_number().over(rank_window))

    return ranked.select(
        "window_start",
        "window_end",
        "product_id",
        "purchase_count_30m",
        "purchase_count_120m",
        "trend_score",
        "trend_rank",
    )


def select_top_k(ranked_df: DataFrame, config: Config) -> DataFrame:
    """Apply the serving bound only after full-population normalization/ranking."""
    return ranked_df.where(F.col("trend_rank") <= config.mongo_top_k_per_window)


def build_trending(events_df: DataFrame, config: Config) -> DataFrame:
    """Static serving convenience; production ranks inside ``foreachBatch``."""
    if events_df.isStreaming:
        raise ValueError("Use build_window_counts for streaming DataFrames")
    ranked = rank_trending_batch(build_window_counts(events_df, config), config)
    return select_top_k(ranked, config)
