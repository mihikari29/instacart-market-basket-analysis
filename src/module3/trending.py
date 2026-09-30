"""Trending score pipeline (Proposal §18):

    Trend(p,t) = w_short * N(C_30m(p,t)) + w_long * N(C_120m(p,t))

Two sliding windows (30 / 120 minutes, 5-minute slide) share the same
window_end cadence because slide is identical. Normalization is window-scope
min-max on purchase_count, falling back to 0.0 when the window is degenerate.
validate đi cùng watermark 10 phút (Proposal §19).
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


def build_trending(events_df: DataFrame, config: Config) -> DataFrame:
    """Two sliding windows joined on (window_end, product_id) -> ranked trend."""
    watermarked = events_df.withWatermark("event_time", config.watermark)

    short_w = _normalize(_window_agg(watermarked, config.window_short, config.slide, "purchase_count_short"),
                         "purchase_count_short")
    long_w = _normalize(_window_agg(watermarked, config.window_long, config.slide, "purchase_count_long"),
                        "purchase_count_long")

    # Both windows share slide=5min so window_end cadence is identical.
    # Outer join so products only seen in one window still rank.
    joined = short_w.join(
        long_w,
        on=["window_end", "product_id"],
        how="outer",
    ).select(
        F.coalesce(short_w["window_start"], long_w["window_start"]).alias("window_start"),
        "window_end",
        "product_id",
        F.coalesce(F.col("purchase_count_short"), F.lit(0)).alias("purchase_count_30m"),
        F.coalesce(F.col("purchase_count_long"), F.lit(0)).alias("purchase_count_120m"),
        F.coalesce(F.col("norm_purchase_count_short"), F.lit(0.0)).alias("norm_short"),
        F.coalesce(F.col("norm_purchase_count_long"), F.lit(0.0)).alias("norm_long"),
    )

    trend = joined.withColumn(
        "trend_score",
        F.col("norm_short") * F.lit(config.weight_short)
        + F.col("norm_long") * F.lit(config.weight_long),
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
