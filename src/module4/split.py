"""Leakage-safe internal validation split for ALS tuning."""

from __future__ import annotations


def make_internal_holdout(prior_enriched):
    """Hold out each eligible user's last prior order.

    Users with fewer than two prior orders are excluded from this tuning split.
    The official train set remains untouched and is reserved for final evaluation.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    required = {"user_id", "order_id", "order_number", "product_id"}
    missing = required.difference(prior_enriched.columns)
    if missing:
        raise ValueError(f"prior_enriched: missing required columns {sorted(missing)}")

    user_window = Window.partitionBy("user_id")
    eligible = (
        prior_enriched.select("user_id", "order_id")
        .distinct()
        .groupBy("user_id")
        .agg(F.count("order_id").alias("prior_order_count"))
        .where(F.col("prior_order_count") >= 2)
        .select("user_id")
    )
    eligible_facts = prior_enriched.join(eligible, "user_id", "inner")
    last_order = (
        eligible_facts.withColumn("last_order_number", F.max("order_number").over(user_window))
        .where(F.col("order_number") == F.col("last_order_number"))
        .select("user_id", "order_id")
        .distinct()
    )
    training = eligible_facts.join(last_order, ["user_id", "order_id"], "left_anti")
    validation = eligible_facts.join(last_order, ["user_id", "order_id"], "inner")
    truth = validation.groupBy("user_id").agg(
        F.sort_array(F.collect_set("product_id")).alias("ground_truth")
    )
    return {
        "training": training,
        "validation": validation,
        "validation_truth": truth,
        "summary": {
            "eligible_users": eligible.count(),
            "heldout_orders": last_order.select("order_id").distinct().count(),
            "training_rows": training.count(),
            "validation_rows": validation.count(),
        },
    }
