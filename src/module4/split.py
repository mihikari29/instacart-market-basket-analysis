"""Leakage-safe internal validation split for ALS tuning."""

from __future__ import annotations


def make_internal_holdout(prior_enriched, official_train_order_ids=None):
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

    orders = prior_enriched.select("user_id", "order_id", "order_number").distinct()
    if orders.groupBy("user_id", "order_number").count().where("count > 1").limit(1).count():
        raise ValueError("prior orders do not have unique user/order_number chronology keys")
    if orders.groupBy("order_id").count().where("count > 1").limit(1).count():
        raise ValueError("prior order_id maps to multiple users or chronology values")

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
    heldout_order_ids = last_order.select("order_id").distinct()
    training_order_ids = training.select("order_id").distinct()
    if training_order_ids.join(heldout_order_ids, "order_id", "inner").limit(1).count():
        raise ValueError("internal split leaked an order between training and validation")
    holdout_chronology = last_order.join(
        orders, ["user_id", "order_id"]
    ).select("user_id", F.col("order_number").alias("holdout_order_number"))
    if (
        training.alias("history")
        .join(holdout_chronology.alias("holdout"), "user_id")
        .where(F.col("history.order_number") >= F.col("holdout.holdout_order_number"))
        .limit(1)
        .count()
    ):
        raise ValueError("internal training order is not chronologically before holdout")
    if validation.groupBy("user_id").agg(F.countDistinct("order_id").alias("n")).where("n != 1").limit(1).count():
        raise ValueError("internal split must hold out exactly one last order per eligible user")
    if official_train_order_ids is not None and (
        training_order_ids.join(official_train_order_ids.select("order_id").distinct(), "order_id", "inner")
        .limit(1)
        .count()
        or heldout_order_ids.join(
            official_train_order_ids.select("order_id").distinct(), "order_id", "inner"
        ).limit(1).count()
    ):
        raise ValueError("official train order leaked into internal split")
    expected_rows = eligible_facts.count()
    actual_rows = training.count() + validation.count()
    if expected_rows != actual_rows:
        raise ValueError("internal split does not cover all eligible prior facts exactly once")
    train_products = training.select("product_id").distinct()
    cold_products = validation.select("product_id").distinct().join(
        train_products, "product_id", "left_anti"
    ).count()
    total_users = prior_enriched.select("user_id").distinct().count()
    return {
        "training": training,
        "validation": validation,
        "validation_truth": truth,
        "summary": {
            "eligible_users": eligible.count(),
            "excluded_users": total_users - eligible.count(),
            "total_users": total_users,
            "heldout_orders": last_order.select("order_id").distinct().count(),
            "training_orders": training_order_ids.count(),
            "validation_users": truth.select("user_id").distinct().count(),
            "training_rows": training.count(),
            "validation_rows": validation.count(),
            "eligible_prior_rows": expected_rows,
            "cold_start_validation_products": cold_products,
            "official_train_overlap": 0,
            "order_sets_disjoint": True,
        },
    }
