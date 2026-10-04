"""Input validation and interaction preparation for Module 4A/4B."""

from __future__ import annotations

from .config import TABLES


REQUIRED = {
    "orders": {"order_id", "user_id", "eval_set", "order_number"},
    "prior": {"order_id", "product_id", "add_to_cart_order", "reordered"},
    "train": {"order_id", "product_id", "add_to_cart_order", "reordered"},
    "products": {"product_id", "product_name", "aisle_id", "department_id"},
}


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def read_inputs(spark, config):
    """Read the four Module 4 inputs and fail early on missing columns."""
    tables = {name: spark.read.parquet(config.table(name)) for name in TABLES}
    for name, required in REQUIRED.items():
        missing = required.difference(tables[name].columns)
        _require(not missing, f"{name}: missing required columns {sorted(missing)}")
    return tables


def _no_rows(df, message: str) -> None:
    _require(df.limit(1).count() == 0, message)


def _enrich_facts(facts, orders, eval_set: str):
    order_columns = orders.select("order_id", "user_id", "order_number", "eval_set")
    _no_rows(
        facts.select("order_id").join(
            order_columns.select("order_id"), "order_id", "left_anti"
        ),
        f"{eval_set}: orphan order",
    )
    enriched = facts.join(order_columns, "order_id", "inner")
    _no_rows(
        enriched.where(enriched.eval_set != eval_set),
        f"{eval_set}: fact references an order from another eval_set",
    )
    return enriched


def validate_inputs(tables) -> dict:
    """Validate keys, domains and foreign keys before any ML or graph job."""
    from pyspark.sql import functions as F

    for name, required in REQUIRED.items():
        missing = required.difference(tables[name].columns)
        _require(not missing, f"{name}: missing required columns {sorted(missing)}")
        _require(tables[name].limit(1).count() > 0, f"{name}: empty input")

    orders = tables["orders"]
    products = tables["products"]
    prior = tables["prior"]
    train = tables["train"]

    _no_rows(orders.groupBy("order_id").count().where("count > 1"), "orders: duplicate order_id")
    _no_rows(products.groupBy("product_id").count().where("count > 1"), "products: duplicate product_id")
    for name, facts in (("prior", prior), ("train", train)):
        _no_rows(
            facts.groupBy("order_id", "product_id").count().where("count > 1"),
            f"{name}: duplicate order/product fact",
        )
        _no_rows(
            facts.where(
                F.col("order_id").isNull()
                | F.col("product_id").isNull()
                | (F.col("product_id") <= 0)
                | (F.col("add_to_cart_order") < 1)
                | ~F.col("reordered").cast("int").isin(0, 1)
            ),
            f"{name}: invalid fact domain",
        )
        _no_rows(
            facts.select("product_id").join(products.select("product_id"), "product_id", "left_anti"),
            f"{name}: orphan product",
        )

    prior_enriched = _enrich_facts(prior, orders, "prior")
    train_enriched = _enrich_facts(train, orders, "train")
    _no_rows(
        prior.select("order_id").join(train.select("order_id"), "order_id", "inner"),
        "prior/train: order_id overlap",
    )
    return {
        "counts": {
            "orders": orders.count(),
            "prior": prior.count(),
            "train": train.count(),
            "products": products.count(),
            "prior_users": prior_enriched.select("user_id").distinct().count(),
            "train_users": train_enriched.select("user_id").distinct().count(),
        },
        "prior_eval_set": "prior",
        "train_eval_set": "train",
    }


def enrich_prior(tables):
    """Attach user/order chronology to prior facts for ALS and graph jobs."""
    return _enrich_facts(tables["prior"], tables["orders"], "prior")


def build_prior_interactions(prior_enriched):
    """Aggregate prior facts into one implicit ALS row per user/product."""
    from pyspark.sql import functions as F

    return (
        prior_enriched.groupBy("user_id", "product_id")
        .agg(F.count(F.lit(1)).cast("double").alias("purchase_count"))
        .withColumn("strength_raw", F.col("purchase_count"))
        .withColumn("strength_log", F.lit(1.0) + F.log1p("purchase_count"))
    )


def build_train_truth(tables):
    """Return distinct held-out products per train user."""
    from pyspark.sql import functions as F

    train_enriched = _enrich_facts(tables["train"], tables["orders"], "train")
    return train_enriched.groupBy("user_id").agg(
        F.sort_array(F.collect_set("product_id")).alias("ground_truth")
    )
