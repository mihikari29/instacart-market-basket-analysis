"""Prior-only batch metrics, report views, and MongoDB snapshot publication."""

from datetime import datetime, timezone
import re

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def rows(df: DataFrame, *sort: str | Column) -> list[dict]:
    """Collect a report DataFrame as dictionaries, optionally sorting it first."""
    if sort:
        df = df.orderBy(*sort)
    return [r.asDict(recursive=True) for r in df.collect()]


def history(tables: dict[str, DataFrame]) -> DataFrame:
    """Enrich prior purchases with order fields and broadcast product dimensions."""
    order_cols = ["order_id", "user_id", "order_number", "order_dow", "order_hour_of_day",
                  "days_since_prior_order"]
    return (
        tables["prior"]
        .join(tables["orders"].where("eval_set = 'prior'").select(*order_cols), "order_id")
        .join(F.broadcast(tables["products"]), "product_id")
    )


def metrics(tables: dict[str, DataFrame]) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    """Return enriched history plus product, department, and user aggregates.

    Feature outputs use prior only: train remains Module 4 evaluation truth.
    """
    enriched = history(tables)
    product = enriched.groupBy("product_id").agg(
        F.count("*").alias("purchase_count"),
        F.countDistinct("user_id").alias("unique_users"),
        F.sum(F.col("reordered").cast("long")).alias("reorder_count"),
    )
    product = product.withColumn("reorder_rate", F.col("reorder_count") / F.col("purchase_count"))
    department = enriched.groupBy("department_id").agg(
        F.count("*").alias("purchase_count"),
        F.countDistinct("user_id").alias("unique_users"),
        F.avg(F.col("reordered").cast("double")).alias("reorder_rate"),
    )
    users = enriched.groupBy("user_id").agg(
        F.countDistinct("order_id").alias("order_count"),
        F.count("*").alias("purchase_count"),
        F.countDistinct("product_id").alias("unique_products"),
        F.avg(F.col("reordered").cast("double")).alias("reorder_rate"),
    )
    users = users.withColumn("avg_basket_size", F.col("purchase_count") / F.col("order_count"))
    return enriched, product, department, users


def compute_product_features(enriched: DataFrame) -> DataFrame:
    """Compute product-level demand, cart addition priority, and reorder signatures."""
    return (
        enriched.groupBy("product_id").agg(
            F.count("*").alias("purchase_count"),
            F.countDistinct("user_id").alias("unique_users"),
            F.sum(F.col("reordered").cast("long")).alias("reorder_count"),
            F.avg(F.col("reordered").cast("double")).alias("reorder_rate"),
            F.avg(F.col("add_to_cart_order").cast("double")).alias("avg_cart_position"),
            (
                F.sum(F.when(F.col("add_to_cart_order") <= 3, 1).otherwise(0))
                / F.count("*")
            ).cast("double").alias("cart_priority_ratio"),
        )
    )


def compute_user_behavior_features(enriched: DataFrame, orders: DataFrame) -> DataFrame:
    """Compute distributed user purchasing cadence, order intervals, and basket trajectories."""
    # Restrict cadence to the exact baskets used by this feature generation.
    # The cleaner sets the first gap to zero; it is not an observed interval.
    prior_orders = orders.where("eval_set = 'prior'").join(
        enriched.select("order_id").distinct(), "order_id", "left_semi",
    )
    intervals = F.when(F.col("order_number") > 1, F.col("days_since_prior_order").cast("double"))
    interval_stats = prior_orders.groupBy("user_id").agg(
        F.coalesce(F.avg(intervals), F.lit(0.0)).alias("order_interval_mean"),
        F.coalesce(F.stddev_pop(intervals), F.lit(0.0)).alias("order_interval_std"),
    )

    baskets = enriched.groupBy("user_id", "order_id", "order_number").agg(
        F.count("*").alias("basket_size")
    )
    user_order_window = Window.partitionBy("user_id").orderBy(F.col("order_number").desc(), F.col("order_id").desc())
    baskets_ranked = baskets.withColumn("recency_rank", F.row_number().over(user_order_window))

    recent_baskets = baskets_ranked.where("recency_rank <= 3").groupBy("user_id").agg(
        F.avg("basket_size").alias("recent_basket_size_avg")
    )
    lifetime_baskets = baskets.groupBy("user_id").agg(
        F.avg("basket_size").alias("lifetime_basket_size_avg"),
        F.count("order_id").alias("total_prior_orders"),
        F.max("order_number").alias("max_order_number"),
    )

    return (
        lifetime_baskets
        .join(recent_baskets, "user_id", "left")
        .join(interval_stats, "user_id", "left")
        .withColumn(
            "basket_growth_slope",
            F.coalesce(F.col("recent_basket_size_avg") - F.col("lifetime_basket_size_avg"), F.lit(0.0)),
        )
    )


def compute_user_product_features(enriched: DataFrame, user_behavior: DataFrame) -> DataFrame:
    """Compute user-product interactions, purchase streaks, and recency."""
    up = enriched.groupBy("user_id", "product_id").agg(
        F.count("*").alias("user_product_purchases"),
        F.sum(F.col("reordered").cast("long")).alias("user_product_reorders"),
        F.avg(F.col("add_to_cart_order").cast("double")).alias("user_product_avg_cart_position"),
        F.max("order_number").alias("user_product_last_order_number"),
    )
    up = up.withColumn(
        "user_product_reorder_rate",
        F.col("user_product_reorders") / F.col("user_product_purchases"),
    )
    max_orders = user_behavior.select("user_id", "max_order_number")
    up = up.join(max_orders, "user_id", "inner")
    return (
        up.withColumn(
            "orders_since_last_purchase",
            F.col("max_order_number") - F.col("user_product_last_order_number"),
        )
        .withColumn(
            "purchase_streak_active",
            F.when(F.col("user_product_last_order_number") == F.col("max_order_number"), 1).otherwise(0),
        )
        .drop("max_order_number")
    )


def tag_features(
    frame: DataFrame,
    run_id: str,
    generated_at_utc: str,
) -> DataFrame:
    """Attach explicit prior-only lineage without adding features to Module 4A inputs."""
    if not isinstance(run_id, str) or not run_id.strip() or len(run_id) > 80:
        raise ValueError("feature run_id must be a non-empty string of at most 80 characters")
    if not isinstance(generated_at_utc, str) or not re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?\+00:00", generated_at_utc,
    ):
        raise ValueError("feature timestamp must be an explicit UTC ISO-8601 string")
    try:
        timestamp = datetime.fromisoformat(generated_at_utc)
    except (ValueError, TypeError):
        raise ValueError("feature timestamp must be an explicit UTC ISO-8601 string") from None
    if timestamp.tzinfo is None or timestamp.utcoffset().total_seconds() != 0:
        raise ValueError("feature timestamp must be an explicit UTC ISO-8601 string")
    return (
        frame.withColumn("module2_run_id", F.lit(run_id))
        .withColumn("feature_scope", F.lit("prior_only"))
        .withColumn("generated_at_utc", F.lit(generated_at_utc))
    )


def department_hour_pivot_unpivot(enriched: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Produce a wide teaching/report view and its normalized analytical form."""
    hour_columns = [str(hour) for hour in range(24)]
    pivoted = (
        enriched.groupBy("department_id")
        .pivot("order_hour_of_day", list(range(24)))
        .count()
        .fillna(0)
    )
    unpivoted = (
        pivoted.unpivot(
            ids="department_id",
            values=hour_columns,
            variableColumnName="order_hour_of_day",
            valueColumnName="purchase_count",
        )
        .withColumn("order_hour_of_day", F.col("order_hour_of_day").cast("int"))
        .withColumn("purchase_count", F.col("purchase_count").cast("long"))
    )
    return pivoted, unpivoted


def analyze(
    spark: SparkSession,
    tables: dict[str, DataFrame],
    counts: dict[str, int],
    min_support: int = 100,
) -> tuple[dict, dict[str, DataFrame]]:
    """Return the report and output DataFrames, checking pivot/unpivot totals.

    Purchase metrics use prior facts; order distributions include all orders.
    The caller's validated table counts are included unchanged in the report.
    """
    enriched, product, department, users = metrics(tables)
    product_features = compute_product_features(enriched)
    user_behavior = compute_user_behavior_features(enriched, tables["orders"])
    user_product_features = compute_user_product_features(enriched, user_behavior)

    department_hour_pivot, department_hour_unpivot = department_hour_pivot_unpivot(enriched)
    pivot_total = sum(
        int(row[column])
        for row in department_hour_pivot.collect()
        for column in (str(hour) for hour in range(24))
    )
    unpivot_total = int(
        department_hour_unpivot.agg(F.sum("purchase_count").alias("total")).first().total
    )
    if pivot_total != unpivot_total:
        raise ValueError(
            f"Department-hour pivot/unpivot total mismatch: {pivot_total} != {unpivot_total}"
        )
    tables["prior"].createOrReplaceTempView("prior_facts")
    # SparkSQL is also exercised directly, including a nested basket aggregation.
    purchase = (
        spark.sql(
            "SELECT count(*) AS purchases, avg(cast(reordered as double)) AS reorder_rate FROM prior_facts"
        )
        .first()
        .asDict()
    )
    basket = (
        spark.sql(
            "SELECT avg(n) AS average_basket_size FROM "
            "(SELECT order_id, count(*) n FROM prior_facts GROUP BY order_id)"
        )
        .first()
        .asDict()
    )
    order_summary = (
        tables["orders"]
        .groupBy("user_id")
        .count()
        .agg(
            F.min("count").alias("min"),
            F.max("count").alias("max"),
            F.avg("count").alias("mean"),
            F.expr("percentile_approx(count, array(0.5, 0.9, 0.99), 10000)").alias("quantiles"),
        )
    )
    named = product.join(F.broadcast(tables["products"].select("product_id", "product_name")), "product_id")
    summary = {
        "scope": "purchase metrics and features: prior only; order distributions: all orders",
        "counts": counts,
        "purchase_behavior": purchase | basket,
        "orders_per_user": order_summary.first().asDict(),
        "top_purchased": rows(named.orderBy(F.desc("purchase_count"), "product_id").limit(20)),
        "top_reordered": rows(
            named.where(F.col("purchase_count") >= min_support)
            .orderBy(F.desc("reorder_rate"), F.desc("purchase_count"), "product_id")
            .limit(20)
        ),
        "minimum_reorder_support": min_support,
        "departments": rows(department.join(tables["departments"], "department_id"), "department_id"),
        "orders_by_dow": rows(tables["orders"].groupBy("order_dow").count(), "order_dow"),
        "orders_by_hour": rows(tables["orders"].groupBy("order_hour_of_day").count(), "order_hour_of_day"),
        "department_hour_pivot": rows(department_hour_pivot, "department_id"),
        "department_hour_unpivot": rows(
            department_hour_unpivot, "department_id", "order_hour_of_day"
        ),
        "pivot_unpivot_validation": {
            "pivot_total": pivot_total,
            "unpivot_total": unpivot_total,
            "consistent": True,
        },
    }
    # Calendar-day range, not seven preceding observations (dates can be sparse).
    events = tables["interactions"].join(
        tables["orders"].where("eval_set = 'prior'").select("order_id"), "order_id"
    )
    daily = events.groupBy("department_id", "synthetic_date").agg(F.count("*").alias("purchase_count"))
    daily = daily.withColumn("day_index", F.datediff(F.to_date("synthetic_date"), F.lit("1970-01-01")))
    window = Window.partitionBy("department_id").orderBy("day_index").rangeBetween(-6, 0)
    trends = daily.withColumn("rolling_7day_purchase_count", F.sum("purchase_count").over(window)).drop(
        "day_index"
    )
    return summary, {
        "batch_product_metrics": product,
        "department_metrics": department,
        "user_features": users,
        "user_behavior_features": user_behavior,
        "product_features": product_features,
        "user_product_features": user_product_features,
        "department_daily_trends": trends,
        "department_hour_metrics": department_hour_unpivot,
    }


def publish_mongo(outputs: dict[str, DataFrame], uri: str, run_id: str) -> None:
    """Stream compact aggregates in bounded batches; replace each snapshot atomically.

    Cross-collection publication is not a transaction. Consumers can check run_id.
    Only Module 2 collections are replaced; unrelated collections are untouched.
    """
    from pymongo import MongoClient

    timestamp = datetime.now(timezone.utc)
    with MongoClient(uri, serverSelectionTimeoutMS=10000) as client:
        db = client.get_default_database(default="instacart")
        for name, key in (
            ("batch_product_metrics", "product_id"),
            ("department_metrics", "department_id"),
            ("product_features", "product_id"),
        ):
            temporary = name + "__" + run_id
            collection = db[temporary]
            collection.drop()
            db.create_collection(temporary)
            batch = []
            for row in outputs[name].toLocalIterator():
                document = row.asDict()
                document.update(_id=document[key], run_id=run_id, last_batch_time=timestamp)
                batch.append(document)
                if len(batch) == 1000:
                    collection.insert_many(batch)
                    batch.clear()
            if batch:
                collection.insert_many(batch)
            if key == "product_id":
                index_prefix = "batch_product" if name == "batch_product_metrics" else name
                collection.create_index(
                    [("purchase_count", -1), ("product_id", 1)],
                    name=f"idx_{index_prefix}_purchase_count_desc_product_id_asc",
                )
                collection.create_index(
                    [("reorder_rate", -1), ("product_id", 1)],
                    name=f"idx_{index_prefix}_reorder_rate_desc_product_id_asc",
                )
            collection.rename(name, dropTarget=True)
