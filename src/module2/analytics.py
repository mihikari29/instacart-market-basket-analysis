from datetime import datetime, timezone
from pyspark.sql import functions as F, Window


def rows(df, *sort):
    if sort:
        df = df.orderBy(*sort)
    return [r.asDict(recursive=True) for r in df.collect()]


def history(tables):
    return (
        tables["prior"]
        .join(tables["orders"].select("order_id", "user_id", "order_dow", "order_hour_of_day"), "order_id")
        .join(F.broadcast(tables["products"]), "product_id")
    )


def metrics(tables):
    """Feature outputs use prior only: train remains Module 4 evaluation truth."""
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


def analyze(spark, tables, counts, min_support=100):
    enriched, product, department, users = metrics(tables)
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
            "SELECT avg(n) AS average_basket_size FROM (SELECT order_id, count(*) n FROM prior_facts GROUP BY order_id)"
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
        "department_hour_pivot": rows(
            enriched.groupBy("department_id").pivot("order_hour_of_day", list(range(24))).count().fillna(0),
            "department_id",
        ),
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
        "department_daily_trends": trends,
    }


def publish_mongo(outputs, uri, run_id):
    """Stream compact aggregates in bounded batches; replace each snapshot atomically.

    Cross-collection publication is not a transaction. Consumers can check run_id.
    Only Module 2 collections are replaced; unrelated collections are untouched.
    """
    from pymongo import MongoClient

    timestamp = datetime.now(timezone.utc)
    with MongoClient(uri, serverSelectionTimeoutMS=10000) as client:
        db = client.get_default_database(default="instacart")
        for name, key in (("batch_product_metrics", "product_id"), ("department_metrics", "department_id")):
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
            collection.create_index("purchase_count")
            collection.create_index("reorder_rate")
            collection.rename(name, dropTarget=True)
