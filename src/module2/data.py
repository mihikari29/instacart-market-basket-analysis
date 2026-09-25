import json
from functools import reduce
from pyspark.sql import functions as F
from .config import TABLES

REQUIRED = {
    "orders": ["order_id", "user_id", "eval_set", "order_number", "order_dow", "order_hour_of_day"],
    "prior": ["order_id", "product_id", "add_to_cart_order", "reordered"],
    "train": ["order_id", "product_id", "add_to_cart_order", "reordered"],
    "products": ["product_id", "product_name", "aisle_id", "department_id"],
    "aisles": ["aisle_id", "aisle"],
    "departments": ["department_id", "department"],
    "interactions": [
        "event_id",
        "order_id",
        "user_id",
        "product_id",
        "reordered",
        "add_to_cart_order",
        "aisle_id",
        "department_id",
        "event_time_epoch_ms",
        "synthetic_date",
        "synthetic_year",
        "synthetic_month",
        "synthetic_day",
    ],
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def read_tables(spark, config):
    tables = {name: spark.read.parquet(config.table(name)) for name in TABLES}
    for name, df in tables.items():
        require(set(REQUIRED[name]) <= set(df.columns), f"{name}: missing required columns")
    return tables


def read_json(spark, path):
    # Spark's file datasource deliberately ignores underscore-prefixed metadata.
    jvm = spark._jvm
    location = jvm.org.apache.hadoop.fs.Path(path)
    fs = location.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    require(fs.getFileStatus(location).getLen() < 1024 * 1024, "Oversized source receipt")
    stream = fs.open(location)
    try:
        return json.loads(jvm.org.apache.commons.io.IOUtils.toString(stream, "UTF-8"))
    finally:
        stream.close()


def no_rows(df, message):
    require(not df.limit(1).count(), message)


def foreign(child, parent, keys, label):
    no_rows(child.select(*keys).join(parent.select(*keys), keys, "left_anti"), label)


def validate(spark, config, tables):
    sources = read_json(spark, config.root.rstrip("/") + "/curated/_sources.json")
    receipt = read_json(spark, config.table("interactions") + "/_handoff.json")
    counts = {}
    for name, df in tables.items():
        nulls = reduce(lambda a, b: a | b, [F.col(c).isNull() for c in REQUIRED[name]])
        stats = df.agg(
            F.count("*").alias("rows"), F.sum(F.when(nulls, 1).otherwise(0)).alias("nulls")
        ).first()
        counts[name] = stats.rows
        require(stats.rows > 0 and stats.nulls == 0, f"{name}: empty table or critical null")
        if name != "interactions":
            key = TABLES[name].split("/")[-1]
            require(counts[name] == sources[key]["rows"], f"{name}: source/staged count mismatch")
    for name, keys in {
        "orders": ["order_id"],
        "products": ["product_id"],
        "aisles": ["aisle_id"],
        "departments": ["department_id"],
        "prior": ["order_id", "product_id"],
        "train": ["order_id", "product_id"],
        "interactions": ["event_id"],
    }.items():
        no_rows(tables[name].groupBy(*keys).count().where("count > 1"), f"{name}: duplicate key")
    orders, products, events = tables["orders"], tables["products"], tables["interactions"]
    foreign(products, tables["aisles"], ["aisle_id"], "Orphan product aisle")
    foreign(products, tables["departments"], ["department_id"], "Orphan product department")
    no_rows(
        orders.where(
            ~F.col("order_dow").between(0, 6)
            | ~F.col("order_hour_of_day").between(0, 23)
            | ~F.col("eval_set").isin("prior", "train", "test")
        ),
        "Invalid order domain",
    )
    for name in ("prior", "train"):
        foreign(tables[name], products, ["product_id"], f"{name}: orphan product")
        foreign(
            tables[name],
            orders.where(F.col("eval_set") == name),
            ["order_id"],
            f"{name}: orphan order or wrong eval_set",
        )
        no_rows(
            tables[name].where(~F.col("reordered").cast("int").isin(0, 1) | (F.col("add_to_cart_order") < 1)),
            f"{name}: invalid fact value",
        )
    require(
        counts["interactions"] == receipt["source_rows"] == receipt["local_rows"],
        "Source/local/Spark interaction count mismatch",
    )
    date = F.to_date("synthetic_date", "yyyy-MM-dd")
    no_rows(
        events.where(
            date.isNull()
            | (F.year(date) != F.col("synthetic_year"))
            | (F.month(date) != F.col("synthetic_month"))
            | (F.dayofmonth(date) != F.col("synthetic_day"))
            | (F.to_date(F.timestamp_millis("event_time_epoch_ms")) != date)
        ),
        "Invalid date partition/epoch",
    )
    no_rows(
        events.where(F.col("event_id") != F.concat_ws("_", "order_id", "add_to_cart_order")),
        "event_id differs from order/cart position",
    )
    foreign(events, orders, ["order_id", "user_id"], "Event order/user mismatch")
    foreign(events, products, ["product_id", "aisle_id", "department_id"], "Event dimension mismatch")
    fact_cols = ["order_id", "product_id", "add_to_cart_order", "reordered"]
    facts = tables["prior"].select(*fact_cols).unionByName(tables["train"].select(*fact_cols))
    limit_users = receipt.get("feed_manifest", {}).get("config", {}).get("limit_users")
    if limit_users:
        users = (
            orders.where(F.col("eval_set") != "test")
            .select("user_id")
            .distinct()
            .orderBy("user_id")
            .limit(limit_users)
        )
        facts = (
            facts.join(orders.select("order_id", "user_id"), "order_id")
            .join(users, "user_id")
            .select(*fact_cols)
        )
    # Exact multiset equality catches missing+duplicated rows even when counts happen to agree.
    no_rows(facts.exceptAll(events.select(*fact_cols)), "Interactions missing source facts")
    no_rows(events.select(*fact_cols).exceptAll(facts), "Interactions contain unexpected facts")
    counts["users"] = orders.select("user_id").distinct().count()
    return {
        "counts": counts,
        "source_receipt": receipt,
        "source_tables": sources,
        "source_local_spark_equal": True,
        "partition_integrity": True,
        "fact_event_multisets_equal": True,
    }
