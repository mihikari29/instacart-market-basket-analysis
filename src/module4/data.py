"""Input validation and interaction preparation for Module 4A/4B."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
import hashlib
import json
import math
from numbers import Real
from typing import Any

from .config import TABLES


REQUIRED = {
    "orders": {"order_id", "user_id", "eval_set", "order_number"},
    "prior": {"order_id", "product_id", "add_to_cart_order", "reordered"},
    "train": {"order_id", "product_id", "add_to_cart_order", "reordered"},
    "products": {"product_id", "product_name", "aisle_id", "department_id"},
}

FACT_COLUMNS = {"order_id", "product_id", "add_to_cart_order", "reordered"}


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


def _numeric_type(frame, column: str, label: str) -> None:
    kind = frame.schema[column].dataType.typeName()
    if kind not in {"byte", "short", "integer", "long", "float", "double", "decimal"}:
        raise ValueError(f"{label}.{column}: expected a numeric/boolean Parquet type, got {kind}")


def _invalid_integer(column, maximum=2147483647):
    from pyspark.sql import functions as F

    value = F.col(column).cast("double")
    return (
        F.col(column).isNull()
        | value.isNull()
        | F.isnan(value)
        | (F.abs(value) == float("inf"))
        | (value < 1)
        | (value > maximum)
        | (value != value.cast("long").cast("double"))
    )


def read_hdfs_json(spark, path: str) -> dict:
    jvm = spark._jvm
    location = jvm.org.apache.hadoop.fs.Path(path)
    fs = location.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    status = fs.getFileStatus(location)
    if status.getLen() > 1024 * 1024:
        raise ValueError(f"HDFS receipt is unexpectedly large: {path}")
    stream = fs.open(location)
    try:
        raw = jvm.org.apache.commons.io.IOUtils.toString(stream, "UTF-8")
    finally:
        stream.close()
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid HDFS receipt JSON: {path}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"HDFS receipt must contain a JSON object: {path}")
    return value


def hdfs_sha256(spark, fs, path) -> str:
    jvm = spark._jvm
    stream = fs.open(path)
    try:
        return str(jvm.org.apache.commons.codec.digest.DigestUtils.sha256Hex(stream))
    finally:
        stream.close()


def validate_source_receipts(spark, config, tables, verify_counts=True) -> dict:
    """Compare staged Parquet bytes and row counts with Module 1's receipt."""
    root = config.root.rstrip("/") + "/curated/_sources.json"
    sources = read_hdfs_json(spark, root)
    jvm = spark._jvm
    hadoop_config = spark.sparkContext._jsc.hadoopConfiguration()
    inputs = {}
    for name, frame in tables.items():
        source_name = TABLES[name].split("/")[-1]
        expected = sources.get(source_name)
        if not isinstance(expected, dict) or "rows" not in expected or "sha256" not in expected:
            raise ValueError(f"_sources.json is missing rows/sha256 for {source_name}")
        actual_rows = frame.count() if verify_counts else int(expected["rows"])
        if verify_counts and int(expected["rows"]) != actual_rows:
            raise ValueError(
                f"{source_name}: receipt row count {expected['rows']} != staged rows {actual_rows}"
            )
        location = jvm.org.apache.hadoop.fs.Path(config.table(name))
        fs = location.getFileSystem(hadoop_config)
        files = sorted(
            (status.getPath() for status in fs.listStatus(location) if status.isFile()),
            key=lambda item: item.getName(),
        )
        parquet_files = [path for path in files if path.getName().lower().endswith(".parquet")]
        if len(parquet_files) != 1:
            raise ValueError(
                f"{source_name}: expected exactly one staged Parquet file for the SHA-256 receipt; "
                f"found {len(parquet_files)}"
            )
        actual_hash = hdfs_sha256(spark, fs, parquet_files[0])
        if actual_hash.lower() != str(expected["sha256"]).lower():
            raise ValueError(f"{source_name}: staged Parquet SHA-256 does not match _sources.json")
        inputs[source_name] = {
            "rows": actual_rows,
            "sha256": actual_hash,
            "path": parquet_files[0].toString(),
            "file_bytes": int(fs.getFileStatus(parquet_files[0]).getLen()),
        }
    canonical = json.dumps(sources, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return {
        "validated_at_utc": datetime.now(timezone.utc).isoformat(),
        "receipt_path": root,
        "receipt_sha256": hashlib.sha256(canonical).hexdigest(),
        "inputs": inputs,
        "source_scope": "staged_curated",
    }


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

    for name, frame, columns in (
        ("orders", orders, ("order_id", "user_id", "order_number")),
        ("products", products, ("product_id", "aisle_id", "department_id")),
        ("prior", prior, ("order_id", "product_id", "add_to_cart_order")),
        ("train", train, ("order_id", "product_id", "add_to_cart_order")),
    ):
        for column in columns:
            _numeric_type(frame, column, name)
    if orders.schema["eval_set"].dataType.typeName() != "string":
        raise ValueError("orders.eval_set: expected a string Parquet type")
    if products.schema["product_name"].dataType.typeName() != "string":
        raise ValueError("products.product_name: expected a string Parquet type")

    _no_rows(
        orders.where(
            _invalid_integer("order_id", 9223372036854775807)
            | _invalid_integer("user_id")
            | F.col("order_number").isNull()
            | _invalid_integer("order_number", 9223372036854775807)
            | F.col("eval_set").isNull()
            | ~F.col("eval_set").isin("prior", "train", "test")
        ),
        "orders: invalid key or eval_set domain",
    )
    _no_rows(
        products.where(
            _invalid_integer("product_id")
            | _invalid_integer("aisle_id")
            | _invalid_integer("department_id")
            | F.col("product_name").isNull()
            | (F.length(F.trim("product_name")) == 0)
        ),
        "products: invalid key domain",
    )

    _no_rows(orders.groupBy("order_id").count().where("count > 1"), "orders: duplicate order_id")
    _no_rows(
        orders.groupBy("user_id", "order_number").count().where("count > 1"),
        "orders: duplicate user/order_number chronology key",
    )
    _no_rows(products.groupBy("product_id").count().where("count > 1"), "products: duplicate product_id")
    for name, facts in (("prior", prior), ("train", train)):
        if facts.schema["reordered"].dataType.typeName() not in {
            "boolean", "byte", "short", "integer", "long"
        }:
            raise ValueError(f"{name}.reordered: expected boolean or integer type")
        _no_rows(
            facts.groupBy("order_id", "product_id").count().where("count > 1"),
            f"{name}: duplicate order/product fact",
        )
        _no_rows(
            facts.where(
                _invalid_integer("order_id", 9223372036854775807)
                | _invalid_integer("product_id")
                | _invalid_integer("add_to_cart_order")
                | F.col("reordered").isNull()
                | ~F.col("reordered").cast("int").isin(0, 1)
            ),
            f"{name}: invalid fact domain",
        )
        _no_rows(
            facts.select("product_id").join(products.select("product_id"), "product_id", "left_anti"),
            f"{name}: orphan product",
        )
        cart_stats = facts.groupBy("order_id").agg(
            F.count("*").alias("row_count"),
            F.countDistinct("add_to_cart_order").alias("position_count"),
            F.min("add_to_cart_order").alias("min_position"),
            F.max("add_to_cart_order").alias("max_position"),
        )
        _no_rows(
            cart_stats.where(
                (F.col("position_count") != F.col("row_count"))
                | (F.col("min_position") != 1)
                | (F.col("max_position") != F.col("row_count"))
            ),
            f"{name}: cart positions must be unique and contiguous from 1",
        )

    prior_enriched = _enrich_facts(prior, orders, "prior")
    train_enriched = _enrich_facts(train, orders, "train")
    for name, eval_set, facts in (("prior", "prior", prior), ("train", "train", train)):
        _no_rows(
            orders.where(F.col("eval_set") == eval_set)
            .select("order_id")
            .join(facts.select("order_id").distinct(), "order_id", "left_anti"),
            f"{name}: order without a fact row",
        )
    _no_rows(
        prior.select("order_id").join(train.select("order_id"), "order_id", "inner"),
        "prior/train: order_id overlap",
    )
    prior_distribution = (
        prior.groupBy("reordered")
        .count()
        .orderBy("reordered")
        .collect()
    )
    train_distribution = (
        train.groupBy("reordered")
        .count()
        .orderBy("reordered")
        .collect()
    )
    interactions = build_prior_interactions(prior_enriched)
    purchase_quantiles = interactions.approxQuantile(
        "purchase_count", [0.0, 0.25, 0.5, 0.75, 0.95, 1.0], 0.001
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
        "distinct": {
            "prior_orders": prior.select("order_id").distinct().count(),
            "train_orders": train.select("order_id").distinct().count(),
            "prior_products": prior.select("product_id").distinct().count(),
            "train_products": train.select("product_id").distinct().count(),
        },
        "purchase_distribution": {
            "prior_reordered": {str(row.reordered): int(row["count"]) for row in prior_distribution},
            "train_reordered": {str(row.reordered): int(row["count"]) for row in train_distribution},
            "prior_purchases_per_user_product_quantiles": purchase_quantiles,
        },
        "cohort": {
            "prior_orders_without_facts": 0,
            "train_orders_without_facts": 0,
            "train_users_with_truth": train_enriched.select("user_id").distinct().count(),
        },
        "prior_eval_set": "prior",
        "train_eval_set": "train",
        "official_train_used_for_fit": False,
        "official_train_used_for_tuning": False,
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


USER_FEATURE_COLUMNS = frozenset(
    {
        "user_id", "order_count", "purchase_count", "unique_products", "reorder_rate",
        "avg_basket_size", "module2_run_id", "feature_scope", "generated_at_utc",
    }
)


def validate_user_feature_columns(columns: Sequence[str]) -> None:
    """Require the exact descriptive contract and reject unexpected model inputs."""
    missing = sorted(USER_FEATURE_COLUMNS.difference(columns))
    unexpected = sorted(set(columns).difference(USER_FEATURE_COLUMNS))
    if missing or unexpected:
        problems = []
        if missing:
            problems.append("missing fields " + ", ".join(missing))
        if unexpected:
            problems.append("unexpected fields " + ", ".join(unexpected))
        raise ValueError(
            "Module 2 user_features schema mismatch: " + "; ".join(problems)
        )


def _feature_record_mapping(row: Any) -> Mapping[str, Any]:
    if isinstance(row, Mapping):
        return row
    try:
        result = row.asDict(recursive=True)
        if isinstance(result, Mapping):
            return result
    except (AttributeError, TypeError):
        pass
    raise ValueError("Module 2 user_features returned a non-record row")


def _feature_positive_integer(row: Mapping[str, Any], field: str) -> int:
    value = row.get(field)
    if type(value) is not int or value <= 0:
        raise ValueError(f"Module 2 user_features.{field} must be a positive integer")
    return value


def _feature_finite_number(row: Mapping[str, Any], field: str, *, minimum: float) -> float:
    value = row.get(field)
    if isinstance(value, bool) or not isinstance(value, Real):
        raise ValueError(f"Module 2 user_features.{field} must be a finite number")
    try:
        number = float(value)
    except (OverflowError, ValueError) as exc:
        raise ValueError(f"Module 2 user_features.{field} must be a finite number") from exc
    if not math.isfinite(number) or number < minimum:
        raise ValueError(f"Module 2 user_features.{field} is outside its allowed range")
    return number


def inspect_user_feature_rows(rows: Sequence[Any], user_id: int) -> dict[str, Any]:
    """Validate at most two filtered rows and return a descriptive-only result."""
    if type(user_id) is not int or user_id <= 0:
        raise ValueError("user_id must be a positive integer")
    if len(rows) > 1:
        raise ValueError("Module 2 user_features contains duplicate rows for the requested user")
    if not rows:
        return {
            "user_id": user_id,
            "status": "no_prior_activity",
            "reason": "no_module2_prior_only_user_feature_row",
            "scope": "prior_only descriptive features; not ALS model input",
            "features": None,
            "source": None,
        }

    row = _feature_record_mapping(rows[0])
    if _feature_positive_integer(row, "user_id") != user_id:
        raise ValueError("Module 2 user_features returned a different user_id")
    if row.get("feature_scope") != "prior_only":
        raise ValueError("Module 2 user_features lineage is not prior_only")
    run_id = row.get("module2_run_id")
    timestamp = row.get("generated_at_utc")
    if not isinstance(run_id, str) or not run_id.strip() or len(run_id) > 80:
        raise ValueError("Module 2 user_features module2_run_id is invalid")
    if not isinstance(timestamp, str):
        raise ValueError("Module 2 user_features generated_at_utc is invalid")
    try:
        generated_at = datetime.fromisoformat(timestamp)
    except ValueError as exc:
        raise ValueError("Module 2 user_features generated_at_utc is invalid") from exc
    if generated_at.tzinfo is None or generated_at.utcoffset() != timezone.utc.utcoffset(generated_at):
        raise ValueError("Module 2 user_features generated_at_utc must be UTC")

    features = {
        "order_count": _feature_positive_integer(row, "order_count"),
        "purchase_count": _feature_positive_integer(row, "purchase_count"),
        "unique_products": _feature_positive_integer(row, "unique_products"),
        "reorder_rate": _feature_finite_number(row, "reorder_rate", minimum=0.0),
        "avg_basket_size": _feature_finite_number(row, "avg_basket_size", minimum=0.0),
    }
    if features["reorder_rate"] > 1.0 or features["avg_basket_size"] <= 0.0:
        raise ValueError("Module 2 user_features contains out-of-range descriptive values")
    return {
        "user_id": user_id,
        "status": "available",
        "reason": None,
        "scope": "prior_only descriptive features; not ALS model input",
        "features": features,
        "source": {"module2_run_id": run_id, "generated_at_utc": timestamp},
    }


def read_user_features(spark: Any, config: Any, user_id: int) -> dict[str, Any]:
    """Read one user's prior-only descriptive row with a bounded HDFS query."""
    if type(user_id) is not int or not 0 < user_id <= 9_223_372_036_854_775_807:
        raise ValueError("user_id must be a positive signed 64-bit integer")
    frame = spark.read.parquet(config.features())
    validate_user_feature_columns(frame.columns)
    from pyspark.sql import functions as F

    rows = frame.where(F.col("user_id") == user_id).limit(2).collect()
    return inspect_user_feature_rows(rows, user_id)

