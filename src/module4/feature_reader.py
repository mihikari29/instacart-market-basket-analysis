"""Read Module 2's prior-only user features for descriptive Module 4 inspection."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
import math
from numbers import Real
from typing import Any


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


def _record_mapping(row: Any) -> Mapping[str, Any]:
    if isinstance(row, Mapping):
        return row
    as_dict = getattr(row, "asDict", None)
    if callable(as_dict):
        result = as_dict(recursive=True)
        if isinstance(result, Mapping):
            return result
    raise ValueError("Module 2 user_features returned a non-record row")


def _positive_integer(row: Mapping[str, Any], field: str) -> int:
    value = row.get(field)
    if type(value) is not int or value <= 0:
        raise ValueError(f"Module 2 user_features.{field} must be a positive integer")
    return value


def _finite_number(row: Mapping[str, Any], field: str, *, minimum: float) -> float:
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

    row = _record_mapping(rows[0])
    if _positive_integer(row, "user_id") != user_id:
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
        "order_count": _positive_integer(row, "order_count"),
        "purchase_count": _positive_integer(row, "purchase_count"),
        "unique_products": _positive_integer(row, "unique_products"),
        "reorder_rate": _finite_number(row, "reorder_rate", minimum=0.0),
        "avg_basket_size": _finite_number(row, "avg_basket_size", minimum=0.0),
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
