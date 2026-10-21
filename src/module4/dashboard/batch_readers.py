"""Bounded read-only access to Module 2's published MongoDB snapshots."""

from __future__ import annotations
from ...common.type_guards import stored_utc

from collections.abc import Mapping
from datetime import datetime
import math
from numbers import Integral, Real
from typing import Any

from pymongo.errors import PyMongoError


PRODUCT_COLLECTION = "batch_product_metrics"
DEPARTMENT_COLLECTION = "department_metrics"
MAX_PRODUCTS = 50
MAX_DEPARTMENTS = 21
PRODUCT_INDEXES = {
    "purchase_count": (
        "idx_batch_product_purchase_count_desc_product_id_asc",
        (("purchase_count", -1), ("product_id", 1)),
    ),
    "reorder_rate": (
        "idx_batch_product_reorder_rate_desc_product_id_asc",
        (("reorder_rate", -1), ("product_id", 1)),
    ),
}
PRODUCT_PROJECTION = {
    "_id": 1,
    "product_id": 1,
    "purchase_count": 1,
    "unique_users": 1,
    "reorder_count": 1,
    "reorder_rate": 1,
    "run_id": 1,
    "last_batch_time": 1,
}
DEPARTMENT_PROJECTION = {
    "_id": 1,
    "department_id": 1,
    "purchase_count": 1,
    "unique_users": 1,
    "reorder_rate": 1,
    "run_id": 1,
    "last_batch_time": 1,
}


class BatchReadError(RuntimeError):
    """Base class for sanitized Module 2 read failures."""


class BatchIndexUnavailable(BatchReadError):
    """The required administrator-provisioned ranking index is absent."""


class InvalidBatchDocument(BatchReadError):
    """A stored Module 2 document violates the published metric contract."""


class BatchDatabaseFailure(BatchReadError):
    """MongoDB could not complete a bounded Module 2 read."""


def _strict_int(value: Any, field: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, Integral):
        raise InvalidBatchDocument(f"{field} must be an integer")
    result = int(value)
    if result < minimum:
        raise InvalidBatchDocument(f"{field} is outside its allowed range")
    return result


def _finite_number(value: Any, field: str, *, minimum: float, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise InvalidBatchDocument(f"{field} must be numeric")
    result = float(value)
    if not math.isfinite(result) or not minimum <= result <= maximum:
        raise InvalidBatchDocument(f"{field} is outside its allowed range")
    return result



def _run_id(value: Any) -> str:
    if not isinstance(value, str) or not value or len(value) > 128 or value != value.strip():
        raise InvalidBatchDocument("run_id must be a non-empty stable string")
    return value


def _validated_product(document: Mapping[str, Any]) -> dict[str, Any]:
    product_id = _strict_int(document.get("product_id"), "product_id", minimum=1)
    stored_id = _strict_int(document.get("_id"), "_id", minimum=1)
    if stored_id != product_id:
        raise InvalidBatchDocument("_id must equal product_id")
    purchase_count = _strict_int(document.get("purchase_count"), "purchase_count", minimum=1)
    unique_users = _strict_int(document.get("unique_users"), "unique_users", minimum=1)
    reorder_count = _strict_int(document.get("reorder_count"), "reorder_count", minimum=0)
    if unique_users > purchase_count or reorder_count > purchase_count:
        raise InvalidBatchDocument("product counts are inconsistent")
    reorder_rate = _finite_number(document.get("reorder_rate"), "reorder_rate", minimum=0, maximum=1)
    if not math.isclose(reorder_rate, reorder_count / purchase_count, rel_tol=1e-9, abs_tol=1e-12):
        raise InvalidBatchDocument("reorder_rate does not match reorder_count / purchase_count")
    return {
        "product_id": product_id,
        "purchase_count": purchase_count,
        "unique_users": unique_users,
        "reorder_count": reorder_count,
        "reorder_rate": reorder_rate,
        "run_id": _run_id(document.get("run_id")),
        "last_batch_time": stored_utc(document.get("last_batch_time"), "last_batch_time", error=InvalidBatchDocument),
    }


def _validated_department(document: Mapping[str, Any]) -> dict[str, Any]:
    department_id = _strict_int(document.get("department_id"), "department_id", minimum=1)
    stored_id = _strict_int(document.get("_id"), "_id", minimum=1)
    if department_id > MAX_DEPARTMENTS or stored_id != department_id:
        raise InvalidBatchDocument("department_id must equal _id and be between 1 and 21")
    purchase_count = _strict_int(document.get("purchase_count"), "purchase_count", minimum=1)
    unique_users = _strict_int(document.get("unique_users"), "unique_users", minimum=1)
    if unique_users > purchase_count:
        raise InvalidBatchDocument("department unique_users exceeds purchase_count")
    return {
        "department_id": department_id,
        "purchase_count": purchase_count,
        "unique_users": unique_users,
        "reorder_rate": _finite_number(document.get("reorder_rate"), "reorder_rate", minimum=0, maximum=1),
        "run_id": _run_id(document.get("run_id")),
        "last_batch_time": stored_utc(document.get("last_batch_time"), "last_batch_time", error=InvalidBatchDocument),
    }


def _index_keys(document: Mapping[str, Any]) -> tuple[tuple[str, int], ...] | None:
    key = document.get("key")
    if not isinstance(key, Mapping):
        return None
    try:
        return tuple((name, int(direction)) for name, direction in key.items())
    except (TypeError, ValueError):
        return None


def _require_product_index(collection: Any, order: str, max_time_ms: int) -> str:
    index_name, expected_keys = PRODUCT_INDEXES[order]
    cursor = None
    try:
        cursor = collection.list_indexes()
        if hasattr(cursor, "max_time_ms"):
            cursor = cursor.max_time_ms(max_time_ms)
        for index in cursor:
            collation = index.get("collation")
            if collation is None:
                locale = "simple"
            elif isinstance(collation, Mapping):
                locale = collation.get("locale")
            else:
                locale = None
            if (
                index.get("name") == index_name
                and _index_keys(index) == expected_keys
                and not index.get("hidden", False)
                and not index.get("sparse", False)
                and "partialFilterExpression" not in index
                and locale == "simple"
            ):
                return index_name
    except PyMongoError as exc:
        raise BatchDatabaseFailure("MongoDB index metadata could not be read") from exc
    finally:
        if cursor is not None and hasattr(cursor, "close"):
            try:
                cursor.close()
            except PyMongoError:
                pass
    raise BatchIndexUnavailable(f"required product index is unavailable for {order}")


def _uniform_source(items: list[dict[str, Any]]) -> tuple[str | None, datetime | None]:
    if not items:
        return None, None
    run_ids = {item["run_id"] for item in items}
    timestamps = {item["last_batch_time"] for item in items}
    if len(run_ids) != 1 or len(timestamps) != 1:
        raise InvalidBatchDocument("rows in one collection do not share a publication identity")
    return items[0]["run_id"], items[0]["last_batch_time"]


def read_batch_products(
    database: Any,
    *,
    order: str = "purchase_count",
    limit: int = 20,
    max_time_ms: int = 5_000,
) -> dict[str, Any]:
    """Read a stable indexed top-K from the selected Module 2 product metric."""
    if order not in PRODUCT_INDEXES:
        raise ValueError("unsupported product metric order")
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= MAX_PRODUCTS:
        raise ValueError("limit must be between 1 and 50")
    if isinstance(max_time_ms, bool) or not isinstance(max_time_ms, int) or max_time_ms < 1:
        raise ValueError("max_time_ms must be positive")

    try:
        collection = database[PRODUCT_COLLECTION]
        index_name = _require_product_index(collection, order, max_time_ms)
        sort = [(order, -1), ("product_id", 1)]
        cursor = collection.find({}, PRODUCT_PROJECTION).sort(sort).hint(index_name).limit(limit)
        cursor = cursor.max_time_ms(max_time_ms)
        try:
            items = [_validated_product(document) for document in cursor]
        finally:
            cursor.close()
    except BatchReadError:
        raise
    except PyMongoError as exc:
        raise BatchDatabaseFailure("MongoDB could not complete the product metric read") from exc

    if len(items) > limit:
        raise InvalidBatchDocument("product query returned more rows than its requested limit")
    run_id, last_batch_time = _uniform_source(items)
    return {
        "order": order,
        "items": items,
        "source": {
            "collection": PRODUCT_COLLECTION,
            "scope": "prior-only item purchases",
            "run_id": run_id,
            "last_batch_time": last_batch_time,
            "atomic_with_other_collections": False,
        },
        "metric_definition": {
            "purchase_count": "count of prior order-product rows for the product",
            "unique_users": "distinct users with a prior order-product row for the product",
            "reorder_count": "prior rows marked reordered=1",
            "reorder_rate": "reorder_count / purchase_count; prior-only denominator",
        },
    }


def read_batch_departments(database: Any, *, max_time_ms: int = 5_000) -> dict[str, Any]:
    """Read at most the contract maximum plus one department for overflow detection."""
    if isinstance(max_time_ms, bool) or not isinstance(max_time_ms, int) or max_time_ms < 1:
        raise ValueError("max_time_ms must be positive")
    try:
        collection = database[DEPARTMENT_COLLECTION]
        cursor = collection.find({}, DEPARTMENT_PROJECTION).sort("_id", 1).limit(MAX_DEPARTMENTS + 1)
        cursor = cursor.max_time_ms(max_time_ms)
        try:
            documents = list(cursor)
        finally:
            cursor.close()
    except PyMongoError as exc:
        raise BatchDatabaseFailure("MongoDB could not complete the department metric read") from exc

    if len(documents) > MAX_DEPARTMENTS:
        raise InvalidBatchDocument("department collection exceeds its 21-document contract")
    items = [_validated_department(document) for document in documents]
    items.sort(key=lambda item: (-item["purchase_count"], item["department_id"]))
    run_id, last_batch_time = _uniform_source(items)
    return {
        "items": items,
        "source": {
            "collection": DEPARTMENT_COLLECTION,
            "scope": "prior-only item purchases",
            "run_id": run_id,
            "last_batch_time": last_batch_time,
            "atomic_with_other_collections": False,
        },
        "metric_definition": {
            "purchase_count": "count of prior order-product rows assigned to the department",
            "unique_users": "distinct users with a prior order-product row in the department",
            "reorder_rate": "mean reordered flag across prior rows in the department",
        },
    }
