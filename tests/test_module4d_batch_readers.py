"""Network-free contract tests for bounded Module 2 snapshot reads."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import math

from pymongo.errors import OperationFailure
import pytest

from src.module4d.batch_readers import (
    BatchDatabaseFailure,
    BatchIndexUnavailable,
    InvalidBatchDocument,
    PRODUCT_INDEXES,
    read_batch_departments,
    read_batch_products,
)


TIME = datetime(2026, 10, 14, tzinfo=timezone.utc)


def product(product_id=1, *, purchase=10, users=7, reorders=4, rate=None, run="module2-run"):
    return {
        "_id": product_id,
        "product_id": product_id,
        "purchase_count": purchase,
        "unique_users": users,
        "reorder_count": reorders,
        "reorder_rate": reorders / purchase if rate is None else rate,
        "run_id": run,
        "last_batch_time": TIME,
        "unprojected_secret": "must not escape",
    }


def department(department_id=1, *, purchase=100, users=40, rate=0.4, run="module2-run"):
    return {
        "_id": department_id,
        "department_id": department_id,
        "purchase_count": purchase,
        "unique_users": users,
        "reorder_rate": rate,
        "run_id": run,
        "last_batch_time": TIME,
        "unprojected_secret": "must not escape",
    }


class IndexCursor:
    def __init__(self, indexes):
        self.indexes = indexes
        self.max_time = None
        self.closed = False

    def max_time_ms(self, value):
        self.max_time = value
        return self

    def __iter__(self):
        return iter(self.indexes)

    def close(self):
        self.closed = True


class RowCursor:
    def __init__(self, rows):
        self.rows = rows
        self.sort_value = None
        self.limit_value = None
        self.hint_value = None
        self.max_time = None
        self.closed = False

    def sort(self, value, direction=None):
        if isinstance(value, str):
            self.sort_value = [(value, direction)]
        else:
            self.sort_value = list(value)
        for name, order in reversed(self.sort_value):
            self.rows.sort(key=lambda row: row.get(name), reverse=order < 0)
        return self

    def hint(self, value):
        self.hint_value = value
        return self

    def limit(self, value):
        self.limit_value = value
        self.rows = self.rows[:value]
        return self

    def max_time_ms(self, value):
        self.max_time = value
        return self

    def __iter__(self):
        return iter(self.rows)

    def close(self):
        self.closed = True


class BatchCollection:
    def __init__(self, documents=(), indexes=None):
        self.documents = [deepcopy(row) for row in documents]
        self.indexes = indexes or {"_id_": {"name": "_id_", "key": {"_id": 1}}}
        self.find_calls = []
        self.cursors = []
        self.index_cursors = []
        self.fail_find = False
        self.fail_indexes = False
        self.write_calls = []

    def list_indexes(self):
        if self.fail_indexes:
            raise OperationFailure("private connection details")
        cursor = IndexCursor(list(self.indexes.values()))
        self.index_cursors.append(cursor)
        return cursor

    def find(self, query, projection=None):
        self.find_calls.append((deepcopy(query), deepcopy(projection)))
        if self.fail_find:
            raise OperationFailure("private connection details")
        rows = [deepcopy(row) for row in self.documents]
        if projection:
            included = {field for field, enabled in projection.items() if enabled}
            rows = [{key: value for key, value in row.items() if key in included} for row in rows]
        cursor = RowCursor(rows)
        self.cursors.append(cursor)
        return cursor

    def __getattr__(self, name):
        if name in {"insert_one", "insert_many", "update_one", "replace_one", "delete_one", "delete_many", "drop", "rename", "create_index"}:
            self.write_calls.append(name)
            raise AssertionError(f"read API attempted write operation {name}")
        raise AttributeError(name)


def product_indexes(*, purchase=True, reorder=True):
    indexes = {"_id_": {"name": "_id_", "key": {"_id": 1}}}
    for order, enabled in (("purchase_count", purchase), ("reorder_rate", reorder)):
        if enabled:
            name, keys = PRODUCT_INDEXES[order]
            indexes[name] = {"name": name, "key": dict(keys)}
    return indexes


class BatchDatabase:
    def __init__(self, products=(), departments=(), *, indexes=None):
        self.products = BatchCollection(products, indexes or product_indexes())
        self.departments = BatchCollection(departments)
        self.names = []

    def __getitem__(self, name):
        self.names.append(name)
        return {
            "batch_product_metrics": self.products,
            "department_metrics": self.departments,
        }[name]


@pytest.mark.parametrize(
    ("order", "sort"),
    [
        ("purchase_count", [("purchase_count", -1), ("product_id", 1)]),
        ("reorder_rate", [("reorder_rate", -1), ("product_id", 1)]),
    ],
)
def test_product_read_uses_requested_compound_index_and_stable_product_id_ties(order, sort):
    docs = [
        product(9, purchase=10, reorders=5),
        product(7, purchase=10, reorders=5),
        product(8, purchase=20, reorders=10),
    ]
    database = BatchDatabase(docs)

    result = read_batch_products(database, order=order, limit=2, max_time_ms=2500)

    assert [row["product_id"] for row in result["items"]] == ([8, 7] if order == "purchase_count" else [7, 8])
    assert result["source"]["run_id"] == "module2-run"
    assert result["source"]["scope"] == "prior-only item purchases"
    assert result["source"]["atomic_with_other_collections"] is False
    assert result["metric_definition"]["reorder_rate"].startswith("reorder_count / purchase_count")
    assert set(result["items"][0]) == {
        "product_id", "purchase_count", "unique_users", "reorder_count", "reorder_rate", "run_id", "last_batch_time"
    }
    cursor = database.products.cursors[0]
    assert database.products.find_calls[0][0] == {}
    assert "unprojected_secret" not in database.products.find_calls[0][1]
    assert cursor.sort_value == sort
    assert cursor.hint_value == PRODUCT_INDEXES[order][0]
    assert cursor.limit_value == 2
    assert cursor.max_time == 2500
    assert cursor.closed
    assert database.products.index_cursors[0].closed
    assert database.products.write_calls == []


@pytest.mark.parametrize("limit", [0, -1, 51, True, 1.5])
def test_product_read_rejects_invalid_page_sizes(limit):
    with pytest.raises(ValueError, match="limit"):
        read_batch_products(BatchDatabase(), limit=limit)


def test_product_read_rejects_unknown_sort_mode():
    with pytest.raises(ValueError, match="unsupported"):
        read_batch_products(BatchDatabase(), order="purchase_count desc")


@pytest.mark.parametrize("order", ["purchase_count", "reorder_rate"])
def test_missing_required_product_index_fails_before_find(order):
    indexes = product_indexes(purchase=order != "purchase_count", reorder=order != "reorder_rate")
    database = BatchDatabase([product()], indexes=indexes)

    with pytest.raises(BatchIndexUnavailable):
        read_batch_products(database, order=order)

    assert database.products.find_calls == []


def test_hidden_or_partial_index_is_not_accepted():
    name, keys = PRODUCT_INDEXES["purchase_count"]
    database = BatchDatabase(
        [product()],
        indexes={
            "_id_": {"name": "_id_", "key": {"_id": 1}},
            name: {"name": name, "key": dict(keys), "hidden": True},
        },
    )
    with pytest.raises(BatchIndexUnavailable):
        read_batch_products(database)


@pytest.mark.parametrize("unsafe_metadata", [{"sparse": True}, {"collation": "invalid"}, {"collation": {"locale": "en"}}])
def test_sparse_or_incompatible_collation_indexes_are_not_accepted(unsafe_metadata):
    name, keys = PRODUCT_INDEXES["purchase_count"]
    database = BatchDatabase(
        [product()],
        indexes={
            "_id_": {"name": "_id_", "key": {"_id": 1}},
            name: {"name": name, "key": dict(keys), **unsafe_metadata},
        },
    )
    with pytest.raises(BatchIndexUnavailable):
        read_batch_products(database)


@pytest.mark.parametrize(
    "changes",
    [
        {"purchase_count": -1},
        {"purchase_count": None},
        {"purchase_count": 1.25},
        {"unique_users": 11},
        {"reorder_count": -1},
        {"reorder_count": 11},
        {"reorder_rate": None},
        {"reorder_rate": math.nan},
        {"reorder_rate": math.inf},
        {"run_id": " "},
        {"last_batch_time": None},
        {"product_id": 5},
        {"_id": False},
    ],
)
def test_invalid_product_metric_documents_fail_closed(changes):
    doc = product()
    doc.update(changes)
    database = BatchDatabase([doc])
    with pytest.raises(InvalidBatchDocument):
        read_batch_products(database)


def test_product_rows_with_mixed_run_generation_fail_closed():
    database = BatchDatabase([product(1), product(2, run="other-run")])
    with pytest.raises(InvalidBatchDocument, match="publication identity"):
        read_batch_products(database)


def test_empty_product_collection_is_valid_and_keeps_no_fabricated_run():
    result = read_batch_products(BatchDatabase())
    assert result["items"] == []
    assert result["source"]["run_id"] is None
    assert result["source"]["last_batch_time"] is None


def test_product_mongo_failures_are_sanitized():
    database = BatchDatabase([product()])
    database.products.fail_find = True
    with pytest.raises(BatchDatabaseFailure):
        read_batch_products(database)

    database.products.fail_find = False
    database.products.fail_indexes = True
    with pytest.raises(BatchDatabaseFailure):
        read_batch_products(database)


def test_departments_are_bounded_validated_and_stably_ranked():
    database = BatchDatabase(
        departments=[department(2, purchase=100), department(1, purchase=100), department(3, purchase=200)]
    )
    result = read_batch_departments(database)
    assert [row["department_id"] for row in result["items"]] == [3, 1, 2]
    assert len(result["items"]) == 3
    assert result["source"]["run_id"] == "module2-run"
    cursor = database.departments.cursors[0]
    assert cursor.sort_value == [("_id", 1)]
    assert cursor.limit_value == 22
    assert cursor.closed
    assert "unprojected_secret" not in database.departments.find_calls[0][1]


@pytest.mark.parametrize(
    "changes",
    [
        {"purchase_count": -1},
        {"purchase_count": 0},
        {"unique_users": 101},
        {"reorder_rate": None},
        {"reorder_rate": math.nan},
        {"reorder_rate": math.inf},
        {"department_id": 22},
        {"_id": 8},
        {"_id": True},
    ],
)
def test_invalid_department_metric_documents_fail_closed(changes):
    doc = department()
    doc.update(changes)
    with pytest.raises(InvalidBatchDocument):
        read_batch_departments(BatchDatabase(departments=[doc]))


def test_department_read_rejects_more_than_contract_bound():
    docs = [department(i) for i in range(1, 23)]
    database = BatchDatabase(departments=docs)
    with pytest.raises(InvalidBatchDocument, match="21-document"):
        read_batch_departments(database)
    assert database.departments.cursors[0].limit_value == 22


def test_empty_department_collection_is_valid():
    result = read_batch_departments(BatchDatabase())
    assert result["items"] == []
    assert result["source"]["run_id"] is None


def test_department_mongo_failure_is_sanitized():
    database = BatchDatabase()
    database.departments.fail_find = True
    with pytest.raises(BatchDatabaseFailure):
        read_batch_departments(database)
