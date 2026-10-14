"""Network-free contract tests for Module 4D read models."""

from __future__ import annotations

from copy import deepcopy
from datetime import timedelta
import hashlib
import math

import pytest

from src.module3.trend_bundle import _build_document
from src.module4d.readers import (
    DatabaseReadFailure,
    InvalidSnapshot,
    SnapshotChanged,
    SnapshotNotFound,
    SnapshotNotReady,
    read_hybrid_snapshot,
    read_trend_snapshot,
)
from tests.module4d_helpers import BASE_TIME, NAMESPACE, make_snapshot


class Cursor:
    def __init__(self, rows):
        self.rows = list(rows)
        self.sort_keys = None
        self.limit_value = None
        self.max_time = None
        self.closed = False

    def sort(self, keys):
        self.sort_keys = list(keys)
        for key, direction in reversed(self.sort_keys):
            self.rows.sort(key=lambda row: row.get(key), reverse=direction < 0)
        return self

    def hint(self, _value):
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


class Collection:
    def __init__(self, documents=(), name=""):
        self.name = name
        self.documents = [deepcopy(item) for item in documents]
        self.find_calls = []
        self.find_one_calls = []
        self.cursors = []
        self.fail_find = False
        self.fail_find_one = False
        self.find_one_hook = None
        self.write_calls = []

    def find_one(self, query, **kwargs):
        self.find_one_calls.append((deepcopy(query), dict(kwargs)))
        if self.fail_find_one:
            raise OSError("mongodb://private-user:secret@private-host")
        if self.find_one_hook:
            self.find_one_hook(len(self.find_one_calls))
        identity = query.get("_id")
        for row in self.documents:
            if row.get("_id") == identity:
                return deepcopy(row)
        return None

    def find(self, query, **kwargs):
        self.find_calls.append((deepcopy(query), dict(kwargs)))
        if self.fail_find:
            raise OSError("mongodb://private-user:secret@private-host")
        rows = []
        if "_id" in query:
            bounds = query["_id"]
            rows = [
                deepcopy(row)
                for row in self.documents
                if bounds["$gte"] <= row.get("_id", "") <= bounds["$lte"]
            ]
        else:
            rows = [
                deepcopy(row)
                for row in self.documents
                if all(row.get(key) == value for key, value in query.items())
            ]
        cursor = Cursor(rows)
        self.cursors.append(cursor)
        return cursor

    def __getattr__(self, name):
        if name in {"insert_one", "insert_many", "update_one", "replace_one", "delete_one", "delete_many", "drop", "rename", "create_index"}:
            self.write_calls.append(name)
            raise AssertionError(f"read path attempted MongoDB write operation {name}")
        raise AttributeError(name)


class Database:
    def __init__(self, pointer=None, rows=(), trend_documents=()):
        self.readiness = Collection([pointer] if pointer else (), "module4_snapshot_readiness")
        self.recommendations = Collection(rows, "final_recommendations")
        self.trends = Collection(trend_documents, "realtime_trending_windows")
        self.names = []

    def __getitem__(self, name):
        self.names.append(name)
        return {
            "module4_snapshot_readiness": self.readiness,
            "final_recommendations": self.recommendations,
            "realtime_trending_windows": self.trends,
        }[name]


def test_reads_top_twenty_exact_user_and_version_with_limit_and_closes_cursor():
    _, pointer, rows, _ = make_snapshot(als_count=20, trend_count=0, trend_available=False)
    database = Database(pointer, rows)

    result = read_hybrid_snapshot(database, 71)

    assert result["recommendation_count"] == 20
    assert [row["rank"] for row in result["recommendations"]] == list(range(1, 21))
    assert database.recommendations.find_calls[0][0] == {
        "user_id": 71,
        "serving_version": pointer["serving_version"],
    }
    cursor = database.recommendations.cursors[0]
    assert cursor.sort_keys == [("rank", 1)]
    assert cursor.limit_value == 21
    assert cursor.max_time == 5000
    assert cursor.closed
    assert [query["_id"] for query, _ in database.readiness.find_one_calls] == ["hybrid:71", "hybrid:71"]


def test_reads_short_top_k_and_preserves_raw_and_normalized_scores():
    _, pointer, rows, _ = make_snapshot(als_count=1, trend_count=1)
    result = read_hybrid_snapshot(Database(pointer, rows), 71)

    assert result["recommendation_count"] == len(rows) == 2
    assert any(row["from_als"] for row in result["recommendations"])
    assert any(row["from_trend"] for row in result["recommendations"])
    for row in result["recommendations"]:
        stored = next(item for item in rows if item["product_id"] == row["product_id"])
        assert row["als_score"] == stored["als_score"]
        assert row["trend_score"] == stored["trend_score"]


def test_ready_empty_snapshot_is_successful_and_does_not_fabricate_rows():
    _, pointer, rows, _ = make_snapshot(als_count=0, trend_count=0, trend_available=False)
    result = read_hybrid_snapshot(Database(pointer, rows), 71)

    assert pointer["expected_rows"] == 0
    assert result["recommendation_count"] == 0
    assert result["recommendations"] == []
    assert result["reason"]
    assert result["generated_at"] is None
    assert result["ranking_config"]["lambda_requested"] is None


def test_missing_pointer_is_not_found_without_active_pointer_lookup():
    database = Database()
    with pytest.raises(SnapshotNotFound):
        read_hybrid_snapshot(database, 909)
    assert database.readiness.find_one_calls[0][0] == {"_id": "hybrid:909"}
    assert "active" not in [query["_id"] for query, _ in database.readiness.find_one_calls]


def test_historical_hybrid_source_can_differ_from_active_ab_pointer():
    _, pointer, rows, _ = make_snapshot()
    database = Database(pointer, rows)
    database.readiness.documents.append(
        {
            "_id": "active",
            "status": "ready",
            "ready": True,
            "model_version": "a-newer-active-model",
            "run_id": "a-newer-active-run",
            "graph_version": "a-newer-active-graph",
        }
    )

    result = read_hybrid_snapshot(database, 71)

    assert result["source"]["model_version"] == pointer["model_version"]
    assert [query["_id"] for query, _ in database.readiness.find_one_calls] == ["hybrid:71", "hybrid:71"]


def test_nonready_pointer_fails_closed():
    _, pointer, rows, _ = make_snapshot()
    pointer["ready"] = False
    with pytest.raises(SnapshotNotReady):
        read_hybrid_snapshot(Database(pointer, rows), 71)


def test_wrong_pointer_schema_fails_as_not_ready():
    _, pointer, rows, _ = make_snapshot()
    pointer["schema_version"] = 2
    with pytest.raises(SnapshotNotReady):
        read_hybrid_snapshot(Database(pointer, rows), 71)


def test_malformed_pointer_epsilon_is_not_reported_as_corrupt_rows():
    _, pointer, rows, _ = make_snapshot()
    pointer["epsilon"] = math.nan
    with pytest.raises(SnapshotNotReady):
        read_hybrid_snapshot(Database(pointer, rows), 71)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda row: row.update(serving_version="hybrid:" + "f" * 64),
        lambda row: row.update(model_version="other-als-v2"),
        lambda row: row.update(graph_version="other-graph-v2"),
        lambda row: row.update(source_run_id="other-run-002"),
        lambda row: row.update(trend_content_sha256="f" * 64),
        lambda row: row.pop("product_name"),
    ],
)
def test_row_pointer_provenance_and_required_schema_mismatches_fail(mutation):
    _, pointer, rows, _ = make_snapshot()
    mutation(rows[0])
    with pytest.raises(InvalidSnapshot):
        read_hybrid_snapshot(Database(pointer, rows), 71)


def test_row_count_mismatch_fails_closed():
    _, pointer, rows, _ = make_snapshot()
    with pytest.raises(InvalidSnapshot):
        read_hybrid_snapshot(Database(pointer, rows[:-1]), 71)


def test_duplicate_product_fails_closed():
    _, pointer, rows, _ = make_snapshot()
    duplicate = deepcopy(rows[1])
    duplicate["product_id"] = rows[0]["product_id"]
    duplicate["_id"] = rows[0]["_id"]
    duplicate["rank"] = 2
    rows[1] = duplicate
    with pytest.raises(InvalidSnapshot):
        read_hybrid_snapshot(Database(pointer, rows), 71)


@pytest.mark.parametrize("ranks", [(1, 1), (1, 3)])
def test_duplicate_or_noncontiguous_ranks_fail_closed(ranks):
    _, pointer, rows, _ = make_snapshot(als_count=1, trend_count=1)
    rows[0]["rank"], rows[1]["rank"] = ranks
    with pytest.raises(InvalidSnapshot):
        read_hybrid_snapshot(Database(pointer, rows), 71)


def test_modality_nullability_and_nonfinite_score_are_validated():
    _, pointer, rows, _ = make_snapshot()
    rows[0]["final_score"] = math.inf
    with pytest.raises(InvalidSnapshot):
        read_hybrid_snapshot(Database(pointer, rows), 71)
    _, pointer, rows, _ = make_snapshot()
    rows[0]["from_trend"] = False
    rows[0]["trend_score"] = 0.4
    with pytest.raises(InvalidSnapshot):
        read_hybrid_snapshot(Database(pointer, rows), 71)


def test_rows_must_share_ranking_parameters_and_publisher_generation_time():
    _, pointer, rows, _ = make_snapshot()
    rows[1]["lambda_requested"] = 0.9
    with pytest.raises(InvalidSnapshot):
        read_hybrid_snapshot(Database(pointer, rows), 71)

    _, pointer, rows, _ = make_snapshot()
    pointer["updated_at"] += timedelta(seconds=1)
    with pytest.raises(InvalidSnapshot):
        read_hybrid_snapshot(Database(pointer, rows), 71)


def test_pointer_change_between_reads_returns_conflict():
    _, pointer, rows, _ = make_snapshot()
    database = Database(pointer, rows)
    changed = deepcopy(pointer)
    changed["updated_at"] += timedelta(seconds=1)
    database.readiness.find_one_hook = lambda calls: database.readiness.documents.__setitem__(0, changed) if calls == 2 else None

    with pytest.raises(SnapshotChanged):
        read_hybrid_snapshot(database, 71)


def test_mongo_query_error_is_not_misclassified_as_missing_and_is_sanitized_upstack():
    _, pointer, rows, _ = make_snapshot()
    database = Database(pointer, rows)
    database.recommendations.fail_find = True
    with pytest.raises(DatabaseReadFailure) as error:
        read_hybrid_snapshot(database, 71)
    assert "private-host" not in str(error.value)
    assert "secret" not in str(error.value)


def test_trend_reader_uses_only_the_deterministic_id_index_range_and_c5_verifier():
    start = BASE_TIME - timedelta(hours=2)
    end = BASE_TIME - timedelta(hours=1)
    item = {
        "window_start": start,
        "window_end": end,
        "product_id": 501,
        "purchase_count_30m": 2,
        "purchase_count_120m": 5,
        "trend_score": 0.75,
        "trend_rank": 1,
    }
    document = _build_document(
        {"window_start": start, "window_end": end, "items": [item], "_row_count": 1},
        source_namespace=NAMESPACE,
        source_scope="historical_test",
        top_k=20,
        config_hash=hashlib.sha256(b"config").hexdigest(),
        published_at=BASE_TIME,
        retention_seconds=0,
    )
    database = Database(trend_documents=[document])

    result = read_trend_snapshot(
        database,
        source_namespace=NAMESPACE,
        as_of_event_time=BASE_TIME,
    )

    assert result.available
    assert result.source_scope == "historical_test"
    assert result.trend_recommendations[0].trend_score == 0.75
    query, kwargs = database.trends.find_calls[0]
    prefix = f"trend-window:{hashlib.sha256(NAMESPACE.encode()).hexdigest()}:"
    assert query == {"_id": {"$gte": prefix, "$lte": prefix + BASE_TIME.isoformat(timespec="microseconds").replace("+00:00", "Z")}}
    assert kwargs == {"max_time_ms": 5000}
    assert database.trends.cursors[0].sort_keys == [("_id", -1)]
    assert database.trends.cursors[0].limit_value == 1
    assert database.trends.cursors[0].closed
    assert not database.trends.write_calls


@pytest.mark.parametrize(
    "status,expected_reason",
    [("missing", "no_matching_snapshot"), ("expired", "snapshot_expired"), ("stale", "snapshot_event_time_stale")],
)
def test_c5_valid_unavailable_trend_states_are_preserved(status, expected_reason):
    start = BASE_TIME - timedelta(hours=2)
    end = BASE_TIME - timedelta(hours=1)
    documents = []
    age = None
    if status in {"expired", "stale"}:
        item = {
            "window_start": start,
            "window_end": end,
            "product_id": 501,
            "purchase_count_30m": 1,
            "purchase_count_120m": 1,
            "trend_score": 0.5,
            "trend_rank": 1,
        }
        documents.append(_build_document(
            {"window_start": start, "window_end": end, "items": [item], "_row_count": 1},
            source_namespace=NAMESPACE,
            source_scope="historical_test",
            top_k=20,
            config_hash=hashlib.sha256(b"config").hexdigest(),
            published_at=BASE_TIME - timedelta(days=2) if status == "expired" else BASE_TIME,
            retention_seconds=60 if status == "expired" else 0,
        ))
        if status == "stale":
            age = 0
    result = read_trend_snapshot(
        Database(trend_documents=documents),
        source_namespace=NAMESPACE,
        as_of_event_time=BASE_TIME,
        max_event_age_seconds=age,
    )
    assert result.available is False
    assert result.status == status
    assert result.reason == expected_reason
    assert result.trend_recommendations == ()


def test_corrupted_trend_bundle_propagates_integrity_failure():
    start = BASE_TIME - timedelta(hours=2)
    end = BASE_TIME - timedelta(hours=1)
    item = {
        "window_start": start,
        "window_end": end,
        "product_id": 501,
        "purchase_count_30m": 1,
        "purchase_count_120m": 1,
        "trend_score": 0.5,
        "trend_rank": 1,
    }
    document = _build_document(
        {"window_start": start, "window_end": end, "items": [item], "_row_count": 1},
        source_namespace=NAMESPACE,
        source_scope="historical_test",
        top_k=20,
        config_hash=hashlib.sha256(b"config").hexdigest(),
        published_at=BASE_TIME,
        retention_seconds=0,
    )
    document["content_sha256"] = "f" * 64
    with pytest.raises(Exception) as error:
        read_trend_snapshot(
            Database(trend_documents=[document]),
            source_namespace=NAMESPACE,
            as_of_event_time=BASE_TIME,
        )
    assert type(error.value).__name__ == "TrendSnapshotIntegrityError"


def test_trend_mongo_failure_is_a_query_failure_not_missing():
    database = Database()
    database.trends.fail_find = True
    with pytest.raises(Exception) as error:
        read_trend_snapshot(database, source_namespace=NAMESPACE, as_of_event_time=BASE_TIME)
    assert type(error.value).__name__ == "TrendSnapshotQueryError"
