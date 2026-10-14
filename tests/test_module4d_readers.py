"""Network-free contract tests for Module 4D read models."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import math

import pytest

from src.module3.trend_bundle import _build_document
from src.module4.hybrid.trend_reader import TrendSnapshotIntegrityError
from src.module4.dashboard.readers import (
    DatabaseReadFailure,
    InvalidSnapshot,
    SnapshotChanged,
    SnapshotNotFound,
    SnapshotNotReady,
    TREND_SELECTION_INDEX_NAME,
    TrendSnapshotIndexUnavailable,
    TrendSnapshotQueryError,
    read_hybrid_snapshot,
    read_trend_snapshot,
)
from tests.module4d_helpers import BASE_TIME, NAMESPACE, make_snapshot


class Cursor:
    def __init__(self, rows):
        self.rows = list(rows)
        self.sort_keys = None
        self.limit_value = None
        self.hint_value = None
        self.max_time = None
        self.closed = False

    def sort(self, keys):
        self.sort_keys = list(keys)
        for key, direction in reversed(self.sort_keys):
            self.rows.sort(key=lambda row: row.get(key), reverse=direction < 0)
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
        self.indexes = (
            {
                "_id_": {"key": [("_id", 1)]},
                TREND_SELECTION_INDEX_NAME: {
                    "key": [("source_namespace", 1), ("window_end", -1)]
                },
            }
            if name == "realtime_trending_windows"
            else {"_id_": {"key": [("_id", 1)]}}
        )
        self.index_information_calls = 0
        self.fail_index_information = False

    def index_information(self):
        self.index_information_calls += 1
        if self.fail_index_information:
            raise OSError("mongodb://private-user:secret@private-host")
        return deepcopy(self.indexes)

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
        elif set(query) == {"source_namespace", "window_end"}:
            namespace = query["source_namespace"]
            cutoff = query["window_end"]["$lte"]

            def as_utc(value):
                if not isinstance(value, datetime):
                    return None
                if value.tzinfo is None or value.utcoffset() is None:
                    return value.replace(tzinfo=timezone.utc)
                return value.astimezone(timezone.utc)

            cutoff_utc = as_utc(cutoff)
            rows = [
                deepcopy(row)
                for row in self.documents
                if row.get("source_namespace") == namespace
                and as_utc(row.get("window_end")) is not None
                and as_utc(row["window_end"]) <= cutoff_utc
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


def _trend_document(window_end, *, namespace=NAMESPACE, product_id=501, scope="historical_test"):
    start = window_end - timedelta(hours=1)
    item = {
        "window_start": start,
        "window_end": window_end,
        "product_id": product_id,
        "purchase_count_30m": 2,
        "purchase_count_120m": 5,
        "trend_score": 0.75,
        "trend_rank": 1,
    }
    return _build_document(
        {"window_start": start, "window_end": window_end, "items": [item], "_row_count": 1},
        source_namespace=namespace,
        source_scope=scope,
        top_k=20,
        config_hash=hashlib.sha256(b"config").hexdigest(),
        published_at=BASE_TIME,
        retention_seconds=0,
    )


def test_trend_reader_passes_c5_selection_unchanged_and_hints_the_verified_index():
    end = BASE_TIME - timedelta(hours=1)
    document = _trend_document(end)
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
    assert query == {"source_namespace": NAMESPACE, "window_end": {"$lte": BASE_TIME}}
    assert kwargs == {"max_time_ms": 5000}
    assert database.trends.cursors[0].hint_value == TREND_SELECTION_INDEX_NAME
    assert database.trends.cursors[0].sort_keys == [("window_end", -1)]
    assert database.trends.cursors[0].limit_value == 1
    assert database.trends.cursors[0].closed
    assert database.trends.index_information_calls == 1
    assert not database.trends.write_calls


def test_newest_noncanonical_id_is_an_integrity_error_without_older_fallback():
    older = _trend_document(BASE_TIME - timedelta(hours=2))
    newer = _trend_document(BASE_TIME - timedelta(hours=1))
    newer["_id"] = "malformed-newest-id"
    database = Database(trend_documents=[older, newer])

    with pytest.raises(TrendSnapshotIntegrityError):
        read_trend_snapshot(database, source_namespace=NAMESPACE, as_of_event_time=BASE_TIME)
    assert database.trends.cursors[0].rows == [newer]


@pytest.mark.parametrize(
    "mutation",
    [
        lambda document: document.update(content_sha256="f" * 64),
        lambda document: document.update(schema_version=999),
    ],
)
def test_corrupt_newest_bundle_is_not_skipped_for_an_older_valid_one(mutation):
    older = _trend_document(BASE_TIME - timedelta(hours=2))
    newer = _trend_document(BASE_TIME - timedelta(hours=1))
    mutation(newer)
    with pytest.raises(TrendSnapshotIntegrityError):
        read_trend_snapshot(
            Database(trend_documents=[older, newer]),
            source_namespace=NAMESPACE,
            as_of_event_time=BASE_TIME,
        )


def test_namespace_prefix_and_similar_character_collisions_do_not_leak():
    exact = _trend_document(BASE_TIME - timedelta(hours=1), product_id=501)
    prefix_collision = _trend_document(
        BASE_TIME - timedelta(minutes=5), namespace=NAMESPACE + "-child", product_id=502
    )
    similar_characters = _trend_document(
        BASE_TIME - timedelta(minutes=3), namespace=NAMESPACE.replace("d1a", "d1A"), product_id=503
    )
    database = Database(trend_documents=[exact, prefix_collision, similar_characters])

    result = read_trend_snapshot(database, source_namespace=NAMESPACE, as_of_event_time=BASE_TIME)

    assert result.available
    assert result.source_namespace == NAMESPACE
    assert [row.product_id for row in result.trend_recommendations] == [501]
    assert database.trends.find_calls[0][0] == {
        "source_namespace": NAMESPACE,
        "window_end": {"$lte": BASE_TIME},
    }


@pytest.mark.parametrize(
    "window_delta,expected_available",
    [
        (timedelta(0), True),
        (-timedelta(milliseconds=1), True),
        (timedelta(milliseconds=1), False),
    ],
)
def test_c5_millisecond_cutoff_includes_equal_and_prior_but_excludes_next(
    window_delta, expected_available
):
    document = _trend_document(BASE_TIME + window_delta)
    result = read_trend_snapshot(
        Database(trend_documents=[document]),
        source_namespace=NAMESPACE,
        as_of_event_time=BASE_TIME,
    )
    assert result.available is expected_available
    assert result.status == ("available" if expected_available else "missing")


def test_bson_decoded_naive_utc_timestamps_preserve_c5_selection():
    from bson import BSON

    document = _trend_document(BASE_TIME)
    decoded = BSON.encode(document).decode()
    assert decoded["window_end"].tzinfo is None
    result = read_trend_snapshot(
        Database(trend_documents=[decoded]),
        source_namespace=NAMESPACE,
        as_of_event_time=BASE_TIME.astimezone(timezone(timedelta(hours=5, minutes=30))),
    )
    assert result.available
    assert result.window_end == BASE_TIME


def test_no_eligible_bundle_returns_c5_missing_result():
    future = _trend_document(BASE_TIME + timedelta(milliseconds=1))
    result = read_trend_snapshot(
        Database(trend_documents=[future]), source_namespace=NAMESPACE, as_of_event_time=BASE_TIME
    )
    assert not result.available
    assert result.status == "missing"


@pytest.mark.parametrize("max_event_age_seconds,expected", [(None, "expired"), (0, "stale")])
def test_expired_or_stale_newest_bundle_does_not_fall_back(max_event_age_seconds, expected):
    older = _trend_document(BASE_TIME - timedelta(hours=2))
    newest = _trend_document(BASE_TIME - timedelta(hours=1))
    if max_event_age_seconds is None:
        newest["published_at"] = BASE_TIME - timedelta(days=2)
        newest["expires_at"] = BASE_TIME - timedelta(days=1)
    else:
        newest["published_at"] = BASE_TIME
        newest.pop("expires_at", None)
    result = read_trend_snapshot(
        Database(trend_documents=[older, newest]),
        source_namespace=NAMESPACE,
        as_of_event_time=BASE_TIME,
        max_event_age_seconds=max_event_age_seconds,
    )
    assert not result.available
    assert result.status == expected


def test_missing_or_unsuitable_selection_index_fails_before_any_find():
    database = Database(trend_documents=[_trend_document(BASE_TIME - timedelta(hours=1))])
    database.trends.indexes = {"_id_": {"key": [("_id", 1)]}}
    with pytest.raises(TrendSnapshotIndexUnavailable):
        read_trend_snapshot(database, source_namespace=NAMESPACE, as_of_event_time=BASE_TIME)
    assert database.trends.find_calls == []

    database.trends.indexes = {
        "wrong_order": {
            "key": [("window_end", -1), ("source_namespace", 1)]
        }
    }
    with pytest.raises(TrendSnapshotIndexUnavailable):
        read_trend_snapshot(database, source_namespace=NAMESPACE, as_of_event_time=BASE_TIME)
    assert database.trends.find_calls == []


@pytest.mark.parametrize(
    "unusable",
    [
        {"partialFilterExpression": {"source_namespace": NAMESPACE}},
        {"hidden": True},
        {"collation": {"locale": "en", "strength": 2}},
    ],
)
def test_partial_hidden_or_non_simple_collation_index_is_not_accepted(unusable):
    database = Database(trend_documents=[_trend_document(BASE_TIME - timedelta(hours=1))])
    database.trends.indexes = {
        "otherwise_compatible": {
            "key": [("source_namespace", 1), ("window_end", -1)],
            **unusable,
        }
    }
    with pytest.raises(TrendSnapshotIndexUnavailable):
        read_trend_snapshot(database, source_namespace=NAMESPACE, as_of_event_time=BASE_TIME)
    assert database.trends.find_calls == []


def test_reverse_scan_compatible_index_is_accepted():
    database = Database(trend_documents=[_trend_document(BASE_TIME - timedelta(hours=1))])
    database.trends.indexes = {
        "reverse_scan_compatible": {
            "key": [("source_namespace", -1), ("window_end", 1), ("_id", 1)]
        }
    }
    result = read_trend_snapshot(database, source_namespace=NAMESPACE, as_of_event_time=BASE_TIME)
    assert result.available
    assert database.trends.cursors[0].hint_value == "reverse_scan_compatible"


def test_index_metadata_failure_is_a_sanitized_query_failure():
    database = Database()
    database.trends.fail_index_information = True
    with pytest.raises(TrendSnapshotQueryError) as error:
        read_trend_snapshot(database, source_namespace=NAMESPACE, as_of_event_time=BASE_TIME)
    assert "private-host" not in str(error.value)
    assert "secret" not in str(error.value)


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
