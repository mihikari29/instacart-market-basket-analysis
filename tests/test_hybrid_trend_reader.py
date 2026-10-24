"""Contract tests for the verified atomic Module 3 trend snapshot reader."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest
from bson import BSON

from src.module3.config import Config as Module3Config
from src.module3.trend_bundle import (
    BUNDLE_COLLECTION_NAME,
    DEFAULT_SOURCE_SCOPE,
    build_document,
    _config_payload,
    _sha256,
)
from src.module4.config import Config as Module4Config
from src.module4.hybrid import (
    HybridRankingConfig,
    TrendRecommendation,
    TrendSnapshot,
    TrendSnapshotIntegrityError,
    TrendSnapshotQueryError,
    TrendSnapshotRequestError,
    capture_verified_user_sources,
    rank_hybrid_recommendations,
    read_verified_trend_snapshot,
)
from src.module4.hybrid import data as module4c_data


NAMESPACE = "fixture:trend-reader:v1"
BASE_START = datetime(2024, 1, 1, tzinfo=timezone.utc)
PROCESSING_TIME = datetime(2025, 1, 1, tzinfo=timezone.utc)
RUN_ID = "reader-fixture-run"
MODEL_VERSION = "reader-als-v1"
GRAPH_VERSION = "reader-graph-v1"


def _bundle_document(
    start=BASE_START,
    *,
    specifications=((101, 20, 40, 0.75), (102, 15, 40, 0.5)),
    source_namespace=NAMESPACE,
    source_scope=DEFAULT_SOURCE_SCOPE,
    top_k=20,
    published_at=datetime(2024, 1, 1, 3, tzinfo=timezone.utc),
    retention_seconds=0,
):
    config = Module3Config(mongo_top_k_per_window=top_k, ttl_seconds=0)
    items = [
        {
            "product_id": product_id,
            "purchase_count_30m": count_30m,
            "purchase_count_120m": count_120m,
            "trend_score": score,
            "trend_rank": rank,
        }
        for rank, (product_id, count_30m, count_120m, score) in enumerate(
            specifications, start=1
        )
    ]
    config_hash = _sha256(_config_payload(config, top_k))
    return build_document(
        {
            "window_start": start,
            "window_end": start + timedelta(hours=2),
            "items": items,
            "_row_count": len(items),
        },
        source_namespace=source_namespace,
        source_scope=source_scope,
        top_k=top_k,
        config_hash=config_hash,
        published_at=published_at,
        retention_seconds=retention_seconds,
    )


def _utc(value):
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


class FakeCursor:
    def __init__(self, documents):
        self.documents = list(documents)
        self.sort_fields = None
        self.limit_value = None
        self.iterated = 0

    def sort(self, fields):
        self.sort_fields = list(fields)
        for field, direction in reversed(self.sort_fields):
            self.documents.sort(
                key=lambda document: (
                    0,
                    _utc(document[field]),
                )
                if isinstance(document.get(field), datetime)
                else (1, repr(document.get(field))),
                reverse=direction < 0,
            )
        return self

    def limit(self, value):
        self.limit_value = value
        self.documents = self.documents[:value]
        return self

    def __iter__(self):
        for document in self.documents:
            self.iterated += 1
            yield document


class FakeBundleCollection:
    name = BUNDLE_COLLECTION_NAME

    def __init__(self, documents=(), *, force_documents=False, error=None):
        self.documents = list(documents)
        self.force_documents = force_documents
        self.error = error
        self.find_calls = []
        self.cursors = []

    def find(self, query):
        self.find_calls.append(query)
        if self.error is not None:
            raise self.error
        if self.force_documents:
            selected = list(self.documents)
        else:
            namespace = query["source_namespace"]
            cutoff = _utc(query["window_end"]["$lte"])
            selected = [
                document
                for document in self.documents
                if document.get("source_namespace") == namespace
                and _utc(document.get("window_end")) <= cutoff
            ]
        cursor = FakeCursor(selected)
        self.cursors.append(cursor)
        return cursor


def _read(collection, *, as_of=BASE_START + timedelta(hours=8), **kwargs):
    processing_time = kwargs.pop("processing_time_utc", PROCESSING_TIME)
    return read_verified_trend_snapshot(
        collection,
        source_namespace=NAMESPACE,
        as_of_event_time=as_of,
        processing_time_utc=processing_time,
        **kwargs,
    )


def test_latest_eligible_bundle_uses_namespace_filter_descending_sort_and_limit_one():
    older = _bundle_document(BASE_START)
    latest = _bundle_document(BASE_START + timedelta(hours=2))
    future = _bundle_document(BASE_START + timedelta(hours=4))
    other_namespace = _bundle_document(
        BASE_START + timedelta(hours=4), source_namespace="fixture:other:v1"
    )
    collection = FakeBundleCollection([older, latest, future, other_namespace])

    result = _read(collection, as_of=BASE_START + timedelta(hours=4))

    assert result.available is True
    assert result.window_end == latest["window_end"]
    assert collection.find_calls == [
        {
            "source_namespace": NAMESPACE,
            "window_end": {"$lte": BASE_START + timedelta(hours=4)},
        }
    ]
    cursor = collection.cursors[0]
    assert cursor.sort_fields == [("window_end", -1)]
    assert cursor.limit_value == 1
    assert cursor.iterated == 1


def test_window_ending_exactly_at_as_of_boundary_is_eligible():
    document = _bundle_document()
    collection = FakeBundleCollection([document])

    result = _read(collection, as_of=document["window_end"])

    assert result.available is True
    assert result.window_end == document["window_end"]


def test_submillisecond_as_of_is_floored_for_bson_query_and_checked_exactly():
    boundary = BASE_START + timedelta(hours=2, microseconds=123_000)
    eligible = _bundle_document(BASE_START + timedelta(microseconds=123_000))
    future = _bundle_document(BASE_START + timedelta(microseconds=124_000))
    collection = FakeBundleCollection([eligible, future])
    as_of = boundary + timedelta(microseconds=456)

    result = _read(collection, as_of=as_of)

    assert result.available is True
    assert result.window_end == eligible["window_end"]
    assert collection.find_calls[0]["window_end"]["$lte"] == boundary
    assert result.window_end <= as_of


def test_future_window_is_excluded_and_namespace_isolation_is_exact():
    future = _bundle_document(BASE_START + timedelta(hours=2))
    another_namespace = _bundle_document(
        BASE_START, source_namespace="fixture:another-namespace:v1"
    )

    result = _read(
        FakeBundleCollection([future, another_namespace]),
        as_of=BASE_START + timedelta(hours=2),
    )

    assert result.available is False
    assert result.status == "missing"
    assert result.reason == "no_matching_snapshot"
    assert result.trend_recommendations == ()
    assert result.trend_snapshot == TrendSnapshot(
        available=False,
        unavailable_reason="no_matching_snapshot",
    )


def test_missing_snapshot_is_an_explicit_unavailable_result():
    result = _read(FakeBundleCollection())

    assert result.available is False
    assert result.status == "missing"
    assert result.source_namespace == NAMESPACE
    assert result.source_scope is None
    assert result.window_start is None
    assert result.content_sha256 is None


def test_valid_all_zero_score_snapshot_is_available():
    document = _bundle_document(
        specifications=((501, 0, 0, 0.0), (502, 0, 0, 0.0))
    )

    result = _read(FakeBundleCollection([document]))

    assert result.available is True
    assert [row.trend_score for row in result.trend_recommendations] == [0.0, 0.0]
    assert result.trend_snapshot.available is True


def test_explicit_event_age_policy_marks_latest_snapshot_stale_without_fallback():
    older = _bundle_document(BASE_START)
    latest = _bundle_document(BASE_START + timedelta(hours=2))
    collection = FakeBundleCollection([older, latest])

    result = _read(
        collection,
        as_of=BASE_START + timedelta(hours=8),
        max_event_age=timedelta(hours=1),
    )

    assert result.available is False
    assert result.status == "stale"
    assert result.reason == "snapshot_event_time_stale"
    assert result.window_end == latest["window_end"]
    assert result.trend_recommendations == ()
    assert collection.cursors[0].limit_value == 1


def test_no_implicit_event_time_staleness_and_historical_time_with_future_expiration():
    document = _bundle_document(
        BASE_START,
        published_at=datetime(2030, 1, 1, tzinfo=timezone.utc),
        retention_seconds=3600,
    )
    old_processing_time = datetime(2029, 12, 31, tzinfo=timezone.utc)

    result = read_verified_trend_snapshot(
        FakeBundleCollection([document]),
        source_namespace=NAMESPACE,
        as_of_event_time=BASE_START + timedelta(hours=8),
        processing_time_utc=old_processing_time,
    )

    assert result.available is True
    assert result.status == "available"
    assert result.window_end == BASE_START + timedelta(hours=2)
    assert result.expires_at == document["expires_at"]


def test_expired_latest_snapshot_is_unavailable_without_older_fallback():
    older = _bundle_document(BASE_START)
    latest = _bundle_document(
        BASE_START + timedelta(hours=2),
        published_at=datetime(2024, 1, 1, 3, tzinfo=timezone.utc),
        retention_seconds=60,
    )
    collection = FakeBundleCollection([older, latest])

    result = _read(
        collection,
        as_of=BASE_START + timedelta(hours=8),
        processing_time_utc=latest["expires_at"],
    )

    assert result.available is False
    assert result.status == "expired"
    assert result.reason == "snapshot_expired"
    assert result.window_end == latest["window_end"]
    assert result.trend_recommendations == ()


def test_fixed_c4a_fixture_digest_and_identity_are_independently_known():
    config = Module3Config(mongo_top_k_per_window=20, ttl_seconds=0)
    config_hash = _sha256(_config_payload(config, 20))
    document = build_document(
        {
            "window_start": datetime(2024, 1, 1, tzinfo=timezone.utc),
            "window_end": datetime(2024, 1, 1, 2, tzinfo=timezone.utc),
            "items": [
                {
                    "product_id": 11,
                    "purchase_count_30m": 20,
                    "purchase_count_120m": 40,
                    "trend_score": 0.95,
                    "trend_rank": 1,
                }
            ],
            "_row_count": 1,
        },
        source_namespace="instacart:fixture-a",
        source_scope=DEFAULT_SOURCE_SCOPE,
        top_k=20,
        config_hash=config_hash,
        published_at=datetime(2024, 1, 1, 3, tzinfo=timezone.utc),
        retention_seconds=0,
    )
    collection = FakeBundleCollection([document])

    result = read_verified_trend_snapshot(
        collection,
        source_namespace="instacart:fixture-a",
        as_of_event_time=document["window_end"],
        processing_time_utc=PROCESSING_TIME,
    )

    assert result.content_sha256 == "942795d87ff1332f9573a0a88ee8133af6b9fb0fcd823ab9ec7429608526a866"
    assert result.available is True
    assert document["_id"] == (
        "trend-window:e0b059af7cf1417f20691e5ef8ac9c5039622f90e8342a96d307b8ac5b35996a:"
        "2024-01-01T02:00:00.000000Z"
    )


def _mutate(document, mutation):
    changed = dict(document)
    changed["items"] = [dict(item) for item in document.get("items", [])]
    mutation(changed)
    return changed


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda doc: doc.update(schema_version=999), "schema_version"),
        (lambda doc: doc.update(top_k=0), "top_k"),
        (lambda doc: doc.update(top_k=21), "top_k"),
        (lambda doc: doc.update(_id="wrong-identity"), "_id"),
        (lambda doc: doc.update(source_namespace="fixture:wrong:v1"), "source_namespace"),
        (lambda doc: doc.update(content_sha256="not-a-digest"), "content_sha256"),
        (lambda doc: doc["items"][0].update(trend_score=0.25), "content_sha256"),
        (lambda doc: doc.update(config_hash="not-a-digest"), "config_hash"),
        (lambda doc: doc.update(row_count=99), "row_count"),
        (lambda doc: doc["items"][1].update(product_id=101), "duplicate product_id"),
        (lambda doc: doc["items"][1].update(trend_rank=1), "contiguous ranks"),
        (lambda doc: doc["items"][1].update(trend_rank=3), "contiguous ranks"),
        (lambda doc: doc["items"][0].pop("trend_rank"), "exactly the C4A item fields"),
        (lambda doc: doc["items"][0].update(purchase_count_30m=-1), "purchase_count_30m"),
        (lambda doc: doc["items"][0].update(purchase_count_120m=-1), "purchase_count_120m"),
        (
            lambda doc: doc["items"][0].update(purchase_count_30m=41),
            "cannot exceed purchase_count_120m",
        ),
        (lambda doc: doc["items"][0].update(trend_score=float("nan")), "finite"),
        (lambda doc: doc["items"][0].update(trend_score=float("inf")), "finite"),
        (lambda doc: doc["items"][0].update(trend_score=-0.1), r"\[0, 1\]"),
        (lambda doc: doc["items"][0].update(trend_score=1.1), r"\[0, 1\]"),
    ],
)
def test_corrupt_bundle_contracts_raise_integrity_error(mutation, message):
    document = _mutate(_bundle_document(), mutation)

    with pytest.raises(TrendSnapshotIntegrityError, match=message):
        _read(FakeBundleCollection([document], force_documents=True))


def test_invalid_config_hash_with_valid_hex_but_old_content_digest_is_rejected():
    document = _mutate(
        _bundle_document(),
        lambda value: value.update(config_hash="f" * 64),
    )

    with pytest.raises(TrendSnapshotIntegrityError, match="content_sha256"):
        _read(FakeBundleCollection([document], force_documents=True))


def test_bundle_with_more_than_twenty_items_is_rejected():
    specifications = tuple((product_id, 10, 20, 0.5) for product_id in range(1, 21))
    document = _bundle_document(specifications=specifications)
    changed = _mutate(
        document,
        lambda value: value["items"].append(
            {
                "product_id": 21,
                "purchase_count_30m": 1,
                "purchase_count_120m": 2,
                "trend_score": 0.1,
                "trend_rank": 21,
            }
        ),
    )
    changed["row_count"] = 21

    with pytest.raises(TrendSnapshotIntegrityError, match="exceeds top_k"):
        _read(FakeBundleCollection([changed], force_documents=True))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("window_start", None),
        ("window_start", "2024-01-01T00:00:00Z"),
        ("window_end", None),
        ("window_end", "2024-01-01T02:00:00Z"),
    ],
)
def test_malformed_window_timestamps_are_integrity_errors(field, value):
    document = _mutate(_bundle_document(), lambda doc: doc.update({field: value}))

    with pytest.raises(TrendSnapshotIntegrityError, match="window timestamp is invalid"):
        _read(FakeBundleCollection([document], force_documents=True))


def test_non_utc_and_submillisecond_legacy_window_instants_are_rejected_or_normalized():
    bson_naive = BSON.encode(_bundle_document()).decode()
    naive_result = _read(FakeBundleCollection([bson_naive]))
    assert naive_result.available is True
    assert naive_result.window_start.tzinfo is timezone.utc

    document = _bundle_document()
    non_utc = timezone(timedelta(hours=-5))
    document["window_start"] = document["window_start"].astimezone(non_utc)
    document["window_end"] = document["window_end"].astimezone(non_utc)
    offset_result = _read(FakeBundleCollection([document], force_documents=True))
    assert offset_result.available is True
    assert offset_result.window_start == _utc(bson_naive["window_start"])

    legacy = _mutate(
        _bundle_document(),
        lambda doc: doc.update(
            window_start=doc["window_start"].replace(microsecond=1)
        ),
    )
    with pytest.raises(TrendSnapshotIntegrityError, match="millisecond precision"):
        _read(FakeBundleCollection([legacy], force_documents=True))


def test_source_scope_is_preserved_as_descriptive_metadata():
    document = _bundle_document(source_scope="prior_only")

    result = _read(FakeBundleCollection([document]))

    assert result.source_scope == "prior_only"
    assert result.status == "available"


def test_wrong_collection_name_and_naive_request_time_are_rejected():
    class WrongCollection(FakeBundleCollection):
        name = "realtime_trending"

    with pytest.raises(TrendSnapshotRequestError, match="realtime_trending_windows"):
        _read(WrongCollection())
    with pytest.raises(TrendSnapshotRequestError, match="timezone-aware"):
        _read(FakeBundleCollection(), as_of=datetime(2024, 1, 1))


def test_query_result_is_revalidated_for_namespace_and_as_of_constraints():
    wrong_namespace = _bundle_document(source_namespace="fixture:wrong:v1")
    with pytest.raises(TrendSnapshotIntegrityError, match="does not match the request"):
        _read(FakeBundleCollection([wrong_namespace], force_documents=True))

    future = _bundle_document(BASE_START + timedelta(hours=8))
    with pytest.raises(TrendSnapshotIntegrityError, match="exceeds as_of_event_time"):
        _read(FakeBundleCollection([future], force_documents=True))


def test_corrupt_newest_bundle_raises_without_falling_back_to_older_snapshot():
    older = _bundle_document(BASE_START)
    newest = _mutate(
        _bundle_document(BASE_START + timedelta(hours=2)),
        lambda doc: doc["items"][0].update(trend_score=0.25),
    )
    collection = FakeBundleCollection([older, newest])

    with pytest.raises(TrendSnapshotIntegrityError, match="content_sha256"):
        _read(collection)

    assert collection.cursors[0].limit_value == 1
    assert collection.cursors[0].iterated == 1


def test_database_query_failure_is_not_reported_as_missing():
    collection = FakeBundleCollection(error=OSError("injected Mongo read failure"))

    with pytest.raises(TrendSnapshotQueryError, match="latest atomic trend bundle") as raised:
        _read(collection)

    assert isinstance(raised.value.__cause__, OSError)


def test_repeated_reads_are_deterministic():
    collection = FakeBundleCollection([_bundle_document()])

    first = _read(collection)
    second = _read(collection)

    assert first == second


class FakeC3Cursor:
    def __init__(self, rows):
        self.rows = list(rows)

    def sort(self, fields):
        for field, direction in reversed(fields):
            self.rows.sort(key=lambda row: row.get(field), reverse=direction < 0)
        return self

    def limit(self, value):
        self.rows = self.rows[:value]
        return self

    def __iter__(self):
        return iter(self.rows)


class FakeC3Collection:
    def __init__(self, rows=(), one=None):
        self.rows = list(rows)
        self.one = one
        self.find_calls = []

    def find_one(self, query):
        assert query == {"_id": "active"}
        return self.one

    def find(self, query, projection):
        self.find_calls.append((query, projection))
        selected = []
        for row in self.rows:
            if all(
                row.get(key) in value["$in"]
                if isinstance(value, dict) and "$in" in value
                else row.get(key) == value
                for key, value in query.items()
            ):
                selected.append(
                    {
                        key: row[key]
                        for key, enabled in projection.items()
                        if enabled and key in row
                    }
                )
        return FakeC3Cursor(selected)


class FakeC3Database:
    def __init__(self, collections):
        self.collections = collections

    def __getitem__(self, name):
        return self.collections[name]


class FakeC3Client:
    def __init__(self, database):
        self.database = database

    def __getitem__(self, name):
        assert name == "instacart"
        return self.database


def test_reader_output_flows_through_c3_catalog_and_hybrid_ranker(monkeypatch):
    trend_doc = _bundle_document(
        specifications=((102, 8, 20, 0.8), (103, 4, 10, 0.4)),
        source_scope="controlled_synthetic",
    )
    trend_result = _read(FakeBundleCollection([trend_doc]))
    assert isinstance(trend_result.trend_snapshot, TrendSnapshot)
    assert trend_result.trend_recommendations == (
        TrendRecommendation(product_id=102, trend_score=0.8, trend_rank=1),
        TrendRecommendation(product_id=103, trend_score=0.4, trend_rank=2),
    )

    readiness = {
        "_id": "active",
        "status": "ready",
        "ready": True,
        "run_id": RUN_ID,
        "model_version": MODEL_VERSION,
        "graph_version": GRAPH_VERSION,
        "recommendation_published": True,
        "graph_published": True,
        "single_writer_policy": "module4_publication_lock",
    }
    als_rows = [
        {
            "user_id": 7,
            "product_id": product_id,
            "als_score": score,
            "als_rank": rank,
            "model_version": MODEL_VERSION,
            "run_id": RUN_ID,
        }
        for rank, (product_id, score) in enumerate(((101, 10.0), (102, 5.0)), start=1)
    ]
    catalog_ids = (101, 102, 103)
    graph_rows = [
        {
            "product_id": product_id,
            "product_name": f"Product {product_id}",
            "graph_version": GRAPH_VERSION,
            "run_id": RUN_ID,
        }
        for product_id in catalog_ids
    ]
    collections = {
        module4c_data.READINESS_COLLECTION: FakeC3Collection(one=readiness),
        "user_recommendations": FakeC3Collection(rows=als_rows),
        "product_graph_metrics": FakeC3Collection(rows=graph_rows),
    }
    client = FakeC3Client(FakeC3Database(collections))

    @contextmanager
    def fake_publication_lock(_config, _operation_id, lease_minutes=180):
        assert lease_minutes == 180
        yield client

    monkeypatch.setattr(module4c_data, "publication_lock", fake_publication_lock)
    captured = capture_verified_user_sources(
        Module4Config(recommendation_k=2),
        7,
        prospective_trend_product_ids=(
            row.product_id for row in trend_result.trend_recommendations
        ),
    )

    graph_query = collections["product_graph_metrics"].find_calls[0][0]
    assert set(graph_query["product_id"]["$in"]) == set(catalog_ids)
    assert captured.missing_prospective_trend_product_ids == ()
    assert {row.product_id for row in captured.product_catalog} == set(catalog_ids)

    ranking_config = HybridRankingConfig(lambda_weight=0.5)
    ranked = rank_hybrid_recommendations(
        captured.user_id,
        captured.als_recommendations,
        trend_result.trend_snapshot,
        captured.product_catalog,
        captured.source_context,
        ranking_config,
    )
    epsilon = ranking_config.epsilon
    expected_scores = {
        101: 0.5 * (5.0 / (5.0 + epsilon)),
        102: 0.5 * (0.8 / (0.8 + epsilon)),
        103: 0.5 * (0.4 / (0.8 + epsilon)),
    }

    assert [row.product_id for row in ranked.recommendations] == [101, 102, 103]
    assert ranked.metadata.candidate_count == 3
    assert ranked.metadata.mode == "hybrid"
    assert [row.final_score for row in ranked.recommendations] == pytest.approx(
        [expected_scores[product_id] for product_id in (101, 102, 103)]
    )
    assert ranked.recommendations[1].trend_score == 0.8
    assert ranked.recommendations[2].trend_score == 0.4
