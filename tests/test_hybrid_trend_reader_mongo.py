"""Live MongoDB contract checks for the verified Module 4C trend reader."""

from __future__ import annotations

from tests.helpers import FindCommandRecorder

from datetime import datetime, timedelta, timezone
import hashlib
import os
import uuid

import pytest
from pymongo import MongoClient
from pymongo.uri_parser import parse_uri

from src.module3.trend_bundle import (
    BUNDLE_COLLECTION_NAME,
    BUNDLE_SCHEMA_VERSION,
    build_document,
)
from src.module4.hybrid import (
    ALSRecommendation,
    HybridRankingConfig,
    ProductMetadata,
    VerifiedSourceContext,
    rank_hybrid_recommendations,
    read_verified_trend_snapshot,
)
from src.module4.hybrid.trend_reader import (
    TrendSnapshotIntegrityError,
    TrendSnapshotQueryError,
)


pytestmark = pytest.mark.integration

TEST_URI_ENV = "I2_MONGODB_TEST_URI"
ALLOWED_HOST = ("i2-mongo", 27017)
FORBIDDEN_DATABASES = {"admin", "config", "instacart", "local"}
NAMESPACE = "i2:module3:trend-reader-v1"
SCOPE = "historical_fixture"
PUBLISHED_AT = datetime(2026, 1, 1, tzinfo=timezone.utc)
BASE_START = datetime(2024, 3, 1, 12, 0, tzinfo=timezone.utc)




def _dedicated_uri() -> str:
    uri = os.environ.get(TEST_URI_ENV)
    if not uri:
        pytest.skip(f"{TEST_URI_ENV} is not set; live C5 tests require the isolated I2 MongoDB service")
    try:
        parsed = parse_uri(uri)
    except Exception as exc:
        pytest.fail(f"{TEST_URI_ENV} is not a valid MongoDB URI: {exc}")
    hosts = parsed.get("nodelist", ())
    if len(hosts) != 1 or (hosts[0][0].lower(), hosts[0][1]) != ALLOWED_HOST:
        pytest.fail(f"{TEST_URI_ENV} must target only the isolated Docker alias i2-mongo:27017")
    if parsed.get("username") is not None or parsed.get("password") is not None:
        pytest.fail(f"{TEST_URI_ENV} must not contain credentials")
    if parsed.get("database") not in (None, ""):
        pytest.fail(f"{TEST_URI_ENV} must not select a database; tests create owned databases")
    if parsed.get("options"):
        pytest.fail(f"{TEST_URI_ENV} must not contain connection options")
    return uri


@pytest.mark.parametrize(
    "unsafe_uri",
    [
        "mongodb://localhost:27017",
        "mongodb://127.0.0.1:27017",
        "mongodb://i1-mongo:27017",
        "mongodb://i2-mongo:27017/instacart",
        "mongodb://user:secret@i2-mongo:27017",
        "mongodb://i2-mongo:27017/?authSource=admin",
        "not-a-mongodb-uri",
    ],
)
def test_uri_guard_rejects_unsafe_hosts_credentials_and_database_targets(monkeypatch, unsafe_uri):
    monkeypatch.setenv(TEST_URI_ENV, unsafe_uri)
    with pytest.raises(BaseException) as error:
        _dedicated_uri()
    assert not isinstance(error.value, pytest.skip.Exception)


def test_uri_guard_skips_clearly_when_environment_variable_is_absent(monkeypatch):
    monkeypatch.delenv(TEST_URI_ENV, raising=False)
    with pytest.raises(pytest.skip.Exception, match=TEST_URI_ENV):
        _dedicated_uri()


@pytest.fixture
def live_database():
    uri = _dedicated_uri()
    recorder = FindCommandRecorder()
    client = MongoClient(
        uri,
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=5000,
        tz_aware=True,
        event_listeners=[recorder],
    )
    database_name = f"i2_module4c_c5_{uuid.uuid4().hex}"
    if database_name.lower() in FORBIDDEN_DATABASES:
        client.close()
        pytest.fail("generated an unsafe integration database name")
    owned = False
    try:
        client.admin.command("ping")
        version = client.server_info()["version"]
        if not version.startswith("7.0.16"):
            pytest.fail(f"I2 requires MongoDB 7.0.16, received {version}")
        if database_name in client.list_database_names():
            pytest.fail("generated integration database already exists")
        owned = True
        database = client[database_name]
        database["_i2_test_ownership"].insert_one({"_id": "created-by-i2-c5-test"})
        yield client, database, recorder
    finally:
        try:
            if owned:
                client.drop_database(database_name)
        finally:
            client.close()


def _bundle_document(
    *,
    source_namespace=NAMESPACE,
    source_scope=SCOPE,
    window_start=BASE_START,
    window_end=None,
    scores=(0.9, 0.4),
    product_ids=(101, 102),
    published_at=PUBLISHED_AT,
    retention_seconds=0,
):
    if window_end is None:
        window_end = window_start + timedelta(hours=1)
    rows = [
        {
            "window_start": window_start,
            "window_end": window_end,
            "product_id": product_id,
            "purchase_count_30m": rank,
            "purchase_count_120m": rank + 2,
            "trend_score": float(score),
            "trend_rank": rank,
        }
        for rank, (product_id, score) in enumerate(zip(product_ids, scores), start=1)
    ]
    return build_document(
        {
            "window_start": window_start,
            "window_end": window_end,
            "items": rows,
            "_row_count": len(rows),
        },
        source_namespace=source_namespace,
        source_scope=source_scope,
        top_k=20,
        config_hash=hashlib.sha256(b"i2-c4a-config").hexdigest(),
        published_at=published_at,
        retention_seconds=retention_seconds,
    )


def _insert_bundle(database, **kwargs):
    document = _bundle_document(**kwargs)
    database[BUNDLE_COLLECTION_NAME].insert_one(document)
    return document


def _read(collection, *, namespace=NAMESPACE, as_of, **kwargs):
    return read_verified_trend_snapshot(
        collection,
        source_namespace=namespace,
        as_of_event_time=as_of,
        **kwargs,
    )


def test_live_reader_validates_persisted_bundle_identity_hash_bson_and_scores(live_database):
    _client, database, _recorder = live_database
    collection = database[BUNDLE_COLLECTION_NAME]
    start = BASE_START
    end = start + timedelta(hours=1)
    expected = _insert_bundle(
        database,
        window_start=start,
        window_end=end,
        scores=(0.875, 0.125),
    )

    result = _read(collection, as_of=end)
    stored = collection.find_one({"_id": expected["_id"]})

    assert result.available is True
    assert result.status == "available"
    assert result.reason is None
    assert result.schema_version == BUNDLE_SCHEMA_VERSION
    assert result.source_namespace == NAMESPACE
    assert result.source_scope == SCOPE
    assert result.content_sha256 == expected["content_sha256"]
    assert result.window_start == start
    assert result.window_end == end
    assert stored["_id"] == expected["_id"]
    assert stored["content_sha256"] == expected["content_sha256"]
    assert stored["window_start"] == start and stored["window_start"].tzinfo is not None
    assert stored["window_end"] == end and stored["window_end"].tzinfo is not None
    assert [(row.product_id, row.trend_score, row.trend_rank) for row in result.trend_recommendations] == [
        (101, 0.875, 1),
        (102, 0.125, 2),
    ]


def test_live_reader_filters_namespace_and_selects_latest_eligible_window(live_database):
    _client, database, _recorder = live_database
    collection = database[BUNDLE_COLLECTION_NAME]
    first_end = BASE_START + timedelta(hours=1)
    second_end = BASE_START + timedelta(hours=2)
    future_end = BASE_START + timedelta(hours=3)
    _insert_bundle(database, window_end=first_end)
    expected = _insert_bundle(
        database,
        window_start=BASE_START + timedelta(hours=1),
        window_end=second_end,
        product_ids=(201, 202),
    )
    _insert_bundle(
        database,
        source_namespace="i2:other-source",
        window_start=BASE_START + timedelta(hours=2),
        window_end=future_end,
        product_ids=(301, 302),
    )

    result = _read(collection, as_of=future_end)

    assert result.available is True
    assert result.window_end == second_end
    assert result.source_namespace == NAMESPACE
    assert result.content_sha256 == expected["content_sha256"]


def test_live_reader_includes_exact_millisecond_boundary_and_excludes_future_window(live_database):
    _client, database, _recorder = live_database
    collection = database[BUNDLE_COLLECTION_NAME]
    end = BASE_START + timedelta(hours=1, milliseconds=1)
    _insert_bundle(database, window_end=end)
    later = end + timedelta(milliseconds=1)
    _insert_bundle(
        database,
        window_start=end,
        window_end=later,
        product_ids=(201, 202),
    )

    exact = _read(collection, as_of=end)
    before = _read(collection, as_of=end - timedelta(milliseconds=1))

    assert exact.available is True and exact.window_end == end
    assert before.status == "missing" and before.reason == "no_matching_snapshot"


def test_live_reader_floors_submillisecond_query_boundary_then_checks_exact_time(live_database):
    _client, database, _recorder = live_database
    collection = database[BUNDLE_COLLECTION_NAME]
    end = BASE_START + timedelta(hours=1, milliseconds=123)
    _insert_bundle(database, window_end=end)

    eligible = _read(collection, as_of=end + timedelta(microseconds=999))
    ineligible = _read(collection, as_of=end - timedelta(microseconds=1))

    assert eligible.available is True and eligible.window_end == end
    assert ineligible.status == "missing"


def test_live_reader_accepts_a_valid_all_zero_score_window(live_database):
    _client, database, _recorder = live_database
    end = BASE_START + timedelta(hours=1)
    _insert_bundle(database, window_end=end, scores=(0.0, 0.0))

    result = _read(database[BUNDLE_COLLECTION_NAME], as_of=end)

    assert result.available is True
    assert [row.trend_score for row in result.trend_recommendations] == [0.0, 0.0]


def test_live_reader_reports_missing_snapshot_expiry_and_historical_event_time_policy(live_database):
    _client, database, _recorder = live_database
    collection = database[BUNDLE_COLLECTION_NAME]
    missing = _read(collection, as_of=BASE_START + timedelta(hours=2))
    assert missing.available is False
    assert missing.status == "missing"
    assert missing.reason == "no_matching_snapshot"

    old_end = BASE_START + timedelta(hours=1)
    old_publish = datetime(2020, 1, 1, tzinfo=timezone.utc)
    _insert_bundle(
        database,
        window_end=old_end,
        published_at=old_publish,
        retention_seconds=60,
    )
    processing_time = old_publish + timedelta(seconds=61)
    expired = _read(
        collection,
        as_of=old_end,
        processing_time_utc=processing_time,
    )
    historical = _read(
        collection,
        as_of=old_end,
        processing_time_utc=old_publish + timedelta(seconds=30),
    )

    assert expired.available is False
    assert expired.status == "expired"
    assert expired.reason == "snapshot_expired"
    assert historical.available is True


def test_live_reader_applies_only_explicit_maximum_event_age(live_database):
    _client, database, _recorder = live_database
    collection = database[BUNDLE_COLLECTION_NAME]
    end = BASE_START + timedelta(hours=1)
    _insert_bundle(database, window_end=end)
    request_time = end + timedelta(days=30)

    default_policy = _read(collection, as_of=request_time)
    bounded_policy = _read(
        collection,
        as_of=request_time,
        max_event_age=timedelta(days=1),
    )

    assert default_policy.available is True
    assert bounded_policy.available is False
    assert bounded_policy.status == "stale"
    assert bounded_policy.reason == "snapshot_event_time_stale"


@pytest.mark.parametrize("corruption", ["item", "rank", "hash", "identity"])
def test_live_reader_rejects_tampered_bundle_content_and_identity(live_database, corruption):
    _client, database, _recorder = live_database
    collection = database[BUNDLE_COLLECTION_NAME]
    end = BASE_START + timedelta(hours=1)
    document = _insert_bundle(
        database,
        window_end=end,
        scores=(0.8,),
        product_ids=(501,),
    )
    if corruption == "item":
        changed = [dict(item) for item in document["items"]]
        changed[0]["purchase_count_120m"] += 1
        collection.update_one({"_id": document["_id"]}, {"$set": {"items": changed}})
    elif corruption == "rank":
        changed = [dict(item) for item in document["items"]]
        changed[0]["trend_rank"] = 2
        collection.update_one({"_id": document["_id"]}, {"$set": {"items": changed}})
    elif corruption == "hash":
        collection.update_one(
            {"_id": document["_id"]}, {"$set": {"content_sha256": "f" * 64}}
        )
    else:
        collection.delete_one({"_id": document["_id"]})
        collection.insert_one({**document, "_id": "tampered-id"})

    with pytest.raises(TrendSnapshotIntegrityError):
        _read(collection, as_of=end)


def test_live_corrupt_newest_window_does_not_fall_back_to_older_valid_window(live_database):
    _client, database, _recorder = live_database
    collection = database[BUNDLE_COLLECTION_NAME]
    old_end = BASE_START + timedelta(hours=1)
    new_end = BASE_START + timedelta(hours=2)
    _insert_bundle(database, window_end=old_end)
    newest = _insert_bundle(
        database,
        window_start=BASE_START + timedelta(hours=1),
        window_end=new_end,
        product_ids=(201, 202),
    )
    changed = [dict(item) for item in newest["items"]]
    changed[0]["trend_score"] = 0.333
    collection.update_one({"_id": newest["_id"]}, {"$set": {"items": changed}})

    with pytest.raises(TrendSnapshotIntegrityError):
        _read(collection, as_of=new_end)


def test_live_query_failure_is_not_reported_as_missing_snapshot(live_database):
    _client, database, _recorder = live_database
    actual = database[BUNDLE_COLLECTION_NAME]

    class _ServerRejectingCollection:
        name = BUNDLE_COLLECTION_NAME

        def find(self, _query):
            return actual.find({"$i2_invalid_operator": True})

    with pytest.raises(TrendSnapshotQueryError):
        _read(_ServerRejectingCollection(), as_of=BASE_START + timedelta(hours=1))


def test_live_query_is_namespace_filtered_sorted_descending_and_limited_to_one(live_database):
    _client, database, recorder = live_database
    collection = database[BUNDLE_COLLECTION_NAME]
    _insert_bundle(database, window_end=BASE_START + timedelta(hours=1))
    _insert_bundle(
        database,
        source_namespace="i2:other-source",
        window_end=BASE_START + timedelta(hours=2),
        product_ids=(201, 202),
    )
    recorder.find_commands.clear()

    result = _read(collection, as_of=BASE_START + timedelta(hours=3))

    assert result.available is True
    finds = [command for command in recorder.find_commands if command.get("find") == collection.name]
    assert len(finds) == 1
    command = finds[0]
    assert command["filter"]["source_namespace"] == NAMESPACE
    assert command["filter"]["window_end"]["$lte"].replace(tzinfo=timezone.utc) == (
        BASE_START + timedelta(hours=3)
    )
    assert command["sort"] == {"window_end": -1}
    assert command["limit"] == 1


def test_live_reader_result_flows_into_existing_hybrid_ranker(live_database):
    _client, database, _recorder = live_database
    start = BASE_START
    end = start + timedelta(hours=1)
    _insert_bundle(
        database,
        window_end=end,
        scores=(0.9, 0.4),
        product_ids=(2, 3),
    )
    trend = _read(database[BUNDLE_COLLECTION_NAME], as_of=end)
    als = (
        ALSRecommendation(7001, 1, 12.0, 1),
        ALSRecommendation(7001, 2, 6.0, 2),
    )
    catalog = tuple(ProductMetadata(product_id, f"Product {product_id}") for product_id in (1, 2, 3))
    context = VerifiedSourceContext(als_available=True, graph_available=True)

    ranked = rank_hybrid_recommendations(
        7001,
        als,
        trend.trend_snapshot,
        catalog,
        context,
        HybridRankingConfig(lambda_weight=0.5),
    )

    assert ranked.metadata.mode == "hybrid"
    assert [row.rank for row in ranked.recommendations] == [1, 2, 3]
    assert {row.product_id for row in ranked.recommendations} == {1, 2, 3}
    assert any(row.from_trend and row.trend_score == 0.9 for row in ranked.recommendations)
