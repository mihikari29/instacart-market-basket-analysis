"""Live MongoDB publication checks for atomic Module 3 trend bundles."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
import threading
import uuid

import pytest
from pymongo import MongoClient
from pymongo.uri_parser import parse_uri
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StructField,
    StructType,
    TimestampType,
)

from src.module3.config import Config, session
from src.module3.trend_bundle import (
    BUNDLE_COLLECTION_NAME,
    TTL_INDEX_NAME,
    TrendBundleConflictError,
    TrendBundlePublicationError,
    TrendBundleValidationError,
    write_trend_bundles,
)


pytestmark = pytest.mark.integration

TEST_URI_ENV = "I1_MONGODB_TEST_URI"
FORBIDDEN_DATABASES = {"admin", "config", "instacart", "local"}
WINDOW_SCHEMA = StructType(
    [
        StructField("window_start", TimestampType(), True),
        StructField("window_end", TimestampType(), True),
        StructField("product_id", LongType(), True),
        StructField("purchase_count_30m", LongType(), True),
        StructField("purchase_count_120m", LongType(), True),
        StructField("trend_score", DoubleType(), True),
        StructField("trend_rank", IntegerType(), True),
    ]
)


def _dedicated_uri() -> str:
    uri = os.environ.get(TEST_URI_ENV)
    if not uri:
        pytest.skip(f"{TEST_URI_ENV} is not set; live MongoDB tests require the isolated I1 service")
    parsed = parse_uri(uri)
    hosts = {host.lower() for host, _port in parsed.get("nodelist", ())}
    if hosts != {"i1-mongo"}:
        pytest.fail(f"{TEST_URI_ENV} must target only the isolated Docker alias 'i1-mongo'")
    if parsed.get("username") is not None or parsed.get("password") is not None:
        pytest.fail(f"{TEST_URI_ENV} must not contain application credentials")
    if parsed.get("database") not in (None, ""):
        pytest.fail(f"{TEST_URI_ENV} must not select a database; tests create owned databases")
    return uri


@pytest.fixture
def live_database():
    uri = _dedicated_uri()
    client = MongoClient(uri, serverSelectionTimeoutMS=5000, connectTimeoutMS=5000, tz_aware=True)
    database_name = f"i1_module3_bundle_{uuid.uuid4().hex}"
    if database_name.lower() in FORBIDDEN_DATABASES:
        client.close()
        pytest.fail("generated an unsafe integration database name")

    owned = False
    try:
        client.admin.command("ping")
        if database_name in client.list_database_names():
            pytest.fail("generated integration database already exists")
        database = client[database_name]
        database["_i1_test_ownership"].insert_one(
            {"_id": "created-by-test", "created_at": datetime.now(timezone.utc)}
        )
        owned = True
        yield client, database, uri
    finally:
        try:
            if owned:
                client.drop_database(database_name)
        finally:
            client.close()


@pytest.fixture(scope="module")
def spark_session(tmp_path_factory):
    directory = tmp_path_factory.mktemp("module3-trend-bundle-mongo")
    config = Config(
        root=(directory / "fixture").as_uri(),
        master="local[2]",
        shuffle_partitions=2,
        mongo_top_k_per_window=3,
        ttl_seconds=0,
    )
    spark = session(config, directory / "events")
    yield spark
    spark.stop()


def _writer_config(uri: str, database_name: str, *, ttl_seconds: int = 0) -> Config:
    return Config(
        mongo_top_k_per_window=3,
        ttl_seconds=ttl_seconds,
        root="file:///tmp/i1-module3-trend-bundle",
        mongodb_uri=uri,
        mongo_database=database_name,
    )


def _window_rows(start: datetime, products=(11, 12, 13), *, changed=False):
    end = start + timedelta(hours=2)
    rows = []
    for rank, product_id in enumerate(products, start=1):
        short_count = 20 - rank + (1 if changed and rank == 1 else 0)
        rows.append(
            (
                start,
                end,
                product_id,
                short_count,
                40 - rank,
                1.0 - rank / 10,
                rank,
            )
        )
    return rows


def _frame(spark, rows):
    return spark.createDataFrame(rows, schema=WINDOW_SCHEMA)


def _publish(spark, collection, config, rows, **kwargs):
    return write_trend_bundles(
        _frame(spark, rows),
        collection,
        config,
        source_namespace=kwargs.pop("source_namespace", "i1:module3:fixture-v1"),
        **kwargs,
    )


def _canonical_time(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _sha256(value) -> str:
    canonical = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _expected_document_parts(start, rows, config, namespace, scope="unknown_or_mixed"):
    ordered_rows = sorted(rows, key=lambda row: row[6])
    items = [
        {
            "product_id": row[2],
            "purchase_count_30m": row[3],
            "purchase_count_120m": row[4],
            "trend_score": row[5],
            "trend_rank": row[6],
        }
        for row in ordered_rows
    ]
    end = start + timedelta(hours=2)
    config_hash = _sha256(
        {
            "formula_version": "module3-trend-v1",
            "window_short": config.window_short,
            "window_long": config.window_long,
            "slide": config.slide,
            "watermark": config.watermark,
            "weight_short": float(config.weight_short),
            "weight_long": float(config.weight_long),
            "top_k": config.mongo_top_k_per_window,
        }
    )
    logical = {
        "schema_version": 1,
        "source_namespace": namespace,
        "source_scope": scope,
        "window_start": _canonical_time(start),
        "window_end": _canonical_time(end),
        "top_k": config.mongo_top_k_per_window,
        "row_count": len(items),
        "items": items,
        "config_hash": config_hash,
    }
    namespace_digest = hashlib.sha256(namespace.encode("utf-8")).hexdigest()
    return {
        "_id": f"trend-window:{namespace_digest}:{_canonical_time(end)}",
        "window_start": start.replace(tzinfo=timezone.utc),
        "window_end": end.replace(tzinfo=timezone.utc),
        "items": items,
        "row_count": len(items),
        "config_hash": config_hash,
        "content_sha256": _sha256(logical),
    }


def test_live_first_publication_has_correct_identity_schema_items_and_hash(
    live_database, spark_session
):
    _client, database, uri = live_database
    config = _writer_config(uri, database.name)
    collection = database[BUNDLE_COLLECTION_NAME]
    start = datetime(2019, 2, 3, 4, 5)
    rows = _window_rows(start)

    report = _publish(spark_session, collection, config, rows)

    expected = _expected_document_parts(start, rows, config, "i1:module3:fixture-v1")
    document = collection.find_one({"_id": expected["_id"]})
    assert report.bundles_created == 1
    assert report.windows_processed == 1
    assert report.trend_rows_represented == 3
    assert collection.count_documents({}) == 1
    assert document["schema_version"] == 1
    assert document["source_namespace"] == "i1:module3:fixture-v1"
    assert document["source_scope"] == "unknown_or_mixed"
    for field in ("_id", "window_start", "window_end", "items", "row_count", "config_hash", "content_sha256"):
        assert document[field] == expected[field]
    assert [item["trend_rank"] for item in document["items"]] == [1, 2, 3]
    assert "expires_at" not in document


def test_live_same_content_retry_preserves_first_document_and_publication_time(
    live_database, spark_session
):
    _client, database, uri = live_database
    config = _writer_config(uri, database.name, ttl_seconds=3600)
    collection = database[BUNDLE_COLLECTION_NAME]
    start = datetime(2019, 2, 3, 4, 5)
    rows = _window_rows(start)
    first_time = datetime(2026, 10, 1, 12, tzinfo=timezone.utc)

    first = _publish(
        spark_session,
        collection,
        config,
        rows,
        published_at=first_time,
    )
    original = collection.find_one({"source_namespace": "i1:module3:fixture-v1"})
    retry = _publish(
        spark_session,
        collection,
        config,
        rows,
        published_at=first_time + timedelta(hours=3),
    )
    after = collection.find_one({"_id": original["_id"]})

    assert first.bundles_created == 1
    assert retry.bundles_created == 0
    assert retry.identical_content_retries == 1
    assert collection.count_documents({}) == 1
    assert after == original
    assert after["published_at"] == first_time
    assert after["expires_at"] == first_time + timedelta(seconds=3600)


def test_live_conflicting_retry_preserves_original_complete_document(live_database, spark_session):
    _client, database, uri = live_database
    config = _writer_config(uri, database.name)
    collection = database[BUNDLE_COLLECTION_NAME]
    start = datetime(2019, 2, 3, 4, 5)
    rows = _window_rows(start)
    _publish(spark_session, collection, config, rows)
    original = collection.find_one({"source_namespace": "i1:module3:fixture-v1"})

    with pytest.raises(TrendBundleConflictError, match="conflicting content"):
        _publish(spark_session, collection, config, _window_rows(start, changed=True))

    assert collection.count_documents({}) == 1
    assert collection.find_one({"_id": original["_id"]}) == original


def test_live_multiple_windows_get_separate_stable_documents(live_database, spark_session):
    _client, database, uri = live_database
    config = _writer_config(uri, database.name)
    collection = database[BUNDLE_COLLECTION_NAME]
    starts = [datetime(2019, 2, 3, 4, 5) + timedelta(minutes=5 * index) for index in range(4)]
    rows = [row for index, start in enumerate(starts) for row in _window_rows(start, (31 + index, 41 + index))]

    report = _publish(spark_session, collection, config, rows, batch_size=2)

    documents = list(collection.find({"source_namespace": "i1:module3:fixture-v1"}))
    assert report.bundles_created == 4
    assert report.windows_processed == 4
    assert collection.count_documents({}) == 4
    assert len({document["_id"] for document in documents}) == 4
    assert {document["window_start"] for document in documents} == {
        start.replace(tzinfo=timezone.utc) for start in starts
    }
    for document in documents:
        assert document["row_count"] == 2
        assert len(document["items"]) == 2
        assert {item["product_id"] for item in document["items"]} in (
            {31, 41}, {32, 42}, {33, 43}, {34, 44}
        )
        assert [item["trend_rank"] for item in document["items"]] == [1, 2]


def test_live_namespace_isolation_allows_same_window_content_to_coexist(live_database, spark_session):
    _client, database, uri = live_database
    config = _writer_config(uri, database.name)
    collection = database[BUNDLE_COLLECTION_NAME]
    start = datetime(2019, 2, 3, 4, 5)

    _publish(spark_session, collection, config, _window_rows(start), source_namespace="i1:stream:a-v1")
    _publish(
        spark_session,
        collection,
        config,
        _window_rows(start, changed=True),
        source_namespace="i1:stream:b-v1",
    )

    documents = list(collection.find({}).sort("source_namespace", 1))
    assert collection.count_documents({}) == 2
    assert documents[0]["source_namespace"] == "i1:stream:a-v1"
    assert documents[1]["source_namespace"] == "i1:stream:b-v1"
    assert documents[0]["_id"] != documents[1]["_id"]
    assert documents[0]["content_sha256"] != documents[1]["content_sha256"]


def test_live_ttl_index_and_historical_event_time_policy(live_database, spark_session):
    _client, database, uri = live_database
    collection = database[BUNDLE_COLLECTION_NAME]
    start = datetime(2010, 1, 1)
    now = datetime.now(timezone.utc)
    published_at = now.replace(microsecond=(now.microsecond // 1000) * 1000)
    ttl_config = _writer_config(uri, database.name, ttl_seconds=7200)
    _publish(
        spark_session,
        collection,
        ttl_config,
        _window_rows(start),
        published_at=published_at,
    )

    indexes = collection.index_information()
    ttl_index = indexes[TTL_INDEX_NAME]
    historical = collection.find_one({"source_namespace": "i1:module3:fixture-v1"})
    assert list(ttl_index["key"]) == [("expires_at", 1)]
    assert ttl_index["expireAfterSeconds"] == 0
    assert historical["window_start"] == start.replace(tzinfo=timezone.utc)
    assert historical["expires_at"] == published_at + timedelta(seconds=7200)
    assert historical["expires_at"] > datetime.now(timezone.utc)

    disabled_config = _writer_config(uri, database.name, ttl_seconds=0)
    _publish(
        spark_session,
        collection,
        disabled_config,
        _window_rows(start + timedelta(minutes=5)),
        source_namespace="i1:module3:no-ttl-v1",
    )
    disabled = collection.find_one({"source_namespace": "i1:module3:no-ttl-v1"})
    assert TTL_INDEX_NAME not in collection.index_information()
    assert "expires_at" not in disabled
    assert collection.find_one({"_id": historical["_id"]}) == historical


def test_live_validation_failure_preserves_previously_published_document(live_database, spark_session):
    _client, database, uri = live_database
    config = _writer_config(uri, database.name)
    collection = database[BUNDLE_COLLECTION_NAME]
    start = datetime(2019, 2, 3, 4, 5)
    _publish(spark_session, collection, config, _window_rows(start))
    original = collection.find_one({"source_namespace": "i1:module3:fixture-v1"})
    malformed = _window_rows(start)
    malformed[0] = malformed[0][:3] + (-1,) + malformed[0][4:]

    with pytest.raises(TrendBundleValidationError, match="purchase_count_30m"):
        _publish(spark_session, collection, config, malformed)

    assert collection.count_documents({}) == 1
    assert collection.find_one({"_id": original["_id"]}) == original


class _PreflightBarrierCollection:
    """Synchronize the first real Mongo preflight read from each writer."""

    def __init__(self, collection, barrier):
        self._collection = collection
        self._barrier = barrier
        self._waited = False
        self.name = collection.name

    def find(self, *args, **kwargs):
        rows = list(self._collection.find(*args, **kwargs))
        if not self._waited:
            self._waited = True
            self._barrier.wait(timeout=30)
        return iter(rows)

    def __getattr__(self, name):
        return getattr(self._collection, name)


def _enable_duplicate_key_failpoint(client, identity):
    client.admin.command(
        {
            "configureFailPoint": "failCommand",
            "mode": {"times": 1},
            "data": {
                "failCommands": ["update"],
                "errorCode": 11000,
                "errorExtraInfo": {
                    "keyPattern": {"_id": 1},
                    "keyValue": {"_id": identity},
                },
            },
        }
    )


def _disable_duplicate_key_failpoint(client):
    client.admin.command(
        {"configureFailPoint": "failCommand", "mode": "off"}
    )


class _IdenticalRaceCollection(_PreflightBarrierCollection):
    """Make one writer win, then inject a server E11000 into the other writer."""

    def __init__(self, collection, preflight_barrier, bulk_barrier, winner_done, role, observed):
        super().__init__(collection, preflight_barrier)
        self._bulk_barrier = bulk_barrier
        self._winner_done = winner_done
        self._role = role
        self._observed = observed
        self.winner_document = None

    def bulk_write(self, operations, ordered):
        operations = list(operations)
        self._bulk_barrier.wait(timeout=30)
        if self._role == "winner":
            result = self._collection.bulk_write(operations, ordered=ordered)
            self.winner_document = self._collection.find_one(
                {"_id": operations[0]._filter["_id"]}
            )
            self._winner_done.set()
            return result

        if not self._winner_done.wait(timeout=30):
            raise RuntimeError("winner did not publish before the duplicate-key injection")
        identity = operations[0]._filter["_id"]
        _enable_duplicate_key_failpoint(self._collection.database.client, identity)
        try:
            try:
                return self._collection.bulk_write(operations, ordered=ordered)
            except Exception as exc:
                if getattr(exc, "code", None) == 11000:
                    self._observed.append(exc)
                raise
        finally:
            _disable_duplicate_key_failpoint(self._collection.database.client)


class _FailSecondUpdateWithDuplicateKey:
    """Keep one window, then return MongoDB's E11000 for the next update."""

    def __init__(self, collection):
        self._collection = collection
        self._bulk_calls = 0
        self.duplicate_error = None
        self.name = collection.name

    def bulk_write(self, operations, ordered):
        from pymongo.errors import BulkWriteError, DuplicateKeyError

        operations = list(operations)
        self._bulk_calls += 1
        if self._bulk_calls == 1:
            return self._collection.bulk_write(operations, ordered=ordered)

        identity = operations[0]._filter["_id"]
        client = self._collection.database.client
        _enable_duplicate_key_failpoint(client, identity)
        try:
            try:
                return self._collection.bulk_write(operations, ordered=ordered)
            except (BulkWriteError, DuplicateKeyError) as exc:
                if getattr(exc, "code", None) == 11000 or any(
                    error.get("code") == 11000
                    for error in (getattr(exc, "details", None) or {}).get("writeErrors", [])
                ):
                    self.duplicate_error = exc
                raise
        finally:
            _disable_duplicate_key_failpoint(client)

    def __getattr__(self, name):
        return getattr(self._collection, name)


def test_live_concurrent_identical_publishers_recover_server_duplicate_key(
    live_database, spark_session
):
    _client, database, uri = live_database
    config = _writer_config(uri, database.name)
    namespace = "i1:module3:concurrent-identical-v1"
    start = datetime(2019, 2, 3, 4, 5)
    preflight_barrier = threading.Barrier(2)
    bulk_barrier = threading.Barrier(2)
    winner_done = threading.Event()
    observed_duplicate_errors = []
    results = []

    def publish(role):
        with MongoClient(uri, serverSelectionTimeoutMS=5000, connectTimeoutMS=5000, tz_aware=True) as client:
            raw_collection = client[database.name][BUNDLE_COLLECTION_NAME]
            synchronized = _IdenticalRaceCollection(
                raw_collection,
                preflight_barrier,
                bulk_barrier,
                winner_done,
                role,
                observed_duplicate_errors,
            )
            try:
                report = _publish(
                    spark_session,
                    synchronized,
                    config,
                    _window_rows(start),
                    source_namespace=namespace,
                )
                results.append((role, "success", report, synchronized.winner_document))
            except Exception as exc:
                results.append((role, "error", exc, synchronized.winner_document))

    threads = [
        threading.Thread(target=publish, args=("winner",)),
        threading.Thread(target=publish, args=("retry",)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=90)

    assert all(not thread.is_alive() for thread in threads), "concurrent publishers did not finish"
    assert len(results) == 2
    assert all(state == "success" for _role, state, _value, _doc in results), results
    assert len(observed_duplicate_errors) == 1
    assert observed_duplicate_errors[0].code == 11000
    winner_result = next(value for role, _state, value, _doc in results if role == "winner")
    retry_result = next(value for role, _state, value, _doc in results if role == "retry")
    winner_document = next(doc for role, _state, _value, doc in results if role == "winner")
    after = database[BUNDLE_COLLECTION_NAME].find_one({"source_namespace": namespace})

    assert winner_result.bundles_created == 1
    assert winner_result.identical_content_retries == 0
    assert retry_result.bundles_created == 0
    assert retry_result.identical_content_retries == 1
    assert database[BUNDLE_COLLECTION_NAME].count_documents({"source_namespace": namespace}) == 1
    assert winner_document == after
    assert after["_id"] == winner_document["_id"]
    assert after["content_sha256"] == winner_document["content_sha256"]


def test_live_duplicate_recovery_rejects_missing_window_then_identical_retry_converges(
    live_database, spark_session
):
    _client, database, uri = live_database
    config = _writer_config(uri, database.name)
    collection = database[BUNDLE_COLLECTION_NAME]
    namespace = "i1:module3:partial-duplicate-v1"
    starts = [datetime(2019, 2, 3, 4, 5), datetime(2019, 2, 3, 4, 10)]
    rows = [row for start in starts for row in _window_rows(start, (51, 52))]
    first_time = datetime(2026, 10, 14, 5, 30, tzinfo=timezone.utc)

    with pytest.raises(TrendBundlePublicationError, match="missing expected") as raised:
        write_trend_bundles(
            _frame(spark_session, rows),
            _FailSecondUpdateWithDuplicateKey(collection),
            config,
            source_namespace=namespace,
            published_at=first_time,
            batch_size=1,
        )

    assert raised.value.report.windows_processed == 2
    assert raised.value.report.bundles_created == 1
    partial = list(collection.find({"source_namespace": namespace}))
    assert len(partial) == 1
    original = partial[0]
    assert original["row_count"] == 2
    assert original["published_at"] == first_time

    retry = _publish(
        spark_session,
        collection,
        config,
        rows,
        source_namespace=namespace,
        published_at=first_time + timedelta(hours=1),
        batch_size=1,
    )
    completed = list(collection.find({"source_namespace": namespace}))

    assert len(completed) == 2
    assert retry.bundles_created == 1
    assert retry.identical_content_retries == 1
    assert collection.find_one({"_id": original["_id"]}) == original
    assert {document["window_start"] for document in completed} == {
        start.replace(tzinfo=timezone.utc) for start in starts
    }


def test_live_concurrent_different_content_cannot_overwrite_bundle(live_database, spark_session):
    _client, database, uri = live_database
    config = _writer_config(uri, database.name)
    namespace = "i1:module3:concurrent-v1"
    start = datetime(2019, 2, 3, 4, 5)
    barrier = threading.Barrier(2)
    results = []

    def publish_variant(changed):
        with MongoClient(uri, serverSelectionTimeoutMS=5000, connectTimeoutMS=5000, tz_aware=True) as client:
            collection = client[database.name][BUNDLE_COLLECTION_NAME]
            synchronized = _PreflightBarrierCollection(collection, barrier)
            try:
                report = _publish(
                    spark_session,
                    synchronized,
                    config,
                    _window_rows(start, changed=changed),
                    source_namespace=namespace,
                )
                results.append(("success", report))
            except Exception as exc:  # the losing concurrent write must be observable
                results.append(("error", exc))

    threads = [
        threading.Thread(target=publish_variant, args=(False,)),
        threading.Thread(target=publish_variant, args=(True,)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=90)

    assert all(not thread.is_alive() for thread in threads), "concurrent publishers did not finish"
    assert len(results) == 2
    successes = [value for state, value in results if state == "success"]
    failures = [value for state, value in results if state == "error"]
    assert len(successes) == 1
    assert len(failures) == 1
    assert isinstance(failures[0], TrendBundleConflictError)

    documents = list(database[BUNDLE_COLLECTION_NAME].find({"source_namespace": namespace}))
    assert len(documents) == 1
    assert documents[0]["row_count"] == 3
    product_sets = {frozenset(item["product_id"] for item in document["items"]) for document in documents}
    assert product_sets in ({frozenset({11, 12, 13})},)
    assert documents[0]["content_sha256"]


class _FailSecondBulkCollection:
    def __init__(self, collection):
        self._collection = collection
        self._bulk_calls = 0
        self.name = collection.name

    def bulk_write(self, *args, **kwargs):
        self._bulk_calls += 1
        if self._bulk_calls == 2:
            raise RuntimeError("injected failure after one complete window")
        return self._collection.bulk_write(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._collection, name)


def test_live_multiwindow_partial_failure_preserves_completed_documents_and_retry_converges(
    live_database, spark_session
):
    _client, database, uri = live_database
    config = _writer_config(uri, database.name)
    collection = database[BUNDLE_COLLECTION_NAME]
    namespace = "i1:module3:partial-v1"
    starts = [datetime(2019, 2, 3, 4, 5), datetime(2019, 2, 3, 4, 10)]
    rows = [row for start in starts for row in _window_rows(start, (51, 52))]

    with pytest.raises(TrendBundlePublicationError, match="atomically publish"):
        write_trend_bundles(
            _frame(spark_session, rows),
            _FailSecondBulkCollection(collection),
            config,
            source_namespace=namespace,
            batch_size=1,
        )

    partial = list(collection.find({"source_namespace": namespace}))
    assert len(partial) == 1
    first_complete = partial[0]
    assert first_complete["row_count"] == 2
    assert len(first_complete["items"]) == 2

    contradictory = []
    for start in starts:
        window_rows = _window_rows(start, (51, 52), changed=start.replace(tzinfo=timezone.utc) == first_complete["window_start"])
        contradictory.extend(window_rows)
    with pytest.raises(TrendBundleConflictError, match="conflicting content"):
        _publish(
            spark_session,
            collection,
            config,
            contradictory,
            source_namespace=namespace,
            batch_size=1,
        )
    assert list(collection.find({"source_namespace": namespace})) == partial

    retry = _publish(
        spark_session,
        collection,
        config,
        rows,
        source_namespace=namespace,
        batch_size=1,
    )
    completed = list(collection.find({"source_namespace": namespace}))
    assert len(completed) == 2
    assert retry.bundles_created == 1
    assert retry.identical_content_retries == 1
    assert all(document["row_count"] == 2 for document in completed)
    assert {document["window_start"] for document in completed} == {
        start.replace(tzinfo=timezone.utc) for start in starts
    }
