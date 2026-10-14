"""Live MongoDB contract checks for immutable Module 4C publication."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import hashlib
import math
import os
from threading import Event, Thread, current_thread
import uuid

import pytest
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo.synchronous.collection import Collection
from pymongo.uri_parser import parse_uri

from src.module3.trend_bundle import BUNDLE_COLLECTION_NAME, _build_document
from src.module4 import sink_mongo as module4_sink
from src.module4.config import Config as Module4Config
from src.module4c import (
    ALSRecommendation,
    CapturedUserSources,
    HybridPublicationConflictError,
    HybridPublicationPromotionError,
    HybridPublicationWriteError,
    HybridRankingConfig,
    HybridSnapshotContractError,
    ProductMetadata,
    VerifiedSourceContext,
    prepare_hybrid_snapshot,
    publish_hybrid_recommendations,
    rank_hybrid_recommendations,
    read_verified_trend_snapshot,
)
from src.module4c.data import READINESS_COLLECTION
from src.module4c.sink_mongo import FINAL_RECOMMENDATIONS_COLLECTION


pytestmark = pytest.mark.integration

TEST_URI_ENV = "I2_MONGODB_TEST_URI"
ALLOWED_HOST = ("i2-mongo", 27017)
FORBIDDEN_DATABASES = {"admin", "config", "instacart", "local"}
NAMESPACE = "i2:module3:publisher-v1"
OTHER_NAMESPACE = "i2:module3:publisher-v2"
RUN_ID = "i2-fixture-run-001"
MODEL_VERSION = "i2-als-v1"
GRAPH_VERSION = "i2-graph-v1"
USER_ID = 7701
BASE_START = datetime(2025, 1, 1, tzinfo=timezone.utc)
WINDOW_END = BASE_START + timedelta(hours=1)
AS_OF = WINDOW_END + timedelta(hours=1)
PROCESSING_TIME = datetime(2026, 1, 1, 0, 0, 0, 123000, tzinfo=timezone.utc)
FINAL_ROW_FIELDS = {
    "_id",
    "schema_version",
    "serving_version",
    "algorithm_version",
    "user_id",
    "product_id",
    "product_name",
    "als_score",
    "trend_score",
    "normalized_als_score",
    "normalized_trend_score",
    "final_score",
    "rank",
    "from_als",
    "from_trend",
    "lambda_requested",
    "lambda_effective",
    "mode",
    "reason",
    "source_run_id",
    "model_version",
    "graph_version",
    "trend_source_namespace",
    "trend_source_scope",
    "trend_window_start",
    "trend_window_end",
    "trend_content_sha256",
    "as_of_event_time",
    "input_content_sha256",
    "generated_at",
    "als_top_n",
    "trend_top_m",
    "output_top_k",
    "epsilon",
    "ranking_config_sha256",
}


def _dedicated_uri() -> str:
    uri = os.environ.get(TEST_URI_ENV)
    if not uri:
        pytest.skip(f"{TEST_URI_ENV} is not set; live C6 tests require the isolated I2 MongoDB service")
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
    client = MongoClient(
        uri,
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=5000,
        tz_aware=True,
    )
    database_name = f"i2_module4c_c6_{uuid.uuid4().hex}"
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
        database["_i2_test_ownership"].insert_one({"_id": "created-by-i2-c6-test"})
        yield client, database, uri
    finally:
        try:
            if owned:
                client.drop_database(database_name)
        finally:
            client.close()


def _config(uri, database_name):
    return Module4Config(mongodb_uri=uri, mongo_database=database_name)


def _seed_active(
    database,
    *,
    status="ready",
    ready=True,
    run_id=RUN_ID,
    model_version=MODEL_VERSION,
    graph_version=GRAPH_VERSION,
):
    document = {
        "_id": "active",
        "status": status,
        "ready": ready,
        "run_id": run_id,
        "model_version": model_version,
        "graph_version": graph_version,
        "recommendation_published": True,
        "graph_published": True,
        "single_writer_policy": "module4_publication_lock",
        "updated_at": PROCESSING_TIME,
        "i2_preservation_marker": "leave-active-document-unchanged",
    }
    database[READINESS_COLLECTION].replace_one({"_id": "active"}, document, upsert=True)
    return database[READINESS_COLLECTION].find_one({"_id": "active"})


def _bundle_document(
    *,
    namespace=NAMESPACE,
    window_start=BASE_START,
    window_end=WINDOW_END,
    product_ids=(1102, 1103),
    scores=(0.9, 0.6),
    published_at=PROCESSING_TIME,
):
    items = [
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
    return _build_document(
        {
            "window_start": window_start,
            "window_end": window_end,
            "items": items,
            "_row_count": len(items),
        },
        source_namespace=namespace,
        source_scope="i2_persisted_fixture",
        top_k=20,
        config_hash=hashlib.sha256(b"i2-module3-config").hexdigest(),
        published_at=published_at,
        retention_seconds=0,
    )


def _ensure_bundle(
    database,
    *,
    namespace=NAMESPACE,
    window_start=BASE_START,
    window_end=WINDOW_END,
    product_ids=(1102, 1103),
    scores=(0.9, 0.6),
):
    collection = database[BUNDLE_COLLECTION_NAME]
    document = _bundle_document(
        namespace=namespace,
        window_start=window_start,
        window_end=window_end,
        product_ids=product_ids,
        scores=scores,
    )
    if collection.find_one({"_id": document["_id"]}) is None:
        collection.insert_one(document)
    else:
        existing = collection.find_one({"_id": document["_id"]})
        assert existing["content_sha256"] == document["content_sha256"]
    return document


def _captured(
    *,
    user_id=USER_ID,
    run_id=RUN_ID,
    model_version=MODEL_VERSION,
    graph_version=GRAPH_VERSION,
    als_rows=((1101, 12.0), (1102, 6.0)),
):
    als = tuple(
        ALSRecommendation(user_id, product_id, score, rank)
        for rank, (product_id, score) in enumerate(als_rows, start=1)
    )
    catalog = tuple(
        ProductMetadata(product_id, f"Fixture product {product_id}")
        for product_id in (1101, 1102, 1103, 1201, 1202)
    )
    return CapturedUserSources(
        user_id=user_id,
        source_context=VerifiedSourceContext(als_available=True, graph_available=True),
        als_recommendations=als,
        product_catalog=catalog,
        missing_prospective_trend_product_ids=(),
        run_id=run_id,
        model_version=model_version,
        graph_version=graph_version,
    )


def _prepare(
    database,
    *,
    user_id=USER_ID,
    as_of=AS_OF,
    namespace=NAMESPACE,
    lambda_weight=0.5,
    run_id=RUN_ID,
    model_version=MODEL_VERSION,
    graph_version=GRAPH_VERSION,
    als_rows=((1101, 12.0), (1102, 6.0)),
    trend_product_ids=(1102, 1103),
    trend_scores=(0.9, 0.6),
):
    if namespace != NAMESPACE:
        _ensure_bundle(
            database,
            namespace=namespace,
            window_start=BASE_START + timedelta(minutes=5),
            window_end=WINDOW_END + timedelta(minutes=5),
            product_ids=trend_product_ids,
            scores=trend_scores,
        )
    else:
        _ensure_bundle(
            database,
            namespace=namespace,
            product_ids=trend_product_ids,
            scores=trend_scores,
        )
    trend = read_verified_trend_snapshot(
        database[BUNDLE_COLLECTION_NAME],
        source_namespace=namespace,
        as_of_event_time=as_of,
        processing_time_utc=PROCESSING_TIME,
    )
    captured = _captured(
        user_id=user_id,
        run_id=run_id,
        model_version=model_version,
        graph_version=graph_version,
        als_rows=als_rows,
    )
    config = HybridRankingConfig(lambda_weight=lambda_weight)
    ranked = rank_hybrid_recommendations(
        user_id,
        captured.als_recommendations,
        trend.trend_snapshot,
        captured.product_catalog,
        captured.source_context,
        config,
    )
    prepared = prepare_hybrid_snapshot(
        captured,
        trend,
        ranked,
        config,
        as_of_event_time=as_of,
    )
    return prepared, trend, ranked, config


def _publish(client, database, uri, prepared, *, processing_time=PROCESSING_TIME):
    return publish_hybrid_recommendations(
        _config(uri, database.name),
        prepared,
        client=client,
        processing_time_utc=processing_time,
    )


def _pointer(database, user_id=USER_ID):
    return database[READINESS_COLLECTION].find_one({"_id": f"hybrid:{user_id}"})


def _consumer_read(database, user_id):
    readiness = database[READINESS_COLLECTION]
    first = readiness.find_one({"_id": f"hybrid:{user_id}"})
    assert first is not None and first["ready"] is True
    rows = list(
        database[FINAL_RECOMMENDATIONS_COLLECTION]
        .find({"serving_version": first["serving_version"], "user_id": user_id})
        .sort("rank", 1)
    )
    assert len(rows) == first["expected_rows"]
    assert [row["rank"] for row in rows] == list(range(1, len(rows) + 1))
    assert len({row["product_id"] for row in rows}) == len(rows)
    second = readiness.find_one({"_id": f"hybrid:{user_id}"})
    accepted = (
        second is not None
        and first["serving_version"] == second["serving_version"]
        and first["expected_rows"] == second["expected_rows"]
        and first["input_content_sha256"] == second["input_content_sha256"]
    )
    return first, rows, second, accepted


def test_live_success_publishes_exact_schema_indexes_and_preserves_active_readiness(live_database):
    client, database, uri = live_database
    active_before = _seed_active(database)
    prepared, trend, ranked, config = _prepare(database)

    result = _publish(client, database, uri, prepared)

    collection = database[FINAL_RECOMMENDATIONS_COLLECTION]
    rows = list(
        collection.find({"serving_version": prepared.serving_version, "user_id": USER_ID}).sort(
            "rank", 1
        )
    )
    indexes = collection.index_information()
    pointer = _pointer(database)
    assert result.status == "published" and result.idempotent is False
    assert result.serving_version == prepared.serving_version
    assert result.expected_rows == len(ranked.recommendations) == len(rows) == 3
    assert result.pointer_id == f"hybrid:{USER_ID}"
    assert pointer["serving_version"] == prepared.serving_version
    assert pointer["expected_rows"] == len(rows)
    assert pointer["status"] == "ready" and pointer["ready"] is True
    assert pointer["source_run_id"] == RUN_ID
    assert pointer["model_version"] == MODEL_VERSION
    assert pointer["graph_version"] == GRAPH_VERSION
    assert pointer["trend_source_namespace"] == NAMESPACE
    assert pointer["trend_content_sha256"] == trend.content_sha256
    assert pointer["as_of_event_time"] == prepared.as_of_event_time
    assert [row["rank"] for row in rows] == [1, 2, 3]
    assert len({row["product_id"] for row in rows}) == 3
    assert all(set(row) == FINAL_ROW_FIELDS for row in rows)
    assert [row["product_id"] for row in rows] == [row.product_id for row in ranked.recommendations]
    assert all(row["schema_version"] == 1 for row in rows)
    assert all(row["algorithm_version"] == config.algorithm_version for row in rows)
    assert all(row["lambda_requested"] == config.lambda_weight for row in rows)
    assert all(row["source_run_id"] == RUN_ID for row in rows)
    assert all(row["model_version"] == MODEL_VERSION for row in rows)
    assert all(row["graph_version"] == GRAPH_VERSION for row in rows)
    assert all(row["trend_content_sha256"] == trend.content_sha256 for row in rows)
    trend_only = next(row for row in rows if row["product_id"] == 1103)
    assert trend_only["from_als"] is False and trend_only["from_trend"] is True
    assert trend_only["als_score"] is None
    assert trend_only["trend_score"] == 0.6
    assert rows[0]["generated_at"] == PROCESSING_TIME
    assert rows[0]["as_of_event_time"] == prepared.as_of_event_time
    assert rows[0]["as_of_event_time"].tzinfo is not None
    assert list(indexes["uq_serving_user_product"]["key"]) == [
        ("serving_version", 1), ("user_id", 1), ("product_id", 1)
    ]
    assert indexes["uq_serving_user_product"]["unique"] is True
    assert list(indexes["uq_serving_user_rank"]["key"]) == [
        ("serving_version", 1), ("user_id", 1), ("rank", 1)
    ]
    assert indexes["uq_serving_user_rank"]["unique"] is True
    duplicate_product = {**rows[0], "_id": "i2-duplicate-product-row"}
    with pytest.raises(DuplicateKeyError):
        collection.insert_one(duplicate_product)
    duplicate_rank = {
        **rows[0],
        "_id": "i2-duplicate-rank-row",
        "product_id": 1999,
    }
    with pytest.raises(DuplicateKeyError):
        collection.insert_one(duplicate_rank)
    assert database[READINESS_COLLECTION].find_one({"_id": "active"}) == active_before
    first, read_rows, second, accepted = _consumer_read(database, USER_ID)
    assert accepted is True and first == second and len(read_rows) == 3


def test_live_identical_retry_is_idempotent_and_keeps_immutable_rows(live_database):
    client, database, uri = live_database
    _seed_active(database)
    prepared, _trend, _ranked, _config_value = _prepare(database)
    first_result = _publish(client, database, uri, prepared)
    collection = database[FINAL_RECOMMENDATIONS_COLLECTION]
    before_rows = list(collection.find({"serving_version": prepared.serving_version}).sort("rank", 1))
    before_pointer = _pointer(database)

    retry = _publish(
        client,
        database,
        uri,
        prepared,
        processing_time=PROCESSING_TIME + timedelta(hours=1),
    )

    after_rows = list(collection.find({"serving_version": prepared.serving_version}).sort("rank", 1))
    assert retry.status == "already_active" and retry.idempotent is True
    assert retry.serving_version == first_result.serving_version
    assert retry.generated_at == first_result.generated_at
    assert after_rows == before_rows
    assert _pointer(database) == before_pointer
    assert collection.count_documents({"serving_version": prepared.serving_version}) == 3


def test_live_different_lambda_changes_serving_version_and_published_config(live_database):
    client, database, uri = live_database
    _seed_active(database)
    first, _trend, _ranked, _config_value = _prepare(database)
    _publish(client, database, uri, first)
    changed, _changed_trend, changed_ranked, changed_config = _prepare(
        database,
        as_of=AS_OF + timedelta(minutes=5),
        lambda_weight=0.25,
    )

    result = _publish(client, database, uri, changed)

    assert changed.serving_version != first.serving_version
    assert changed.ranking_config_sha256 != first.ranking_config_sha256
    assert result.status == "published"
    assert result.serving_version == changed.serving_version
    assert result.mode == changed_ranked.metadata.mode == "hybrid"
    assert changed_config.lambda_weight == 0.25
    rows = list(
        database[FINAL_RECOMMENDATIONS_COLLECTION].find(
            {"serving_version": changed.serving_version, "user_id": USER_ID}
        )
    )
    assert rows and all(row["lambda_requested"] == 0.25 for row in rows)
    assert _pointer(database)["serving_version"] == changed.serving_version


def _publish_baseline(client, database, uri):
    _seed_active(database)
    old, _trend, _ranked, _config_value = _prepare(database)
    _publish(client, database, uri, old)
    return old, _pointer(database)


def _later_snapshot(database, *, as_of=AS_OF + timedelta(minutes=5), lambda_weight=0.25):
    return _prepare(database, as_of=as_of, lambda_weight=lambda_weight)[0]


def test_live_failure_before_row_insert_preserves_ready_pointer_and_retry_recovers(
    live_database, monkeypatch
):
    client, database, uri = live_database
    _old, pointer_before = _publish_baseline(client, database, uri)
    candidate = _later_snapshot(database)
    original = Collection.insert_many

    def fail_before_insert(collection, documents, *args, **kwargs):
        if collection.name == FINAL_RECOMMENDATIONS_COLLECTION:
            raise OSError("I2 injected failure before row insertion")
        return original(collection, documents, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(Collection, "insert_many", fail_before_insert)
        with pytest.raises(HybridPublicationWriteError):
            _publish(client, database, uri, candidate)
    assert _pointer(database) == pointer_before
    assert database[FINAL_RECOMMENDATIONS_COLLECTION].count_documents(
        {"serving_version": candidate.serving_version}
    ) == 0

    recovered = _publish(client, database, uri, candidate)
    assert recovered.status == "published"
    assert _pointer(database)["serving_version"] == candidate.serving_version


def test_live_partial_row_insert_keeps_old_pointer_and_retry_completes_snapshot(
    live_database, monkeypatch
):
    client, database, uri = live_database
    _old, pointer_before = _publish_baseline(client, database, uri)
    candidate = _later_snapshot(database)
    original = Collection.insert_many

    def insert_one_then_fail(collection, documents, *args, **kwargs):
        if collection.name == FINAL_RECOMMENDATIONS_COLLECTION:
            original(collection, list(documents)[:1], ordered=False)
            raise OSError("I2 injected failure after one row")
        return original(collection, documents, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(Collection, "insert_many", insert_one_then_fail)
        with pytest.raises(HybridPublicationWriteError, match="partial"):
            _publish(client, database, uri, candidate)
    assert _pointer(database) == pointer_before
    rows = database[FINAL_RECOMMENDATIONS_COLLECTION]
    assert rows.count_documents({"serving_version": candidate.serving_version}) == 1

    recovered = _publish(client, database, uri, candidate)
    assert recovered.status == "published"
    assert rows.count_documents({"serving_version": candidate.serving_version}) == candidate.expected_rows
    assert _pointer(database)["serving_version"] == candidate.serving_version


def test_live_failure_after_verified_rows_before_promotion_preserves_pointer_and_recovers(
    live_database, monkeypatch
):
    client, database, uri = live_database
    _old, pointer_before = _publish_baseline(client, database, uri)
    candidate = _later_snapshot(database)

    class _FailingLockContext:
        def __enter__(self):
            raise RuntimeError("I2 injected failure before pointer promotion")

        def __exit__(self, *_args):
            return False

    with monkeypatch.context() as patch:
        patch.setattr(module4_sink, "publication_lock", lambda *_args, **_kwargs: _FailingLockContext())
        with pytest.raises(HybridPublicationPromotionError):
            _publish(client, database, uri, candidate)
    rows = database[FINAL_RECOMMENDATIONS_COLLECTION]
    assert rows.count_documents({"serving_version": candidate.serving_version}) == candidate.expected_rows
    assert _pointer(database) == pointer_before

    recovered = _publish(client, database, uri, candidate)
    assert recovered.status == "published"
    assert _pointer(database)["serving_version"] == candidate.serving_version


def test_live_pointer_replace_failure_preserves_old_pointer_and_retry_recovers(
    live_database, monkeypatch
):
    client, database, uri = live_database
    _old, pointer_before = _publish_baseline(client, database, uri)
    candidate = _later_snapshot(database)
    original = Collection.replace_one

    def fail_hybrid_pointer_replace(collection, filter_query, replacement, *args, **kwargs):
        if (
            collection.name == READINESS_COLLECTION
            and filter_query.get("_id") == f"hybrid:{USER_ID}"
        ):
            raise OSError("I2 injected pointer replace failure")
        return original(collection, filter_query, replacement, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(Collection, "replace_one", fail_hybrid_pointer_replace)
        with pytest.raises(HybridPublicationPromotionError):
            _publish(client, database, uri, candidate)
    assert _pointer(database) == pointer_before
    assert database[FINAL_RECOMMENDATIONS_COLLECTION].count_documents(
        {"serving_version": candidate.serving_version}
    ) == candidate.expected_rows

    recovered = _publish(client, database, uri, candidate)
    assert recovered.status == "published"
    assert _pointer(database)["serving_version"] == candidate.serving_version


@pytest.mark.parametrize(
    ("change", "expected_error"),
    [
        ("publishing", HybridPublicationPromotionError),
        ("failed", HybridPublicationPromotionError),
        ("run_id", HybridPublicationConflictError),
        ("model_version", HybridPublicationConflictError),
        ("graph_version", HybridPublicationConflictError),
    ],
)
def test_live_source_readiness_change_rejects_prepared_snapshot_and_preserves_pointer(
    live_database, change, expected_error
):
    client, database, uri = live_database
    _old, pointer_before = _publish_baseline(client, database, uri)
    candidate = _later_snapshot(database)
    if change in {"publishing", "failed"}:
        _seed_active(
            database,
            status=change,
            ready=False,
        )
    elif change == "run_id":
        _seed_active(database, run_id="i2-other-run")
    elif change == "model_version":
        _seed_active(database, model_version="i2-other-model")
    else:
        _seed_active(database, graph_version="i2-other-graph")

    with pytest.raises(expected_error):
        _publish(client, database, uri, candidate)
    assert _pointer(database) == pointer_before


def _install_deterministic_lock_contender(monkeypatch, *, winner_name, loser_name):
    actual_lock = module4_sink.publication_lock
    winner_acquired = Event()
    loser_attempt_finished = Event()

    @contextmanager
    def synchronized_lock(config, run_id, lease_minutes=180):
        thread_name = current_thread().name
        if thread_name == winner_name:
            with actual_lock(config, run_id, lease_minutes=lease_minutes) as lock_client:
                winner_acquired.set()
                if not loser_attempt_finished.wait(timeout=30):
                    raise AssertionError("concurrent loser did not finish its lock attempt")
                yield lock_client
            return
        if thread_name == loser_name:
            if not winner_acquired.wait(timeout=30):
                raise AssertionError("winner did not acquire the MongoDB publication lock")
            try:
                with actual_lock(config, run_id, lease_minutes=lease_minutes) as lock_client:
                    yield lock_client
            finally:
                loser_attempt_finished.set()
            return
        with actual_lock(config, run_id, lease_minutes=lease_minutes) as lock_client:
            yield lock_client

    monkeypatch.setattr(module4_sink, "publication_lock", synchronized_lock)
    return actual_lock


def _run_two_publishers(client_specs):
    outcomes = {}

    def run(name, client, database, uri, prepared):
        try:
            outcomes[name] = ("success", _publish(client, database, uri, prepared))
        except Exception as exc:
            outcomes[name] = ("error", exc)

    threads = [
        Thread(
            target=run,
            name=name,
            args=(name, client, database, uri, prepared),
        )
        for name, client, database, uri, prepared in client_specs
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=45)
    assert all(not thread.is_alive() for thread in threads), "MongoDB publisher thread did not finish"
    assert len(outcomes) == len(client_specs)
    return outcomes


def test_live_concurrent_initial_publications_do_not_regress_newer_pointer(
    live_database, monkeypatch
):
    _client, database, uri = live_database
    _seed_active(database)
    older = _prepare(database, as_of=AS_OF)[0]
    newer = _prepare(database, as_of=AS_OF + timedelta(hours=1))[0]
    older_client = MongoClient(uri, serverSelectionTimeoutMS=5000, tz_aware=True)
    newer_client = MongoClient(uri, serverSelectionTimeoutMS=5000, tz_aware=True)
    winner_name, loser_name = "i2-newer-winner", "i2-older-contender"
    _install_deterministic_lock_contender(
        monkeypatch,
        winner_name=winner_name,
        loser_name=loser_name,
    )
    try:
        outcomes = _run_two_publishers(
            [
                (winner_name, newer_client, database, uri, newer),
                (loser_name, older_client, database, uri, older),
            ]
        )
    finally:
        older_client.close()
        newer_client.close()

    assert outcomes[winner_name][0] == "success"
    assert outcomes[loser_name][0] == "error"
    assert isinstance(outcomes[loser_name][1], HybridPublicationPromotionError)
    assert _pointer(database)["serving_version"] == newer.serving_version
    with pytest.raises(HybridPublicationConflictError, match="stale"):
        _publish(_client, database, uri, older)
    assert _pointer(database)["serving_version"] == newer.serving_version


def test_live_identical_concurrent_retry_converges_without_duplicate_rows(
    live_database, monkeypatch
):
    _client, database, uri = live_database
    _seed_active(database)
    prepared = _prepare(database)[0]
    first_client = MongoClient(uri, serverSelectionTimeoutMS=5000, tz_aware=True)
    second_client = MongoClient(uri, serverSelectionTimeoutMS=5000, tz_aware=True)
    first_name, second_name = "i2-retry-winner", "i2-retry-contender"
    _install_deterministic_lock_contender(
        monkeypatch,
        winner_name=first_name,
        loser_name=second_name,
    )
    try:
        outcomes = _run_two_publishers(
            [
                (first_name, first_client, database, uri, prepared),
                (second_name, second_client, database, uri, prepared),
            ]
        )
    finally:
        first_client.close()
        second_client.close()

    assert outcomes[first_name][0] == "success", repr(outcomes)
    assert outcomes[second_name][0] == "error"
    assert isinstance(outcomes[second_name][1], HybridPublicationPromotionError)
    retried = _publish(_client, database, uri, prepared)
    assert retried.status == "already_active" and retried.idempotent is True
    assert database[FINAL_RECOMMENDATIONS_COLLECTION].count_documents(
        {"serving_version": prepared.serving_version, "user_id": USER_ID}
    ) == prepared.expected_rows


def test_live_concurrent_different_versions_at_same_as_of_cannot_replace_winner(
    live_database, monkeypatch
):
    _client, database, uri = live_database
    _seed_active(database)
    first = _prepare(database, lambda_weight=0.5)[0]
    second = _prepare(database, lambda_weight=0.2)[0]
    first_client = MongoClient(uri, serverSelectionTimeoutMS=5000, tz_aware=True)
    second_client = MongoClient(uri, serverSelectionTimeoutMS=5000, tz_aware=True)
    winner_name, loser_name = "i2-same-time-winner", "i2-same-time-contender"
    _install_deterministic_lock_contender(
        monkeypatch,
        winner_name=winner_name,
        loser_name=loser_name,
    )
    try:
        outcomes = _run_two_publishers(
            [
                (winner_name, first_client, database, uri, first),
                (loser_name, second_client, database, uri, second),
            ]
        )
    finally:
        first_client.close()
        second_client.close()

    assert outcomes[winner_name][0] == "success"
    assert outcomes[loser_name][0] == "error"
    assert isinstance(outcomes[loser_name][1], HybridPublicationPromotionError)
    with pytest.raises(HybridPublicationConflictError, match="same as_of_event_time"):
        _publish(_client, database, uri, second)
    assert _pointer(database)["serving_version"] == first.serving_version


def test_live_incompatible_trend_namespace_is_rejected(live_database):
    client, database, uri = live_database
    _seed_active(database)
    active = _prepare(database, namespace=NAMESPACE)[0]
    _publish(client, database, uri, active)
    before = _pointer(database)
    incompatible = _prepare(
        database,
        namespace=OTHER_NAMESPACE,
        as_of=AS_OF + timedelta(hours=1),
    )[0]

    with pytest.raises(HybridPublicationConflictError, match="trend source namespaces"):
        _publish(client, database, uri, incompatible)
    assert _pointer(database) == before


def test_live_separate_users_keep_independent_pointers_under_concurrent_attempts(
    live_database, monkeypatch
):
    _client, database, uri = live_database
    _seed_active(database)
    first = _prepare(database, user_id=7801)[0]
    second = _prepare(database, user_id=7802)[0]
    first_client = MongoClient(uri, serverSelectionTimeoutMS=5000, tz_aware=True)
    second_client = MongoClient(uri, serverSelectionTimeoutMS=5000, tz_aware=True)
    first_name, second_name = "i2-user-one", "i2-user-two"
    _install_deterministic_lock_contender(
        monkeypatch,
        winner_name=first_name,
        loser_name=second_name,
    )
    try:
        outcomes = _run_two_publishers(
            [
                (first_name, first_client, database, uri, first),
                (second_name, second_client, database, uri, second),
            ]
        )
    finally:
        first_client.close()
        second_client.close()

    assert outcomes[first_name][0] == "success"
    assert outcomes[second_name][0] == "error"
    assert isinstance(outcomes[second_name][1], HybridPublicationPromotionError)
    retried = _publish(_client, database, uri, second)
    assert retried.status == "published"
    assert _pointer(database, 7801)["serving_version"] == first.serving_version
    assert _pointer(database, 7802)["serving_version"] == second.serving_version


def test_live_empty_recommendations_publish_explicit_ready_zero_row_pointer(live_database):
    client, database, uri = live_database
    _seed_active(database)
    missing_trend = read_verified_trend_snapshot(
        database[BUNDLE_COLLECTION_NAME],
        source_namespace=NAMESPACE,
        as_of_event_time=AS_OF,
        processing_time_utc=PROCESSING_TIME,
    )
    captured = _captured(als_rows=())
    config = HybridRankingConfig(lambda_weight=0.5)
    ranked = rank_hybrid_recommendations(
        USER_ID,
        captured.als_recommendations,
        missing_trend.trend_snapshot,
        captured.product_catalog,
        captured.source_context,
        config,
    )
    prepared = prepare_hybrid_snapshot(
        captured,
        missing_trend,
        ranked,
        config,
        as_of_event_time=AS_OF,
    )

    result = _publish(client, database, uri, prepared)

    assert result.status == "published"
    assert result.expected_rows == 0
    assert prepared.mode == "no_candidates"
    assert prepared.reason == "user_has_no_als_recommendations_and_trend_snapshot_unavailable"
    assert database[FINAL_RECOMMENDATIONS_COLLECTION].count_documents(
        {"serving_version": prepared.serving_version}
    ) == 0
    pointer = _pointer(database)
    assert pointer["ready"] is True
    assert pointer["expected_rows"] == 0
    assert pointer["mode"] == prepared.mode
    assert pointer["reason"] == prepared.reason


def test_live_consumer_pointer_rows_pointer_protocol_rejects_during_competing_promotion(
    live_database, monkeypatch
):
    client, database, uri = live_database
    _seed_active(database)
    old = _prepare(database)[0]
    _publish(client, database, uri, old)
    candidate = _later_snapshot(database)
    rows_verified = Event()
    allow_promotion = Event()
    actual_lock = module4_sink.publication_lock

    @contextmanager
    def pause_after_row_verification(config, run_id, lease_minutes=180):
        rows_verified.set()
        if not allow_promotion.wait(timeout=30):
            raise AssertionError("consumer did not release the publication barrier")
        with actual_lock(config, run_id, lease_minutes=lease_minutes) as lock_client:
            yield lock_client

    monkeypatch.setattr(module4_sink, "publication_lock", pause_after_row_verification)
    outcome = {}

    def publish_candidate():
        try:
            outcome["result"] = _publish(client, database, uri, candidate)
        except Exception as exc:
            outcome["error"] = exc

    publisher = Thread(target=publish_candidate, name="i2-reader-protocol-publisher")
    publisher.start()
    try:
        assert rows_verified.wait(timeout=30)
        first_pointer = database[READINESS_COLLECTION].find_one({"_id": f"hybrid:{USER_ID}"})
        old_rows = list(
            database[FINAL_RECOMMENDATIONS_COLLECTION]
            .find({"serving_version": first_pointer["serving_version"], "user_id": USER_ID})
            .sort("rank", 1)
        )
        assert len(old_rows) == first_pointer["expected_rows"] == old.expected_rows
        assert [row["rank"] for row in old_rows] == list(range(1, len(old_rows) + 1))
        allow_promotion.set()
        publisher.join(timeout=30)
        assert not publisher.is_alive()
        assert "error" not in outcome
        second_pointer = database[READINESS_COLLECTION].find_one({"_id": f"hybrid:{USER_ID}"})
        accepted = (
            first_pointer["serving_version"] == second_pointer["serving_version"]
            and first_pointer["expected_rows"] == second_pointer["expected_rows"]
            and first_pointer["input_content_sha256"] == second_pointer["input_content_sha256"]
        )
        assert accepted is False
        assert second_pointer["serving_version"] == candidate.serving_version
    finally:
        allow_promotion.set()
        publisher.join(timeout=30)


def test_live_version_and_bson_precision_are_independent_of_publication_time(live_database):
    client, database, uri = live_database
    _seed_active(database)
    prepared, _trend, _ranked, _config_value = _prepare(database)
    same_prepared, _trend2, _ranked2, _config2 = _prepare(database)
    first_time = PROCESSING_TIME
    second_time = PROCESSING_TIME + timedelta(days=1)

    first = _publish(client, database, uri, prepared, processing_time=first_time)
    retry = _publish(client, database, uri, same_prepared, processing_time=second_time)

    assert prepared.serving_version == same_prepared.serving_version
    assert first.serving_version == retry.serving_version
    assert first.generated_at == first_time
    assert retry.generated_at == first.generated_at
    row = database[FINAL_RECOMMENDATIONS_COLLECTION].find_one(
        {"serving_version": prepared.serving_version}
    )
    assert row["generated_at"] == first_time
    assert row["generated_at"].microsecond % 1000 == 0
    assert row["as_of_event_time"] == prepared.as_of_event_time
    assert row["as_of_event_time"].utcoffset() == timedelta(0)
    assert _pointer(database)["as_of_event_time"] == prepared.as_of_event_time


def test_live_nonfinite_source_scores_are_rejected_before_publication(live_database):
    _client, database, _uri = live_database
    _seed_active(database)
    _valid, trend, ranked, config = _prepare(database)
    malformed_sources = _captured(als_rows=((1101, math.nan), (1102, 6.0)))
    with pytest.raises(HybridSnapshotContractError):
        prepare_hybrid_snapshot(
            malformed_sources,
            trend,
            ranked,
            config,
            as_of_event_time=AS_OF,
        )
