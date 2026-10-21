"""Disposable MongoDB vertical-slice tests for the Module 4C serving CLI."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
import subprocess
import sys
import uuid

import pytest
from pymongo import MongoClient
from pymongo.uri_parser import parse_uri

from src.module3.trend_bundle import BUNDLE_COLLECTION_NAME, build_document
from src.module4.hybrid.data import READINESS_COLLECTION
from src.module4.hybrid.sink_mongo import FINAL_RECOMMENDATIONS_COLLECTION


pytestmark = pytest.mark.integration

TEST_URI_ENV = "I3_MONGODB_TEST_URI"
ALLOWED_HOST = ("i3-mongo", 27017)
FORBIDDEN_DATABASES = {"admin", "config", "instacart", "local"}
RUN_ID = "i3-cli-source-run-001"
MODEL_VERSION = "i3-cli-als-v1"
GRAPH_VERSION = "i3-cli-graph-v1"
NAMESPACE = "i3:module3:cli-fixture:v1"
BASE_START = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
FIXTURE_PUBLISHED_AT = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
FIXTURE_CONFIG_HASH = hashlib.sha256(b"i3-c7b-trend-config").hexdigest()


def _dedicated_uri() -> str:
    uri = os.environ.get(TEST_URI_ENV)
    if not uri:
        pytest.skip(
            f"{TEST_URI_ENV} is not set; live C7B tests require the isolated i3-mongo service"
        )
    try:
        parsed = parse_uri(uri)
    except Exception as exc:
        pytest.fail(f"{TEST_URI_ENV} is not a valid MongoDB URI: {exc}")
    hosts = parsed.get("nodelist", ())
    if len(hosts) != 1 or (hosts[0][0].lower(), hosts[0][1]) != ALLOWED_HOST:
        pytest.fail(f"{TEST_URI_ENV} must target only the isolated Docker alias i3-mongo:27017")
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
        "mongodb://i2-mongo:27017",
        "mongodb://i3-mongo:27017/instacart",
        "mongodb://fixture:secret@i3-mongo:27017",
        "mongodb://i3-mongo:27017/?authSource=admin",
        "not-a-mongodb-uri",
    ],
)
def test_i3_uri_guard_rejects_other_hosts_credentials_database_and_options(
    monkeypatch, unsafe_uri
):
    monkeypatch.setenv(TEST_URI_ENV, unsafe_uri)
    with pytest.raises(BaseException) as error:
        _dedicated_uri()
    assert not isinstance(error.value, pytest.skip.Exception)


def test_i3_uri_guard_skips_clearly_when_unconfigured(monkeypatch):
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
    database_name = f"i3_module4c_cli_{uuid.uuid4().hex}"
    if database_name.lower() in FORBIDDEN_DATABASES:
        client.close()
        pytest.fail("generated a reserved integration database name")
    owned = False
    try:
        client.admin.command("ping")
        version = client.server_info()["version"]
        if version != "7.0.16":
            pytest.fail(f"C7B requires MongoDB 7.0.16, received {version}")
        if database_name in client.list_database_names():
            pytest.fail("generated integration database already exists")
        database = client[database_name]
        database["_c7b_test_ownership"].insert_one({"_id": "created-by-c7b-test"})
        owned = True
        yield {"client": client, "database": database, "name": database_name, "uri": uri}
    finally:
        try:
            if owned:
                client.drop_database(database_name)
        finally:
            client.close()


def _seed_active(database, *, status="ready", ready=True):
    document = {
        "_id": "active",
        "status": status,
        "ready": ready,
        "run_id": RUN_ID,
        "model_version": MODEL_VERSION,
        "graph_version": GRAPH_VERSION,
        "recommendation_published": True,
        "graph_published": True,
        "single_writer_policy": "module4_publication_lock",
        "updated_at": FIXTURE_PUBLISHED_AT,
        "c7b_preservation_marker": "keep-active-ab-readiness-unchanged",
    }
    database[READINESS_COLLECTION].replace_one({"_id": "active"}, document, upsert=True)
    return database[READINESS_COLLECTION].find_one({"_id": "active"})


def _seed_user(database, user_id, *, with_als=True, extra_product_ids=()):
    product_ids = tuple(range(1, 21))
    if with_als:
        database["user_recommendations"].insert_many(
            [
                {
                    "_id": f"i3-als:{user_id}:{product_id}",
                    "user_id": user_id,
                    "product_id": product_id,
                    "als_score": 10.0 if product_id == 1 else 1.0 if product_id == 2 else 0.0,
                    "als_rank": rank,
                    "model_version": MODEL_VERSION,
                    "run_id": RUN_ID,
                }
                for rank, product_id in enumerate(product_ids, start=1)
            ]
        )
    graph_ids = tuple(sorted(set(product_ids if with_als else ()).union(extra_product_ids)))
    if graph_ids:
        database["product_graph_metrics"].insert_many(
            [
                {
                    "_id": f"i3-graph:{product_id}",
                    "product_id": product_id,
                    "product_name": f"I3 Fixture Product {product_id}",
                    "graph_version": GRAPH_VERSION,
                    "run_id": RUN_ID,
                }
                for product_id in graph_ids
            ]
        )
    return product_ids


def _bundle_document(
    *,
    namespace=NAMESPACE,
    window_start=BASE_START,
    window_end=BASE_START + timedelta(hours=1),
    specifications=((2, 0.8), (100, 0.6)),
    published_at=FIXTURE_PUBLISHED_AT,
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
        for rank, (product_id, score) in enumerate(specifications, start=1)
    ]
    return build_document(
        {
            "window_start": window_start,
            "window_end": window_end,
            "items": items,
            "_row_count": len(items),
        },
        source_namespace=namespace,
        source_scope="synthetic_i3_fixture",
        top_k=20,
        config_hash=FIXTURE_CONFIG_HASH,
        published_at=published_at,
        retention_seconds=0,
    )


def _insert_bundle(database, **kwargs):
    document = _bundle_document(**kwargs)
    database[BUNDLE_COLLECTION_NAME].insert_one(document)
    return document


def _as_of(value):
    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _cli(database_info, *, user_id, namespace=NAMESPACE, as_of, lambda_weight=None):
    command = [
        sys.executable,
        "-m",
        "src.module4.hybrid.cli",
        "serve-user",
        "--user-id",
        str(user_id),
        "--source-namespace",
        namespace,
        "--as-of-event-time",
        _as_of(as_of),
        "--mongodb-uri",
        database_info["uri"],
        "--mongo-database",
        database_info["name"],
    ]
    if lambda_weight is not None:
        command.extend(("--lambda-weight", str(lambda_weight)))
    return subprocess.run(
        command,
        cwd=os.getcwd(),
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        timeout=45,
        check=False,
    )


def _assert_success(process):
    assert process.returncode == 0, process.stderr
    assert process.stderr == ""
    return json.loads(process.stdout)


def _consumer_read(database, user_id):
    readiness = database[READINESS_COLLECTION]
    first = readiness.find_one({"_id": f"hybrid:{user_id}"})
    assert first is not None and first["ready"] is True
    assert first["status"] == "ready"
    rows = list(
        database[FINAL_RECOMMENDATIONS_COLLECTION]
        .find({"user_id": user_id, "serving_version": first["serving_version"]})
        .sort("rank", 1)
    )
    assert len(rows) == first["expected_rows"]
    assert [row["rank"] for row in rows] == list(range(1, len(rows) + 1))
    assert len({row["product_id"] for row in rows}) == len(rows)
    second = readiness.find_one({"_id": f"hybrid:{user_id}"})
    accepted = (
        second is not None
        and first["_id"] == second["_id"]
        and first["serving_version"] == second["serving_version"]
        and first["expected_rows"] == second["expected_rows"]
        and first["input_content_sha256"] == second["input_content_sha256"]
    )
    assert accepted
    return first, rows, second


def _assert_rows_for_version(database, user_id, serving_version, expected_count):
    rows = list(
        database[FINAL_RECOMMENDATIONS_COLLECTION]
        .find({"user_id": user_id, "serving_version": serving_version})
        .sort("rank", 1)
    )
    assert len(rows) == expected_count
    assert [row["rank"] for row in rows] == list(range(1, len(rows) + 1))
    assert len({row["product_id"] for row in rows}) == len(rows)
    return rows


def test_cli_warm_hybrid_publication_and_consumer_pointer_protocol(live_database):
    database = live_database["database"]
    active_before = _seed_active(database)
    _seed_user(database, 8101, extra_product_ids=(100,))
    bundle = _insert_bundle(database)

    report = _assert_success(
        _cli(live_database, user_id=8101, as_of=BASE_START + timedelta(hours=1))
    )

    pointer, rows, pointer_again = _consumer_read(database, 8101)
    assert report["status"] == "published"
    assert report["ranking_mode"] == "hybrid"
    assert report["trend_window_end"] == _as_of(bundle["window_end"])
    assert report["trend_content_sha256"] == bundle["content_sha256"]
    assert report["serving_version"] == pointer["serving_version"]
    assert pointer == pointer_again
    assert pointer["source_run_id"] == RUN_ID
    assert pointer["model_version"] == MODEL_VERSION
    assert pointer["graph_version"] == GRAPH_VERSION
    assert pointer["trend_source_namespace"] == NAMESPACE
    assert pointer["trend_content_sha256"] == bundle["content_sha256"]
    assert all(row["user_id"] == 8101 for row in rows)
    assert all(row["source_run_id"] == RUN_ID for row in rows)
    assert all(row["model_version"] == MODEL_VERSION for row in rows)
    assert all(row["graph_version"] == GRAPH_VERSION for row in rows)
    assert all(row["trend_content_sha256"] == bundle["content_sha256"] for row in rows)
    assert database[READINESS_COLLECTION].find_one({"_id": "active"}) == active_before
    assert "realtime_trending" not in database.list_collection_names()


def test_cli_identical_retry_keeps_version_rows_and_ready_pointer(live_database):
    database = live_database["database"]
    _seed_active(database)
    _seed_user(database, 8102, extra_product_ids=(100,))
    _insert_bundle(database)
    arguments = {"user_id": 8102, "as_of": BASE_START + timedelta(hours=1)}

    first = _assert_success(_cli(live_database, **arguments))
    rows_before = _assert_rows_for_version(
        database, 8102, first["serving_version"], first["published_rows"]
    )
    pointer_before = database[READINESS_COLLECTION].find_one({"_id": "hybrid:8102"})
    retry = _assert_success(_cli(live_database, **arguments))
    rows_after = _assert_rows_for_version(
        database, 8102, retry["serving_version"], retry["published_rows"]
    )

    assert retry["status"] == "already_active"
    assert retry["serving_version"] == first["serving_version"]
    assert rows_after == rows_before
    assert database[READINESS_COLLECTION].find_one({"_id": "hybrid:8102"}) == pointer_before


def test_cli_cold_user_publishes_trend_only_rows_with_null_als_scores(live_database):
    database = live_database["database"]
    _seed_active(database)
    _seed_user(database, 8103, with_als=False, extra_product_ids=(501, 502))
    bundle = _insert_bundle(
        database,
        specifications=((501, 0.9), (502, 0.4)),
    )

    report = _assert_success(
        _cli(live_database, user_id=8103, as_of=BASE_START + timedelta(hours=1))
    )

    pointer, rows, _pointer_again = _consumer_read(database, 8103)
    assert report["ranking_mode"] == "cold_user_trend_only"
    assert pointer["expected_rows"] == 2
    assert pointer["trend_content_sha256"] == bundle["content_sha256"]
    assert {row["product_id"] for row in rows} == {501, 502}
    assert all(row["als_score"] is None and row["from_als"] is False for row in rows)
    assert all(row["trend_score"] is not None and row["from_trend"] is True for row in rows)


def test_cli_warm_user_missing_trend_publishes_als_fallback(live_database):
    database = live_database["database"]
    _seed_active(database)
    _seed_user(database, 8104)

    report = _assert_success(
        _cli(
            live_database,
            user_id=8104,
            namespace="i3:module3:no-window:v1",
            as_of=BASE_START + timedelta(hours=2),
        )
    )

    pointer, rows, _pointer_again = _consumer_read(database, 8104)
    assert report["ranking_mode"] == "als_fallback_trend_unavailable"
    assert report["fallback_reason"] == "no_matching_snapshot"
    assert report["trend_availability_status"] == "missing"
    assert pointer["reason"] == "no_matching_snapshot"
    assert len(rows) == 20
    assert all(row["trend_score"] is None and row["from_trend"] is False for row in rows)


def test_cli_cold_user_without_trend_publishes_ready_zero_row_snapshot(live_database):
    database = live_database["database"]
    _seed_active(database)
    _seed_user(database, 8105, with_als=False)

    report = _assert_success(
        _cli(
            live_database,
            user_id=8105,
            namespace="i3:module3:empty-user:v1",
            as_of=BASE_START + timedelta(hours=2),
        )
    )

    pointer, rows, _pointer_again = _consumer_read(database, 8105)
    assert report["ranking_mode"] == "no_candidates"
    assert report["fallback_reason"] == (
        "user_has_no_als_recommendations_and_trend_snapshot_unavailable"
    )
    assert rows == []
    assert pointer["expected_rows"] == 0
    assert pointer["ready"] is True


def test_cli_corrupt_bundle_fails_closed_without_advancing_pointer(live_database):
    database = live_database["database"]
    _seed_active(database)
    _seed_user(database, 8106, extra_product_ids=(100,))
    bundle = _insert_bundle(database)
    changed_items = [dict(item) for item in bundle["items"]]
    changed_items[0]["trend_score"] = 0.123
    database[BUNDLE_COLLECTION_NAME].update_one(
        {"_id": bundle["_id"]}, {"$set": {"items": changed_items}}
    )

    process = _cli(live_database, user_id=8106, as_of=BASE_START + timedelta(hours=1))

    assert process.returncode != 0
    assert process.stdout == ""
    assert "TrendSnapshotIntegrityError" in process.stderr
    assert database[READINESS_COLLECTION].find_one({"_id": "hybrid:8106"}) is None


def test_cli_invalid_ab_readiness_fails_without_publication(live_database):
    database = live_database["database"]
    _seed_active(database, status="publishing", ready=False)
    _seed_user(database, 8107)
    _insert_bundle(database)

    process = _cli(live_database, user_id=8107, as_of=BASE_START + timedelta(hours=1))

    assert process.returncode != 0
    assert process.stdout == ""
    assert "ReadinessContractError" in process.stderr
    assert database[READINESS_COLLECTION].find_one({"_id": "hybrid:8107"}) is None


def test_cli_trend_change_uses_independent_ranking_oracle_and_keeps_old_version(live_database):
    database = live_database["database"]
    _seed_active(database)
    _seed_user(database, 8108, extra_product_ids=(100,))
    first_end = BASE_START + timedelta(hours=1)
    second_end = BASE_START + timedelta(hours=2)
    _insert_bundle(
        database,
        window_start=BASE_START,
        window_end=first_end,
        specifications=((2, 0.1), (100, 1.0)),
    )
    second_bundle = _insert_bundle(
        database,
        window_start=first_end,
        window_end=second_end,
        specifications=((2, 1.0), (100, 0.1)),
    )

    earlier = _assert_success(
        _cli(
            live_database,
            user_id=8108,
            as_of=first_end,
            lambda_weight=0.4,
        )
    )
    earlier_rows = _assert_rows_for_version(
        database, 8108, earlier["serving_version"], earlier["published_rows"]
    )
    later = _assert_success(
        _cli(
            live_database,
            user_id=8108,
            as_of=second_end,
            lambda_weight=0.4,
        )
    )

    pointer, later_rows, _pointer_again = _consumer_read(database, 8108)
    assert earlier["trend_window_end"] == _as_of(first_end)
    assert later["trend_window_end"] == _as_of(second_bundle["window_end"])
    assert earlier["serving_version"] != later["serving_version"]
    assert earlier_rows[0]["product_id"] == 100
    assert later_rows[0]["product_id"] == 2
    assert pointer["serving_version"] == later["serving_version"]
    assert _assert_rows_for_version(
        database, 8108, earlier["serving_version"], earlier["published_rows"]
    ) == earlier_rows


def test_cli_als_only_to_hybrid_transition_promotes_same_namespace(live_database):
    database = live_database["database"]
    _seed_active(database)
    _seed_user(database, 8109, extra_product_ids=(100,))
    earlier_as_of = BASE_START + timedelta(hours=1)

    fallback = _assert_success(
        _cli(live_database, user_id=8109, as_of=earlier_as_of)
    )
    fallback_rows = _assert_rows_for_version(
        database, 8109, fallback["serving_version"], fallback["published_rows"]
    )
    assert fallback["ranking_mode"] == "als_fallback_trend_unavailable"
    assert fallback["fallback_reason"] == "no_matching_snapshot"
    fallback_pointer = database[READINESS_COLLECTION].find_one({"_id": "hybrid:8109"})
    assert fallback_pointer["trend_source_namespace"] == NAMESPACE

    later_end = BASE_START + timedelta(hours=2)
    bundle = _insert_bundle(
        database,
        window_start=earlier_as_of,
        window_end=later_end,
        specifications=((2, 0.7), (100, 0.8)),
    )
    hybrid = _assert_success(
        _cli(live_database, user_id=8109, as_of=later_end)
    )

    pointer, hybrid_rows, _pointer_again = _consumer_read(database, 8109)
    assert hybrid["ranking_mode"] == "hybrid"
    assert hybrid["trend_content_sha256"] == bundle["content_sha256"]
    assert hybrid["serving_version"] != fallback["serving_version"]
    assert pointer["serving_version"] == hybrid["serving_version"]
    assert _assert_rows_for_version(
        database, 8109, fallback["serving_version"], fallback["published_rows"]
    ) == fallback_rows
    assert {2, 100}.issubset(
        {row["product_id"] for row in hybrid_rows if row["trend_score"] is not None}
    )


def test_cli_stale_asof_cannot_regress_newer_ready_pointer(live_database):
    database = live_database["database"]
    _seed_active(database)
    _seed_user(database, 8110, extra_product_ids=(100,))
    first_end = BASE_START + timedelta(hours=1)
    second_end = BASE_START + timedelta(hours=2)
    _insert_bundle(
        database,
        window_start=BASE_START,
        window_end=first_end,
        specifications=((2, 0.8), (100, 0.5)),
    )
    _insert_bundle(
        database,
        window_start=first_end,
        window_end=second_end,
        specifications=((2, 0.9), (100, 0.6)),
    )
    newer = _assert_success(
        _cli(live_database, user_id=8110, as_of=second_end)
    )
    pointer_before = database[READINESS_COLLECTION].find_one({"_id": "hybrid:8110"})

    older_attempt = _cli(live_database, user_id=8110, as_of=first_end)

    assert older_attempt.returncode != 0
    assert older_attempt.stdout == ""
    assert "stale as_of_event_time" in older_attempt.stderr
    assert database[READINESS_COLLECTION].find_one({"_id": "hybrid:8110"}) == pointer_before
    assert pointer_before["serving_version"] == newer["serving_version"]
