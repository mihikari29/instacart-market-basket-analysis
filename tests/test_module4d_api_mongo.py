"""Live read-only API checks against the disposable D1A MongoDB service."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import os
import time
import uuid

from fastapi.testclient import TestClient
from pymongo import MongoClient
from pymongo.errors import OperationFailure
from pymongo.monitoring import CommandListener
from pymongo.uri_parser import parse_uri
import pytest
from pyspark.sql import SparkSession

from src.module3.config import Config as Module3Config
from src.module3.trend_bundle import _build_document, write_trend_bundles
from src.module4.config import Config as Module4Config
from src.module4.sink_mongo import update_pair_readiness
from src.module4.hybrid import publish_hybrid_recommendations, read_verified_trend_snapshot
from src.module4.hybrid.data import READINESS_COLLECTION
from src.module4.hybrid.trend_reader import TrendSnapshotIntegrityError
from src.module4.dashboard.api import create_app
from src.module4.dashboard.config import ApiConfig
from src.module4.dashboard import readers
from tests.module4d_helpers import (
    BASE_TIME,
    GRAPH_VERSION,
    MODEL_VERSION,
    NAMESPACE,
    RUN_ID,
    SCOPE,
    make_snapshot,
)


TEST_URI_ENV = "D1A_MONGODB_TEST_URI"
ALLOWED_HOST = ("d1a-mongo", 27017)
FORBIDDEN_DATABASES = {"admin", "config", "instacart", "local"}
USER_HYBRID = 701
USER_EMPTY = 702
USER_TREND_ONLY = 703
USER_ALS_ONLY = 704


def _guarded_test_uri() -> str:
    uri = os.environ.get(TEST_URI_ENV)
    if not uri:
        pytest.fail(f"{TEST_URI_ENV} is required; run the isolated D1A MongoDB test harness")
    try:
        parsed = parse_uri(uri)
    except Exception:
        pytest.fail(f"{TEST_URI_ENV} is not a valid MongoDB URI")
    hosts = parsed.get("nodelist", ())
    if len(hosts) != 1 or (hosts[0][0].lower(), hosts[0][1]) != ALLOWED_HOST:
        pytest.fail(f"{TEST_URI_ENV} must target only d1a-mongo:27017")
    if parsed.get("database") not in (None, ""):
        pytest.fail(f"{TEST_URI_ENV} must not select a database")
    if parsed.get("username") != "root" or not parsed.get("password"):
        pytest.fail(f"{TEST_URI_ENV} must use the isolated MongoDB root fixture account")
    options = {str(key).lower(): value for key, value in (parsed.get("options") or {}).items()}
    if options != {"authsource": "admin"}:
        pytest.fail(f"{TEST_URI_ENV} may contain only authSource=admin")
    return uri


def _trend_document(
    window_end: datetime,
    *,
    namespace: str = NAMESPACE,
    product_id: int = 900,
    published_at: datetime | None = None,
    retention_seconds: int = 0,
    source_scope: str = SCOPE,
) -> dict:
    window_start = window_end - timedelta(minutes=30)
    item = {
        "window_start": window_start,
        "window_end": window_end,
        "product_id": product_id,
        "purchase_count_30m": 2,
        "purchase_count_120m": 3,
        "trend_score": 0.75,
        "trend_rank": 1,
    }
    return _build_document(
        {
            "window_start": window_start,
            "window_end": window_end,
            "items": [item],
            "_row_count": 1,
        },
        source_namespace=namespace,
        source_scope=source_scope,
        top_k=20,
        config_hash=hashlib.sha256(b"d1a-fix trend config").hexdigest(),
        published_at=published_at or BASE_TIME + timedelta(minutes=1),
        retention_seconds=retention_seconds,
    )


@pytest.mark.parametrize(
    "unsafe_uri",
    [
        "mongodb://root:secret@localhost:27017/?authSource=admin",
        "mongodb://root:secret@127.0.0.1:27017/?authSource=admin",
        "mongodb://root:secret@other-mongo:27017/?authSource=admin",
        "mongodb://root:secret@d1a-mongo:27017/instacart?authSource=admin",
        "mongodb://root:secret@d1a-mongo:27017/?authSource=admin&tls=true",
        "not-a-mongodb-uri",
    ],
)
def test_live_uri_guard_rejects_unintended_or_production_targets(monkeypatch, unsafe_uri):
    monkeypatch.setenv(TEST_URI_ENV, unsafe_uri)
    with pytest.raises(BaseException) as error:
        _guarded_test_uri()
    assert not isinstance(error.value, pytest.skip.Exception)


class _CommandRecorder(CommandListener):
    def __init__(self):
        self.events: list[tuple[str, str, dict]] = []

    def started(self, event):
        self.events.append((event.command_name, event.command.get("find", ""), dict(event.command)))

    def succeeded(self, event):
        pass

    def failed(self, event):
        pass


@dataclass
class LiveService:
    admin_client: MongoClient
    database: object
    database_name: str
    admin_uri: str
    readonly_uri: str
    api_config: ApiConfig
    active_pointer: dict
    pointers: dict[int, dict]
    rows: dict[int, list[dict]]
    spark: SparkSession


def _api_client(service: LiveService, listener: _CommandRecorder | None = None) -> TestClient:
    factory = None
    if listener is not None:
        def factory(uri, **kwargs):
            return MongoClient(uri, event_listeners=[listener], **kwargs)

    return TestClient(create_app(service.api_config, client_factory=factory))


@pytest.fixture(scope="module")
def live_service():
    admin_uri = _guarded_test_uri()
    admin_client = MongoClient(
        admin_uri,
        serverSelectionTimeoutMS=30_000,
        connectTimeoutMS=5_000,
        tz_aware=True,
        appname="module4d-live-fixture",
    )
    database_name = f"d1a_module4d_{uuid.uuid4().hex}"
    if database_name.lower() in FORBIDDEN_DATABASES:
        admin_client.close()
        pytest.fail("generated an unsafe live integration database name")
    database = admin_client[database_name]
    user_name = f"d1a_reader_{uuid.uuid4().hex}"
    user_password = uuid.uuid4().hex + uuid.uuid4().hex
    spark = None
    user_created = False
    database_owned = False
    try:
        deadline = time.monotonic() + 60
        while True:
            try:
                admin_client.admin.command("ping")
                break
            except Exception:
                if time.monotonic() >= deadline:
                    pytest.fail("isolated MongoDB did not become ready within 60 seconds")
                time.sleep(0.25)
        version = admin_client.server_info()["version"]
        if version != "7.0.16":
            pytest.fail(f"D1A requires MongoDB 7.0.16, received {version}")
        if database_name in admin_client.list_database_names():
            pytest.fail("generated integration database already exists")
        database_owned = True

        module4_config = Module4Config(mongodb_uri=admin_uri, mongo_database=database_name)
        update_pair_readiness(
            admin_client,
            module4_config,
            RUN_ID,
            MODEL_VERSION,
            GRAPH_VERSION,
            "ready",
            True,
            True,
        )

        spark = (
            SparkSession.builder.master("local[1]")
            .appName("d1a-module4d-fixture-writer")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.sql.warehouse.dir", f"/tmp/d1a-spark-{uuid.uuid4().hex}")
            .getOrCreate()
        )
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        window_end = BASE_TIME - timedelta(hours=1)
        window_start = window_end - timedelta(hours=2)
        frame = spark.createDataFrame(
            [
                {
                    "window_start": window_start,
                    "window_end": window_end,
                    "product_id": 110,
                    "purchase_count_30m": 8,
                    "purchase_count_120m": 14,
                    "trend_score": 0.95,
                    "trend_rank": 1,
                },
                {
                    "window_start": window_start,
                    "window_end": window_end,
                    "product_id": 111,
                    "purchase_count_30m": 5,
                    "purchase_count_120m": 9,
                    "trend_score": 0.65,
                    "trend_rank": 2,
                },
            ]
        )
        module3_config = Module3Config(
            mongodb_uri=admin_uri,
            mongo_database=database_name,
            mongo_top_k_per_window=20,
            ttl_seconds=0,
            publish_trend_bundles=True,
            trend_source_namespace=NAMESPACE,
            trend_source_scope=SCOPE,
        )
        write_trend_bundles(
            frame,
            database["realtime_trending_windows"],
            module3_config,
            source_namespace=NAMESPACE,
            source_scope=SCOPE,
            published_at=BASE_TIME + timedelta(minutes=1),
            retention_seconds=0,
        )
        database["realtime_trending_windows"].create_index(
            [("source_namespace", 1), ("window_end", -1)],
            name=readers.TREND_SELECTION_INDEX_NAME,
        )
        verified_trend = read_verified_trend_snapshot(
            database["realtime_trending_windows"],
            source_namespace=NAMESPACE,
            as_of_event_time=BASE_TIME,
            processing_time_utc=datetime.now(timezone.utc),
        )
        if not verified_trend.available:
            pytest.fail("accepted C5 reader did not select the fixture trend bundle")

        snapshots = {
            USER_HYBRID: make_snapshot(USER_HYBRID, als_count=2, trend_count=2, trend_result=verified_trend),
            USER_EMPTY: make_snapshot(USER_EMPTY, als_count=0, trend_count=0, trend_available=False),
            USER_TREND_ONLY: make_snapshot(
                USER_TREND_ONLY, als_count=0, trend_count=2, trend_result=verified_trend
            ),
            USER_ALS_ONLY: make_snapshot(
                USER_ALS_ONLY, als_count=2, trend_count=0, trend_available=False
            ),
        }
        pointers = {}
        rows_by_user = {}
        for user_id, (prepared, pointer, rows, _trend) in snapshots.items():
            publish_hybrid_recommendations(
                module4_config,
                prepared,
                client=admin_client,
                processing_time_utc=BASE_TIME + timedelta(minutes=2),
            )
            stored_pointer = database[READINESS_COLLECTION].find_one({"_id": f"hybrid:{user_id}"})
            stored_rows = list(
                database["final_recommendations"].find(
                    {"user_id": user_id, "serving_version": prepared.serving_version}
                )
            )
            pointers[user_id] = stored_pointer
            rows_by_user[user_id] = stored_rows

        database.command(
            "createUser",
            user_name,
            pwd=user_password,
            roles=[{"role": "read", "db": database_name}],
        )
        user_created = True
        readonly_uri = (
            f"mongodb://{user_name}:{user_password}@d1a-mongo:27017/{database_name}"
            f"?authSource={database_name}"
        )
        api_config = ApiConfig(
            readonly_uri,
            database_name,
            (NAMESPACE,),
            server_selection_timeout_ms=1500,
            connect_timeout_ms=1000,
            socket_timeout_ms=3000,
            max_time_ms=2500,
        )
        active_pointer = deepcopy(database[READINESS_COLLECTION].find_one({"_id": "active"}))
        yield LiveService(
            admin_client=admin_client,
            database=database,
            database_name=database_name,
            admin_uri=admin_uri,
            readonly_uri=readonly_uri,
            api_config=api_config,
            active_pointer=active_pointer,
            pointers=pointers,
            rows=rows_by_user,
            spark=spark,
        )
    finally:
        if user_created:
            try:
                database.command("dropUser", user_name)
            except Exception:
                pass
        if database_owned:
            try:
                admin_client.drop_database(database_name)
            except Exception:
                pass
        if spark is not None:
            spark.stop()
        admin_client.close()


def test_live_readonly_role_rejects_insert_and_update(live_service):
    readonly = MongoClient(live_service.readonly_uri, serverSelectionTimeoutMS=2500)
    database = readonly[live_service.database_name]
    existing = database["final_recommendations"].find_one({"user_id": USER_HYBRID})
    trends = database["realtime_trending_windows"]
    with pytest.raises(OperationFailure) as insert_error:
        database["final_recommendations"].insert_one({"_id": "forbidden-d1a-write"})
    with pytest.raises(OperationFailure) as update_error:
        database["final_recommendations"].update_one(
            {"_id": existing["_id"]}, {"$set": {"product_name": "forbidden"}}
        )
    assert insert_error.value.code == 13
    assert update_error.value.code == 13
    with pytest.raises(OperationFailure) as index_error:
        trends.create_index([("forbidden_probe", 1)], name="forbidden_d1a_index")
    assert index_error.value.code == 13
    assert readers.TREND_SELECTION_INDEX_NAME in trends.index_information()
    readonly.close()


def test_live_http_reads_hybrid_empty_trend_only_and_als_only_snapshots(live_service):
    with _api_client(live_service) as client:
        hybrid = client.get(f"/api/v1/recommendations/{USER_HYBRID}")
        empty = client.get(f"/api/v1/recommendations/{USER_EMPTY}")
        trend_only = client.get(f"/api/v1/recommendations/{USER_TREND_ONLY}")
        als_only = client.get(f"/api/v1/recommendations/{USER_ALS_ONLY}")
    assert hybrid.status_code == 200 and hybrid.json()["recommendation_count"] > 0
    assert empty.status_code == 200 and empty.json()["recommendations"] == []
    assert trend_only.status_code == 200
    assert all(item["from_trend"] and not item["from_als"] for item in trend_only.json()["recommendations"])
    assert als_only.status_code == 200
    assert all(item["from_als"] and not item["from_trend"] for item in als_only.json()["recommendations"])


def test_live_trend_api_reads_atomic_bundle_with_c5_selection(live_service):
    with _api_client(live_service) as client:
        response = client.get(
            "/api/v1/trends",
            params={"source_namespace": NAMESPACE, "as_of_event_time": BASE_TIME.isoformat()},
        )
    assert response.status_code == 200
    body = response.json()
    assert body["available"] is True
    assert body["source_scope"] == SCOPE
    assert body["row_count"] == 2
    assert body["content_sha256"]
    assert [item["trend_rank"] for item in body["items"]] == [1, 2]
    assert body["window_end"] == (BASE_TIME - timedelta(hours=1)).isoformat().replace("+00:00", "Z")


def test_live_malformed_newest_id_matches_c5_error_and_never_falls_back(live_service):
    collection = live_service.database["realtime_trending_windows"]
    older = _trend_document(BASE_TIME - timedelta(hours=2), product_id=910)
    newer = _trend_document(BASE_TIME - timedelta(minutes=30), product_id=911)
    newer["_id"] = f"malformed-newest-{uuid.uuid4().hex}"
    inserted_ids = [older["_id"], newer["_id"]]
    collection.insert_many([older, newer])
    before = list(collection.find({"_id": {"$in": inserted_ids}}).sort("_id", 1))
    listener = _CommandRecorder()
    try:
        with pytest.raises(TrendSnapshotIntegrityError):
            read_verified_trend_snapshot(
                collection,
                source_namespace=NAMESPACE,
                as_of_event_time=BASE_TIME,
                processing_time_utc=datetime.now(timezone.utc),
            )

        with _api_client(live_service, listener) as client:
            response = client.get(
                "/api/v1/trends",
                params={"source_namespace": NAMESPACE, "as_of_event_time": BASE_TIME.isoformat()},
            )
        assert response.status_code == 502
        assert response.json()["error"]["code"] == "invalid_source_document"
        assert "malformed-newest" not in response.text
        trend_finds = [
            command
            for name, _collection_name, command in listener.events
            if name == "find" and command.get("find") == collection.name
        ]
        assert len(trend_finds) == 1
        assert trend_finds[0]["filter"] == {
            "source_namespace": NAMESPACE,
            "window_end": {"$lte": BASE_TIME},
        }
        assert trend_finds[0]["sort"] == {"window_end": -1}
        assert trend_finds[0]["limit"] == 1
        assert trend_finds[0]["hint"] == readers.TREND_SELECTION_INDEX_NAME
        prohibited = {"insert", "update", "delete", "createIndexes", "drop", "dropDatabase", "bulkWrite"}
        assert prohibited.isdisjoint({name for name, _collection_name, _command in listener.events})
        assert list(collection.find({"_id": {"$in": inserted_ids}}).sort("_id", 1)) == before
    finally:
        collection.delete_many({"_id": {"$in": inserted_ids}})


def test_live_trend_namespace_prefix_and_character_variants_are_isolated(live_service):
    collection = live_service.database["realtime_trending_windows"]
    similar = [
        _trend_document(
            BASE_TIME - timedelta(minutes=10),
            namespace=NAMESPACE + "-child",
            product_id=920,
            source_scope="prefix_collision",
        ),
        _trend_document(
            BASE_TIME - timedelta(minutes=5),
            namespace=NAMESPACE.replace("d1a", "d1A"),
            product_id=921,
            source_scope="similar_characters",
        ),
    ]
    ids = [document["_id"] for document in similar]
    collection.insert_many(similar)
    try:
        with _api_client(live_service) as client:
            response = client.get(
                "/api/v1/trends",
                params={"source_namespace": NAMESPACE, "as_of_event_time": BASE_TIME.isoformat()},
            )
        assert response.status_code == 200
        body = response.json()
        assert body["source_namespace"] == NAMESPACE
        assert body["source_scope"] == SCOPE
        assert {row["product_id"] for row in body["items"]} == {110, 111}
    finally:
        collection.delete_many({"_id": {"$in": ids}})


def test_live_bson_millisecond_boundary_and_offset_timezone_are_exact(live_service):
    collection = live_service.database["realtime_trending_windows"]
    cutoff_utc = BASE_TIME + timedelta(microseconds=123_456)
    windows = [
        _trend_document(cutoff_utc.replace(microsecond=122_000), product_id=930),
        _trend_document(cutoff_utc.replace(microsecond=123_000), product_id=931),
        _trend_document(cutoff_utc.replace(microsecond=124_000), product_id=932),
    ]
    ids = [document["_id"] for document in windows]
    collection.insert_many(windows)
    try:
        local_offset = timezone(timedelta(hours=2))
        cutoff_text = cutoff_utc.astimezone(local_offset).isoformat()
        with _api_client(live_service) as client:
            response = client.get(
                "/api/v1/trends",
                params={"source_namespace": NAMESPACE, "as_of_event_time": cutoff_text},
            )
        assert response.status_code == 200
        body = response.json()
        assert body["available"] is True
        assert body["items"][0]["product_id"] == 931
        assert body["window_end"] == cutoff_utc.replace(microsecond=123_000).isoformat().replace("+00:00", "Z")
    finally:
        collection.delete_many({"_id": {"$in": ids}})


@pytest.mark.parametrize("state", ["expired", "stale"])
def test_live_expired_or_stale_newest_window_does_not_fall_back(live_service, state):
    collection = live_service.database["realtime_trending_windows"]
    older = _trend_document(BASE_TIME - timedelta(hours=2), product_id=940)
    newest = _trend_document(
        BASE_TIME - timedelta(minutes=20),
        product_id=941,
        published_at=BASE_TIME + timedelta(minutes=1),
        retention_seconds=1 if state == "expired" else 0,
    )
    ids = [older["_id"], newest["_id"]]
    collection.insert_many([older, newest])
    try:
        params = {"source_namespace": NAMESPACE, "as_of_event_time": BASE_TIME.isoformat()}
        if state == "stale":
            params["max_event_age_seconds"] = "0"
        with _api_client(live_service) as client:
            response = client.get("/api/v1/trends", params=params)
        assert response.status_code == 200
        body = response.json()
        assert body["available"] is False
        assert body["status"] == state
        assert body["items"] == []
    finally:
        collection.delete_many({"_id": {"$in": ids}})


def _plan_stages(node):
    if isinstance(node, dict):
        stage = node.get("stage")
        yield stage
        for value in node.values():
            yield from _plan_stages(value)
    elif isinstance(node, list):
        for value in node:
            yield from _plan_stages(value)


def test_live_c5_selection_query_uses_approved_compound_index(live_service):
    collection = live_service.database["realtime_trending_windows"]
    indexes = collection.index_information()
    definition = indexes[readers.TREND_SELECTION_INDEX_NAME]
    assert definition["key"] == [("source_namespace", 1), ("window_end", -1)]

    plan = (
        collection.find(
            {"source_namespace": NAMESPACE, "window_end": {"$lte": BASE_TIME}},
            max_time_ms=2500,
        )
        .sort([("window_end", -1)])
        .limit(1)
        .hint(readers.TREND_SELECTION_INDEX_NAME)
        .explain()
    )
    stages = list(_plan_stages(plan.get("queryPlanner", {}).get("winningPlan", {})))
    assert "IXSCAN" in stages
    assert "COLLSCAN" not in stages
    assert readers.TREND_SELECTION_INDEX_NAME in repr(plan)


def test_live_missing_compound_index_fails_without_finding_or_scanning(live_service):
    collection = live_service.database["realtime_trending_windows"]
    index_name = readers.TREND_SELECTION_INDEX_NAME
    collection.drop_index(index_name)
    listener = _CommandRecorder()
    try:
        with _api_client(live_service, listener) as client:
            response = client.get(
                "/api/v1/trends",
                params={"source_namespace": NAMESPACE, "as_of_event_time": BASE_TIME.isoformat()},
            )
        assert response.status_code == 503
        assert response.json()["error"]["code"] == "trend_index_unavailable"
        assert not any(
            name == "find" and command.get("find") == collection.name
            for name, _collection_name, command in listener.events
        )
    finally:
        collection.create_index(
            [("source_namespace", 1), ("window_end", -1)], name=index_name
        )


def test_live_pointer_change_during_read_returns_409_and_restores_fixture(monkeypatch, live_service):
    readiness = live_service.database[READINESS_COLLECTION]
    original_pointer = deepcopy(readiness.find_one({"_id": f"hybrid:{USER_HYBRID}"}))
    original_validate_rows = readers._validate_rows
    changed = deepcopy(original_pointer)
    changed["updated_at"] += timedelta(seconds=1)

    def mutate_after_rows(*args, **kwargs):
        result = original_validate_rows(*args, **kwargs)
        readiness.replace_one({"_id": changed["_id"]}, changed)
        return result

    monkeypatch.setattr(readers, "_validate_rows", mutate_after_rows)
    try:
        with _api_client(live_service) as client:
            response = client.get(f"/api/v1/recommendations/{USER_HYBRID}")
        assert response.status_code == 409
        assert response.json()["error"]["code"] == "snapshot_changed"
    finally:
        readiness.replace_one({"_id": original_pointer["_id"]}, original_pointer)


@pytest.mark.parametrize("corruption", ["row", "incomplete"])
def test_live_corrupt_or_incomplete_rows_fail_closed_and_are_restored(live_service, corruption):
    collection = live_service.database["final_recommendations"]
    original = deepcopy(live_service.rows[USER_HYBRID][0])
    if corruption == "row":
        collection.update_one({"_id": original["_id"]}, {"$set": {"model_version": "wrong-version"}})
    else:
        collection.delete_one({"_id": original["_id"]})
    try:
        with _api_client(live_service) as client:
            response = client.get(f"/api/v1/recommendations/{USER_HYBRID}")
        assert response.status_code == 502
        assert response.json()["error"]["code"] == "invalid_snapshot"
    finally:
        if corruption == "row":
            collection.replace_one({"_id": original["_id"]}, original)
        else:
            collection.insert_one(original)


def test_live_database_outage_is_503_without_connection_details(live_service):
    unavailable_config = ApiConfig(
        "mongodb://127.0.0.1:1/d1a_outage_test",
        "d1a_outage_test",
        (NAMESPACE,),
        server_selection_timeout_ms=250,
        connect_timeout_ms=200,
        socket_timeout_ms=250,
        max_time_ms=250,
    )
    with TestClient(create_app(unavailable_config)) as client:
        response = client.get(f"/api/v1/recommendations/{USER_HYBRID}")
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "service_unavailable"
    assert "127.0.0.1" not in response.text


def test_live_gets_issue_no_write_commands_and_do_not_change_indexes_or_active_pointer(live_service):
    collections_before = set(live_service.database.list_collection_names())
    indexes_before = {
        name: live_service.database[name].index_information() for name in collections_before
    }
    active_before = deepcopy(live_service.database[READINESS_COLLECTION].find_one({"_id": "active"}))
    listener = _CommandRecorder()
    with _api_client(live_service, listener) as client:
        assert client.get("/health").status_code == 200
        response = client.get(f"/api/v1/recommendations/{USER_HYBRID}")
        assert response.status_code == 200, response.text
        assert client.get(
            "/api/v1/trends",
            params={"source_namespace": NAMESPACE, "as_of_event_time": BASE_TIME.isoformat()},
        ).status_code == 200
    prohibited = {
        "insert",
        "update",
        "delete",
        "createIndexes",
        "drop",
        "dropDatabase",
        "renameCollection",
        "findAndModify",
        "bulkWrite",
    }
    assert prohibited.isdisjoint({name for name, _collection, _command in listener.events})
    assert set(live_service.database.list_collection_names()) == collections_before
    indexes_after = {
        name: live_service.database[name].index_information() for name in collections_before
    }
    assert indexes_after == indexes_before
    assert live_service.database[READINESS_COLLECTION].find_one({"_id": "active"}) == active_before
    find_commands = [command for name, _collection, command in listener.events if name == "find"]
    assert any(
        command.get("find") == "final_recommendations"
        and command.get("filter") == {
            "user_id": USER_HYBRID,
            "serving_version": live_service.pointers[USER_HYBRID]["serving_version"],
        }
        and command.get("sort") == {"rank": 1}
        and command.get("limit") == 21
        and command.get("hint") == "uq_serving_user_rank"
        for command in find_commands
    )
    assert any(
        command.get("find") == "realtime_trending_windows"
        and command.get("filter") == {
            "source_namespace": NAMESPACE,
            "window_end": {"$lte": BASE_TIME},
        }
        and command.get("sort") == {"window_end": -1}
        and command.get("limit") == 1
        and command.get("hint") == readers.TREND_SELECTION_INDEX_NAME
        for command in find_commands
    )
