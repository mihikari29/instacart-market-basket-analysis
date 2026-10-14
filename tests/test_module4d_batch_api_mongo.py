"""Live D1B read-only API checks against the isolated MongoDB 7.0.16 fixture."""

from __future__ import annotations

from datetime import datetime, timezone
import os
import time
import uuid

from fastapi.testclient import TestClient
from pymongo import MongoClient
from pymongo.errors import OperationFailure
from pymongo.monitoring import CommandListener
from pymongo.uri_parser import parse_uri
import pytest

from src.module4d.api import create_app
from src.module4d.batch_readers import PRODUCT_INDEXES
from src.module4d.config import ApiConfig


TEST_URI_ENV = "D1B_MONGODB_TEST_URI"
ALLOWED_HOST = ("d1b-mongo", 27017)
FORBIDDEN_DATABASES = {"admin", "config", "instacart", "local"}
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _guarded_test_uri() -> str:
    uri = os.environ.get(TEST_URI_ENV)
    if not uri:
        pytest.fail(f"{TEST_URI_ENV} is required; run the isolated D1B MongoDB test harness")
    try:
        parsed = parse_uri(uri)
    except Exception:
        pytest.fail(f"{TEST_URI_ENV} is not a valid MongoDB URI")
    hosts = parsed.get("nodelist", ())
    if len(hosts) != 1 or (hosts[0][0].lower(), hosts[0][1]) != ALLOWED_HOST:
        pytest.fail(f"{TEST_URI_ENV} must target only d1b-mongo:27017")
    if parsed.get("database") not in (None, ""):
        pytest.fail(f"{TEST_URI_ENV} must not select a database")
    if parsed.get("username") != "root" or not parsed.get("password"):
        pytest.fail(f"{TEST_URI_ENV} must use the isolated MongoDB root fixture account")
    options = {str(key).lower(): value for key, value in (parsed.get("options") or {}).items()}
    if options != {"authsource": "admin"}:
        pytest.fail(f"{TEST_URI_ENV} may contain only authSource=admin")
    return uri


class _CommandRecorder(CommandListener):
    def __init__(self):
        self.commands: list[tuple[str, dict]] = []

    def started(self, event):
        self.commands.append((event.command_name, dict(event.command)))

    def succeeded(self, event):
        pass

    def failed(self, event):
        pass


def _product(product_id: int, purchase_count: int, reorder_count: int) -> dict:
    return {
        "_id": product_id,
        "product_id": product_id,
        "purchase_count": purchase_count,
        "unique_users": min(purchase_count, 10),
        "reorder_count": reorder_count,
        "reorder_rate": reorder_count / purchase_count,
        "run_id": "d1b-live-run",
        "last_batch_time": datetime(2026, 10, 14, tzinfo=timezone.utc),
    }


def _department(department_id: int) -> dict:
    return {
        "_id": department_id,
        "department_id": department_id,
        "purchase_count": 1000 - department_id,
        "unique_users": 100,
        "reorder_rate": 0.4,
        "run_id": "d1b-live-run",
        "last_batch_time": datetime(2026, 10, 14, tzinfo=timezone.utc),
    }


def _stages(value) -> list[str]:
    stages = []
    if isinstance(value, dict):
        stage = value.get("stage")
        if isinstance(stage, str):
            stages.append(stage)
        for child in value.values():
            stages.extend(_stages(child))
    elif isinstance(value, list):
        for child in value:
            stages.extend(_stages(child))
    return stages


@pytest.fixture(scope="module")
def live_service():
    admin_uri = _guarded_test_uri()
    admin_client = MongoClient(admin_uri, serverSelectionTimeoutMS=30_000, connectTimeoutMS=5_000)
    database_name = f"d1b_module4d_{uuid.uuid4().hex}"
    if database_name.lower() in FORBIDDEN_DATABASES:
        admin_client.close()
        pytest.fail("generated an unsafe live integration database name")
    database = admin_client[database_name]
    user_name = f"d1b_reader_{uuid.uuid4().hex}"
    user_password = uuid.uuid4().hex + uuid.uuid4().hex
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
        if admin_client.server_info()["version"] != "7.0.16":
            pytest.fail("D1B integration requires MongoDB 7.0.16")
        if database_name in admin_client.list_database_names():
            pytest.fail("generated D1B database already exists")
        database_owned = True

        database["batch_product_metrics"].insert_many(
            [
                _product(1, 100, 40),
                _product(2, 100, 40),
                _product(3, 50, 40),
            ]
        )
        database["department_metrics"].insert_many([_department(i) for i in range(1, 22)])
        for order, (index_name, keys) in PRODUCT_INDEXES.items():
            database["batch_product_metrics"].create_index(list(keys), name=index_name)
        database.command(
            "createUser",
            user_name,
            pwd=user_password,
            roles=[{"role": "read", "db": database_name}],
        )
        user_created = True
        readonly_uri = (
            f"mongodb://{user_name}:{user_password}@d1b-mongo:27017/{database_name}?authSource={database_name}"
        )
        config = ApiConfig(
            readonly_uri,
            database_name,
            ("d1b-live-namespace",),
            application_root=ROOT,
        )
        yield {
            "admin_client": admin_client,
            "database": database,
            "database_name": database_name,
            "readonly_uri": readonly_uri,
            "config": config,
            "user_name": user_name,
            "user_password": user_password,
        }
    finally:
        if user_created:
            try:
                database.command("dropUser", user_name)
            except Exception:
                pass
        if database_owned:
            admin_client.drop_database(database_name)
        admin_client.close()


def _api_client(service, recorder=None):
    factory = None
    if recorder is not None:
        def factory(uri, **kwargs):
            return MongoClient(uri, event_listeners=[recorder], **kwargs)

    return TestClient(create_app(service["config"], client_factory=factory))


def test_live_product_rankings_use_indexed_sort_and_deterministic_ties(live_service):
    database = live_service["database"]
    with _api_client(live_service) as client:
        purchased = client.get("/api/v1/batch/products", params={"order": "purchase_count", "limit": 10})
        reordered = client.get("/api/v1/batch/products", params={"order": "reorder_rate", "limit": 10})
    assert purchased.status_code == reordered.status_code == 200
    assert [row["product_id"] for row in purchased.json()["items"]] == [1, 2, 3]
    assert [row["product_id"] for row in reordered.json()["items"]] == [3, 1, 2]
    assert purchased.json()["source"]["run_id"] == "d1b-live-run"

    for order, (index_name, keys) in PRODUCT_INDEXES.items():
        explain = database.command(
            "explain",
            {
                "find": "batch_product_metrics",
                "filter": {},
                "projection": {"_id": 1, "product_id": 1, "purchase_count": 1, "reorder_rate": 1},
                "sort": dict(((order, -1), ("product_id", 1))),
                "hint": index_name,
                "limit": 20,
            },
            verbosity="queryPlanner",
        )
        stages = _stages(explain["queryPlanner"]["winningPlan"])
        assert "IXSCAN" in stages
        assert "COLLSCAN" not in stages
        assert "SORT" not in stages


def test_live_department_endpoint_returns_all_21_rows_with_stable_order(live_service):
    with _api_client(live_service) as client:
        response = client.get("/api/v1/batch/departments")
    assert response.status_code == 200
    rows = response.json()["items"]
    assert len(rows) == 21
    assert [row["department_id"] for row in rows] == list(range(1, 22))
    assert all(row["run_id"] == "d1b-live-run" for row in rows)
    explain = live_service["database"].command(
        "explain",
        {
            "find": "department_metrics",
            "filter": {},
            "projection": {"_id": 1, "department_id": 1, "purchase_count": 1},
            "sort": {"_id": 1},
            "limit": 22,
        },
        verbosity="queryPlanner",
    )
    stages = _stages(explain["queryPlanner"]["winningPlan"])
    assert "IXSCAN" in stages
    assert "COLLSCAN" not in stages
    assert "SORT" not in stages


def test_live_read_role_cannot_write_or_create_indexes(live_service):
    read_client = MongoClient(live_service["readonly_uri"], serverSelectionTimeoutMS=5_000)
    database = read_client[live_service["database_name"]]
    try:
        with pytest.raises(OperationFailure):
            database["batch_product_metrics"].insert_one(_product(9, 1, 0))
        with pytest.raises(OperationFailure):
            database["batch_product_metrics"].create_index([("arbitrary", 1)])
    finally:
        read_client.close()


def test_live_get_requests_do_not_change_database_and_issue_no_write_commands(live_service):
    recorder = _CommandRecorder()
    database = live_service["database"]
    collections_before = set(database.list_collection_names())
    product_count_before = database["batch_product_metrics"].count_documents({})
    department_count_before = database["department_metrics"].count_documents({})
    indexes_before = {
        name: list(database[name].list_indexes()) for name in ("batch_product_metrics", "department_metrics")
    }
    with _api_client(live_service, recorder) as client:
        assert client.get("/api/v1/batch/products").status_code == 200
        assert client.get("/api/v1/batch/departments").status_code == 200
    assert set(database.list_collection_names()) == collections_before
    assert database["batch_product_metrics"].count_documents({}) == product_count_before
    assert database["department_metrics"].count_documents({}) == department_count_before
    indexes_after = {
        name: list(database[name].list_indexes()) for name in ("batch_product_metrics", "department_metrics")
    }
    assert indexes_after == indexes_before
    write_commands = {"insert", "update", "delete", "createIndexes", "dropIndexes", "renameCollection", "drop"}
    assert not write_commands.intersection(command for command, _ in recorder.commands)


def test_live_missing_compound_index_returns_explicit_error_without_find(live_service):
    database = live_service["database"]
    index_name = PRODUCT_INDEXES["reorder_rate"][0]
    database["batch_product_metrics"].drop_index(index_name)
    recorder = _CommandRecorder()
    try:
        with _api_client(live_service, recorder) as client:
            response = client.get("/api/v1/batch/products", params={"order": "reorder_rate"})
        assert response.status_code == 503
        assert response.json()["error"]["code"] == "batch_index_unavailable"
        assert not any(
            command == "find" and payload.get("find") == "batch_product_metrics"
            for command, payload in recorder.commands
        )
    finally:
        keys = PRODUCT_INDEXES["reorder_rate"][1]
        database["batch_product_metrics"].create_index(list(keys), name=index_name)
