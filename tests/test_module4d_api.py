"""Network-free HTTP tests for the Module 4D read API."""

from __future__ import annotations

from datetime import timedelta
import hashlib
import sys

from fastapi.testclient import TestClient
import pytest
from pyspark import SparkContext

from src.module3.trend_bundle import _build_document
from src.module4d.api import create_app
from src.module4d.config import ApiConfig
from tests.module4d_helpers import BASE_TIME, NAMESPACE, make_snapshot
from tests.test_module4d_readers import Database


def _config():
    return ApiConfig("mongodb://demo-user:demo-secret@mongo.invalid:27017/d1a_test", "d1a_test", (NAMESPACE,))


def _app(database):
    return create_app(_config(), database=database)


def test_backend_configuration_requires_explicit_database_uri_and_namespace(monkeypatch):
    for key in (
        "MODULE4D_MONGODB_URI",
        "MODULE4D_MONGO_DATABASE",
        "MODULE4D_TREND_SOURCE_NAMESPACES",
    ):
        monkeypatch.delenv(key, raising=False)
    with pytest.raises(ValueError, match="MODULE4D_MONGODB_URI is required"):
        ApiConfig.from_environment()

    monkeypatch.setenv("MODULE4D_MONGODB_URI", "mongodb://demo:secret@mongo.invalid:27017/d1a_test")
    monkeypatch.setenv("MODULE4D_MONGO_DATABASE", "d1a_test")
    monkeypatch.setenv("MODULE4D_TREND_SOURCE_NAMESPACES", NAMESPACE)
    configured = ApiConfig.from_environment()
    assert configured.mongo_database == "d1a_test"
    assert NAMESPACE in configured.trend_source_namespaces
    assert "secret" not in repr(configured)

    monkeypatch.setenv("MODULE4D_MONGO_DATABASE", "other_db")
    with pytest.raises(ValueError, match="must match"):
        ApiConfig.from_environment()


def _trend_document(*, digest_valid=True, retention_seconds=0):
    start = BASE_TIME - timedelta(hours=2)
    end = BASE_TIME - timedelta(hours=1)
    item = {
        "window_start": start,
        "window_end": end,
        "product_id": 991,
        "purchase_count_30m": 2,
        "purchase_count_120m": 4,
        "trend_score": 0.875,
        "trend_rank": 1,
    }
    document = _build_document(
        {"window_start": start, "window_end": end, "items": [item], "_row_count": 1},
        source_namespace=NAMESPACE,
        source_scope="historical_api_test",
        top_k=20,
        config_hash=hashlib.sha256(b"api trend config").hexdigest(),
        published_at=BASE_TIME,
        retention_seconds=retention_seconds,
    )
    if not digest_valid:
        document["content_sha256"] = "f" * 64
    return document


def test_health_is_liveness_only_and_does_not_touch_mongo():
    database = Database()
    with TestClient(_app(database)) as client:
        response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    assert database.names == []


def test_recommendation_response_has_stable_fields_and_utc_timestamps():
    _, pointer, rows, _ = make_snapshot(als_count=1, trend_count=1)
    with TestClient(_app(Database(pointer, rows))) as client:
        response = client.get("/api/v1/recommendations/71")

    assert response.status_code == 200
    body = response.json()
    assert set(body) == {
        "user_id", "serving_version", "status", "recommendation_count", "mode", "reason",
        "source", "ranking_config", "generated_at", "recommendations", "observed_at",
    }
    assert body["user_id"] == 71
    assert body["status"] == "ready"
    assert body["recommendation_count"] == 2
    assert body["recommendations"][0]["rank"] == 1
    assert any(row["from_als"] for row in body["recommendations"])
    assert any(row["from_trend"] for row in body["recommendations"])
    assert body["source"]["as_of_event_time"].endswith("Z")
    assert body["observed_at"].endswith("Z")


def test_valid_empty_hybrid_snapshot_is_http_200_with_reason():
    _, pointer, rows, _ = make_snapshot(als_count=0, trend_count=0, trend_available=False)
    with TestClient(_app(Database(pointer, rows))) as client:
        response = client.get("/api/v1/recommendations/71")
    assert response.status_code == 200
    assert response.json()["recommendations"] == []
    assert response.json()["recommendation_count"] == 0
    assert response.json()["reason"]
    assert response.json()["ranking_config"]["lambda_requested"] is None


@pytest.mark.parametrize(
    "user_id,expected_status",
    [("999", 404), ("0", 422), ("bad", 422), ("9223372036854775808", 422)],
)
def test_unknown_and_invalid_users_have_explicit_envelopes(user_id, expected_status):
    with TestClient(_app(Database())) as client:
        response = client.get(f"/api/v1/recommendations/{user_id}")
    assert response.status_code == expected_status
    assert set(response.json()) == {"error"}
    assert set(response.json()["error"]) == {"code", "message", "request_id"}
    assert response.json()["error"]["request_id"]


def test_nonready_hybrid_pointer_returns_distinct_unavailable_error():
    _, pointer, rows, _ = make_snapshot()
    pointer["status"] = "publishing"
    with TestClient(_app(Database(pointer, rows))) as client:
        response = client.get("/api/v1/recommendations/71")
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "snapshot_not_ready"


def test_mongo_failure_is_503_and_redacts_uri_and_password():
    database = Database()
    database.readiness.fail_find_one = True
    with TestClient(_app(database)) as client:
        response = client.get("/api/v1/recommendations/71")
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "service_unavailable"
    body = response.text
    for secret in ("demo-secret", "private-user", "private-host", "mongodb://"):
        assert secret not in body


def test_trend_route_preserves_c5_metadata_and_scores():
    database = Database(trend_documents=[_trend_document()])
    with TestClient(_app(database)) as client:
        response = client.get(
            "/api/v1/trends",
            params={"source_namespace": NAMESPACE, "as_of_event_time": BASE_TIME.isoformat()},
        )
    assert response.status_code == 200
    body = response.json()
    assert body["available"] is True
    assert body["source_scope"] == "historical_api_test"
    assert body["content_sha256"]
    assert body["row_count"] == 1
    assert body["items"] == [{"product_id": 991, "trend_score": 0.875, "trend_rank": 1}]
    assert database.trends.find_calls[0][0].keys() == {"_id"}


def test_missing_trend_is_a_successful_explicit_unavailable_result():
    with TestClient(_app(Database())) as client:
        response = client.get(
            "/api/v1/trends",
            params={"source_namespace": NAMESPACE, "as_of_event_time": BASE_TIME.isoformat()},
        )
    assert response.status_code == 200
    assert response.json()["available"] is False
    assert response.json()["status"] == "missing"
    assert response.json()["reason"] == "no_matching_snapshot"
    assert response.json()["items"] == []


@pytest.mark.parametrize(
    "document,extra_params,expected_status,expected_reason",
    [
        (_trend_document(retention_seconds=1), {}, "expired", "snapshot_expired"),
        (
            _trend_document(),
            {"max_event_age_seconds": 0},
            "stale",
            "snapshot_event_time_stale",
        ),
    ],
)
def test_expired_and_explicitly_stale_trends_keep_c5_status_and_reason(
    document, extra_params, expected_status, expected_reason
):
    params = {"source_namespace": NAMESPACE, "as_of_event_time": BASE_TIME.isoformat(), **extra_params}
    with TestClient(_app(Database(trend_documents=[document]))) as client:
        response = client.get("/api/v1/trends", params=params)
    assert response.status_code == 200
    assert response.json()["available"] is False
    assert response.json()["status"] == expected_status
    assert response.json()["reason"] == expected_reason
    assert response.json()["items"] == []


@pytest.mark.parametrize(
    "params,code",
    [
        ({"source_namespace": "not-configured", "as_of_event_time": BASE_TIME.isoformat()}, "invalid_namespace"),
        ({"source_namespace": NAMESPACE, "as_of_event_time": "2026-10-01T12:00:00"}, "invalid_filter"),
        ({"source_namespace": NAMESPACE, "as_of_event_time": "not-a-time"}, "invalid_filter"),
        (
            {"source_namespace": NAMESPACE, "as_of_event_time": BASE_TIME.isoformat(), "max_event_age_seconds": 31_536_001},
            "invalid_request",
        ),
    ],
)
def test_trend_inputs_are_strictly_validated(params, code):
    with TestClient(_app(Database())) as client:
        response = client.get("/api/v1/trends", params=params)
    assert response.status_code == 422
    assert response.json()["error"]["code"] == code


def test_corrupted_trend_bundle_is_502_not_missing():
    with TestClient(_app(Database(trend_documents=[_trend_document(digest_valid=False)]))) as client:
        response = client.get(
            "/api/v1/trends",
            params={"source_namespace": NAMESPACE, "as_of_event_time": BASE_TIME.isoformat()},
        )
    assert response.status_code == 502
    assert response.json()["error"]["code"] == "invalid_source_document"


def test_trend_database_failure_is_503():
    database = Database()
    database.trends.fail_find = True
    with TestClient(_app(database)) as client:
        response = client.get(
            "/api/v1/trends",
            params={"source_namespace": NAMESPACE, "as_of_event_time": BASE_TIME.isoformat()},
        )
    assert response.status_code == 503


def test_openapi_is_get_only_for_d1a_and_has_no_wildcard_cors():
    with TestClient(_app(Database())) as client:
        schema = client.get("/openapi.json").json()
        health = client.get("/health")
    assert set(schema["paths"]) == {"/health", "/api/v1/recommendations/{user_id}", "/api/v1/trends"}
    assert all(set(item) == {"get"} for item in schema["paths"].values())
    assert "Access-Control-Allow-Origin" not in health.headers


def test_lifespan_closes_owned_mongo_client_once():
    class OwnedClient:
        def __init__(self):
            self.closed = 0
            self.database = Database()

        def __getitem__(self, name):
            assert name == "d1a_test"
            return self.database

        def close(self):
            self.closed += 1

    owned = OwnedClient()
    app = create_app(_config(), client_factory=lambda *args, **kwargs: owned)
    with TestClient(app) as client:
        assert client.get("/health").status_code == 200
        assert owned.closed == 0
    assert owned.closed == 1


def test_api_import_and_openapi_do_not_start_spark_kafka_or_hdfs():
    database = Database()
    app = _app(database)
    with TestClient(app) as client:
        assert client.get("/openapi.json").status_code == 200
    assert database.names == []
    assert SparkContext._active_spark_context is None
    assert "src.module3.cli" not in sys.modules
    assert "src.module3.streaming" not in sys.modules
