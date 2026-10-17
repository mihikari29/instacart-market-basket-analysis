"""Deterministic tests for the bounded Streamlit-to-FastAPI HTTP client."""

from __future__ import annotations

from datetime import datetime, timezone
import json

import pytest
import requests

from src.module4.dashboard.ui.api_client import (
    ApiClient,
    ApiClientError,
    validate_as_of_event_time,
    validate_base_url,
    validate_trend_namespace,
    validate_user_id,
)


class FakeResponse:
    def __init__(self, payload=None, *, status=200, content_type="application/json", raw=None, headers=None):
        self.status_code = status
        self.headers = {"Content-Type": content_type, **(headers or {})}
        self.raw = raw if raw is not None else json.dumps(payload).encode("utf-8")
        self.closed = False
        self.chunks = [self.raw[index : index + 7] for index in range(0, len(self.raw), 7)]

    def iter_content(self, chunk_size):
        assert chunk_size == 64 * 1024
        yield from self.chunks

    def close(self):
        self.closed = True


class FakeSession:
    def __init__(self, response=None, error=None):
        self.response = response
        self.error = error
        self.calls = []
        self.trust_env = True

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if self.error:
            raise self.error
        return self.response


def health_payload():
    return {"status": "ok"}


def test_base_url_accepts_localhost_loopback_and_explicit_docker_service():
    assert validate_base_url("http://localhost:8000") == "http://localhost:8000"
    assert validate_base_url("http://127.0.0.1:8000/") == "http://127.0.0.1:8000"
    assert validate_base_url("http://[::1]:8000") == "http://[::1]:8000"
    assert validate_base_url("http://module4d-api:8000") == "http://module4d-api:8000"


@pytest.mark.parametrize(
    "url",
    [
        "https://api.example.com",
        "http://module4d-api.evil:8000",
        "http://user:password@127.0.0.1:8000",
        "ftp://127.0.0.1:8000",
        "http://127.0.0.1:8000/api/v1",
        "http://127.0.0.1:8000/?x=1",
        "http://127.0.0.1:8000/#fragment",
        "http://127.0.0.1:99999",
        " http://127.0.0.1:8000",
        "",
    ],
)
def test_invalid_api_base_urls_are_rejected(url):
    with pytest.raises(ApiClientError) as caught:
        validate_base_url(url)
    assert caught.value.code == "invalid_api_configuration"
    assert "password" not in caught.value.message


def test_client_disables_environment_proxy_and_sends_fixed_timeout_and_no_redirects():
    session = FakeSession(FakeResponse(health_payload()))
    client = ApiClient("http://127.0.0.1:8000", session=session)
    assert client.get_health() == health_payload()
    assert session.trust_env is False
    url, kwargs = session.calls[0]
    assert url == "http://127.0.0.1:8000/health"
    assert kwargs["timeout"] == (2.0, 6.0)
    assert kwargs["allow_redirects"] is False
    assert kwargs["stream"] is True
    assert kwargs["headers"] == {"Accept": "application/json"}
    assert session.response.closed


def test_timeout_and_connection_failures_are_sanitized():
    for exception, code in (
        (requests.Timeout("timeout with http://private"), "api_timeout"),
        (requests.ConnectionError("private-host:27017 secret"), "api_unavailable"),
    ):
        client = ApiClient("http://127.0.0.1:8000", session=FakeSession(error=exception))
        with pytest.raises(ApiClientError) as caught:
            client.get_health()
        assert caught.value.code == code
        assert caught.value.status_code is None
        assert "private" not in caught.value.message
        assert "27017" not in caught.value.message


def test_non_json_error_response_is_not_parsed_or_echoed():
    response = FakeResponse(raw=b"secret database host", status=503, content_type="text/html")
    with pytest.raises(ApiClientError) as caught:
        ApiClient("http://localhost:8000", session=FakeSession(response)).get_health()
    assert caught.value.status_code == 503
    assert caught.value.code == "non_json_response"
    assert "secret" not in caught.value.message
    assert response.closed


def test_oversized_response_is_rejected_from_header_and_streamed_body():
    header_response = FakeResponse(health_payload(), headers={"Content-Length": "20"})
    with pytest.raises(ApiClientError) as header_error:
        ApiClient("http://localhost:8000", session=FakeSession(header_response), max_response_bytes=10).get_health()
    assert header_error.value.code == "response_too_large"
    assert header_response.closed

    stream_response = FakeResponse(raw=b"{" + b" " * 40 + b"}")
    with pytest.raises(ApiClientError) as stream_error:
        ApiClient("http://localhost:8000", session=FakeSession(stream_response), max_response_bytes=16).get_health()
    assert stream_error.value.code == "response_too_large"


def test_invalid_json_and_invalid_response_shape_are_rejected():
    bad_json = FakeResponse(raw=b"not-json")
    with pytest.raises(ApiClientError) as json_error:
        ApiClient("http://localhost:8000", session=FakeSession(bad_json)).get_health()
    assert json_error.value.code == "invalid_json"

    bad_shape = FakeResponse({"not_status": "invalid"})
    with pytest.raises(ApiClientError) as shape_error:
        ApiClient("http://localhost:8000", session=FakeSession(bad_shape)).get_health()
    assert shape_error.value.code == "invalid_response"
    assert shape_error.value.status_code == 200


def test_backend_error_code_http_status_and_request_id_are_preserved():
    payload = {
        "error": {
            "code": "snapshot_changed",
            "message": "The snapshot changed during the read; retry the request.",
            "request_id": "req-123",
        }
    }
    response = FakeResponse(payload, status=409)
    with pytest.raises(ApiClientError) as caught:
        ApiClient("http://localhost:8000", session=FakeSession(response)).get_health()
    assert caught.value.status_code == 409
    assert caught.value.code == "snapshot_changed"
    assert caught.value.request_id == "req-123"
    assert caught.value.message == payload["error"]["message"]


def test_redirect_is_rejected_without_following_location():
    response = FakeResponse({}, status=302, headers={"Location": "https://attacker.example"})
    with pytest.raises(ApiClientError) as caught:
        ApiClient("http://localhost:8000", session=FakeSession(response)).get_health()
    assert caught.value.code == "unexpected_redirect"
    assert caught.value.status_code == 302


def test_recommendation_path_accepts_only_positive_signed_64_bit_user_ids():
    session = FakeSession(FakeResponse({"wrong": "shape"}))
    client = ApiClient("http://localhost:8000", session=session)
    for value in (1, "71", 9_223_372_036_854_775_807):
        assert validate_user_id(value) == int(value)
    for value in (0, -1, True, 1.0, "", "+1", "1.2", "9223372036854775808"):
        with pytest.raises(ApiClientError):
            validate_user_id(value)
    with pytest.raises(ApiClientError):
        client.get_recommendations("not-an-id")
    assert session.calls == []


def test_als_client_sends_explicit_version_and_validates_bounded_ranked_response():
    payload = {
        "user_id": 71,
        "model_version": "als-v42",
        "status": "available",
        "reason": None,
        "recommendation_count": 1,
        "requested_limit": 1,
        "has_more": True,
        "source": {"run_id": "run-42", "model_version": "als-v42", "graph_version": "graph-v42"},
        "recommendations": [
            {"product_id": 501, "product_name": "Banana", "als_rank": 1, "als_score": 0.95}
        ],
        "observed_at": "2026-10-14T10:00:00Z",
    }
    session = FakeSession(FakeResponse(payload))
    client = ApiClient("http://localhost:8000", session=session)
    assert client.get_als_recommendations("71", "als-v42", limit=1) == payload
    url, kwargs = session.calls[0]
    assert url == "http://localhost:8000/api/v1/als/recommendations/71"
    assert kwargs["params"] == {"limit": 1, "model_version": "als-v42"}

    for version in (" bad", "bad/route", "..", "x" * 81):
        with pytest.raises(ApiClientError) as caught:
            client.get_als_recommendations(71, version)
        assert caught.value.code == "invalid_model_version"


def test_trend_namespace_must_be_a_configured_scalar_without_whitespace_or_nul():
    assert validate_trend_namespace("module3/replay", ("module3/replay",)) == "module3/replay"
    for namespace in ("", " module3/replay", "module3/replay ", "module3\x00/replay", "unlisted"):
        with pytest.raises(ApiClientError) as caught:
            validate_trend_namespace(namespace, ("module3/replay",))
        assert caught.value.code == "invalid_namespace"


def test_as_of_event_time_requires_timezone_and_is_normalized_to_utc():
    assert validate_as_of_event_time("2026-10-14T12:00:00+02:00") == "2026-10-14T10:00:00Z"
    assert validate_as_of_event_time(datetime(2026, 10, 14, 10, 0, tzinfo=timezone.utc)) == "2026-10-14T10:00:00Z"
    for value in ("not-a-time", "2026-10-14T10:00:00", datetime(2026, 10, 14, 10, 0)):
        with pytest.raises(ApiClientError) as caught:
            validate_as_of_event_time(value)
        assert caught.value.code == "invalid_filter"


def test_trend_request_sends_configured_namespace_cutoff_and_optional_age():
    body = {
        "available": False,
        "status": "missing",
        "reason": "no_matching_snapshot",
        "source_namespace": "module3/replay",
        "source_scope": None,
        "window_start": None,
        "window_end": None,
        "published_at": None,
        "expires_at": None,
        "schema_version": None,
        "config_hash": None,
        "content_sha256": None,
        "row_count": 0,
        "items": [],
        "observed_at": "2026-10-14T10:00:00Z",
    }
    session = FakeSession(FakeResponse(body))
    client = ApiClient("http://localhost:8000", session=session)
    client.get_trends("module3/replay", "2026-10-14T12:00:00+02:00", 3600)
    url, kwargs = session.calls[0]
    assert url == "http://localhost:8000/api/v1/trends"
    assert kwargs["params"] == {
        "source_namespace": "module3/replay",
        "as_of_event_time": "2026-10-14T10:00:00Z",
        "max_event_age_seconds": 3600,
    }
    assert session.response.closed


def test_batch_modes_and_limits_are_fixed_and_bounded():
    body = {
        "order": "reorder_rate",
        "items": [],
        "source": {
            "collection": "batch_product_metrics",
            "scope": "prior-only item purchases",
            "run_id": None,
            "last_batch_time": None,
            "atomic_with_other_collections": False,
        },
        "metric_definition": {"reorder_rate": "reorders / prior purchases"},
        "observed_at": "2026-10-14T10:00:00Z",
    }
    session = FakeSession(FakeResponse(body))
    client = ApiClient("http://localhost:8000", session=session)
    assert client.get_batch_products("reorder_rate", 50)["items"] == []
    assert session.calls[0][1]["params"] == {"order": "reorder_rate", "limit": 50}
    for order, limit in (("purchase_count desc", 20), ("purchase_count", 51), ("reorder_rate", 0)):
        with pytest.raises(ApiClientError):
            client.get_batch_products(order, limit)
    assert len(session.calls) == 1


def test_empty_source_is_preserved_as_a_valid_empty_response():
    body = {
        "order": "purchase_count",
        "items": [],
        "source": {
            "collection": "batch_product_metrics",
            "scope": "prior-only item purchases",
            "run_id": None,
            "last_batch_time": None,
            "atomic_with_other_collections": False,
        },
        "metric_definition": {"purchase_count": "prior purchases"},
        "observed_at": "2026-10-14T10:00:00Z",
    }
    assert ApiClient("http://localhost:8000", session=FakeSession(FakeResponse(body))).get_batch_products()["items"] == []


def test_invalid_nested_batch_rows_are_rejected_instead_of_reaching_a_chart():
    body = {
        "order": "purchase_count",
        "items": [
            {
                "product_id": "not-an-integer",
                "purchase_count": 100,
                "unique_users": 40,
                "reorder_count": 60,
                "reorder_rate": 0.6,
                "run_id": "module2-run",
                "last_batch_time": "2026-10-14T10:00:00Z",
            }
        ],
        "source": {
            "collection": "batch_product_metrics",
            "scope": "prior-only item purchases",
            "run_id": "module2-run",
            "last_batch_time": "2026-10-14T10:00:00Z",
            "atomic_with_other_collections": False,
        },
        "metric_definition": {"purchase_count": "prior-only item purchases"},
        "observed_at": "2026-10-14T10:00:00Z",
    }
    with pytest.raises(ApiClientError) as caught:
        ApiClient("http://localhost:8000", session=FakeSession(FakeResponse(body))).get_batch_products()
    assert caught.value.code == "invalid_response"
    assert caught.value.status_code == 200
