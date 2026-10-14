"""AppTest and session-state coverage for all D2A dashboard pages."""

from __future__ import annotations

from pathlib import Path

from streamlit.testing.v1 import AppTest

from src.module4.dashboard.ui.api_client import ApiClientError
from src.module4.dashboard.ui.state import fetch


ROOT = Path(__file__).resolve().parents[1]
APP_FILE = ROOT / "src/module4/dashboard/ui/app.py"
NAMESPACE = "module3/replay"


def _time(value="2026-10-14T10:00:00Z"):
    return value


def _recommendation(user_id):
    return {
        "user_id": user_id,
        "serving_version": f"hybrid-run-user-{user_id}",
        "status": "ready",
        "recommendation_count": 1,
        "mode": "hybrid",
        "reason": None,
        "source": {
            "source_run_id": "source-run-42",
            "model_version": "als-2026-10",
            "graph_version": "graph-2026-10",
            "trend_source_namespace": NAMESPACE,
            "trend_source_scope": "historical_replay",
            "trend_window_start": _time("2026-10-14T09:00:00Z"),
            "trend_window_end": _time("2026-10-14T09:30:00Z"),
            "trend_content_sha256": "a" * 64,
            "as_of_event_time": _time("2026-10-14T10:00:00Z"),
            "input_content_sha256": "b" * 64,
            "pointer_updated_at": _time("2026-10-14T10:01:00Z"),
        },
        "ranking_config": {
            "algorithm_version": "hybrid-v1",
            "lambda_requested": 0.5,
            "lambda_effective": 0.5,
            "als_top_n": 20,
            "trend_top_m": 20,
            "output_top_k": 20,
            "epsilon": 1e-9,
            "ranking_config_sha256": "c" * 64,
        },
        "generated_at": _time("2026-10-14T10:00:30Z"),
        "recommendations": [
            {
                "product_id": 24852,
                "product_name": "Banana",
                "rank": 1,
                "from_als": True,
                "from_trend": True,
                "als_score": 0.84,
                "trend_score": 0.75,
                "normalized_als_score": 1.0,
                "normalized_trend_score": 0.9,
                "final_score": 0.95,
            }
        ],
        "observed_at": _time("2026-10-14T10:02:00Z"),
    }


def _product_response():
    return {
        "order": "purchase_count",
        "items": [
            {
                "product_id": 24852,
                "purchase_count": 100,
                "unique_users": 40,
                "reorder_count": 60,
                "reorder_rate": 0.6,
                "run_id": "module2-run-42",
                "last_batch_time": _time("2026-10-14T08:00:00Z"),
            }
        ],
        "source": {
            "collection": "batch_product_metrics",
            "scope": "prior-only item purchases",
            "run_id": "module2-run-42",
            "last_batch_time": _time("2026-10-14T08:00:00Z"),
            "atomic_with_other_collections": False,
        },
        "metric_definition": {"purchase_count": "prior-only order-product rows"},
        "observed_at": _time("2026-10-14T08:01:00Z"),
    }


def _department_response():
    return {
        "items": [
            {
                "department_id": 1,
                "purchase_count": 100,
                "unique_users": 40,
                "reorder_rate": 0.4,
                "run_id": "department-run-42",
                "last_batch_time": _time("2026-10-14T08:05:00Z"),
            }
        ],
        "source": {
            "collection": "department_metrics",
            "scope": "prior-only item purchases",
            "run_id": "department-run-42",
            "last_batch_time": _time("2026-10-14T08:05:00Z"),
            "atomic_with_other_collections": False,
        },
        "metric_definition": {"purchase_count": "prior-only order-product rows"},
        "observed_at": _time("2026-10-14T08:06:00Z"),
    }


def _trend_response(available=True):
    return {
        "available": available,
        "status": "available" if available else "missing",
        "reason": None if available else "no_matching_snapshot",
        "source_namespace": NAMESPACE,
        "source_scope": "historical_replay",
        "window_start": _time("2026-10-14T09:00:00Z") if available else None,
        "window_end": _time("2026-10-14T09:30:00Z") if available else None,
        "published_at": _time("2026-10-14T09:31:00Z") if available else None,
        "expires_at": _time("2026-10-21T09:31:00Z") if available else None,
        "schema_version": 1 if available else None,
        "config_hash": "d" * 64 if available else None,
        "content_sha256": "e" * 64 if available else None,
        "row_count": 1 if available else 0,
        "items": [{"product_id": 24852, "trend_score": 0.75, "trend_rank": 1}] if available else [],
        "observed_at": _time("2026-10-14T10:02:00Z"),
    }


def _orders_response():
    return {
        "scope": "all orders",
        "measure": "order count, not item-level prior purchases",
        "total_orders": 70,
        "orders_by_day_of_week": [{"order_dow": index, "order_count": 10} for index in range(7)],
        "orders_by_hour": [
            {"order_hour_of_day": index, "order_count": (70 if index == 0 else 0)} for index in range(24)
        ],
        "provenance": {
            "artifact_key": "analytics",
            "source_label": "Module 2 archived full-data analytics",
            "classification": "full_public_dataset",
            "archived": True,
            "sha256": "f" * 64,
        },
        "observed_at": _time("2026-10-14T10:02:00Z"),
    }


def _evidence_response():
    return {
        "items": [
            {
                "evidence_id": "module4c-c8b-streaming-demo",
                "title": "Module 4C C8B bounded streaming demonstration receipt",
                "kind": "synthetic_streaming_demo",
                "classification": "bounded_synthetic_kafka_spark_demonstration",
                "source_label": "Module 4C archived C8B synthetic Kafka/Spark demonstration",
                "available": True,
                "status": "available",
                "sha256": "f" * 64,
            }
        ],
        "observed_at": _time("2026-10-14T10:02:00Z"),
    }


class FakeApi:
    base_url = "http://127.0.0.1:8000"

    def __init__(self):
        self.calls = []
        self.trend_available = True
        self.recommendation_errors = []
        self.trend_error = None
        self.product_error = None
        self.empty_recommendations = False

    def get_health(self):
        self.calls.append(("health", ()))
        return {"status": "ok"}

    def get_recommendations(self, user_id):
        self.calls.append(("recommendations", (user_id,)))
        if self.recommendation_errors:
            raise self.recommendation_errors.pop(0)
        payload = _recommendation(user_id)
        if self.empty_recommendations:
            payload["recommendations"] = []
            payload["recommendation_count"] = 0
            payload["mode"] = "trend_only"
            payload["reason"] = "no_candidates"
        return payload

    def get_trends(self, source_namespace, as_of_event_time, max_event_age_seconds=None):
        self.calls.append(("trends", (source_namespace, as_of_event_time, max_event_age_seconds)))
        if self.trend_error:
            raise self.trend_error
        return _trend_response(self.trend_available)

    def get_batch_products(self, order="purchase_count", limit=20):
        self.calls.append(("products", (order, limit)))
        if self.product_error:
            raise self.product_error
        response = _product_response()
        response["order"] = order
        return response

    def get_batch_departments(self):
        self.calls.append(("departments", ()))
        return _department_response()

    def get_evidence_registry(self):
        self.calls.append(("evidence", ()))
        return _evidence_response()

    def get_orders_distribution(self):
        self.calls.append(("orders", ()))
        return _orders_response()


def _app(monkeypatch, api=None, page="Overview"):
    monkeypatch.setenv("MODULE4D_TREND_SOURCE_NAMESPACES", NAMESPACE)
    app = AppTest.from_file(str(APP_FILE), default_timeout=10)
    app.session_state["_module4d_api_client"] = api or FakeApi()
    app.session_state["ui_page"] = page
    return app.run(), api or app.session_state["_module4d_api_client"]


def _go_to(app, page):
    app.radio(key="ui_page").set_value(page)
    return app.run()


def _submit(app, submit_key, input_key, value):
    if input_key == "analytics_limit":
        app.number_input(key=input_key).set_value(int(value))
    else:
        app.text_input(key=input_key).set_value(value)
    app.button(key=submit_key).click()
    return app.run()


def _texts(elements):
    return "\n".join(str(element.value) for element in elements)


def test_overview_imports_and_renders_with_independent_source_states(monkeypatch):
    app, api = _app(monkeypatch)
    assert not app.exception
    rendered = _texts(app.title) + _texts(app.caption) + _texts(app.markdown) + _texts(app.success)
    assert "Overview" in rendered
    assert "process liveness only" in rendered
    assert "Finalized trends" in rendered
    assert "Archived reports are historical evidence" in rendered
    assert {call[0] for call in api.calls} == {"health", "products", "departments", "trends", "evidence"}


def test_analytics_page_loads_fixed_routes_and_labels_real_sources(monkeypatch):
    app, api = _app(monkeypatch, page="Realtime & Batch Analytics")
    assert not app.exception
    app = _submit(app, "analytics_submit", "analytics_limit", "20")
    assert not app.exception
    rendered = _texts(app.title) + _texts(app.subheader) + _texts(app.caption) + _texts(app.markdown)
    assert "Finalized trend ranking" in rendered
    assert "historical_replay" in rendered
    assert "prior-only item purchases" in rendered
    assert "all orders" in rendered or "All orders" in rendered
    assert "source day-of-week bin" in rendered
    assert {call[0] for call in api.calls} >= {"trends", "products", "departments", "orders"}
    first_call_count = len(api.calls)
    app = app.run()
    assert len(api.calls) == first_call_count
    assert "Last submitted filters" in _texts(app.caption)


def test_trend_missing_state_is_not_rendered_as_a_successful_empty_chart(monkeypatch):
    api = FakeApi()
    api.trend_available = False
    app, _ = _app(monkeypatch, api, page="Realtime & Batch Analytics")
    app = _submit(app, "analytics_submit", "analytics_limit", "20")
    rendered = _texts(app.warning) + _texts(app.info)
    assert "Trend source unavailable: missing" in rendered
    assert "no_matching_snapshot" in rendered


def test_missing_trend_index_is_shown_as_an_api_error(monkeypatch):
    api = FakeApi()
    api.trend_error = ApiClientError(
        "The finalized trend query index is not provisioned.", status_code=503, code="trend_index_unavailable", request_id="req-trend"
    )
    app, _ = _app(monkeypatch, api, page="Realtime & Batch Analytics")
    app = _submit(app, "analytics_submit", "analytics_limit", "20")
    rendered = _texts(app.error)
    assert "trend_index_unavailable" in rendered
    assert "HTTP 503" in rendered
    assert "req-trend" in rendered


def test_missing_batch_index_is_shown_as_an_api_error_not_an_empty_table(monkeypatch):
    api = FakeApi()
    api.product_error = ApiClientError(
        "A required product ranking index is not provisioned.", status_code=503, code="batch_index_unavailable"
    )
    app, _ = _app(monkeypatch, api, page="Realtime & Batch Analytics")
    app = _submit(app, "analytics_submit", "analytics_limit", "20")
    rendered = _texts(app.error)
    assert "batch_index_unavailable" in rendered
    assert "HTTP 503" in rendered


def test_archived_evidence_and_distinct_prior_only_all_order_scopes_are_visible(monkeypatch):
    app, _ = _app(monkeypatch)
    rendered = _texts(app.caption) + _texts(app.dataframe) + _texts(app.markdown)
    assert "archived reports are historical evidence" in rendered.lower()
    assert "C8B is not live Kafka ingestion" in rendered
    analytics, _ = _app(monkeypatch, page="Realtime & Batch Analytics")
    analytics = _submit(analytics, "analytics_submit", "analytics_limit", "20")
    text = _texts(analytics.caption) + _texts(analytics.subheader)
    assert "prior-only" in text
    assert "all orders" in text or "All orders" in text


def test_no_product_or_department_names_are_invented_and_trends_remain_single_window(monkeypatch):
    app, api = _app(monkeypatch, page="Realtime & Batch Analytics")
    app = _submit(app, "analytics_submit", "analytics_limit", "20")
    assert not app.exception
    page_source = (ROOT / "src/module4/dashboard/ui/pages/analytics.py").read_text()
    assert "product_name" not in page_source
    assert "department_name" not in page_source
    assert "trend_history" not in page_source
    trend_requests = [call for call in api.calls if call[0] == "trends"]
    assert len(trend_requests) == 1
    assert len(_trend_response()["items"]) == 1


def test_recommendation_page_shows_verified_hybrid_rows_and_separate_score_components(monkeypatch):
    app, _ = _app(monkeypatch, page="Hybrid Recommendations")
    app = _submit(app, "recommendation_submit", "recommendation_user_id", "71")
    assert not app.exception
    rendered = _texts(app.title) + _texts(app.metric) + _texts(app.info) + _texts(app.caption)
    assert "Serving version" in rendered
    assert "hybrid-run-user-71" in rendered
    assert "Direct ALS-versus-Hybrid comparison is unavailable" in rendered
    assert "persisted final_score (0–1)" in rendered
    # Dataframe headers carry each score on its own field; these labels are not collapsed into a chart.
    recommendation_page = (ROOT / "src/module4/dashboard/ui/pages/recommendations.py").read_text()
    for label in ("Raw ALS score", "Raw trend score", "Normalized ALS score", "Normalized trend score", "Final score"):
        assert label in recommendation_page


def test_unknown_recommendation_snapshot_has_not_found_semantics(monkeypatch):
    api = FakeApi()
    api.recommendation_errors = [
        ApiClientError("No published snapshot was found.", status_code=404, code="recommendation_snapshot_not_found")
    ]
    app, _ = _app(monkeypatch, api, page="Hybrid Recommendations")
    app = _submit(app, "recommendation_submit", "recommendation_user_id", "71")
    rendered = _texts(app.error)
    assert "No published recommendation snapshot was found" in rendered
    assert "HTTP 404" in rendered


def test_changed_recommendation_pointer_offers_manual_retry(monkeypatch):
    api = FakeApi()
    api.recommendation_errors = [
        ApiClientError("The snapshot changed during the read; retry the request.", status_code=409, code="snapshot_changed", request_id="req-race")
    ]
    app, api = _app(monkeypatch, api, page="Hybrid Recommendations")
    app = _submit(app, "recommendation_submit", "recommendation_user_id", "71")
    assert "snapshot_changed" in _texts(app.error)
    assert any(button.label == "Retry this read" for button in app.button)
    app.button(key="retry_recommendation_read").click()
    app = app.run()
    assert not app.exception
    assert api.calls.count(("recommendations", (71,))) == 2
    assert any(metric.label == "Serving version" for metric in app.metric)


def test_ready_zero_recommendation_snapshot_is_a_distinct_empty_success(monkeypatch):
    api = FakeApi()
    api.empty_recommendations = True
    app, _ = _app(monkeypatch, api, page="Hybrid Recommendations")
    app = _submit(app, "recommendation_submit", "recommendation_user_id", "71")
    assert "ready published snapshot with zero recommendations" in _texts(app.info)


def test_user_recommendation_cache_is_keyed_by_user_id_and_never_leaks(monkeypatch):
    api = FakeApi()
    state = {}
    first, error, cached = fetch(state, api, "get_recommendations", 71)
    second, second_error, second_cached = fetch(state, api, "get_recommendations", 72)
    assert first["user_id"] == 71
    assert second["user_id"] == 72
    assert error is second_error is None
    assert not cached and not second_cached
    assert fetch(state, api, "get_recommendations", 71)[0]["user_id"] == 71
    assert api.calls.count(("recommendations", (71,))) == 1
    assert api.calls.count(("recommendations", (72,))) == 1


def test_failed_responses_are_not_cached_and_success_refresh_bypasses_cache(monkeypatch):
    api = FakeApi()
    api.recommendation_errors = [ApiClientError("missing", status_code=404, code="recommendation_snapshot_not_found")]
    state = {}
    payload, error, _ = fetch(state, api, "get_recommendations", 91)
    assert payload is None and error.code == "recommendation_snapshot_not_found"
    payload, error, cached = fetch(state, api, "get_recommendations", 91)
    assert payload["user_id"] == 91 and error is None and not cached
    assert fetch(state, api, "get_recommendations", 91)[2]
    payload, error, cached = fetch(state, api, "get_recommendations", 91, refresh=True)
    assert payload["user_id"] == 91 and error is None and not cached
    assert api.calls.count(("recommendations", (91,))) == 3


def test_session_cache_is_bounded_to_32_successful_request_keys():
    api = FakeApi()
    state = {}
    for user_id in range(1, 35):
        fetch(state, api, "get_recommendations", user_id)
    cache = state["_module4d_response_cache"]
    assert len(cache) == 32
    assert all('"method":"get_recommendations"' in key for key in cache)


def test_configured_namespace_is_shown_as_fixed_choice_and_no_auto_polling(monkeypatch):
    app, api = _app(monkeypatch, page="Realtime & Batch Analytics")
    assert app.selectbox(key="analytics_namespace").value == NAMESPACE
    assert api.calls == []
    assert "No trend history or automatic polling" in _texts(app.info)


def test_frontend_source_has_no_database_credentials_publishers_or_compute_startup():
    source_root = ROOT / "src/module4/dashboard/ui"
    source = "\n".join(path.read_text() for path in source_root.rglob("*.py"))
    forbidden = (
        "pymongo", "MongoClient", "MONGODB_URI", "mongodb://", "password@", "KafkaConsumer",
        "SparkSession", "pyspark", "HDFS", "serve_hybrid_user", "publish_hybrid", "publish_recommendations",
        "subprocess.Popen", "spark-submit",
    )
    assert not [token for token in forbidden if token in source]


def test_frontend_dependency_set_excludes_backend_compute_and_database_packages():
    requirements = (ROOT / "deploy/module4/requirements/ui.txt").read_text().lower()
    assert "streamlit==1.65.0" in requirements
    assert "requests==2.34.2" in requirements
    assert not any(token in requirements for token in ("pymongo", "pyspark", "kafka", "hdfs", "fastapi"))
