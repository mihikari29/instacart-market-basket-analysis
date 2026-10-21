"""Network-free HTTP tests for D1B batch and curated evidence routes."""

from __future__ import annotations

import json

from fastapi.testclient import TestClient

from src.module4.dashboard.api import create_app
from src.module4.dashboard.config import ApiConfig
from src.module4.dashboard.batch_readers import PRODUCT_INDEXES
from src.module4.dashboard.evidence import EVIDENCE_SPECS
from tests.test_dashboard_batch_readers import BatchDatabase, department, product
from tests.test_dashboard_readers import Database
from tests.test_dashboard_api import _config


def app_for(database, *, root=None):
    config = _config()
    if root is not None:
        config = ApiConfig(
            config.mongodb_uri,
            config.mongo_database,
            config.trend_source_namespaces,
            application_root=root,
        )
    return create_app(config, database=database)


def test_product_endpoint_defaults_to_twenty_and_returns_prior_only_metadata():
    database = BatchDatabase([product(i, purchase=100 - i, reorders=40) for i in range(1, 26)])
    with TestClient(app_for(database)) as client:
        response = client.get("/api/v1/batch/products")

    assert response.status_code == 200
    body = response.json()
    assert len(body["items"]) == 20
    assert body["order"] == "purchase_count"
    assert body["source"]["scope"] == "prior-only item purchases"
    assert body["source"]["atomic_with_other_collections"] is False
    assert body["source"]["run_id"] == "module2-run"
    assert body["items"][0]["product_id"] == 1
    assert "product_name" not in json.dumps(body)
    assert "must not escape" not in json.dumps(body)
    assert body["observed_at"].endswith("Z") or body["observed_at"].endswith("+00:00")


def test_product_endpoint_supports_reorder_rate_and_hard_maximum():
    database = BatchDatabase([product(i, purchase=60, reorders=i) for i in range(1, 55)])
    with TestClient(app_for(database)) as client:
        response = client.get("/api/v1/batch/products", params={"order": "reorder_rate", "limit": 50})
        too_many = client.get("/api/v1/batch/products", params={"limit": 51})
        bad_order = client.get("/api/v1/batch/products", params={"order": "purchase_count desc"})

    assert response.status_code == 200
    assert len(response.json()["items"]) == 50
    assert response.json()["order"] == "reorder_rate"
    assert response.json()["items"][0]["product_id"] == 54
    assert too_many.status_code == 422
    assert bad_order.status_code == 422
    assert "mongodb://" not in bad_order.text


def test_missing_product_index_is_explicit_and_does_not_read_rows():
    missing_purchase_name = PRODUCT_INDEXES["purchase_count"][0]
    database = BatchDatabase(
        [product()],
        indexes={"_id_": {"name": "_id_", "key": {"_id": 1}}, missing_purchase_name: {
            "name": missing_purchase_name,
            "key": dict(PRODUCT_INDEXES["purchase_count"][1]),
            "partialFilterExpression": {"purchase_count": {"$gt": 0}},
        }},
    )
    with TestClient(app_for(database)) as client:
        response = client.get("/api/v1/batch/products")

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "batch_index_unavailable"
    assert "private" not in response.text
    assert database.products.find_calls == []


def test_product_corruption_maps_to_sanitized_502():
    database = BatchDatabase([product(1, rate=0.8)])
    with TestClient(app_for(database)) as client:
        response = client.get("/api/v1/batch/products")
    assert response.status_code == 502
    assert response.json()["error"]["code"] == "invalid_source_document"
    assert "mongodb://" not in response.text


def test_department_endpoint_returns_prior_only_rows_and_stable_ties():
    database = BatchDatabase(departments=[department(3), department(1), department(2, purchase=200)])
    with TestClient(app_for(database)) as client:
        response = client.get("/api/v1/batch/departments")

    assert response.status_code == 200
    body = response.json()
    assert [row["department_id"] for row in body["items"]] == [2, 1, 3]
    assert body["source"]["scope"] == "prior-only item purchases"
    assert body["items"][0]["run_id"] == "module2-run"
    assert "department_name" not in json.dumps(body)


def test_department_overflow_returns_502_instead_of_partial_ranking():
    database = BatchDatabase(departments=[department(i) for i in range(1, 23)])
    with TestClient(app_for(database)) as client:
        response = client.get("/api/v1/batch/departments")
    assert response.status_code == 502
    assert response.json()["error"]["code"] == "invalid_source_document"


def test_empty_batch_sources_return_empty_success_without_fabricated_run():
    with TestClient(app_for(BatchDatabase())) as client:
        products_response = client.get("/api/v1/batch/products")
        departments_response = client.get("/api/v1/batch/departments")
    assert products_response.status_code == departments_response.status_code == 200
    assert products_response.json()["items"] == []
    assert departments_response.json()["items"] == []
    assert products_response.json()["source"]["run_id"] is None
    assert departments_response.json()["source"]["last_batch_time"] is None


def test_batch_endpoints_preserve_separate_collection_generations():
    database = BatchDatabase(
        [product(run="product-generation")],
        [department(run="department-generation")],
    )
    with TestClient(app_for(database)) as client:
        products_response = client.get("/api/v1/batch/products")
        departments_response = client.get("/api/v1/batch/departments")
    assert products_response.json()["source"]["run_id"] == "product-generation"
    assert departments_response.json()["source"]["run_id"] == "department-generation"


def test_mongo_read_failures_map_to_sanitized_service_errors():
    database = BatchDatabase([product()])
    database.products.fail_find = True
    with TestClient(app_for(database)) as client:
        response = client.get("/api/v1/batch/products")
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "service_unavailable"
    assert "private connection details" not in response.text


def test_evidence_registry_and_detail_routes_are_curated_and_available():
    with TestClient(app_for(Database())) as client:
        registry = client.get("/api/v1/evidence")
        analytics = client.get("/api/v1/evidence/module2-full-analytics")
        benchmark = client.get("/api/v1/evidence/module2-full-hardening-benchmarks")
        c8a = client.get("/api/v1/evidence/module4c-c8a-responsiveness")
        c8b = client.get("/api/v1/evidence/module4c-c8b-streaming-demo")

    assert registry.status_code == analytics.status_code == benchmark.status_code == c8a.status_code == c8b.status_code == 200
    assert [item["evidence_id"] for item in registry.json()["items"]] == list(EVIDENCE_SPECS)
    assert all(item["available"] for item in registry.json()["items"])
    assert analytics.json()["data"]["scope"]["purchase_metrics"] == "prior only"
    assert benchmark.json()["data"]["metrics"]["shuffle_metrics"]["available"] is False
    assert c8a.json()["data"]["recommendation_accuracy_measured"] is False
    assert "not live streaming" in c8b.json()["data"]["classification"]
    assert "/home/" not in registry.text
    assert "results/" not in c8a.text + c8b.text


def test_orders_distribution_route_labels_all_orders_and_validates_domains():
    with TestClient(app_for(Database())) as client:
        response = client.get("/api/v1/analytics/orders-distribution")
    assert response.status_code == 200
    body = response.json()
    assert body["scope"] == "all orders"
    assert body["measure"] == "order count, not item-level prior purchases"
    assert len(body["orders_by_day_of_week"]) == 7
    assert len(body["orders_by_hour"]) == 24
    assert sum(row["order_count"] for row in body["orders_by_hour"]) == body["total_orders"]


def test_als_evaluation_route_preserves_cohort_k_and_non_hybrid_scores():
    with TestClient(app_for(Database())) as client:
        response = client.get("/api/v1/evaluation/als")
    assert response.status_code == 200
    body = response.json()
    assert body["ks"] == [5, 10, 20]
    assert body["cohorts"]["all_official_train_users"]["metrics"]["als"]["10"]["recall_at_k"] == 0.12147208248298487
    assert body["cohorts"]["all_official_train_users"]["metrics"]["personal_frequency"]["10"]["recall_at_k"] == 0.31923221187877215
    assert body["cohorts"]["all_official_train_users"]["users"] == 131209
    assert "hybrid" not in response.text.lower()
    assert body["evaluation_method"]["ground_truth"] == "official order_products_train, distinct product IDs per user"


def test_unknown_or_missing_evidence_uses_distinct_sanitized_errors(tmp_path):
    with TestClient(app_for(Database())) as client:
        unknown = client.get("/api/v1/evidence/not-registered")
    with TestClient(app_for(Database(), root=tmp_path)) as client:
        missing = client.get("/api/v1/evidence/module2-full-analytics")
    assert unknown.status_code == 404
    assert unknown.json()["error"]["code"] == "unknown_evidence_id"
    assert missing.status_code == 503
    assert missing.json()["error"]["code"] == "evidence_unavailable"
    assert str(tmp_path) not in missing.text


def test_get_routes_do_not_write_or_create_mongo_state():
    database = BatchDatabase([product()], [department()])
    with TestClient(app_for(database)) as client:
        assert client.get("/api/v1/batch/products").status_code == 200
        assert client.get("/api/v1/batch/departments").status_code == 200
    assert database.products.write_calls == []
    assert database.departments.write_calls == []


def test_openapi_lists_all_d1b_and_preserved_d1a_routes():
    schema = create_app(_config()).openapi()
    paths = schema["paths"]
    assert {
        "/api/v1/batch/products",
        "/api/v1/batch/departments",
        "/api/v1/evidence",
        "/api/v1/evidence/{evidence_id}",
        "/api/v1/analytics/orders-distribution",
        "/api/v1/evaluation/als",
        "/api/v1/recommendations/{user_id}",
        "/api/v1/trends",
    } <= set(paths)
    assert paths["/api/v1/batch/products"]["get"]["parameters"]


def test_batch_timestamp_wire_values_are_utc():
    database = BatchDatabase([product()])
    with TestClient(app_for(database)) as client:
        response = client.get("/api/v1/batch/products")
    assert response.status_code == 200
    assert response.json()["items"][0]["last_batch_time"].endswith("Z")
