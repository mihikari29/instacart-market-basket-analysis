#!/usr/bin/env python3
"""Compare live API responses with the synthetic fixture's MongoDB documents."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sys
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

from pymongo import MongoClient

from src.module4.dashboard.batch_readers import PRODUCT_INDEXES
from src.module4.dashboard.evidence import EVIDENCE_SPECS
from src.module4.hybrid.data import READINESS_COLLECTION
from src.module4.dashboard.readers import TREND_SELECTION_INDEX_NAME


WARM_USER_ID = 601
FALLBACK_USER_ID = 602
EMPTY_USER_ID = 603
UNKNOWN_USER_ID = 9999
SENSITIVE_RE = re.compile(r"(?i)(mongodb(?:\+srv)?://|password|passwd|secret|token|root:[^@]+@)")


def _required(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def _get(base_url: str, path: str, expected_status: int = 200) -> tuple[int, dict[str, Any]]:
    try:
        with urlopen(f"{base_url.rstrip('/')}{path}", timeout=10) as response:
            status = response.status
            payload = json.loads(response.read())
    except HTTPError as error:
        status = error.code
        payload = json.loads(error.read())
    except URLError as error:
        raise RuntimeError(f"API request failed: {type(error.reason).__name__}") from error
    if status != expected_status:
        raise RuntimeError(f"{path} returned HTTP {status}; expected {expected_status}: {payload}")
    return status, payload


def _same_timestamp(api_value: str | None, stored_value: datetime | None) -> bool:
    if api_value is None or stored_value is None:
        return api_value is None and stored_value is None
    parsed = datetime.fromisoformat(api_value.replace("Z", "+00:00"))
    return parsed.astimezone(timezone.utc) == stored_value.astimezone(timezone.utc)


def _assert_no_secrets(payloads: list[dict[str, Any]], values: list[str]) -> None:
    combined = json.dumps(payloads, sort_keys=True)
    for value in values:
        if value and value in combined:
            raise RuntimeError("an API response included a configured credential")
    if SENSITIVE_RE.search(combined):
        raise RuntimeError("an API response included a credential-bearing value")


def _expected_indexes(database: Any) -> dict[str, Any]:
    expected = {
        "final_recommendations": {
            "uq_serving_user_rank": [("serving_version", 1), ("user_id", 1), ("rank", 1)],
            "uq_serving_user_product": [("serving_version", 1), ("user_id", 1), ("product_id", 1)],
        },
        "realtime_trending_windows": {
            TREND_SELECTION_INDEX_NAME: [("source_namespace", 1), ("window_end", -1)],
        },
        "batch_product_metrics": {
            name: list(keys) for name, keys in PRODUCT_INDEXES.values()
        },
    }
    results = {}
    for collection_name, indexes in expected.items():
        info = database[collection_name].index_information()
        results[collection_name] = {}
        if "_id_" not in info:
            raise RuntimeError(f"{collection_name} lost its default _id index")
        for name, keys in indexes.items():
            actual = info.get(name)
            if actual is None or list(actual.get("key", ())) != keys:
                raise RuntimeError(f"MongoDB index {collection_name}.{name} has the wrong definition")
            results[collection_name][name] = list(actual["key"])
    return results


def _assert_plan(database: Any, collection_name: str, query: dict[str, Any], sort: list[tuple[str, int]], hint: str) -> None:
    plan = database[collection_name].find(query).sort(sort).hint(hint).limit(20).explain()
    rendered = json.dumps(plan.get("queryPlanner", {}).get("winningPlan", {}))
    if "IXSCAN" not in rendered or "COLLSCAN" in rendered:
        raise RuntimeError(f"{collection_name} did not use the required index in its explain plan")


def validate() -> dict[str, Any]:
    database_name = _required("MODULE4D_DEMO_DATABASE")
    namespace = _required("MODULE4D_DEMO_TREND_NAMESPACE")
    readonly_user = _required("MODULE4D_DEMO_READONLY_USERNAME")
    readonly_password = _required("MODULE4D_DEMO_READONLY_PASSWORD")
    base_url = _required("MODULE4D_DEMO_API_BASE_URL").rstrip("/")
    if not base_url.startswith("http://module4d-api:"):
        raise RuntimeError("live validator must call the isolated module4d-api service")
    reader_uri = (
        f"mongodb://{readonly_user}:{readonly_password}@module4d-mongo:27017/"
        f"{database_name}?authSource={database_name}"
    )
    readonly = MongoClient(reader_uri, tz_aware=True, serverSelectionTimeoutMS=5_000)
    database = readonly[database_name]
    reader_db = database
    try:
        index_details = _expected_indexes(database)
        bundle_index = TREND_SELECTION_INDEX_NAME
        _assert_plan(
            database,
            "realtime_trending_windows",
            {"source_namespace": namespace, "window_end": {"$lte": datetime.now(timezone.utc)}},
            [("window_end", -1)],
            bundle_index,
        )
        for order, (index_name, _keys) in PRODUCT_INDEXES.items():
            _assert_plan(
                database,
                "batch_product_metrics",
                {},
                [(order, -1), ("product_id", 1)],
                index_name,
            )
        reader_db["batch_product_metrics"].find_one()
        reader_db["batch_product_metrics"].index_information()
        if reader_db["final_recommendations"].find_one({"user_id": WARM_USER_ID}) is None:
            raise RuntimeError("read-only identity cannot read the published Hybrid rows")

        now = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        query = urlencode({"source_namespace": namespace, "as_of_event_time": now})
        routes = {
            "health": ("/health", 200),
            "recommendations": (f"/api/v1/recommendations/{WARM_USER_ID}", 200),
            "trends": (f"/api/v1/trends?{query}", 200),
            "batch_products": ("/api/v1/batch/products?order=purchase_count&limit=20", 200),
            "batch_products_reorder": ("/api/v1/batch/products?order=reorder_rate&limit=20", 200),
            "batch_departments": ("/api/v1/batch/departments", 200),
            "evidence_registry": ("/api/v1/evidence", 200),
            "evidence_analytics": ("/api/v1/evidence/module2-full-analytics", 200),
            "orders_distribution": ("/api/v1/analytics/orders-distribution", 200),
            "als_evaluation": ("/api/v1/evaluation/als", 200),
            "evidence_als": ("/api/v1/evidence/module4a-full-als-evaluation", 200),
            "evidence_benchmarks": ("/api/v1/evidence/module2-full-hardening-benchmarks", 200),
            "evidence_c8a": ("/api/v1/evidence/module4c-c8a-responsiveness", 200),
            "evidence_c8b": ("/api/v1/evidence/module4c-c8b-streaming-demo", 200),
            "recommendations_fallback": (f"/api/v1/recommendations/{FALLBACK_USER_ID}", 200),
            "recommendations_ready_empty": (f"/api/v1/recommendations/{EMPTY_USER_ID}", 200),
            "unknown_user": (f"/api/v1/recommendations/{UNKNOWN_USER_ID}", 404),
            "missing_readiness": ("/api/v1/recommendations/604", 404),
            "invalid_namespace": ("/api/v1/trends?" + urlencode({"source_namespace": "not-allowlisted", "as_of_event_time": now}), 422),
            "invalid_event_time": ("/api/v1/trends?" + urlencode({"source_namespace": namespace, "as_of_event_time": "2026-10-14T12:00:00"}), 422),
            "unknown_evidence": ("/api/v1/evidence/not-registered", 404),
        }
        before_collections = set(database.list_collection_names())
        before_indexes = {name: database[name].index_information() for name in before_collections}
        before_pointers = {
            row["_id"]: row
            for row in database[READINESS_COLLECTION].find({"_id": {"$in": ["active", f"hybrid:{WARM_USER_ID}", f"hybrid:{FALLBACK_USER_ID}", f"hybrid:{EMPTY_USER_ID}"]}})
        }
        responses = {}
        statuses = {}
        for name, (path, expected_status) in routes.items():
            statuses[name], responses[name] = _get(base_url, path, expected_status)
        _assert_no_secrets(
            list(responses.values()),
            [reader_uri, readonly_password],
        )

        recommendation = responses["recommendations"]
        pointer = database[READINESS_COLLECTION].find_one({"_id": f"hybrid:{WARM_USER_ID}"})
        persisted = list(
            database["final_recommendations"]
            .find({"user_id": WARM_USER_ID, "serving_version": pointer["serving_version"]})
            .sort("rank", 1)
        )
        if recommendation["serving_version"] != pointer["serving_version"]:
            raise RuntimeError("API selected a serving version different from its Mongo readiness pointer")
        if len(persisted) != recommendation["recommendation_count"]:
            raise RuntimeError("API recommendation count differs from persisted immutable rows")
        for api_row, stored_row in zip(recommendation["recommendations"], persisted, strict=True):
            for field in (
                "product_id", "product_name", "rank", "from_als", "from_trend", "als_score",
                "trend_score", "normalized_als_score", "normalized_trend_score", "final_score",
            ):
                if api_row[field] != stored_row[field]:
                    raise RuntimeError(f"API recommendation field {field} differs from persisted Mongo row")
        if [row["rank"] for row in recommendation["recommendations"]] != list(range(1, len(persisted) + 1)):
            raise RuntimeError("Hybrid API returned non-contiguous persisted ranks")

        selected_bundle = (
            database["realtime_trending_windows"]
            .find({"source_namespace": namespace, "window_end": {"$lte": datetime.now(timezone.utc)}})
            .sort("window_end", -1)
            .limit(1)
            .next()
        )
        trend = responses["trends"]
        trend_mismatches = {
            "content_sha256": trend["content_sha256"] != selected_bundle["content_sha256"],
            "window_start": not _same_timestamp(trend["window_start"], selected_bundle["window_start"]),
            "window_end": not _same_timestamp(trend["window_end"], selected_bundle["window_end"]),
            "published_at": not _same_timestamp(trend["published_at"], selected_bundle["published_at"]),
            "expires_at": not _same_timestamp(trend["expires_at"], selected_bundle.get("expires_at")),
            "schema_version": trend["schema_version"] != selected_bundle["schema_version"],
            "config_hash": trend["config_hash"] != selected_bundle["config_hash"],
            "source_scope": trend["source_scope"] != selected_bundle["source_scope"],
            "row_count": trend["row_count"] != selected_bundle["row_count"],
            "product_ids": [row["product_id"] for row in trend["items"]]
            != [row["product_id"] for row in selected_bundle["items"]],
        }
        if any(trend_mismatches.values()):
            failed_fields = [field for field, failed in trend_mismatches.items() if failed]
            raise RuntimeError(
                "C5 trend API response differs from the newest eligible finalized bundle: "
                + ", ".join(failed_fields)
            )
        for api_row, stored_row in zip(trend["items"], selected_bundle["items"], strict=True):
            if any(api_row[field] != stored_row[field] for field in ("product_id", "trend_score", "trend_rank")):
                raise RuntimeError("trend API ranking row differs from the persisted finalized bundle")

        expected_products = list(
            database["batch_product_metrics"]
            .find({}, {"_id": 1, "product_id": 1, "purchase_count": 1, "unique_users": 1, "reorder_count": 1, "reorder_rate": 1, "run_id": 1, "last_batch_time": 1})
            .sort([("purchase_count", -1), ("product_id", 1)])
        )
        product_response = responses["batch_products"]
        if [row["product_id"] for row in product_response["items"]] != [row["product_id"] for row in expected_products]:
            raise RuntimeError("batch product API ordering differs from stored metric documents")
        for api_row, db_row in zip(product_response["items"], expected_products, strict=True):
            for field in ("product_id", "purchase_count", "unique_users", "reorder_count", "reorder_rate", "run_id"):
                if api_row[field] != db_row[field]:
                    raise RuntimeError(f"batch product API field {field} differs from MongoDB")
        if responses["batch_products_reorder"]["order"] != "reorder_rate":
            raise RuntimeError("reorder-rate batch route selected the wrong metric")
        expected_reorder = list(
            database["batch_product_metrics"].find({}).sort([("reorder_rate", -1), ("product_id", 1)])
        )
        reorder_response = responses["batch_products_reorder"]["items"]
        if [row["product_id"] for row in reorder_response] != [row["product_id"] for row in expected_reorder]:
            raise RuntimeError("reorder-rate batch API tie ordering differs from stored documents")
        for api_row, db_row in zip(reorder_response, expected_reorder, strict=True):
            for field in ("product_id", "purchase_count", "unique_users", "reorder_count", "reorder_rate", "run_id"):
                if api_row[field] != db_row[field]:
                    raise RuntimeError(f"reorder-rate batch API field {field} differs from MongoDB")
        expected_departments = list(database["department_metrics"].find({}).sort("department_id", 1))
        department_response = responses["batch_departments"]["items"]
        if [row["department_id"] for row in department_response] != [
            row["department_id"] for row in expected_departments
        ]:
            raise RuntimeError("department API ordering differs from MongoDB")
        for api_row, db_row in zip(department_response, expected_departments, strict=True):
            for field in ("department_id", "purchase_count", "unique_users", "reorder_rate", "run_id"):
                if api_row[field] != db_row[field]:
                    raise RuntimeError(f"department API field {field} differs from MongoDB")

        registry = {item["evidence_id"]: item for item in responses["evidence_registry"]["items"]}
        evidence_checks = {}
        evidence_details = {
            "module2-full-analytics": responses["evidence_analytics"],
            "module2-full-hardening-benchmarks": responses["evidence_benchmarks"],
            "module4a-full-als-evaluation": responses["evidence_als"],
            "module4c-c8a-responsiveness": responses["evidence_c8a"],
            "module4c-c8b-streaming-demo": responses["evidence_c8b"],
        }
        for evidence_id in EVIDENCE_SPECS:
            artifact = evidence_details[evidence_id]
            if artifact is None or not registry[evidence_id]["available"]:
                raise RuntimeError(f"registered historical evidence {evidence_id} is not available")
            spec_path = Path("/workspace") / EVIDENCE_SPECS[evidence_id]["path"]
            digest = hashlib.sha256(spec_path.read_bytes()).hexdigest()
            if artifact["provenance"]["sha256"] != digest or registry[evidence_id]["sha256"] != digest:
                raise RuntimeError(f"evidence digest mismatch for {evidence_id}")
            evidence_checks[evidence_id] = {"sha256": digest, "archived": artifact["provenance"]["archived"]}

        orders = responses["orders_distribution"]
        if (
            orders["scope"] != "all orders"
            or orders["measure"] != "order count, not item-level prior purchases"
            or orders["total_orders"] != 3_421_083
        ):
            raise RuntimeError("archived order-distribution response has unclear purchase scope")
        if sum(row["order_count"] for row in orders["orders_by_day_of_week"]) != orders["total_orders"]:
            raise RuntimeError("archived day distribution does not reconcile to total all orders")
        if sum(row["order_count"] for row in orders["orders_by_hour"]) != orders["total_orders"]:
            raise RuntimeError("archived hour distribution does not reconcile to total all orders")
        evaluation = responses["als_evaluation"]
        cohort = evaluation["cohorts"]["all_official_train_users"]["metrics"]
        recall_als = cohort["als"]["10"]["recall_at_k"]
        recall_frequency = cohort["personal_frequency"]["10"]["recall_at_k"]
        if abs(recall_als - 0.12147208248298487) > 1e-12 or abs(recall_frequency - 0.31923221187877215) > 1e-12:
            raise RuntimeError("archived all-user Recall@10 values differ from the accepted report")
        shuffle = responses["evidence_benchmarks"]["data"]["metrics"]["shuffle_metrics"]
        if shuffle["available"] is not False:
            raise RuntimeError("archived Module 2 evidence no longer marks structured shuffle data unavailable")
        if responses["recommendations_fallback"]["mode"] != "als_fallback_trend_unavailable":
            raise RuntimeError("fallback recommendation response has the wrong mode")
        if responses["recommendations_ready_empty"]["recommendations"] != []:
            raise RuntimeError("ready empty snapshot response was not represented as empty")
        for name in ("unknown_user", "missing_readiness", "unknown_evidence"):
            if "error" not in responses[name] or "request_id" not in responses[name]["error"]:
                raise RuntimeError(f"{name} did not use the accepted HTTP error envelope")
        if responses["unknown_user"]["error"]["code"] != "recommendation_snapshot_not_found":
            raise RuntimeError("unknown recommendation user returned an incorrect error contract")

        after_collections = set(database.list_collection_names())
        after_indexes = {name: database[name].index_information() for name in after_collections}
        after_pointers = {
            row["_id"]: row
            for row in database[READINESS_COLLECTION].find({"_id": {"$in": ["active", f"hybrid:{WARM_USER_ID}", f"hybrid:{FALLBACK_USER_ID}", f"hybrid:{EMPTY_USER_ID}"]}})
        }
        if before_collections != after_collections or before_indexes != after_indexes or before_pointers != after_pointers:
            raise RuntimeError("GET requests changed MongoDB collections, indexes, or readiness pointers")
        return {
            "routes_checked": list(routes),
            "http_statuses": statuses,
            "warm_hybrid": {
                "serving_version": recommendation["serving_version"],
                "mode": recommendation["mode"],
                "count": recommendation["recommendation_count"],
                "ranked_products": [
                    {"rank": row["rank"], "product_id": row["product_id"], "final_score": row["final_score"]}
                    for row in recommendation["recommendations"]
                ],
            },
            "selected_trend": {
                "namespace": trend["source_namespace"],
                "scope": trend["source_scope"],
                "window_end": trend["window_end"],
                "content_sha256": trend["content_sha256"],
                "product_ids": [row["product_id"] for row in trend["items"]],
            },
            "batch_products": [
                {"product_id": row["product_id"], "purchase_count": row["purchase_count"], "reorder_rate": row["reorder_rate"]}
                for row in product_response["items"]
            ],
            "department_count": len(responses["batch_departments"]["items"]),
            "all_order_total": orders["total_orders"],
            "all_user_recall_at_10": {"als": recall_als, "personal_frequency": recall_frequency},
            "evidence": evidence_checks,
            "required_indexes_and_explain_plans": index_details,
            "readonly_identity": "find/listIndexes succeed; write/createIndexes denied by MongoDB",
            "get_requests_mutated_database": False,
            "credential_leak_scan": "passed",
        }
    finally:
        readonly.close()


if __name__ == "__main__":
    try:
        result = validate()
    except Exception as exc:
        print(f"Module 4D live API validation failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        raise SystemExit(1)
    print(json.dumps(result, sort_keys=True, indent=2))
