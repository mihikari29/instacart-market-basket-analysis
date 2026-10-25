"""Small bounded HTTP client for the fixed Module 4D read API."""

from __future__ import annotations

from datetime import datetime, timezone
import ipaddress
import json
import math
import os
import re
from typing import Any
from urllib.parse import urlsplit

import requests


MAX_RESPONSE_BYTES = 1_048_576
CONNECT_TIMEOUT_SECONDS = 2.0
READ_TIMEOUT_SECONDS = 6.0
MAX_USER_ID = 9_223_372_036_854_775_807
MAX_TREND_AGE_SECONDS = 31_536_000
ALLOWED_DOCKER_HOSTS = frozenset({"module4d-api"})
ALLOWED_EVIDENCE_IDS = frozenset(
    {
        "module2-full-hardening-benchmarks",
        "module4c-c8a-responsiveness",
        "module4c-c8b-streaming-demo",
    }
)
EVALUATION_MODEL_IDS = ("als", "global_popularity", "personal_frequency")
EVALUATION_COHORT_IDS = ("all_official_train_users", "warm_users_and_all_truth_items")
EVALUATION_HISTORY_IDS = ("new_to_user", "reordered")
EVALUATION_METRIC_IDS = ("precision_at_k", "recall_at_k", "map_at_k", "ndcg_at_k")
EVALUATION_KS = (5, 10, 20)


class ApiClientError(RuntimeError):
    """Safe, structured failure from API configuration or a read request."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        code: str = "api_unavailable",
        request_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.request_id = request_id
        self.message = message


def _safe_json(raw: bytes) -> Any:
    def reject_constant(value: str) -> None:
        raise ValueError(f"invalid JSON constant: {value}")

    return json.loads(raw.decode("utf-8"), parse_constant=reject_constant)


def _is_json_content_type(value: str | None) -> bool:
    if not value:
        return False
    media_type = value.split(";", 1)[0].strip().lower()
    return media_type == "application/json" or (
        media_type.startswith("application/") and media_type.endswith("+json")
    )


def validate_base_url(value: str) -> str:
    """Accept only a plain local or explicit internal-service API origin."""
    if not isinstance(value, str) or not value or value != value.strip() or len(value) > 512:
        raise ApiClientError(
            "Dashboard API URL configuration is invalid.", code="invalid_api_configuration"
        )
    try:
        parsed = urlsplit(value)
        hostname = parsed.hostname
        port = parsed.port
    except ValueError as exc:
        raise ApiClientError(
            "Dashboard API URL configuration is invalid.", code="invalid_api_configuration"
        ) from exc
    if (
        parsed.scheme not in {"http", "https"}
        or not hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"", "/"}
    ):
        raise ApiClientError(
            "Dashboard API URL configuration is invalid.", code="invalid_api_configuration"
        )
    host = hostname.lower().rstrip(".")
    try:
        is_loopback = ipaddress.ip_address(host).is_loopback
    except ValueError:
        is_loopback = host == "localhost"
    if not is_loopback and host not in ALLOWED_DOCKER_HOSTS:
        raise ApiClientError(
            "Dashboard API URL must use localhost or the configured internal API service.",
            code="invalid_api_configuration",
        )
    if port is not None and not 1 <= port <= 65535:
        raise ApiClientError(
            "Dashboard API URL configuration is invalid.", code="invalid_api_configuration"
        )
    return value.rstrip("/")


def validate_user_id(value: Any) -> int:
    if isinstance(value, bool):
        raise ApiClientError("Enter a positive integer user ID.", status_code=422, code="invalid_user_id")
    try:
        if isinstance(value, str):
            if not value or not value.isdecimal():
                raise ValueError
            user_id = int(value)
        elif isinstance(value, int):
            user_id = value
        else:
            raise ValueError
    except (TypeError, ValueError, OverflowError) as exc:
        raise ApiClientError("Enter a positive integer user ID.", status_code=422, code="invalid_user_id") from exc
    if not 1 <= user_id <= MAX_USER_ID:
        raise ApiClientError("Enter a positive integer user ID.", status_code=422, code="invalid_user_id")
    return user_id


def validate_trend_namespace(value: str, allowed: tuple[str, ...] | None = None) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > 256
        or "\x00" in value
        or (allowed is not None and value not in allowed)
    ):
        raise ApiClientError(
            "Select a configured trend source namespace.", status_code=422, code="invalid_namespace"
        )
    return value


def validate_as_of_event_time(value: datetime | str) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
    except (ValueError, OverflowError) as exc:
        raise ApiClientError(
            "Choose a valid as-of event time.", status_code=422, code="invalid_filter"
        ) from exc
    if not isinstance(parsed, datetime) or parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ApiClientError(
            "As-of event time must include a timezone.", status_code=422, code="invalid_filter"
        )
    try:
        utc_value = parsed.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    except (ValueError, OverflowError) as exc:
        raise ApiClientError(
            "Choose a valid as-of event time.", status_code=422, code="invalid_filter"
        ) from exc
    if len(utc_value) > 64:
        raise ApiClientError("Choose a valid as-of event time.", status_code=422, code="invalid_filter")
    return utc_value


def _require_mapping(body: Any, endpoint: str) -> dict[str, Any]:
    if not isinstance(body, dict):
        raise ValueError(f"{endpoint} response must be an object")
    return body


def _require_keys(body: dict[str, Any], required: set[str], endpoint: str) -> None:
    if not required.issubset(body):
        raise ValueError(f"{endpoint} response is missing fields")


def _require_list(value: Any, endpoint: str, maximum: int) -> list[Any]:
    if not isinstance(value, list) or len(value) > maximum:
        raise ValueError(f"{endpoint} response list is invalid")
    return value


def _validate_timestamp(value: Any, endpoint: str, *, nullable: bool = False) -> None:
    if value is None and nullable:
        return
    if not isinstance(value, str):
        raise ValueError(f"{endpoint} timestamp is invalid")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{endpoint} timestamp is missing a timezone")


def _check_text(value: Any, endpoint: str, *, nullable: bool = False) -> None:
    if nullable and value is None:
        return
    if not isinstance(value, str) or not value or len(value) > 4096:
        raise ValueError(f"{endpoint} text field is invalid")


def _check_bool(value: Any, endpoint: str) -> None:
    if type(value) is not bool:
        raise ValueError(f"{endpoint} boolean field is invalid")


def _check_int(
    value: Any,
    endpoint: str,
    *,
    minimum: int = 0,
    maximum: int | None = None,
    nullable: bool = False,
) -> None:
    if nullable and value is None:
        return
    if type(value) is not int or value < minimum or (maximum is not None and value > maximum):
        raise ValueError(f"{endpoint} integer field is invalid")


def _check_number(
    value: Any,
    endpoint: str,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
    nullable: bool = False,
) -> None:
    if nullable and value is None:
        return
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f"{endpoint} number field is invalid")
    if minimum is not None and value < minimum or maximum is not None and value > maximum:
        raise ValueError(f"{endpoint} number is outside its allowed range")


def _check_provenance(value: Any, endpoint: str) -> None:
    provenance = _require_mapping(value, endpoint)
    _require_keys(provenance, {"artifact_key", "source_label", "classification", "archived", "sha256"}, endpoint)
    for key in ("artifact_key", "source_label", "classification", "sha256"):
        _check_text(provenance[key], endpoint)
    _check_bool(provenance["archived"], endpoint)
    if not provenance["archived"]:
        raise ValueError("evidence provenance is not archived")


def _check_metric_row(value: Any, endpoint: str, expected_k: int) -> None:
    row = _require_mapping(value, endpoint)
    required = {
        "k", "status", *EVALUATION_METRIC_IDS, "evaluated_users", "user_coverage", "catalog_coverage_rate"
    }
    _require_keys(row, required, endpoint)
    if row["status"] != "evaluated":
        raise ValueError("evaluation metric status is invalid")
    _check_int(row["k"], endpoint, minimum=1, maximum=100)
    if row["k"] != expected_k:
        raise ValueError("evaluation metric K does not match its key")
    for key in EVALUATION_METRIC_IDS:
        _check_number(row[key], endpoint, minimum=0, maximum=1)
    _check_int(row["evaluated_users"], endpoint, minimum=1)
    for key in ("user_coverage", "catalog_coverage_rate"):
        _check_number(row[key], endpoint, minimum=0, maximum=1, nullable=True)
    for key in ("catalog_coverage", "users_with_no_recommendations"):
        if key in row:
            _check_int(row[key], endpoint, nullable=True)


def _check_evaluation_groups(
    value: Any, endpoint: str, expected_ids: tuple[str, ...]
) -> None:
    groups = _require_mapping(value, endpoint)
    if set(groups) != set(expected_ids):
        raise ValueError("evaluation cohort identities are invalid")
    for cohort_id in expected_ids:
        cohort = _require_mapping(groups[cohort_id], endpoint)
        _require_keys(cohort, {"definition", "users", "metrics"}, endpoint)
        _check_text(cohort["definition"], endpoint, nullable=True)
        _check_int(cohort["users"], endpoint, minimum=1)
        model_groups = _require_mapping(cohort["metrics"], endpoint)
        if set(model_groups) != set(EVALUATION_MODEL_IDS):
            raise ValueError("evaluation method identities are invalid")
        for model_id in EVALUATION_MODEL_IDS:
            by_k = _require_mapping(model_groups[model_id], endpoint)
            if set(by_k) != {str(k) for k in EVALUATION_KS}:
                raise ValueError("evaluation K identities are invalid")
            for k in EVALUATION_KS:
                _check_metric_row(by_k[str(k)], endpoint, k)


def _validate_als_evaluation(value: Any) -> dict[str, Any]:
    endpoint = "als_evaluation"
    body = _require_mapping(value, endpoint)
    _require_keys(
        body,
        {
            "evidence_id", "title", "provenance", "algorithm_scope", "evaluation_method", "ks", "cohorts",
            "history_subgroups", "als_vs_baseline_deltas", "official_test_truth_available", "truth_summary",
            "observed_at",
        },
        endpoint,
    )
    _check_text(body["evidence_id"], endpoint)
    if body["evidence_id"] != "module4a-full-als-evaluation":
        raise ValueError("ALS evaluation identity is invalid")
    _check_text(body["title"], endpoint)
    _check_provenance(body["provenance"], endpoint)
    if body["algorithm_scope"] != "ALS and recorded baselines":
        raise ValueError("ALS evaluation scope is invalid")
    method = _require_mapping(body["evaluation_method"], endpoint)
    if not method or not all(isinstance(key, str) and isinstance(item, str) for key, item in method.items()):
        raise ValueError("ALS evaluation methodology is invalid")
    ks = _require_list(body["ks"], endpoint, 3)
    if ks != list(EVALUATION_KS):
        raise ValueError("ALS evaluation K values are unsupported")
    _check_evaluation_groups(body["cohorts"], endpoint, EVALUATION_COHORT_IDS)
    _check_evaluation_groups(body["history_subgroups"], endpoint, EVALUATION_HISTORY_IDS)
    deltas = _require_mapping(body["als_vs_baseline_deltas"], endpoint)
    if set(deltas) != set(EVALUATION_COHORT_IDS):
        raise ValueError("ALS baseline comparison identities are invalid")
    for cohort_id in EVALUATION_COHORT_IDS:
        baselines = _require_mapping(deltas[cohort_id], endpoint)
        if set(baselines) != {"global_popularity", "personal_frequency"}:
            raise ValueError("ALS baseline comparison methods are invalid")
        for model_id, by_k_value in baselines.items():
            by_k = _require_mapping(by_k_value, endpoint)
            if set(by_k) != {str(k) for k in EVALUATION_KS}:
                raise ValueError("ALS baseline comparison K values are invalid")
            for metrics in by_k.values():
                metrics = _require_mapping(metrics, endpoint)
                if set(metrics) != set(EVALUATION_METRIC_IDS):
                    raise ValueError("ALS baseline comparison metrics are invalid")
                for score in metrics.values():
                    _check_number(score, endpoint)
    _check_bool(body["official_test_truth_available"], endpoint)
    summary = _require_mapping(body["truth_summary"], endpoint)
    if not summary:
        raise ValueError("ALS truth summary is empty")
    for count in summary.values():
        _check_int(count, endpoint)
    _validate_timestamp(body["observed_at"], endpoint)
    return body


def _validate_evidence_data(evidence_id: str, data_value: Any) -> None:
    endpoint = f"evidence:{evidence_id}"
    data = _require_mapping(data_value, endpoint)
    if evidence_id == "module2-full-hardening-benchmarks":
        _require_keys(data, {"run", "environment", "metrics", "method_notes", "module2_validation"}, endpoint)
        run = _require_mapping(data["run"], endpoint)
        _require_keys(run, {"run_id", "timestamp_utc", "dataset_classification", "date_utc"}, endpoint)
        for field in run.values():
            _check_text(field, endpoint)
        environment = _require_mapping(data["environment"], endpoint)
        _require_keys(
            environment,
            {"platform", "resources", "spark", "java", "python", "shuffle_partitions", "warmups", "measured_repetitions"},
            endpoint,
        )
        for key in ("platform", "resources", "spark", "java", "python"):
            _check_text(environment[key], endpoint)
        for key in ("shuffle_partitions", "measured_repetitions"):
            _check_int(environment[key], endpoint, minimum=1)
        _check_int(environment["warmups"], endpoint)
        metrics = _require_mapping(data["metrics"], endpoint)
        join = _require_mapping(metrics.get("join_strategy_medians_seconds"), endpoint)
        _require_keys(join, {"broadcast_hash_join", "sort_merge_join"}, endpoint)
        scans = _require_mapping(metrics.get("partition_scan_medians_seconds"), endpoint)
        _require_keys(scans, {"full_scan", "date_filter", "partition_filter", "partition_filter_rows"}, endpoint)
        cache = _require_mapping(metrics.get("cache_medians_seconds"), endpoint)
        _require_keys(cache, {"uncached", "cache_materialization", "cached_reuse"}, endpoint)
        for group in (join, scans, cache):
            for key, number in group.items():
                if key.endswith("_rows"):
                    _check_int(number, endpoint)
                else:
                    _check_number(number, endpoint, minimum=0)
        shuffle = _require_mapping(metrics.get("shuffle_metrics"), endpoint)
        _require_keys(shuffle, {"available", "reason"}, endpoint)
        _check_bool(shuffle["available"], endpoint)
        if not shuffle["available"]:
            _check_text(shuffle["reason"], endpoint)
        notes = _require_mapping(data["method_notes"], endpoint)
        for value in notes.values():
            if isinstance(value, str):
                continue
            if type(value) is bool:
                continue
            raise ValueError("benchmark method notes are invalid")
        validation = _require_mapping(data["module2_validation"], endpoint)
        for value in validation.values():
            if type(value) is bool:
                continue
            _check_int(value, endpoint)
    elif evidence_id == "module4c-c8a-responsiveness":
        _require_keys(
            data,
            {
                "experiment_id", "status", "classification", "top20_overlap", "ranking_response",
                "first_publication_cli_wall_ms_at_lambda_0_5", "recommendation_accuracy_measured", "limitations",
            },
            endpoint,
        )
        for key in ("experiment_id", "status", "classification", "top20_overlap"):
            _check_text(data[key], endpoint)
        if "synthetic" not in data["classification"].lower():
            raise ValueError("C8A experiment classification is invalid")
        rows = _require_list(data["ranking_response"], endpoint, 5)
        expected_lambdas = [0.0, 0.5, 0.7, 0.9, 1.0]
        if len(rows) != len(expected_lambdas):
            raise ValueError("C8A lambda series is incomplete")
        for row, expected_lambda in zip(rows, expected_lambdas, strict=True):
            row = _require_mapping(row, endpoint)
            _require_keys(
                row,
                {"lambda", "baseline_top1", "changed_top1", "rank_changed_products", "mean_absolute_rank_displacement"},
                endpoint,
            )
            _check_number(row["lambda"], endpoint, minimum=0, maximum=1)
            if row["lambda"] != expected_lambda:
                raise ValueError("C8A lambda ordering is invalid")
            _check_text(row["baseline_top1"], endpoint)
            _check_text(row["changed_top1"], endpoint)
            _check_int(row["rank_changed_products"], endpoint)
            _check_number(row["mean_absolute_rank_displacement"], endpoint, minimum=0)
        timing = _require_mapping(data["first_publication_cli_wall_ms_at_lambda_0_5"], endpoint)
        _require_keys(timing, {"baseline", "changed", "warmups", "includes_process_startup_and_mongodb_serving", "excludes_fixture_and_bundle_setup", "not_kafka_to_serving_latency"}, endpoint)
        for variant in ("baseline", "changed"):
            values = _require_mapping(timing[variant], endpoint)
            _require_keys(values, {"repetitions", "median_ms", "min_ms", "max_ms"}, endpoint)
            _check_int(values["repetitions"], endpoint, minimum=1)
            for key in ("median_ms", "min_ms", "max_ms"):
                _check_number(values[key], endpoint, minimum=0)
        _check_int(timing["warmups"], endpoint)
        for key in ("includes_process_startup_and_mongodb_serving", "excludes_fixture_and_bundle_setup", "not_kafka_to_serving_latency"):
            _check_bool(timing[key], endpoint)
        if not all(
            timing[key]
            for key in (
                "includes_process_startup_and_mongodb_serving",
                "excludes_fixture_and_bundle_setup",
                "not_kafka_to_serving_latency",
            )
        ):
            raise ValueError("C8A timing interpretation flags are invalid")
        if data["recommendation_accuracy_measured"] is not False:
            raise ValueError("C8A accuracy state is invalid")
        limitations = _require_list(data["limitations"], endpoint, 10)
        for limitation in limitations:
            _check_text(limitation, endpoint)
    else:
        _require_keys(
            data,
            {
                "experiment_id", "status", "classification", "runtime_versions", "runtime_isolation", "events",
                "windows", "spark_batch_durations_ms", "serving", "limitations", "interpretation",
            },
            endpoint,
        )
        for key in ("experiment_id", "status", "classification", "interpretation"):
            _check_text(data[key], endpoint)
        if "synthetic" not in data["classification"].lower():
            raise ValueError("C8B experiment classification is invalid")
        versions = _require_mapping(data["runtime_versions"], endpoint)
        _require_keys(versions, {"kafka", "spark", "mongodb"}, endpoint)
        for value in versions.values():
            _check_text(value, endpoint)
        isolation = _require_mapping(data["runtime_isolation"], endpoint)
        _require_keys(isolation, {"host_ports_published", "persistent_volumes", "spark_mode", "spark_shuffle_partitions"}, endpoint)
        for key in ("host_ports_published", "persistent_volumes"):
            _check_bool(isolation[key], endpoint)
        _check_text(isolation["spark_mode"], endpoint)
        _check_int(isolation["spark_shuffle_partitions"], endpoint, minimum=1)
        events = _require_mapping(data["events"], endpoint)
        _require_keys(events, {"planned", "acknowledged", "consumed", "invalid", "finalized_bundle_documents"}, endpoint)
        for value in events.values():
            _check_int(value, endpoint)
        windows = _require_list(data["windows"], endpoint, 2)
        if len(windows) != 2:
            raise ValueError("C8B window records are incomplete")
        for row, expected_name in zip(windows, ("A", "B"), strict=True):
            row = _require_mapping(row, endpoint)
            _require_keys(row, {"name", "start_utc", "end_utc", "watermark_utc", "microbatch_id", "row_count", "top1_product_id", "bundle_id", "content_sha256", "last_watermark_ack_to_bundle_visible_ms"}, endpoint)
            if row["name"] != expected_name:
                raise ValueError("C8B windows are out of order")
            for key in ("start_utc", "end_utc", "watermark_utc"):
                _validate_timestamp(row[key], endpoint)
            _check_int(row["microbatch_id"], endpoint)
            _check_int(row["row_count"], endpoint)
            _check_int(row["top1_product_id"], endpoint, minimum=1)
            _check_text(row["bundle_id"], endpoint)
            _check_text(row["content_sha256"], endpoint)
            _check_number(row["last_watermark_ack_to_bundle_visible_ms"], endpoint, minimum=0, nullable=True)
        durations = _require_mapping(data["spark_batch_durations_ms"], endpoint)
        _require_keys(durations, {"add_batch", "trigger_execution"}, endpoint)
        for sequence in durations.values():
            values = _require_list(sequence, endpoint, 100)
            for duration in values:
                _check_number(duration, endpoint, minimum=0)
        serving = _require_mapping(data["serving"], endpoint)
        _require_keys(serving, {"complete_rows_per_version", "pointer_advanced_to_window_b", "previous_version_rows_immutable", "repeat_b_status", "repeat_b_same_version_and_rows", "repeat_b_cli_wall_ms"}, endpoint)
        _check_int(serving["complete_rows_per_version"], endpoint)
        for key in ("pointer_advanced_to_window_b", "previous_version_rows_immutable", "repeat_b_same_version_and_rows"):
            _check_bool(serving[key], endpoint)
        _check_text(serving["repeat_b_status"], endpoint)
        _check_number(serving["repeat_b_cli_wall_ms"], endpoint, minimum=0)
        limitations = _require_list(data["limitations"], endpoint, 10)
        for limitation in limitations:
            _check_text(limitation, endpoint)
        if not any("checkpoint" in limitation.lower() and "not exercised" in limitation.lower() for limitation in limitations):
            raise ValueError("C8B checkpoint-restart limitation is missing")


def _validate_evidence(value: Any, evidence_id: str) -> dict[str, Any]:
    endpoint = f"evidence:{evidence_id}"
    body = _require_mapping(value, endpoint)
    _require_keys(body, {"evidence_id", "title", "kind", "provenance", "data", "observed_at"}, endpoint)
    if body["evidence_id"] != evidence_id:
        raise ValueError("evidence ID does not match the requested report")
    _check_text(body["title"], endpoint)
    _check_text(body["kind"], endpoint)
    _check_provenance(body["provenance"], endpoint)
    _validate_evidence_data(evidence_id, body["data"])
    _validate_timestamp(body["observed_at"], endpoint)
    return body


def _validate_response(endpoint: str, value: Any) -> dict[str, Any]:
    body = _require_mapping(value, endpoint)
    if endpoint == "streaming_metrics":
        from src.module3.metrics import validate_snapshot
        _require_keys(body, {"available", "source_namespace", "metrics", "observed_at"}, endpoint)
        _check_bool(body["available"], endpoint)
        _check_text(body["source_namespace"], endpoint)
        _validate_timestamp(body["observed_at"], endpoint)
        if body["available"] != (body["metrics"] is not None):
            raise ValueError("streaming metrics availability is inconsistent")
        if body["metrics"] is not None:
            validate_snapshot(body["metrics"])
            if body["metrics"]["source_namespace"] != body["source_namespace"]:
                raise ValueError("streaming metrics namespace is inconsistent")
    elif endpoint == "health":
        _require_keys(body, {"status"}, endpoint)
        if body["status"] != "ok":
            raise ValueError("health status is invalid")
    elif endpoint == "recommendations":
        _require_keys(
            body,
            {
                "user_id", "serving_version", "status", "recommendation_count", "mode", "reason",
                "source", "ranking_config", "generated_at", "recommendations", "observed_at",
            },
            endpoint,
        )
        if body["status"] != "ready":
            raise ValueError("recommendation identity is invalid")
        _check_int(body["user_id"], endpoint, minimum=1, maximum=MAX_USER_ID)
        for key in ("serving_version", "mode"):
            _check_text(body[key], endpoint)
        _check_text(body["reason"], endpoint, nullable=True)
        rows = _require_list(body["recommendations"], endpoint, 20)
        _check_int(body["recommendation_count"], endpoint, maximum=20)
        if body["recommendation_count"] != len(rows):
            raise ValueError("recommendation count does not match rows")
        product_ids = set()
        ranks = []
        for row in rows:
            row = _require_mapping(row, endpoint)
            _require_keys(
                row,
                {
                    "product_id", "product_name", "rank", "from_als", "from_trend", "als_score", "trend_score",
                    "normalized_als_score", "normalized_trend_score", "final_score",
                },
                endpoint,
            )
            _check_int(row["product_id"], endpoint, minimum=1)
            _check_int(row["rank"], endpoint, minimum=1, maximum=20)
            _check_text(row["product_name"], endpoint)
            _check_bool(row["from_als"], endpoint)
            _check_bool(row["from_trend"], endpoint)
            for key in ("als_score", "trend_score"):
                _check_number(row[key], endpoint, nullable=True)
            for key in ("normalized_als_score", "normalized_trend_score", "final_score"):
                _check_number(row[key], endpoint, minimum=0, maximum=1)
            product_ids.add(row["product_id"])
            ranks.append(row["rank"])
        if len(product_ids) != len(rows) or ranks != list(range(1, len(rows) + 1)):
            raise ValueError("recommendation ranks or products are inconsistent")
        source = _require_mapping(body["source"], endpoint)
        _require_keys(
            source,
            {
                "source_run_id", "model_version", "graph_version", "trend_source_namespace", "trend_source_scope",
                "trend_window_start", "trend_window_end", "trend_content_sha256", "as_of_event_time",
                "input_content_sha256", "pointer_updated_at",
            },
            endpoint,
        )
        for key in ("source_run_id", "model_version", "graph_version", "trend_source_namespace", "input_content_sha256"):
            _check_text(source[key], endpoint)
        for key in ("trend_source_scope", "trend_content_sha256"):
            _check_text(source[key], endpoint, nullable=True)
        for key in ("trend_window_start", "trend_window_end"):
            _validate_timestamp(source[key], endpoint, nullable=True)
        for key in ("as_of_event_time", "pointer_updated_at"):
            _validate_timestamp(source[key], endpoint)
        config = _require_mapping(body["ranking_config"], endpoint)
        _require_keys(
            config,
            {
                "algorithm_version", "lambda_requested", "lambda_effective", "als_top_n", "trend_top_m",
                "output_top_k", "epsilon", "ranking_config_sha256",
            },
            endpoint,
        )
        for key in ("algorithm_version", "ranking_config_sha256"):
            _check_text(config[key], endpoint)
        for key in ("lambda_requested", "lambda_effective"):
            _check_number(config[key], endpoint, minimum=0, maximum=1, nullable=True)
        for key in ("als_top_n", "trend_top_m", "output_top_k"):
            _check_int(config[key], endpoint, minimum=1, maximum=20)
        _check_number(config["epsilon"], endpoint, minimum=0)
        if config["epsilon"] <= 0:
            raise ValueError("recommendation epsilon must be positive")
        _validate_timestamp(body["observed_at"], endpoint)
        _validate_timestamp(body["generated_at"], endpoint, nullable=True)
    elif endpoint == "als_recommendations":
        _require_keys(
            body,
            {"user_id", "model_version", "status", "reason", "recommendation_count", "requested_limit", "has_more", "source", "recommendations", "observed_at"},
            endpoint,
        )
        _check_int(body["user_id"], endpoint, minimum=1, maximum=MAX_USER_ID)
        _check_text(body["model_version"], endpoint)
        if body["status"] not in {"available", "empty"}:
            raise ValueError("ALS status is invalid")
        _check_text(body["reason"], endpoint, nullable=True)
        _check_int(body["recommendation_count"], endpoint, maximum=20)
        _check_int(body["requested_limit"], endpoint, minimum=1, maximum=20)
        _check_bool(body["has_more"], endpoint)
        rows = _require_list(body["recommendations"], endpoint, 20)
        if body["recommendation_count"] != len(rows) or len(rows) > body["requested_limit"]:
            raise ValueError("ALS recommendation count does not match rows or requested limit")
        if (body["status"] == "empty") != (len(rows) == 0):
            raise ValueError("ALS empty status does not match the recommendation rows")
        product_ids = set()
        ranks = []
        previous_score = None
        for row in rows:
            row = _require_mapping(row, endpoint)
            _require_keys(row, {"product_id", "product_name", "als_rank", "als_score"}, endpoint)
            _check_int(row["product_id"], endpoint, minimum=1)
            _check_text(row["product_name"], endpoint, nullable=True)
            _check_int(row["als_rank"], endpoint, minimum=1, maximum=20)
            _check_number(row["als_score"], endpoint)
            if previous_score is not None and row["als_score"] > previous_score:
                raise ValueError("ALS rows are not ordered by rank and score")
            previous_score = row["als_score"]
            product_ids.add(row["product_id"])
            ranks.append(row["als_rank"])
        if len(product_ids) != len(rows) or ranks != list(range(1, len(rows) + 1)):
            raise ValueError("ALS recommendation ranks or products are inconsistent")
        source = _require_mapping(body["source"], endpoint)
        _require_keys(source, {"run_id", "model_version", "graph_version"}, endpoint)
        for key in ("run_id", "model_version", "graph_version"):
            _check_text(source[key], endpoint)
        if source["model_version"] != body["model_version"]:
            raise ValueError("ALS source model version does not match the response")
        _validate_timestamp(body["observed_at"], endpoint)
    elif endpoint == "trends":
        _require_keys(
            body,
            {
                "available", "status", "reason", "source_namespace", "source_scope", "window_start", "window_end",
                "published_at", "expires_at", "schema_version", "config_hash", "content_sha256", "row_count", "items",
                "observed_at",
            },
            endpoint,
        )
        if type(body["available"]) is not bool or body["status"] not in {"available", "missing", "expired", "stale"}:
            raise ValueError("trend availability is invalid")
        if body["available"] != (body["status"] == "available"):
            raise ValueError("trend availability and status disagree")
        _check_text(body["source_namespace"], endpoint)
        _check_text(body["source_scope"], endpoint, nullable=True)
        _check_text(body["reason"], endpoint, nullable=True)
        _check_text(body["config_hash"], endpoint, nullable=True)
        _check_text(body["content_sha256"], endpoint, nullable=True)
        if body["schema_version"] is not None:
            _check_int(body["schema_version"], endpoint, minimum=1)
        rows = _require_list(body["items"], endpoint, 20)
        _check_int(body["row_count"], endpoint, maximum=20)
        if body["row_count"] != len(rows):
            raise ValueError("trend row count does not match rows")
        _validate_timestamp(body["observed_at"], endpoint)
        for key in ("window_start", "window_end", "published_at", "expires_at"):
            _validate_timestamp(body[key], endpoint, nullable=True)
        for row in rows:
            row = _require_mapping(row, endpoint)
            _require_keys(row, {"product_id", "trend_score", "trend_rank"}, endpoint)
            _check_int(row["product_id"], endpoint, minimum=1)
            _check_int(row["trend_rank"], endpoint, minimum=1, maximum=20)
            _check_number(row["trend_score"], endpoint, minimum=0, maximum=1)
    elif endpoint in {"batch_products", "batch_departments"}:
        required = {"items", "source", "metric_definition", "observed_at"}
        if endpoint == "batch_products":
            required.add("order")
        _require_keys(body, required, endpoint)
        maximum = 50 if endpoint == "batch_products" else 21
        items = _require_list(body["items"], endpoint, maximum)
        source = _require_mapping(body["source"], endpoint)
        _require_keys(
            source,
            {"collection", "scope", "run_id", "last_batch_time", "atomic_with_other_collections"},
            endpoint,
        )
        _check_text(source["collection"], endpoint)
        _check_text(source["scope"], endpoint)
        _check_text(source["run_id"], endpoint, nullable=True)
        _validate_timestamp(source["last_batch_time"], endpoint, nullable=True)
        if source["atomic_with_other_collections"] is not False:
            raise ValueError("batch atomicity contract is invalid")
        definitions = _require_mapping(body["metric_definition"], endpoint)
        if not all(isinstance(key, str) and isinstance(item, str) for key, item in definitions.items()):
            raise ValueError("batch metric definitions are invalid")
        if endpoint == "batch_products":
            if body["order"] not in {"purchase_count", "reorder_rate"}:
                raise ValueError("batch product order is invalid")
        row_fields = (
            {"product_id", "purchase_count", "unique_users", "reorder_count", "reorder_rate", "run_id", "last_batch_time"}
            if endpoint == "batch_products"
            else {"department_id", "purchase_count", "unique_users", "reorder_rate", "run_id", "last_batch_time"}
        )
        for row in items:
            row = _require_mapping(row, endpoint)
            _require_keys(row, row_fields, endpoint)
            id_field = "product_id" if endpoint == "batch_products" else "department_id"
            _check_int(row[id_field], endpoint, minimum=1, maximum=None if endpoint == "batch_products" else 21)
            _check_int(row["purchase_count"], endpoint, minimum=1)
            _check_int(row["unique_users"], endpoint, minimum=1)
            if endpoint == "batch_products":
                _check_int(row["reorder_count"], endpoint)
            _check_number(row["reorder_rate"], endpoint, minimum=0, maximum=1)
            _check_text(row["run_id"], endpoint)
            _validate_timestamp(row["last_batch_time"], endpoint)
        _validate_timestamp(body["observed_at"], endpoint)
    elif endpoint == "evidence_registry":
        _require_keys(body, {"items", "observed_at"}, endpoint)
        items = _require_list(body["items"], endpoint, 20)
        for item in items:
            item = _require_mapping(item, endpoint)
            _require_keys(
                item,
                {"evidence_id", "title", "kind", "classification", "source_label", "available", "status", "sha256"},
                endpoint,
            )
            for key in ("evidence_id", "title", "kind", "classification", "source_label"):
                _check_text(item[key], endpoint)
            _check_bool(item["available"], endpoint)
            if item["status"] not in {"available", "missing", "invalid"}:
                raise ValueError("evidence status is invalid")
            if item["available"] != (item["status"] == "available"):
                raise ValueError("evidence availability and status disagree")
            _check_text(item["sha256"], endpoint, nullable=True)
        _validate_timestamp(body["observed_at"], endpoint)
    elif endpoint == "als_evaluation":
        return _validate_als_evaluation(value)
    elif endpoint.startswith("evidence:"):
        return _validate_evidence(value, endpoint.partition(":")[2])
    elif endpoint == "orders_distribution":
        _require_keys(
            body,
            {"scope", "measure", "total_orders", "orders_by_day_of_week", "orders_by_hour", "provenance", "observed_at"},
            endpoint,
        )
        if body["scope"] != "all orders" or body["measure"] != "order count, not item-level prior purchases":
            raise ValueError("order distribution scope is invalid")
        days = _require_list(body["orders_by_day_of_week"], endpoint, 7)
        hours = _require_list(body["orders_by_hour"], endpoint, 24)
        if len(days) != 7 or len(hours) != 24:
            raise ValueError("order distribution bins are incomplete")
        _check_int(body["total_orders"], endpoint, minimum=1)
        for rows, key, maximum, size in (
            (days, "order_dow", 6, 7),
            (hours, "order_hour_of_day", 23, 24),
        ):
            bins = []
            counts = []
            for row in rows:
                row = _require_mapping(row, endpoint)
                _require_keys(row, {key, "order_count"}, endpoint)
                _check_int(row[key], endpoint, maximum=maximum)
                _check_int(row["order_count"], endpoint)
                bins.append(row[key])
                counts.append(row["order_count"])
            if bins != list(range(size)) or sum(counts) != body["total_orders"]:
                raise ValueError("order distribution bins do not reconcile")
        provenance = _require_mapping(body["provenance"], endpoint)
        _require_keys(provenance, {"artifact_key", "source_label", "classification", "archived", "sha256"}, endpoint)
        for key in ("artifact_key", "source_label", "classification", "sha256"):
            _check_text(provenance[key], endpoint)
        _check_bool(provenance["archived"], endpoint)
        _validate_timestamp(body["observed_at"], endpoint)
    else:
        raise ValueError("unknown fixed endpoint")
    return body


class ApiClient:
    """Only exposes the fixed, bounded GET operations used by the MVP pages."""

    def __init__(
        self,
        base_url: str | None = None,
        *,
        session: requests.Session | None = None,
        timeout: tuple[float, float] = (CONNECT_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS),
        max_response_bytes: int = MAX_RESPONSE_BYTES,
    ) -> None:
        raw_url = base_url if base_url is not None else os.environ.get(
            "MODULE4D_API_BASE_URL", "http://127.0.0.1:8000"
        )
        self.base_url = validate_base_url(raw_url)
        if (
            not isinstance(timeout, tuple)
            or len(timeout) != 2
            or any(
                not isinstance(item, (int, float))
                or not math.isfinite(item)
                or not 0 < item <= 60
                for item in timeout
            )
        ):
            raise ApiClientError("Dashboard API timeout configuration is invalid.", code="invalid_api_configuration")
        if type(max_response_bytes) is not int or not 1 <= max_response_bytes <= MAX_RESPONSE_BYTES:
            raise ApiClientError("Dashboard API response limit configuration is invalid.", code="invalid_api_configuration")
        self.timeout = timeout
        self.max_response_bytes = max_response_bytes
        self.session = session or requests.Session()
        try:
            self.session.trust_env = False
        except AttributeError:
            pass

    def _read_response(self, response: requests.Response) -> bytes:
        length = response.headers.get("Content-Length")
        if length:
            try:
                declared_size = int(length)
            except (TypeError, ValueError) as exc:
                raise ApiClientError(
                    "The dashboard API returned an invalid response.",
                    status_code=response.status_code,
                    code="invalid_response",
                ) from exc
            if declared_size < 0 or declared_size > self.max_response_bytes:
                raise ApiClientError(
                    "The dashboard API response exceeded the size limit.",
                    status_code=response.status_code,
                    code="response_too_large",
                )
        chunks: list[bytes] = []
        total = 0
        for chunk in response.iter_content(chunk_size=64 * 1024):
            if not chunk:
                continue
            total += len(chunk)
            if total > self.max_response_bytes:
                raise ApiClientError(
                    "The dashboard API response exceeded the size limit.",
                    status_code=response.status_code,
                    code="response_too_large",
                )
            chunks.append(chunk)
        return b"".join(chunks)

    def _get(
        self,
        path: str,
        *,
        endpoint: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        try:
            response = self.session.get(
                url,
                params=params,
                headers={"Accept": "application/json"},
                timeout=self.timeout,
                allow_redirects=False,
                stream=True,
            )
        except requests.Timeout as exc:
            raise ApiClientError("The dashboard API did not respond before the timeout.", code="api_timeout") from exc
        except requests.RequestException as exc:
            raise ApiClientError("The dashboard API is unavailable.", code="api_unavailable") from exc
        try:
            status = response.status_code
            if 300 <= status < 400:
                raise ApiClientError(
                    "The dashboard API returned an unexpected redirect.",
                    status_code=status,
                    code="unexpected_redirect",
                )
            content_type = response.headers.get("Content-Type")
            if not _is_json_content_type(content_type):
                message = (
                    "The dashboard API returned a non-JSON error response."
                    if status >= 400
                    else "The dashboard API returned an unexpected response format."
                )
                raise ApiClientError(message, status_code=status, code="non_json_response")
            try:
                raw = self._read_response(response)
            except requests.Timeout as exc:
                raise ApiClientError(
                    "The dashboard API did not finish its response before the timeout.",
                    status_code=status,
                    code="api_timeout",
                ) from exc
            except requests.RequestException as exc:
                raise ApiClientError(
                    "The dashboard API response could not be read.",
                    status_code=status,
                    code="api_unavailable",
                ) from exc
            try:
                payload = _safe_json(raw)
            except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, ValueError) as exc:
                raise ApiClientError(
                    "The dashboard API returned invalid JSON.",
                    status_code=status,
                    code="invalid_json",
                ) from exc
            if status >= 400:
                return self._raise_backend_error(status, payload)
            if status != 200:
                raise ApiClientError(
                    "The dashboard API returned an unexpected status.", status_code=status, code="invalid_response"
                )
            try:
                return _validate_response(endpoint, payload)
            except (TypeError, ValueError, OverflowError) as exc:
                raise ApiClientError(
                    "The dashboard API response did not match its documented contract.",
                    status_code=status,
                    code="invalid_response",
                ) from exc
        finally:
            response.close()

    @staticmethod
    def _raise_backend_error(status: int, payload: Any) -> None:
        if isinstance(payload, dict) and "error" in payload:
            from src.module4.dashboard.schemas import ErrorEnvelope
            envelope = None
            try:
                envelope = ErrorEnvelope.model_validate(payload)
            except Exception:
                pass
            if envelope is not None:
                raise ApiClientError(
                    envelope.error.message[:512],
                    status_code=status,
                    code=envelope.error.code,
                    request_id=envelope.error.request_id[:128],
                )
        raise ApiClientError(
            "The dashboard API could not complete this request.", status_code=status, code="http_error"
        )

    def get_health(self) -> dict[str, Any]:
        return self._get("/health", endpoint="health")

    def get_recommendations(self, user_id: int | str) -> dict[str, Any]:
        validated = validate_user_id(user_id)
        payload = self._get(
            f"/api/v1/recommendations/{validated}", endpoint="recommendations"
        )
        if payload["user_id"] != validated:
            raise ApiClientError(
                "The dashboard API returned a response for a different user ID.",
                status_code=502,
                code="invalid_response",
            )
        return payload

    def get_als_recommendations(
        self, user_id: int | str, model_version: str | None = None, limit: int = 20
    ) -> dict[str, Any]:
        validated = validate_user_id(user_id)
        if type(limit) is not int or not 1 <= limit <= 20:
            raise ApiClientError("ALS recommendation limit must be between 1 and 20.", status_code=422, code="invalid_filter")
        params: dict[str, Any] = {"limit": limit}
        if model_version is not None:
            if (
                not isinstance(model_version, str)
                or not model_version
                or model_version != model_version.strip()
                or len(model_version) > 80
                or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,79}", model_version)
                or ".." in model_version
            ):
                raise ApiClientError("ALS model version has an invalid format.", status_code=422, code="invalid_model_version")
            params["model_version"] = model_version
        payload = self._get(
            f"/api/v1/als/recommendations/{validated}",
            endpoint="als_recommendations",
            params=params,
        )
        if payload["user_id"] != validated:
            raise ApiClientError(
                "The dashboard API returned a response for a different user ID.",
                status_code=502,
                code="invalid_response",
            )
        if model_version is not None and payload["model_version"] != model_version:
            raise ApiClientError(
                "The dashboard API returned a different ALS model version.",
                status_code=502,
                code="invalid_response",
            )
        return payload

    def get_trends(
        self,
        source_namespace: str,
        as_of_event_time: datetime | str,
        max_event_age_seconds: int | None = None,
    ) -> dict[str, Any]:
        namespace = validate_trend_namespace(source_namespace)
        as_of = validate_as_of_event_time(as_of_event_time)
        params: dict[str, Any] = {
            "source_namespace": namespace,
            "as_of_event_time": as_of,
        }
        if max_event_age_seconds is not None:
            if type(max_event_age_seconds) is not int or not 0 <= max_event_age_seconds <= MAX_TREND_AGE_SECONDS:
                raise ApiClientError(
                    "Maximum event age must be between 0 and 31,536,000 seconds.",
                    status_code=422,
                    code="invalid_filter",
                )
            params["max_event_age_seconds"] = max_event_age_seconds
        payload = self._get("/api/v1/trends", endpoint="trends", params=params)
        if payload["source_namespace"] != namespace:
            raise ApiClientError(
                "The dashboard API returned a different trend source namespace.",
                status_code=502,
                code="invalid_response",
            )
        return payload

    def get_streaming_metrics(self, source_namespace: str) -> dict[str, Any]:
        namespace = validate_trend_namespace(source_namespace)
        payload = self._get("/api/v1/streaming/metrics", endpoint="streaming_metrics", params={"source_namespace": namespace})
        if payload["source_namespace"] != namespace:
            raise ApiClientError("The API returned a different metrics namespace.", status_code=502, code="invalid_response")
        return payload

    def get_batch_products(self, order: str = "purchase_count", limit: int = 20) -> dict[str, Any]:
        if not isinstance(order, str) or order not in {"purchase_count", "reorder_rate"}:
            raise ApiClientError("Choose a supported batch ranking metric.", status_code=422, code="invalid_filter")
        if type(limit) is not int or not 1 <= limit <= 50:
            raise ApiClientError("Batch product limit must be between 1 and 50.", status_code=422, code="invalid_filter")
        payload = self._get(
            "/api/v1/batch/products",
            endpoint="batch_products",
            params={"order": order, "limit": limit},
        )
        if payload["order"] != order:
            raise ApiClientError(
                "The dashboard API returned a different batch ranking mode.",
                status_code=502,
                code="invalid_response",
            )
        return payload

    def get_batch_departments(self) -> dict[str, Any]:
        return self._get("/api/v1/batch/departments", endpoint="batch_departments")

    def get_evidence_registry(self) -> dict[str, Any]:
        return self._get("/api/v1/evidence", endpoint="evidence_registry")

    def get_als_evaluation(self) -> dict[str, Any]:
        return self._get("/api/v1/evaluation/als", endpoint="als_evaluation")

    def get_evidence(self, evidence_id: str) -> dict[str, Any]:
        if not isinstance(evidence_id, str) or evidence_id not in ALLOWED_EVIDENCE_IDS:
            raise ApiClientError(
                "Choose a registered evaluation or performance evidence report.",
                status_code=422,
                code="invalid_evidence_id",
            )
        return self._get(
            f"/api/v1/evidence/{evidence_id}",
            endpoint=f"evidence:{evidence_id}",
        )

    def get_orders_distribution(self) -> dict[str, Any]:
        return self._get("/api/v1/analytics/orders-distribution", endpoint="orders_distribution")
