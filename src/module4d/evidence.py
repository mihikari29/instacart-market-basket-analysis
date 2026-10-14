"""Fixed, size-bounded readers for curated archived Module 2/4 evidence."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
from numbers import Integral, Real
from pathlib import Path, PurePosixPath
from typing import Any


MAX_EVIDENCE_BYTES = 256 * 1024
EVIDENCE_SPECS = {
    "module2-full-analytics": {
        "path": "docs/evidence/full/analytics.json",
        "title": "Module 2 full public dataset analytics",
        "kind": "analytics",
        "classification": "full_public_dataset",
        "source_label": "Module 2 archived full-data analytics",
    },
    "module2-full-hardening-benchmarks": {
        "path": "docs/evidence/full/current-hardening-2026-10-03.json",
        "title": "Module 2 full-data hardening benchmark",
        "kind": "benchmarks",
        "classification": "full_public_dataset_isolated_runtime_validation",
        "source_label": "Module 2 archived benchmark run 2026-10-03",
    },
    "module4a-full-als-evaluation": {
        "path": "docs/evidence/module4/full-ab-20261010-fixed-first/evaluation.json",
        "title": "Module 4A full-data ALS and baseline evaluation",
        "kind": "als_evaluation",
        "classification": "official_train_evaluation",
        "source_label": "Module 4A archived official-train evaluation",
    },
    "module4c-c8a-responsiveness": {
        "path": "docs/evidence/module4c/c8a-receipt.json",
        "title": "Module 4C C8A controlled responsiveness receipt",
        "kind": "synthetic_responsiveness",
        "classification": "controlled_synthetic_experiment",
        "source_label": "Module 4C archived C8A synthetic responsiveness experiment",
    },
    "module4c-c8b-streaming-demo": {
        "path": "docs/evidence/module4c/c8b-receipt.json",
        "title": "Module 4C C8B bounded streaming demonstration receipt",
        "kind": "synthetic_streaming_demo",
        "classification": "bounded_synthetic_kafka_spark_demonstration",
        "source_label": "Module 4C archived C8B synthetic Kafka/Spark demonstration",
    },
}

_COUNT_KEYS = {
    "aisles",
    "departments",
    "interactions",
    "orders",
    "prior",
    "products",
    "train",
    "users",
}
_METRIC_KEYS = ("precision_at_k", "recall_at_k", "map_at_k", "ndcg_at_k")
_COVERAGE_KEYS = ("user_coverage", "catalog_coverage_rate")
_MODEL_KEYS = ("als", "global_popularity", "personal_frequency")
_EVALUATION_COHORTS = ("all_official_train_users", "warm_users_and_all_truth_items")
_HISTORY_COHORTS = ("new_to_user", "reordered")


class EvidenceError(RuntimeError):
    """Base class for public evidence endpoint failures."""


class UnknownEvidence(EvidenceError):
    """The caller supplied an evidence ID outside the fixed registry."""


class RegisteredEvidenceMissing(EvidenceError):
    """A known artifact is missing from this API runtime."""


class InvalidEvidence(EvidenceError):
    """A registered artifact failed size, JSON, path, or schema validation."""


class EvidenceReadFailure(EvidenceError):
    """The registered artifact could not be read by the API runtime."""


def _boolean(value: Any, field: str) -> bool:
    if type(value) is not bool:
        raise InvalidEvidence(f"{field} must be boolean")
    return value


@dataclass(frozen=True, slots=True)
class _Artifact:
    spec: dict[str, str]
    document: dict[str, Any]
    digest: str


def _integer(value: Any, field: str, *, minimum: int = 0, maximum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, Integral):
        raise InvalidEvidence(f"{field} must be an integer")
    result = int(value)
    if result < minimum or maximum is not None and result > maximum:
        raise InvalidEvidence(f"{field} is outside its allowed range")
    return result


def _number(
    value: Any,
    field: str,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise InvalidEvidence(f"{field} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise InvalidEvidence(f"{field} must be finite")
    if minimum is not None and result < minimum or maximum is not None and result > maximum:
        raise InvalidEvidence(f"{field} is outside its allowed range")
    return result


def _text(value: Any, field: str, *, maximum: int = 4096) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise InvalidEvidence(f"{field} must be a non-empty bounded string")
    return value


def _artifact(root: str | Path, evidence_id: str) -> _Artifact:
    if evidence_id not in EVIDENCE_SPECS:
        raise UnknownEvidence("evidence ID is not registered")
    spec = EVIDENCE_SPECS[evidence_id]
    try:
        app_root = Path(root).resolve()
        relative = PurePosixPath(spec["path"])
        candidate = app_root.joinpath(*relative.parts)
        resolved = candidate.resolve(strict=False)
    except (OSError, RuntimeError) as exc:
        raise InvalidEvidence("configured evidence root or registered artifact path is invalid") from exc
    if not resolved.is_relative_to(app_root):
        raise InvalidEvidence("registered artifact resolves outside the configured application root")
    try:
        size = resolved.stat().st_size
    except FileNotFoundError as exc:
        raise RegisteredEvidenceMissing("registered evidence is missing from this API runtime") from exc
    except OSError as exc:
        raise EvidenceReadFailure("registered evidence could not be inspected") from exc
    if not resolved.is_file():
        raise InvalidEvidence("registered evidence is not a regular file")
    if size > MAX_EVIDENCE_BYTES:
        raise InvalidEvidence("registered evidence exceeds the configured size limit")
    try:
        with resolved.open("rb") as handle:
            raw = handle.read(MAX_EVIDENCE_BYTES + 1)
    except FileNotFoundError as exc:
        raise RegisteredEvidenceMissing("registered evidence is missing from this API runtime") from exc
    except OSError as exc:
        raise EvidenceReadFailure("registered evidence could not be read") from exc
    if len(raw) > MAX_EVIDENCE_BYTES:
        raise InvalidEvidence("registered evidence exceeds the configured size limit")
    try:
        document = json.loads(
            raw.decode("utf-8"),
            parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)),
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError) as exc:
        raise InvalidEvidence("registered evidence is not valid finite JSON") from exc
    if not isinstance(document, dict):
        raise InvalidEvidence("registered evidence root must be a JSON object")
    return _Artifact(spec, document, hashlib.sha256(raw).hexdigest())


def _source(artifact: _Artifact) -> dict[str, Any]:
    return {
        "artifact_key": artifact.spec["kind"],
        "source_label": artifact.spec["source_label"],
        "classification": artifact.spec["classification"],
        "archived": True,
        "sha256": artifact.digest,
    }


def _orders_distribution(document: dict[str, Any]) -> dict[str, Any]:
    expected_scope = "purchase metrics and features: prior only; order distributions: all orders"
    if document.get("scope") != expected_scope:
        raise InvalidEvidence("analytics scope does not match the registered Module 2 contract")
    counts = document.get("counts")
    if not isinstance(counts, dict) or set(counts) != _COUNT_KEYS:
        raise InvalidEvidence("analytics counts do not match the registered schema")
    counts_out = {name: _integer(counts[name], f"counts.{name}") for name in sorted(_COUNT_KEYS)}
    total_orders = counts_out["orders"]
    if total_orders <= 0:
        raise InvalidEvidence("orders count must be positive")
    distributions = []
    for field, bin_field, expected_bins in (
        ("orders_by_dow", "order_dow", set(range(7))),
        ("orders_by_hour", "order_hour_of_day", set(range(24))),
    ):
        rows = document.get(field)
        if not isinstance(rows, list) or len(rows) != len(expected_bins):
            raise InvalidEvidence(f"{field} must contain every domain bin exactly once")
        parsed = []
        seen = set()
        for row in rows:
            if not isinstance(row, dict):
                raise InvalidEvidence(f"{field} row must be an object")
            bin_value = _integer(row.get(bin_field), f"{field}.{bin_field}", maximum=max(expected_bins))
            if bin_value not in expected_bins or bin_value in seen:
                raise InvalidEvidence(f"{field} contains an invalid or duplicate bin")
            seen.add(bin_value)
            parsed.append({bin_field: bin_value, "order_count": _integer(row.get("count"), f"{field}.count")})
        if seen != expected_bins or sum(row["order_count"] for row in parsed) != total_orders:
            raise InvalidEvidence(f"{field} does not reconcile to the all-orders total")
        parsed.sort(key=lambda row: row[bin_field])
        distributions.append(parsed)
    return {
        "scope": "all orders",
        "measure": "order count, not item-level prior purchases",
        "total_orders": total_orders,
        "orders_by_day_of_week": distributions[0],
        "orders_by_hour": distributions[1],
    }


def _product_report_rows(value: Any, field: str) -> list[dict[str, Any]]:
    if not isinstance(value, list) or len(value) > 20:
        raise InvalidEvidence(f"{field} must contain at most 20 product rows")
    output = []
    seen = set()
    for row in value:
        if not isinstance(row, dict):
            raise InvalidEvidence(f"{field} row must be an object")
        product_id = _integer(row.get("product_id"), f"{field}.product_id", minimum=1)
        purchase_count = _integer(row.get("purchase_count"), f"{field}.purchase_count", minimum=1)
        reorder_count = _integer(row.get("reorder_count"), f"{field}.reorder_count")
        unique_users = _integer(row.get("unique_users"), f"{field}.unique_users", minimum=1)
        rate = _number(row.get("reorder_rate"), f"{field}.reorder_rate", minimum=0, maximum=1)
        if product_id in seen or reorder_count > purchase_count or unique_users > purchase_count:
            raise InvalidEvidence(f"{field} contains inconsistent product metrics")
        if not math.isclose(rate, reorder_count / purchase_count, rel_tol=1e-9, abs_tol=1e-12):
            raise InvalidEvidence(f"{field} reorder_rate does not match its counts")
        seen.add(product_id)
        output.append(
            {
                "product_id": product_id,
                "product_name": _text(row.get("product_name"), f"{field}.product_name", maximum=256),
                "purchase_count": purchase_count,
                "unique_users": unique_users,
                "reorder_count": reorder_count,
                "reorder_rate": rate,
            }
        )
    return output


def _analytics_summary(document: dict[str, Any]) -> dict[str, Any]:
    distribution = _orders_distribution(document)
    counts = document["counts"]
    purchase = document.get("purchase_behavior")
    if not isinstance(purchase, dict):
        raise InvalidEvidence("purchase_behavior must be an object")
    purchases = _integer(purchase.get("purchases"), "purchase_behavior.purchases", minimum=1)
    if purchases != counts["prior"]:
        raise InvalidEvidence("purchase behavior does not reconcile to the prior-only row count")
    reorder_rate = _number(purchase.get("reorder_rate"), "purchase_behavior.reorder_rate", minimum=0, maximum=1)
    basket_size = _number(purchase.get("average_basket_size"), "purchase_behavior.average_basket_size", minimum=0)
    departments = document.get("departments")
    if not isinstance(departments, list) or len(departments) != 21:
        raise InvalidEvidence("department analytics must contain the 21 published departments")
    department_rows = []
    department_ids = set()
    for row in departments:
        if not isinstance(row, dict):
            raise InvalidEvidence("department analytics row must be an object")
        department_id = _integer(row.get("department_id"), "departments.department_id", minimum=1, maximum=21)
        if department_id in department_ids:
            raise InvalidEvidence("department analytics contains duplicate IDs")
        department_ids.add(department_id)
        department_rows.append(
            {
                "department_id": department_id,
                "department_name": _text(row.get("department"), "departments.department", maximum=128),
                "purchase_count": _integer(row.get("purchase_count"), "departments.purchase_count", minimum=1),
                "unique_users": _integer(row.get("unique_users"), "departments.unique_users", minimum=1),
                "reorder_rate": _number(row.get("reorder_rate"), "departments.reorder_rate", minimum=0, maximum=1),
            }
        )
    if department_ids != set(range(1, 22)):
        raise InvalidEvidence("department analytics does not cover IDs 1 through 21")
    support = _integer(document.get("minimum_reorder_support"), "minimum_reorder_support", minimum=1)
    top_reordered = _product_report_rows(document.get("top_reordered"), "top_reordered")
    if any(row["purchase_count"] < support for row in top_reordered):
        raise InvalidEvidence("top_reordered includes a product below minimum support")
    return {
        "scope": {
            "purchase_metrics": "prior only",
            "order_distributions": "all orders",
        },
        "counts": {name: _integer(counts[name], f"counts.{name}") for name in sorted(_COUNT_KEYS)},
        "purchase_behavior": {
            "purchases": purchases,
            "reorder_rate": reorder_rate,
            "average_basket_size": basket_size,
            "denominator": "prior order-product rows",
        },
        "minimum_reorder_support": support,
        "top_purchased": _product_report_rows(document.get("top_purchased"), "top_purchased"),
        "top_reordered": top_reordered,
        "departments": sorted(department_rows, key=lambda row: row["department_id"]),
        "order_distribution_scope": distribution["scope"],
    }


def _metric_record(record: Any, cohort: str, model: str, k: int) -> dict[str, Any]:
    if not isinstance(record, dict) or record.get("status") != "evaluated":
        raise InvalidEvidence("evaluation metric record is malformed or unavailable")
    if _integer(record.get("k"), "evaluation.k", minimum=1, maximum=100) != k:
        raise InvalidEvidence("evaluation K key does not match its metric record")
    if _integer(record.get("evaluated_users"), "evaluation.evaluated_users", minimum=1) <= 0:
        raise InvalidEvidence("evaluation must record evaluated_users")
    metrics = {
        name: _number(record.get(name), f"evaluation.{name}", minimum=0, maximum=1)
        for name in _METRIC_KEYS
    }
    output = {
        "k": k,
        "status": "evaluated",
        **metrics,
        "evaluated_users": int(record["evaluated_users"]),
    }
    for name in _COVERAGE_KEYS:
        output[name] = (
            _number(record[name], f"evaluation.{name}", minimum=0, maximum=1)
            if name in record
            else None
        )
    if "catalog_coverage" in record:
        output["catalog_coverage"] = _integer(record["catalog_coverage"], "evaluation.catalog_coverage")
    if "users_with_no_recommendations" in record:
        output["users_with_no_recommendations"] = _integer(
            record["users_with_no_recommendations"], "evaluation.users_with_no_recommendations"
        )
    return output


def _evaluation_group(value: Any, group_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise InvalidEvidence(f"{group_name} must be an object")
    expected = _EVALUATION_COHORTS if group_name == "cohorts" else _HISTORY_COHORTS
    if set(value) != set(expected):
        raise InvalidEvidence(f"{group_name} cohort IDs do not match the registered evaluation")
    result = {}
    for cohort_name in expected:
        cohort = value[cohort_name]
        if not isinstance(cohort, dict):
            raise InvalidEvidence("evaluation cohort must be an object")
        users = _integer(cohort.get("users"), f"{cohort_name}.users", minimum=1)
        definition = _text(cohort.get("definition"), f"{cohort_name}.definition") if "definition" in cohort else None
        metrics_in = cohort.get("metrics")
        if not isinstance(metrics_in, dict) or set(metrics_in) != set(_MODEL_KEYS):
            raise InvalidEvidence(f"{cohort_name} model metrics are incomplete")
        metrics_out = {}
        for model_name in _MODEL_KEYS:
            by_k = metrics_in[model_name]
            if not isinstance(by_k, dict) or set(by_k) != {"5", "10", "20"}:
                raise InvalidEvidence(f"{cohort_name}.{model_name} K metrics are incomplete")
            metrics_out[model_name] = {
                str(k): _metric_record(by_k[str(k)], cohort_name, model_name, k) for k in (5, 10, 20)
            }
        result[cohort_name] = {"definition": definition, "users": users, "metrics": metrics_out}
    return result


def _als_evaluation(document: dict[str, Any]) -> dict[str, Any]:
    ks = document.get("ks")
    if not isinstance(ks, list) or [_integer(k, "ks", minimum=1, maximum=100) for k in ks] != [5, 10, 20]:
        raise InvalidEvidence("evaluation K values do not match the registered schema")
    official_train_truth = _text(document.get("ground_truth"), "ground_truth")
    cohort_exclusion = _text(document.get("cohort_exclusion"), "cohort_exclusion")
    deltas_in = document.get("als_vs_baseline_deltas")
    if not isinstance(deltas_in, dict) or set(deltas_in) != set(_EVALUATION_COHORTS):
        raise InvalidEvidence("ALS baseline deltas do not match the primary cohort schema")
    deltas = {}
    for cohort in _EVALUATION_COHORTS:
        deltas[cohort] = {}
        for model in ("global_popularity", "personal_frequency"):
            by_k = deltas_in[cohort].get(model) if isinstance(deltas_in[cohort], dict) else None
            if not isinstance(by_k, dict) or set(by_k) != {"5", "10", "20"}:
                raise InvalidEvidence("ALS baseline deltas are incomplete")
            deltas[cohort][model] = {
                str(k): {
                    metric: _number(by_k[str(k)].get(metric), f"delta.{metric}")
                    for metric in _METRIC_KEYS
                }
                for k in (5, 10, 20)
            }
    fallback = _text(document.get("fallback"), "fallback")
    official_truth = _boolean(document.get("official_test_truth_available"), "official_test_truth_available")
    truth_summary = document.get("truth_summary")
    if not isinstance(truth_summary, dict):
        raise InvalidEvidence("truth_summary must be an object")
    return {
        "algorithm_scope": "ALS and recorded baselines",
        "evaluation_method": {
            "ground_truth": official_train_truth,
            "cohort_exclusion": cohort_exclusion,
            "fallback": fallback,
            "als_training_method": "not recorded in this allowlisted evaluation artifact",
        },
        "ks": [5, 10, 20],
        "cohorts": _evaluation_group(document.get("cohorts"), "cohorts"),
        "history_subgroups": _evaluation_group(document.get("history_subgroups"), "history_subgroups"),
        "als_vs_baseline_deltas": deltas,
        "official_test_truth_available": official_truth,
        "truth_summary": {
            key: _integer(truth_summary.get(key), f"truth_summary.{key}")
            for key in (
                "cold_truth_products",
                "truth_products",
                "truth_products_with_item_factors",
                "users",
                "users_with_mixed_warm_and_cold_truth",
                "users_with_user_factors",
                "users_without_user_factors",
            )
        },
    }


_BENCHMARK_FIELDS = (
    "sort_merge_join_median_seconds",
    "broadcast_hash_join_median_seconds",
    "full_scan_median_seconds",
    "date_filter_median_seconds",
    "partition_filter_median_seconds",
    "partition_filter_rows",
    "uncached_median_seconds",
    "cache_materialization_median_seconds",
    "cached_reuse_median_seconds",
)


def _benchmarks(document: dict[str, Any]) -> dict[str, Any]:
    bench_in = document.get("benchmarks")
    if not isinstance(bench_in, dict) or set(bench_in) != set(_BENCHMARK_FIELDS):
        raise InvalidEvidence("Module 2 benchmark fields do not match the registered schema")
    values = {
        name: _number(bench_in[name], f"benchmarks.{name}", minimum=0)
        for name in _BENCHMARK_FIELDS
        if name != "partition_filter_rows"
    }
    values["partition_filter_rows"] = _integer(bench_in["partition_filter_rows"], "partition_filter_rows")
    execution = document.get("execution")
    if not isinstance(execution, dict):
        raise InvalidEvidence("benchmark execution provenance is missing")
    module2 = document.get("module2")
    if not isinstance(module2, dict):
        raise InvalidEvidence("Module 2 benchmark source facts are missing")
    return {
        "run": {
            "run_id": _text(execution.get("run_id"), "execution.run_id", maximum=128),
            "timestamp_utc": _text(execution.get("timestamp_utc"), "execution.timestamp_utc", maximum=64),
            "dataset_classification": _text(document.get("classification"), "classification", maximum=256),
            "date_utc": _text(document.get("date_utc"), "date_utc", maximum=32),
        },
        "environment": {
            "platform": _text(execution.get("platform"), "execution.platform", maximum=256),
            "resources": _text(execution.get("resources"), "execution.resources", maximum=512),
            "spark": _text(execution.get("spark"), "execution.spark", maximum=64),
            "java": _text(execution.get("java"), "execution.java", maximum=64),
            "python": _text(execution.get("python"), "execution.python", maximum=64),
            "shuffle_partitions": _integer(execution.get("shuffle_partitions"), "shuffle_partitions", minimum=1),
            "warmups": _integer(execution.get("warmups"), "warmups"),
            "measured_repetitions": _integer(execution.get("measured_repetitions"), "measured_repetitions", minimum=1),
        },
        "metrics": {
            "join_strategy_medians_seconds": {
                "broadcast_hash_join": values["broadcast_hash_join_median_seconds"],
                "sort_merge_join": values["sort_merge_join_median_seconds"],
            },
            "partition_scan_medians_seconds": {
                "full_scan": values["full_scan_median_seconds"],
                "date_filter": values["date_filter_median_seconds"],
                "partition_filter": values["partition_filter_median_seconds"],
                "partition_filter_rows": values["partition_filter_rows"],
            },
            "cache_medians_seconds": {
                "uncached": values["uncached_median_seconds"],
                "cache_materialization": values["cache_materialization_median_seconds"],
                "cached_reuse": values["cached_reuse_median_seconds"],
            },
            "shuffle_metrics": {
                "available": False,
                "reason": "No structured shuffle metrics are recorded in this selected artifact.",
            },
        },
        "method_notes": {
            "partition_pruning": "Full scan has a different scope; date and partition filters are paired benchmark arms.",
            "cache": "Materialization cost and cached reuse are separate values.",
            "runs_are_comparable": True,
        },
        "module2_validation": {
            "prior_only_analytics": module2.get("prior_only_analytics") is True,
            "mongo_published": module2.get("mongo_published") is True,
            "published_product_documents": _integer(module2.get("mongo_batch_product_metrics"), "mongo_batch_product_metrics"),
            "published_department_documents": _integer(module2.get("mongo_department_metrics"), "mongo_department_metrics"),
        },
    }


def _c8a(document: dict[str, Any]) -> dict[str, Any]:
    response = document.get("ranking_response")
    if not isinstance(response, list) or len(response) != 5:
        raise InvalidEvidence("C8A ranking response schema is invalid")
    ranking = []
    for row in response:
        if not isinstance(row, dict):
            raise InvalidEvidence("C8A ranking row must be an object")
        ranking.append(
            {
                "lambda": _number(row.get("lambda"), "ranking_response.lambda", minimum=0, maximum=1),
                "baseline_top1": _text(row.get("baseline_top1"), "baseline_top1", maximum=64),
                "changed_top1": _text(row.get("changed_top1"), "changed_top1", maximum=64),
                "rank_changed_products": _integer(row.get("rank_changed_products"), "rank_changed_products"),
                "mean_absolute_rank_displacement": _number(
                    row.get("mean_absolute_rank_displacement"), "mean_absolute_rank_displacement", minimum=0
                ),
            }
        )
    timing = document.get("lambda_0_5_first_publication_cli_wall_ms")
    if not isinstance(timing, dict):
        raise InvalidEvidence("C8A timing schema is invalid")
    timings = {}
    for variant in ("baseline", "changed"):
        block = timing.get(variant)
        if not isinstance(block, dict):
            raise InvalidEvidence("C8A timing variant is missing")
        timings[variant] = {
            "repetitions": _integer(block.get("repetitions"), f"{variant}.repetitions", minimum=1),
            "median_ms": _number(block.get("median"), f"{variant}.median", minimum=0),
            "min_ms": _number(block.get("min"), f"{variant}.min", minimum=0),
            "max_ms": _number(block.get("max"), f"{variant}.max", minimum=0),
        }
    if document.get("recommendation_accuracy_measured") is not False:
        raise InvalidEvidence("C8A must explicitly state that recommendation accuracy was not measured")
    if document.get("dataset_type") != "controlled_synthetic":
        raise InvalidEvidence("C8A dataset classification is invalid")
    limitations = document.get("limitations")
    if not isinstance(limitations, list) or len(limitations) > 10:
        raise InvalidEvidence("C8A limitations must be a bounded list")
    return {
        "experiment_id": _text(document.get("experiment_id"), "experiment_id", maximum=128),
        "status": _text(document.get("status"), "status", maximum=32),
        "classification": "controlled synthetic responsiveness; not recommendation-quality evidence",
        "top20_overlap": _text(document.get("top20_overlap"), "top20_overlap", maximum=128),
        "ranking_response": ranking,
        "first_publication_cli_wall_ms_at_lambda_0_5": {
            **timings,
            "warmups": _integer(timing.get("warmups"), "warmups"),
            "includes_process_startup_and_mongodb_serving": _boolean(
                timing.get("includes_process_startup_and_mongodb_serving"),
                "includes_process_startup_and_mongodb_serving",
            ),
            "excludes_fixture_and_bundle_setup": _boolean(
                timing.get("excludes_fixture_and_bundle_setup"), "excludes_fixture_and_bundle_setup"
            ),
            "not_kafka_to_serving_latency": _boolean(
                timing.get("not_kafka_to_serving_latency"), "not_kafka_to_serving_latency"
            ),
        },
        "recommendation_accuracy_measured": False,
        "limitations": [_text(item, "limitations", maximum=512) for item in limitations],
    }


def _c8b(document: dict[str, Any]) -> dict[str, Any]:
    events = document.get("events")
    serving = document.get("serving")
    windows = document.get("windows")
    versions = document.get("runtime_versions")
    isolation = document.get("runtime_isolation")
    durations = document.get("spark_batch_durations_ms")
    if not all(isinstance(value, dict) for value in (events, serving, versions, isolation, durations)):
        raise InvalidEvidence("C8B evidence sections are malformed")
    window_rows = []
    if not isinstance(windows, list) or len(windows) != 2:
        raise InvalidEvidence("C8B must contain its two recorded windows")
    for expected_name, row in zip(("A", "B"), windows, strict=True):
        if not isinstance(row, dict) or row.get("name") != expected_name:
            raise InvalidEvidence("C8B window record is missing or out of order")
        window_rows.append(
            {
                "name": expected_name,
                "start_utc": _text(row.get("start_utc"), "window.start_utc", maximum=64),
                "end_utc": _text(row.get("end_utc"), "window.end_utc", maximum=64),
                "watermark_utc": _text(row.get("watermark_utc"), "window.watermark_utc", maximum=64),
                "microbatch_id": _integer(row.get("microbatch_id"), "window.microbatch_id"),
                "row_count": _integer(row.get("row_count"), "window.row_count"),
                "top1_product_id": _integer(row.get("top1_product_id"), "window.top1_product_id", minimum=1),
                "bundle_id": _text(row.get("bundle_id"), "window.bundle_id", maximum=256),
                "content_sha256": _text(row.get("content_sha256"), "window.content_sha256", maximum=64),
                "last_watermark_ack_to_bundle_visible_ms": (
                    _number(row["last_watermark_ack_to_bundle_visible_ms"], "window.ack_to_visible", minimum=0)
                    if "last_watermark_ack_to_bundle_visible_ms" in row
                    else None
                ),
            }
        )
    if document.get("dataset_type") != "controlled_synthetic":
        raise InvalidEvidence("C8B dataset classification is invalid")
    limitations = document.get("limitations")
    if not isinstance(limitations, list) or len(limitations) > 10:
        raise InvalidEvidence("C8B limitations must be a bounded list")
    return {
        "experiment_id": _text(document.get("experiment_id"), "experiment_id", maximum=128),
        "status": _text(document.get("status"), "status", maximum=32),
        "classification": "bounded synthetic Kafka/Spark demonstration; archived evidence, not live streaming",
        "runtime_versions": {
            name: _text(versions.get(name), f"runtime_versions.{name}", maximum=64)
            for name in ("kafka", "spark", "mongodb")
        },
        "runtime_isolation": {
            "host_ports_published": _boolean(isolation.get("host_ports_published"), "host_ports_published"),
            "persistent_volumes": _boolean(isolation.get("persistent_volumes"), "persistent_volumes"),
            "spark_mode": _text(isolation.get("spark_mode"), "spark_mode", maximum=32),
            "spark_shuffle_partitions": _integer(isolation.get("spark_shuffle_partitions"), "spark_shuffle_partitions", minimum=1),
        },
        "events": {
            key: _integer(events.get(key), f"events.{key}")
            for key in ("planned", "acknowledged", "consumed", "invalid", "finalized_bundle_documents")
        },
        "windows": window_rows,
        "spark_batch_durations_ms": {
            name: [
                _number(value, f"spark_batch_durations_ms.{name}", minimum=0)
                for value in durations.get(name, [])
            ]
            for name in ("add_batch", "trigger_execution")
        },
        "serving": {
            "complete_rows_per_version": _integer(serving.get("complete_rows_per_version"), "complete_rows_per_version"),
            "pointer_advanced_to_window_b": _boolean(
                serving.get("pointer_advanced_to_window_b"), "pointer_advanced_to_window_b"
            ),
            "previous_version_rows_immutable": _boolean(
                serving.get("previous_version_rows_immutable"), "previous_version_rows_immutable"
            ),
            "repeat_b_status": _text(serving.get("repeat_b_status"), "repeat_b_status", maximum=64),
            "repeat_b_same_version_and_rows": _boolean(
                serving.get("repeat_b_same_version_and_rows"), "repeat_b_same_version_and_rows"
            ),
            "repeat_b_cli_wall_ms": _number(serving.get("repeat_b_cli_wall_ms"), "repeat_b_cli_wall_ms", minimum=0),
        },
        "limitations": [_text(item, "limitations", maximum=512) for item in limitations],
        "interpretation": "The named acknowledgement-to-bundle-visible interval is a bounded synthetic boundary measurement, not Kafka lag or generic end-to-end latency.",
    }


def _curated(evidence_id: str, artifact: _Artifact) -> dict[str, Any]:
    try:
        if evidence_id == "module2-full-analytics":
            return _analytics_summary(artifact.document)
        if evidence_id == "module2-full-hardening-benchmarks":
            return _benchmarks(artifact.document)
        if evidence_id == "module4a-full-als-evaluation":
            return _als_evaluation(artifact.document)
        if evidence_id == "module4c-c8a-responsiveness":
            return _c8a(artifact.document)
        if evidence_id == "module4c-c8b-streaming-demo":
            return _c8b(artifact.document)
        raise UnknownEvidence("evidence ID is not registered")
    except EvidenceError:
        raise
    except (AttributeError, IndexError, KeyError, OverflowError, TypeError, ValueError) as exc:
        raise InvalidEvidence("registered evidence does not match its schema") from exc


def read_evidence(root: str | Path, evidence_id: str) -> dict[str, Any]:
    """Return a curated validated projection of one fixed evidence artifact."""
    artifact = _artifact(root, evidence_id)
    return {
        "evidence_id": evidence_id,
        "title": artifact.spec["title"],
        "kind": artifact.spec["kind"],
        "provenance": _source(artifact),
        "data": _curated(evidence_id, artifact),
    }


def read_orders_distribution(root: str | Path) -> dict[str, Any]:
    """Return only validated all-order day and hour bins from Module 2 analytics."""
    evidence_id = "module2-full-analytics"
    artifact = _artifact(root, evidence_id)
    try:
        distribution = _orders_distribution(artifact.document)
    except EvidenceError:
        raise
    except (AttributeError, IndexError, KeyError, OverflowError, TypeError, ValueError) as exc:
        raise InvalidEvidence("registered evidence does not match its schema") from exc
    return {
        **distribution,
        "provenance": _source(artifact),
    }


def read_als_evaluation(root: str | Path) -> dict[str, Any]:
    """Return the nested recorded ALS/baseline cohorts without recalculation."""
    evidence_id = "module4a-full-als-evaluation"
    artifact = _artifact(root, evidence_id)
    return {
        "evidence_id": evidence_id,
        "title": artifact.spec["title"],
        "provenance": _source(artifact),
        **_curated(evidence_id, artifact),
    }


def evidence_registry(root: str | Path) -> list[dict[str, Any]]:
    """Report availability and safe provenance for each fixed registry entry."""
    registry = []
    for evidence_id, spec in EVIDENCE_SPECS.items():
        item: dict[str, Any] = {
            "evidence_id": evidence_id,
            "title": spec["title"],
            "kind": spec["kind"],
            "classification": spec["classification"],
            "source_label": spec["source_label"],
            "available": False,
            "status": "invalid",
            "sha256": None,
        }
        try:
            artifact = _artifact(root, evidence_id)
            _curated(evidence_id, artifact)
        except RegisteredEvidenceMissing:
            item["status"] = "missing"
        except (InvalidEvidence, EvidenceReadFailure):
            item["status"] = "invalid"
        else:
            item.update(available=True, status="available", sha256=artifact.digest)
        registry.append(item)
    return registry
