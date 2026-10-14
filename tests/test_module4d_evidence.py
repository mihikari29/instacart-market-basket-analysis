"""Network-free validation tests for the fixed archived evidence registry."""

from __future__ import annotations

import json
from pathlib import Path
import shutil

import pytest

from src.module4d.evidence import (
    EVIDENCE_SPECS,
    InvalidEvidence,
    RegisteredEvidenceMissing,
    UnknownEvidence,
    evidence_registry,
    read_als_evaluation,
    read_evidence,
    read_orders_distribution,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def copy_artifact(root: Path, evidence_id: str) -> Path:
    relative = EVIDENCE_SPECS[evidence_id]["path"]
    target = root / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(REPOSITORY_ROOT / relative, target)
    return target


def test_registry_is_exact_bounded_allowlist_and_reports_safe_provenance():
    registry = evidence_registry(REPOSITORY_ROOT)
    assert [item["evidence_id"] for item in registry] == list(EVIDENCE_SPECS)
    assert all(item["available"] and item["status"] == "available" for item in registry)
    assert all(len(item["sha256"]) == 64 for item in registry)
    assert all("/home/" not in json.dumps(item) and ".json" not in json.dumps(item) for item in registry)


def test_unknown_id_and_path_traversal_never_select_a_filename():
    for value in ("../../etc/passwd", "docs/evidence/full/analytics.json", "unknown"):
        with pytest.raises(UnknownEvidence):
            read_evidence(REPOSITORY_ROOT, value)


def test_missing_registered_artifact_is_distinct_from_unknown_id(tmp_path):
    with pytest.raises(RegisteredEvidenceMissing):
        read_evidence(tmp_path, "module2-full-analytics")
    statuses = {row["evidence_id"]: row["status"] for row in evidence_registry(tmp_path)}
    assert statuses["module2-full-analytics"] == "missing"


def test_analytics_orders_distribution_has_complete_domains_and_all_order_scope():
    result = read_orders_distribution(REPOSITORY_ROOT)
    assert result["scope"] == "all orders"
    assert result["measure"] == "order count, not item-level prior purchases"
    assert result["total_orders"] == 3_421_083
    assert [row["order_dow"] for row in result["orders_by_day_of_week"]] == list(range(7))
    assert [row["order_hour_of_day"] for row in result["orders_by_hour"]] == list(range(24))
    assert sum(row["order_count"] for row in result["orders_by_day_of_week"]) == result["total_orders"]
    assert sum(row["order_count"] for row in result["orders_by_hour"]) == result["total_orders"]


def test_module2_analytics_preserves_prior_only_purchase_metrics_and_names_only_from_archive():
    result = read_evidence(REPOSITORY_ROOT, "module2-full-analytics")
    data = result["data"]
    assert data["scope"] == {"purchase_metrics": "prior only", "order_distributions": "all orders"}
    assert data["purchase_behavior"]["purchases"] == data["counts"]["prior"]
    assert data["top_purchased"][0]["product_name"] == "Banana"
    assert len(data["departments"]) == 21
    assert "module2-full-analytics" == result["evidence_id"]
    assert result["provenance"]["archived"] is True


def test_als_evaluation_preserves_nested_cohorts_k_metrics_and_baseline_values():
    result = read_als_evaluation(REPOSITORY_ROOT)
    assert result["ks"] == [5, 10, 20]
    assert set(result["cohorts"]) == {"all_official_train_users", "warm_users_and_all_truth_items"}
    assert set(result["history_subgroups"]) == {"new_to_user", "reordered"}
    cohort = result["cohorts"]["all_official_train_users"]
    als_recall = cohort["metrics"]["als"]["10"]["recall_at_k"]
    personal_recall = cohort["metrics"]["personal_frequency"]["10"]["recall_at_k"]
    assert als_recall == pytest.approx(0.12147208248298487)
    assert personal_recall == pytest.approx(0.31923221187877215)
    assert cohort["metrics"]["als"]["10"]["evaluated_users"] == 131_209
    assert "hybrid" not in json.dumps(result).lower()
    assert result["evaluation_method"]["als_training_method"].startswith("not recorded")


def test_benchmark_evidence_separates_join_pruning_cache_and_unavailable_shuffle():
    result = read_evidence(REPOSITORY_ROOT, "module2-full-hardening-benchmarks")["data"]
    assert result["metrics"]["join_strategy_medians_seconds"] == {
        "broadcast_hash_join": pytest.approx(2.304045968000082),
        "sort_merge_join": pytest.approx(17.581933944999946),
    }
    assert result["metrics"]["partition_scan_medians_seconds"]["partition_filter_rows"] == 177_739
    assert "cache_materialization" in result["metrics"]["cache_medians_seconds"]
    assert "cached_reuse" in result["metrics"]["cache_medians_seconds"]
    assert result["metrics"]["shuffle_metrics"]["available"] is False
    assert result["run"]["run_id"] == "20261003T080535_74b9d443"
    assert "Spark 3.5.5" in result["environment"]["resources"] or result["environment"]["spark"] == "3.5.5"


@pytest.mark.parametrize(
    ("evidence_id", "classification"),
    [
        ("module4c-c8a-responsiveness", "controlled synthetic responsiveness"),
        ("module4c-c8b-streaming-demo", "bounded synthetic Kafka/Spark demonstration"),
    ],
)
def test_c8_receipts_keep_synthetic_and_non_quality_provenance(evidence_id, classification):
    result = read_evidence(REPOSITORY_ROOT, evidence_id)
    serialized = json.dumps(result["data"])
    assert classification in result["data"]["classification"]
    assert "recommendation-quality" in result["data"]["classification"] or evidence_id.endswith("streaming-demo")
    assert result["provenance"]["archived"] is True
    if evidence_id.endswith("responsiveness"):
        assert result["data"]["recommendation_accuracy_measured"] is False
    else:
        assert "not live streaming" in result["data"]["classification"]
        assert "not Kafka lag" in result["data"]["interpretation"]
    assert "original_report" not in serialized
    assert "results/" not in serialized


def test_invalid_json_and_nonstandard_nan_are_rejected(tmp_path):
    target = copy_artifact(tmp_path, "module2-full-analytics")
    target.write_text("{not json", encoding="utf-8")
    with pytest.raises(InvalidEvidence):
        read_orders_distribution(tmp_path)

    original = json.loads((REPOSITORY_ROOT / EVIDENCE_SPECS["module2-full-analytics"]["path"]).read_text())
    original["purchase_behavior"]["reorder_rate"] = float("nan")
    target.write_text(json.dumps(original), encoding="utf-8")
    with pytest.raises(InvalidEvidence):
        read_evidence(tmp_path, "module2-full-analytics")


def test_invalid_order_distribution_bins_fail_schema_validation(tmp_path):
    target = copy_artifact(tmp_path, "module2-full-analytics")
    document = json.loads(target.read_text())
    document["orders_by_hour"][0]["order_hour_of_day"] = 23
    target.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(InvalidEvidence, match="duplicate bin"):
        read_orders_distribution(tmp_path)


def test_oversized_evidence_is_rejected_and_registry_marks_invalid(tmp_path):
    target = copy_artifact(tmp_path, "module2-full-analytics")
    target.write_bytes(b" " * (256 * 1024 + 1))
    with pytest.raises(InvalidEvidence, match="size limit"):
        read_orders_distribution(tmp_path)
    statuses = {row["evidence_id"]: row["status"] for row in evidence_registry(tmp_path)}
    assert statuses["module2-full-analytics"] == "invalid"


def test_symlink_escape_from_configured_application_root_is_rejected(tmp_path):
    evidence_id = "module2-full-analytics"
    target = tmp_path / EVIDENCE_SPECS[evidence_id]["path"]
    target.parent.mkdir(parents=True)
    target.symlink_to(REPOSITORY_ROOT / EVIDENCE_SPECS[evidence_id]["path"])
    with pytest.raises(InvalidEvidence, match="outside"):
        read_orders_distribution(tmp_path)


def test_registry_response_does_not_include_unselected_nested_raw_fields():
    body = read_evidence(REPOSITORY_ROOT, "module4c-c8b-streaming-demo")
    serialized = json.dumps(body)
    assert "historical_git_commit" not in serialized
    assert "original_report_sha256" not in serialized
    assert "historical_cleanup_interpretation" not in serialized
