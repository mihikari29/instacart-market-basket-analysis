"""Pure contract tests for the C8A controlled responsiveness experiment."""

from __future__ import annotations

from datetime import timedelta
import hashlib
import math

import pytest

from scripts.module4.module4c_responsiveness import (
    BASE_END,
    CHANGED_END,
    SOURCE_NAMESPACE,
    USER_ID,
    build_cli_command,
    calculate_rank_metrics,
    independent_oracle,
    redact_secrets,
    sha256_json,
    _git_state,
    validate_disposable_database_name,
    validate_mongodb_target,
    validate_report,
)
from src.module4.hybrid.config import (
    ALSRecommendation,
    HybridRankingConfig,
    ProductMetadata,
    TrendRecommendation,
    VerifiedSourceContext,
)
from src.module4.hybrid.data import CapturedUserSources
from src.module4.hybrid.ranking import rank_hybrid_recommendations
from src.module4.hybrid.sink_mongo import prepare_hybrid_snapshot
from src.module4.hybrid.trend_reader import TrendSnapshotReadResult


def _identity(content_token: str, trend_scores: dict[int, float]):
    source_context = VerifiedSourceContext(als_available=True, graph_available=True)
    als = tuple(
        ALSRecommendation(
            user_id=USER_ID,
            product_id=product_id,
            als_score=3.0 if product_id == 1 else 2.0 if product_id == 2 else 1.0,
            als_rank=product_id,
        )
        for product_id in range(1, 21)
    )
    catalog = tuple(
        ProductMetadata(product_id, f"Fixture product {product_id}")
        for product_id in range(1, 21)
    )
    captured = CapturedUserSources(
        user_id=USER_ID,
        source_context=source_context,
        als_recommendations=als,
        product_catalog=catalog,
        missing_prospective_trend_product_ids=(),
        run_id="c8a-fixed-source-run-v1",
        model_version="c8a-fixed-als-model-v1",
        graph_version="c8a-fixed-graph-v1",
    )
    ordered_scores = sorted(
        trend_scores.items(), key=lambda item: (-item[1], item[0])
    )
    trend_rows = tuple(
        TrendRecommendation(product_id, score, rank)
        for rank, (product_id, score) in enumerate(ordered_scores, start=1)
    )
    content_sha = hashlib.sha256(content_token.encode("utf-8")).hexdigest()
    trend_result = TrendSnapshotReadResult(
        available=True,
        status="available",
        reason=None,
        source_namespace=SOURCE_NAMESPACE,
        source_scope="synthetic_controlled_fixture",
        window_start=BASE_END - timedelta(hours=2),
        window_end=BASE_END,
        published_at=BASE_END,
        expires_at=None,
        schema_version=1,
        config_hash=hashlib.sha256(b"c8a config").hexdigest(),
        content_sha256=content_sha,
        trend_recommendations=trend_rows,
    )
    config = HybridRankingConfig(lambda_weight=0.5)
    ranking = rank_hybrid_recommendations(
        USER_ID,
        captured.als_recommendations,
        trend_result.trend_snapshot,
        captured.product_catalog,
        captured.source_context,
        config,
    )
    prepared = prepare_hybrid_snapshot(
        captured,
        trend_result,
        ranking,
        config,
        as_of_event_time=BASE_END,
    )
    return prepared, ranking


def test_independent_oracle_scores_and_expected_ranks():
    baseline = independent_oracle(0.5, changed=False)
    changed = independent_oracle(0.5, changed=True)
    assert math.isclose(baseline[1], 0.5 * (2 / (2 + 1e-12)) + 0.5 * (1 / (1 + 1e-12)))
    assert math.isclose(changed[1], 0.5 * (2 / (2 + 1e-12)) + 0.5 * ((10 / 99) / (1 + 1e-12)))
    assert math.isclose(changed[2], 0.5 * (1 / (2 + 1e-12)) + 0.5 * (1 / (1 + 1e-12)))
    baseline_order = sorted(baseline, key=lambda product_id: (-baseline[product_id], product_id))
    changed_order = sorted(changed, key=lambda product_id: (-changed[product_id], product_id))
    assert baseline_order[0] == 1
    assert changed_order[:2] == [2, 1]


def test_lambda_endpoints_preserve_trend_only_and_als_only_behavior():
    trend_only = independent_oracle(0.0, changed=True)
    als_only = independent_oracle(1.0, changed=True)
    assert max(trend_only, key=lambda product_id: (trend_only[product_id], -product_id)) == 2
    assert max(als_only, key=lambda product_id: (als_only[product_id], -product_id)) == 1


def test_top_k_overlap_jaccard_and_absent_product_rank_sentinel():
    metrics = calculate_rank_metrics([1, 2, 3], [1, 4, 3], comparison_k=3)
    assert metrics["top_k_overlap_count"] == 2
    assert metrics["top_k_jaccard_similarity"] == 0.5
    assert metrics["candidate_universe"] == [1, 2, 3, 4]
    assert metrics["missing_product_rank_sentinel"] == 4
    assert metrics["rank_changed_product_count"] == 2
    assert metrics["mean_absolute_rank_displacement"] == 1.0


def test_rank_metrics_are_stable_for_identical_inputs():
    first = calculate_rank_metrics([1, 2, 3], [1, 2, 3], comparison_k=3)
    second = calculate_rank_metrics(tuple([1, 2, 3]), tuple([1, 2, 3]), comparison_k=3)
    assert first == second
    assert sha256_json(first) == sha256_json(second)


def test_identical_inputs_have_deterministic_serving_identity():
    trend_scores = {1: 1.0, **{product_id: 0.0 for product_id in range(2, 21)}}
    first, _ranking = _identity("same-content", trend_scores)
    retry, _retry_ranking = _identity("same-content", trend_scores)
    assert first.serving_version == retry.serving_version
    assert first.fingerprint_sha256 == retry.fingerprint_sha256


def test_changed_trend_content_changes_serving_identity_and_rank():
    baseline_scores = {1: 1.0, **{product_id: 0.0 for product_id in range(2, 21)}}
    changed_scores = {1: 10.0 / 99.0, 2: 1.0, **{product_id: 0.0 for product_id in range(3, 21)}}
    baseline, _baseline_ranking = _identity("baseline-content", baseline_scores)
    changed, changed_ranking = _identity("changed-content", changed_scores)
    assert baseline.serving_version != changed.serving_version
    assert changed.rows[0].product_id == 2
    assert changed.as_of_event_time == BASE_END
    assert CHANGED_END > BASE_END


def test_report_schema_and_finite_numeric_validation():
    report = {
        "experiment_id": "c8a-unit-test-0001",
        "status": "passed",
        "git_commit": "a" * 40,
        "git_dirty": True,
        "experiment_type": "synthetic_finalized_window_responsiveness",
        "dataset_type": "synthetic fixture",
        "source_scope": "synthetic_controlled_fixture",
        "source_namespace": SOURCE_NAMESPACE,
        "user_id": USER_ID,
        "recommendation_accuracy_measured": False,
        "lambda_values": [0.0, 0.5, 1.0],
        "ranking_config": {"epsilon": 1e-12},
        "window_definitions": {"window_end": CHANGED_END.isoformat()},
        "fixture_checksums": {"sha256": "b" * 64},
        "results_by_lambda": {"0.5": {"jaccard": 1.0}},
        "responsiveness_metrics": [],
        "cli_timing_summary": {"median_ms": 12.5},
        "runtime_environment": {"client_target": "mongodb://c8a-mongo:27017"},
        "limitations": ["No recommendation accuracy was measured."],
    }
    validate_report(report)
    report["cli_timing_summary"]["median_ms"] = math.nan
    with pytest.raises(ValueError, match="non-finite"):
        validate_report(report)


def test_credential_redaction_covers_mongodb_uri_and_inline_secret():
    redacted = redact_secrets(
        "connect mongodb://fixture:topsecret@c8a-mongo:27017?authSource=admin password=othersecret"
    )
    assert "topsecret" not in redacted
    assert "othersecret" not in redacted
    assert "mongodb://<redacted>" in redacted
    assert "password=<redacted>" in redacted


@pytest.mark.parametrize("database_name", ["instacart", "admin", "c8a", "i3_module4c"])
def test_production_or_non_owned_database_names_are_refused(database_name):
    with pytest.raises(ValueError, match="disposable database"):
        validate_disposable_database_name(database_name)


@pytest.mark.parametrize(
    "uri",
    [
        "mongodb://localhost:27017",
        "mongodb://127.0.0.1:27017",
        "mongodb://fixture:secret@c8a-mongo:27017",
        "mongodb://c8a-mongo:27017/instacart",
        "mongodb://c8a-mongo:27017/?authSource=admin",
    ],
)
def test_mongodb_target_guard_refuses_non_isolated_or_credentialed_targets(uri):
    with pytest.raises(ValueError):
        validate_mongodb_target(uri)


def test_cli_command_has_explicit_as_of_and_no_implicit_freshness_threshold():
    command = build_cli_command(
        python_executable="python3",
        mongodb_uri="mongodb://c8a-mongo:27017",
        database_name="i3_c8a_c8a_test_lambda_05_abcdef12",
        lambda_weight=0.5,
        as_of_event_time=BASE_END,
    )
    assert command[command.index("--as-of-event-time") + 1].endswith("Z")
    assert "--max-event-age-seconds" not in command


def test_report_cannot_claim_official_train_quality_evaluation():
    report = {
        "experiment_id": "c8a-unit-test-0001",
        "status": "passed",
        "git_commit": "a" * 40,
        "git_dirty": False,
        "experiment_type": "synthetic_finalized_window_responsiveness",
        "dataset_type": "synthetic fixture",
        "source_scope": "synthetic_controlled_fixture",
        "source_namespace": SOURCE_NAMESPACE,
        "user_id": USER_ID,
        "recommendation_accuracy_measured": False,
        "lambda_values": [],
        "ranking_config": {},
        "window_definitions": {},
        "fixture_checksums": {},
        "results_by_lambda": {},
        "responsiveness_metrics": [],
        "cli_timing_summary": {},
        "runtime_environment": {},
        "limitations": ["No official-train recommendation evaluation was run."],
    }
    validate_report(report)
    report["recommendation_accuracy_measured"] = True
    with pytest.raises(ValueError, match="accuracy was not measured"):
        validate_report(report)


def test_git_metadata_uses_verified_launcher_values_when_pinned_image_has_no_git(
    monkeypatch,
):
    monkeypatch.setattr(
        "scripts.module4.module4c_responsiveness.subprocess.run",
        lambda *args, **kwargs: (_ for _ in ()).throw(FileNotFoundError("git")),
    )
    monkeypatch.setenv("C8A_GIT_COMMIT", "a" * 40)
    monkeypatch.setenv("C8A_GIT_STATUS_JSON", '["?? scripts/module4/module4c_responsiveness.py"]')
    commit, dirty, status = _git_state()
    assert commit == "a" * 40
    assert dirty is True
    assert status == ["?? scripts/module4/module4c_responsiveness.py"]
