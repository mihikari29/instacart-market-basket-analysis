"""Single-user orchestration tests for the Module 4C serving path."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import inspect

import pytest

from src.module4.config import Config as Module4Config
from src.module4.hybrid import service as service_module
from src.module4.hybrid.service import HybridServingPublicationError
from src.module4.hybrid.config import (
    ALSRecommendation,
    HybridRankingConfig,
    ProductMetadata,
    TrendRecommendation,
    VerifiedSourceContext,
)
from src.module4.hybrid.data import CapturedUserSources, ReadinessContractError
from src.module4.hybrid.ranking import RankingContractError
from src.module4.hybrid.sink_mongo import (
    HybridPublicationConflictError,
    HybridPublicationResult,
    PreparedHybridSnapshot,
)
from src.module4.hybrid.trend_reader import (
    TrendSnapshotIntegrityError,
    TrendSnapshotQueryError,
    TrendSnapshotReadResult,
)


USER_ID = 701
NAMESPACE = "fixture:module3:trending:v1"
AS_OF = datetime(2025, 2, 3, 12, 0, tzinfo=timezone.utc)
PROCESSING_TIME = datetime(2025, 2, 3, 12, 5, tzinfo=timezone.utc)
WINDOW_START = AS_OF - timedelta(hours=2)
WINDOW_END = AS_OF - timedelta(hours=1)
RUN_ID = "service-run-001"
MODEL_VERSION = "service-als-v1"
GRAPH_VERSION = "service-graph-v1"


class FakeTrendCollection:
    name = "realtime_trending_windows"


def _config() -> Module4Config:
    return Module4Config(mongodb_uri="mongodb://fake.invalid:27017/fixture")


def _trend(
    *,
    status="available",
    available=True,
    reason=None,
    namespace=NAMESPACE,
    specs=((2, 0.8), (3, 0.4)),
    digest="a" * 64,
):
    unavailable = not available
    return TrendSnapshotReadResult(
        available=available,
        status=status,
        reason=reason,
        source_namespace=namespace,
        source_scope="fixture_scope",
        window_start=WINDOW_START,
        window_end=WINDOW_END,
        published_at=WINDOW_END + timedelta(minutes=1),
        expires_at=(PROCESSING_TIME - timedelta(seconds=1)) if status == "expired" else None,
        schema_version=None if unavailable and status == "missing" else 1,
        config_hash=None if unavailable and status == "missing" else "b" * 64,
        content_sha256=None if unavailable and status == "missing" else digest,
        trend_recommendations=(
            tuple(
                TrendRecommendation(product_id, score, rank)
                for rank, (product_id, score) in enumerate(specs, start=1)
            )
            if available
            else ()
        ),
    )


def _captured(
    user_id=USER_ID,
    *,
    als_rows=((1, 10.0), (2, 1.0)),
    catalog_ids=(1, 2, 3, 4),
    missing_trend_ids=(),
):
    return CapturedUserSources(
        user_id=user_id,
        source_context=VerifiedSourceContext(als_available=True, graph_available=True),
        als_recommendations=tuple(
            ALSRecommendation(user_id, product_id, score, rank)
            for rank, (product_id, score) in enumerate(als_rows, start=1)
        ),
        product_catalog=tuple(
            ProductMetadata(product_id, f"fixture-product-{product_id}")
            for product_id in catalog_ids
        ),
        missing_prospective_trend_product_ids=tuple(missing_trend_ids),
        run_id=RUN_ID,
        model_version=MODEL_VERSION,
        graph_version=GRAPH_VERSION,
    )


def _publication(prepared, *, status="published"):
    return HybridPublicationResult(
        status=status,
        serving_version=prepared.serving_version,
        user_id=prepared.user_id,
        expected_rows=prepared.expected_rows,
        input_content_sha256=prepared.input_content_sha256,
        mode=prepared.mode,
        reason=prepared.reason,
        generated_at=PROCESSING_TIME,
        pointer_id=f"hybrid:{prepared.user_id}",
        idempotent=status == "already_active",
    )


def _install_boundaries(
    monkeypatch,
    *,
    trend=None,
    captured=None,
    capture_error=None,
    read_error=None,
    publish_error=None,
    publish_status="published",
    publish_statuses=None,
):
    events = []
    calls = {"read": [], "capture": [], "prepare": [], "publish": []}
    captured_value = captured or _captured()
    trend_value = trend or _trend()
    remaining_publication_statuses = list(publish_statuses or ())

    def read(collection, **kwargs):
        events.append("read")
        calls["read"].append((collection, kwargs))
        if read_error is not None:
            raise read_error
        return trend_value

    def capture(config, user_id, *, prospective_trend_product_ids=()):
        events.append("capture")
        calls["capture"].append((config, user_id, tuple(prospective_trend_product_ids)))
        if capture_error is not None:
            raise capture_error
        return replace(captured_value, user_id=user_id) if captured_value.user_id != user_id else captured_value

    original_prepare = service_module.prepare_hybrid_snapshot

    def prepare(*args, **kwargs):
        events.append("prepare")
        prepared = original_prepare(*args, **kwargs)
        calls["prepare"].append((args, kwargs, prepared))
        return prepared

    def publish(config, prepared, *, client=None, processing_time_utc=None):
        events.append("publish")
        calls["publish"].append((config, prepared, client, processing_time_utc))
        if publish_error is not None:
            raise publish_error
        status = (
            remaining_publication_statuses.pop(0)
            if remaining_publication_statuses
            else publish_status
        )
        return _publication(prepared, status=status)

    monkeypatch.setattr(service_module, "read_verified_trend_snapshot", read)
    monkeypatch.setattr(service_module, "capture_verified_user_sources", capture)
    monkeypatch.setattr(service_module, "prepare_hybrid_snapshot", prepare)
    monkeypatch.setattr(service_module, "publish_hybrid_recommendations", publish)
    return events, calls


def _serve(**kwargs):
    return service_module.serve_hybrid_user(
        _config(),
        user_id=USER_ID,
        source_namespace=NAMESPACE,
        as_of_event_time=AS_OF,
        ranking_config=HybridRankingConfig(lambda_weight=0.5),
        client=object(),
        trend_collection=FakeTrendCollection(),
        **kwargs,
    )


def test_warm_user_uses_verified_als_and_trend_and_returns_published_identity(monkeypatch):
    events, calls = _install_boundaries(monkeypatch)

    result = _serve(processing_time_utc=PROCESSING_TIME)

    assert events == ["read", "capture", "prepare", "publish"]
    assert result.mode == "hybrid"
    assert result.trend_available is True and result.trend_status == "available"
    assert result.recommendation_count == 3
    assert result.source_run_id == RUN_ID
    assert result.model_version == MODEL_VERSION
    assert result.graph_version == GRAPH_VERSION
    assert result.readiness_confirmed is True
    assert result.readiness_pointer_id == f"hybrid:{USER_ID}"
    assert result.serving_version == calls["publish"][0][1].serving_version
    assert result.recommendation_count == calls["publish"][0][1].expected_rows
    assert result.generated_at == PROCESSING_TIME


def test_cold_user_with_valid_trend_publishes_trend_only_without_als_scores(monkeypatch):
    events, calls = _install_boundaries(
        monkeypatch,
        captured=_captured(als_rows=()),
    )

    result = _serve()

    prepared = calls["publish"][0][1]
    assert events[-1] == "publish"
    assert result.mode == "cold_user_trend_only"
    assert [row.product_id for row in prepared.rows] == [2, 3]
    assert all(row.als_score is None and not row.from_als for row in prepared.rows)
    assert all(row.from_trend for row in prepared.rows)


@pytest.mark.parametrize(
    ("status", "reason"),
    [
        ("missing", "no_matching_snapshot"),
        ("expired", "snapshot_expired"),
        ("stale", "snapshot_event_time_stale"),
    ],
)
def test_warm_user_uses_als_fallback_and_preserves_each_c5_reason(monkeypatch, status, reason):
    trend = _trend(status=status, available=False, reason=reason)
    _events, calls = _install_boundaries(monkeypatch, trend=trend)

    result = _serve()

    assert result.mode == "als_fallback_trend_unavailable"
    assert result.reason == reason
    assert result.trend_status == status
    assert result.trend_reason == reason
    assert calls["capture"][0][2] == ()


def test_cold_user_without_usable_trend_publishes_explicit_ready_empty_snapshot(monkeypatch):
    trend = _trend(status="missing", available=False, reason="no_matching_snapshot")
    _events, calls = _install_boundaries(
        monkeypatch,
        trend=trend,
        captured=_captured(als_rows=()),
    )

    result = _serve()

    prepared = calls["publish"][0][1]
    assert result.mode == "no_candidates"
    assert result.recommendation_count == 0
    assert prepared.expected_rows == 0
    assert result.reason == "user_has_no_als_recommendations_and_trend_snapshot_unavailable"


@pytest.mark.parametrize(
    "failure",
    [TrendSnapshotIntegrityError("bundle hash mismatch"), TrendSnapshotQueryError("database unavailable")],
)
def test_trend_corruption_and_query_failure_propagate_without_capture_or_publish(
    monkeypatch, failure
):
    events, calls = _install_boundaries(monkeypatch, read_error=failure)

    with pytest.raises(type(failure)):
        _serve()

    assert events == ["read"]
    assert calls["capture"] == [] and calls["publish"] == []


def test_unknown_c5_status_fails_closed_before_c3_or_publication(monkeypatch):
    unknown = _trend(status="future_status", available=False, reason="unknown")
    events, calls = _install_boundaries(monkeypatch, trend=unknown)

    with pytest.raises(service_module.HybridServingContractError, match="unsupported"):
        _serve()

    assert events == ["read"]
    assert calls["publish"] == []


def test_invalid_ab_readiness_propagates_without_ranking_or_publication(monkeypatch):
    events, calls = _install_boundaries(
        monkeypatch,
        capture_error=ReadinessContractError("active A/B readiness status is not ready"),
    )

    with pytest.raises(ReadinessContractError):
        _serve()

    assert events == ["read", "capture"]
    assert calls["publish"] == []


def test_missing_als_catalog_metadata_fails_in_existing_ranker_without_publication(monkeypatch):
    captured = _captured(catalog_ids=(2, 3, 4))
    _events, calls = _install_boundaries(monkeypatch, captured=captured)

    with pytest.raises(RankingContractError, match="ALS product 1"):
        _serve()

    assert calls["publish"] == []


def test_missing_trend_only_catalog_product_is_excluded_and_reported(monkeypatch):
    trend = _trend(specs=((5, 0.9), (3, 0.5)))
    captured = _captured(catalog_ids=(1, 2, 3), missing_trend_ids=(5,))
    _events, calls = _install_boundaries(monkeypatch, trend=trend, captured=captured)

    result = _serve()

    ranked_rows = calls["publish"][0][1].rows
    assert 5 not in {row.product_id for row in ranked_rows}
    assert result.excluded_trend_product_ids == (5,)
    assert result.candidate_count == len(ranked_rows)


@pytest.mark.parametrize("lambda_weight", [0.0, 0.5, 0.7, 0.9, 1.0])
def test_all_required_lambda_values_flow_through_existing_ranker_and_preparation(
    monkeypatch, lambda_weight
):
    events, calls = _install_boundaries(monkeypatch)

    result = service_module.serve_hybrid_user(
        _config(),
        user_id=USER_ID,
        source_namespace=NAMESPACE,
        as_of_event_time=AS_OF,
        ranking_config=HybridRankingConfig(lambda_weight=lambda_weight),
        client=object(),
        trend_collection=FakeTrendCollection(),
    )

    prepared = calls["publish"][0][1]
    assert prepared.lambda_requested == lambda_weight
    assert result.recommendation_count <= 20
    assert events[-1] == "publish"


def test_c3_receives_only_bounded_trend_candidate_ids_and_no_catalog_scan(monkeypatch):
    trend = _trend(specs=tuple((product_id, 0.9 - index / 100) for index, product_id in enumerate(range(10, 30))))
    captured = _captured(catalog_ids=tuple(range(1, 30)))
    _events, calls = _install_boundaries(monkeypatch, trend=trend, captured=captured)

    _serve()

    config, requested_user, candidates = calls["capture"][0]
    assert config is not None and requested_user == USER_ID
    assert candidates == tuple(range(10, 30))
    assert len(candidates) == 20


def test_c5_receives_explicit_collection_namespace_asof_and_staleness_options(monkeypatch):
    _events, calls = _install_boundaries(monkeypatch)
    collection = FakeTrendCollection()
    client = object()

    service_module.serve_hybrid_user(
        _config(),
        user_id=USER_ID,
        source_namespace=NAMESPACE,
        as_of_event_time=AS_OF,
        ranking_config=HybridRankingConfig(lambda_weight=0.5),
        max_event_age=timedelta(days=5),
        processing_time_utc=PROCESSING_TIME,
        client=client,
        trend_collection=collection,
    )

    queried_collection, kwargs = calls["read"][0]
    assert queried_collection is collection
    assert kwargs == {
        "source_namespace": NAMESPACE,
        "as_of_event_time": AS_OF,
        "max_event_age": timedelta(days=5),
        "processing_time_utc": PROCESSING_TIME,
    }
    assert calls["capture"][0][0] is not None
    assert calls["publish"][0][2] is client
    assert calls["publish"][0][3] == PROCESSING_TIME


def test_c6_preparation_and_publisher_receive_the_exact_verified_inputs(monkeypatch):
    events, calls = _install_boundaries(monkeypatch)

    result = _serve()

    args, kwargs, prepared = calls["prepare"][0]
    assert args[0].user_id == USER_ID
    assert args[1].status == "available"
    assert args[2].user_id == USER_ID
    assert args[3].lambda_weight == 0.5
    assert kwargs == {"as_of_event_time": AS_OF}
    publish_config, published, _client, _processing_time = calls["publish"][0]
    assert isinstance(prepared, PreparedHybridSnapshot)
    assert published is prepared
    assert isinstance(publish_config, Module4Config)
    assert result.serving_version == prepared.serving_version
    assert events == ["read", "capture", "prepare", "publish"]


def test_no_publish_occurs_after_source_capture_validation_failure(monkeypatch):
    events, calls = _install_boundaries(
        monkeypatch,
        capture_error=ReadinessContractError("source generation changed during capture"),
    )

    with pytest.raises(ReadinessContractError):
        _serve()

    assert events == ["read", "capture"]
    assert calls["publish"] == []


def test_c6_detects_changed_ab_generation_and_service_does_not_report_success(monkeypatch):
    events, calls = _install_boundaries(
        monkeypatch,
        publish_error=HybridPublicationConflictError("active A/B generation changed"),
    )

    with pytest.raises(HybridServingPublicationError) as error:
        _serve()

    assert isinstance(error.value.__cause__, HybridPublicationConflictError)
    assert "user 701" in str(error.value)
    assert events[-1] == "publish"
    assert len(calls["publish"]) == 1


def test_publication_failure_is_chained_with_safe_user_and_version_context(monkeypatch):
    events, calls = _install_boundaries(monkeypatch, publish_error=OSError("write failed"))

    with pytest.raises(HybridServingPublicationError) as error:
        _serve()

    assert isinstance(error.value.__cause__, OSError)
    assert "mongodb://" not in str(error.value)
    assert len(calls["publish"]) == 1
    assert events[-1] == "publish"


def test_same_input_retry_reports_c6_idempotent_status(monkeypatch):
    _events, calls = _install_boundaries(
        monkeypatch,
        publish_statuses=("published", "already_active"),
    )

    first = _serve()
    retry = _serve()

    assert first.publication_status == "published" and first.idempotent is False
    assert retry.publication_status == "already_active" and retry.idempotent is True
    assert retry.serving_version == first.serving_version
    assert retry.generated_at == first.generated_at
    assert retry.readiness_confirmed is True
    assert len(calls["publish"]) == 2


def test_new_trend_content_changes_c6_derived_serving_version(monkeypatch):
    first_trend = _trend(digest="a" * 64)
    first_events, first_calls = _install_boundaries(monkeypatch, trend=first_trend)
    first = _serve()
    first_version = first_calls["publish"][0][1].serving_version
    assert first_events[-1] == "publish"

    second_trend = _trend(digest="c" * 64)
    second_events, second_calls = _install_boundaries(monkeypatch, trend=second_trend)
    second = _serve()

    assert second_events[-1] == "publish"
    assert second_calls["publish"][0][1].serving_version != first_version
    assert second.serving_version != first.serving_version


def test_different_users_receive_independent_source_and_pointer_identity(monkeypatch):
    _events, _calls = _install_boundaries(monkeypatch)
    first = _serve()
    _other_events, _other_calls = _install_boundaries(
        monkeypatch,
        captured=_captured(user_id=USER_ID + 1),
    )
    second = service_module.serve_hybrid_user(
        _config(),
        user_id=USER_ID + 1,
        source_namespace=NAMESPACE,
        as_of_event_time=AS_OF,
        ranking_config=HybridRankingConfig(lambda_weight=0.5),
        client=object(),
        trend_collection=FakeTrendCollection(),
    )

    assert first.user_id == USER_ID
    assert second.user_id == USER_ID + 1
    assert first.serving_version != second.serving_version
    assert first.readiness_pointer_id == f"hybrid:{USER_ID}"
    assert second.readiness_pointer_id == f"hybrid:{USER_ID + 1}"


def test_no_default_event_freshness_policy_and_no_current_time_asof(monkeypatch):
    old_window_start = AS_OF - timedelta(days=365)
    old_trend = replace(
        _trend(),
        window_start=old_window_start,
        window_end=old_window_start + timedelta(hours=1),
    )
    _events, calls = _install_boundaries(monkeypatch, trend=old_trend)

    _serve()

    assert calls["read"][0][1]["max_event_age"] is None
    assert "as_of_event_time" in inspect.signature(service_module.serve_hybrid_user).parameters
    assert inspect.signature(service_module.serve_hybrid_user).parameters[
        "as_of_event_time"
    ].default is inspect.Parameter.empty


def test_descriptive_trend_scope_is_returned_without_leakage_safety_claim(monkeypatch):
    _events, _calls = _install_boundaries(monkeypatch, trend=_trend())

    result = _serve()

    assert result.trend_source_scope == "fixture_scope"
    assert not hasattr(result, "leakage_safe")
    assert "leakage_safe" not in repr(result)


def test_service_does_not_write_legacy_trend_or_ab_collections(monkeypatch):
    events, calls = _install_boundaries(monkeypatch)
    collection = FakeTrendCollection()

    result = service_module.serve_hybrid_user(
        _config(),
        user_id=USER_ID,
        source_namespace=NAMESPACE,
        as_of_event_time=AS_OF,
        ranking_config=HybridRankingConfig(lambda_weight=0.5),
        client=object(),
        trend_collection=collection,
    )

    assert result.readiness_confirmed
    assert collection.name == "realtime_trending_windows"
    assert events == ["read", "capture", "prepare", "publish"]
    assert len(calls["publish"]) == 1
    assert not hasattr(result, "mongodb_uri")


@pytest.mark.parametrize(
    "overrides",
    [
        {"user_id": 0},
        {"user_id": True},
        {"source_namespace": " "},
        {"as_of_event_time": datetime(2025, 1, 1)},
        {"as_of_event_time": AS_OF + timedelta(microseconds=1)},
        {"ranking_config": object()},
        {"max_event_age": timedelta(seconds=-1)},
        {"processing_time_utc": datetime(2025, 1, 1)},
    ],
)
def test_invalid_request_is_rejected_before_any_dependency_call(monkeypatch, overrides):
    events, calls = _install_boundaries(monkeypatch)
    args = {
        "user_id": USER_ID,
        "source_namespace": NAMESPACE,
        "as_of_event_time": AS_OF,
        "ranking_config": HybridRankingConfig(lambda_weight=0.5),
        "client": object(),
        "trend_collection": FakeTrendCollection(),
    }
    args.update(overrides)

    with pytest.raises(service_module.HybridServingRequestError):
        service_module.serve_hybrid_user(_config(), **args)

    assert events == []
    assert calls["publish"] == []


def _independent_product_order(als_rows, trend_specs, catalog_ids, lambda_weight):
    als = dict(als_rows)
    trends = dict(trend_specs)
    catalog = set(catalog_ids)
    candidates = (set(als) | set(trends)) & catalog
    minimum = min(als.values()) if als else 0.0
    maximum = max(als.values()) if als else 0.0
    als_span = maximum - minimum
    max_trend = max(trends.values(), default=0.0)
    scores = {}
    for product_id in candidates:
        normalized_als = (
            (als[product_id] - minimum) / (als_span + 1e-12)
            if product_id in als and als_span
            else 0.0
        )
        normalized_trend = (
            trends[product_id] / (max_trend + 1e-12)
            if product_id in trends and max_trend
            else 0.0
        )
        scores[product_id] = (
            lambda_weight * normalized_als + (1.0 - lambda_weight) * normalized_trend
        )
    return tuple(sorted(candidates, key=lambda product_id: (-scores[product_id], product_id)))


def test_in_process_ranker_and_prepare_respond_to_trend_scores_deterministically(monkeypatch):
    captured = _captured(catalog_ids=(1, 2, 3))
    score_cases = (
        ((2, 0.1), (3, 1.0)),
        ((2, 1.0), (3, 0.1)),
    )
    actual_orders = []
    actual_versions = []

    for specifications in score_cases:
        _events, calls = _install_boundaries(
            monkeypatch,
            trend=_trend(specs=specifications),
            captured=captured,
        )
        result = service_module.serve_hybrid_user(
            _config(),
            user_id=USER_ID,
            source_namespace=NAMESPACE,
            as_of_event_time=AS_OF,
            ranking_config=HybridRankingConfig(lambda_weight=0.4),
            client=object(),
            trend_collection=FakeTrendCollection(),
        )
        prepared = calls["publish"][0][1]
        expected = _independent_product_order(
            ((1, 10.0), (2, 1.0)),
            specifications,
            (1, 2, 3),
            0.4,
        )
        assert tuple(row.product_id for row in prepared.rows) == expected
        actual_orders.append(expected)
        actual_versions.append(result.serving_version)

    assert actual_orders[0][0] == 3
    assert actual_orders[1][0] == 2
    assert actual_versions[0] != actual_versions[1]


def test_actual_ranker_is_deterministic_when_source_rows_are_permuted():
    captured_a = _captured(
        als_rows=((1, 10.0), (2, 1.0)),
        catalog_ids=(1, 2, 3),
    )
    captured_b = replace(
        captured_a,
        als_recommendations=tuple(reversed(captured_a.als_recommendations)),
        product_catalog=tuple(reversed(captured_a.product_catalog)),
    )
    trend_a = _trend(specs=((2, 0.8), (3, 0.4)))
    trend_b = replace(trend_a, trend_recommendations=tuple(reversed(trend_a.trend_recommendations)))
    first = service_module.rank_hybrid_recommendations(
        USER_ID,
        captured_a.als_recommendations,
        trend_a.trend_snapshot,
        captured_a.product_catalog,
        captured_a.source_context,
        HybridRankingConfig(lambda_weight=0.5),
    )
    second = service_module.rank_hybrid_recommendations(
        USER_ID,
        captured_b.als_recommendations,
        trend_b.trend_snapshot,
        captured_b.product_catalog,
        captured_b.source_context,
        HybridRankingConfig(lambda_weight=0.5),
    )

    assert first == second


def test_wrong_c5_namespace_is_rejected_before_capture_or_publication(monkeypatch):
    events, calls = _install_boundaries(monkeypatch, trend=_trend(namespace="other:namespace"))

    with pytest.raises(service_module.HybridServingContractError, match="different trend source"):
        _serve()

    assert events == ["read"]
    assert calls["publish"] == []


def test_public_result_preserves_temporal_fields_as_distinct_metadata(monkeypatch):
    trend = _trend()
    _events, _calls = _install_boundaries(monkeypatch, trend=trend)

    result = _serve(processing_time_utc=PROCESSING_TIME)

    assert result.as_of_event_time == AS_OF
    assert result.trend_window_end == WINDOW_END
    assert result.trend_published_at == trend.published_at
    assert result.generated_at == PROCESSING_TIME
    assert result.trend_content_sha256 == trend.content_sha256


def test_service_exports_a_small_public_api():
    assert service_module.__all__ == [
        "HybridServingContractError",
        "HybridServingPublicationError",
        "HybridServingRequestError",
        "HybridServingResult",
        "serve_hybrid_user",
    ]
