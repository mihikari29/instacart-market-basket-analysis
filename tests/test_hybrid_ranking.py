"""Pure contract and deterministic ranking tests for Module 4C C1-C2."""

from dataclasses import FrozenInstanceError
from datetime import datetime, timezone
import math

import pytest

from src.module4.hybrid import (
    ALSRecommendation,
    HybridRankingConfig,
    ProductMetadata,
    RankingContractError,
    SourceContextError,
    TrendSnapshot,
    VerifiedSourceContext,
    rank_hybrid_recommendations,
)


USER_ID = 7
WINDOW_START = datetime(2030, 1, 1, 0, 0, tzinfo=timezone.utc)
WINDOW_END = datetime(2030, 1, 1, 0, 5, tzinfo=timezone.utc)
VERIFIED_SOURCES = VerifiedSourceContext(als_available=True, graph_available=True)


def als_rows(scores, user_id=USER_ID):
    """Build Module 4A-shaped rows in source-rank order."""
    return [
        {"user_id": user_id, "product_id": product_id, "als_score": score, "als_rank": rank}
        for rank, (product_id, score) in enumerate(scores, start=1)
    ]


def trend_rows(scores):
    """Build Module 3-shaped Top-M rows for one finalized window."""
    return [
        {
            "window_start": WINDOW_START,
            "window_end": WINDOW_END,
            "product_id": product_id,
            "purchase_count_30m": rank,
            "purchase_count_120m": rank,
            "trend_score": score,
            "trend_rank": rank,
        }
        for rank, (product_id, score) in enumerate(scores, start=1)
    ]


def available_trends(scores):
    return TrendSnapshot(
        available=True,
        rows=tuple(trend_rows(scores)),
        window_start=WINDOW_START,
        window_end=WINDOW_END,
    )


def catalog_for(*product_ids):
    return [ProductMetadata(product_id, f"product-{product_id}") for product_id in product_ids]


def rank(als, trends, catalog, config=None, user_id=USER_ID, context=VERIFIED_SOURCES):
    return rank_hybrid_recommendations(
        user_id=user_id,
        als_recommendations=als,
        trend_snapshot=trends,
        product_catalog=catalog,
        source_context=context,
        config=config or HybridRankingConfig(),
    )


def by_product(result):
    return {recommendation.product_id: recommendation for recommendation in result.recommendations}


@pytest.mark.parametrize(
    "field,value",
    [
        ("als_top_n", 0),
        ("als_top_n", -1),
        ("als_top_n", 21),
        ("als_top_n", 1.5),
        ("als_top_n", True),
        ("trend_top_m", 21),
        ("output_top_k", 0),
    ],
)
def test_config_rejects_invalid_candidate_limits(field, value):
    with pytest.raises(ValueError, match=field):
        HybridRankingConfig(**{field: value})


@pytest.mark.parametrize("value", [-0.1, 1.1, float("nan"), float("inf"), True, "0.7"])
def test_config_rejects_invalid_lambda(value):
    with pytest.raises(ValueError, match="lambda_weight"):
        HybridRankingConfig(lambda_weight=value)


@pytest.mark.parametrize("value", [0, -1, float("nan"), float("inf"), True, "1e-12"])
def test_config_rejects_invalid_epsilon(value):
    with pytest.raises(ValueError, match="epsilon"):
        HybridRankingConfig(epsilon=value)


def test_config_defaults_and_immutability():
    config = HybridRankingConfig()
    assert (config.als_top_n, config.trend_top_m, config.output_top_k) == (20, 20, 20)
    assert config.lambda_weight == 0.7
    assert config.epsilon == 1e-12
    assert config.algorithm_version == "hybrid-v1"
    with pytest.raises(FrozenInstanceError):
        config.lambda_weight = 0.5


@pytest.mark.parametrize("user_id", [0, -1, None, True, 1.0])
def test_invalid_requested_user_ids_are_rejected(user_id):
    with pytest.raises(RankingContractError, match="request.user_id"):
        rank([], TrendSnapshot(available=False), catalog_for(1), user_id=user_id)


@pytest.mark.parametrize("product_id", [0, -1, None, True, 1.0])
def test_invalid_als_product_ids_are_rejected(product_id):
    rows = [{"user_id": USER_ID, "product_id": product_id, "als_score": 1.0, "als_rank": 1}]
    with pytest.raises(RankingContractError, match="ALS.product_id"):
        rank(rows, TrendSnapshot(available=False), catalog_for(1))


@pytest.mark.parametrize("product_id", [0, -1, None, True, 1.0])
def test_invalid_trend_product_ids_are_rejected(product_id):
    rows = trend_rows([(1, 1.0)])
    rows[0]["product_id"] = product_id
    with pytest.raises(RankingContractError, match="trend.product_id"):
        rank([], TrendSnapshot(available=True, rows=tuple(rows)), catalog_for(1))


def test_invalid_catalog_metadata_is_rejected():
    with pytest.raises(RankingContractError, match="catalog.product_id"):
        rank([], TrendSnapshot(available=False), [ProductMetadata(0, "invalid")])
    with pytest.raises(RankingContractError, match="product_name.*non-empty"):
        rank([], TrendSnapshot(available=False), [ProductMetadata(1, "  ")])


def test_als_dataclass_contract_uses_published_module4_fields():
    rows = [
        ALSRecommendation(user_id=USER_ID, product_id=1, als_score=0.0, als_rank=1),
    ]
    result = rank(rows, TrendSnapshot(available=False), catalog_for(1))
    recommendation = result.recommendations[0]
    assert recommendation.als_score == 0.0
    assert recommendation.from_als is True


def test_exact_union_merges_overlap_and_preserves_source_membership():
    result = rank(
        als_rows([(1, 9.0), (2, 4.0)]),
        available_trends([(2, 1.0), (3, 0.5)]),
        catalog_for(1, 2, 3),
        HybridRankingConfig(lambda_weight=0.5),
    )
    rows = by_product(result)
    assert set(rows) == {1, 2, 3}
    assert rows[2].from_als is True and rows[2].from_trend is True
    assert rows[1].from_als is True and rows[1].from_trend is False
    assert rows[3].from_als is False and rows[3].from_trend is True
    assert rows[3].als_score is None
    assert rows[1].trend_score is None
    assert result.metadata.candidate_count == 3
    assert result.metadata.overlap_count == 1


def test_als_only_when_trend_snapshot_is_unavailable():
    result = rank(
        als_rows([(1, 2.0), (2, 1.0)]),
        TrendSnapshot(available=False, unavailable_reason="snapshot_not_ready"),
        catalog_for(1, 2),
    )
    assert [row.product_id for row in result.recommendations] == [1, 2]
    assert result.metadata.mode == "als_fallback_trend_unavailable"
    assert result.metadata.reason == "snapshot_not_ready"
    assert result.metadata.lambda_effective == 1.0


def test_unavailable_trend_fallback_overrides_requested_zero_lambda():
    result = rank(
        als_rows([(1, 2.0), (2, 1.0)]),
        TrendSnapshot(available=False),
        catalog_for(1, 2),
        HybridRankingConfig(lambda_weight=0.0),
    )
    assert [row.product_id for row in result.recommendations] == [1, 2]
    assert result.metadata.mode == "als_fallback_trend_unavailable"
    assert result.metadata.lambda_requested == 0.0
    assert result.metadata.lambda_effective == 1.0


def test_trend_only_for_lambda_zero_even_when_user_is_warm():
    result = rank(
        als_rows([(1, 2.0)]),
        available_trends([(2, 0.5), (3, 0.25)]),
        catalog_for(1, 2, 3),
        HybridRankingConfig(lambda_weight=0.0),
    )
    assert [row.product_id for row in result.recommendations] == [2, 3]
    assert result.metadata.mode == "lambda_0_trend_only"
    assert result.metadata.lambda_effective == 0.0


def test_warm_user_uses_hybrid_formula():
    result = rank(
        als_rows([(1, 3.0), (2, 1.0)]),
        available_trends([(2, 0.5), (3, 0.25)]),
        catalog_for(1, 2, 3),
        HybridRankingConfig(lambda_weight=0.5),
    )
    rows = by_product(result)
    assert result.metadata.mode == "hybrid"
    expected_scores = {
        1: 0.5 * (3.0 - 1.0) / (3.0 - 1.0 + 1e-12),
        2: 0.5 * (1.0 - 1.0) / (3.0 - 1.0 + 1e-12)
        + 0.5 * 0.5 / (0.5 + 1e-12),
        3: 0.5 * 0.25 / (0.5 + 1e-12),
    }
    assert [row.product_id for row in result.recommendations] == [1, 2, 3]
    for product_id, expected_score in expected_scores.items():
        assert rows[product_id].final_score == pytest.approx(expected_score, abs=1e-15)


def test_cold_user_uses_valid_trend_source_with_effective_zero():
    result = rank(
        [],
        available_trends([(2, 0.8), (3, 0.4)]),
        catalog_for(2, 3),
        HybridRankingConfig(lambda_weight=0.9),
    )
    assert [row.product_id for row in result.recommendations] == [2, 3]
    assert result.metadata.mode == "cold_user_trend_only"
    assert result.metadata.reason == "user_has_no_als_recommendations"
    assert result.metadata.lambda_requested == 0.9
    assert result.metadata.lambda_effective == 0.0
    assert all(row.als_score is None and not row.from_als for row in result.recommendations)


def test_cold_user_without_trends_returns_explicit_empty_reason():
    result = rank([], TrendSnapshot(available=False), catalog_for(1))
    assert result.recommendations == ()
    assert result.metadata.mode == "no_candidates"
    assert result.metadata.reason == "user_has_no_als_recommendations_and_trend_snapshot_unavailable"
    assert result.metadata.recommendation_count == 0


def test_available_empty_trend_snapshot_has_distinct_empty_reason():
    result = rank([], TrendSnapshot(available=True), catalog_for(1))
    assert result.recommendations == ()
    assert result.metadata.mode == "cold_user_trend_only"
    assert result.metadata.reason == "trend_snapshot_empty"


@pytest.mark.parametrize("lambda_weight", [0.0, 0.5, 0.7, 0.9, 1.0])
def test_lambda_values_are_reported_and_applied(lambda_weight):
    config = HybridRankingConfig(lambda_weight=lambda_weight)
    result = rank(
        als_rows([(1, 3.0), (2, 2.0)]),
        available_trends([(2, 1.0), (3, 0.5)]),
        catalog_for(1, 2, 3),
        config,
    )
    assert result.metadata.lambda_requested == lambda_weight
    assert result.metadata.lambda_effective == lambda_weight
    if lambda_weight == 0.0:
        assert {row.product_id for row in result.recommendations} == {2, 3}
    elif lambda_weight == 1.0:
        assert {row.product_id for row in result.recommendations} == {1, 2}
    else:
        assert {row.product_id for row in result.recommendations} == {1, 2, 3}


def test_negative_finite_als_scores_normalize_correctly():
    result = rank(
        als_rows([(1, -2.0), (2, -4.0)]),
        TrendSnapshot(available=False),
        catalog_for(1, 2),
    )
    rows = by_product(result)
    assert rows[1].normalized_als_score == pytest.approx(2.0 / (2.0 + 1e-12))
    assert rows[2].normalized_als_score == 0.0


def test_equal_als_scores_produce_zero_normalized_scores():
    result = rank(
        als_rows([(1, 5.0), (2, 5.0)]),
        TrendSnapshot(available=False),
        catalog_for(1, 2),
    )
    assert [row.normalized_als_score for row in result.recommendations] == [0.0, 0.0]


def test_singleton_als_candidate_has_zero_normalized_score():
    result = rank(
        als_rows([(1, -8.0)]),
        TrendSnapshot(available=False),
        catalog_for(1),
    )
    assert result.recommendations[0].normalized_als_score == 0.0


def test_all_zero_trend_scores_normalize_to_zero():
    result = rank(
        [],
        available_trends([(1, 0.0), (2, 0.0)]),
        catalog_for(1, 2),
    )
    assert [row.normalized_trend_score for row in result.recommendations] == [0.0, 0.0]


def test_trending_maximum_below_one_uses_window_max_scaling():
    result = rank(
        [],
        available_trends([(1, 0.4), (2, 0.2)]),
        catalog_for(1, 2),
    )
    rows = by_product(result)
    assert rows[1].normalized_trend_score == pytest.approx(0.4 / (0.4 + 1e-12))
    assert rows[2].normalized_trend_score == pytest.approx(0.2 / (0.4 + 1e-12))


def test_trend_maximum_is_calculated_before_catalog_exclusion():
    result = rank(
        als_rows([(1, 1.0)]),
        available_trends([(99, 1.0), (2, 0.5)]),
        catalog_for(1, 2),
        HybridRankingConfig(lambda_weight=0.5),
    )
    rows = by_product(result)
    assert rows[2].normalized_trend_score == pytest.approx(0.5 / (1.0 + 1e-12))
    assert result.metadata.excluded_trend_product_ids == (99,)


def test_missing_als_product_in_catalog_is_a_contract_error():
    with pytest.raises(RankingContractError, match="ALS product 2.*catalog"):
        rank(
            als_rows([(2, 1.0)]),
            TrendSnapshot(available=False),
            catalog_for(1),
        )


def test_trend_only_missing_catalog_product_is_excluded_and_reported():
    result = rank(
        [],
        available_trends([(99, 1.0), (2, 0.5)]),
        catalog_for(2),
    )
    assert [row.product_id for row in result.recommendations] == [2]
    assert result.metadata.excluded_trend_product_ids == (99,)
    assert result.metadata.candidate_count == 1


@pytest.mark.parametrize(
    "rows,pattern",
    [
        ([{"user_id": USER_ID, "product_id": 1, "als_score": 1.0, "als_rank": 1},
          {"user_id": USER_ID, "product_id": 1, "als_score": 0.5, "als_rank": 2}], "duplicate product_id"),
        ([{"user_id": USER_ID, "product_id": 1, "als_score": 1.0, "als_rank": 1},
          {"user_id": USER_ID, "product_id": 2, "als_score": 0.5, "als_rank": 1}], "duplicate ranks"),
        ([{"user_id": USER_ID, "product_id": 1, "als_score": 1.0, "als_rank": 1},
          {"user_id": USER_ID, "product_id": 2, "als_score": 0.5, "als_rank": 3}], "contiguous"),
    ],
)
def test_als_duplicate_products_and_malformed_ranks_are_rejected(rows, pattern):
    with pytest.raises(RankingContractError, match=pattern):
        rank(rows, TrendSnapshot(available=False), catalog_for(1, 2))


@pytest.mark.parametrize("score", [float("nan"), float("inf"), float("-inf"), None])
def test_invalid_published_als_scores_are_rejected(score):
    with pytest.raises(RankingContractError, match="ALS.als_score"):
        rank(
            [{"user_id": USER_ID, "product_id": 1, "als_score": score, "als_rank": 1}],
            TrendSnapshot(available=False),
            catalog_for(1),
        )


@pytest.mark.parametrize("score", [-0.01, 1.01, float("nan"), float("inf"), None])
def test_invalid_published_trend_scores_are_rejected(score):
    with pytest.raises(RankingContractError):
        rank([], available_trends([(1, score)]), catalog_for(1))


def test_duplicate_and_noncontiguous_trend_ranks_are_rejected():
    duplicate = trend_rows([(1, 1.0), (2, 0.5)])
    duplicate[1]["trend_rank"] = 1
    with pytest.raises(RankingContractError, match="duplicate ranks"):
        rank([], TrendSnapshot(available=True, rows=tuple(duplicate)), catalog_for(1, 2))

    gap = trend_rows([(1, 1.0), (2, 0.5)])
    gap[1]["trend_rank"] = 3
    with pytest.raises(RankingContractError, match="contiguous"):
        rank([], TrendSnapshot(available=True, rows=tuple(gap)), catalog_for(1, 2))


def test_duplicate_trend_product_ids_are_rejected():
    rows = trend_rows([(1, 1.0), (1, 0.5)])
    with pytest.raises(RankingContractError, match="duplicate product_id 1"):
        rank([], TrendSnapshot(available=True, rows=tuple(rows)), catalog_for(1))


def test_source_context_failure_is_not_treated_as_a_cold_user():
    with pytest.raises(SourceContextError, match="graph.*cannot classify user as cold"):
        rank(
            [],
            available_trends([(1, 1.0)]),
            catalog_for(1),
            context=VerifiedSourceContext(als_available=True, graph_available=False),
        )


def test_real_zero_als_score_is_distinct_from_missing_als_membership():
    result = rank(
        als_rows([(1, 0.0)]),
        available_trends([(2, 1.0)]),
        catalog_for(1, 2),
        HybridRankingConfig(lambda_weight=0.5),
    )
    rows = by_product(result)
    assert rows[1].als_score == 0.0 and rows[1].from_als is True
    assert rows[2].als_score is None and rows[2].from_als is False


def test_stable_tie_break_uses_ascending_product_id():
    result = rank(
        als_rows([(9, 2.0), (3, 2.0), (5, 2.0)]),
        TrendSnapshot(available=False),
        catalog_for(3, 5, 9),
    )
    assert [row.product_id for row in result.recommendations] == [3, 5, 9]


def test_input_order_permutation_does_not_change_results():
    als = als_rows([(1, 3.0), (2, 2.0), (3, 1.0)])
    trends = trend_rows([(3, 1.0), (1, 0.5), (4, 0.25)])
    catalog = catalog_for(1, 2, 3, 4)
    first = rank(als, TrendSnapshot(available=True, rows=tuple(trends)), catalog)
    second = rank(
        list(reversed(als)),
        TrendSnapshot(available=True, rows=tuple(reversed(trends))),
        list(reversed(catalog)),
    )
    assert first == second


def test_output_is_capped_at_k_with_contiguous_output_ranks():
    result = rank(
        als_rows([(1, 5.0), (2, 4.0), (3, 3.0)]),
        available_trends([(4, 1.0), (5, 0.8), (6, 0.6)]),
        catalog_for(1, 2, 3, 4, 5, 6),
        HybridRankingConfig(als_top_n=3, trend_top_m=3, output_top_k=2),
    )
    assert len(result.recommendations) == 2
    assert [row.rank for row in result.recommendations] == [1, 2]
    assert result.metadata.candidate_count == 6
    assert result.metadata.recommendation_count == 2


def test_identical_inputs_produce_identical_results():
    args = (
        als_rows([(1, 3.0), (2, 2.0)]),
        available_trends([(2, 1.0), (3, 0.5)]),
        catalog_for(1, 2, 3),
    )
    assert rank(*args) == rank(*args)


def test_independent_responsiveness_oracle_changes_trend_driven_order():
    als = als_rows([(1, 3.0), (2, 2.0), (3, 1.0)])
    unchanged_als = list(als)
    config = HybridRankingConfig(lambda_weight=0.5)

    baseline = rank(
        als,
        available_trends([(1, 1.0), (2, 0.0), (3, 0.0)]),
        catalog_for(1, 2, 3),
        config,
    )
    updated = rank(
        als,
        available_trends([(2, 1.0), (1, 10.0 / 99.0), (3, 0.0)]),
        catalog_for(1, 2, 3),
        config,
    )

    assert baseline.recommendations[0].product_id == 1
    assert updated.recommendations[0].product_id == 2
    updated_rows = by_product(updated)
    assert updated_rows[2].final_score == pytest.approx(0.75, abs=1e-9)
    assert updated_rows[1].final_score == pytest.approx(0.5505050505, abs=1e-9)
    assert als == unchanged_als


def test_extreme_finite_als_scores_do_not_overflow_normalization():
    result = rank(
        als_rows([(1, 1e308), (2, -1e308)]),
        TrendSnapshot(available=False),
        catalog_for(1, 2),
    )
    rows = by_product(result)
    assert math.isfinite(rows[1].normalized_als_score)
    assert rows[1].normalized_als_score == pytest.approx(1.0)
    assert rows[2].normalized_als_score == 0.0


def test_extreme_finite_epsilon_does_not_overflow_als_normalization():
    config = HybridRankingConfig(epsilon=1.7e308)
    result = rank(
        als_rows([(1, 1e308), (2, -1e308)]),
        TrendSnapshot(available=False),
        catalog_for(1, 2),
        config,
    )
    assert all(math.isfinite(row.normalized_als_score) for row in result.recommendations)
