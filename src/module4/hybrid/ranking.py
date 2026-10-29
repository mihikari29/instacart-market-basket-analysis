"""Pure Python, deterministic hybrid recommendation ranking."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
import math
from numbers import Real
from typing import Any

from .config import (
    HybridRankingConfig,
    HybridRankingMetadata,
    HybridRankingResult,
    ProductMetadata,
    Recommendation,
    TrendSnapshot,
    VerifiedSourceContext,
)


class RankingContractError(ValueError):
    """Raised when a published source or catalog violates its data contract."""


class SourceContextError(RankingContractError):
    """Raised when the caller has not verified the required Module 4A/4B sources."""


def _value(row: Any, key: str, source: str) -> Any:
    if isinstance(row, Mapping):
        if key not in row:
            raise RankingContractError(f"{source}: missing required field {key!r}")
        return row[key]
    try:
        return getattr(row, key)
    except AttributeError:
        raise RankingContractError(f"{source}: missing required field {key!r}") from None


def _positive_integer(value: Any, field: str, source: str) -> int:
    if type(value) is not int or value <= 0:
        raise RankingContractError(f"{source}.{field} must be a positive integer")
    return value


def _finite_number(value: Any, field: str, source: str) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise RankingContractError(f"{source}.{field} must be a finite number")
    try:
        number = float(value)
    except (OverflowError, ValueError) as exc:
        raise RankingContractError(f"{source}.{field} must be a finite number") from exc
    if not math.isfinite(number):
        raise RankingContractError(f"{source}.{field} must be a finite number")
    return number


def _rows(rows: Iterable[Any], source: str) -> tuple[Any, ...]:
    if isinstance(rows, (str, bytes, Mapping)):
        raise RankingContractError(f"{source} must be an iterable of row records")
    try:
        return tuple(rows)
    except TypeError as exc:
        raise RankingContractError(f"{source} must be an iterable of row records") from exc


def _rank_sequence(ranks: list[int], source: str) -> None:
    if len(set(ranks)) != len(ranks):
        raise RankingContractError(f"{source} contains duplicate ranks")
    expected = set(range(1, len(ranks) + 1))
    if set(ranks) != expected:
        raise RankingContractError(f"{source} ranks must be contiguous starting at 1")


def _normalize_als(scores: dict[int, float], epsilon: float) -> dict[int, float]:
    if not scores:
        return {}
    minimum = min(scores.values())
    maximum = max(scores.values())
    if minimum == maximum:
        return {product_id: 0.0 for product_id in scores}

    span = maximum - minimum
    if math.isfinite(span):
        denominator = span + epsilon
        if math.isfinite(denominator):
            return {
                product_id: (score - minimum) / denominator
                for product_id, score in scores.items()
            }

        scale = max(span, epsilon)
        scaled_denominator = span / scale + epsilon / scale
        return {
            product_id: ((score - minimum) / scale) / scaled_denominator
            for product_id, score in scores.items()
        }

    # Keep the specified ratio finite even for valid finite scores whose range
    # overflows a binary float (for example, -1e308 through +1e308).
    scale = max(abs(minimum), abs(maximum), epsilon)
    scaled_minimum = minimum / scale
    scaled_maximum = maximum / scale
    scaled_epsilon = epsilon / scale
    denominator = scaled_maximum - scaled_minimum + scaled_epsilon
    return {
        product_id: (score / scale - scaled_minimum) / denominator
        for product_id, score in scores.items()
    }


def _read_als_rows(
    requested_user_id: int,
    input_rows: Iterable[Any],
    limit: int,
) -> dict[int, float]:
    rows = _rows(input_rows, "ALS recommendations")
    if len(rows) > limit:
        raise RankingContractError(f"ALS recommendations exceed configured Top-{limit}")

    scores: dict[int, float] = {}
    ranks: list[int] = []
    for row in rows:
        user_id = _positive_integer(_value(row, "user_id", "ALS recommendation"), "user_id", "ALS")
        if user_id != requested_user_id:
            raise RankingContractError(
                f"ALS recommendation user_id {user_id} does not match requested user {requested_user_id}"
            )
        product_id = _positive_integer(
            _value(row, "product_id", "ALS recommendation"), "product_id", "ALS"
        )
        rank = _positive_integer(_value(row, "als_rank", "ALS recommendation"), "als_rank", "ALS")
        score = _finite_number(_value(row, "als_score", "ALS recommendation"), "als_score", "ALS")
        if product_id in scores:
            raise RankingContractError(f"ALS recommendations contain duplicate product_id {product_id}")
        scores[product_id] = score
        ranks.append(rank)

    _rank_sequence(ranks, "ALS recommendations")
    return scores


def _read_trend_rows(
    snapshot: TrendSnapshot,
    limit: int,
) -> dict[int, float]:
    if not isinstance(snapshot, TrendSnapshot):
        raise RankingContractError("trend_snapshot must be a TrendSnapshot")
    if not snapshot.available:
        return {}
    if len(snapshot.rows) > limit:
        raise RankingContractError(f"trending recommendations exceed configured Top-{limit}")

    scores: dict[int, float] = {}
    ranks: list[int] = []
    for row in snapshot.rows:
        product_id = _positive_integer(
            _value(row, "product_id", "trending recommendation"), "product_id", "trend"
        )
        rank = _positive_integer(
            _value(row, "trend_rank", "trending recommendation"), "trend_rank", "trend"
        )
        score = _finite_number(
            _value(row, "trend_score", "trending recommendation"), "trend_score", "trend"
        )
        if score < 0.0 or score > 1.0:
            raise RankingContractError("trend.trend_score must be within [0, 1]")
        if product_id in scores:
            raise RankingContractError(f"trending recommendations contain duplicate product_id {product_id}")
        scores[product_id] = score
        ranks.append(rank)

    _rank_sequence(ranks, "trending recommendations")
    return scores


def _read_catalog_details(
    catalog: Iterable[ProductMetadata | Mapping[str, Any]] | Mapping[int, str]
) -> tuple[dict[int, str], dict[int, float]]:
    if isinstance(catalog, Mapping):
        # A compact product_id -> product_name map is convenient for callers;
        # record-shaped mappings are also accepted as an iterable of records.
        catalog_rows: Iterable[Any]
        if "product_id" in catalog or "product_name" in catalog:
            catalog_rows = (catalog,)
        else:
            catalog_rows = tuple(
                {"product_id": product_id, "product_name": product_name}
                for product_id, product_name in catalog.items()
            )
    else:
        catalog_rows = catalog

    names: dict[int, str] = {}
    pageranks: dict[int, float] = {}
    for row in _rows(catalog_rows, "product catalog"):
        product_id = _positive_integer(
            _value(row, "product_id", "product catalog"), "product_id", "catalog"
        )
        product_name = _value(row, "product_name", "product catalog")
        if not isinstance(product_name, str) or not product_name.strip():
            raise RankingContractError(f"catalog.product_name for product {product_id} must be non-empty")
        if product_id in names:
            raise RankingContractError(f"product catalog contains duplicate product_id {product_id}")
        names[product_id] = product_name
        pr = getattr(row, "pagerank", None) if not isinstance(row, Mapping) else row.get("pagerank")
        if pr is not None and isinstance(pr, (int, float)) and math.isfinite(pr):
            pageranks[product_id] = float(pr)
    return names, pageranks


def rank_hybrid_recommendations(
    user_id: int,
    als_recommendations: Iterable[Any],
    trend_snapshot: TrendSnapshot,
    product_catalog: Iterable[ProductMetadata | Mapping[str, Any]] | Mapping[int, str],
    source_context: VerifiedSourceContext,
    config: HybridRankingConfig = HybridRankingConfig(),
) -> HybridRankingResult:
    """Rank one user's ALS and finalized trend candidates without external state.

    Source order has no scoring effect. Ranks are validated as complete source
    sequences, while final ordering uses unrounded scores then ascending product
    ID. ``candidate_count`` describes the selected candidate set after missing
    trend catalog products are excluded and before the output Top-K is applied.
    """
    requested_user_id = _positive_integer(user_id, "user_id", "request")
    if not isinstance(source_context, VerifiedSourceContext):
        raise SourceContextError("source_context must be a VerifiedSourceContext")
    if type(source_context.als_available) is not bool or type(source_context.graph_available) is not bool:
        raise SourceContextError("ALS and graph source availability must be booleans")
    if not source_context.als_available or not source_context.graph_available:
        unavailable = []
        if not source_context.als_available:
            unavailable.append("ALS")
        if not source_context.graph_available:
            unavailable.append("graph")
        raise SourceContextError(
            f"required source context is unavailable: {', '.join(unavailable)}; cannot classify user as cold"
        )
    if not isinstance(config, HybridRankingConfig):
        raise RankingContractError("config must be a HybridRankingConfig")

    als_scores = _read_als_rows(requested_user_id, als_recommendations, config.als_top_n)
    trend_scores = _read_trend_rows(trend_snapshot, config.trend_top_m)
    catalog, pagerank_scores = _read_catalog_details(product_catalog)

    for product_id in als_scores:
        if product_id not in catalog:
            raise RankingContractError(
                f"ALS product {product_id} is missing from the verified product catalog"
            )

    excluded_trend_product_ids = tuple(sorted(set(trend_scores).difference(catalog)))

    normalized_als = _normalize_als(als_scores, config.epsilon)
    max_trend_score = max(trend_scores.values(), default=0.0)
    if max_trend_score == 0.0:
        normalized_trend = {product_id: 0.0 for product_id in trend_scores}
    else:
        trend_denominator = max_trend_score + config.epsilon
        normalized_trend = {
            product_id: score / trend_denominator for product_id, score in trend_scores.items()
        }

    max_pr = max(pagerank_scores.values(), default=0.0)
    min_pr = min(pagerank_scores.values(), default=0.0)
    pr_span = max_pr - min_pr
    normalized_pr = {
        pid: (score - min_pr) / (pr_span + config.epsilon) if pr_span > 0 else (1.0 if max_pr > 0 else 0.0)
        for pid, score in pagerank_scores.items()
    }

    has_als = bool(als_scores)
    trend_available = trend_snapshot.available
    requested_lambda = float(config.lambda_weight)
    reason: str | None = None

    if has_als and trend_available:
        if requested_lambda == 1.0:
            candidate_source = "als"
            lambda_effective = 1.0
            mode = "lambda_1_als_only"
        elif requested_lambda == 0.0:
            candidate_source = "trend"
            lambda_effective = 0.0
            mode = "lambda_0_trend_only"
        else:
            candidate_source = "both"
            lambda_effective = requested_lambda
            mode = "hybrid"
    elif has_als:
        candidate_source = "als"
        lambda_effective = 1.0
        mode = "als_fallback_trend_unavailable"
        reason = trend_snapshot.unavailable_reason or "trend_snapshot_unavailable"
    elif trend_available:
        candidate_source = "trend"
        lambda_effective = 0.0
        mode = "cold_user_trend_only"
        reason = "user_has_no_als_recommendations"
    else:
        candidate_source = "none"
        lambda_effective = 0.0
        mode = "no_candidates"
        reason = "user_has_no_als_recommendations_and_trend_snapshot_unavailable"

    if candidate_source == "als":
        candidate_ids = set(als_scores)
    elif candidate_source == "trend":
        candidate_ids = set(trend_scores)
    elif candidate_source == "both":
        candidate_ids = set(als_scores).union(trend_scores)
    else:
        candidate_ids = set()
    candidate_ids.difference_update(excluded_trend_product_ids)

    if not candidate_ids and mode != "no_candidates":
        if candidate_source == "trend":
            reason = (
                "trend_snapshot_empty"
                if not trend_scores
                else "trend_candidates_missing_from_catalog"
            )
        elif candidate_source == "als":
            reason = "user_has_no_als_recommendations"
        else:
            reason = "selected_sources_have_no_candidates"

    candidates: list[Recommendation] = []
    for product_id in candidate_ids:
        from_als = product_id in als_scores
        from_trend = product_id in trend_scores
        als_score = als_scores.get(product_id) if from_als else None
        trend_score = trend_scores.get(product_id) if from_trend else None
        normalized_als_score = normalized_als.get(product_id, 0.0)
        normalized_trend_score = normalized_trend.get(product_id, 0.0)
        norm_pr = normalized_pr.get(product_id, 0.0)
        pr_score = pagerank_scores.get(product_id)

        if config.beta > 0.0 and pagerank_scores:
            final_score = (
                config.alpha * normalized_als_score
                + config.beta * norm_pr
                + config.gamma * normalized_trend_score
            )
        else:
            final_score = (
                lambda_effective * normalized_als_score
                + (1.0 - lambda_effective) * normalized_trend_score
            )
        candidates.append(
            Recommendation(
                user_id=requested_user_id,
                product_id=product_id,
                product_name=catalog[product_id],
                als_score=als_score,
                trend_score=trend_score,
                normalized_als_score=normalized_als_score,
                normalized_trend_score=normalized_trend_score,
                final_score=final_score,
                rank=0,
                from_als=from_als,
                from_trend=from_trend,
                pagerank_score=pr_score,
                normalized_pagerank_score=norm_pr,
            )
        )

    ordered = sorted(candidates, key=lambda row: (-row.final_score, row.product_id))
    recommendations = tuple(
        Recommendation(
            user_id=row.user_id,
            product_id=row.product_id,
            product_name=row.product_name,
            als_score=row.als_score,
            trend_score=row.trend_score,
            normalized_als_score=row.normalized_als_score,
            normalized_trend_score=row.normalized_trend_score,
            final_score=row.final_score,
            rank=rank,
            from_als=row.from_als,
            from_trend=row.from_trend,
            pagerank_score=row.pagerank_score,
            normalized_pagerank_score=row.normalized_pagerank_score,
        )
        for rank, row in enumerate(ordered[: config.output_top_k], start=1)
    )

    metadata = HybridRankingMetadata(
        mode=mode,
        reason=reason,
        lambda_requested=requested_lambda,
        lambda_effective=lambda_effective,
        algorithm_version=config.algorithm_version,
        candidate_count=len(candidate_ids),
        recommendation_count=len(recommendations),
        als_source_available=source_context.als_available,
        graph_source_available=source_context.graph_available,
        trend_source_available=trend_available,
        als_source_count=len(als_scores),
        trend_source_count=len(trend_scores),
        overlap_count=len(set(als_scores).intersection(trend_scores)),
        excluded_trend_product_ids=excluded_trend_product_ids,
    )
    return HybridRankingResult(
        user_id=requested_user_id,
        recommendations=recommendations,
        metadata=metadata,
    )
