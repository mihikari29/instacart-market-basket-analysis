"""Immutable MongoDB publication for one user's final hybrid snapshot."""

from __future__ import annotations
from ...common.runtime import aware_utc_millisecond, positive_int

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
from numbers import Real
import re
from typing import Any

from src.module4 import sink_mongo as module4_sink
from src.module4.config import Config as Module4Config
from src.module3.trend_bundle import BUNDLE_SCHEMA_VERSION

from .config import (
    HybridRankingConfig,
    HybridRankingMetadata,
    HybridRankingResult,
    Recommendation,
    ALSRecommendation,
    ProductMetadata,
    TrendRecommendation,
    VerifiedSourceContext,
)
from .data import (
    READINESS_COLLECTION,
    READINESS_ID,
    CapturedUserSources,
    readiness_identity,
)
from .ranking import RankingContractError, rank_hybrid_recommendations
from .trend_reader import TrendSnapshotReadResult


FINAL_RECOMMENDATIONS_COLLECTION = "final_recommendations"
SERVING_SCHEMA_VERSION = 1
MAX_FINAL_RECOMMENDATIONS = 20
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_ROW_INDEXES = (
    (
        [("serving_version", 1), ("user_id", 1), ("product_id", 1)],
        {"unique": True, "name": "uq_serving_user_product"},
    ),
    (
        [("serving_version", 1), ("user_id", 1), ("rank", 1)],
        {"unique": True, "name": "uq_serving_user_rank"},
    ),
)


class HybridPublisherError(RuntimeError):
    """Base error for final recommendation preparation and publication."""


class HybridSnapshotContractError(HybridPublisherError, ValueError):
    """Inputs or ranked output violate the C1-C5 serving contract."""


class HybridPublicationConflictError(HybridPublisherError):
    """An immutable version or per-user pointer conflicts with this request."""


class HybridPublicationWriteError(HybridPublisherError):
    """Rows could not be completely written and verified."""


class HybridPublicationPromotionError(HybridPublisherError):
    """The per-user pointer could not be safely promoted or verified."""


@dataclass(frozen=True, slots=True)
class PreparedHybridSnapshot:
    """Pure, validated logical content ready for immutable row publication."""

    user_id: int
    serving_version: str
    input_content_sha256: str
    input_fingerprint_payload_json: str
    fingerprint_sha256: str
    fingerprint_payload_json: str
    algorithm_version: str
    ranking_config_sha256: str
    als_top_n: int
    trend_top_m: int
    output_top_k: int
    epsilon: float
    lambda_requested: float
    lambda_effective: float
    mode: str
    reason: str | None
    source_run_id: str
    model_version: str
    graph_version: str
    trend_source_namespace: str
    trend_source_scope: str | None
    trend_window_start: datetime | None
    trend_window_end: datetime | None
    trend_content_sha256: str | None
    as_of_event_time: datetime
    rows: tuple[Recommendation, ...]

    @property
    def expected_rows(self) -> int:
        return len(self.rows)


@dataclass(frozen=True, slots=True)
class HybridPublicationResult:
    """Outcome and stable identity of one per-user pointer publication."""

    status: str
    serving_version: str
    user_id: int
    expected_rows: int
    input_content_sha256: str
    mode: str
    reason: str | None
    generated_at: datetime
    pointer_id: str
    idempotent: bool



def _nonempty_string(value: Any, field: str, *, maximum: int | None = 256) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or (maximum is not None and len(value) > maximum)
        or "\x00" in value
    ):
        raise HybridSnapshotContractError(f"{field} must be a stable non-empty string")
    return value



def _stored_bson_utc(value: Any, field: str) -> datetime:
    if not isinstance(value, datetime):
        raise HybridPublicationConflictError(f"existing pointer {field} is not a BSON datetime")
    try:
        normalized = (
            value.replace(tzinfo=timezone.utc)
            if value.tzinfo is None or value.utcoffset() is None
            else value.astimezone(timezone.utc)
        )
    except (OverflowError, ValueError) as exc:
        raise HybridPublicationConflictError(f"existing pointer {field} is invalid") from exc
    if normalized.microsecond % 1000:
        raise HybridPublicationConflictError(
            f"existing pointer {field} is not BSON millisecond-aligned"
        )
    return normalized


def _finite_float(value: Any, field: str, *, nullable: bool = False) -> float | None:
    if value is None and nullable:
        return None
    if isinstance(value, bool) or not isinstance(value, Real):
        raise HybridSnapshotContractError(f"{field} must be a finite number")
    try:
        number = float(value)
    except (OverflowError, ValueError) as exc:
        raise HybridSnapshotContractError(f"{field} must be a finite number") from exc
    if not math.isfinite(number):
        raise HybridSnapshotContractError(f"{field} must be a finite number")
    return number


def _unit_float(value: Any, field: str) -> float:
    number = _finite_float(value, field)
    assert number is not None
    if not 0.0 <= number <= 1.0:
        raise HybridSnapshotContractError(f"{field} must be within [0, 1]")
    return number


def _float_token(value: float | None) -> str | None:
    return None if value is None else float(value).hex()


def _canonical_json(payload: Any) -> str:
    try:
        return json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise HybridSnapshotContractError("fingerprint payload is not canonical JSON") from exc


def _sha256(payload: Any) -> str:
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def _time_token(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _config_payload(config: HybridRankingConfig) -> dict[str, Any]:
    return {
        "als_top_n": config.als_top_n,
        "trend_top_m": config.trend_top_m,
        "output_top_k": config.output_top_k,
        "lambda_weight": _float_token(float(config.lambda_weight)),
        "epsilon": _float_token(float(config.epsilon)),
        "algorithm_version": config.algorithm_version,
    }


def _canonical_recommendation(row: Recommendation, index: int) -> Recommendation:
    if not isinstance(row, Recommendation):
        raise HybridSnapshotContractError(f"recommendations[{index}] must be a Recommendation")
    positive_int(row.user_id, f"recommendations[{index}].user_id", error=HybridSnapshotContractError)
    positive_int(row.product_id, f"recommendations[{index}].product_id", error=HybridSnapshotContractError)
    positive_int(row.rank, f"recommendations[{index}].rank", error=HybridSnapshotContractError)
    if not isinstance(row.product_name, str) or not row.product_name.strip():
        raise HybridSnapshotContractError(f"recommendations[{index}].product_name must be non-empty")
    if type(row.from_als) is not bool or type(row.from_trend) is not bool:
        raise HybridSnapshotContractError(f"recommendations[{index}] modality flags must be booleans")
    if not row.from_als and not row.from_trend:
        raise HybridSnapshotContractError(f"recommendations[{index}] has no source modality")

    als_score = _finite_float(row.als_score, f"recommendations[{index}].als_score", nullable=True)
    trend_score = _finite_float(
        row.trend_score, f"recommendations[{index}].trend_score", nullable=True
    )
    if row.from_als != (als_score is not None):
        raise HybridSnapshotContractError(
            f"recommendations[{index}] ALS score nullability disagrees with from_als"
        )
    if row.from_trend != (trend_score is not None):
        raise HybridSnapshotContractError(
            f"recommendations[{index}] trend score nullability disagrees with from_trend"
        )
    if trend_score is not None and not 0.0 <= trend_score <= 1.0:
        raise HybridSnapshotContractError(f"recommendations[{index}].trend_score must be within [0, 1]")

    normalized_als = _unit_float(
        row.normalized_als_score, f"recommendations[{index}].normalized_als_score"
    )
    normalized_trend = _unit_float(
        row.normalized_trend_score, f"recommendations[{index}].normalized_trend_score"
    )
    final_score = _unit_float(row.final_score, f"recommendations[{index}].final_score")
    if not row.from_als and normalized_als != 0.0:
        raise HybridSnapshotContractError(
            f"recommendations[{index}] normalized ALS score must be zero when absent"
        )
    if not row.from_trend and normalized_trend != 0.0:
        raise HybridSnapshotContractError(
            f"recommendations[{index}] normalized trend score must be zero when absent"
        )
    return Recommendation(
        user_id=row.user_id,
        product_id=row.product_id,
        product_name=row.product_name,
        als_score=als_score,
        trend_score=trend_score,
        normalized_als_score=normalized_als,
        normalized_trend_score=normalized_trend,
        final_score=final_score,
        rank=row.rank,
        from_als=row.from_als,
        from_trend=row.from_trend,
    )


def _source_input_payload(
    captured: CapturedUserSources,
    trend_result: TrendSnapshotReadResult,
    as_of_event_time: datetime,
    config: HybridRankingConfig,
    mode: str,
    reason: str | None,
    lambda_effective: float,
) -> dict[str, Any]:
    als_rows = []
    for row in captured.als_recommendations:
        product_id = positive_int(row.product_id, "captured ALS product_id", error=HybridSnapshotContractError)
        user_id = positive_int(row.user_id, "captured ALS user_id", error=HybridSnapshotContractError)
        score = _finite_float(row.als_score, "captured ALS score", nullable=True)
        rank = positive_int(row.als_rank, "captured ALS rank", error=HybridSnapshotContractError)
        als_rows.append(
            {
                "user_id": user_id,
                "product_id": product_id,
                "als_rank": rank,
                "als_score": _float_token(score),
            }
        )
    als_rows.sort(key=lambda value: (value["als_rank"], value["product_id"]))

    catalog_rows = []
    for row in captured.product_catalog:
        product_id = positive_int(row.product_id, "captured catalog product_id", error=HybridSnapshotContractError)
        if not isinstance(row.product_name, str) or not row.product_name.strip():
            raise HybridSnapshotContractError("captured catalog product_name must be non-empty")
        catalog_rows.append({"product_id": product_id, "product_name": row.product_name})
    catalog_rows.sort(key=lambda value: value["product_id"])

    trend_rows = []
    for row in trend_result.trend_recommendations:
        product_id = positive_int(row.product_id, "captured trend product_id", error=HybridSnapshotContractError)
        trend_rank = positive_int(row.trend_rank, "captured trend rank", error=HybridSnapshotContractError)
        trend_score = _unit_float(row.trend_score, "captured trend score")
        trend_rows.append(
            {
                "product_id": product_id,
                "trend_rank": trend_rank,
                "trend_score": _float_token(trend_score),
            }
        )
    trend_rows.sort(key=lambda value: (value["trend_rank"], value["product_id"]))

    return {
        "schema_version": SERVING_SCHEMA_VERSION,
        "user_id": captured.user_id,
        "source": {
            "source_run_id": captured.run_id,
            "model_version": captured.model_version,
            "graph_version": captured.graph_version,
        },
        "as_of_event_time": _time_token(as_of_event_time),
        "trend": {
            "status": trend_result.status,
            "reason": trend_result.reason,
            "schema_version": trend_result.schema_version,
            "config_hash": trend_result.config_hash,
            "source_namespace": trend_result.source_namespace,
            "source_scope": trend_result.source_scope,
            "window_start": _time_token(trend_result.window_start),
            "window_end": _time_token(trend_result.window_end),
            "content_sha256": trend_result.content_sha256,
            "recommendations": trend_rows,
        },
        "als_recommendations": als_rows,
        "product_catalog": catalog_rows,
        "ranking_config": _config_payload(config),
        "mode": mode,
        "reason": reason,
        "lambda_effective": _float_token(lambda_effective),
    }


def prepare_hybrid_snapshot(
    captured_sources: CapturedUserSources,
    trend_result: TrendSnapshotReadResult,
    ranking_result: HybridRankingResult,
    ranking_config: HybridRankingConfig,
    *,
    as_of_event_time: datetime,
) -> PreparedHybridSnapshot:
    """Validate C1-C5 outputs and derive a deterministic, per-user version."""
    if not isinstance(captured_sources, CapturedUserSources):
        raise HybridSnapshotContractError("captured_sources must be CapturedUserSources")
    if not isinstance(trend_result, TrendSnapshotReadResult):
        raise HybridSnapshotContractError("trend_result must be TrendSnapshotReadResult")
    if not isinstance(ranking_result, HybridRankingResult):
        raise HybridSnapshotContractError("ranking_result must be HybridRankingResult")
    if not isinstance(ranking_config, HybridRankingConfig):
        raise HybridSnapshotContractError("ranking_config must be HybridRankingConfig")
    if not isinstance(ranking_result.metadata, HybridRankingMetadata):
        raise HybridSnapshotContractError("ranking metadata must be HybridRankingMetadata")
    if not isinstance(ranking_result.recommendations, tuple):
        raise HybridSnapshotContractError("ranking recommendations must be an immutable tuple")
    if not isinstance(captured_sources.als_recommendations, tuple) or any(
        not isinstance(row, ALSRecommendation) for row in captured_sources.als_recommendations
    ):
        raise HybridSnapshotContractError("captured ALS inputs must use the C3 ALSRecommendation contract")
    if not isinstance(captured_sources.product_catalog, tuple) or any(
        not isinstance(row, ProductMetadata) for row in captured_sources.product_catalog
    ):
        raise HybridSnapshotContractError("captured catalog must use the C3 ProductMetadata contract")
    if not isinstance(trend_result.trend_recommendations, tuple) or any(
        not isinstance(row, TrendRecommendation) for row in trend_result.trend_recommendations
    ):
        raise HybridSnapshotContractError("C5 trend rows must use the TrendRecommendation contract")

    user_id = positive_int(captured_sources.user_id, "captured_sources.user_id", error=HybridSnapshotContractError)
    if positive_int(ranking_result.user_id, "ranking_result.user_id", error=HybridSnapshotContractError) != user_id:
        raise HybridSnapshotContractError("ranking result user_id does not match captured sources")
    if len(ranking_result.recommendations) > min(
        MAX_FINAL_RECOMMENDATIONS, ranking_config.output_top_k
    ):
        raise HybridSnapshotContractError("recommendation output exceeds resolved Top-K")
    if not isinstance(captured_sources.source_context, VerifiedSourceContext):
        raise HybridSnapshotContractError("captured source_context must be VerifiedSourceContext")
    if captured_sources.source_context.als_available is not True or captured_sources.source_context.graph_available is not True:
        raise HybridSnapshotContractError("captured A/B source context is not fully available")

    run_id = _nonempty_string(captured_sources.run_id, "source_run_id", maximum=80)
    model_version = _nonempty_string(captured_sources.model_version, "model_version", maximum=80)
    graph_version = _nonempty_string(captured_sources.graph_version, "graph_version", maximum=80)
    try:
        # Enforce the same source-version lexical rules as the active readiness reader.
        for value, field in (
            (run_id, "source_run_id"),
            (model_version, "model_version"),
            (graph_version, "graph_version"),
        ):
            if (
                re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,79}", value) is None
                or ".." in value
            ):
                raise ValueError(field)
    except ValueError as exc:
        raise HybridSnapshotContractError(f"{exc.args[0]} is not a valid source version") from exc

    as_of = aware_utc_millisecond(as_of_event_time, "as_of_event_time", error=HybridSnapshotContractError)
    metadata = ranking_result.metadata
    if not isinstance(metadata.mode, str) or not metadata.mode.strip():
        raise HybridSnapshotContractError("ranking metadata mode must be non-empty")
    if metadata.algorithm_version != ranking_config.algorithm_version:
        raise HybridSnapshotContractError("ranking algorithm version does not match its config")
    lambda_requested = _unit_float(metadata.lambda_requested, "lambda_requested")
    lambda_effective = _unit_float(metadata.lambda_effective, "lambda_effective")
    if lambda_requested != float(ranking_config.lambda_weight):
        raise HybridSnapshotContractError("ranking lambda_requested does not match its config")
    if type(metadata.als_source_available) is not bool or not metadata.als_source_available:
        raise HybridSnapshotContractError("ranking metadata does not verify ALS source availability")
    if type(metadata.graph_source_available) is not bool or not metadata.graph_source_available:
        raise HybridSnapshotContractError("ranking metadata does not verify graph source availability")
    if metadata.trend_source_available is not trend_result.available:
        raise HybridSnapshotContractError("ranking trend availability does not match C5 result")
    if type(metadata.als_source_count) is not int or metadata.als_source_count != len(captured_sources.als_recommendations):
        raise HybridSnapshotContractError("ranking ALS source count does not match captured sources")
    expected_trend_count = len(trend_result.trend_recommendations) if trend_result.available else 0
    if type(metadata.trend_source_count) is not int or metadata.trend_source_count != expected_trend_count:
        raise HybridSnapshotContractError("ranking trend source count does not match C5 result")
    if type(metadata.recommendation_count) is not int or metadata.recommendation_count != len(ranking_result.recommendations):
        raise HybridSnapshotContractError("ranking recommendation_count does not match result rows")
    if type(metadata.candidate_count) is not int or metadata.candidate_count < len(ranking_result.recommendations):
        raise HybridSnapshotContractError("ranking candidate_count is inconsistent")
    if metadata.reason is not None and (
        not isinstance(metadata.reason, str) or not metadata.reason.strip()
    ):
        raise HybridSnapshotContractError("ranking reason must be None or a non-empty string")

    if trend_result.status not in {"available", "missing", "expired", "stale"}:
        raise HybridSnapshotContractError("C5 trend result has an unsupported status")
    if trend_result.available != (trend_result.status == "available"):
        raise HybridSnapshotContractError("C5 trend availability and status disagree")
    if trend_result.reason is not None and (
        not isinstance(trend_result.reason, str) or not trend_result.reason.strip()
    ):
        raise HybridSnapshotContractError("C5 trend reason must be None or a non-empty string")
    if trend_result.available and trend_result.reason is not None:
        raise HybridSnapshotContractError("available C5 trend result cannot have a reason")
    if not trend_result.available and trend_result.reason is None:
        raise HybridSnapshotContractError("unavailable C5 trend result must explain its status")
    if not trend_result.available and trend_result.trend_recommendations:
        raise HybridSnapshotContractError("unavailable C5 trend result cannot contain recommendations")
    trend_namespace = _nonempty_string(trend_result.source_namespace, "trend_source_namespace")
    trend_scope = trend_result.source_scope
    if trend_scope is not None:
        trend_scope = _nonempty_string(trend_scope, "trend_source_scope", maximum=128)
    trend_start = (
        aware_utc_millisecond(trend_result.window_start, "trend_window_start", error=HybridSnapshotContractError)
        if trend_result.window_start is not None
        else None
    )
    trend_end = (
        aware_utc_millisecond(trend_result.window_end, "trend_window_end", error=HybridSnapshotContractError)
        if trend_result.window_end is not None
        else None
    )
    if (trend_start is None) != (trend_end is None):
        raise HybridSnapshotContractError("trend window bounds must both be present or absent")
    if trend_start is not None and trend_start >= trend_end:
        raise HybridSnapshotContractError("trend_window_start must precede trend_window_end")
    if trend_end is not None and trend_end > as_of:
        raise HybridSnapshotContractError("trend_window_end must not exceed as_of_event_time")
    trend_hash = trend_result.content_sha256
    if trend_hash is not None and (
        not isinstance(trend_hash, str) or _SHA256_PATTERN.fullmatch(trend_hash) is None
    ):
        raise HybridSnapshotContractError("trend_content_sha256 must be lowercase hexadecimal SHA-256")
    if trend_result.available and (
        type(trend_result.schema_version) is not int
        or trend_result.schema_version != BUNDLE_SCHEMA_VERSION
    ):
        raise HybridSnapshotContractError("available C5 trend result has an unsupported schema version")
    if trend_result.config_hash is not None and (
        not isinstance(trend_result.config_hash, str)
        or _SHA256_PATTERN.fullmatch(trend_result.config_hash) is None
    ):
        raise HybridSnapshotContractError("trend config_hash must be lowercase hexadecimal SHA-256")
    if trend_result.available and (
        trend_scope is None
        or trend_start is None
        or trend_end is None
        or trend_hash is None
        or trend_result.config_hash is None
    ):
        raise HybridSnapshotContractError("available C5 trend result is missing verified provenance")

    rows = tuple(
        sorted(
            (
                _canonical_recommendation(row, index)
                for index, row in enumerate(ranking_result.recommendations)
            ),
            key=lambda row: row.rank,
        )
    )
    if any(row.user_id != user_id for row in rows):
        raise HybridSnapshotContractError("recommendation rows contain a different user_id")
    if len({row.product_id for row in rows}) != len(rows):
        raise HybridSnapshotContractError("recommendations contain duplicate product IDs")
    if [row.rank for row in rows] != list(range(1, len(rows) + 1)):
        raise HybridSnapshotContractError("recommendation ranks must be unique and contiguous from 1")
    if not rows and (metadata.mode == "" or not metadata.reason):
        raise HybridSnapshotContractError("empty results require a descriptive mode and reason")

    try:
        recomputed = rank_hybrid_recommendations(
            user_id=user_id,
            als_recommendations=captured_sources.als_recommendations,
            trend_snapshot=trend_result.trend_snapshot,
            product_catalog=captured_sources.product_catalog,
            source_context=captured_sources.source_context,
            config=ranking_config,
        )
    except RankingContractError as exc:
        raise HybridSnapshotContractError("captured C1-C5 inputs fail the existing ranker contract") from exc
    recomputed_rows = tuple(sorted(recomputed.recommendations, key=lambda row: row.rank))
    if metadata != recomputed.metadata or rows != recomputed_rows:
        raise HybridSnapshotContractError(
            "ranking result does not match the captured C1-C5 inputs and resolved config"
        )

    config_payload = _config_payload(ranking_config)
    config_sha256 = _sha256(config_payload)
    input_payload = _source_input_payload(
        captured_sources,
        trend_result,
        as_of,
        ranking_config,
        metadata.mode,
        metadata.reason,
        lambda_effective,
    )
    input_payload_json = _canonical_json(input_payload)
    input_sha256 = hashlib.sha256(input_payload_json.encode("utf-8")).hexdigest()
    output_payload = [
        {
            "rank": row.rank,
            "product_id": row.product_id,
            "product_name": row.product_name,
            "als_score": _float_token(row.als_score),
            "trend_score": _float_token(row.trend_score),
            "normalized_als_score": _float_token(row.normalized_als_score),
            "normalized_trend_score": _float_token(row.normalized_trend_score),
            "final_score": _float_token(row.final_score),
            "from_als": row.from_als,
            "from_trend": row.from_trend,
        }
        for row in rows
    ]
    fingerprint_payload = {
        "schema_version": SERVING_SCHEMA_VERSION,
        "user_id": user_id,
        "input_content_sha256": input_sha256,
        "source_run_id": run_id,
        "model_version": model_version,
        "graph_version": graph_version,
        "trend_source_namespace": trend_namespace,
        "trend_source_scope": trend_scope,
        "trend_window_start": _time_token(trend_start),
        "trend_window_end": _time_token(trend_end),
        "trend_content_sha256": trend_hash,
        "as_of_event_time": _time_token(as_of),
        "algorithm_version": ranking_config.algorithm_version,
        "ranking_config": config_payload,
        "mode": metadata.mode,
        "reason": metadata.reason,
        "lambda_effective": _float_token(lambda_effective),
        "recommendations": output_payload,
    }
    fingerprint_json = _canonical_json(fingerprint_payload)
    fingerprint_sha256 = hashlib.sha256(fingerprint_json.encode("utf-8")).hexdigest()
    serving_version = f"hybrid:{fingerprint_sha256}"

    return PreparedHybridSnapshot(
        user_id=user_id,
        serving_version=serving_version,
        input_content_sha256=input_sha256,
        input_fingerprint_payload_json=input_payload_json,
        fingerprint_sha256=fingerprint_sha256,
        fingerprint_payload_json=fingerprint_json,
        algorithm_version=ranking_config.algorithm_version,
        ranking_config_sha256=config_sha256,
        als_top_n=ranking_config.als_top_n,
        trend_top_m=ranking_config.trend_top_m,
        output_top_k=ranking_config.output_top_k,
        epsilon=float(ranking_config.epsilon),
        lambda_requested=lambda_requested,
        lambda_effective=lambda_effective,
        mode=metadata.mode,
        reason=metadata.reason,
        source_run_id=run_id,
        model_version=model_version,
        graph_version=graph_version,
        trend_source_namespace=trend_namespace,
        trend_source_scope=trend_scope,
        trend_window_start=trend_start,
        trend_window_end=trend_end,
        trend_content_sha256=trend_hash,
        as_of_event_time=as_of,
        rows=rows,
    )


def ensure_final_recommendation_indexes(collection: Any) -> None:
    """Create and verify the two immutable per-version uniqueness indexes."""
    if not hasattr(collection, "create_index"):
        raise HybridSnapshotContractError("final recommendation collection must support create_index")
    for keys, options in _ROW_INDEXES:
        try:
            collection.create_index(keys, **options)
        except Exception as exc:
            raise HybridPublicationWriteError(
                f"could not create final recommendation index {options['name']!r}"
            ) from exc
    try:
        indexes = collection.index_information()
    except Exception as exc:
        raise HybridPublicationWriteError("could not verify final recommendation indexes") from exc
    for keys, options in _ROW_INDEXES:
        name = options["name"]
        actual = indexes.get(name)
        if actual is None:
            raise HybridPublicationWriteError(f"required index {name!r} is missing")
        if list(actual.get("key", ())) != keys or actual.get("unique") is not True:
            raise HybridPublicationConflictError(f"existing index {name!r} has an incompatible definition")


def row_document(prepared: PreparedHybridSnapshot, row: Recommendation, generated_at: datetime) -> dict[str, Any]:
    document = {
        "schema_version": SERVING_SCHEMA_VERSION,
        "_id": f"{prepared.serving_version}:{prepared.user_id}:{row.product_id}",
        "serving_version": prepared.serving_version,
        "algorithm_version": prepared.algorithm_version,
        "user_id": prepared.user_id,
        "product_id": row.product_id,
        "product_name": row.product_name,
        "als_score": row.als_score,
        "trend_score": row.trend_score,
        "normalized_als_score": row.normalized_als_score,
        "normalized_trend_score": row.normalized_trend_score,
        "final_score": row.final_score,
        "rank": row.rank,
        "from_als": row.from_als,
        "from_trend": row.from_trend,
        "lambda_requested": prepared.lambda_requested,
        "lambda_effective": prepared.lambda_effective,
        "mode": prepared.mode,
        "reason": prepared.reason,
        "source_run_id": prepared.source_run_id,
        "model_version": prepared.model_version,
        "graph_version": prepared.graph_version,
        "trend_source_namespace": prepared.trend_source_namespace,
        "trend_source_scope": prepared.trend_source_scope,
        "trend_window_start": prepared.trend_window_start,
        "trend_window_end": prepared.trend_window_end,
        "trend_content_sha256": prepared.trend_content_sha256,
        "as_of_event_time": prepared.as_of_event_time,
        "input_content_sha256": prepared.input_content_sha256,
        "generated_at": generated_at,
        "als_top_n": prepared.als_top_n,
        "trend_top_m": prepared.trend_top_m,
        "output_top_k": prepared.output_top_k,
        "epsilon": prepared.epsilon,
        "ranking_config_sha256": prepared.ranking_config_sha256,
    }
    return document


def _logical_row(document: Mapping[str, Any]) -> dict[str, Any]:
    logical = {key: value for key, value in document.items() if key != "generated_at"}
    for field in ("trend_window_start", "trend_window_end", "as_of_event_time"):
        if logical.get(field) is not None:
            logical[field] = _stored_bson_utc(logical[field], field)
    return logical


def _cursor_rows(collection: Any, query: dict[str, Any]) -> list[Mapping[str, Any]]:
    try:
        cursor = collection.find(query).limit(MAX_FINAL_RECOMMENDATIONS + 1)
        rows = list(cursor)
    except Exception as exc:
        raise HybridPublicationWriteError("failed to read bounded final recommendation rows") from exc
    if len(rows) > MAX_FINAL_RECOMMENDATIONS:
        raise HybridPublicationConflictError("stored serving snapshot exceeds the Top-20 bound")
    if any(not isinstance(row, Mapping) for row in rows):
        raise HybridPublicationConflictError("stored final recommendation row is not a document")
    return rows


def _verify_stored_rows(
    collection: Any,
    prepared: PreparedHybridSnapshot,
    expected_documents: list[dict[str, Any]],
) -> tuple[list[Mapping[str, Any]], datetime | None]:
    query = {"serving_version": prepared.serving_version, "user_id": prepared.user_id}
    existing = _cursor_rows(collection, query)
    expected_by_id = {document["_id"]: document for document in expected_documents}
    generated_at_values: set[datetime] = set()
    for stored in existing:
        stored_id = stored.get("_id")
        expected = expected_by_id.get(stored_id)
        if expected is None or _logical_row(stored) != _logical_row(expected):
            raise HybridPublicationConflictError(
                "stored rows conflict with immutable serving-version content"
            )
        generated_at_values.add(_stored_bson_utc(stored.get("generated_at"), "generated_at"))
    if len(generated_at_values) > 1:
        raise HybridPublicationConflictError("stored rows have inconsistent generated_at timestamps")
    if len(existing) > len(expected_documents):
        raise HybridPublicationConflictError("stored serving version has unexpected extra rows")
    return existing, next(iter(generated_at_values), None)


def _write_and_verify_rows(
    collection: Any,
    prepared: PreparedHybridSnapshot,
    processing_time: datetime,
) -> datetime:
    ensure_final_recommendation_indexes(collection)
    existing: list[Mapping[str, Any]] = []
    existing_generated_at: datetime | None = None
    if prepared.rows:
        # The initial lookup is against the exact immutable version and bounded to 21.
        existing, existing_generated_at = _verify_stored_rows(
            collection,
            prepared,
            [
                row_document(prepared, row, processing_time)
                for row in prepared.rows
            ],
        )
    generated_at = existing_generated_at or processing_time
    expected_documents = [
        row_document(prepared, row, generated_at) for row in prepared.rows
    ]
    existing_ids = {document.get("_id") for document in existing}
    missing_documents = [
        document for document in expected_documents if document["_id"] not in existing_ids
    ]
    if missing_documents:
        try:
            collection.insert_many(missing_documents, ordered=False)
        except Exception as exc:
            # A duplicate-key race for identical content can converge. Any other
            # partial/error state remains unready and can be retried safely.
            try:
                existing, existing_generated_at = _verify_stored_rows(
                    collection, prepared, expected_documents
                )
            except HybridPublisherError:
                raise HybridPublicationWriteError(
                    "final recommendation row write failed before verification"
                ) from exc
            if len(existing) != len(expected_documents):
                raise HybridPublicationWriteError(
                    "final recommendation row write was partial; retry the same snapshot"
                ) from exc
            generated_at = existing_generated_at or generated_at
    try:
        existing, verified_generated_at = _verify_stored_rows(
            collection, prepared, expected_documents
        )
    except HybridPublisherError:
        raise
    if len(existing) != prepared.expected_rows:
        raise HybridPublicationWriteError(
            f"stored row count {len(existing)} does not match expected {prepared.expected_rows}"
        )
    stored_products = {row["product_id"] for row in existing}
    expected_products = {row.product_id for row in prepared.rows}
    if stored_products != expected_products:
        raise HybridPublicationConflictError("stored product membership differs from prepared snapshot")
    if prepared.expected_rows and verified_generated_at is not None:
        generated_at = verified_generated_at
    return generated_at


def _pointer_id(user_id: int) -> str:
    return f"hybrid:{user_id}"


def pointer_document(prepared: PreparedHybridSnapshot, updated_at: datetime) -> dict[str, Any]:
    return {
        "schema_version": SERVING_SCHEMA_VERSION,
        "_id": _pointer_id(prepared.user_id),
        "user_id": prepared.user_id,
        "status": "ready",
        "ready": True,
        "serving_version": prepared.serving_version,
        "expected_rows": prepared.expected_rows,
        "source_run_id": prepared.source_run_id,
        "model_version": prepared.model_version,
        "graph_version": prepared.graph_version,
        "trend_source_namespace": prepared.trend_source_namespace,
        "trend_window_end": prepared.trend_window_end,
        "trend_content_sha256": prepared.trend_content_sha256,
        "as_of_event_time": prepared.as_of_event_time,
        "algorithm_version": prepared.algorithm_version,
        "mode": prepared.mode,
        "reason": prepared.reason,
        "input_content_sha256": prepared.input_content_sha256,
        "ranking_config_sha256": prepared.ranking_config_sha256,
        "als_top_n": prepared.als_top_n,
        "trend_top_m": prepared.trend_top_m,
        "output_top_k": prepared.output_top_k,
        "epsilon": prepared.epsilon,
        "updated_at": updated_at,
    }


def _validate_active_ab_readiness(database: Any, prepared: PreparedHybridSnapshot) -> None:
    try:
        document = database[READINESS_COLLECTION].find_one({"_id": READINESS_ID})
    except Exception as exc:
        raise HybridPublicationPromotionError("failed to recheck active A/B readiness") from exc
    try:
        identity = readiness_identity(document)
    except Exception as exc:
        raise HybridPublicationPromotionError("active A/B readiness is not publishable") from exc
    if (
        identity.run_id != prepared.source_run_id
        or identity.model_version != prepared.model_version
        or identity.graph_version != prepared.graph_version
    ):
        raise HybridPublicationConflictError(
            "active A/B readiness generation changed after source capture"
        )


def _validate_existing_pointer(document: Any, user_id: int) -> dict[str, Any]:
    if not isinstance(document, Mapping):
        raise HybridPublicationConflictError("per-user readiness pointer is not a document")
    if (
        document.get("_id") != _pointer_id(user_id)
        or type(document.get("user_id")) is not int
        or document.get("user_id") != user_id
    ):
        raise HybridPublicationConflictError("per-user readiness pointer identity is inconsistent")
    if type(document.get("schema_version")) is not int or document["schema_version"] != SERVING_SCHEMA_VERSION:
        raise HybridPublicationConflictError("existing per-user pointer has an unsupported schema_version")
    if document.get("status") != "ready" or document.get("ready") is not True:
        raise HybridPublicationConflictError("existing per-user readiness pointer is not ready")
    if type(document.get("expected_rows")) is not int or not 0 <= document["expected_rows"] <= 20:
        raise HybridPublicationConflictError("existing per-user pointer has invalid expected_rows")
    for field in ("trend_window_end", "trend_content_sha256", "reason", "updated_at"):
        if field not in document:
            raise HybridPublicationConflictError(f"existing per-user pointer lacks {field}")
    for field in (
        "serving_version",
        "source_run_id",
        "model_version",
        "graph_version",
        "trend_source_namespace",
        "algorithm_version",
        "mode",
        "input_content_sha256",
        "ranking_config_sha256",
    ):
        value = document.get(field)
        if not isinstance(value, str) or not value:
            raise HybridPublicationConflictError(f"existing per-user pointer lacks {field}")
    if _SHA256_PATTERN.fullmatch(document["input_content_sha256"]) is None:
        raise HybridPublicationConflictError("existing per-user pointer has invalid input_content_sha256")
    serving_version = document["serving_version"]
    if (
        not serving_version.startswith("hybrid:")
        or _SHA256_PATTERN.fullmatch(serving_version[len("hybrid:") :]) is None
    ):
        raise HybridPublicationConflictError("existing per-user pointer has invalid serving_version")
    if _SHA256_PATTERN.fullmatch(document["ranking_config_sha256"]) is None:
        raise HybridPublicationConflictError("existing per-user pointer has invalid ranking config digest")
    for field in ("als_top_n", "trend_top_m", "output_top_k"):
        if type(document.get(field)) is not int or not 1 <= document[field] <= 20:
            raise HybridPublicationConflictError(f"existing per-user pointer has invalid {field}")
    epsilon = document.get("epsilon")
    if isinstance(epsilon, bool) or not isinstance(epsilon, Real):
        raise HybridPublicationConflictError("existing per-user pointer has invalid epsilon")
    if not math.isfinite(float(epsilon)) or epsilon <= 0.0:
        raise HybridPublicationConflictError("existing per-user pointer has invalid epsilon")
    if document.get("trend_content_sha256") is not None and (
        not isinstance(document["trend_content_sha256"], str)
        or _SHA256_PATTERN.fullmatch(document["trend_content_sha256"]) is None
    ):
        raise HybridPublicationConflictError("existing per-user pointer has invalid trend digest")
    if document.get("reason") is not None and not isinstance(document.get("reason"), str):
        raise HybridPublicationConflictError("existing per-user pointer has invalid reason")
    if document["expected_rows"] == 0 and (
        not isinstance(document.get("reason"), str) or not document["reason"].strip()
    ):
        raise HybridPublicationConflictError("empty per-user pointer must describe its reason")
    result = dict(document)
    result["as_of_event_time"] = _stored_bson_utc(
        document.get("as_of_event_time"), "as_of_event_time"
    )
    result["trend_window_end"] = (
        _stored_bson_utc(document["trend_window_end"], "trend_window_end")
        if document.get("trend_window_end") is not None
        else None
    )
    result["updated_at"] = _stored_bson_utc(document["updated_at"], "updated_at")
    return result


def _pointer_matches(document: Mapping[str, Any], prepared: PreparedHybridSnapshot) -> bool:
    try:
        normalized = _validate_existing_pointer(document, prepared.user_id)
    except HybridPublicationConflictError:
        return False
    return all(
        normalized.get(field) == value
        for field, value in (
            ("_id", _pointer_id(prepared.user_id)),
            ("schema_version", SERVING_SCHEMA_VERSION),
            ("user_id", prepared.user_id),
            ("status", "ready"),
            ("ready", True),
            ("serving_version", prepared.serving_version),
            ("expected_rows", prepared.expected_rows),
            ("source_run_id", prepared.source_run_id),
            ("model_version", prepared.model_version),
            ("graph_version", prepared.graph_version),
            ("trend_source_namespace", prepared.trend_source_namespace),
            ("trend_content_sha256", prepared.trend_content_sha256),
            ("algorithm_version", prepared.algorithm_version),
            ("mode", prepared.mode),
            ("reason", prepared.reason),
            ("input_content_sha256", prepared.input_content_sha256),
            ("ranking_config_sha256", prepared.ranking_config_sha256),
            ("als_top_n", prepared.als_top_n),
            ("trend_top_m", prepared.trend_top_m),
            ("output_top_k", prepared.output_top_k),
            ("epsilon", prepared.epsilon),
        )
    ) and normalized["as_of_event_time"] == prepared.as_of_event_time and normalized[
        "trend_window_end"
    ] == prepared.trend_window_end


def _validate_prepared_snapshot(prepared: PreparedHybridSnapshot) -> None:
    """Reject manually constructed or internally inconsistent prepared objects."""
    positive_int(prepared.user_id, "prepared.user_id", error=HybridSnapshotContractError)
    aware_utc_millisecond(prepared.as_of_event_time, "prepared.as_of_event_time", error=HybridSnapshotContractError)
    for field, value in (
        ("source_run_id", prepared.source_run_id),
        ("model_version", prepared.model_version),
        ("graph_version", prepared.graph_version),
        ("trend_source_namespace", prepared.trend_source_namespace),
    ):
        _nonempty_string(value, f"prepared.{field}")
    _nonempty_string(prepared.algorithm_version, "prepared.algorithm_version", maximum=None)
    _nonempty_string(prepared.mode, "prepared.mode", maximum=None)
    if prepared.reason is not None and (
        not isinstance(prepared.reason, str) or not prepared.reason.strip()
    ):
        raise HybridSnapshotContractError("prepared.reason must be None or a non-empty string")
    for field, value in (
        ("als_top_n", prepared.als_top_n),
        ("trend_top_m", prepared.trend_top_m),
        ("output_top_k", prepared.output_top_k),
    ):
        if type(value) is not int or not 1 <= value <= MAX_FINAL_RECOMMENDATIONS:
            raise HybridSnapshotContractError(f"prepared.{field} must be between 1 and 20")
    if _finite_float(prepared.epsilon, "prepared.epsilon") <= 0.0:
        raise HybridSnapshotContractError("prepared.epsilon must be positive")
    _unit_float(prepared.lambda_requested, "prepared.lambda_requested")
    _unit_float(prepared.lambda_effective, "prepared.lambda_effective")
    if _SHA256_PATTERN.fullmatch(prepared.input_content_sha256) is None:
        raise HybridSnapshotContractError("prepared input_content_sha256 is invalid")
    if _SHA256_PATTERN.fullmatch(prepared.ranking_config_sha256) is None:
        raise HybridSnapshotContractError("prepared ranking_config_sha256 is invalid")
    if _SHA256_PATTERN.fullmatch(prepared.fingerprint_sha256) is None:
        raise HybridSnapshotContractError("prepared fingerprint_sha256 is invalid")
    if prepared.serving_version != f"hybrid:{prepared.fingerprint_sha256}":
        raise HybridSnapshotContractError("prepared serving_version does not match its fingerprint")
    try:
        input_payload = json.loads(prepared.input_fingerprint_payload_json)
        fingerprint_payload = json.loads(prepared.fingerprint_payload_json)
    except (TypeError, ValueError) as exc:
        raise HybridSnapshotContractError("prepared fingerprint payload is invalid JSON") from exc
    if not isinstance(input_payload, dict) or not isinstance(fingerprint_payload, dict):
        raise HybridSnapshotContractError("prepared fingerprint payloads must be JSON objects")
    if _canonical_json(input_payload) != prepared.input_fingerprint_payload_json:
        raise HybridSnapshotContractError("prepared input fingerprint JSON is not canonical")
    if hashlib.sha256(prepared.input_fingerprint_payload_json.encode("utf-8")).hexdigest() != prepared.input_content_sha256:
        raise HybridSnapshotContractError("prepared input fingerprint does not match its digest")
    if _canonical_json(fingerprint_payload) != prepared.fingerprint_payload_json:
        raise HybridSnapshotContractError("prepared serving fingerprint JSON is not canonical")
    if hashlib.sha256(prepared.fingerprint_payload_json.encode("utf-8")).hexdigest() != prepared.fingerprint_sha256:
        raise HybridSnapshotContractError("prepared serving fingerprint does not match its digest")
    if fingerprint_payload.get("input_content_sha256") != prepared.input_content_sha256:
        raise HybridSnapshotContractError("prepared serving fingerprint has a different input digest")
    if fingerprint_payload.get("schema_version") != SERVING_SCHEMA_VERSION:
        raise HybridSnapshotContractError("prepared serving fingerprint schema is unsupported")
    if fingerprint_payload.get("user_id") != prepared.user_id:
        raise HybridSnapshotContractError("prepared serving fingerprint has a different user_id")
    if fingerprint_payload.get("source_run_id") != prepared.source_run_id:
        raise HybridSnapshotContractError("prepared serving fingerprint has a different run_id")
    if fingerprint_payload.get("model_version") != prepared.model_version:
        raise HybridSnapshotContractError("prepared serving fingerprint has a different model_version")
    if fingerprint_payload.get("graph_version") != prepared.graph_version:
        raise HybridSnapshotContractError("prepared serving fingerprint has a different graph_version")
    if fingerprint_payload.get("trend_source_namespace") != prepared.trend_source_namespace:
        raise HybridSnapshotContractError("prepared serving fingerprint has a different trend namespace")
    if fingerprint_payload.get("trend_source_scope") != prepared.trend_source_scope:
        raise HybridSnapshotContractError("prepared serving fingerprint has a different trend scope")
    if fingerprint_payload.get("trend_window_start") != _time_token(prepared.trend_window_start):
        raise HybridSnapshotContractError("prepared serving fingerprint has a different trend start")
    if fingerprint_payload.get("trend_window_end") != _time_token(prepared.trend_window_end):
        raise HybridSnapshotContractError("prepared serving fingerprint has a different trend end")
    if fingerprint_payload.get("trend_content_sha256") != prepared.trend_content_sha256:
        raise HybridSnapshotContractError("prepared serving fingerprint has a different trend digest")
    if fingerprint_payload.get("as_of_event_time") != _time_token(prepared.as_of_event_time):
        raise HybridSnapshotContractError("prepared serving fingerprint has a different event time")
    if fingerprint_payload.get("algorithm_version") != prepared.algorithm_version:
        raise HybridSnapshotContractError("prepared serving fingerprint has a different algorithm version")
    if (
        fingerprint_payload.get("mode") != prepared.mode
        or fingerprint_payload.get("reason") != prepared.reason
    ):
        raise HybridSnapshotContractError("prepared serving fingerprint has different ranking mode metadata")
    if fingerprint_payload.get("lambda_effective") != _float_token(prepared.lambda_effective):
        raise HybridSnapshotContractError("prepared serving fingerprint has a different effective lambda")
    config_payload = fingerprint_payload.get("ranking_config")
    expected_config = {
        "als_top_n": prepared.als_top_n,
        "trend_top_m": prepared.trend_top_m,
        "output_top_k": prepared.output_top_k,
        "lambda_weight": _float_token(prepared.lambda_requested),
        "epsilon": _float_token(prepared.epsilon),
        "algorithm_version": prepared.algorithm_version,
    }
    if config_payload != expected_config or _sha256(config_payload) != prepared.ranking_config_sha256:
        raise HybridSnapshotContractError("prepared ranking configuration does not match its fingerprint")
    if input_payload.get("ranking_config") != expected_config:
        raise HybridSnapshotContractError("prepared input has a different ranking configuration")
    input_source = input_payload.get("source", {})
    input_trend = input_payload.get("trend", {})
    if not isinstance(input_source, dict) or not isinstance(input_trend, dict):
        raise HybridSnapshotContractError("prepared input fingerprint is missing source objects")
    if (
        input_payload.get("user_id") != prepared.user_id
        or input_payload.get("as_of_event_time") != _time_token(prepared.as_of_event_time)
        or input_source.get("source_run_id") != prepared.source_run_id
        or input_source.get("model_version") != prepared.model_version
        or input_source.get("graph_version") != prepared.graph_version
    ):
        raise HybridSnapshotContractError("prepared input fingerprint has different source identity")
    if (
        input_trend.get("source_namespace") != prepared.trend_source_namespace
        or input_trend.get("source_scope") != prepared.trend_source_scope
        or input_trend.get("window_start") != _time_token(prepared.trend_window_start)
        or input_trend.get("window_end") != _time_token(prepared.trend_window_end)
        or input_trend.get("content_sha256") != prepared.trend_content_sha256
        or type(input_trend.get("schema_version")) not in (int, type(None))
        or type(input_trend.get("schema_version")) is int
        and input_trend.get("schema_version") != BUNDLE_SCHEMA_VERSION
        or input_trend.get("config_hash") is not None
        and (
            not isinstance(input_trend.get("config_hash"), str)
            or _SHA256_PATTERN.fullmatch(input_trend["config_hash"]) is None
        )
    ):
        raise HybridSnapshotContractError("prepared input fingerprint has different trend provenance")
    if (
        input_payload.get("mode") != prepared.mode
        or input_payload.get("reason") != prepared.reason
        or input_payload.get("lambda_effective") != _float_token(prepared.lambda_effective)
        or input_payload.get("schema_version") != SERVING_SCHEMA_VERSION
    ):
        raise HybridSnapshotContractError("prepared input fingerprint has different ranking metadata")
    if not prepared.rows and (not prepared.mode or not prepared.reason):
        raise HybridSnapshotContractError("empty prepared snapshot requires a mode and reason")
    if len(prepared.rows) > min(MAX_FINAL_RECOMMENDATIONS, prepared.output_top_k):
        raise HybridSnapshotContractError("prepared snapshot exceeds resolved Top-K")
    if (prepared.trend_window_start is None) != (prepared.trend_window_end is None):
        raise HybridSnapshotContractError("prepared trend window bounds must both be present or absent")
    if prepared.trend_window_start is not None:
        start = aware_utc_millisecond(prepared.trend_window_start, "prepared.trend_window_start", error=HybridSnapshotContractError)
        end = aware_utc_millisecond(prepared.trend_window_end, "prepared.trend_window_end", error=HybridSnapshotContractError)
        if start >= end or end > prepared.as_of_event_time:
            raise HybridSnapshotContractError("prepared trend window is inconsistent with its event time")
    if prepared.trend_source_scope is not None:
        _nonempty_string(prepared.trend_source_scope, "prepared.trend_source_scope", maximum=128)
    if prepared.trend_content_sha256 is not None and _SHA256_PATTERN.fullmatch(prepared.trend_content_sha256) is None:
        raise HybridSnapshotContractError("prepared trend content digest is invalid")
    canonical_rows = tuple(
        _canonical_recommendation(row, index) for index, row in enumerate(prepared.rows)
    )
    if canonical_rows != prepared.rows:
        raise HybridSnapshotContractError("prepared rows are not canonical recommendation records")
    if any(row.user_id != prepared.user_id for row in prepared.rows):
        raise HybridSnapshotContractError("prepared snapshot rows contain a different user")
    if len({row.product_id for row in prepared.rows}) != len(prepared.rows):
        raise HybridSnapshotContractError("prepared snapshot contains duplicate products")
    if [row.rank for row in prepared.rows] != list(range(1, len(prepared.rows) + 1)):
        raise HybridSnapshotContractError("prepared snapshot ranks are not contiguous")
    expected_rows = [
        {
            "rank": row.rank,
            "product_id": row.product_id,
            "product_name": row.product_name,
            "als_score": _float_token(row.als_score),
            "trend_score": _float_token(row.trend_score),
            "normalized_als_score": _float_token(row.normalized_als_score),
            "normalized_trend_score": _float_token(row.normalized_trend_score),
            "final_score": _float_token(row.final_score),
            "from_als": row.from_als,
            "from_trend": row.from_trend,
        }
        for row in prepared.rows
    ]
    if fingerprint_payload.get("recommendations") != expected_rows:
        raise HybridSnapshotContractError("prepared rows do not match the serving fingerprint")


def _assert_pointer_source_matches(
    previous: Mapping[str, Any], prepared: PreparedHybridSnapshot
) -> None:
    if (
        previous["source_run_id"] != prepared.source_run_id
        or previous["model_version"] != prepared.model_version
        or previous["graph_version"] != prepared.graph_version
    ):
        raise HybridPublicationConflictError(
            "cannot order a new publication across changed A/B source generations"
        )
    if previous["trend_source_namespace"] != prepared.trend_source_namespace:
        raise HybridPublicationConflictError(
            "cannot order a new publication across trend source namespaces"
        )


def _compare_and_promote(
    database: Any,
    prepared: PreparedHybridSnapshot,
    updated_at: datetime,
) -> tuple[str, bool]:
    readiness = database[READINESS_COLLECTION]
    pointer_id = _pointer_id(prepared.user_id)
    try:
        existing_raw = readiness.find_one({"_id": pointer_id})
    except Exception as exc:
        raise HybridPublicationPromotionError("failed to read per-user readiness pointer") from exc
    desired = pointer_document(prepared, updated_at)

    if existing_raw is None:
        try:
            readiness.insert_one(desired)
            inserted_pointer = readiness.find_one({"_id": pointer_id})
            if isinstance(inserted_pointer, Mapping) and _pointer_matches(
                inserted_pointer, prepared
            ):
                return "published", False
            raise HybridPublicationPromotionError(
                "initial per-user pointer insert was not visible after acknowledgement"
            )
        except Exception as exc:
            if isinstance(exc, HybridPublicationPromotionError):
                raise
            try:
                after_error = readiness.find_one({"_id": pointer_id})
            except Exception as read_error:
                raise HybridPublicationPromotionError(
                    "pointer insert failed and resulting state could not be verified"
                ) from read_error
            if isinstance(after_error, Mapping) and _pointer_matches(after_error, prepared):
                return "already_active", True
            if after_error is None:
                raise HybridPublicationPromotionError("initial per-user pointer insert failed") from exc
            existing_raw = after_error

    previous = _validate_existing_pointer(existing_raw, prepared.user_id)
    _assert_pointer_source_matches(previous, prepared)
    if previous["serving_version"] == prepared.serving_version:
        if not _pointer_matches(previous, prepared):
            raise HybridPublicationConflictError(
                "same serving_version is already active with conflicting pointer content"
            )
        return "already_active", True

    previous_time = previous["as_of_event_time"]
    if previous_time > prepared.as_of_event_time:
        raise HybridPublicationConflictError("stale as_of_event_time cannot replace a newer pointer")
    if previous_time == prepared.as_of_event_time:
        raise HybridPublicationConflictError(
            "different serving content cannot replace the pointer at the same as_of_event_time"
        )

    filter_query = {
        "_id": pointer_id,
        "serving_version": previous["serving_version"],
        "expected_rows": previous["expected_rows"],
        "source_run_id": previous["source_run_id"],
        "model_version": previous["model_version"],
        "graph_version": previous["graph_version"],
        "trend_source_namespace": previous["trend_source_namespace"],
        "input_content_sha256": previous["input_content_sha256"],
        "as_of_event_time": previous["as_of_event_time"],
    }
    try:
        result = readiness.replace_one(filter_query, desired, upsert=False)
    except Exception as exc:
        try:
            after_error = readiness.find_one({"_id": pointer_id})
        except Exception as read_error:
            raise HybridPublicationPromotionError(
                "pointer promotion failed and resulting state could not be verified"
            ) from read_error
        if isinstance(after_error, Mapping) and _pointer_matches(after_error, prepared):
            return "published", False
        raise HybridPublicationPromotionError("per-user pointer promotion failed") from exc
    if getattr(result, "matched_count", 0) == 1:
        try:
            promoted_pointer = readiness.find_one({"_id": pointer_id})
        except Exception as exc:
            raise HybridPublicationPromotionError(
                "pointer promotion succeeded but resulting state could not be verified"
            ) from exc
        if isinstance(promoted_pointer, Mapping) and _pointer_matches(promoted_pointer, prepared):
            return "published", False
        raise HybridPublicationPromotionError(
            "pointer promotion was acknowledged but new pointer content did not verify"
        )

    try:
        after_cas = readiness.find_one({"_id": pointer_id})
    except Exception as exc:
        raise HybridPublicationPromotionError(
            "pointer compare-and-set missed and resulting state could not be read"
        ) from exc
    if isinstance(after_cas, Mapping) and _pointer_matches(after_cas, prepared):
        return "already_active", True
    raise HybridPublicationPromotionError("per-user pointer changed during compare-and-set")


def _publish_with_client(
    client: Any,
    config: Module4Config,
    prepared: PreparedHybridSnapshot,
    processing_time: datetime,
) -> HybridPublicationResult:
    try:
        database = client[config.mongo_database]
        row_collection = database[FINAL_RECOMMENDATIONS_COLLECTION]
    except Exception as exc:
        raise HybridPublicationWriteError("could not select final recommendation collection") from exc

    generated_at = _write_and_verify_rows(row_collection, prepared, processing_time)
    operation_id = f"module4c-hybrid-publish-{prepared.user_id}-{prepared.serving_version}"
    try:
        with module4_sink.publication_lock(config, operation_id) as lock_client:
            locked_database = lock_client[config.mongo_database]
            _validate_active_ab_readiness(locked_database, prepared)
            status, idempotent = _compare_and_promote(
                locked_database, prepared, processing_time
            )
    except HybridPublisherError:
        raise
    except Exception as exc:
        raise HybridPublicationPromotionError(
            "could not complete the locked readiness and pointer promotion section"
        ) from exc

    return HybridPublicationResult(
        status=status,
        serving_version=prepared.serving_version,
        user_id=prepared.user_id,
        expected_rows=prepared.expected_rows,
        input_content_sha256=prepared.input_content_sha256,
        mode=prepared.mode,
        reason=prepared.reason,
        generated_at=generated_at,
        pointer_id=_pointer_id(prepared.user_id),
        idempotent=idempotent,
    )


def publish_hybrid_recommendations(
    config: Module4Config,
    prepared: PreparedHybridSnapshot,
    *,
    client: Any | None = None,
    processing_time_utc: datetime | None = None,
) -> HybridPublicationResult:
    """Write immutable rows, verify them, then promote the per-user pointer.

    The global Module 4 publication lock is held only for the active A/B source
    recheck and per-user pointer compare-and-set. Recommendation ranking and
    row writes occur before entering that short critical section.
    """
    if not isinstance(config, Module4Config):
        raise HybridSnapshotContractError("config must be a Module4 Config")
    if not isinstance(prepared, PreparedHybridSnapshot):
        raise HybridSnapshotContractError("prepared must be PreparedHybridSnapshot")
    _validate_prepared_snapshot(prepared)
    if processing_time_utc is None:
        now = datetime.now(timezone.utc)
        processing_time = now.replace(microsecond=(now.microsecond // 1000) * 1000)
    else:
        processing_time = aware_utc_millisecond(processing_time_utc, "processing_time_utc", error=HybridSnapshotContractError)
    if client is not None:
        return _publish_with_client(client, config, prepared, processing_time)

    from pymongo import MongoClient

    try:
        with MongoClient(config.mongodb_uri, serverSelectionTimeoutMS=10000) as owned_client:
            owned_client.admin.command("ping")
            return _publish_with_client(owned_client, config, prepared, processing_time)
    except HybridPublisherError:
        raise
    except Exception as exc:
        raise HybridPublicationWriteError("MongoDB final recommendation publication failed") from exc


__all__ = [
    "FINAL_RECOMMENDATIONS_COLLECTION",
    "HybridPublicationConflictError",
    "HybridPublicationPromotionError",
    "HybridPublicationResult",
    "HybridPublicationWriteError",
    "HybridPublisherError",
    "HybridSnapshotContractError",
    "PreparedHybridSnapshot",
    "ensure_final_recommendation_indexes",
    "prepare_hybrid_snapshot",
    "publish_hybrid_recommendations",
]
