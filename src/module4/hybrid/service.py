"""Single-user orchestration for the verified Module 4C serving path."""

from __future__ import annotations
from ...common.runtime import aware_utc_millisecond

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from src.module3.trend_bundle import BUNDLE_COLLECTION_NAME
from src.module4.config import Config as Module4Config

from .config import HybridRankingConfig
from .data import CapturedUserSources, capture_verified_user_sources
from .ranking import rank_hybrid_recommendations
from .sink_mongo import (
    HybridPublicationResult,
    PreparedHybridSnapshot,
    prepare_hybrid_snapshot,
    publish_hybrid_recommendations,
)
from .trend_reader import TrendSnapshotReadResult, read_verified_trend_snapshot


_SUPPORTED_TREND_STATUSES = frozenset({"available", "missing", "expired", "stale"})
_UNAVAILABLE_TREND_STATUSES = frozenset({"missing", "expired", "stale"})
_READY_PUBLICATION_STATUSES = frozenset({"published", "already_active"})


class HybridServingRequestError(ValueError):
    """The requested single-user serving inputs are invalid."""


class HybridServingContractError(RuntimeError):
    """An upstream C1-C6 result violates the serving orchestration contract."""


class HybridServingPublicationError(RuntimeError):
    """C6 failed or did not confirm this user's final ready pointer."""


@dataclass(frozen=True, slots=True)
class HybridServingResult:
    """Verified metadata for one published per-user recommendation snapshot."""

    user_id: int
    as_of_event_time: datetime
    serving_version: str
    recommendation_count: int
    mode: str
    reason: str | None
    source_run_id: str
    model_version: str
    graph_version: str
    trend_available: bool
    trend_status: str
    trend_reason: str | None
    trend_source_namespace: str
    trend_source_scope: str | None
    trend_window_start: datetime | None
    trend_window_end: datetime | None
    trend_published_at: datetime | None
    trend_expires_at: datetime | None
    trend_schema_version: int | None
    trend_config_hash: str | None
    trend_content_sha256: str | None
    candidate_count: int
    excluded_trend_product_ids: tuple[int, ...]
    publication_status: str
    readiness_pointer_id: str
    readiness_confirmed: bool
    generated_at: datetime
    idempotent: bool



def _validate_request(
    config: Module4Config,
    user_id: Any,
    source_namespace: Any,
    as_of_event_time: Any,
    ranking_config: Any,
    max_event_age: Any,
    processing_time_utc: Any,
) -> tuple[datetime, datetime | None]:
    if not isinstance(config, Module4Config):
        raise HybridServingRequestError("config must be a Module 4 Config")
    if type(user_id) is not int or user_id <= 0:
        raise HybridServingRequestError("user_id must be a positive integer")
    if (
        not isinstance(source_namespace, str)
        or not source_namespace
        or source_namespace != source_namespace.strip()
        or len(source_namespace) > 256
        or "\x00" in source_namespace
    ):
        raise HybridServingRequestError("source_namespace must be a stable non-empty string")
    if not isinstance(ranking_config, HybridRankingConfig):
        raise HybridServingRequestError("ranking_config must be a HybridRankingConfig")
    event_time = aware_utc_millisecond(as_of_event_time, "as_of_event_time", error=HybridServingRequestError)
    if max_event_age is not None and (
        not isinstance(max_event_age, timedelta) or max_event_age < timedelta(0)
    ):
        raise HybridServingRequestError("max_event_age must be a nonnegative timedelta or None")
    processing_time = (
        None
        if processing_time_utc is None
        else aware_utc_millisecond(processing_time_utc, "processing_time_utc", error=HybridServingRequestError)
    )
    return event_time, processing_time


def _validate_trend_result(result: Any, requested_namespace: str) -> TrendSnapshotReadResult:
    if not isinstance(result, TrendSnapshotReadResult):
        raise HybridServingContractError("C5 did not return a TrendSnapshotReadResult")
    if not isinstance(result.status, str) or result.status not in _SUPPORTED_TREND_STATUSES:
        raise HybridServingContractError(
            f"C5 returned unsupported trend availability status {result.status!r}"
        )
    if type(result.available) is not bool:
        raise HybridServingContractError("C5 trend availability must be a boolean")
    if result.available is not (result.status == "available"):
        raise HybridServingContractError("C5 trend status and availability disagree")
    if result.source_namespace != requested_namespace:
        raise HybridServingContractError("C5 returned a different trend source namespace")
    if result.status in _UNAVAILABLE_TREND_STATUSES and result.reason is None:
        raise HybridServingContractError("unavailable C5 trend result must preserve its reason")
    if result.status == "available" and result.reason is not None:
        raise HybridServingContractError("available C5 trend result cannot have an unavailable reason")
    return result


def _validate_publication(
    publication: Any,
    prepared: PreparedHybridSnapshot,
) -> HybridPublicationResult:
    if not isinstance(publication, HybridPublicationResult):
        raise HybridServingPublicationError("C6 did not return a HybridPublicationResult")
    expected_pointer_id = f"hybrid:{prepared.user_id}"
    if publication.status not in _READY_PUBLICATION_STATUSES:
        raise HybridServingPublicationError(
            f"C6 returned non-ready publication status {publication.status!r}"
        )
    if (
        publication.user_id != prepared.user_id
        or publication.serving_version != prepared.serving_version
        or publication.expected_rows != prepared.expected_rows
        or publication.input_content_sha256 != prepared.input_content_sha256
        or publication.mode != prepared.mode
        or publication.reason != prepared.reason
        or publication.pointer_id != expected_pointer_id
    ):
        raise HybridServingPublicationError(
            "C6 publication identity does not match the prepared user snapshot"
        )
    return publication


def _mongo_client(config: Module4Config) -> Any:
    from pymongo import MongoClient

    return MongoClient(config.mongodb_uri, serverSelectionTimeoutMS=10000)


def serve_hybrid_user(
    config: Module4Config,
    *,
    user_id: int,
    source_namespace: str,
    as_of_event_time: datetime,
    ranking_config: HybridRankingConfig,
    max_event_age: timedelta | None = None,
    processing_time_utc: datetime | None = None,
    client: Any | None = None,
    trend_collection: Any | None = None,
) -> HybridServingResult:
    """Capture, rank, prepare and publish recommendations for one user.

    ``as_of_event_time`` is required and never defaults to wall-clock time.
    C3 owns its accepted short publication-lock scope; C6 owns final A/B
    generation verification and the per-user readiness pointer promotion.
    """
    event_time, processing_time = _validate_request(
        config,
        user_id,
        source_namespace,
        as_of_event_time,
        ranking_config,
        max_event_age,
        processing_time_utc,
    )

    owned_client = None
    serving_client = client
    if trend_collection is None and serving_client is None:
        owned_client = _mongo_client(config)
        serving_client = owned_client

    try:
        if trend_collection is None:
            trend_collection = serving_client[config.mongo_database][BUNDLE_COLLECTION_NAME]
        trend_result = _validate_trend_result(
            read_verified_trend_snapshot(
                trend_collection,
                source_namespace=source_namespace,
                as_of_event_time=event_time,
                max_event_age=max_event_age,
                processing_time_utc=processing_time,
            ),
            source_namespace,
        )

        prospective_ids = (
            tuple(row.product_id for row in trend_result.trend_recommendations)
            if trend_result.available
            else ()
        )
        captured = capture_verified_user_sources(
            config,
            user_id,
            prospective_trend_product_ids=prospective_ids,
        )
        if not isinstance(captured, CapturedUserSources):
            raise HybridServingContractError("C3 did not return CapturedUserSources")

        ranking_result = rank_hybrid_recommendations(
            user_id,
            captured.als_recommendations,
            trend_result.trend_snapshot,
            captured.product_catalog,
            captured.source_context,
            ranking_config,
        )
        prepared = prepare_hybrid_snapshot(
            captured,
            trend_result,
            ranking_result,
            ranking_config,
            as_of_event_time=event_time,
        )
        try:
            publication = publish_hybrid_recommendations(
                config,
                prepared,
                client=serving_client,
                processing_time_utc=processing_time,
            )
        except Exception as exc:
            raise HybridServingPublicationError(
                f"C6 publication failed for user {user_id} and serving version "
                f"{prepared.serving_version}"
            ) from exc
        publication = _validate_publication(publication, prepared)

        metadata = ranking_result.metadata
        return HybridServingResult(
            user_id=user_id,
            as_of_event_time=prepared.as_of_event_time,
            serving_version=publication.serving_version,
            recommendation_count=publication.expected_rows,
            mode=publication.mode,
            reason=publication.reason,
            source_run_id=prepared.source_run_id,
            model_version=prepared.model_version,
            graph_version=prepared.graph_version,
            trend_available=trend_result.available,
            trend_status=trend_result.status,
            trend_reason=trend_result.reason,
            trend_source_namespace=trend_result.source_namespace,
            trend_source_scope=trend_result.source_scope,
            trend_window_start=trend_result.window_start,
            trend_window_end=trend_result.window_end,
            trend_published_at=trend_result.published_at,
            trend_expires_at=trend_result.expires_at,
            trend_schema_version=trend_result.schema_version,
            trend_config_hash=trend_result.config_hash,
            trend_content_sha256=trend_result.content_sha256,
            candidate_count=metadata.candidate_count,
            excluded_trend_product_ids=metadata.excluded_trend_product_ids,
            publication_status=publication.status,
            readiness_pointer_id=publication.pointer_id,
            readiness_confirmed=True,
            generated_at=publication.generated_at,
            idempotent=publication.idempotent,
        )
    finally:
        if owned_client is not None:
            owned_client.close()


__all__ = [
    "HybridServingContractError",
    "HybridServingPublicationError",
    "HybridServingRequestError",
    "HybridServingResult",
    "serve_hybrid_user",
]
