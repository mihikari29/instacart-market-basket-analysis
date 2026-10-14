"""Fixtures built from the accepted Module 4C ranking and publication contracts."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib

from src.module4.hybrid import (
    ALSRecommendation,
    CapturedUserSources,
    HybridRankingConfig,
    ProductMetadata,
    TrendRecommendation,
    TrendSnapshotReadResult,
    VerifiedSourceContext,
    rank_hybrid_recommendations,
)
from src.module4.hybrid.sink_mongo import _pointer_document, _row_document, prepare_hybrid_snapshot


BASE_TIME = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
NAMESPACE = "d1a:module3:fixture-v1"
SCOPE = "historical_fixture"
RUN_ID = "d1a-run-001"
MODEL_VERSION = "d1a-als-v1"
GRAPH_VERSION = "d1a-graph-v1"


def make_snapshot(
    user_id: int = 71,
    *,
    als_count: int = 2,
    trend_count: int = 2,
    trend_available: bool = True,
    trend_result: TrendSnapshotReadResult | None = None,
    as_of: datetime | None = None,
    generated_at: datetime | None = None,
):
    """Create documents using C1-C6 types and the publisher's document builders."""
    at = as_of or BASE_TIME
    published = generated_at or BASE_TIME + timedelta(minutes=1)
    products = tuple(ProductMetadata(pid, f"fixture product {pid}") for pid in range(100, 140))
    als = tuple(
        ALSRecommendation(user_id, 100 + index, float(10 - index), index + 1)
        for index in range(als_count)
    )
    trend_rows = tuple(
        TrendRecommendation(110 + index, float(0.95 - index * 0.05), index + 1)
        for index in range(trend_count)
    )
    trend_start = at - timedelta(hours=2)
    trend_end = at - timedelta(hours=1)
    digest = hashlib.sha256(b"d1a trend content").hexdigest() if trend_available else None
    trend = trend_result or TrendSnapshotReadResult(
        available=trend_available,
        status="available" if trend_available else "missing",
        reason=None if trend_available else "no_matching_snapshot",
        source_namespace=NAMESPACE,
        source_scope=SCOPE if trend_available else None,
        window_start=trend_start if trend_available else None,
        window_end=trend_end if trend_available else None,
        published_at=published if trend_available else None,
        expires_at=None,
        schema_version=1 if trend_available else None,
        config_hash=hashlib.sha256(b"d1a trend config").hexdigest() if trend_available else None,
        content_sha256=digest,
        trend_recommendations=trend_rows if trend_available else (),
    )
    captured = CapturedUserSources(
        user_id=user_id,
        source_context=VerifiedSourceContext(als_available=True, graph_available=True),
        als_recommendations=als,
        product_catalog=products,
        missing_prospective_trend_product_ids=(),
        run_id=RUN_ID,
        model_version=MODEL_VERSION,
        graph_version=GRAPH_VERSION,
    )
    ranking_config = HybridRankingConfig(lambda_weight=0.55)
    ranked = rank_hybrid_recommendations(
        user_id,
        captured.als_recommendations,
        trend.trend_snapshot,
        captured.product_catalog,
        captured.source_context,
        ranking_config,
    )
    prepared = prepare_hybrid_snapshot(
        captured,
        trend,
        ranked,
        ranking_config,
        as_of_event_time=at,
    )
    rows = [_row_document(prepared, row, published) for row in prepared.rows]
    pointer = _pointer_document(prepared, published)
    return prepared, pointer, rows, trend
