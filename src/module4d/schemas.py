"""Stable JSON response and error schemas for the Module 4D API."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, FiniteFloat, StrictBool, StrictInt, StrictStr


class StrictResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


class ErrorDetail(StrictResponse):
    code: StrictStr
    message: StrictStr
    request_id: StrictStr


class ErrorEnvelope(StrictResponse):
    error: ErrorDetail


class HealthResponse(StrictResponse):
    status: Literal["ok"]


class RankingConfigResponse(StrictResponse):
    algorithm_version: StrictStr
    lambda_requested: FiniteFloat | None
    lambda_effective: FiniteFloat | None
    als_top_n: StrictInt = Field(ge=1, le=20)
    trend_top_m: StrictInt = Field(ge=1, le=20)
    output_top_k: StrictInt = Field(ge=1, le=20)
    epsilon: FiniteFloat = Field(gt=0)
    ranking_config_sha256: StrictStr


class RecommendationItem(StrictResponse):
    product_id: StrictInt = Field(gt=0)
    product_name: StrictStr = Field(max_length=4096)
    rank: StrictInt = Field(ge=1, le=20)
    from_als: StrictBool
    from_trend: StrictBool
    als_score: FiniteFloat | None
    trend_score: FiniteFloat | None
    normalized_als_score: FiniteFloat = Field(ge=0, le=1)
    normalized_trend_score: FiniteFloat = Field(ge=0, le=1)
    final_score: FiniteFloat = Field(ge=0, le=1)


class HybridSourceResponse(StrictResponse):
    source_run_id: StrictStr
    model_version: StrictStr
    graph_version: StrictStr
    trend_source_namespace: StrictStr
    trend_source_scope: StrictStr | None
    trend_window_start: datetime | None
    trend_window_end: datetime | None
    trend_content_sha256: StrictStr | None
    as_of_event_time: datetime
    input_content_sha256: StrictStr
    pointer_updated_at: datetime


class RecommendationResponse(StrictResponse):
    user_id: StrictInt = Field(gt=0)
    serving_version: StrictStr
    status: Literal["ready"]
    recommendation_count: StrictInt = Field(ge=0, le=20)
    mode: StrictStr
    reason: StrictStr | None
    source: HybridSourceResponse
    ranking_config: RankingConfigResponse
    generated_at: datetime | None
    recommendations: list[RecommendationItem] = Field(max_length=20)
    observed_at: datetime


class TrendItem(StrictResponse):
    product_id: StrictInt = Field(gt=0)
    trend_score: FiniteFloat = Field(ge=0, le=1)
    trend_rank: StrictInt = Field(ge=1, le=20)


class TrendResponse(StrictResponse):
    available: StrictBool
    status: Literal["available", "missing", "expired", "stale"]
    reason: StrictStr | None
    source_namespace: StrictStr
    source_scope: StrictStr | None
    window_start: datetime | None
    window_end: datetime | None
    published_at: datetime | None
    expires_at: datetime | None
    schema_version: StrictInt | None
    config_hash: StrictStr | None
    content_sha256: StrictStr | None
    row_count: StrictInt = Field(ge=0, le=20)
    items: list[TrendItem] = Field(max_length=20)
    observed_at: datetime
