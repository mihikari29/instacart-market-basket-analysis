"""Stable JSON response and error schemas for the Module 4D API."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

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


class ALSRecommendationItem(StrictResponse):
    product_id: StrictInt = Field(gt=0)
    product_name: StrictStr | None
    als_rank: StrictInt = Field(ge=1, le=20)
    als_score: FiniteFloat


class ALSRecommendationSource(StrictResponse):
    run_id: StrictStr
    model_version: StrictStr
    graph_version: StrictStr


class ALSRecommendationResponse(StrictResponse):
    user_id: StrictInt = Field(gt=0)
    model_version: StrictStr
    status: Literal["available", "empty"]
    reason: StrictStr | None
    recommendation_count: StrictInt = Field(ge=0, le=20)
    requested_limit: StrictInt = Field(ge=1, le=20)
    has_more: StrictBool
    source: ALSRecommendationSource
    recommendations: list[ALSRecommendationItem] = Field(max_length=20)
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


class BatchProductItem(StrictResponse):
    product_id: StrictInt = Field(gt=0)
    purchase_count: StrictInt = Field(gt=0)
    unique_users: StrictInt = Field(gt=0)
    reorder_count: StrictInt = Field(ge=0)
    reorder_rate: FiniteFloat = Field(ge=0, le=1)
    run_id: StrictStr
    last_batch_time: datetime


class BatchDepartmentItem(StrictResponse):
    department_id: StrictInt = Field(ge=1, le=21)
    purchase_count: StrictInt = Field(gt=0)
    unique_users: StrictInt = Field(gt=0)
    reorder_rate: FiniteFloat = Field(ge=0, le=1)
    run_id: StrictStr
    last_batch_time: datetime


class BatchSource(StrictResponse):
    collection: StrictStr
    scope: Literal["prior-only item purchases"]
    run_id: StrictStr | None
    last_batch_time: datetime | None
    atomic_with_other_collections: Literal[False]


class BatchProductsResponse(StrictResponse):
    order: Literal["purchase_count", "reorder_rate"]
    items: list[BatchProductItem] = Field(max_length=50)
    source: BatchSource
    metric_definition: dict[str, StrictStr]
    observed_at: datetime


class BatchDepartmentsResponse(StrictResponse):
    items: list[BatchDepartmentItem] = Field(max_length=21)
    source: BatchSource
    metric_definition: dict[str, StrictStr]
    observed_at: datetime


class EvidenceRegistryItem(StrictResponse):
    evidence_id: StrictStr
    title: StrictStr
    kind: StrictStr
    classification: StrictStr
    source_label: StrictStr
    available: StrictBool
    status: Literal["available", "missing", "invalid"]
    sha256: StrictStr | None


class EvidenceRegistryResponse(StrictResponse):
    items: list[EvidenceRegistryItem] = Field(max_length=20)
    observed_at: datetime


class EvidenceProvenance(StrictResponse):
    artifact_key: StrictStr
    source_label: StrictStr
    classification: StrictStr
    archived: StrictBool
    sha256: StrictStr


class EvidenceResponse(StrictResponse):
    evidence_id: StrictStr
    title: StrictStr
    kind: StrictStr
    provenance: EvidenceProvenance
    data: dict[str, Any]
    observed_at: datetime


class OrderDayBin(StrictResponse):
    order_dow: StrictInt = Field(ge=0, le=6)
    order_count: StrictInt = Field(ge=0)


class OrderHourBin(StrictResponse):
    order_hour_of_day: StrictInt = Field(ge=0, le=23)
    order_count: StrictInt = Field(ge=0)


class OrdersDistributionResponse(StrictResponse):
    scope: Literal["all orders"]
    measure: Literal["order count, not item-level prior purchases"]
    total_orders: StrictInt = Field(gt=0)
    orders_by_day_of_week: list[OrderDayBin] = Field(min_length=7, max_length=7)
    orders_by_hour: list[OrderHourBin] = Field(min_length=24, max_length=24)
    provenance: EvidenceProvenance
    observed_at: datetime


class EvaluationMetric(StrictResponse):
    k: StrictInt = Field(ge=1, le=100)
    status: Literal["evaluated"]
    precision_at_k: FiniteFloat = Field(ge=0, le=1)
    recall_at_k: FiniteFloat = Field(ge=0, le=1)
    map_at_k: FiniteFloat = Field(ge=0, le=1)
    ndcg_at_k: FiniteFloat = Field(ge=0, le=1)
    evaluated_users: StrictInt = Field(gt=0)
    user_coverage: FiniteFloat | None
    catalog_coverage_rate: FiniteFloat | None
    catalog_coverage: StrictInt | None = None
    users_with_no_recommendations: StrictInt | None = None


class EvaluationCohort(StrictResponse):
    definition: StrictStr | None
    users: StrictInt = Field(gt=0)
    metrics: dict[str, dict[str, EvaluationMetric]]


class AlsEvaluationResponse(StrictResponse):
    evidence_id: StrictStr
    title: StrictStr
    provenance: EvidenceProvenance
    algorithm_scope: Literal["ALS and recorded baselines"]
    evaluation_method: dict[str, StrictStr]
    ks: list[StrictInt] = Field(min_length=3, max_length=3)
    cohorts: dict[str, EvaluationCohort]
    history_subgroups: dict[str, EvaluationCohort]
    als_vs_baseline_deltas: dict[str, dict[str, dict[str, dict[str, FiniteFloat]]]]
    official_test_truth_available: StrictBool
    truth_summary: dict[str, StrictInt]
    observed_at: datetime
