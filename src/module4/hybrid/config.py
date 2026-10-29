"""Configuration and immutable data contracts for the Module 4C ranker."""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any, Mapping

from pathlib import Path

from ...common.runtime import config_section, finite_number, read_yaml_mapping

@dataclass(frozen=True, slots=True)
class HybridRankingConfig:
    """Validated limits and scoring parameters for deterministic hybrid ranking."""

    als_top_n: int = 20
    trend_top_m: int = 20
    output_top_k: int = 20
    lambda_weight: float = 0.7
    alpha: float = 0.5
    beta: float = 0.0
    gamma: float = 0.5
    epsilon: float = 1e-12
    algorithm_version: str = "hybrid-v1"

    def __post_init__(self) -> None:
        for name in ("als_top_n", "trend_top_m", "output_top_k"):
            value = getattr(self, name)
            if type(value) is not int or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
            if value > 20:
                raise ValueError(f"{name} must not exceed 20")

        if isinstance(self.lambda_weight, bool) or not isinstance(self.lambda_weight, (int, float)):
            raise ValueError("lambda_weight must be a finite number between 0 and 1")
        try:
            lambda_weight = float(self.lambda_weight)
        except (OverflowError, ValueError) as exc:
            raise ValueError("lambda_weight must be a finite number between 0 and 1") from exc
        if not math.isfinite(lambda_weight) or not 0.0 <= lambda_weight <= 1.0:
            raise ValueError("lambda_weight must be a finite number between 0 and 1")

        for name, value in (("alpha", self.alpha), ("beta", self.beta), ("gamma", self.gamma)):
            weight = finite_number(value, name)
            if not 0.0 <= weight <= 1.0:
                raise ValueError(f"{name} must be between 0 and 1")
        if self.beta > 0 and abs(self.alpha + self.beta + self.gamma - 1.0) > 1e-9:
            raise ValueError("active alpha, beta and gamma weights must sum to 1")

        if isinstance(self.epsilon, bool) or not isinstance(self.epsilon, (int, float)):
            raise ValueError("epsilon must be finite and strictly positive")
        try:
            epsilon = float(self.epsilon)
        except (OverflowError, ValueError) as exc:
            raise ValueError("epsilon must be finite and strictly positive") from exc
        if not math.isfinite(epsilon) or epsilon <= 0.0:
            raise ValueError("epsilon must be finite and strictly positive")

        if not isinstance(self.algorithm_version, str) or not self.algorithm_version.strip():
            raise ValueError("algorithm_version must be a non-empty string")

    @classmethod
    def from_pipeline_config(cls, path: Any = None) -> "HybridRankingConfig":
        cfg_path = Path(path) if path is not None else Path("config/pipeline.yaml")
        if path is None and not cfg_path.exists():
            return cls()
        hybrid_cfg = config_section(read_yaml_mapping(cfg_path), "models", "hybrid")
        if set(hybrid_cfg) - {"alpha", "beta", "gamma", "output_top_k", "window_minutes"}:
            raise ValueError("models.hybrid contains unsupported settings")
        defaults = cls()
        weights = {}
        for name, default in (("alpha", defaults.alpha), ("beta", defaults.beta), ("gamma", defaults.gamma)):
            value = hybrid_cfg.get(name, default)
            if isinstance(value, bool):
                raise ValueError(f"models.hybrid.{name} must be a finite number")
            try:
                weights[name] = float(value)
            except (TypeError, ValueError, OverflowError):
                raise ValueError(f"models.hybrid.{name} must be a finite number") from None
        count = hybrid_cfg.get("output_top_k", defaults.output_top_k)
        if isinstance(count, bool) or isinstance(count, float) and not count.is_integer():
            raise ValueError("models.hybrid.output_top_k must be an integer")
        try:
            output_top_k = int(count)
        except (TypeError, ValueError, OverflowError):
            raise ValueError("models.hybrid.output_top_k must be an integer") from None
        return cls(**weights, output_top_k=output_top_k)


@dataclass(frozen=True, slots=True)
class VerifiedSourceContext:
    """Readiness assertion supplied by a future source reader.

    The pure ranker requires both Module 4A and 4B snapshots to have been
    verified. It does not perform readiness or database checks itself.
    """

    als_available: bool
    graph_available: bool


@dataclass(frozen=True, slots=True)
class ALSRecommendation:
    """One Module 4A row using its published field names."""

    user_id: int
    product_id: int
    als_score: float | None
    als_rank: int


@dataclass(frozen=True, slots=True)
class TrendRecommendation:
    """One Module 3 row's ranking fields; window/count fields may be carried separately."""

    product_id: int
    trend_score: float
    trend_rank: int


@dataclass(frozen=True, slots=True)
class TrendSnapshot:
    """Explicitly available finalized trend rows or an unavailable snapshot.

    ``available=True`` means the caller has verified a finalized snapshot. An
    available snapshot may contain zero rows. An unavailable snapshot must not
    carry rows and can include a diagnostic reason.
    """

    available: bool
    rows: tuple[TrendRecommendation | Mapping[str, Any], ...] = ()
    window_start: Any | None = None
    window_end: Any | None = None
    unavailable_reason: str | None = None

    def __post_init__(self) -> None:
        try:
            object.__setattr__(self, "rows", tuple(self.rows))
        except TypeError as exc:
            raise ValueError("trend snapshot rows must be iterable") from exc
        if type(self.available) is not bool:
            raise ValueError("trend snapshot available must be a boolean")
        if not self.available and self.rows:
            raise ValueError("unavailable trend snapshot cannot contain rows")
        if self.unavailable_reason is not None and (
            not isinstance(self.unavailable_reason, str) or not self.unavailable_reason.strip()
        ):
            raise ValueError("unavailable_reason must be None or a non-empty string")
        if self.available and self.unavailable_reason is not None:
            raise ValueError("available trend snapshot cannot have an unavailable_reason")


@dataclass(frozen=True, slots=True)
class ProductMetadata:
    """Verified catalog metadata using Module 4A's product catalog field names."""

    product_id: int
    product_name: str
    pagerank: float | None = None


@dataclass(frozen=True, slots=True)
class Recommendation:
    """One final hybrid recommendation, before database identity/publication fields."""

    user_id: int
    product_id: int
    product_name: str
    als_score: float | None
    trend_score: float | None
    normalized_als_score: float
    normalized_trend_score: float
    final_score: float
    rank: int
    from_als: bool
    from_trend: bool
    pagerank_score: float | None = None
    normalized_pagerank_score: float = 0.0


@dataclass(frozen=True, slots=True)
class HybridRankingMetadata:
    """Pure scoring and coverage diagnostics for one requested user."""

    mode: str
    reason: str | None
    lambda_requested: float
    lambda_effective: float
    algorithm_version: str
    candidate_count: int
    recommendation_count: int
    als_source_available: bool
    graph_source_available: bool
    trend_source_available: bool
    als_source_count: int
    trend_source_count: int
    overlap_count: int
    excluded_trend_product_ids: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class HybridRankingResult:
    """Ranked recommendations and their diagnostics."""

    user_id: int
    recommendations: tuple[Recommendation, ...]
    metadata: HybridRankingMetadata
