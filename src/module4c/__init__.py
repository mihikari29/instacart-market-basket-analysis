"""Pure Python hybrid recommendation ranking for Module 4C."""

from .config import (
    ALSRecommendation,
    HybridRankingConfig,
    HybridRankingMetadata,
    HybridRankingResult,
    ProductMetadata,
    Recommendation,
    TrendRecommendation,
    TrendSnapshot,
    VerifiedSourceContext,
)
from .ranking import RankingContractError, SourceContextError, rank_hybrid_recommendations
from .data import (
    CapturedUserSources,
    ReadinessContractError,
    SourceCaptureError,
    SourceLockError,
    SourceQueryError,
    SourceSnapshotError,
    capture_verified_user_sources,
)

__all__ = [
    "ALSRecommendation",
    "CapturedUserSources",
    "HybridRankingConfig",
    "HybridRankingMetadata",
    "HybridRankingResult",
    "ProductMetadata",
    "Recommendation",
    "ReadinessContractError",
    "RankingContractError",
    "SourceContextError",
    "SourceCaptureError",
    "SourceLockError",
    "SourceQueryError",
    "SourceSnapshotError",
    "TrendRecommendation",
    "TrendSnapshot",
    "VerifiedSourceContext",
    "capture_verified_user_sources",
    "rank_hybrid_recommendations",
]
