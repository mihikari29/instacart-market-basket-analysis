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

__all__ = [
    "ALSRecommendation",
    "HybridRankingConfig",
    "HybridRankingMetadata",
    "HybridRankingResult",
    "ProductMetadata",
    "Recommendation",
    "RankingContractError",
    "SourceContextError",
    "TrendRecommendation",
    "TrendSnapshot",
    "VerifiedSourceContext",
    "rank_hybrid_recommendations",
]
