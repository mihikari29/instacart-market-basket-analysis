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
from .trend_reader import (
    TrendSnapshotIntegrityError,
    TrendSnapshotQueryError,
    TrendSnapshotReaderError,
    TrendSnapshotReadResult,
    TrendSnapshotRequestError,
    read_verified_trend_snapshot,
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
    "TrendSnapshotIntegrityError",
    "TrendSnapshotQueryError",
    "TrendSnapshotReaderError",
    "TrendSnapshotReadResult",
    "TrendSnapshotRequestError",
    "TrendSnapshot",
    "VerifiedSourceContext",
    "capture_verified_user_sources",
    "rank_hybrid_recommendations",
    "read_verified_trend_snapshot",
]
