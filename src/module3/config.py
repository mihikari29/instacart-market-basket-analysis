"""Frozen configuration for the streaming speed layer.

Mirrors module2.config: same env-first defaults, same fail-fast __post_init__.
All window/watermark/weight choices are explicit so the streaming job is
reproducible from the run summary alone.
"""

from dataclasses import dataclass
import os
import re

# Re-exported so `from .config import parse_bool` keeps working for callers
# that treat the Module 3 configuration module as the owner of the flag.
from ..common.env import parse_bool

DEFAULT_TOPIC = "instacart-purchase-events"
DEFAULT_MONGO_TOP_K = 20
DEFAULT_TTL_SECONDS = 7 * 24 * 60 * 60
MAX_TREND_BUNDLE_TOP_K = 20
_STABLE_LABEL = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]*\Z")


def _validate_stable_label(value: str | None, field: str, maximum: int) -> None:
    """Reject labels that cannot serve as stable trend-source identifiers."""
    if (
        not isinstance(value, str)
        or len(value) > maximum
        or not _STABLE_LABEL.fullmatch(value)
    ):
        raise ValueError(
            f"{field} must use 1-{maximum} letters, digits, '.', '_', ':', '/', or '-'; "
            "it must start with a letter or digit"
        )


@dataclass(frozen=True)
class Config:
    """Immutable source, event-time, and serving settings for one stream run."""

    # Storage / Spark runtime
    root: str = "hdfs://namenode:8020/instacart"
    master: str = "local[2]"
    shuffle_partitions: int = 8
    repetitions: int = 3
    warmups: int = 1

    # Kafka source
    bootstrap_servers: str = "kafka:29092"
    topic: str = DEFAULT_TOPIC
    starting_offsets: str = "earliest"
    fail_on_data_loss: bool = True

    # Event-time windows (Proposal §19)
    window_short: str = "30 minutes"
    window_long: str = "120 minutes"
    slide: str = "5 minutes"
    watermark: str = "10 minutes"

    # Trending blend (Proposal §18)
    weight_short: float = 0.7
    weight_long: float = 0.3

    # Processing cadence is independent of the five-minute event-time slide.
    trigger_interval: str = "10 seconds"

    # Serving store
    mongodb_uri: str = "mongodb://mongodb:27017/instacart"
    mongo_database: str = "instacart"
    mongo_collection: str = "realtime_trending"
    mongo_top_k_per_window: int = DEFAULT_MONGO_TOP_K
    ttl_seconds: int = DEFAULT_TTL_SECONDS
    checkpoint_dir: str = "checkpoint"

    # Optional immutable finalized-window bundle publication.
    publish_trend_bundles: bool = False
    trend_source_namespace: str | None = None
    trend_source_scope: str = "unknown_or_mixed"

    def __post_init__(self) -> None:
        if self.shuffle_partitions < 1 or self.repetitions < 3 or self.warmups < 1:
            raise ValueError("Require positive partitions, >=3 measured runs and >=1 warm-up")
        if not self.root or self.root.rstrip("/") in ("", "hdfs:", "file:"):
            raise ValueError("An explicit dataset root is required")
        if not 0.0 <= self.weight_short <= 1.0 or not 0.0 <= self.weight_long <= 1.0:
            raise ValueError("Trend weights must lie in [0, 1]")
        if abs(self.weight_short + self.weight_long - 1.0) > 1e-9:
            raise ValueError("Trend weights must sum to 1.0")
        if self.ttl_seconds < 0:
            raise ValueError("ttl_seconds must be non-negative (0 disables TTL)")
        if self.mongo_top_k_per_window <= 0:
            raise ValueError("mongo_top_k_per_window must be positive")
        if not self.topic or not self.bootstrap_servers:
            raise ValueError("Kafka topic and bootstrap servers are required")
        if not isinstance(self.fail_on_data_loss, bool):
            raise ValueError("fail_on_data_loss must be a boolean")
        if not isinstance(self.publish_trend_bundles, bool):
            raise ValueError("publish_trend_bundles must be a boolean")
        if self.trend_source_namespace is not None:
            _validate_stable_label(
                self.trend_source_namespace, "trend_source_namespace", 256
            )
        _validate_stable_label(self.trend_source_scope, "trend_source_scope", 128)
        if self.publish_trend_bundles:
            if self.trend_source_namespace is None:
                raise ValueError(
                    "trend_source_namespace is required when trend bundle publication is enabled"
                )
            if (
                type(self.mongo_top_k_per_window) is not int
                or not 1 <= self.mongo_top_k_per_window <= MAX_TREND_BUNDLE_TOP_K
            ):
                raise ValueError(
                    "mongo_top_k_per_window must be an integer between 1 and "
                    f"{MAX_TREND_BUNDLE_TOP_K} when trend bundle publication is enabled"
                )

    @classmethod
    def environment(cls, **overrides) -> "Config":
        """Parse environment defaults, apply explicit overrides, then validate."""
        values = {
            "root": os.environ.get("MODULE3_ROOT", cls.root),
            "master": os.environ.get("SPARK_MASTER", cls.master),
            "bootstrap_servers": os.environ.get(
                "KAFKA_BOOTSTRAP_SERVERS", cls.bootstrap_servers
            ),
            "fail_on_data_loss": parse_bool(
                os.environ.get(
                    "MODULE3_FAIL_ON_DATA_LOSS", str(cls.fail_on_data_loss)
                )
            ),
            "mongodb_uri": os.environ.get("MONGODB_URI", cls.mongodb_uri),
            "mongo_top_k_per_window": int(
                os.environ.get("MONGO_TOP_K_PER_WINDOW", cls.mongo_top_k_per_window)
            ),
            "ttl_seconds": int(os.environ.get("MONGO_TTL_SECONDS", cls.ttl_seconds)),
            "publish_trend_bundles": parse_bool(
                os.environ.get(
                    "MODULE3_PUBLISH_TREND_BUNDLES",
                    str(cls.publish_trend_bundles),
                )
            ),
            "trend_source_namespace": os.environ.get(
                "MODULE3_TREND_SOURCE_NAMESPACE", cls.trend_source_namespace
            ),
            "trend_source_scope": os.environ.get(
                "MODULE3_TREND_SOURCE_SCOPE", cls.trend_source_scope
            ),
        }
        values.update(overrides)
        return cls(**values)
