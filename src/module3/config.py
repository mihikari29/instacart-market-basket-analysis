"""Frozen configuration for the streaming speed layer.

Mirrors module2.config: same env-first defaults, same fail-fast __post_init__.
All window/watermark/weight choices are explicit so the streaming job is
reproducible from the run summary alone.
"""

from dataclasses import dataclass
from decimal import Decimal
import os
from pathlib import Path
import re

# Re-exported so `from .config import parse_bool` keeps working for callers
# that treat the Module 3 configuration module as the owner of the flag.
from ..common.runtime import env_int, finite_number, parse_bool, positive_int

DEFAULT_TOPIC = "instacart-purchase-events"
DEFAULT_MONGO_TOP_K = 20
DEFAULT_TTL_SECONDS = 7 * 24 * 60 * 60
MAX_TREND_BUNDLE_TOP_K = 20
MIN_BENCHMARK_SECONDS = 12
_STABLE_LABEL = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]*\Z")
_INTERVAL_PART = re.compile(
    r"\s*([+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+))\s+"
    r"(weeks?|days?|hours?|minutes?|seconds?|milliseconds?|microseconds?)\b", re.IGNORECASE
)
_INTERVAL_UNITS = {"week": 604800000000, "day": 86400000000, "hour": 3600000000,
                   "minute": 60000000, "second": 1000000, "millisecond": 1000, "microsecond": 1}


def _interval_microseconds(value: str, field: str) -> int:
    """Validate Spark's fixed-length interval components without starting Spark."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a fixed-length Spark interval")
    text = value.strip()
    if text.lower().startswith("interval ") and field != "window_short":
        text = text[len("interval "):].strip()
    position = 0
    total = Decimal(0)
    while position < len(text):
        part = _INTERVAL_PART.match(text, position)
        if part is None:
            raise ValueError(f"{field} must use fixed-length Spark interval units")
        amount, unit = part.groups()
        unit = unit.lower().rstrip("s")
        if "." in amount and unit != "second":
            raise ValueError(f"{field} permits fractional amounts only for seconds")
        total += Decimal(amount) * _INTERVAL_UNITS[unit]
        position = part.end()
    if total != total.to_integral_value() or not 0 <= total <= 2**63 - 1:
        raise ValueError(f"{field} must be a non-negative interval with microsecond precision")
    return int(total)


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
    # Kept in the public config/report schema. Actual checkpoint paths are
    # owned by run_stream(checkpoint_root=...) and ensure_checkpoint().

    # Optional immutable finalized-window bundle publication.
    publish_trend_bundles: bool = False
    trend_source_namespace: str | None = None
    trend_source_scope: str = "unknown_or_mixed"

    def __post_init__(self) -> None:
        for name, value in (("shuffle_partitions", self.shuffle_partitions), ("repetitions", self.repetitions), ("warmups", self.warmups)):
            positive_int(value, name)
        if self.repetitions < 3:
            raise ValueError("Require positive partitions, >=3 measured runs and >=1 warm-up")
        if not isinstance(self.root, str) or not self.root.strip() or "\x00" in self.root or self.root.rstrip("/") in ("", "hdfs:", "file:"):
            raise ValueError("An explicit dataset root is required")
        if not isinstance(self.master, str) or not self.master.strip() or "\x00" in self.master:
            raise ValueError("Spark master must not be empty or contain NUL")
        short_weight = finite_number(self.weight_short, "weight_short")
        long_weight = finite_number(self.weight_long, "weight_long")
        if not 0.0 <= short_weight <= 1.0 or not 0.0 <= long_weight <= 1.0:
            raise ValueError("Trend weights must lie in [0, 1]")
        if abs(self.weight_short + self.weight_long - 1.0) > 1e-9:
            raise ValueError("Trend weights must sum to 1.0")
        if type(self.ttl_seconds) is not int or self.ttl_seconds < 0:
            raise ValueError("ttl_seconds must be non-negative (0 disables TTL)")
        positive_int(self.mongo_top_k_per_window, "mongo_top_k_per_window")
        if not isinstance(self.topic, str) or not re.fullmatch(r"[A-Za-z0-9._-]{1,249}", self.topic) or self.topic in {".", ".."}:
            raise ValueError("topic must be a valid Kafka topic name")
        if not isinstance(self.bootstrap_servers, str) or not self.bootstrap_servers.strip() or "\x00" in self.bootstrap_servers:
            raise ValueError("Kafka topic and bootstrap servers are required")
        if not isinstance(self.mongodb_uri, str) or not self.mongodb_uri.strip() or "\x00" in self.mongodb_uri:
            raise ValueError("mongodb_uri must not be empty or contain NUL")
        for name, value, forbidden in (
            ("mongo_database", self.mongo_database, (".", "$", "\x00", "/", "\\", " ")),
            ("mongo_collection", self.mongo_collection, ("$", "\x00")),
        ):
            if not isinstance(value, str) or not value.strip() or any(character in value for character in forbidden):
                raise ValueError(f"{name} must be a valid MongoDB identifier")
        short = _interval_microseconds(self.window_short, "window_short")
        long = _interval_microseconds(self.window_long, "window_long")
        slide = _interval_microseconds(self.slide, "slide")
        if short == 0 or long == 0 or slide == 0:
            raise ValueError("window_short, window_long and slide must be positive intervals")
        if short > long:
            raise ValueError("window_short must not exceed window_long")
        if slide > long:
            raise ValueError("slide must not exceed window_long")
        _interval_microseconds(self.watermark, "watermark")
        if self.trigger_interval not in (None, ""):
            _interval_microseconds(self.trigger_interval, "trigger_interval")
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
            "mongodb_uri": os.environ.get("MONGODB_URI", cls.mongodb_uri),
            "trend_source_namespace": os.environ.get(
                "MODULE3_TREND_SOURCE_NAMESPACE", cls.trend_source_namespace
            ),
            "trend_source_scope": os.environ.get(
                "MODULE3_TREND_SOURCE_SCOPE", cls.trend_source_scope
            ),
        }
        for field, variable, default in (
            ("mongo_top_k_per_window", "MONGO_TOP_K_PER_WINDOW", cls.mongo_top_k_per_window),
            ("ttl_seconds", "MONGO_TTL_SECONDS", cls.ttl_seconds),
        ):
            if field not in overrides:
                values[field] = env_int(variable, default)
        for field, variable, default in (
            ("fail_on_data_loss", "MODULE3_FAIL_ON_DATA_LOSS", cls.fail_on_data_loss),
            ("publish_trend_bundles", "MODULE3_PUBLISH_TREND_BUNDLES", cls.publish_trend_bundles),
        ):
            if field not in overrides:
                values[field] = parse_bool(os.environ.get(variable, str(default)))
        values.update(overrides)
        return cls(**values)


def session(config: Config, event_dir: Path | None = None):
    """Create the Module 3 Spark session."""
    from ..common.spark import build_session

    return build_session(
        config,
        event_dir,
        app_name="instacart-module3",
        total_cores=2,
        executor_memory="1g",
        executor_cores=2,
        extra_config={"spark.sql.streaming.metricsEnabled": "true"},
    )
