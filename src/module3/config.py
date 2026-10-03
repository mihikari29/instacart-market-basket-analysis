"""Frozen configuration for the streaming speed layer.

Mirrors module2.config: same env-first defaults, same fail-fast __post_init__.
All window/watermark/weight choices are explicit so the streaming job is
reproducible from the run summary alone.
"""

from dataclasses import dataclass
import os

DEFAULT_TOPIC = "instacart-purchase-events"
DEFAULT_MONGO_TOP_K = 20
DEFAULT_TTL_SECONDS = 7 * 24 * 60 * 60


@dataclass(frozen=True)
class Config:
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

    def __post_init__(self):
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

    @classmethod
    def environment(cls, **overrides):
        values = {
            "root": os.environ.get("MODULE3_ROOT", cls.root),
            "master": os.environ.get("SPARK_MASTER", cls.master),
            "bootstrap_servers": os.environ.get(
                "KAFKA_BOOTSTRAP_SERVERS", cls.bootstrap_servers
            ),
            "mongodb_uri": os.environ.get("MONGODB_URI", cls.mongodb_uri),
            "mongo_top_k_per_window": int(
                os.environ.get("MONGO_TOP_K_PER_WINDOW", cls.mongo_top_k_per_window)
            ),
            "ttl_seconds": int(os.environ.get("MONGO_TTL_SECONDS", cls.ttl_seconds)),
        }
        values.update(overrides)
        return cls(**values)
