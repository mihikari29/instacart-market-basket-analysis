"""Configuration and paths shared by the Module 4A/4B jobs."""

from dataclasses import dataclass
import math
import os
from pathlib import Path


TABLES = {
    "orders": "orders",
    "prior": "order_products_prior",
    "train": "order_products_train",
    "products": "dimensions/products",
}


@dataclass(frozen=True)
class Config:
    root: str = "hdfs://namenode:8020/instacart"
    master: str = "local[2]"
    shuffle_partitions: int = 8
    seed: int = 42
    recommendation_k: int = 20
    graph_min_edge_weight: int = 2
    graph_reset_probability: float = 0.15
    graph_max_iter: int = 10
    graph_backend: str = "graphframes"
    graph_top_products: int | None = None
    executor_memory: str = "1g"
    executor_cores: int = 2
    total_cores: int = 2
    als_blocks: int = 4
    mongodb_uri: str = "mongodb://localhost:27017/instacart"
    mongo_database: str = "instacart"
    mongo_recommendation_collection: str = "user_recommendations"
    mongo_graph_collection: str = "product_graph_metrics"

    def __post_init__(self):
        if not self.root or self.root.rstrip("/") in ("", "hdfs:", "file:"):
            raise ValueError("An explicit dataset root is required")
        if not self.master:
            raise ValueError("Spark master must not be empty")
        if self.shuffle_partitions < 1:
            raise ValueError("shuffle_partitions must be positive")
        if self.recommendation_k < 1:
            raise ValueError("recommendation_k must be positive")
        if self.graph_min_edge_weight < 1:
            raise ValueError("graph_min_edge_weight must be positive")
        if not 0.0 < self.graph_reset_probability < 1.0:
            raise ValueError("graph_reset_probability must be between 0 and 1")
        if self.graph_max_iter < 1:
            raise ValueError("graph_max_iter must be positive")
        if not math.isfinite(self.graph_reset_probability):
            raise ValueError("graph_reset_probability must be finite")
        if self.graph_backend != "graphframes":
            raise ValueError("graph_backend must be graphframes")
        if self.graph_top_products is not None and self.graph_top_products < 2:
            raise ValueError("graph_top_products must be at least two")
        if min(self.executor_cores, self.total_cores, self.als_blocks) < 1:
            raise ValueError("executor_cores, total_cores and als_blocks must be positive")
        import re

        if not re.fullmatch(r"[1-9][0-9]*[mgMG]", self.executor_memory):
            raise ValueError("executor_memory must be an explicit positive Spark memory size")
        if not self.mongodb_uri:
            raise ValueError("mongodb_uri must not be empty")
        if not self.mongo_database:
            raise ValueError("mongo_database must not be empty")
        for name, value in (
            ("mongo_database", self.mongo_database),
            ("mongo_recommendation_collection", self.mongo_recommendation_collection),
            ("mongo_graph_collection", self.mongo_graph_collection),
        ):
            if not value or "." in value or "$" in value or "\x00" in value:
                raise ValueError(f"{name} must be a valid MongoDB identifier")

    def table(self, name: str) -> str:
        try:
            relative = TABLES[name]
        except KeyError as exc:
            raise ValueError(f"Unknown Module 4 table: {name}") from exc
        return self.root.rstrip("/") + "/curated/" + relative

    def model_path(self, model_version: str) -> str:
        return self.root.rstrip("/") + "/models/als/" + model_version

    def features(self) -> str:
        """Return the optional Module 2 user-feature handoff path."""
        return self.root.rstrip("/") + "/features/user_features"

    @classmethod
    def environment(cls, **overrides):
        def env_int(name, default):
            value = os.environ.get(name)
            return int(value) if value is not None else default

        def env_float(name, default):
            value = os.environ.get(name)
            return float(value) if value is not None else default

        values = {
            "root": os.environ.get("MODULE4_ROOT", cls.root),
            "master": os.environ.get("SPARK_MASTER", cls.master),
            "shuffle_partitions": env_int("MODULE4_SHUFFLE_PARTITIONS", cls.shuffle_partitions),
            "seed": env_int("MODULE4_SEED", cls.seed),
            "recommendation_k": env_int("MODULE4_RECOMMENDATION_K", cls.recommendation_k),
            "graph_min_edge_weight": env_int(
                "MODULE4_GRAPH_MIN_EDGE_WEIGHT", cls.graph_min_edge_weight
            ),
            "graph_reset_probability": env_float(
                "MODULE4_GRAPH_RESET_PROBABILITY", cls.graph_reset_probability
            ),
            "graph_max_iter": env_int("MODULE4_GRAPH_MAX_ITER", cls.graph_max_iter),
            "graph_backend": os.environ.get("MODULE4_GRAPH_BACKEND", cls.graph_backend),
            "graph_top_products": env_int("MODULE4_GRAPH_TOP_PRODUCTS", cls.graph_top_products),
            "executor_memory": os.environ.get("MODULE4_EXECUTOR_MEMORY", cls.executor_memory),
            "executor_cores": env_int("MODULE4_EXECUTOR_CORES", cls.executor_cores),
            "total_cores": env_int("MODULE4_TOTAL_CORES", cls.total_cores),
            "als_blocks": env_int("MODULE4_ALS_BLOCKS", cls.als_blocks),
            "mongodb_uri": os.environ.get("MONGODB_URI", cls.mongodb_uri),
            "mongo_database": os.environ.get("MODULE4_MONGO_DATABASE", cls.mongo_database),
            "mongo_recommendation_collection": cls.mongo_recommendation_collection,
            "mongo_graph_collection": cls.mongo_graph_collection,
        }
        values.update(overrides)
        return cls(**values)


def session(config: Config, event_dir: Path | None = None):
    """Create the Module 4 Spark session."""
    from ..common.spark import build_session

    return build_session(
        config,
        event_dir,
        app_name="instacart-module4",
        total_cores=config.total_cores,
        executor_memory=config.executor_memory,
        executor_cores=config.executor_cores,
    )
