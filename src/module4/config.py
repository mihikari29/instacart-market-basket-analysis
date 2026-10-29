"""Configuration and paths shared by the Module 4A/4B jobs."""

from dataclasses import dataclass
import os
from pathlib import Path
import re

from ..common.runtime import env_float, env_int, finite_number, positive_int


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
        if not isinstance(self.root, str) or not self.root.strip() or "\x00" in self.root or self.root.rstrip("/") in ("", "hdfs:", "file:"):
            raise ValueError("An explicit dataset root is required")
        if not isinstance(self.master, str) or not self.master.strip() or "\x00" in self.master:
            raise ValueError("Spark master must not be empty")
        for name, value in (
            ("shuffle_partitions", self.shuffle_partitions), ("recommendation_k", self.recommendation_k),
            ("graph_min_edge_weight", self.graph_min_edge_weight), ("graph_max_iter", self.graph_max_iter),
            ("executor_cores", self.executor_cores), ("total_cores", self.total_cores), ("als_blocks", self.als_blocks),
        ):
            positive_int(value, name)
        if type(self.seed) is not int:
            raise ValueError("seed must be an integer")
        reset_probability = finite_number(self.graph_reset_probability, "graph_reset_probability")
        if not 0.0 < reset_probability < 1.0:
            raise ValueError("graph_reset_probability must be between 0 and 1")
        if self.graph_backend != "graphframes":
            raise ValueError("graph_backend must be graphframes")
        if self.graph_top_products is not None:
            positive_int(self.graph_top_products, "graph_top_products")
            if self.graph_top_products < 2:
                raise ValueError("graph_top_products must be at least two")
        if not isinstance(self.executor_memory, str) or not re.fullmatch(r"[1-9][0-9]*[mgMG]", self.executor_memory):
            raise ValueError("executor_memory must be an explicit positive Spark memory size")
        if not isinstance(self.mongodb_uri, str) or not self.mongodb_uri.strip() or "\x00" in self.mongodb_uri:
            raise ValueError("mongodb_uri must not be empty")
        if not self.mongo_database:
            raise ValueError("mongo_database must not be empty")
        for name, value in (
            ("mongo_database", self.mongo_database),
            ("mongo_recommendation_collection", self.mongo_recommendation_collection),
            ("mongo_graph_collection", self.mongo_graph_collection),
        ):
            if not isinstance(value, str) or not value.strip() or "." in value or "$" in value or "\x00" in value:
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
        values = {
            "root": os.environ.get("MODULE4_ROOT", cls.root),
            "master": os.environ.get("SPARK_MASTER", cls.master),
            "graph_backend": os.environ.get("MODULE4_GRAPH_BACKEND", cls.graph_backend),
            "executor_memory": os.environ.get("MODULE4_EXECUTOR_MEMORY", cls.executor_memory),
            "mongodb_uri": os.environ.get("MONGODB_URI", cls.mongodb_uri),
            "mongo_database": os.environ.get("MODULE4_MONGO_DATABASE", cls.mongo_database),
            "mongo_recommendation_collection": cls.mongo_recommendation_collection,
            "mongo_graph_collection": cls.mongo_graph_collection,
        }
        for field, variable, default in (
            ("shuffle_partitions", "MODULE4_SHUFFLE_PARTITIONS", cls.shuffle_partitions),
            ("seed", "MODULE4_SEED", cls.seed), ("recommendation_k", "MODULE4_RECOMMENDATION_K", cls.recommendation_k),
            ("graph_min_edge_weight", "MODULE4_GRAPH_MIN_EDGE_WEIGHT", cls.graph_min_edge_weight),
            ("graph_max_iter", "MODULE4_GRAPH_MAX_ITER", cls.graph_max_iter),
            ("graph_top_products", "MODULE4_GRAPH_TOP_PRODUCTS", cls.graph_top_products),
            ("executor_cores", "MODULE4_EXECUTOR_CORES", cls.executor_cores),
            ("total_cores", "MODULE4_TOTAL_CORES", cls.total_cores), ("als_blocks", "MODULE4_ALS_BLOCKS", cls.als_blocks),
        ):
            if field not in overrides:
                values[field] = env_int(variable, default)
        if "graph_reset_probability" not in overrides:
            values["graph_reset_probability"] = env_float("MODULE4_GRAPH_RESET_PROBABILITY", cls.graph_reset_probability)
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
