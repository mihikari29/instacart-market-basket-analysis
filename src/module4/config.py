"""Configuration and HDFS paths shared by Module 4A and 4B."""

from dataclasses import dataclass
import os


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

    def __post_init__(self):
        if not self.root or self.root.rstrip("/") in ("", "hdfs:", "file:"):
            raise ValueError("An explicit dataset root is required")
        if self.shuffle_partitions < 1:
            raise ValueError("shuffle_partitions must be positive")
        if self.recommendation_k < 1:
            raise ValueError("recommendation_k must be positive")
        if self.graph_min_edge_weight < 1:
            raise ValueError("graph_min_edge_weight must be positive")

    def table(self, name: str) -> str:
        try:
            relative = TABLES[name]
        except KeyError as exc:
            raise ValueError(f"Unknown Module 4 table: {name}") from exc
        return self.root.rstrip("/") + "/curated/" + relative

    def model_path(self, model_version: str) -> str:
        return self.root.rstrip("/") + "/models/als/" + model_version

    @classmethod
    def environment(cls, **overrides):
        values = {
            "root": os.environ.get("MODULE4_ROOT", cls.root),
            "master": os.environ.get("SPARK_MASTER", cls.master),
            "seed": int(os.environ.get("MODULE4_SEED", cls.seed)),
        }
        values.update(overrides)
        return cls(**values)
