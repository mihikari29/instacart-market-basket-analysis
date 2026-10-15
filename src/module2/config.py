"""Validated dataset paths and runtime settings for Module 2 batch jobs."""

from dataclasses import dataclass
import os

TABLES = {
    "orders": "orders",
    "prior": "order_products_prior",
    "train": "order_products_train",
    "products": "dimensions/products",
    "aisles": "dimensions/aisles",
    "departments": "dimensions/departments",
    "interactions": "interactions",
}


@dataclass(frozen=True)
class Config:
    """Immutable input location and benchmark settings for one batch run."""

    root: str = "hdfs://namenode:8020/instacart"
    master: str = "local[2]"
    shuffle_partitions: int = 8
    repetitions: int = 3
    warmups: int = 1

    def __post_init__(self) -> None:
        if self.shuffle_partitions < 1 or self.repetitions < 3 or self.warmups < 1:
            raise ValueError("Require positive partitions, >=3 measured runs and >=1 warm-up")
        if not self.root or self.root.rstrip("/") in ("", "hdfs:", "file:"):
            raise ValueError("An explicit dataset root is required")

    def table(self, name: str) -> str:
        """Resolve a curated table by its logical key in TABLES."""
        return self.root.rstrip("/") + "/curated/" + TABLES[name]

    def features(self) -> str:
        """Return the user-feature handoff path consumed by Module 4."""
        return self.root.rstrip("/") + "/features/user_features"

    @classmethod
    def environment(cls, **overrides) -> "Config":
        """Read root/master from the environment and accept explicit run options."""
        return cls(
            root=os.environ.get("MODULE2_ROOT", cls.root),
            master=os.environ.get("SPARK_MASTER", cls.master),
            **overrides,
        )
