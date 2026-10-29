"""Validated dataset paths and runtime settings for Module 2 batch jobs."""

from dataclasses import dataclass
import os
from pathlib import Path
import re

from ..common.runtime import positive_int
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
    executor_memory: str = "1g"
    executor_cores: int = 2
    total_cores: int = 2

    def __post_init__(self) -> None:
        for name, value in (("shuffle_partitions", self.shuffle_partitions), ("repetitions", self.repetitions), ("warmups", self.warmups)):
            positive_int(value, name)
        if self.repetitions < 3:
            raise ValueError("Require positive partitions, >=3 measured runs and >=1 warm-up")
        if not isinstance(self.root, str) or not self.root.strip() or "\x00" in self.root or self.root.rstrip("/") in ("", "hdfs:", "file:"):
            raise ValueError("An explicit dataset root is required")
        if not isinstance(self.master, str) or not self.master.strip() or "\x00" in self.master:
            raise ValueError("Spark master must not be empty or contain NUL")
        if not isinstance(self.executor_memory, str) or not re.fullmatch(r"[1-9][0-9]*[mg]", self.executor_memory):
            raise ValueError("executor_memory must be a positive Spark m/g quantity")
        if any(type(value) is not int or value < 1 for value in (self.executor_cores, self.total_cores)):
            raise ValueError("executor and total cores must be positive integers")
        if self.executor_cores > self.total_cores or self.total_cores % self.executor_cores:
            raise ValueError("total cores must be divisible by executor cores")

    def table(self, name: str) -> str:
        """Resolve a curated table by its logical key in TABLES."""
        return self.root.rstrip("/") + "/curated/" + TABLES[name]

    def features(self, kind: str = "user_features") -> str:
        """Return the feature handoff path consumed by downstream stages."""
        return self.root.rstrip("/") + f"/features/{kind}"

    @classmethod
    def environment(cls, **overrides) -> "Config":
        """Read root/master from the environment and accept explicit run options."""
        values = {"root": os.environ.get("MODULE2_ROOT", cls.root), "master": os.environ.get("SPARK_MASTER", cls.master)}
        values.update(overrides)
        return cls(**values)


def session(config: Config, event_dir: Path | None = None):
    """Create the Module 2 Spark session."""
    from ..common.spark import build_session

    return build_session(
        config,
        event_dir,
        app_name="instacart-module2",
        total_cores=config.total_cores,
        executor_memory=config.executor_memory,
        executor_cores=config.executor_cores,
    )
