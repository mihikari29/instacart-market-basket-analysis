"""SyntheticConfig — the knobs that fully describe one generated feed.

The generator reads exactly one object, so a scenario is reproducible and
documentable. See docs/generate.md -> "SyntheticConfig & scenarios".
"""
from __future__ import annotations

from dataclasses import dataclass, fields

import numpy as np

SCATTER_WINDOW_WEEKS = (1, 4, 13, 26, 52)
MINUTE_MODES = ("uniform", "hash")


@dataclass(frozen=True)
class DeltaSampler:
    """Distribution of the seconds between two item-adds inside a basket.

    - exponential: Poisson arrivals -> Exp(mean) clipped to [lo, hi].
    - uniform:     flat gaps in [lo, hi].
    """
    kind: str
    mean_s: float | None = None
    lo_s: float = 5.0
    hi_s: float = 120.0

    @classmethod
    def exponential(cls, mean_s: float, lo_s: float, hi_s: float) -> "DeltaSampler":
        return cls("exponential", mean_s, lo_s, hi_s)

    @classmethod
    def uniform(cls, lo_s: float, hi_s: float) -> "DeltaSampler":
        return cls("uniform", None, lo_s, hi_s)

    def sample(self, rng: "np.random.Generator", size: int) -> "np.ndarray":
        if self.kind == "uniform":
            return rng.uniform(self.lo_s, self.hi_s, size=size)
        gap = -self.mean_s * np.log1p(-rng.random(size))
        return np.clip(gap, self.lo_s, self.hi_s)

    def validate(self) -> None:
        if self.kind == "exponential":
            if not (5.0 <= self.mean_s <= 300.0):
                raise ValueError(f"exponential mean out of range: {self.mean_s}")
        elif self.kind == "uniform":
            if self.lo_s < 0 or self.hi_s <= self.lo_s:
                raise ValueError(f"uniform bounds must satisfy 0 <= lo < hi: {self.lo_s}, {self.hi_s}")
        else:
            raise ValueError(f"unknown delta kind: {self.kind!r}")

    def to_dict(self) -> dict:
        return {"kind": self.kind, "mean_s": self.mean_s, "lo_s": self.lo_s, "hi_s": self.hi_s}


@dataclass(frozen=True)
class SyntheticConfig:
    seed: int = 42
    scatter_window_weeks: int = 13
    minute_mode: str = "uniform"
    delta: DeltaSampler = DeltaSampler.exponential(30.0, 5.0, 120.0)
    limit_users: int | None = None

    @classmethod
    def from_scenario(
        cls,
        scenario: str,
        *,
        seed: int | None = None,
        scatter_window_weeks: int | None = None,
        minute_mode: str | None = None,
        limit_users: int | None = None,
    ) -> "SyntheticConfig":
        if scenario not in SCENARIOS:
            raise ValueError(f"unknown scenario {scenario!r}; choose from {sorted(SCENARIOS)}")
        kwargs = dict(SCENARIOS[scenario])
        if seed is not None:
            kwargs["seed"] = seed
        if scatter_window_weeks is not None:
            kwargs["scatter_window_weeks"] = scatter_window_weeks
        if minute_mode is not None:
            kwargs["minute_mode"] = minute_mode
        if limit_users is not None:
            kwargs["limit_users"] = limit_users
        conf = cls(**kwargs)
        conf.validate()
        return conf

    def validate(self) -> None:
        if self.scatter_window_weeks not in SCATTER_WINDOW_WEEKS:
            raise ValueError(
                f"scatter_window_weeks must be one of {SCATTER_WINDOW_WEEKS}, "
                f"got {self.scatter_window_weeks}"
            )
        if self.minute_mode not in MINUTE_MODES:
            raise ValueError(f"minute_mode must be one of {MINUTE_MODES}")
        if self.limit_users is not None and self.limit_users <= 0:
            raise ValueError(f"limit_users must be positive, got {self.limit_users}")
        self.delta.validate()

    def to_dict(self) -> dict:
        data = {f.name: getattr(self, f.name) for f in fields(self)}
        data["delta"] = self.delta.to_dict()
        return data


SCENARIOS: dict[str, dict] = {
    "default": dict(
        delta=DeltaSampler.exponential(30.0, 5.0, 120.0),
        minute_mode="uniform",
    ),
    "mobile-fast": dict(
        delta=DeltaSampler.exponential(20.0, 3.0, 90.0),
        minute_mode="uniform",
    ),
    "desktop-browse": dict(
        delta=DeltaSampler.exponential(40.0, 10.0, 240.0),
        minute_mode="uniform",
    ),
    "uniform": dict(
        delta=DeltaSampler.uniform(15.0, 50.0),
        minute_mode="uniform",
    ),
    "deterministic": dict(
        delta=DeltaSampler.exponential(30.0, 5.0, 120.0),
        minute_mode="hash",
    ),
}


def preset_summary() -> str:
    lines = []
    for name, cfg in SCENARIOS.items():
        d = cfg["delta"]
        if d.kind == "exponential":
            params = f"Exp(mean={d.mean_s}s, clip=[{d.lo_s}, {d.hi_s}]s)"
        else:
            params = f"U([{d.lo_s}, {d.hi_s}]s)"
        lines.append(f"  {name:<16s} {d.kind:<12s} {params:<42s} minute={cfg['minute_mode']}")
    return "\n".join(lines)