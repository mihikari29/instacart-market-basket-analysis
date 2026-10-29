"""Centralized configuration and operational timeout management for Kubernetes runners."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any
from copy import deepcopy

from src.common.runtime import config_section, env_float, env_int, finite_number, positive_int, read_yaml_mapping

PIPELINE_CONFIG_DEFAULT = Path(__file__).resolve().parents[2] / "config" / "pipeline.yaml"

DEFAULT_CONFIG: dict[str, Any] = {
    "synthesis": {
        "scenario": "deterministic",
        "scatter_weeks": 1,
        "seed": 42,
        "time_mode": "hash",
        "limit_users": 50,
        "limit_events": 5000,
        "delta_sampler": {"kind": "exponential", "mean_s": 30.0, "lo_s": 5.0, "hi_s": 120.0},
    },
    "models": {
        "als": {
            "rank": 32,
            "max_iter": 10,
            "reg_param": 0.1,
            "alpha": 10.0,
            "strength_col": "strength_raw",
            "recommendation_k": 20,
            "pilot_users": 5000,
            "max_trials": 1,
        },
        "graph": {
            "backend": "graphframes",
            "min_edge_weight": 2,
            "reset_probability": 0.15,
            "max_iter": 10,
            "top_products": 20,
        },
        "hybrid": {
            "alpha": 0.5,
            "beta": 0.2,
            "gamma": 0.3,
            "window_minutes": 30,
            "output_top_k": 20,
        },
    },
    "streaming": {
        "trigger_interval": "2 seconds",
        "watermark": "10 minutes",
        "window_short": "5 minutes",
        "window_long": "30 minutes",
        "slide": "1 minute",
        "fixture_rate": 100,
        "full_rate": 1000,
        "fixture_duration_seconds": 90,
        "full_duration_seconds": 3600,
        "mongo_top_k": 20,
    },
    "pipeline_steps": {
        "input": 7200,
        "module1": 1800,
        "module2": 7200,
        "module4": 7200,
        "replay_fixture": 1800,
        "replay_full": 65400,
        "hybrid": 900,
    },
    "acceptance": {
        "checkpoint_recovery_duration": 35,
        "default_command_timeout": 60,
    },
    "infrastructure": {
        "image_tag_timeout": 120,
        "image_pull_timeout": 900,
        "image_load_timeout": 900,
        "rollout_timeout": 900,
        "pvc_bound_timeout": 900,
        "pod_ready_timeout": 300,
    },
    "capacity_reserve": {
        "cpu_cores": 6,
        "memory_gib": 12,
        "free_disk_gib": 40,
    },
    "execution": {
        "resume_policy": "enabled",
        "checkpoint_dir": "/run/checkpoints",
        "data_dir": "/run/pipeline",
    },
}


def load_timeouts_config(path: Path | str | None = None) -> dict[str, Any]:
    """Load the canonical YAML layout; only an absent implicit file is optional."""
    selected = path if path is not None else os.environ.get("IT4931_PIPELINE_CONFIG")
    config_path = Path(selected) if selected is not None else PIPELINE_CONFIG_DEFAULT
    if selected is None and not config_path.exists():
        return deepcopy(DEFAULT_CONFIG)
    loaded = read_yaml_mapping(config_path)
    if {"pipeline_steps", "acceptance", "infrastructure", "capacity_reserve"}.intersection(loaded):
        raise ValueError("pipeline configuration must use the canonical timeouts and resources sections")
    timeouts = config_section(loaded, "timeouts")
    resources = config_section(loaded, "resources")
    acceptance = dict(config_section(timeouts, "acceptance"))
    if "default_command_timeout" in acceptance:
        raise ValueError("timeouts.acceptance must use command_timeout")
    if "command_timeout" in acceptance:
        acceptance["default_command_timeout"] = acceptance.pop("command_timeout")
    result = deepcopy(DEFAULT_CONFIG)
    for section in ("synthesis", "streaming", "execution"):
        result[section].update(config_section(loaded, section))
    models = config_section(loaded, "models")
    result["models"].update(models)
    for model in DEFAULT_CONFIG["models"]:
        config_section(models, model)
    result["pipeline_steps"].update(config_section(timeouts, "steps"))
    result["acceptance"].update(acceptance)
    result["infrastructure"].update(config_section(timeouts, "infrastructure"))
    result["capacity_reserve"].update(config_section(resources, "capacity_reserve"))
    config_section(resources, "pipeline_job")  # Declared intent; Job manifests still own allocation.
    for section in ("pipeline_steps", "acceptance", "infrastructure"):
        for name, value in result[section].items():
            positive_int(value, f"{section}.{name}")
    reserve = result["capacity_reserve"]
    if type(reserve["cpu_cores"]) is not int or reserve["cpu_cores"] < 0:
        raise ValueError("capacity_reserve.cpu_cores must be a non-negative integer")
    for field in ("memory_gib", "free_disk_gib"):
        finite_number(reserve[field], f"capacity_reserve.{field}", minimum=0)
    policy = result["execution"]["resume_policy"]
    if not isinstance(policy, str) or policy.strip().lower() not in {"enabled", "strict", "disabled"}:
        raise ValueError("execution.resume_policy must be enabled, strict, or disabled")
    return result


def get_step_timeout(step: str, mode: str = "fixture") -> int:
    """Return a positive deadline, with an explicit environment override first."""
    variable = f"IT4931_STEP_TIMEOUT_{step.upper()}"
    override = env_int(variable, None)
    if override is not None:
        return positive_int(override, variable)
    steps = load_timeouts_config()["pipeline_steps"]
    key = step
    if step == "replay":
        key = "replay_full" if mode == "full" else "replay_fixture"
    return steps.get(key, 7200)


def get_checkpoint_recovery_duration() -> int:
    """Return the positive streaming probe duration for checkpoint recovery."""
    variable = "IT4931_CHECKPOINT_RECOVERY_DURATION"
    override = env_int(variable, None)
    if override is not None:
        return positive_int(override, variable)
    return load_timeouts_config()["acceptance"]["checkpoint_recovery_duration"]


def get_infrastructure_timeout(name: str) -> int:
    """Return an infrastructure deadline; malformed overrides are errors."""
    variable = f"IT4931_{name.upper()}"
    override = env_int(variable, None)
    if override is not None:
        return positive_int(override, variable)
    return load_timeouts_config()["infrastructure"].get(name, 900)


def get_capacity_reserve() -> dict[str, int | float]:
    """Return host reserves without ignoring invalid environment overrides."""
    reserve = dict(load_timeouts_config()["capacity_reserve"])
    for field, variable, parse in (
        ("cpu_cores", "IT4931_CAPACITY_RESERVE_CPU_CORES", env_int),
        ("memory_gib", "IT4931_CAPACITY_RESERVE_MEMORY_GIB", env_float),
        ("free_disk_gib", "IT4931_CAPACITY_RESERVE_FREE_DISK_GIB", env_float),
    ):
        override = parse(variable, None)
        if override is not None:
            finite_number(override, variable, minimum=0)
            reserve[field] = override
    return reserve


def get_synthesis_config() -> dict[str, Any]:
    """Return the synthesis declarations consumed by the bounded runner."""
    return load_timeouts_config()["synthesis"]


def get_models_config() -> dict[str, Any]:
    """Return model declarations; consumers select their supported fields."""
    return load_timeouts_config()["models"]


def get_streaming_config() -> dict[str, Any]:
    """Return declarations; replay consumes rate, duration and Mongo Top-K."""
    return load_timeouts_config()["streaming"]


def get_execution_config() -> dict[str, Any]:
    """Return execution declarations; resume policy is the active consumer."""
    return load_timeouts_config()["execution"]
