"""Centralized configuration and operational timeout management for Kubernetes runners."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any
import yaml

PIPELINE_CONFIG_DEFAULT = Path(__file__).resolve().parents[2] / "config" / "pipeline.yaml"
TIMEOUTS_FILE_DEFAULT = Path(__file__).resolve().parents[2] / "deploy" / "k8s" / "timeouts.yaml"

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
    """Load configuration from pipeline.yaml or timeouts.yaml with defaults fallback."""
    config_path = None
    if path:
        config_path = Path(path)
    elif os.environ.get("IT4931_PIPELINE_CONFIG"):
        config_path = Path(os.environ["IT4931_PIPELINE_CONFIG"])
    elif PIPELINE_CONFIG_DEFAULT.is_file():
        config_path = PIPELINE_CONFIG_DEFAULT
    elif os.environ.get("IT4931_TIMEOUTS_FILE"):
        config_path = Path(os.environ["IT4931_TIMEOUTS_FILE"])
    elif TIMEOUTS_FILE_DEFAULT.is_file():
        config_path = TIMEOUTS_FILE_DEFAULT

    if config_path and config_path.is_file():
        try:
            loaded = yaml.safe_load(config_path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                # Normalize timeouts section if nested under timeouts: steps
                steps = loaded.get("pipeline_steps")
                if not steps and "timeouts" in loaded and isinstance(loaded["timeouts"], dict):
                    steps = loaded["timeouts"].get("steps")
                return {
                    "synthesis": {**DEFAULT_CONFIG["synthesis"], **loaded.get("synthesis", {})},
                    "models": {**DEFAULT_CONFIG["models"], **loaded.get("models", {})},
                    "streaming": {**DEFAULT_CONFIG["streaming"], **loaded.get("streaming", {})},
                    "pipeline_steps": {**DEFAULT_CONFIG["pipeline_steps"], **(steps or {})},
                    "acceptance": {**DEFAULT_CONFIG["acceptance"], **loaded.get("acceptance", {})},
                    "infrastructure": {**DEFAULT_CONFIG["infrastructure"], **loaded.get("infrastructure", {})},
                    "capacity_reserve": {**DEFAULT_CONFIG["capacity_reserve"], **loaded.get("capacity_reserve", {})},
                    "execution": {**DEFAULT_CONFIG["execution"], **loaded.get("execution", {})},
                }
        except (OSError, UnicodeError, yaml.YAMLError):
            pass
    return DEFAULT_CONFIG


def get_step_timeout(step: str, mode: str = "fixture") -> int:
    """Return the deadline in seconds for a pipeline step."""
    env_override = os.environ.get(f"IT4931_STEP_TIMEOUT_{step.upper()}")
    if env_override and env_override.isdigit():
        return int(env_override)
    config = load_timeouts_config()
    steps = config.get("pipeline_steps", DEFAULT_CONFIG["pipeline_steps"])
    if step == "replay":
        return int(steps.get("replay_full" if mode == "full" else "replay_fixture", 1800))
    return int(steps.get(step, 7200))


def get_checkpoint_recovery_duration() -> int:
    """Return the streaming probe duration for B4 checkpoint recovery validation."""
    env_override = os.environ.get("IT4931_CHECKPOINT_RECOVERY_DURATION")
    if env_override and env_override.isdigit():
        return int(env_override)
    config = load_timeouts_config()
    acceptance = config.get("acceptance", DEFAULT_CONFIG["acceptance"])
    return int(acceptance.get("checkpoint_recovery_duration", 35))


def get_infrastructure_timeout(name: str) -> int:
    """Return infrastructure command timeout (image pull, load, tag, rollout)."""
    env_override = os.environ.get(f"IT4931_{name.upper()}")
    if env_override and env_override.isdigit():
        return int(env_override)
    config = load_timeouts_config()
    infra = config.get("infrastructure", DEFAULT_CONFIG["infrastructure"])
    return int(infra.get(name, DEFAULT_CONFIG["infrastructure"].get(name, 900)))


def get_capacity_reserve() -> dict[str, int]:
    """Return required host capacity reserve (CPU cores, RAM GiB, disk GiB)."""
    config = load_timeouts_config()
    return config.get("capacity_reserve", DEFAULT_CONFIG["capacity_reserve"])


def get_synthesis_config() -> dict[str, Any]:
    """Return dataset synthesis configuration."""
    config = load_timeouts_config()
    return config.get("synthesis", DEFAULT_CONFIG["synthesis"])


def get_models_config() -> dict[str, Any]:
    """Return recommendation models configuration (ALS, Graph, Hybrid)."""
    config = load_timeouts_config()
    return config.get("models", DEFAULT_CONFIG["models"])


def get_streaming_config() -> dict[str, Any]:
    """Return streaming and replay configuration."""
    config = load_timeouts_config()
    return config.get("streaming", DEFAULT_CONFIG["streaming"])


def get_execution_config() -> dict[str, Any]:
    """Return execution and resume policy configuration."""
    config = load_timeouts_config()
    return config.get("execution", DEFAULT_CONFIG["execution"])
