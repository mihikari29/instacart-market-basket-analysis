"""Centralized configuration and operational timeout management for Kubernetes runners."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any
import yaml

TIMEOUTS_FILE_DEFAULT = Path(__file__).resolve().parents[2] / "deploy" / "k8s" / "timeouts.yaml"

DEFAULT_CONFIG: dict[str, Any] = {
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
        "memory_gib": 8,
        "free_disk_gib": 40,
    },
}


def load_timeouts_config(path: Path | str | None = None) -> dict[str, Any]:
    """Load configuration from YAML file with in-memory defaults fallback."""
    config_path = Path(path) if path else Path(os.environ.get("IT4931_TIMEOUTS_FILE", TIMEOUTS_FILE_DEFAULT))
    if config_path.is_file():
        try:
            loaded = yaml.safe_load(config_path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                return {
                    "pipeline_steps": {**DEFAULT_CONFIG["pipeline_steps"], **loaded.get("pipeline_steps", {})},
                    "acceptance": {**DEFAULT_CONFIG["acceptance"], **loaded.get("acceptance", {})},
                    "infrastructure": {**DEFAULT_CONFIG["infrastructure"], **loaded.get("infrastructure", {})},
                    "capacity_reserve": {**DEFAULT_CONFIG["capacity_reserve"], **loaded.get("capacity_reserve", {})},
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
