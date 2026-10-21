"""Typed environment-variable readers with consistent failure behaviour.

An environment variable is untrusted input. A missing variable falls back to
the declared default; a malformed value is a configuration error and must be
reported as one instead of silently becoming `None` or zero.
"""

from __future__ import annotations

import os


def env_int(name: str, default: int | None) -> int | None:
    """Read an integer environment variable, falling back to `default`."""
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {value!r}") from exc


def env_float(name: str, default: float | None) -> float | None:
    """Read a float environment variable, falling back to `default`."""
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number, got {value!r}") from exc


def parse_bool(value: str | bool) -> bool:
    """Parse an explicit environment/CLI boolean without truthy-string traps."""
    if isinstance(value, bool):
        return value
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(
        "Boolean value must be one of: true, false, 1, 0, yes, no, on, off"
    )
