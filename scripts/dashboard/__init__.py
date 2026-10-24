"""Operational commands for the Module 4D dashboard: demo, fixtures and validation.

The bounded Module 4C experiment harnesses live in `scripts/experiments/`;
they are separate from the dashboard tools that ship in the API image.
"""

from __future__ import annotations

import os


def required(name: str) -> str:
    """Return a required environment variable, or fail with its name.

    The demo harness and its validator both need the same private
    configuration, and neither may continue with a silently empty value.
    """
    value = os.environ.get(name, "")
    if not value:
        raise RuntimeError(f"{name} is required")
    return value
