"""Canonical container runner: python scripts/module2.py [all|validate|stats|...]."""

try:  # `python3 -m scripts.module2` from the repository root
    from scripts.compose_runner import MODULE_RUNNERS, main_entry
except ImportError:  # `python3 scripts/module2.py`
    from compose_runner import MODULE_RUNNERS, main_entry


if __name__ == "__main__":
    main_entry(MODULE_RUNNERS["module2"])
