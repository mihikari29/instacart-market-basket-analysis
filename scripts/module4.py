"""Run Module 4A/4B in the pinned Docker Spark runtime."""

try:  # `python3 -m scripts.module4` from the repository root
    from scripts.compose_runner import MODULE_RUNNERS, main_entry
except ImportError:  # `python3 scripts/module4.py`
    from compose_runner import MODULE_RUNNERS, main_entry


if __name__ == "__main__":
    main_entry(MODULE_RUNNERS["module4"])
