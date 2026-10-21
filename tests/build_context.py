"""Decide whether a path enters the Docker build context.

Docker's pattern matcher walks the ignore file in order and keeps the last
match. A pattern also matches every file beneath a directory it names, so
`!scripts/` admits the whole tree. This module reproduces that rule so the
tests assert what actually reaches the image rather than the exact spelling
of an allow-list line.
"""

from __future__ import annotations

from pathlib import Path
import re


def _pattern_matches(pattern: str, path: str) -> bool:
    """Return whether one ignore pattern matches one repository-relative path."""
    if pattern == "**":
        return True
    cleaned = pattern.rstrip("/")
    expression = "".join(
        ".*" if part == "**" else "[^/]*" if part == "*" else "[^/]" if part == "?" else part
        for part in re.split(r"(\*\*|\*|\?)", cleaned)
    )
    return re.fullmatch(expression, path) is not None


def _candidates(path: str) -> list[str]:
    """Return the path and each of its parent directories."""
    parts = path.split("/")
    return ["/".join(parts[: index + 1]) for index in range(len(parts))]


def admitted(ignore_lines: list[str] | Path, path: str) -> bool:
    """Return whether `path` is admitted into the build context.

    A line that starts with `!` admits the paths it matches; any other line
    excludes them. The last matching line wins, and a pattern that names a
    directory also applies to everything beneath it.
    """
    if isinstance(ignore_lines, Path):
        ignore_lines = ignore_lines.read_text(encoding="utf-8").splitlines()
    return context_paths_from_lines(ignore_lines, [path])[path]


def context_paths(ignore_file: Path, relative_paths: list[str]) -> dict[str, bool]:
    """Return the admission decision for each path, read from an ignore file."""
    lines = ignore_file.read_text(encoding="utf-8").splitlines()
    return context_paths_from_lines(lines, relative_paths)


def context_paths_from_lines(ignore_lines: list[str], relative_paths: list[str]) -> dict[str, bool]:
    """Return the admission decision for each path."""
    included = False
    decisions: dict[str, bool] = {}
    for path in relative_paths:
        included = False
        for line in ignore_lines:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            is_inclusion = stripped.startswith("!")
            pattern = stripped[1:] if is_inclusion else stripped
            if any(_pattern_matches(pattern, candidate) for candidate in _candidates(path)):
                included = is_inclusion
        decisions[path] = included
    return decisions
