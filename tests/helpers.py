"""Shared test helpers: build-context admission and MongoDB fakes."""

from __future__ import annotations

from pathlib import Path
import re
from typing import Any




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




class FakeUpdateOne:
    """Record of an `update_one` call, as the writer would issue it."""

    def __init__(self, filter_document, update_document, upsert):
        self.filter = filter_document
        self.update = update_document
        self.upsert = upsert


class FakeBulkResult:
    """The counters PyMongo returns from a bulk write."""

    def __init__(self, upserted=0, matched=0, modified=0):
        self.upserted_count = upserted
        self.matched_count = matched
        self.modified_count = modified


class FakeClient:
    """A MongoClient stand-in that always resolves one database."""

    def __init__(self, database):
        self.database = database

    def __getitem__(self, name):
        assert name == "instacart"
        return self.database


class FindCommandRecorder:
    """A PyMongo command listener that records the issued find commands."""

    def __init__(self):
        self.find_commands: list[dict[str, Any]] = []

    def started(self, event):
        if event.command_name == "find":
            self.find_commands.append(dict(event.command))

    def succeeded(self, event):
        pass

    def failed(self, event):
        pass
