"""Source identity: the repository root, the current revision and a code digest.

A run report must state which source produced it. Three modules each
captured that independently, and two of them resolved the source tree
relative to the current working directory, so running the CLI from outside
the repository root crashed with `FileNotFoundError`.
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
import subprocess

#: Repository root, derived from this file's location rather than the cwd.
REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def repository_root() -> Path:
    """Return the repository root."""
    return REPOSITORY_ROOT


def git_revision(fallback_env: str) -> dict:
    """Return the current commit and whether the tree is dirty.

    The `safe.directory` override keeps this usable on a WSL or Windows bind
    mount, where the repository is owned by a different user than the caller.
    """
    root = REPOSITORY_ROOT
    try:
        git = ["git", "-c", f"safe.directory={root}"]
        commit = subprocess.check_output([*git, "rev-parse", "HEAD"], cwd=root, text=True).strip()
        dirty = bool(subprocess.check_output([*git, "status", "--porcelain"], cwd=root, text=True))
        return {"commit": commit, "dirty": dirty}
    except (OSError, subprocess.CalledProcessError):
        return {"commit": os.environ.get(fallback_env, "unavailable"), "dirty": None}


def code_digest() -> str:
    """Return a digest over every Python source file in the repository."""
    digest = hashlib.sha256()
    for path in sorted(REPOSITORY_ROOT.joinpath("src").rglob("*.py")):
        digest.update(path.relative_to(REPOSITORY_ROOT).as_posix().encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
    return digest.hexdigest()
