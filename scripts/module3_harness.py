"""Shared harness for the Module 3 smoke and recovery scripts.

The bounded smoke run and the two-process checkpoint-recovery run need the
same five helpers: run a command, run Python inside the Compose service,
locate the artifact path a run printed, prepare an output directory the
container can write to, and archive a checkpoint before a run replaces it.
Keeping them here means the two harnesses cannot drift apart in how they talk
to the container.
"""

from __future__ import annotations

import os
from pathlib import Path
import re
import shlex
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def run_command(command: list[str], capture: bool = False) -> subprocess.CompletedProcess:
    """Run a repository command and optionally echo its output."""
    result = subprocess.run(
        command,
        cwd=ROOT,
        check=True,
        text=True,
        capture_output=capture,
    )
    if capture:
        print(result.stdout, end="", flush=True)
        if result.stderr:
            print(result.stderr, end="", flush=True)
    return result


def compose_python(arguments: list[str], capture: bool = False) -> subprocess.CompletedProcess:
    """Run a Python entry point inside the Module 3 Compose service."""
    return run_command(
        [
            "docker",
            "compose",
            "--profile",
            "tools",
            "run",
            "--rm",
            "--no-deps",
            "--entrypoint",
            "python3",
            "module3",
            *arguments,
        ],
        capture=capture,
    )


def artifact_path(output: str) -> Path:
    """Return the repository path a run reported in its artifact line."""
    match = re.search(r"\[ARTIFACTS\]\s+(/workspace/\S+)", output)
    if not match:
        raise RuntimeError(f"Could not locate artifact path in output:\n{output}")
    return ROOT / Path(match.group(1)).relative_to("/workspace")


def ensure_output(path: Path) -> None:
    """Create an output directory the container is also able to write to."""
    try:
        path.mkdir(parents=True, exist_ok=True)
        return
    except PermissionError:
        pass
    container_path = Path("/workspace") / path.relative_to(ROOT)
    command = (
        f"mkdir -p {shlex.quote(str(container_path))} && "
        f"chown {os.getuid()}:{os.getgid()} {shlex.quote(str(container_path))}"
    )
    run_command(
        [
            "docker", "compose", "--profile", "tools", "run", "--rm", "--no-deps",
            "--entrypoint", "sh", "module3", "-c", command,
        ]
    )


def archive_checkpoint(checkpoint: Path, archived: Path) -> None:
    """Move a checkpoint aside so a new run can create its own."""
    try:
        checkpoint.rename(archived)
        return
    except PermissionError:
        pass
    checkpoint_container = Path("/workspace") / checkpoint.relative_to(ROOT)
    archived_container = Path("/workspace") / archived.relative_to(ROOT)
    command = (
        f"mv {shlex.quote(str(checkpoint_container))} {shlex.quote(str(archived_container))} && "
        f"chown -R {os.getuid()}:{os.getgid()} {shlex.quote(str(archived_container.parent))}"
    )
    run_command(
        [
            "docker", "compose", "--profile", "tools", "run", "--rm", "--no-deps",
            "--entrypoint", "sh", "module3", "-c", command,
        ]
    )
