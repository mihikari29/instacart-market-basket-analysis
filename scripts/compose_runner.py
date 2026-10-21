"""Shared plumbing for the Module 2/3/4 container runners.

`scripts/module2.py`, `scripts/module3.py` and `scripts/module4.py` all follow the
same shape: parse one wrapper-only flag, make sure the Compose services the
module needs are healthy, then run the module CLI inside the pinned Spark
image. This module owns that shape so the three runners cannot drift apart in
argument handling, exit-code propagation or service selection.

Two behaviours are deliberate and are exercised by every runner:

* `--help` never starts the Compose project. Showing usage is a read-only
  operation, so it must not create containers, volumes or orphaned services.
* The child process exit code is propagated. A failed module run must not be
  reported as a generic wrapper failure.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import shutil
import subprocess
import sys

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def repository_root() -> Path:
    """Return the repository root, independent of the current directory."""
    return REPOSITORY_ROOT


def git_revision(root: Path) -> str:
    """Return the current commit, tolerating a repository owned by another user.

    Git refuses to operate on a repository whose ownership differs from the
    current user, which is the normal case for a WSL or Windows bind mount.
    The explicit `safe.directory` override keeps the runner usable there.
    """
    return subprocess.check_output(
        ["git", "-c", f"safe.directory={root}", "rev-parse", "HEAD"],
        cwd=root,
        text=True,
    ).strip()


def require_docker() -> None:
    """Fail with an actionable message when the Docker CLI is unavailable."""
    if shutil.which("docker") is None:
        raise RuntimeError("Docker CLI is required; install/start Docker Desktop and retry")


def run_checked(arguments: list[str], *, cwd: Path | None = None) -> None:
    """Run a command and raise `CalledProcessError` on a non-zero exit code."""
    subprocess.run(arguments, cwd=cwd, check=True)


@dataclass(frozen=True)
class ModuleRunner:
    """Declarative description of one module's Compose service contract."""

    service: str
    services: tuple[str, ...]
    commit_env: str
    default_command: tuple[str, ...] = ("all",)
    wait_for_health: bool = False
    remove_orphans: bool = False
    no_deps: bool = False
    serving_service: str | None = None
    help_target: str = ""

    def services_for(self, forwarded: list[str]) -> tuple[str, ...]:
        """Return the services this invocation needs.

        The serving service is only started when the run can publish output,
        because a compute-only or validation run must not touch the database.
        """
        if self.serving_service is None:
            return self.services
        publishes = "--skip-serving" not in forwarded and forwarded[:1] != ["validate"]
        return self.services + ((self.serving_service,) if publishes else ())

    def help_command(self, root: Path) -> list[str]:
        """Return the container command that renders this module's usage."""
        return [
            "docker", "compose", "--profile", "tools", "run", "--rm",
            "--use-aliases", "--no-deps", self.service, "--help",
        ]


MODULE_RUNNERS: dict[str, ModuleRunner] = {
    "module2": ModuleRunner(
        service="module2",
        services=("spark-master", "spark-worker", "mongodb"),
        commit_env="MODULE2_COMMIT",
        remove_orphans=True,
        help_target="src.module2.cli",
    ),
    "module3": ModuleRunner(
        service="module3",
        services=("spark-master", "spark-worker", "mongodb", "kafka"),
        commit_env="MODULE3_COMMIT",
        remove_orphans=True,
        help_target="src.module3.cli",
    ),
    "module4": ModuleRunner(
        service="module4",
        services=("namenode", "datanode", "spark-master", "spark-worker"),
        commit_env="MODULE4_COMMIT",
        wait_for_health=True,
        no_deps=True,
        serving_service="mongodb",
        help_target="src.module4.cli",
    ),
}


def parse_wrapper_arguments(argv: list[str]) -> tuple[bool, list[str]]:
    """Split the wrapper-only `--build` flag from the forwarded module command.

    `--build` is consumed by the wrapper and can never reach the module CLI.
    """
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--build", action="store_true", help="Force rebuild Docker images")
    options, forwarded = parser.parse_known_args(argv)
    return options.build, list(forwarded)


def show_help(runner: ModuleRunner, forwarded: list[str], root: Path) -> int:
    """Render module usage without starting any Docker service.

    The module CLI is imported in-process when its dependencies are available.
    When they are not, the equivalent container command is printed instead so
    the operator still gets the usage text and a way to obtain it.
    """
    arguments = forwarded or list(runner.default_command)
    sys.path.insert(0, str(root))
    try:
        module = __import__(runner.help_target, fromlist=["main"])
    except ImportError as exc:
        print(
            f"Module usage needs the pinned runtime dependencies ({exc}).",
            file=sys.stderr,
        )
        print("Run:", " ".join(runner.help_command(root)), file=sys.stderr)
        return 0
    return int(module.main(arguments) or 0)


def run_module(runner: ModuleRunner, forwarded: list[str], *, build: bool, root: Path) -> int:
    """Start the required services and run the module CLI in the Spark image."""
    require_docker()
    services = runner.services_for(forwarded)
    up = ["docker", "compose", "up", "-d"]
    if runner.wait_for_health:
        up.append("--wait")
    if runner.remove_orphans:
        up.append("--remove-orphans")
    if build:
        up.append("--build")
    run_checked([*up, *services], cwd=root)

    commit = git_revision(root)
    run = ["docker", "compose", "run", "--rm", "--use-aliases"]
    if runner.no_deps:
        run.append("--no-deps")
    run += ["-e", f"{runner.commit_env}={commit}", runner.service, *forwarded]
    run_checked(run, cwd=root)
    return 0


def execute(runner: ModuleRunner, argv: list[str]) -> int:
    """Run one module with the shared wrapper contract."""
    root = repository_root()
    build, forwarded = parse_wrapper_arguments(argv)
    if not forwarded:
        forwarded = list(runner.default_command)
    if any(value in {"-h", "--help"} for value in forwarded):
        return show_help(runner, forwarded, root)
    return run_module(runner, forwarded, build=build, root=root)


def main_entry(runner: ModuleRunner, argv: list[str] | None = None) -> None:
    """CLI entry point that propagates the child process exit code."""
    arguments = list(sys.argv[1:] if argv is None else argv)
    try:
        raise SystemExit(execute(runner, arguments))
    except subprocess.CalledProcessError as exc:
        raise SystemExit(exc.returncode or 1) from None
