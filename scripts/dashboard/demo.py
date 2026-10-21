"""Start or stop one task-owned Module 4D synthetic demonstration.

The demo is the fastest way to see the serving layer working: it builds the
API and UI images, starts an isolated MongoDB on tmpfs, publishes a synthetic
fixture through the accepted publishers, starts the API and the dashboard,
and then verifies that every HTTP response matches what was stored.

The shell wrapper `demo.sh` calls this module, so the documented Linux
procedure and the Windows and macOS procedure run exactly the same steps.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import uuid

ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = ROOT / "deploy" / "dashboard" / "compose.demo.yaml"
RUN_ID_PATTERN = re.compile(r"[a-f0-9]{12}\Z")
TASK_IMAGES = ("instacart-module4d-demo-api", "instacart-module4d-demo-ui")

#: The demo's environment variables, cleared before a start so an inherited
#: value can never steer a new run.
DEMO_VARIABLES = (
    "MODULE4D_DEMO_TAG",
    "MODULE4D_DEMO_DATABASE",
    "MODULE4D_DEMO_ROOT_PASSWORD",
    "MODULE4D_DEMO_READONLY_USERNAME",
    "MODULE4D_DEMO_READONLY_PASSWORD",
    "MODULE4D_DEMO_TREND_NAMESPACE",
    "MODULE4D_DEMO_API_HOST_PORT",
    "MODULE4D_DEMO_UI_HOST_PORT",
)


class DemoError(RuntimeError):
    """A demo step could not complete."""


def env_file_for(run_id: str) -> Path:
    """Return the private environment file that owns one run."""
    return Path(tempfile.gettempdir()) / f"module4d-demo-{run_id}.env"


def project_for(run_id: str) -> str:
    """Return the Compose project name that owns one run."""
    return f"module4d-demo-{run_id}"


def compose_command(run_id: str) -> list[str]:
    """Return the Compose invocation for one run."""
    return [
        "docker",
        "compose",
        "--project-name",
        project_for(run_id),
        "--env-file",
        str(env_file_for(run_id)),
        "-f",
        str(COMPOSE_FILE),
    ]


def run(command: list[str], *, capture: bool = False) -> subprocess.CompletedProcess:
    """Run a command and fail with its output when it does not succeed."""
    result = subprocess.run(
        command,
        cwd=ROOT,
        check=False,
        capture_output=capture,
        text=True,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        raise DemoError(f"{command[0]} {' '.join(command[1:3])} failed: {detail[:400]}")
    return result


def read_tag(env_file: Path) -> str:
    """Return the image tag recorded in the private environment file."""
    for line in env_file.read_text(encoding="utf-8").splitlines():
        if line.startswith("MODULE4D_DEMO_TAG="):
            return line.split("=", 1)[1]
    raise DemoError("the private environment file does not record a demo tag")


def stop_demo(run_id: str) -> None:
    """Stop the demo, remove its images and delete its private file."""
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise DemoError("run ID must be exactly 12 lowercase hexadecimal characters")
    env_file = env_file_for(run_id)
    if not env_file.is_file():
        raise DemoError(f"no private environment file exists for demo run {run_id}")
    try:
        run(compose_command(run_id) + ["down", "--remove-orphans"])
    except DemoError as exc:
        raise DemoError(f"could not stop demo {run_id}; keeping its private file: {exc}") from exc
    tag = read_tag(env_file)
    if not RUN_ID_PATTERN.fullmatch(tag):
        raise DemoError("the private environment file has an invalid image tag; keeping it")
    for name in TASK_IMAGES:
        inspect = subprocess.run(
            ["docker", "image", "inspect", f"{name}:{tag}"],
            check=False,
            capture_output=True,
        )
        if inspect.returncode == 0:
            remove = subprocess.run(
                ["docker", "image", "rm", "--force", f"{name}:{tag}"],
                check=False,
                capture_output=True,
            )
            if remove.returncode != 0:
                raise DemoError(f"could not remove the task-owned image {name}:{tag}")
    env_file.unlink(missing_ok=True)
    print(f"Stopped and removed the task-owned demo {run_id}; tmpfs data was discarded.")


def start_demo(run_id: str | None = None, env_file: Path | None = None) -> str:
    """Start a complete demo session and return its run identifier."""
    for name in DEMO_VARIABLES:
        os.environ.pop(name, None)
    run_id = run_id or uuid.uuid4().hex[:12]
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise DemoError("run ID must be exactly 12 lowercase hexadecimal characters")
    env_file = env_file or env_file_for(run_id)
    env_file.unlink(missing_ok=True)
    try:
        run([sys.executable, "scripts/dashboard/demo_env.py", "--output", str(env_file)])
        if os.name != "nt":
            env_file.chmod(0o600)
        compose = compose_command(run_id)
        run(compose + ["config", "--quiet"])
        run(compose + ["build", "module4d-api", "module4d-ui"])
        run(compose + ["up", "-d", "module4d-mongo"])
        run(compose + ["run", "--rm", "fixture"])
        run(compose + ["up", "-d", "module4d-api", "module4d-ui"])
        run(compose + ["run", "--rm", "validator"])
    except Exception:
        print(f"Demo startup failed; cleaning only the task-owned run {run_id}.", file=sys.stderr)
        try:
            stop_demo(run_id)
        except DemoError:
            pass
        raise

    ports = {}
    for line in env_file.read_text(encoding="utf-8").splitlines():
        if "=" in line:
            key, _, value = line.partition("=")
            ports[key] = value
    api_port = ports.get("MODULE4D_DEMO_API_HOST_PORT", "18000")
    ui_port = ports.get("MODULE4D_DEMO_UI_HOST_PORT", "18501")
    print(f"Module 4D demo is available at http://127.0.0.1:{ui_port}")
    print(f"API health: http://127.0.0.1:{api_port}/health")
    print(f"Run ID: {run_id}")
    print(f"Stop and discard the demo with: python scripts/dashboard/demo.py down {run_id}")
    return run_id


def main(argv: list[str] | None = None) -> int:
    """Run the demo command."""
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    up = commands.add_parser("up", help="start a new demo session")
    up.add_argument("--run-id", help="use this 12 hexadecimal run identifier")
    up.add_argument("--env-file", type=Path, help="write the private environment file here")
    down = commands.add_parser("down", help="stop a demo session and discard its data")
    down.add_argument("run_id", help="the 12 hexadecimal run identifier")
    arguments = parser.parse_args(argv)
    if arguments.command == "up":
        start_demo(arguments.run_id, arguments.env_file)
    else:
        stop_demo(arguments.run_id)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except DemoError as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1) from None
