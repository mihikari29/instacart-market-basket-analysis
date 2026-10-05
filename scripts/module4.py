"""Run Module 4A/4B in the pinned Docker Spark runtime."""

from __future__ import annotations

import argparse
from pathlib import Path
import shutil
import subprocess
import sys


def main(argv=None):
    root = Path(__file__).resolve().parents[1]
    arguments = list(sys.argv[1:] if argv is None else argv)
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--build", action="store_true")
    options, forwarded = parser.parse_known_args(arguments)
    if not forwarded:
        forwarded = ["all"]

    # Help is deliberately side-effect free and works without Docker, Java or PySpark.
    if any(value in {"-h", "--help"} for value in forwarded):
        sys.path.insert(0, str(root))
        from src.module4.cli import main as module4_main

        return module4_main(forwarded)

    if shutil.which("docker") is None:
        raise RuntimeError("Docker CLI is required; install/start Docker Desktop and retry")

    skip_serving = "--skip-serving" in forwarded
    services = ["namenode", "datanode", "spark-master", "spark-worker"]
    if not skip_serving:
        services.append("mongodb")
    up = ["docker", "compose", "up", "-d"]
    if options.build:
        up.append("--build")
    subprocess.run([*up, *services], cwd=root, check=True)

    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    run_args = [
        "docker", "compose", "run", "--rm", "--use-aliases", "--no-deps",
        "-e", f"MODULE4_COMMIT={commit}", "module4", *forwarded,
    ]
    subprocess.run(run_args, cwd=root, check=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except subprocess.CalledProcessError as exc:
        raise SystemExit(exc.returncode or 1)
