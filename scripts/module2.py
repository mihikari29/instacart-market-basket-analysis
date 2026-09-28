"""Canonical container runner: python scripts/module2.py [all|validate|stats|...]."""

import subprocess
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    
    args = list(sys.argv[1:])
    force_build = "--build" in args
    if force_build:
        args.remove("--build")

    up_cmd = ["docker", "compose", "up", "-d", "--remove-orphans"]
    if force_build:
        up_cmd.append("--build")
    up_cmd.extend(["spark-master", "spark-worker", "mongodb"])

    subprocess.run(up_cmd, cwd=root, check=True)
    command = args or ["all"]
    subprocess.run(
        [
            "docker",
            "compose",
            "run",
            "--rm",
            "--use-aliases",
            "-e",
            f"MODULE2_COMMIT={commit}",
            "module2",
            *command,
        ],
        cwd=root,
        check=True,
    )


if __name__ == "__main__":
    main()
