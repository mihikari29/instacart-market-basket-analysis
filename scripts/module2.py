"""Canonical container runner: python scripts/module2.py [all|validate|stats|...]."""

import subprocess
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    subprocess.run(
        ["docker", "compose", "up", "-d", "--build", "spark-master", "spark-worker", "mongodb"],
        cwd=root,
        check=True,
    )
    command = sys.argv[1:] or ["all"]
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
