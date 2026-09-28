"""Canonical container runner: python scripts/module2.py [all|validate|stats|...]."""

import argparse
import subprocess
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()

    parser = argparse.ArgumentParser(description=__doc__, add_help=False)
    parser.add_argument("--build", action="store_true", help="Force rebuild Docker images")
    known, forward_args = parser.parse_known_args()

    build_flags = ["--build"] if known.build else []
    subprocess.run(
        ["docker", "compose", "up", "-d", "--remove-orphans", *build_flags, "spark-master", "spark-worker", "mongodb"],
        cwd=root,
        check=True,
    )
    command = forward_args or ["all"]
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
