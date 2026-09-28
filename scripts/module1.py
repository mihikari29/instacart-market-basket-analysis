"""Canonical runner for Module 1 pipeline: python scripts/module1.py [all|clean|generate|stage]."""

import argparse
import subprocess
import sys
from pathlib import Path


def run_step(module_name: str, *extra_args: str) -> None:
    root = Path(__file__).resolve().parents[1]
    command = [sys.executable, "-m", f"src.module1.{module_name}", *extra_args]
    print(f"==> [Module 1] Running: {' '.join(command)}", flush=True)
    subprocess.run(command, cwd=root, check=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "step",
        nargs="?",
        default="all",
        choices=["all", "clean", "generate", "stage"],
        help="Pipeline step to execute (default: all)",
    )
    parser.add_argument("--scenario", default="default", help="Synthetic generation preset scenario")
    parser.add_argument("--scatter-weeks", type=int, default=13, help="Scatter window in weeks (default: 13)")
    parser.add_argument(
        "--out-dir",
        default="data/synthesized/scatter_3m",
        help="Output directory for generated feed (default: data/synthesized/scatter_3m)",
    )
    args = parser.parse_args()

    if args.step in ("all", "clean"):
        run_step("clean", "--validate")
    if args.step in ("all", "generate"):
        run_step(
            "generate",
            "--scenario",
            args.scenario,
            "--scatter-weeks",
            str(args.scatter_weeks),
            "--out-dir",
            args.out_dir,
        )
    if args.step == "stage":
        run_step("stage_hdfs")


if __name__ == "__main__":
    main()
