"""Compare bounded Spark local execution configurations on the same source.

This harness compares Spark's local[1] and local[2] thread pools. It does not
claim to scale native Spark executors and is deliberately separate from the
normal batch runner, which may target a different Spark master.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import statistics
import subprocess
import sys
import time
import uuid
from typing import Any, Callable, Sequence

from .config import Config


LOCAL_ARMS = ("local[1]", "local[2]")


def validate_arms(arms: Sequence[str]) -> tuple[str, ...]:
    values = tuple(arms)
    if len(values) < 2 or len(set(values)) != len(values):
        raise ValueError("select at least two distinct execution arms")
    if any(value not in LOCAL_ARMS for value in values):
        raise ValueError("only the explicitly supported local[1] and local[2] arms are allowed")
    return values


def build_plan(
    *, root: str, output: str | Path, run_id: str, arms: Sequence[str] = LOCAL_ARMS,
    repetitions: int = 3, warmups: int = 1, shuffle_partitions: int = 8,
    timeout_seconds: int = 1200,
) -> dict[str, Any]:
    selected = validate_arms(arms)
    if repetitions < 3 or warmups < 1 or shuffle_partitions < 1:
        raise ValueError("require at least three repetitions and one warm-up")
    if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 7200:
        raise ValueError("timeout_seconds must be between 1 and 7200")
    if not root or root.rstrip("/") in ("", "hdfs:", "file:"):
        raise ValueError("an explicit dataset root is required")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,79}", run_id) or ".." in run_id:
        raise ValueError("run_id must be a 1-80 character path-safe identifier")
    base = Path(output).resolve() / run_id
    return {
        "run_id": run_id,
        "output_directory": str(base),
        "root": root,
        "arms": list(selected),
        "repetitions": repetitions,
        "warmups": warmups,
        "shuffle_partitions": shuffle_partitions,
        "timeout_seconds": timeout_seconds,
        "spark_session_settings": {
            "spark.sql.adaptive.enabled": "false",
            "spark.sql.autoBroadcastJoinThreshold": "-1",
            "spark.cores.max": "2",
            "spark.executor.memory": "1g",
            "spark.executor.cores": "2",
        },
        "execution_scope": "single-host Spark local thread pool comparison",
        "executor_scaling_claimed": False,
        "workload": "aggregate interaction rows by product and collect the compact product totals",
        "arms_plan": [
            {
                "master": master,
                "execution_model": "Spark local mode",
                "native_executor_scaling": False,
                "command": [
                    sys.executable, "-m", "src.module2.executor_benchmark", "worker",
                    "--root", root, "--master", master,
                    "--repetitions", str(repetitions), "--warmups", str(warmups),
                    "--shuffle-partitions", str(shuffle_partitions),
                    "--output", str(base), "--run-id", master.replace("[", "-").replace("]", ""),
                ],
            }
            for master in selected
        ],
    }


def execute_plan(
    plan: dict[str, Any], runner: Callable[..., Any] = subprocess.run, *, timeout_seconds: int | None = None,
) -> dict[str, Any]:
    """Run arms sequentially and fail if source identity or outputs differ."""
    output = Path(plan["output_directory"])
    timeout = timeout_seconds if timeout_seconds is not None else plan.get("timeout_seconds", 1200)
    if type(timeout) is not int or not 1 <= timeout <= 7200:
        raise ValueError("execution timeout must be between 1 and 7200 seconds")
    output.mkdir(parents=True, exist_ok=False)
    (output / "plan.json").write_text(
        json.dumps(plan, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    summaries = []
    for arm in plan["arms_plan"]:
        runner(arm["command"], check=True, timeout=timeout)
        run_id = arm["command"][arm["command"].index("--run-id") + 1]
        summary_path = output / run_id / "summary.json"
        if not summary_path.is_file():
            raise RuntimeError(f"execution arm {arm['master']} did not write summary.json")
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        if summary.get("status") != "passed":
            raise RuntimeError(f"execution arm {arm['master']} did not pass")
        summaries.append(summary)
    signatures = {
        (
            item["validation"]["source_receipt"]["source_sha256"],
            item["validation"]["counts"]["interactions"],
            item["workload"]["checksum"],
        )
        for item in summaries
    }
    if len(signatures) != 1:
        raise RuntimeError("execution arms used different source identities or produced different outputs")
    result = {
        "run_id": plan["run_id"],
        "status": "passed",
        "execution_scope": plan["execution_scope"],
        "executor_scaling_claimed": False,
        "same_source_and_output_verified": True,
        "arms": summaries,
        "limitations": [
            "local[1] and local[2] compare local task threads on one host",
            "the comparison does not measure Kubernetes pod or native executor scaling",
            "host caches, OS scheduling and background load remain uncontrolled",
        ],
    }
    (output / "summary.json").write_text(
        json.dumps(result, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8"
    )
    return result


def _source_revision() -> dict[str, Any]:
    try:
        commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        dirty = bool(subprocess.check_output(["git", "status", "--porcelain"], text=True))
        return {"commit": commit, "dirty": dirty}
    except (OSError, subprocess.CalledProcessError):
        return {"commit": os.environ.get("MODULE2_COMMIT", "unavailable"), "dirty": None}


def _code_digest() -> str:
    digest = hashlib.sha256()
    for path in sorted(Path("src").rglob("*.py")):
        digest.update(path.as_posix().encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


def run_worker(args: argparse.Namespace) -> int:
    from pyspark.sql import functions as F

    from .data import read_tables, validate
    from .eventlog import attach_metrics, parse_event_logs
    from .benchmarks import trial
    from .spark import session

    config = Config(
        root=args.root,
        master=args.master,
        shuffle_partitions=args.shuffle_partitions,
        repetitions=args.repetitions,
        warmups=args.warmups,
    )
    output = Path(args.output) / args.run_id
    output.mkdir(parents=True, exist_ok=False)
    report: dict[str, Any] = {
        "run_id": args.run_id,
        "status": "running",
        "config": {
            "root": config.root,
            "master": config.master,
            "shuffle_partitions": config.shuffle_partitions,
            "repetitions": config.repetitions,
            "warmups": config.warmups,
        },
        "environment": {
            **_source_revision(),
            "source_code_sha256": _code_digest(),
            "python": platform.python_version(),
            "platform": platform.platform(),
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "requested_master": config.master,
        },
    }
    spark = None
    try:
        spark = session(config, output / "eventlog")
        report["environment"].update(
            spark=spark.version,
            java=spark._jvm.java.lang.System.getProperty("java.version"),
            actual_master=spark.sparkContext.master,
            spark_conf={
                key: value
                for key, value in spark.sparkContext.getConf().getAll()
                if key.startswith(
                    ("spark.sql.shuffle.partitions", "spark.executor.", "spark.cores.", "spark.master")
                )
            },
            execution_model="Spark local mode",
            native_executor_scaling=False,
        )
        tables = read_tables(spark, config)
        validation = validate(spark, config, tables)
        interactions = tables["interactions"]
        location = spark._jvm.org.apache.hadoop.fs.Path(config.table("interactions"))
        fs = location.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
        data_bytes = int(fs.getContentSummary(location).getLength())
        aggregate = interactions.groupBy("product_id").agg(
            F.count(F.lit(1)).alias("purchase_count")
        )
        results = []
        for index in range(-config.warmups, config.repetitions):
            measured = trial(
                spark,
                aggregate,
                f"executor-comparison/{config.master}/{index}",
            )
            results.append({"warmup": index < 0, **measured})
        measured_results = [item for item in results if not item["warmup"]]
        if len({item["checksum"] for item in measured_results}) != 1:
            raise RuntimeError("repeated execution trials produced different aggregate outputs")
        report["validation"] = validation
        report["workload"] = {
            "input_rows": validation["counts"]["interactions"],
            "input_bytes": data_bytes,
            "checksum": measured_results[0]["checksum"],
            "runs_seconds": [item["seconds"] for item in measured_results],
            "median_seconds": statistics.median(item["seconds"] for item in measured_results),
            "warmups": [item for item in results if item["warmup"]],
            "trials": measured_results,
            "shuffle_metrics_source": "Spark event log task metrics attached by job_group",
        }
        spark.stop()
        spark = None
        attach_metrics(report["workload"], parse_event_logs(output / "eventlog"))
        report["status"] = "passed"
        _write_json(output / "summary.json", report)
        return 0
    except Exception as exc:
        report.update(status="failed", error=f"{type(exc).__name__}: {exc}")
        _write_json(output / "summary.json", report)
        raise
    finally:
        if spark is not None:
            spark.stop()


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    plan_parser = subparsers.add_parser("plan", help="print an offline execution plan")
    run_parser = subparsers.add_parser("run", help="execute local[1] and local[2] source-backed arms")
    for command_parser in (plan_parser, run_parser):
        command_parser.add_argument("--root", default=os.environ.get("MODULE2_ROOT", Config.root))
        command_parser.add_argument("--output", type=Path, default=Path("results/module2-executor"))
        command_parser.add_argument("--run-id", default=None)
        command_parser.add_argument("--arms", default=",".join(LOCAL_ARMS))
        command_parser.add_argument("--repetitions", type=int, default=3)
        command_parser.add_argument("--warmups", type=int, default=1)
        command_parser.add_argument("--shuffle-partitions", type=int, default=8)
    worker_parser = subparsers.add_parser("worker", help=argparse.SUPPRESS)
    worker_parser.add_argument("--root", required=True)
    worker_parser.add_argument("--master", choices=LOCAL_ARMS, required=True)
    worker_parser.add_argument("--repetitions", type=int, default=3)
    worker_parser.add_argument("--warmups", type=int, default=1)
    worker_parser.add_argument("--shuffle-partitions", type=int, default=8)
    worker_parser.add_argument("--output", type=Path, required=True)
    worker_parser.add_argument("--run-id", required=True)
    args = parser.parse_args(argv)
    if args.command == "worker":
        return run_worker(args)
    run_id = args.run_id or (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:8])
    plan = build_plan(
        root=args.root,
        output=args.output,
        run_id=run_id,
        arms=tuple(args.arms.split(",")),
        repetitions=args.repetitions,
        warmups=args.warmups,
        shuffle_partitions=args.shuffle_partitions,
    )
    if args.command == "plan":
        print(json.dumps(plan, indent=2, sort_keys=True))
        return 0
    result = execute_plan(plan)
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
