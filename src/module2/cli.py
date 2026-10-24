"""One fail-fast entry point; every experiment first validates its input."""

import argparse
from dataclasses import asdict
from datetime import datetime, timezone
import os
from pathlib import Path
import platform
import re

from ..common.runtime import safe_error_text
from ..common.runtime import new_run_id, save_json
from ..common.runtime import code_digest, git_revision
from .config import Config, session
from .data import read_tables, validate, require
from .analytics import analyze, publish_mongo, tag_user_features
from .benchmarks import benchmark_joins, benchmark_partitions, benchmark_cache
from .eventlog import parse_event_logs, attach_metrics


def save(path, value):
    save_json(path, value)


def revision():
    return git_revision("MODULE2_COMMIT")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=["validate", "stats", "benchmark-joins", "benchmark-partitions", "benchmark-cache", "all"],
    )
    parser.add_argument("--root", default=os.environ.get("MODULE2_ROOT", Config.root))
    parser.add_argument("--master", default=os.environ.get("SPARK_MASTER", Config.master))
    parser.add_argument("--shuffle-partitions", type=int, default=8)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--output", type=Path, default=Path("results/module2"))
    parser.add_argument("--run-id", help="Stable caller-supplied publication identity for a run-scoped retry")
    parser.add_argument("--min-support", type=int, default=100)
    parser.add_argument(
        "--source-events", type=Path, help="Also verify current local source against staging receipt"
    )
    parser.add_argument(
        "--skip-serving", action="store_true", help="Explicitly skip MongoDB publication (fixture/local runs)"
    )
    args = parser.parse_args(argv)
    require(args.min_support >= 1, "min-support must be positive")
    config = Config(args.root, args.master, args.shuffle_partitions, args.repetitions, args.warmups)
    run_id = args.run_id or new_run_id()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,79}", run_id) or ".." in run_id:
        parser.error("--run-id must be a 1-80 character path-safe identifier")
    output = args.output / run_id
    output.mkdir(parents=True, exist_ok=False)
    report = {
        "run_id": run_id,
        "status": "running",
        "environment": {
            **revision(),
            "source_code_sha256": code_digest(),
            "config": asdict(config),
            "python": platform.python_version(),
            "platform": platform.platform(),
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        },
    }
    spark = None
    try:
        spark = session(config, output / "eventlog")
        report["environment"].update(
            spark=spark.version,
            java=spark._jvm.java.lang.System.getProperty("java.version"),
            spark_conf={
                k: v
                for k, v in spark.sparkContext.getConf().getAll()
                if k.startswith(
                    ("spark.sql.", "spark.executor.", "spark.driver.memory", "spark.cores.", "spark.master")
                )
            },
        )
        tables = read_tables(spark, config)
        report["validation"] = validate(spark, config, tables)
        if args.source_events:
            from src.module1.partition_events import file_sha256

            require(
                file_sha256(args.source_events) == report["validation"]["source_receipt"]["source_sha256"],
                "Current source differs from the staged source",
            )
        print("[HANDOFF VALIDATION]", report["validation"]["counts"], flush=True)
        if args.command in ("all", "stats"):
            summary, outputs = analyze(spark, tables, report["validation"]["counts"], args.min_support)
            save(output / "analytics.json", summary)
            feature_timestamp = report["environment"]["timestamp_utc"]
            tag_user_features(outputs["user_features"], run_id, feature_timestamp).write.mode(
                "overwrite"
            ).parquet(config.features())
            for name in (
                "batch_product_metrics",
                "department_metrics",
                "department_daily_trends",
                "department_hour_metrics",
            ):
                outputs[name].write.mode("error").parquet((output / name).resolve().as_uri())
            if not args.skip_serving:
                publish_mongo(
                    outputs, os.environ.get("MONGODB_URI", "mongodb://localhost:27017/instacart"), run_id
                )
            report["analytics"] = {
                "summary": "analytics.json",
                "mongo_published": not args.skip_serving,
                "user_features": config.features(),
                "user_features_scope": "prior_only",
                "user_features_run_id": run_id,
            }
        for command, key, function, argument in (
            ("benchmark-joins", "join_benchmark", benchmark_joins, tables),
            ("benchmark-partitions", "partition_pruning", benchmark_partitions, tables["interactions"]),
            ("benchmark-cache", "cache_benchmark", benchmark_cache, tables),
        ):
            if args.command in ("all", command):
                report[key] = function(spark, argument, config)
                print(f"[{key}] complete", flush=True)
        spark.stop()
        spark = None
        attach_metrics(report, parse_event_logs(output / "eventlog"))
        report["status"] = "passed"
        save(output / "summary.json", report)
        print(f"[ARTIFACTS] {output.resolve()}", flush=True)
        return 0
    except Exception as exc:
        report.update(status="failed", error=safe_error_text(exc))
        save(output / "summary.json", report)
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
