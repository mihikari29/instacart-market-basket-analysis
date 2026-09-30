"""One fail-fast entry point for the speed layer.

Mirrors module2.cli: run_id + summary.json + status + code_digest + Spark eventlog.
Subcommands:
    validate                - Kafka + Mongo connectivity only, no streaming
    run                     - streaming query until Ctrl+C or --duration-seconds
    late-demo               - run + guidance to start producer with --inject late
    benchmark-throughput    - Proposal §15.4
    benchmark-partitions    - Proposal §15.5 (caller recreates topic per arm)
    all                     - validate + benchmark-throughput + benchmark-partitions
"""

import argparse
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import uuid

from .config import Config
from .spark import session
from .validate import validate_kafka, validate_mongo


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def revision():
    try:
        sha = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        status = subprocess.check_output(["git", "status", "--porcelain"], text=True)
        return {"commit": sha, "dirty": bool(status)}
    except (OSError, subprocess.CalledProcessError):
        return {"commit": os.environ.get("MODULE3_COMMIT", "unavailable"), "dirty": None}


def code_digest():
    digest = hashlib.sha256()
    for path in sorted(Path("src").rglob("*.py")):
        digest.update(path.as_posix().encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=[
            "validate",
            "run",
            "late-demo",
            "benchmark-throughput",
            "benchmark-partitions",
            "all",
        ],
    )
    parser.add_argument("--root", default=os.environ.get("MODULE3_ROOT", Config.root))
    parser.add_argument("--master", default=os.environ.get("SPARK_MASTER", Config.master))
    parser.add_argument("--shuffle-partitions", type=int, default=8)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--output", type=Path, default=Path("results/module3"))

    # Kafka
    parser.add_argument("--bootstrap-servers", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", Config.bootstrap_servers))
    parser.add_argument("--topic", default=Config.topic)
    parser.add_argument("--starting-offsets", default=Config.starting_offsets)

    # Windows / watermark / weights
    parser.add_argument("--window-short", default=Config.window_short)
    parser.add_argument("--window-long", default=Config.window_long)
    parser.add_argument("--slide", default=Config.slide)
    parser.add_argument("--watermark", default=Config.watermark)
    parser.add_argument("--weight-short", type=float, default=Config.weight_short)
    parser.add_argument("--weight-long", type=float, default=Config.weight_long)
    parser.add_argument("--trigger-interval", default=Config.trigger_interval)

    # Mongo
    parser.add_argument("--mongodb-uri", default=os.environ.get("MONGODB_URI", Config.mongodb_uri))
    parser.add_argument("--mongo-database", default=Config.mongo_database)
    parser.add_argument("--mongo-collection", default=Config.mongo_collection)
    parser.add_argument("--ttl-seconds", type=int, default=Config.ttl_seconds,
                        help="Mongo TTL on updated_at (0 disables). Set 86400 to demo W4 eventual-consistency cleanup.")

    # Run / benchmark durations
    parser.add_argument("--duration-seconds", type=int, default=60,
                        help="Stream run duration in seconds (run/late-demo/benchmarks). 0 = forever (until Ctrl+C).")

    args = parser.parse_args(argv)

    config = Config(
        root=args.root,
        master=args.master,
        shuffle_partitions=args.shuffle_partitions,
        repetitions=args.repetitions,
        warmups=args.warmups,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        starting_offsets=args.starting_offsets,
        window_short=args.window_short,
        window_long=args.window_long,
        slide=args.slide,
        watermark=args.watermark,
        weight_short=args.weight_short,
        weight_long=args.weight_long,
        trigger_interval=args.trigger_interval,
        mongodb_uri=args.mongodb_uri,
        mongo_database=args.mongo_database,
        mongo_collection=args.mongo_collection,
        ttl_seconds=args.ttl_seconds,
    )

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:8]
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
        # Validate is cheap and runs for all subcommands requiring Kafka/Mongo
        report["kafka_validation"] = validate_kafka(config)
        if args.command in ("run", "late-demo", "benchmark-throughput", "benchmark-partitions", "all"):
            if not report["kafka_validation"].get("ok"):
                raise RuntimeError(f"Kafka unreachable: {report['kafka_validation'].get('error')}")
        report["mongo_validation"] = validate_mongo(config)

        if args.command == "validate":
            report["status"] = "passed"
            save(output / "summary.json", report)
            print(f"[ARTIFACTS] {output.resolve()}", flush=True)
            return 0

        # For everything else we need a Spark session
        from .stream import run_stream
        from .benchmark import benchmark_throughput, benchmark_kafka_partitions

        spark = session(config, output / "eventlog")
        report["environment"].update(
            spark=spark.version,
            java=spark._jvm.java.lang.System.getProperty("java.version"),
            spark_conf={
                k: v
                for k, v in spark.sparkContext.getConf().getAll()
                if k.startswith(("spark.sql.", "spark.executor.", "spark.driver.memory", "spark.cores.", "spark.master"))
            },
        )

        if args.command in ("run", "late-demo"):
            duration = args.duration_seconds if args.duration_seconds > 0 else None
            report["stream"] = run_stream(spark, config, output, run_id, duration_seconds=duration)
            if args.command == "late-demo":
                report["late_demo_guidance"] = (
                    "Start the producer with: python -m src.module1.producer --feed "
                    "data/synthesized/scatter_3m --limit-events 50000 --inject \"late:0.05,dup:0.02\". "
                    "Late events past the 10-minute watermark are dropped by Spark; "
                    "those within watermark update the existing window document (idempotent upsert)."
                )
            report["status"] = "passed"
            spark.stop()
            spark = None
            save(output / "summary.json", report)
            print(f"[ARTIFACTS] {output.resolve()}", flush=True)
            return 0

        if args.command in ("all", "benchmark-throughput"):
            report["throughput_benchmark"] = benchmark_throughput(
                spark, config, output, duration_seconds=args.duration_seconds
            )
            print("[throughput_benchmark] complete", flush=True)

        if args.command in ("all", "benchmark-partitions"):
            report["partition_benchmark"] = benchmark_kafka_partitions(
                spark, config, output, duration_seconds=args.duration_seconds
            )
            print("[partition_benchmark] complete", flush=True)

        spark.stop()
        spark = None
        report["status"] = "passed"
        save(output / "summary.json", report)
        print(f"[ARTIFACTS] {output.resolve()}", flush=True)
        return 0
    except Exception as exc:
        report.update(status="failed", error=f"{type(exc).__name__}: {exc}")
        save(output / "summary.json", report)
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
