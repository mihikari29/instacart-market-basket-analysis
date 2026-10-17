"""Container-side B3 work steps. Each step writes a run-scoped sanitized receipt."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import time
from threading import Thread
from typing import Any

from .data_receipts import inspect_input_directory
from .fixture_data import write_fixture
from .pipeline import (
    FIXTURE_MAX_EVENTS,
    FIXTURE_MAX_USERS,
    TREND_SOURCE_NAMESPACE,
    validate_full_data_storage_budget,
)


WORKSPACE = Path("/workspace")
RUNTIME_ROOT = Path("/run/pipeline")
INPUT_ROOT = RUNTIME_ROOT / "input"
_MONGO_URI = re.compile(r"(?i)mongodb(?:\+srv)?://[^\s\"'<>]+")
_SECRET_ASSIGNMENT = re.compile(r"(?i)\b(password|passwd|pwd|token|secret)(\s*[:=]\s*)([^\s,;&]+)")


class TaskError(RuntimeError):
    """A container step failed without exposing command secrets."""


def _safe(value: str) -> str:
    return _SECRET_ASSIGNMENT.sub(r"\1\2<redacted>", _MONGO_URI.sub("mongodb://<redacted>", value))


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    path.chmod(0o600)


def _artifact_inventory(root: Path) -> list[dict[str, Any]]:
    if not root.is_dir() or root.is_symlink():
        raise TaskError("pipeline output directory is missing or unsafe")
    artifacts = []
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise TaskError("pipeline output contains an unexpected symbolic link")
        if not path.is_file():
            continue
        digest = hashlib.sha256()
        size = 0
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(block)
                size += len(block)
        artifacts.append({"path": path.relative_to(root).as_posix(), "bytes": size, "sha256": digest.hexdigest()})
    return artifacts


def _subprocess(command: list[str], *, env: dict[str, str], log_path: Path, timeout: int) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as log:
        process = subprocess.Popen(
            command, cwd=WORKSPACE, env=env, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=1,
        )

        def copy_sanitized_output() -> None:
            assert process.stdout is not None
            for line in process.stdout:
                clean = _safe(line.rstrip("\n"))
                log.write(clean + "\n")
                log.flush()
                print(clean, flush=True)

        reader = Thread(target=copy_sanitized_output, name="pipeline-log-reader", daemon=True)
        reader.start()
        try:
            code = process.wait(timeout=max(1, timeout))
        except subprocess.TimeoutExpired as exc:
            process.kill()
            process.wait()
            reader.join(timeout=5)
            raise TaskError(f"{Path(command[0]).name} exceeded its {timeout}-second timeout") from exc
        except Exception:
            process.kill()
            process.wait()
            reader.join(timeout=5)
            raise
        else:
            reader.join(timeout=5)
            if reader.is_alive():
                raise TaskError("subprocess output reader did not close after process exit")
    if code != 0:
        raise TaskError(f"{Path(command[0]).name} exited with status {code}; sanitized output: {log_path}")


def _runtime_values() -> dict[str, str]:
    required = ("PIPELINE_RUN_ID", "PIPELINE_MODE", "PIPELINE_HDFS_ROOT", "PIPELINE_HDFS_URI", "PIPELINE_TOPIC")
    values = {key: os.environ.get(key, "") for key in required}
    if not re.fullmatch(r"[0-9a-f]{12}", values["PIPELINE_RUN_ID"]):
        raise TaskError("PIPELINE_RUN_ID must be the 12-character rendered run identity")
    if values["PIPELINE_MODE"] not in {"fixture", "full"}:
        raise TaskError("PIPELINE_MODE must be fixture or full")
    run_id = values["PIPELINE_RUN_ID"]
    if values["PIPELINE_HDFS_ROOT"] != f"/instacart/{run_id}":
        raise TaskError("pipeline HDFS root must exactly match this run identity")
    if values["PIPELINE_HDFS_URI"] != f"hdfs://namenode:8020/instacart/{run_id}":
        raise TaskError("pipeline HDFS URI must target the isolated run-scoped NameNode path")
    if values["PIPELINE_TOPIC"] != f"instacart-purchase-events-{run_id}":
        raise TaskError("pipeline Kafka topic must exactly match this run identity")
    if os.environ.get("PIPELINE_KAFKA_BOOTSTRAP") != "kafka:29092":
        raise TaskError("pipeline Kafka bootstrap must use the isolated in-profile service")
    return values


def input_stage() -> dict:
    values = _runtime_values()
    fixture_manifest = None
    if values["PIPELINE_MODE"] == "fixture":
        fixture_manifest = write_fixture(INPUT_ROOT)
    else:
        INPUT_ROOT.mkdir(parents=True, exist_ok=True)
        marker = INPUT_ROOT / ".upload-complete"
        deadline = time.monotonic() + 900
        while time.monotonic() < deadline and not marker.is_file():
            time.sleep(2)
        if not marker.is_file():
            raise TaskError("full-data input transfer did not complete within 900 seconds")
    input_manifest = inspect_input_directory(INPUT_ROOT)
    if values["PIPELINE_MODE"] == "full":
        validate_full_data_storage_budget(input_manifest["total_bytes"])
    expected_digest = os.environ.get("PIPELINE_SOURCE_RECEIPT_SHA256")
    if values["PIPELINE_MODE"] == "full" and input_manifest["manifest_sha256"] != expected_digest:
        raise TaskError("uploaded six-file input does not match the reviewed preflight source receipt")
    source_receipt = {
        "schema_version": 1,
        "run_id": values["PIPELINE_RUN_ID"],
        "scope": "synthetic_fixture" if values["PIPELINE_MODE"] == "fixture" else "full_data_input",
        "manifest_sha256": input_manifest["manifest_sha256"],
        "total_bytes": input_manifest["total_bytes"],
        "files": input_manifest["files"],
        "fixture": fixture_manifest,
    }
    _write_json(RUNTIME_ROOT / "evidence" / "source-receipt.json", source_receipt)
    receipt = {
        "schema_version": 1,
        "run_id": values["PIPELINE_RUN_ID"],
        "status": "completed",
        "source_manifest_sha256": input_manifest["manifest_sha256"],
        "source_receipt": str(RUNTIME_ROOT / "evidence" / "source-receipt.json"),
        "source_files": input_manifest["files"],
        "fixture_manifest": fixture_manifest,
    }
    _write_json(RUNTIME_ROOT / "evidence" / "input-stage-receipt.json", receipt)
    print(json.dumps(receipt, sort_keys=True), flush=True)
    return receipt


def module1() -> dict:
    values = _runtime_values()
    input_manifest = inspect_input_directory(INPUT_ROOT)
    records = input_manifest["files"]
    expected_digest = os.environ.get("PIPELINE_SOURCE_RECEIPT_SHA256")
    manifest_digest = input_manifest["manifest_sha256"]
    if values["PIPELINE_MODE"] == "full" and manifest_digest != expected_digest:
        raise TaskError("uploaded six-file input does not match the preflight source receipt")
    source_path = RUNTIME_ROOT / "evidence" / "source-receipt.json"
    try:
        source_receipt = json.loads(source_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise TaskError("Module 1 requires the completed input-stage receipt") from exc
    expected_scope = "synthetic_fixture" if values["PIPELINE_MODE"] == "fixture" else "full_data_input"
    if (
        source_receipt.get("run_id") != values["PIPELINE_RUN_ID"]
        or source_receipt.get("scope") != expected_scope
        or source_receipt.get("manifest_sha256") != manifest_digest
    ):
        raise TaskError("Module 1 source receipt does not match this run's six input files")

    work_root = RUNTIME_ROOT / "work" / os.environ.get("POD_NAME", "module1")
    clean_dir = work_root / "clean"
    feed_dir = work_root / "feed"
    if work_root.exists():
        raise TaskError("this Pod's Module 1 work path already exists; refusing to overwrite it")
    env = dict(os.environ)
    _subprocess(
        [sys.executable, "-m", "src.module1.clean", "--data-dir", str(work_root), "--raw-dir", str(INPUT_ROOT), "--output-dir", str(clean_dir), "--validate"],
        env=env, log_path=work_root / "clean.log", timeout=1800,
    )
    generate = [
        sys.executable, "-m", "src.module1.generate", "--data-dir", str(clean_dir),
        "--out-dir", str(feed_dir), "--scenario", "deterministic", "--scatter-weeks", "1", "--seed", "42",
    ]
    if values["PIPELINE_MODE"] == "fixture":
        generate += ["--limit-users", str(FIXTURE_MAX_USERS)]
    _subprocess(generate, env=env, log_path=work_root / "generate.log", timeout=1800)

    import pyarrow.parquet as pq

    event_count = pq.ParquetFile(feed_dir / "events.parquet").metadata.num_rows
    if values["PIPELINE_MODE"] == "fixture" and event_count > FIXTURE_MAX_EVENTS:
        raise TaskError("generated fixture event count exceeds the configured hard bound")
    _subprocess(
        [
            sys.executable, "-m", "src.module1.stage_hdfs", "--feed", str(feed_dir),
            "--webhdfs", os.environ.get("PIPELINE_WEBHDFS_URL", ""),
            "--hdfs-root", values["PIPELINE_HDFS_ROOT"], "--raw-dir", str(INPUT_ROOT),
            "--clean-dir", str(clean_dir), "--no-rewrite-datanode-to-localhost",
        ],
        env=env, log_path=work_root / "stage.log", timeout=3600,
    )
    receipt = {
        "schema_version": 1, "run_id": values["PIPELINE_RUN_ID"], "mode": values["PIPELINE_MODE"],
        "status": "completed", "source_manifest_sha256": manifest_digest,
        "source_receipt": str(source_path), "generated_events": event_count,
        "hdfs_root": values["PIPELINE_HDFS_ROOT"], "source_files": records,
        "fixture_manifest": source_receipt.get("fixture"),
        "synthetic_event_time_provenance": "Synthetic event time for replay simulation",
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    output = RUNTIME_ROOT / "evidence" / f"module1-{os.environ.get('POD_NAME', 'module1')}.json"
    _write_json(output, receipt)
    return receipt


def _result_root(module: str) -> Path:
    return RUNTIME_ROOT / "results" / os.environ.get("POD_NAME", "pipeline") / module


def _verify_api_egress_is_blocked() -> dict[str, Any]:
    host, port = "module4d-api", 8000
    try:
        socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except OSError as exc:
        raise TaskError("pipeline-role API deny-path probe could not resolve the in-profile API") from exc
    try:
        with socket.create_connection((host, port), timeout=3):
            raise TaskError("pipeline-role workload unexpectedly reached the API service")
    except OSError:
        return {"target": f"{host}:{port}", "connection": "blocked"}


def module2() -> dict:
    values = _runtime_values()
    uri = os.environ.get("MONGODB_URI")
    if not uri:
        raise TaskError("Module 2 requires the dedicated Mongo publisher Secret")
    env = dict(os.environ, MODULE2_ROOT=values["PIPELINE_HDFS_URI"], SPARK_MASTER="local[2]", MONGODB_URI=uri)
    _subprocess(
        [sys.executable, "-m", "src.module2.cli", "all", "--root", values["PIPELINE_HDFS_URI"],
         "--master", "local[2]", "--output", str(_result_root("module2")),
         "--run-id", f"k8s-{values['PIPELINE_RUN_ID']}", "--min-support", "1"],
        env=env, log_path=_result_root("module2") / "orchestrator.log", timeout=3600,
    )
    output = _result_root("module2")
    experiment_path = output / f"k8s-{values['PIPELINE_RUN_ID']}" / "summary.json"
    try:
        experiment_summary = json.loads(experiment_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise TaskError("Module 2 join, partition, and cache performance receipt is missing") from exc
    experiment_fields = {"join_benchmark", "partition_pruning", "cache_benchmark"}
    if experiment_summary.get("status") != "passed" or not experiment_fields.issubset(experiment_summary):
        raise TaskError("Module 2 did not complete all required join, partition, and cache experiments")
    experiment_receipt = {
        "status": "passed",
        "summary_path": str(experiment_path),
        "summary_sha256": hashlib.sha256(experiment_path.read_bytes()).hexdigest(),
        "experiments": sorted(experiment_fields),
        "source_sha256": experiment_summary.get("validation", {}).get("source_receipt", {}).get("source_sha256"),
    }
    from src.module2.executor_benchmark import build_plan, execute_plan

    benchmark_plan = build_plan(
        root=values["PIPELINE_HDFS_URI"],
        output=output.parent,
        run_id=f"executor-k8s-{values['PIPELINE_RUN_ID']}",
        repetitions=3,
        warmups=1,
        shuffle_partitions=8,
        timeout_seconds=1200,
    )
    benchmark_result = execute_plan(benchmark_plan)
    benchmark_output = Path(benchmark_plan["output_directory"])
    denied_api = _verify_api_egress_is_blocked()
    return {
        "status": "completed", "run_id": values["PIPELINE_RUN_ID"],
        "module2_output": str(output), "artifacts": _artifact_inventory(output),
        "module2_experiments": experiment_receipt,
        "network_policy_denied_api_path": denied_api,
        "executor_benchmark": benchmark_result,
        "executor_benchmark_artifacts": _artifact_inventory(benchmark_output),
    }


def module4() -> dict:
    values = _runtime_values()
    uri = os.environ.get("MONGODB_URI")
    if not uri:
        raise TaskError("Module 4 requires the dedicated Mongo publisher Secret")
    output = _result_root("module4")
    source_receipt = RUNTIME_ROOT / "evidence" / "source-receipt.json"
    args = [
        sys.executable, "-m", "src.module4.cli", "all", "--root", values["PIPELINE_HDFS_URI"],
        "--master", "local[2]", "--output", str(output), "--run-id", f"k8s-{values['PIPELINE_RUN_ID']}",
        "--model-version", f"als-k8s-{values['PIPELINE_RUN_ID']}",
        "--graph-version", f"graph-k8s-{values['PIPELINE_RUN_ID']}",
        "--graph-backend", "graphframes", "--graph-min-edge-weight", "2",
        "--scope", "staged_fixture" if values["PIPELINE_MODE"] == "fixture" else "full_data",
    ]
    if values["PIPELINE_MODE"] == "fixture":
        args += ["--pilot-users", str(FIXTURE_MAX_USERS), "--max-trials", "3"]
    else:
        args += ["--pilot-users", "5000", "--max-trials", "12", "--source-receipt", str(source_receipt)]
    database = os.environ.get("MODULE4D_MONGO_DATABASE", "")
    if database != f"module4d_demo_{values['PIPELINE_RUN_ID']}":
        raise TaskError("Module 4 requires the exact run-scoped Mongo database")
    env = dict(
        os.environ,
        MODULE4_ROOT=values["PIPELINE_HDFS_URI"],
        SPARK_MASTER="local[2]",
        MONGODB_URI=uri,
        MODULE4_MONGO_DATABASE=database,
    )
    _subprocess(args, env=env, log_path=output / "orchestrator.log", timeout=7200)
    return {
        "status": "completed", "run_id": values["PIPELINE_RUN_ID"],
        "module4_output": str(output), "artifacts": _artifact_inventory(output),
    }


def replay() -> dict:
    values = _runtime_values()
    from src.module3.config import Config as StreamConfig
    from src.module3.spark import session
    from src.module3.stream import run_stream
    from src.module3.validate import validate_kafka, validate_mongo

    uri = os.environ.get("MONGODB_URI")
    if not uri:
        raise TaskError("Module 3 requires the dedicated Mongo publisher Secret")
    output = _result_root("module3") / f"k8s-{values['PIPELINE_RUN_ID']}"
    output.mkdir(parents=True, exist_ok=False)
    output.chmod(0o700)
    event_feed = RUNTIME_ROOT / "work" / "module1" / "feed"
    candidates = sorted((RUNTIME_ROOT / "work").glob("*/feed/events.parquet"))
    if len(candidates) != 1:
        raise TaskError("Module 3 requires exactly one validated run-scoped Module 1 event feed")
    event_feed = candidates[0].parent
    config = StreamConfig(
        root=values["PIPELINE_HDFS_URI"], master="local[2]",
        bootstrap_servers=os.environ.get("PIPELINE_KAFKA_BOOTSTRAP", ""),
        topic=values["PIPELINE_TOPIC"], starting_offsets="earliest", fail_on_data_loss=True,
        mongodb_uri=uri, mongo_database=os.environ.get("MODULE4D_MONGO_DATABASE", ""),
        mongo_top_k_per_window=20, publish_trend_bundles=True,
        trend_source_namespace=os.environ.get("PIPELINE_TREND_NAMESPACE", TREND_SOURCE_NAMESPACE),
        trend_source_scope="synthetic_fixture" if values["PIPELINE_MODE"] == "fixture" else "full_data_input",
    )
    _ensure_topic(config)
    kafka_health = validate_kafka(config)
    mongo_health = validate_mongo(config)
    if kafka_health.get("ok") is not True:
        raise TaskError("run-scoped Kafka topic readiness check failed")
    if mongo_health.get("ok") is not True:
        raise TaskError("Mongo publisher readiness check failed")
    producer_receipt = output / "producer-receipt.json"
    producer_log = output / "producer.log"
    producer_command = [
        sys.executable, "-m", "src.module1.producer", "--feed", str(event_feed),
        "--bootstrap", config.bootstrap_servers, "--topic", config.topic,
        "--target-events-per-second", "100" if values["PIPELINE_MODE"] == "fixture" else "1000",
        "--receipt", str(producer_receipt), "--input-order", "auto",
    ]
    producer_env = dict(os.environ)
    with producer_log.open("w", encoding="utf-8") as handle:
        producer = None
        spark = None
        try:
            spark = session(config, output / "eventlog")

            def start_replay(_query):
                nonlocal producer
                producer = subprocess.Popen(
                    producer_command, cwd=WORKSPACE, env=producer_env,
                    stdout=handle, stderr=subprocess.STDOUT, text=True,
                )
                return {"producer_pid": producer.pid, "start": "after-Structured-Streaming-query-start"}

            duration = 300 if values["PIPELINE_MODE"] == "fixture" else 3600
            stream_receipt = run_stream(
                spark, config, output, f"k8s-{values['PIPELINE_RUN_ID']}",
                duration_seconds=duration, checkpoint_id=f"k8s-{values['PIPELINE_RUN_ID']}",
                checkpoint_root=Path("/run/checkpoints") / "module3", on_query_started=start_replay,
            )
            if stream_receipt.get("interrupted") is True:
                raise TaskError("Structured Streaming stopped before its bounded run duration completed")
            if producer is None:
                raise TaskError("event replay was not started after Spark query readiness")
            code = producer.wait(timeout=600)
            if code != 0 or not producer_receipt.is_file():
                raise TaskError("Kafka event producer failed; no successful delivery receipt was produced")
        except Exception:
            if producer is not None and producer.poll() is None:
                producer.terminate()
                try:
                    producer.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    producer.kill()
                    producer.wait()
            raise
        finally:
            if spark is not None:
                spark.stop()
    replay_values = json.loads(producer_receipt.read_text(encoding="utf-8"))
    if replay_values.get("failed_events") != 0 or replay_values.get("acked_events", 0) != replay_values.get("attempted_events"):
        raise TaskError("Kafka producer acknowledgement receipt does not reconcile all attempted events")
    receipt = {
        "status": "completed", "run_id": values["PIPELINE_RUN_ID"],
        "topic": config.topic, "checkpoint": stream_receipt.get("checkpoint_location"),
        "checkpoint_id": stream_receipt.get("checkpoint_id"),
        "producer_receipt": str(producer_receipt), "producer": replay_values,
        "last_progress": stream_receipt.get("last_progress"),
        "synthetic_event_time_provenance": "Synthetic event time for replay simulation",
    }
    _write_json(output / "replay-receipt.json", receipt)
    return receipt


def _ensure_topic(config) -> None:
    """Create the run-scoped one-partition topic idempotently with bounded retries."""
    from kafka import KafkaAdminClient
    from kafka.admin import NewTopic
    from kafka.errors import TopicAlreadyExistsError

    last_error = None
    for attempt in range(12):
        admin = None
        try:
            admin = KafkaAdminClient(bootstrap_servers=config.bootstrap_servers, request_timeout_ms=5000)
            try:
                admin.create_topics([NewTopic(config.topic, num_partitions=1, replication_factor=1)])
            except TopicAlreadyExistsError:
                pass
            topics = admin.describe_topics([config.topic])
            record = next((item for item in topics if item.get("name", item.get("topic")) == config.topic), None)
            if record is None or len(record.get("partitions", [])) != 1:
                raise TaskError("run-scoped Kafka topic does not have exactly one partition")
            return
        except TaskError:
            raise
        except Exception as exc:
            last_error = type(exc).__name__
            time.sleep(min(2 + attempt, 10))
        finally:
            if admin is not None:
                admin.close()
    raise TaskError(f"Kafka broker did not become ready ({last_error or 'unknown failure'})")


def hybrid() -> dict:
    values = _runtime_values()
    user_id = os.environ.get("PIPELINE_DEMO_USER_ID", "1")
    if not re.fullmatch(r"[1-9][0-9]*", user_id):
        raise TaskError("PIPELINE_DEMO_USER_ID must be a positive integer")
    as_of = os.environ.get("PIPELINE_AS_OF_EVENT_TIME", "")
    try:
        parsed_as_of = datetime.fromisoformat(as_of.replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise TaskError("PIPELINE_AS_OF_EVENT_TIME must be the stable UTC time in the reviewed run plan") from exc
    if parsed_as_of.tzinfo is None or parsed_as_of.utcoffset() != timezone.utc.utcoffset(parsed_as_of):
        raise TaskError("PIPELINE_AS_OF_EVENT_TIME must use UTC")
    from src.module4.hybrid.cli import main as hybrid_main

    output_path = _result_root("hybrid") / "hybrid-receipt.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    uri = os.environ.get("MONGODB_URI")
    if not uri:
        raise TaskError("Hybrid publication requires the dedicated Mongo publisher Secret")
    # The CLI emits only a sanitized success document or a redacted exception.
    from contextlib import redirect_stdout
    import io

    capture = io.StringIO()
    with redirect_stdout(capture):
        code = hybrid_main([
            "serve-user", "--user-id", user_id,
            "--source-namespace", os.environ.get("PIPELINE_TREND_NAMESPACE", TREND_SOURCE_NAMESPACE),
            "--as-of-event-time", as_of, "--mongodb-uri", uri,
            "--mongo-database", os.environ.get("MODULE4D_MONGO_DATABASE", ""),
        ])
    if code != 0:
        raise TaskError("Hybrid publication did not complete; see its sanitized CLI diagnostic")
    try:
        response = json.loads(capture.getvalue())
    except json.JSONDecodeError as exc:
        raise TaskError("Hybrid CLI returned an invalid receipt") from exc
    receipt = {"run_id": values["PIPELINE_RUN_ID"], "as_of_event_time": as_of, **response}
    _write_json(output_path, receipt)
    print(json.dumps(receipt, sort_keys=True), flush=True)
    return receipt


STEPS = {"input": input_stage, "module1": module1, "module2": module2, "module4": module4, "replay": replay, "hybrid": hybrid}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("step", choices=tuple(STEPS))
    arguments = parser.parse_args(argv)
    try:
        result = STEPS[arguments.step]()
        step_receipt = {"schema_version": 1, "step": arguments.step, "status": "completed", "receipt": result}
        _write_json(RUNTIME_ROOT / "evidence" / "steps" / f"{arguments.step}.json", step_receipt)
        print(json.dumps(step_receipt, default=str, sort_keys=True))
        return 0
    except TaskError as exc:
        print(f"pipeline step failed: {_safe(str(exc))}", file=sys.stderr, flush=True)
        return 2
    except Exception as exc:
        print(f"pipeline step failed ({type(exc).__name__}); sensitive details redacted", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
