"""Bounded Module 3 checks: a live producer run and a checkpoint recovery.

Both scenarios drive the real producer, the real Kafka topic and the
real streaming query. `live` proves one bounded run publishes finalized
windows; `recovery` proves a second process resumes the same checkpoint
and reads only new records.
"""

from __future__ import annotations

from __future__ import annotations
import os
from pathlib import Path
import re
import shlex
import subprocess
import argparse
from datetime import datetime, timezone
import json
import math

ROOT = Path(__file__).resolve().parents[1]


def run_command(command: list[str], capture: bool = False) -> subprocess.CompletedProcess:
    """Run a repository command and optionally echo its output."""
    result = subprocess.run(
        command,
        cwd=ROOT,
        check=True,
        text=True,
        capture_output=capture,
    )
    if capture:
        print(result.stdout, end="", flush=True)
        if result.stderr:
            print(result.stderr, end="", flush=True)
    return result


def compose_python(arguments: list[str], capture: bool = False) -> subprocess.CompletedProcess:
    """Run a Python entry point inside the Module 3 Compose service."""
    return run_command(
        [
            "docker",
            "compose",
            "--profile",
            "tools",
            "run",
            "--rm",
            "--no-deps",
            "--entrypoint",
            "python3",
            "module3",
            *arguments,
        ],
        capture=capture,
    )


def artifact_path(output: str) -> Path:
    """Return the repository path a run reported in its artifact line."""
    match = re.search(r"\[ARTIFACTS\]\s+(/workspace/\S+)", output)
    if not match:
        raise RuntimeError(f"Could not locate artifact path in output:\n{output}")
    return ROOT / Path(match.group(1)).relative_to("/workspace")


def ensure_output(path: Path) -> None:
    """Create an output directory the container is also able to write to."""
    try:
        path.mkdir(parents=True, exist_ok=True)
        return
    except PermissionError:
        pass
    container_path = Path("/workspace") / path.relative_to(ROOT)
    command = (
        f"mkdir -p {shlex.quote(str(container_path))} && "
        f"chown {os.getuid()}:{os.getgid()} {shlex.quote(str(container_path))}"
    )
    run_command(
        [
            "docker", "compose", "--profile", "tools", "run", "--rm", "--no-deps",
            "--entrypoint", "sh", "module3", "-c", command,
        ]
    )


def archive_checkpoint(checkpoint: Path, archived: Path) -> None:
    """Move a checkpoint aside so a new run can create its own."""
    try:
        checkpoint.rename(archived)
        return
    except PermissionError:
        pass
    checkpoint_container = Path("/workspace") / checkpoint.relative_to(ROOT)
    archived_container = Path("/workspace") / archived.relative_to(ROOT)
    command = (
        f"mv {shlex.quote(str(checkpoint_container))} {shlex.quote(str(archived_container))} && "
        f"chown -R {os.getuid()}:{os.getgid()} {shlex.quote(str(archived_container.parent))}"
    )
    run_command(
        [
            "docker", "compose", "--profile", "tools", "run", "--rm", "--no-deps",
            "--entrypoint", "sh", "module3", "-c", command,
        ]
    )


ROOT = Path(__file__).resolve().parents[1]







def run_live(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=int, default=50_000)
    parser.add_argument("--duration-seconds", type=int, default=120)
    parser.add_argument("--feed", type=Path, default=Path("data/synthesized/module3_dev"))
    parser.add_argument("--output", type=Path, default=Path("results/module3/smoke"))
    parser.add_argument("--top-k", type=int, default=20)
    args = parser.parse_args(argv)
    if args.events < 1 or args.duration_seconds < 20:
        raise ValueError("Smoke requires positive events and at least 20 seconds")

    topic = "instacart-module3-live-smoke"
    checkpoint_id = "live-smoke"
    output = (ROOT / args.output).resolve()
    ensure_output(output)
    checkpoint = output / "checkpoint" / checkpoint_id
    archived_checkpoint = None
    if checkpoint.exists():
        suffix = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        archived_checkpoint = checkpoint.with_name(f"{checkpoint_id}.previous-{suffix}")
        archive_checkpoint(checkpoint, archived_checkpoint)

    run_command(
        [
            "docker",
            "compose",
            "up",
            "-d",
            "--wait",
            "spark-master",
            "spark-worker",
            "kafka",
            "mongodb",
        ]
    )

    create_code = (
        "import json; "
        "from src.module3.kafka_utils import create_topic; "
        f"print(json.dumps(create_topic('kafka:29092', '{topic}', 4, reset=True)))"
    )
    created = compose_python(["-c", create_code], capture=True)
    topic_before = json.loads(created.stdout.strip().splitlines()[-1])

    producer_receipt = args.output / "producer_receipt.json"
    compose_python(
        [
            "-m",
            "src.module1.producer",
            "--feed",
            str(args.feed),
            "--bootstrap",
            "kafka:29092",
            "--topic",
            topic,
            "--limit-events",
            str(args.events),
            "--replay-speed",
            "0",
            "--receipt",
            str(producer_receipt),
        ]
    )
    receipt = json.loads((ROOT / producer_receipt).read_text(encoding="utf-8"))

    metadata_code = (
        "import json; "
        "from src.module3.kafka_utils import topic_metadata; "
        f"print(json.dumps(topic_metadata('kafka:29092', '{topic}')))"
    )
    metadata_result = compose_python(["-c", metadata_code], capture=True)
    topic_after = json.loads(metadata_result.stdout.strip().splitlines()[-1])
    kafka_delta = topic_after["total_records"] - topic_before["total_records"]
    if receipt["failed_events"] != 0 or kafka_delta != receipt["acked_events"]:
        raise RuntimeError(
            f"Kafka delta {kafka_delta} != producer acked {receipt['acked_events']}: {receipt}"
        )

    stream_started = datetime.now(timezone.utc)
    stream_result = run_command(
        [
            "python3",
            "scripts/module3.py",
            "run",
            "--topic",
            topic,
            "--checkpoint-id",
            checkpoint_id,
            "--duration-seconds",
            str(args.duration_seconds),
            "--output",
            str(args.output),
            "--mongo-top-k-per-window",
            str(args.top_k),
        ],
        capture=True,
    )
    artifact = artifact_path(stream_result.stdout)
    summary = json.loads((artifact / "summary.json").read_text(encoding="utf-8"))
    progress = [
        json.loads(line)
        for line in (artifact / "streaming_progress.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    total_input = sum(int(row.get("numInputRows") or 0) for row in progress)
    non_empty = [row for row in progress if (row.get("numInputRows") or 0) > 0]
    if total_input != receipt["acked_events"] or not non_empty:
        raise RuntimeError(
            f"Spark input mismatch: expected={receipt['acked_events']} observed={total_input}"
        )
    sink_batches = [
        json.loads(line)
        for line in (artifact / "sink_batches.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    latency_measurements = [
        batch.get("ingestion_to_output_latency")
        for batch in sink_batches
        if isinstance(batch.get("ingestion_to_output_latency"), dict)
        and batch["ingestion_to_output_latency"].get("status") == "measured"
    ]
    if not latency_measurements:
        raise RuntimeError("Module 3 emitted no measured ingestion-to-output latency sample")
    for measurement in latency_measurements:
        latencies = measurement.get("latency_ms", {})
        if measurement.get("sample_count", 0) < 1 or any(
            not isinstance(latencies.get(key), (int, float))
            or not math.isfinite(latencies[key])
            for key in ("minimum", "mean", "maximum")
        ):
            raise RuntimeError(f"Module 3 latency evidence is incomplete: {measurement}")

    mongo_code = f"""
import json
from datetime import datetime
from pymongo import MongoClient
started = datetime.fromisoformat('{stream_started.isoformat()}').replace(tzinfo=None)
required = {json.dumps(['window_start', 'window_end', 'product_id', 'purchase_count_30m', 'purchase_count_120m', 'trend_score', 'trend_rank', 'updated_at'])}
with MongoClient('mongodb://mongodb:27017/instacart') as client:
    col = client.instacart.realtime_trending
    query = {{'updated_at': {{'$gte': started}}}}
    sample = col.find_one(query)
    duplicates = list(col.aggregate([
        {{'$match': query}},
        {{'$group': {{'_id': {{'window_end': '$window_end', 'product_id': '$product_id'}}, 'n': {{'$sum': 1}}}}}},
        {{'$match': {{'n': {{'$gt': 1}}}}}},
        {{'$limit': 1}},
    ]))
    rank_duplicates = list(col.aggregate([
        {{'$match': query}},
        {{'$group': {{'_id': {{'window_end': '$window_end', 'trend_rank': '$trend_rank'}}, 'n': {{'$sum': 1}}}}}},
        {{'$match': {{'n': {{'$gt': 1}}}}}},
        {{'$limit': 1}},
    ]))
    invalid_scores = col.count_documents({{'$and': [query, {{'$or': [{{'trend_score': {{'$lt': 0}}}}, {{'trend_score': {{'$gt': 1}}}}]}}]}})
    over_top_k = list(col.aggregate([
        {{'$match': query}},
        {{'$group': {{'_id': '$window_end', 'n': {{'$sum': 1}}}}}},
        {{'$match': {{'n': {{'$gt': {args.top_k}}}}}}},
        {{'$limit': 1}},
    ]))
    result = {{
        'documents_written': col.count_documents(query),
        'sample': sample,
        'missing_fields': sorted(set(required) - set(sample or {{}})),
        'duplicate_window_products': duplicates,
        'duplicate_window_ranks': rank_duplicates,
        'invalid_scores': invalid_scores,
        'windows_over_top_k': over_top_k,
    }}
    print(json.dumps(result, default=str))
"""
    mongo_result = compose_python(["-c", mongo_code], capture=True)
    mongo = json.loads(mongo_result.stdout.strip().splitlines()[-1])
    if (
        mongo["documents_written"] <= 0
        or mongo["missing_fields"]
        or mongo["duplicate_window_products"]
        or mongo["duplicate_window_ranks"]
        or mongo["invalid_scores"]
        or mongo["windows_over_top_k"]
    ):
        raise RuntimeError(f"Mongo smoke verification failed: {mongo}")

    evidence = {
        "topic_before": topic_before,
        "topic_after": topic_after,
        "kafka_records_added": kafka_delta,
        "producer": receipt,
        "spark": {
            "artifact": str(artifact),
            "query_id": summary["stream"]["query_id"],
            "total_input_rows": total_input,
            "non_empty_batches": non_empty,
            "ingestion_to_output_latency": latency_measurements,
        },
        "mongo": mongo,
        "checkpoint_id": checkpoint_id,
        "archived_previous_checkpoint": str(archived_checkpoint) if archived_checkpoint else None,
    }
    evidence_path = output / "smoke_evidence.json"
    evidence_path.write_text(
        json.dumps(evidence, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    print(f"[SMOKE EVIDENCE] {evidence_path}", flush=True)
    return 0


ROOT = Path(__file__).resolve().parents[1]





def _progress(path: Path) -> list[dict]:
    with path.open(encoding="utf-8") as stream:
        return [json.loads(line) for line in stream if line.strip()]


def _non_empty_total(rows: list[dict]) -> int:
    return sum(int(row.get("numInputRows") or 0) for row in rows)




def run_recovery(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration-seconds", type=int, default=25)
    parser.add_argument("--output", type=Path, default=Path("results/module3/recovery"))
    args = parser.parse_args(argv)
    if args.duration_seconds < 15:
        raise ValueError("Recovery runs require at least 15 seconds")

    topic = "instacart-module3-recovery-smoke"
    checkpoint_id = "recovery-smoke"
    output = (ROOT / args.output).resolve()
    ensure_output(output)
    checkpoint = output / "checkpoint" / checkpoint_id
    archived_checkpoint = None
    if checkpoint.exists():
        suffix = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        archived_checkpoint = checkpoint.with_name(f"{checkpoint_id}.previous-{suffix}")
        archive_checkpoint(checkpoint, archived_checkpoint)

    run_command(
        [
            "docker",
            "compose",
            "up",
            "-d",
            "--wait",
            "spark-master",
            "spark-worker",
            "kafka",
            "mongodb",
        ]
    )

    first_send = compose_python(
        [
            "-m",
            "src.module3.demo",
            "recovery-first",
            "--topic",
            topic,
            "--reset-topic",
        ],
        capture=True,
    )
    first_run = run_command(
        [
            "python3",
            "scripts/module3.py",
            "run",
            "--topic",
            topic,
            "--checkpoint-id",
            checkpoint_id,
            "--trigger-interval",
            "3 seconds",
            "--duration-seconds",
            str(args.duration_seconds),
            "--output",
            str(args.output),
        ],
        capture=True,
    )
    first_artifact = artifact_path(first_run.stdout)

    second_send = compose_python(
        ["-m", "src.module3.demo", "recovery-second", "--topic", topic],
        capture=True,
    )
    second_run = run_command(
        [
            "python3",
            "scripts/module3.py",
            "run",
            "--topic",
            topic,
            "--checkpoint-id",
            checkpoint_id,
            "--trigger-interval",
            "3 seconds",
            "--duration-seconds",
            str(args.duration_seconds),
            "--output",
            str(args.output),
        ],
        capture=True,
    )
    second_artifact = artifact_path(second_run.stdout)

    first_summary = json.loads((first_artifact / "summary.json").read_text(encoding="utf-8"))
    second_summary = json.loads((second_artifact / "summary.json").read_text(encoding="utf-8"))
    first_progress = _progress(first_artifact / "streaming_progress.jsonl")
    second_progress = _progress(second_artifact / "streaming_progress.jsonl")

    first_input = _non_empty_total(first_progress)
    second_input = _non_empty_total(second_progress)
    first_batches = [int(row["batchId"]) for row in first_progress]
    second_batches = [int(row["batchId"]) for row in second_progress]
    first_query_id = first_summary["stream"]["query_id"]
    second_query_id = second_summary["stream"]["query_id"]

    if first_input != 3 or second_input != 2:
        raise RuntimeError(f"Unexpected recovery inputs: first={first_input}, second={second_input}")
    if first_query_id != second_query_id:
        raise RuntimeError(f"Checkpoint query id changed: {first_query_id} != {second_query_id}")
    if min(second_batches) <= min(first_batches):
        raise RuntimeError(f"Batch numbering did not resume: {first_batches} -> {second_batches}")

    mongo_code = """
import json
from pymongo import MongoClient
with MongoClient('mongodb://mongodb:27017/instacart') as client:
    col = client.instacart.realtime_trending
    duplicates = list(col.aggregate([
        {'$match': {'product_id': {'$in': [5101, 5102, 5199]}}},
        {'$group': {'_id': {'window_end': '$window_end', 'product_id': '$product_id'}, 'n': {'$sum': 1}}},
        {'$match': {'n': {'$gt': 1}}},
    ]))
    print(json.dumps({'documents': col.count_documents({'product_id': {'$in': [5101, 5102, 5199]}}),
                      'duplicate_groups': duplicates}, default=str))
"""
    mongo_result = compose_python(["-c", mongo_code], capture=True)
    mongo = json.loads(mongo_result.stdout.strip().splitlines()[-1])
    if mongo["documents"] <= 0 or mongo["duplicate_groups"]:
        raise RuntimeError(f"Mongo recovery verification failed: {mongo}")

    evidence = {
        "checkpoint_id": checkpoint_id,
        "checkpoint_path": str(checkpoint),
        "archived_previous_checkpoint": str(archived_checkpoint) if archived_checkpoint else None,
        "topic": topic,
        "first_send": json.loads(first_send.stdout.strip().splitlines()[-1]),
        "second_send": json.loads(second_send.stdout.strip().splitlines()[-1]),
        "first_run": {
            "artifact": str(first_artifact),
            "query_id": first_query_id,
            "batch_ids": first_batches,
            "input_rows": first_input,
        },
        "second_run": {
            "artifact": str(second_artifact),
            "query_id": second_query_id,
            "batch_ids": second_batches,
            "input_rows": second_input,
        },
        "mongo": mongo,
        "resumed": True,
        "replayed_from_earliest": False,
    }
    evidence_path = output / "recovery_evidence.json"
    evidence_path.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"[RECOVERY EVIDENCE] {evidence_path}", flush=True)
    return 0


def main(argv=None) -> int:
    """Run one Module 3 smoke scenario."""
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="scenario", required=True)
    live = commands.add_parser("live", help="run the bounded producer to MongoDB smoke")
    live.add_argument("--events", type=int, default=50_000)
    live.add_argument("--duration-seconds", type=int, default=120)
    live.add_argument("--feed", type=Path, default=Path("data/synthesized/module3_dev"))
    live.add_argument("--output", type=Path, default=Path("results/module3/smoke"))
    live.add_argument("--top-k", type=int, default=20)
    recovery = commands.add_parser("recovery", help="run the checkpoint recovery smoke")
    recovery.add_argument("--duration-seconds", type=int, default=25)
    recovery.add_argument("--output", type=Path, default=Path("results/module3/recovery"))
    arguments = parser.parse_args(argv)
    if arguments.scenario == "live":
        return run_live(arguments)
    return run_recovery(arguments)


if __name__ == "__main__":
    raise SystemExit(main())
