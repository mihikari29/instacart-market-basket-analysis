"""Run a bounded real producer -> Kafka -> Spark -> Mongo Module 3 smoke test."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import shlex
import subprocess


ROOT = Path(__file__).resolve().parents[1]


def _run(command: list[str], capture: bool = False) -> subprocess.CompletedProcess:
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


def _compose_python(arguments: list[str], capture: bool = False) -> subprocess.CompletedProcess:
    return _run(
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


def _artifact_path(output: str) -> Path:
    match = re.search(r"\[ARTIFACTS\]\s+(/workspace/\S+)", output)
    if not match:
        raise RuntimeError(f"Could not locate artifact path in output:\n{output}")
    return ROOT / Path(match.group(1)).relative_to("/workspace")


def _ensure_output(path: Path) -> None:
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
    _run(
        [
            "docker",
            "compose",
            "--profile",
            "tools",
            "run",
            "--rm",
            "--no-deps",
            "--entrypoint",
            "sh",
            "module3",
            "-c",
            command,
        ]
    )


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=int, default=50_000)
    parser.add_argument("--duration-seconds", type=int, default=120)
    parser.add_argument("--feed", type=Path, default=Path("data/synthesized/module3_dev"))
    parser.add_argument("--output", type=Path, default=Path("results/module3/smoke"))
    args = parser.parse_args(argv)
    if args.events < 1 or args.duration_seconds < 20:
        raise ValueError("Smoke requires positive events and at least 20 seconds")

    topic = "instacart-module3-live-smoke"
    checkpoint_id = "live-smoke"
    output = (ROOT / args.output).resolve()
    _ensure_output(output)
    checkpoint = output / "checkpoint" / checkpoint_id
    archived_checkpoint = None
    if checkpoint.exists():
        suffix = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        archived_checkpoint = checkpoint.with_name(f"{checkpoint_id}.previous-{suffix}")
        checkpoint.rename(archived_checkpoint)

    _run(
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
    created = _compose_python(["-c", create_code], capture=True)
    topic_before = json.loads(created.stdout.strip().splitlines()[-1])

    producer_receipt = args.output / "producer_receipt.json"
    _compose_python(
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
    metadata_result = _compose_python(["-c", metadata_code], capture=True)
    topic_after = json.loads(metadata_result.stdout.strip().splitlines()[-1])
    kafka_delta = topic_after["total_records"] - topic_before["total_records"]
    if kafka_delta != receipt["sent_events"]:
        raise RuntimeError(f"Kafka delta {kafka_delta} != producer sent {receipt['sent_events']}")

    stream_started = datetime.now(timezone.utc)
    stream_result = _run(
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
        ],
        capture=True,
    )
    artifact = _artifact_path(stream_result.stdout)
    summary = json.loads((artifact / "summary.json").read_text(encoding="utf-8"))
    progress = [
        json.loads(line)
        for line in (artifact / "streaming_progress.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    total_input = sum(int(row.get("numInputRows") or 0) for row in progress)
    non_empty = [row for row in progress if (row.get("numInputRows") or 0) > 0]
    if total_input != receipt["sent_events"] or not non_empty:
        raise RuntimeError(
            f"Spark input mismatch: expected={receipt['sent_events']} observed={total_input}"
        )

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
    result = {{
        'documents_written': col.count_documents(query),
        'sample': sample,
        'missing_fields': sorted(set(required) - set(sample or {{}})),
        'duplicate_window_products': duplicates,
        'duplicate_window_ranks': rank_duplicates,
        'invalid_scores': invalid_scores,
    }}
    print(json.dumps(result, default=str))
"""
    mongo_result = _compose_python(["-c", mongo_code], capture=True)
    mongo = json.loads(mongo_result.stdout.strip().splitlines()[-1])
    if (
        mongo["documents_written"] <= 0
        or mongo["missing_fields"]
        or mongo["duplicate_window_products"]
        or mongo["duplicate_window_ranks"]
        or mongo["invalid_scores"]
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


if __name__ == "__main__":
    raise SystemExit(main())
