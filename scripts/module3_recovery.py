"""Run the deterministic two-process Module 3 checkpoint recovery smoke test."""

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


def _progress(path: Path) -> list[dict]:
    with path.open(encoding="utf-8") as stream:
        return [json.loads(line) for line in stream if line.strip()]


def _non_empty_total(rows: list[dict]) -> int:
    return sum(int(row.get("numInputRows") or 0) for row in rows)


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
    parser.add_argument("--duration-seconds", type=int, default=25)
    parser.add_argument("--output", type=Path, default=Path("results/module3/recovery"))
    args = parser.parse_args(argv)
    if args.duration_seconds < 15:
        raise ValueError("Recovery runs require at least 15 seconds")

    topic = "instacart-module3-recovery-smoke"
    checkpoint_id = "recovery-smoke"
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

    first_send = _compose_python(
        [
            "-m",
            "src.module3.scenario",
            "recovery-first",
            "--topic",
            topic,
            "--reset-topic",
        ],
        capture=True,
    )
    first_run = _run(
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
    first_artifact = _artifact_path(first_run.stdout)

    second_send = _compose_python(
        ["-m", "src.module3.scenario", "recovery-second", "--topic", topic],
        capture=True,
    )
    second_run = _run(
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
    second_artifact = _artifact_path(second_run.stdout)

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
    mongo_result = _compose_python(["-c", mongo_code], capture=True)
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


if __name__ == "__main__":
    raise SystemExit(main())
