"""Read back a full-data A/B publication and compare same-version replay snapshots."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.module4.config import Config  # noqa: E402
from src.module4.evaluation import validate_prior_only_hybrid_evaluation  # noqa: E402
from src.module4.sink_mongo import snapshot_validation, publication_lock  # noqa: E402


def require(condition, message):
    if not condition:
        raise ValueError(message)


def validate_report(report):
    require(report.get("status") == "passed", "pipeline did not pass")
    require(report.get("scope") == "full_data", "full-data scope is required")
    require(report.get("acceptance_status") == "full_data_pair_validated", "A/B pair was not accepted")
    require(report.get("serving_status") == "published_and_validated", "serving was not validated")
    require(report.get("pair_readiness", {}).get("ready") is True, "A/B pair is not ready")
    require(report.get("source_stability_check") == "passed", "source stability was not checked")
    require(bool(report.get("input_source_receipt")), "verified full source receipt is missing")
    require(report["graph"]["validation"].get("backend") == "graphframes", "real GraphFrames is required")
    try:
        hybrid_evaluation = report["als"]["hybrid_evaluation"]
        validate_prior_only_hybrid_evaluation(
            hybrid_evaluation, report["validation"]["source_receipt"]
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"prior-only Hybrid acceptance gate failed: {exc}") from exc
    require(hybrid_evaluation["run_id"] == report["run_id"], "Hybrid evaluation run ID differs")
    require(
        hybrid_evaluation["model_version"] == report["environment"]["model_version"],
        "Hybrid evaluation model version differs",
    )
    require(
        hybrid_evaluation["source_code_sha256"] == report["environment"]["source_code_sha256"],
        "Hybrid evaluation source code identity differs",
    )


def snapshot_digest(documents):
    """Hash sorted logical rows; replay may update run IDs and timestamps.

    Compare scores/PageRank rounded to six decimal places; IDs, membership,
    ranks, degrees, versions and threshold remain exact.
    """
    digest = hashlib.sha256()
    count = 0
    for document in documents:
        logical = {key: value for key, value in document.items()
                   if key not in {"run_id", "generated_at", "computed_at"}}
        for key, value in logical.items():
            if isinstance(value, float):
                require(math.isfinite(value), f"non-finite snapshot field: {key}")
                logical[key] = round(value, 6)
        digest.update(json.dumps(logical, sort_keys=True, separators=(",", ":")).encode("utf-8"))
        digest.update(b"\n")
        count += 1
    return {"documents": count, "logical_sha256": digest.hexdigest(), "float_decimal_places": 6}


def compare_replay(current, previous):
    for field in ("source_code_sha256", "source_receipt_sha256", "config", "model_version", "graph_version"):
        require(current[field] == previous[field], f"replay changed {field}")
    require(current["run_id"] != previous["run_id"], "replay must use a new run ID")
    require(current["reused_existing_model"] is True, "replay did not load the existing model")
    require(current["snapshots"] == previous["snapshots"], "logical snapshots changed during replay")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--compare-to", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    report = json.loads((args.run_dir / "summary.json").read_text(encoding="utf-8"))
    validate_report(report)
    environment = report["environment"]
    config = Config.environment(**{
        key: value for key, value in environment["config"].items() if key != "mongodb_uri"
    })
    run_id = report["run_id"]
    result = {
        "status": "passed", "run_id": run_id,
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_code_sha256": environment["source_code_sha256"],
        "source_receipt_sha256": report["validation"]["source_receipt"]["receipt_sha256"],
        "config": environment["config"],
        "model_version": environment["model_version"],
        "graph_version": environment["graph_version"],
        "graph_scope": report["graph"]["validation"]["graph_scope"],
        "reused_existing_model": report["als"]["model"]["reused_existing_model"],
        "snapshots": {},
    }
    with publication_lock(config, f"check-{run_id}") as client:
        db = client[config.mongo_database]
        pointer = db["module4_snapshot_readiness"].find_one({"_id": "active"})
        require(pointer and pointer.get("ready") is True and pointer.get("run_id") == run_id,
                "active readiness pointer does not describe this run")
        for name, version_field, version, expected_count in (
            (config.mongo_recommendation_collection, "model_version", environment["model_version"],
             report["als"]["recommendation_validation"]["expected_rows"]),
            (config.mongo_graph_collection, "graph_version", environment["graph_version"],
             report["graph"]["validation"]["vertex_count"]),
        ):
            require(pointer.get(version_field) == version, "active pointer has a different version")
            collection = db[name]
            validation = snapshot_validation(collection, version_field, version, expected_count, run_id)
            require(validation["valid"], f"invalid snapshot {name}: {validation}")
            require(collection.count_documents({}) == expected_count, f"mixed/stale snapshot: {name}")
            with collection.find({}).sort("_id", 1).batch_size(5000) as cursor:
                snapshot = snapshot_digest(cursor)
            require(snapshot["documents"] == expected_count, f"read-back count mismatch: {name}")
            result["snapshots"][name] = snapshot
    if args.compare_to:
        previous = json.loads(args.compare_to.read_text(encoding="utf-8"))
        require(previous.get("status") == "passed", "baseline snapshot check did not pass")
        compare_replay(result, previous)
        result["same_version_replay"] = "passed"
        result["compared_to_run_id"] = previous["run_id"]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
