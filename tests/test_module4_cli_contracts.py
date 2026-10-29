"""CLI and evaluation report boundaries around Module 4 experiments."""

from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys
from unittest.mock import MagicMock, call

from src.module4 import cli, tuning


def test_cli_commands_preserve_defaults_and_explicit_experiment_arguments():
    parser = cli.build_parser()
    defaults = {
        name: None for name in (
            "root", "master", "shuffle_partitions", "seed", "recommendation_k", "graph_min_edge_weight",
            "graph_reset_probability", "graph_max_iter", "graph_top_products", "executor_memory",
            "executor_cores", "total_cores", "als_blocks", "source_receipt", "run_id", "user_id",
            "model_version", "graph_version",
        )
    }
    defaults.update(graph_backend="graphframes", pilot_users=5000, max_trials=12,
                    output=Path("results/module4"), scope="staged_curated", skip_serving=False)
    for command in ("validate", "als", "graph", "all", "user-features"):
        assert vars(parser.parse_args([command])) == {"command": command, **defaults}
    arguments = parser.parse_args([
        "als", "--pilot-users", "2", "--max-trials", "3", "--seed", "7", "--als-blocks", "2",
        "--output", "fixture", "--skip-serving",
    ])
    assert (arguments.pilot_users, arguments.max_trials, arguments.seed, arguments.als_blocks) == (2, 3, 7, 2)
    assert arguments.output == Path("fixture") and arguments.skip_serving


def test_evaluation_dispatch_and_deltas_preserve_order_missing_values_and_sign(monkeypatch):
    first, second, truth = object(), object(), object()
    evaluate = MagicMock(side_effect=[{"5": "first"}, {"5": "second"}])
    monkeypatch.setattr(cli, "evaluate_at_k", evaluate)
    assert cli._evaluate_methods({"personal_frequency": first, "als": second}, truth, 4) == {
        "personal_frequency": {"5": "first"}, "als": {"5": "second"},
    }
    assert evaluate.call_args_list == [call(first, truth, (5, 10, 20), catalog_size=4),
                                       call(second, truth, (5, 10, 20), catalog_size=4)]
    result = cli._evaluation_deltas({"cohort": {"metrics": {
        "als": {"5": {"precision_at_k": 0.5, "recall_at_k": 0.25, "map_at_k": 0.0, "ndcg_at_k": None}},
        "global_popularity": {"5": {"precision_at_k": 0.25, "recall_at_k": 0.5, "map_at_k": 0.0}},
    }}})["cohort"]
    assert list(result) == ["global_popularity", "personal_frequency"]
    assert list(result["global_popularity"]) == ["5", "10", "20"]
    assert result["global_popularity"]["5"] == {
        "precision_at_k": 0.25, "recall_at_k": -0.25, "map_at_k": 0.0, "ndcg_at_k": None,
    }
    assert all(value is None for by_k in result["personal_frequency"].values() for value in by_k.values())


def test_run_artifacts_preserve_manifest_schema_redaction_and_existing_test_evidence(tmp_path, monkeypatch):
    finished = datetime(2026, 1, 1, tzinfo=timezone.utc)

    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz):
            return finished

    monkeypatch.setattr(cli, "datetime", FixedDatetime)
    report = {
        "run_id": "fixture", "status": "passed", "scope": "staged_fixture", "serving_status": "skipped",
        "environment": {"source_code_sha256": "a" * 64, "config": {"password": "synthetic-secret"}},
        "stages": {"als_compute": "passed"}, "stage_timings_ms": {"als_compute": 12},
        "validation": {"source_receipt": {"receipt_sha256": "b" * 64}},
        "started_at_utc": "2025-12-31T23:59:00+00:00",
    }
    (tmp_path / "test-results.txt").write_text("existing evidence\n")
    cli._save_run_artifacts(tmp_path, report)
    manifest = json.loads((tmp_path / "manifest.json").read_text())
    assert manifest == {
        "source_code_sha256": "a" * 64, "config": {"password": "<redacted>"},
        "run_id": "fixture", "status": "passed", "stages": {"als_compute": "passed"},
        "source_receipt": {"receipt_sha256": "b" * 64}, "input_source_receipt": None,
        "scope": "staged_fixture", "acceptance_status": None, "checkpoint": None, "eventlog": None,
        "error": None, "started_at_utc": report["started_at_utc"], "finished_at_utc": finished.isoformat(),
    }
    assert json.loads((tmp_path / "performance.json").read_text()) == {
        "stages": {"als_compute": 12}, "als_ms": None, "graph_ms": None, "spark_job_groups": {},
    }
    assert json.loads((tmp_path / "mongo_validation.json").read_text()) == {
        "recommendations": {}, "graph": {}, "pair_readiness": None,
    }
    assert report["finished_at_utc"] == finished.isoformat()
    assert report["environment"]["config"]["password"] == "synthetic-secret"
    assert "synthetic-secret" not in (tmp_path / "summary.json").read_text()
    assert (tmp_path / "test-results.txt").read_text() == "existing evidence\n"


def test_cli_keeps_direct_imports_of_canonical_tuning_implementations():
    assert cli.tune_als is tuning.tune_als
    assert cli._trial_grid is tuning._trial_grid
    assert cli._metric_summary is tuning._metric_summary
    assert cli._trial_order is tuning._trial_order
    assert cli._sample_users is tuning._sample_users
    assert cli._unpersist_model is tuning._unpersist_model


def test_tuning_imports_independently_without_cli_or_spark():
    result = subprocess.run(
        [sys.executable, "-B", "-c", (
            "import sys; from src.module4.tuning import tune_als; "
            "assert callable(tune_als); assert 'src.module4.cli' not in sys.modules; "
            "assert 'pyspark' not in sys.modules"
        )],
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stderr


def test_source_digest_covers_relocated_tuning_implementation(tmp_path, monkeypatch):
    for name in ("scripts/module4.py", "scripts/module4_smoke.py", "Dockerfile", "compose.yaml",
                 "requirements.txt", "requirements-dev.txt", "src/module4/tuning.py"):
        target = tmp_path / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((cli.REPO_ROOT / name).read_bytes())
    monkeypatch.setattr(cli, "REPO_ROOT", tmp_path)
    original = cli.source_digest()
    source = tmp_path / "src/module4/tuning.py"
    content = source.read_bytes()
    source.write_bytes(content + b"\n# provenance sensitivity fixture\n")
    assert cli.source_digest() != original
    source.write_bytes(content)
    assert cli.source_digest() == original
