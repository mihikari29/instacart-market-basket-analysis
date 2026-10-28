"""Revision provenance and reporting at the Module 2 CLI boundary, without Spark."""

# ruff: noqa: E402 -- imports intentionally follow pytest.importorskip.

from dataclasses import asdict
from datetime import datetime
import json
import re
import subprocess
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

pytest.importorskip("pyspark")

from src.common import runtime
from src.module2 import cli


@pytest.fixture
def cli_runtime(monkeypatch):
    monkeypatch.delenv("MODULE2_ROOT", raising=False)
    monkeypatch.delenv("SPARK_MASTER", raising=False)
    monkeypatch.setattr(runtime, "subprocess", SimpleNamespace(
        check_output=Mock(side_effect=FileNotFoundError("git unavailable")),
        CalledProcessError=subprocess.CalledProcessError,
    ))
    spark = Mock(spec=["version", "_jvm", "sparkContext", "stop"])
    spark.version = "3.5.5"
    spark._jvm.java.lang.System.getProperty.return_value = "17.0.14"
    spark.sparkContext.getConf.return_value.getAll.return_value = [
        ("spark.master", "local[2]"),
        ("spark.sql.shuffle.partitions", "8"),
        ("unrelated.setting", "excluded"),
    ]

    def session(config, event_dir):
        event_dir.mkdir()
        (event_dir / "completed.json").write_text('{"Event": "SparkListenerApplicationEnd"}\n')
        return spark

    monkeypatch.setattr(cli, "session", Mock(side_effect=session))
    monkeypatch.setattr(cli, "read_tables", Mock(return_value={"interactions": object()}))
    monkeypatch.setattr(cli, "validate", Mock(return_value={"counts": {"interactions": 1}}))
    return spark


@pytest.mark.parametrize("dirty", [False, True])
def test_validate_records_revision_and_environment(tmp_path, monkeypatch, cli_runtime, dirty):
    commit = "a" * 40
    monkeypatch.setattr(
        runtime.subprocess, "check_output", Mock(side_effect=[commit + "\n", " M file\n" if dirty else ""])
    )
    revision = Mock(wraps=runtime.git_revision)
    monkeypatch.setattr(cli, "git_revision", revision)

    assert cli.main(["validate", "--output", str(tmp_path), "--run-id", "revision-test"]) == 0

    revision.assert_called_once_with("MODULE2_COMMIT")
    report = json.loads((tmp_path / "revision-test" / "summary.json").read_text())
    assert report["status"] == "passed"
    assert report["run_id"] == "revision-test"
    assert report["validation"] == {"counts": {"interactions": 1}}
    environment = report["environment"]
    assert set(environment) == {
        "commit", "dirty", "source_code_sha256", "config", "python", "platform", "timestamp_utc",
        "spark", "java", "spark_conf",
    }
    assert environment["commit"] == commit
    assert environment["dirty"] is dirty
    assert re.fullmatch(r"[0-9a-f]{64}", environment["source_code_sha256"])
    assert environment["config"] == asdict(cli.Config())
    assert environment["python"]
    assert environment["platform"]
    assert datetime.fromisoformat(environment["timestamp_utc"]).utcoffset().total_seconds() == 0
    assert environment["spark"] == "3.5.5"
    assert environment["java"] == "17.0.14"
    assert environment["spark_conf"] == {"spark.master": "local[2]", "spark.sql.shuffle.partitions": "8"}
    cli_runtime.stop.assert_called_once_with()


def test_validate_records_explicit_environment(tmp_path, monkeypatch, cli_runtime):
    monkeypatch.setenv("MODULE2_ROOT", "file:///isolated-fixture")
    monkeypatch.setenv("SPARK_MASTER", "spark://fixture-master:7077")

    assert cli.main(["validate", "--output", str(tmp_path), "--run-id", "environment-test"]) == 0

    report = json.loads((tmp_path / "environment-test" / "summary.json").read_text())
    assert report["environment"]["config"] == asdict(
        cli.Config(root="file:///isolated-fixture", master="spark://fixture-master:7077")
    )
    cli.session.assert_called_once()
    config = cli.session.call_args.args[0]
    assert config.root == "file:///isolated-fixture"
    assert config.master == "spark://fixture-master:7077"
    cli_runtime.stop.assert_called_once_with()


@pytest.mark.parametrize("failure", [FileNotFoundError("git unavailable"), subprocess.CalledProcessError(128, "git")])
def test_validate_uses_module2_commit_without_git(tmp_path, monkeypatch, cli_runtime, failure):
    monkeypatch.setenv("MODULE2_COMMIT", "b" * 40)
    monkeypatch.setenv("MODULE3_COMMIT", "c" * 40)
    monkeypatch.setattr(runtime.subprocess, "check_output", Mock(side_effect=failure))

    assert cli.main(["validate", "--output", str(tmp_path), "--run-id", "fallback-test"]) == 0

    report = json.loads((tmp_path / "fallback-test" / "summary.json").read_text())
    assert report["environment"]["commit"] == "b" * 40
    assert report["environment"]["dirty"] is None
    assert report["status"] == "passed"
    cli_runtime.stop.assert_called_once_with()


@pytest.mark.parametrize("stage", ["session", "validate"])
def test_later_failure_reports_and_propagates(tmp_path, monkeypatch, cli_runtime, stage):
    failure = RuntimeError("later stage failed")
    monkeypatch.setattr(cli, stage, Mock(side_effect=failure))

    with pytest.raises(RuntimeError, match="later stage failed") as caught:
        cli.main(["validate", "--output", str(tmp_path), "--run-id", "failed-test"])

    assert caught.value is failure
    report = json.loads((tmp_path / "failed-test" / "summary.json").read_text())
    assert report["run_id"] == "failed-test"
    assert report["status"] == "failed"
    assert report["error"] == "RuntimeError: later stage failed"
    assert {"commit", "dirty", "source_code_sha256"} <= report["environment"].keys()
    if stage == "session":
        cli_runtime.stop.assert_not_called()
    else:
        cli_runtime.stop.assert_called_once_with()


@pytest.mark.parametrize("run_id", ["../escape", "a" * 81])
def test_invalid_run_id_fails_before_initialization(tmp_path, cli_runtime, run_id):
    with pytest.raises(SystemExit) as caught:
        cli.main(["validate", "--output", str(tmp_path), "--run-id", run_id])

    assert caught.value.code == 2
    cli.session.assert_not_called()
    assert list(tmp_path.iterdir()) == []


def test_existing_run_output_is_preserved(tmp_path, cli_runtime):
    output = tmp_path / "existing-run"
    output.mkdir()
    summary = output / "summary.json"
    summary.write_text("existing report\n")

    with pytest.raises(FileExistsError):
        cli.main(["validate", "--output", str(tmp_path), "--run-id", "existing-run"])

    assert summary.read_text() == "existing report\n"
    cli.session.assert_not_called()
