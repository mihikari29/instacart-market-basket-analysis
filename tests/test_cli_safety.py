"""Offline CLI help, credential boundaries, and failure reporting contracts."""

import builtins
import importlib
import json
from pathlib import Path
import runpy
import subprocess
import sys
import traceback
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from scripts import compose_runner
from src.common.runtime import redact_arguments, redact_sensitive_text, redact_uri, save_redacted_json
from src.module2 import cli as module2
from src.module3 import cli as module3
from src.module3 import kafka_utils
from src.module4 import cli as module4
from src.module4 import tuning
from src.module4.sink_mongo import update_pair_readiness


URI = "mongodb://synthetic_user:synthetic_password@mongo.invalid:27017/fixture?authMechanismProperties=AWS_SESSION_TOKEN:synthetic_query"
SECRETS = ("synthetic_user", "synthetic_password", "synthetic_query", "synthetic_token")
ERROR = f"connection refused: {URI}; credentials='synthetic_token with spaces'"


def assert_safe(text):
    for secret in SECRETS:
        assert secret not in text


@pytest.mark.parametrize("module", ["module2", "module3", "module4"])
@pytest.mark.parametrize("entry", ["script", "module", "cli"])
def test_help_has_no_runtime_side_effects(module, entry, tmp_path, monkeypatch, capsys):
    cli = importlib.import_module(f"src.{module}.cli")
    subprocess_call = MagicMock(side_effect=AssertionError("help launched a process"))
    session = MagicMock(side_effect=AssertionError("help created Spark"))
    mkdir = MagicMock(side_effect=AssertionError("help created a directory"))
    monkeypatch.setattr(subprocess, "run", subprocess_call)
    monkeypatch.setattr(subprocess, "check_output", subprocess_call)
    monkeypatch.setattr(cli, "session", session)
    monkeypatch.setattr(Path, "mkdir", mkdir)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", [module, "--help"])
    with pytest.raises(SystemExit) as raised:
        if entry == "cli":
            cli.main(["--help"])
        elif entry == "script":
            runpy.run_path(str(compose_runner.REPOSITORY_ROOT / "scripts" / f"{module}.py"), run_name="__main__")
        else:
            runpy.run_module(f"scripts.{module}", run_name="__main__")
    assert raised.value.code == 0
    assert "usage:" in capsys.readouterr().out
    subprocess_call.assert_not_called()
    session.assert_not_called()
    mkdir.assert_not_called()


@pytest.mark.parametrize("module", ["module2", "module3", "module4"])
def test_help_missing_optional_dependency_does_not_start_services(module, monkeypatch, capsys):
    original_import = builtins.__import__

    def missing(name, *args, **kwargs):
        if name == f"src.{module}.cli":
            raise ImportError("synthetic optional dependency unavailable")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", missing)
    launch = MagicMock(side_effect=AssertionError("help started services"))
    monkeypatch.setattr(compose_runner, "run_module", launch)
    assert compose_runner.execute(compose_runner.MODULE_RUNNERS[module], ["--build", "--help"]) == 0
    launch.assert_not_called()
    assert "--no-deps" in capsys.readouterr().err


@pytest.mark.parametrize("module", ["module2", "module3", "module4"])
@pytest.mark.parametrize("exit_code", [0, 23])
def test_normal_wrapper_execution_and_exit_code(module, exit_code, monkeypatch):
    calls = []

    def run(arguments, **kwargs):
        calls.append(arguments)
        if arguments[:3] == ["docker", "compose", "run"] and exit_code:
            raise subprocess.CalledProcessError(exit_code, arguments)

    monkeypatch.setattr(compose_runner, "require_docker", lambda: None)
    monkeypatch.setattr(compose_runner, "git_revision", lambda root: "synthetic-commit")
    monkeypatch.setattr(compose_runner, "run_checked", run)
    with pytest.raises(SystemExit) as raised:
        compose_runner.main_entry(compose_runner.MODULE_RUNNERS[module], ["--build", "validate"])
    assert raised.value.code == exit_code
    assert calls[0][:4] == ["docker", "compose", "up", "-d"]
    assert "--build" in calls[0]
    assert calls[1][-2:] == [module, "validate"]


@pytest.mark.parametrize("text", [
    ERROR,
    "{'username': 'synthetic_user', 'password': 'synthetic_password', 'credentials': 'synthetic_token with spaces'}",
    '--access-token synthetic_token --credentials="synthetic_password with spaces" timeout=10',
])
def test_shared_redaction_covers_quoted_fields_and_secret_options(text):
    assert_safe(redact_sensitive_text(text))


@pytest.mark.parametrize("uri", [URI, "mongodb+srv://synthetic_user:synthetic_password@mongo.invalid/fixture?token=synthetic_query"])
def test_redaction_keeps_operational_uri_metadata(uri):
    result = redact_uri(uri)
    assert_safe(result)
    assert "mongo.invalid" in result
    assert "/fixture" in result


def test_repeated_redaction_preserves_safe_uri_metadata():
    once = redact_sensitive_text(f"connection refused: {URI}")
    assert redact_sensitive_text(once) == once
    assert "/fixture?<redacted>" in once


def test_argument_redaction_preserves_shape_and_operational_flags():
    arguments = ["module3", "run", "--mongodb-uri", URI, "--password", "synthetic_password with spaces",
                 "--access-token=synthetic_token", "--credentials", "synthetic_query", "--duration-seconds", "60"]
    result = redact_arguments(arguments)
    assert_safe(json.dumps(result))
    assert len(result) == len(arguments)
    assert result[:3] == arguments[:3]
    assert result[-2:] == arguments[-2:]
    assert arguments[3] == URI
    assert arguments[5] == "synthetic_password with spaces"


def test_structured_evidence_redaction_preserves_values_and_does_not_mutate_input(tmp_path):
    evidence = {"username": "synthetic_user", "password": "synthetic_password",
                "spark_conf": {"spark.executorEnv.AWS_SECRET_ACCESS_KEY": "synthetic_token"},
                "nested": [{"credentials": "synthetic_query", "rows": 42}],
                "database": "fixture", "mongodb_uri": URI}
    path = tmp_path / "manifest.json"
    save_redacted_json(path, evidence)
    result = json.loads(path.read_text())
    assert_safe(path.read_text())
    assert result["database"] == "fixture"
    assert result["nested"][0]["rows"] == 42
    assert "mongo.invalid" in result["mongodb_uri"]
    assert evidence["username"] == "synthetic_user"
    assert evidence["mongodb_uri"] == URI


def install_module3(monkeypatch):
    monkeypatch.setattr(module3, "git_revision", lambda *_: {"commit": "fixture", "dirty": False})
    monkeypatch.setattr(module3, "code_digest", lambda: "fixture-digest")
    monkeypatch.setattr(module3, "validate_kafka", lambda _: {"ok": True})
    monkeypatch.setattr(module3, "validate_mongo", lambda _: {"ok": True})


@pytest.mark.parametrize("failure", [False, True])
def test_module3_reports_and_errors_are_safe_without_mutating_credentials(tmp_path, monkeypatch, capsys, failure):
    install_module3(monkeypatch)
    connections = []

    def validate(config):
        connections.append(config.mongodb_uri)
        if failure:
            raise RuntimeError(ERROR)
        return {"ok": True, "mongodb_uri": redact_uri(config.mongodb_uri)}

    monkeypatch.setattr(module3, "validate_mongo", validate)
    code = module3.main(["validate", "--output", str(tmp_path), "--run-id", "fixture", "--mongodb-uri", URI])
    assert code == int(failure)
    assert connections == [URI]
    report = (tmp_path / "fixture" / "summary.json").read_text()
    assert json.loads(report)["status"] == ("failed" if failure else "passed")
    captured = capsys.readouterr()
    assert_safe(report + captured.out + captured.err)
    if failure:
        assert "connection refused" in report + captured.out + captured.err


def test_mongo_validation_error_is_safe_and_client_is_closed(monkeypatch):
    client = MagicMock()
    client.__enter__.return_value = client
    client.admin.command.side_effect = RuntimeError(ERROR)
    factory = MagicMock(return_value=client)
    monkeypatch.setattr("pymongo.MongoClient", factory)
    result = kafka_utils.validate_mongo(module3.Config(mongodb_uri=URI))
    assert result["ok"] is False
    assert_safe(json.dumps(result))
    factory.assert_called_once_with(URI, serverSelectionTimeoutMS=5000)
    client.__exit__.assert_called_once()


def install_module4(monkeypatch):
    monkeypatch.setattr(module4, "revision", lambda: {"commit": "fixture", "dirty": False})
    monkeypatch.setattr(module4, "source_digest", lambda: "fixture-digest")
    monkeypatch.setattr(module4, "_runtime_metadata", lambda _: {})
    monkeypatch.setattr(module4, "read_inputs", lambda *_: {})
    monkeypatch.setattr(module4, "validate_inputs", lambda *_: {"counts": {}})
    monkeypatch.setattr(module4, "validate_source_receipts", lambda *_, **__: {"fixture": True})
    monkeypatch.setenv("MONGODB_URI", URI)


@pytest.mark.parametrize("failure", [False, True])
def test_module4_artifacts_redact_config_command_and_errors(tmp_path, monkeypatch, capsys, failure):
    install_module4(monkeypatch)
    spark = MagicMock()
    configs = []

    def session(config, eventlog):
        configs.append(config)
        assert_safe((eventlog.parent / "manifest.json").read_text())
        if failure:
            raise RuntimeError(ERROR)
        return spark

    monkeypatch.setattr(module4, "session", session)
    args = ["validate", "--output", str(tmp_path), "--run-id", "fixture",
            "--root", "file:///fixture?token=synthetic_token"]
    assert module4.main(args) == int(failure)
    assert configs[0].mongodb_uri == URI
    assert configs[0].root == args[-1]
    captured = capsys.readouterr()
    assert_safe(captured.out + captured.err)
    for path in (tmp_path / "fixture").iterdir():
        if path.is_file():
            assert_safe(path.read_text())
    report = json.loads((tmp_path / "fixture" / "summary.json").read_text())
    assert report["status"] == ("failed" if failure else "passed")
    assert report["environment"]["command"][:2] == ["module4", "validate"]


def test_module4_cleanup_failure_and_eventlog_diagnostics_are_safe(tmp_path, monkeypatch, capsys):
    install_module4(monkeypatch)
    spark = MagicMock()
    spark.stop.side_effect = RuntimeError(ERROR)

    def session(config, eventlog):
        eventlog.mkdir()
        return spark

    monkeypatch.setattr(module4, "session", session)
    monkeypatch.setattr("src.module2.eventlog.parse_event_logs", MagicMock(side_effect=RuntimeError(ERROR)))
    assert module4.main(["validate", "--output", str(tmp_path), "--run-id", "fixture"]) == 1
    report = (tmp_path / "fixture" / "summary.json").read_text()
    assert "cleanup_errors" in report
    assert "parse_failed" in report
    assert_safe(report)
    spark.stop.assert_called_once()


@pytest.mark.parametrize("module", [module3, module4])
def test_cli_parse_errors_do_not_echo_secret_arguments(module, monkeypatch, capsys):
    with pytest.raises(SystemExit) as raised:
        module.main(["validate", "--credentials", "synthetic_password with spaces", "--access-token=synthetic_token"])
    assert raised.value.code == 2
    captured = capsys.readouterr()
    assert_safe(captured.out + captured.err)


def test_module4_malformed_uri_redaction_does_not_raise():
    config = module4.Config(mongodb_uri="mongodb://synthetic_user:synthetic_password@mongo.invalid:bad/fixture")
    assert_safe(json.dumps(module4._safe_config(config)))


@pytest.mark.parametrize("phase", ["pilot", "full", "baseline"])
def test_als_tuning_failure_evidence_is_sanitized(tmp_path, monkeypatch, phase):
    frame = MagicMock()
    for method in ("persist", "cache", "select", "distinct", "join"):
        getattr(frame, method).return_value = frame
    frame.count.return_value = 1
    monkeypatch.setattr(tuning, "build_prior_interactions", lambda _: frame)
    monkeypatch.setattr(tuning, "_sample_users", lambda *_: frame)
    monkeypatch.setattr(tuning, "global_popularity", lambda *_: frame)
    monkeypatch.setattr(tuning, "personal_frequency", lambda *_: frame)
    monkeypatch.setattr(tuning, "_metric_summary", lambda *_: {})
    monkeypatch.setattr(tuning, "_trial_grid", lambda _: [module4.ALSParams()])
    monkeypatch.setattr(tuning, "recommend_top_k", lambda *_: frame)
    model = MagicMock()
    model.userFactors.count.return_value = model.itemFactors.count.return_value = 1
    monkeypatch.setattr(tuning, "fit_model", MagicMock(side_effect=(
        [model, RuntimeError(ERROR)] if phase == "full" else RuntimeError(ERROR)
    )))
    if phase == "baseline":
        monkeypatch.setattr(tuning, "global_popularity", MagicMock(side_effect=RuntimeError(ERROR)))
    path = tmp_path / "tuning.json"
    if phase == "baseline":
        with pytest.raises(RuntimeError):
            module4.tune_als({"training": frame, "validation_truth": frame}, module4.Config(), evidence_path=path)
    else:
        result = module4.tune_als({"training": frame, "validation_truth": frame}, module4.Config(), evidence_path=path)
        assert result["status"] == "failed"
    assert_safe(path.read_text())
    assert "connection refused" in path.read_text()


def test_failed_pair_readiness_document_does_not_persist_secrets():
    client = MagicMock()
    result = update_pair_readiness(client, module4.Config(), "fixture", "model", "graph", "failed", True, False, ERROR)
    assert result["ready"] is False
    assert result["recommendation_published"] is True
    assert_safe(json.dumps(result))
    document = client.__getitem__.return_value.__getitem__.return_value.replace_one.call_args.args[1]
    assert_safe(str(document))


@pytest.mark.parametrize("primary_failure", [False, True])
def test_module3_primary_and_cleanup_errors_are_reported_safely(tmp_path, monkeypatch, capsys, primary_failure):
    from src.module3 import stream

    install_module3(monkeypatch)
    spark = MagicMock()
    spark.stop.side_effect = RuntimeError(ERROR)
    monkeypatch.setattr(module3, "session", lambda *_: spark)
    monkeypatch.setattr(stream, "run_stream", MagicMock(
        side_effect=ValueError("synthetic primary failure") if primary_failure else None,
        return_value={"completed": True},
    ))
    assert module3.main(["run", "--output", str(tmp_path), "--run-id", "fixture"]) == 1
    report = json.loads((tmp_path / "fixture" / "summary.json").read_text())
    assert "synthetic primary failure" in report["error"] if primary_failure else "connection refused" in report["error"]
    assert "cleanup_errors" in report
    captured = capsys.readouterr()
    assert_safe(json.dumps(report) + captured.out + captured.err)
    spark.stop.assert_called_once()


@pytest.mark.parametrize("failure", ["read", "stop"])
def test_user_feature_inspection_failure_is_safe(tmp_path, monkeypatch, capsys, failure):
    install_module4(monkeypatch)
    spark = MagicMock()
    monkeypatch.setattr(module4, "session", lambda *_: spark)
    if failure == "read":
        monkeypatch.setattr(module4, "read_user_features", MagicMock(side_effect=RuntimeError(ERROR)))
    else:
        spark.stop.side_effect = RuntimeError(ERROR)
        monkeypatch.setattr(module4, "read_user_features", lambda *_: {
            "status": "available", "scope": "prior_only", "source": "fixture", "user_id": 1, "features": {},
        })
    assert module4.main(["user-features", "--user-id", "1", "--output", str(tmp_path), "--run-id", "fixture"]) == 1
    captured = capsys.readouterr()
    assert_safe(captured.out + captured.err)
    assert "connection refused" in captured.out + captured.err
    report = (tmp_path / "user-features" / "fixture" / "user_features.json").read_text()
    assert json.loads(report)["status"] == "failed"
    assert_safe(report)
    spark.stop.assert_called_once()


@pytest.mark.parametrize("command", ["validate", "run", "late-demo", "benchmark-throughput", "benchmark-partitions", "all"])
def test_module3_successful_commands_stop_spark_once_and_preserve_dispatch(tmp_path, monkeypatch, command):
    from src.module3 import benchmark, demo, stream

    install_module3(monkeypatch)
    spark = MagicMock()
    session = MagicMock(return_value=spark)
    monkeypatch.setattr(module3, "session", session)
    functions = {}
    for owner, name in ((stream, "run_stream"), (demo, "run_late_data_demo"),
                        (benchmark, "benchmark_throughput"), (benchmark, "benchmark_kafka_partitions")):
        functions[name] = MagicMock(return_value={"completed": True})
        monkeypatch.setattr(owner, name, functions[name])
    assert module3.main([command, "--output", str(tmp_path), "--run-id", "fixture"]) == 0
    assert session.call_count == spark.stop.call_count == int(command != "validate")
    assert functions["run_stream"].call_count == int(command == "run")
    assert functions["run_late_data_demo"].call_count == int(command == "late-demo")
    assert functions["benchmark_throughput"].call_count == int(command in {"all", "benchmark-throughput"})
    assert functions["benchmark_kafka_partitions"].call_count == int(command in {"all", "benchmark-partitions"})
    report = json.loads((tmp_path / "fixture" / "summary.json").read_text())
    assert report["status"] == "passed"


@pytest.mark.parametrize("primary_failure,stop_failure", [(False, False), (True, False), (True, True), (False, True)])
def test_module2_revision_fallback_and_failure_cleanup(tmp_path, monkeypatch, capsys, primary_failure, stop_failure):
    spark = MagicMock()
    if stop_failure:
        spark.stop.side_effect = RuntimeError(ERROR)
    monkeypatch.setenv("MODULE2_COMMIT", "synthetic-revision")
    monkeypatch.setattr(subprocess, "check_output", MagicMock(side_effect=subprocess.CalledProcessError(1, ["git"])))
    monkeypatch.setattr(module2, "session", lambda *_: spark)
    monkeypatch.setattr(module2, "read_tables", lambda *_: {"interactions": object()})
    monkeypatch.setattr(module2, "validate", MagicMock(
        side_effect=ValueError("synthetic primary failure") if primary_failure else None,
        return_value={"counts": {}},
    ))
    monkeypatch.setattr(module2, "parse_event_logs", lambda _: {})
    assert module2.main(["validate", "--output", str(tmp_path), "--run-id", "fixture"]) == int(primary_failure or stop_failure)
    report = json.loads((tmp_path / "fixture" / "summary.json").read_text())
    assert report["environment"]["commit"] == "synthetic-revision"
    assert report["status"] == ("failed" if primary_failure or stop_failure else "passed")
    if primary_failure:
        assert "synthetic primary failure" in report["error"]
    captured = capsys.readouterr()
    assert_safe(json.dumps(report) + captured.out + captured.err)
    spark.stop.assert_called_once()


def test_stream_sink_callback_sanitizes_errors_before_spark_can_log_them(tmp_path, monkeypatch):
    from src.module3 import stream

    writer = MagicMock()
    for method in ("outputMode", "option", "foreachBatch", "queryName", "trigger"):
        getattr(writer, method).return_value = writer

    def start():
        callback = writer.foreachBatch.call_args.args[0]
        callback(None, 0)

    writer.start.side_effect = start
    monkeypatch.setattr(stream, "from_kafka", lambda *_: None)
    monkeypatch.setattr(stream, "build_window_counts", lambda *_: SimpleNamespace(writeStream=writer))
    monkeypatch.setattr(stream, "ensure_checkpoint", lambda *_, **__: "file:///fixture/checkpoint")
    monkeypatch.setattr(stream, "_foreach_batch", MagicMock(side_effect=RuntimeError(ERROR)))
    with pytest.raises(RuntimeError) as raised:
        stream.run_stream(object(), module3.Config(), tmp_path, "fixture")
    formatted = "".join(traceback.format_exception(raised.type, raised.value, raised.tb))
    assert_safe(formatted)
    assert "connection refused" in formatted


def test_module4_log_output_uses_shared_redaction(capsys):
    module4._log(ERROR)
    captured = capsys.readouterr()
    assert "connection refused" in captured.out
    assert_safe(captured.out + captured.err)
