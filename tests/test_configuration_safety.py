"""Configuration ownership, precedence, and validation without live services."""

from dataclasses import FrozenInstanceError, asdict
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

from scripts.k8s import config as pipeline
from src.module2.config import Config as BatchConfig
from src.module3.config import Config as StreamConfig
from src.module4.config import Config as ModelConfig
from src.module4.als import ALSParams
from src.module4.hybrid.config import HybridRankingConfig
from src.module4.cli import _trial_grid


@pytest.mark.parametrize("config", [BatchConfig(), StreamConfig(), ModelConfig()])
def test_module_defaults_remain_immutable_and_preserve_storage_contract(config):
    assert config.root == "hdfs://namenode:8020/instacart"
    assert config.master == "local[2]"
    assert config.shuffle_partitions == 8
    with pytest.raises(FrozenInstanceError):
        config.root = "file:///changed"


def test_batch_environment_and_explicit_precedence(monkeypatch):
    monkeypatch.setenv("MODULE2_ROOT", "file:///environment")
    monkeypatch.setenv("SPARK_MASTER", "local[1]")
    assert BatchConfig.environment().root == "file:///environment"
    assert BatchConfig.environment().master == "local[1]"
    actual = BatchConfig.environment(root="file:///explicit", master="local[2]", shuffle_partitions=4)
    assert (actual.root, actual.master, actual.shuffle_partitions) == ("file:///explicit", "local[2]", 4)


@pytest.mark.parametrize("owner,variable,field,text,expected", [
    (StreamConfig, "MONGO_TTL_SECONDS", "ttl_seconds", "0", 0),
    (StreamConfig, "MONGO_TOP_K_PER_WINDOW", "mongo_top_k_per_window", "10", 10),
    (StreamConfig, "MODULE3_FAIL_ON_DATA_LOSS", "fail_on_data_loss", "false", False),
    (ModelConfig, "MODULE4_EXECUTOR_CORES", "executor_cores", "1", 1),
    (ModelConfig, "MODULE4_GRAPH_RESET_PROBABILITY", "graph_reset_probability", "0.2", 0.2),
])
def test_supported_environment_values_reach_configuration(monkeypatch, owner, variable, field, text, expected):
    monkeypatch.setenv(variable, text)
    assert getattr(owner.environment(), field) == expected


@pytest.mark.parametrize("owner,variable,field,value", [
    (StreamConfig, "MONGO_TTL_SECONDS", "ttl_seconds", 0),
    (StreamConfig, "MONGO_TOP_K_PER_WINDOW", "mongo_top_k_per_window", 10),
    (StreamConfig, "MODULE3_FAIL_ON_DATA_LOSS", "fail_on_data_loss", False),
    (StreamConfig, "MODULE3_PUBLISH_TREND_BUNDLES", "publish_trend_bundles", False),
    (ModelConfig, "MODULE4_SHUFFLE_PARTITIONS", "shuffle_partitions", 4),
    (ModelConfig, "MODULE4_GRAPH_RESET_PROBABILITY", "graph_reset_probability", 0.2),
    (ModelConfig, "MODULE4_GRAPH_TOP_PRODUCTS", "graph_top_products", None),
])
def test_explicit_values_do_not_parse_shadowed_invalid_environment(monkeypatch, owner, variable, field, value):
    monkeypatch.setenv(variable, "synthetic-invalid-value")
    assert getattr(owner.environment(**{field: value}), field) == value


@pytest.mark.parametrize("owner,variable", [
    (StreamConfig, "MONGO_TTL_SECONDS"), (ModelConfig, "MODULE4_SHUFFLE_PARTITIONS"),
    (ModelConfig, "MODULE4_GRAPH_RESET_PROBABILITY"),
])
def test_invalid_environment_diagnostics_do_not_echo_values(monkeypatch, owner, variable):
    monkeypatch.setenv(variable, "synthetic-sensitive-value")
    with pytest.raises(ValueError) as error:
        owner.environment()
    assert variable in str(error.value)
    assert "synthetic-sensitive-value" not in str(error.value)
    assert error.value.__suppress_context__


@pytest.mark.parametrize("owner,field,value", [
    (BatchConfig, "shuffle_partitions", True), (BatchConfig, "repetitions", 3.5),
    (BatchConfig, "warmups", float("nan")), (BatchConfig, "master", " "),
    (BatchConfig, "root", "\x00"),
    (StreamConfig, "shuffle_partitions", True), (StreamConfig, "ttl_seconds", float("nan")),
    (StreamConfig, "ttl_seconds", True), (StreamConfig, "mongo_top_k_per_window", 1.5),
    (StreamConfig, "weight_short", True), (StreamConfig, "weight_long", float("nan")),
    (StreamConfig, "master", ""), (StreamConfig, "topic", " "),
    (StreamConfig, "mongo_database", "bad.name"), (StreamConfig, "mongodb_uri", ""),
    (ModelConfig, "shuffle_partitions", True), (ModelConfig, "recommendation_k", 2.5),
    (ModelConfig, "graph_max_iter", float("nan")), (ModelConfig, "seed", True),
    (ModelConfig, "graph_reset_probability", True), (ModelConfig, "als_blocks", False),
    (ModelConfig, "root", " "), (ModelConfig, "executor_memory", None),
])
def test_invalid_module_configuration_is_rejected(owner, field, value):
    with pytest.raises(ValueError):
        owner(**{field: value})


@pytest.mark.parametrize("cores,total", [(3, 2), (2, 3)])
def test_model_executor_core_budgets_are_not_treated_as_exact_allocations(cores, total):
    config = ModelConfig(executor_cores=cores, total_cores=total)
    assert (config.executor_cores, config.total_cores) == (cores, total)


@pytest.mark.parametrize("cores,total", [(1, 2), (4, 4)])
def test_valid_model_executor_allocations_and_optional_catalog_limit(cores, total):
    config = ModelConfig(executor_cores=cores, total_cores=total, executor_memory="2G")
    assert (config.executor_cores, config.total_cores, config.executor_memory) == (cores, total, "2G")
    assert config.graph_top_products is None


@pytest.mark.parametrize("options", [
    {"window_short": "3 hours", "window_long": "2 hours"},
    {"window_short": "invalid"}, {"window_long": "1 month"},
    {"slide": "3 hours"}, {"slide": "0 seconds"},
    {"watermark": "-1 second"}, {"trigger_interval": "-1 second"},
])
def test_streaming_invalid_intervals_fail_before_processing(options):
    with pytest.raises(ValueError):
        StreamConfig(**options)


@pytest.mark.parametrize("options", [
    {"window_short": "1 minute", "window_long": "5 minutes", "slide": "2 minutes"},
    {"window_short": "1 hour", "window_long": "1 hour", "watermark": "2 hours"},
    {"window_short": "1 minute 30 seconds", "window_long": "2 hours", "watermark": "0 seconds"},
    {"trigger_interval": "0 seconds"}, {"trigger_interval": None}, {"trigger_interval": ""},
])
def test_supported_streaming_combinations_remain_valid(options):
    config = StreamConfig(**options)
    for name, value in options.items():
        assert getattr(config, name) == value


@pytest.mark.parametrize("module,arguments", [
    ("module2", ["validate", "--shuffle-partitions", "0"]),
    ("module3", ["validate", "--window-short", "3 hours", "--window-long", "2 hours"]),
    ("module4", ["validate", "--executor-cores", "0"]),
])
def test_cli_rejects_bad_configuration_before_creating_run_directory(monkeypatch, module, arguments):
    import importlib

    cli = importlib.import_module(f"src.{module}.cli")
    mkdir = MagicMock(side_effect=AssertionError("configuration created a directory"))
    monkeypatch.setattr(Path, "mkdir", mkdir)
    with pytest.raises(ValueError):
        cli.main(arguments)
    mkdir.assert_not_called()


def test_stream_help_does_not_parse_runtime_environment(monkeypatch, capsys):
    from src.module3 import cli

    monkeypatch.setenv("MONGO_TTL_SECONDS", "synthetic-invalid-value")
    monkeypatch.setenv("MODULE3_FAIL_ON_DATA_LOSS", "synthetic-invalid-value")
    with pytest.raises(SystemExit) as raised:
        cli.main(["--help"])
    assert raised.value.code == 0
    assert "usage:" in capsys.readouterr().out


def test_stream_cli_explicit_flags_override_invalid_environment(tmp_path, monkeypatch):
    from src.module3 import cli

    monkeypatch.setenv("MONGO_TTL_SECONDS", "synthetic-invalid-value")
    monkeypatch.setenv("MODULE3_FAIL_ON_DATA_LOSS", "synthetic-invalid-value")
    observed = []
    monkeypatch.setattr(cli, "validate_kafka", lambda config: observed.append(config) or {"ok": True})
    monkeypatch.setattr(cli, "validate_mongo", lambda _: {"ok": True})
    assert cli.main(["validate", "--ttl-seconds", "0", "--allow-data-loss", "--output", str(tmp_path)]) == 0
    assert observed[0].ttl_seconds == 0
    assert observed[0].fail_on_data_loss is False


@pytest.mark.parametrize("command,options", [
    ("run", ["--duration-seconds", "-1"]), ("late-demo", ["--duration-seconds", "0"]),
    ("benchmark-throughput", ["--duration-seconds", "11"]),
    ("benchmark-partitions", ["--benchmark-events", "0"]),
    ("benchmark-throughput", ["--benchmark-rates", "nan"]),
    ("benchmark-throughput", ["--benchmark-rates=-1"]),
    ("benchmark-throughput", ["--benchmark-rates", "10,synthetic-invalid-value"]),
])
def test_replay_configuration_is_checked_before_run_side_effects(monkeypatch, command, options):
    from src.module3 import cli

    mkdir = MagicMock(side_effect=AssertionError("invalid replay configuration created a run directory"))
    monkeypatch.setattr(Path, "mkdir", mkdir)
    with pytest.raises(ValueError):
        cli.main([command, *options])
    mkdir.assert_not_called()


def test_zero_duration_still_means_unbounded_stream_run(tmp_path, monkeypatch):
    from src.module3 import cli, stream

    spark = MagicMock()
    run = MagicMock(return_value={"interrupted": True})
    monkeypatch.setattr(cli, "session", lambda *_: spark)
    monkeypatch.setattr(cli, "validate_kafka", lambda _: {"ok": True})
    monkeypatch.setattr(cli, "validate_mongo", lambda _: {"ok": True})
    monkeypatch.setattr(stream, "run_stream", run)
    assert cli.main(["run", "--duration-seconds", "0", "--output", str(tmp_path)]) == 0
    assert run.call_args.kwargs["duration_seconds"] is None
    spark.stop.assert_called_once()


@pytest.mark.parametrize("text", ["models: [broken", "[]", "models: []", "models:\n  als: []", "models:\n  als:\n    rank: true"])
def test_als_configuration_never_silently_discards_invalid_yaml(tmp_path, monkeypatch, text):
    directory = tmp_path / "config"
    directory.mkdir()
    (directory / "pipeline.yaml").write_text(text)
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ValueError):
        _trial_grid(1)


@pytest.mark.parametrize("field,value", [
    ("rank", True), ("max_iter", 1.5), ("reg_param", True), ("alpha", True),
    ("reg_param", float("nan")), ("alpha", float("inf")),
])
def test_als_constructor_rejects_invalid_numeric_parameters(field, value):
    with pytest.raises(ValueError, match=field):
        ALSParams(**{field: value})


def test_als_valid_yaml_keeps_selected_parameters_and_missing_file_defaults(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert asdict(_trial_grid(1)[0]) == {
        "rank": 32, "max_iter": 10, "reg_param": 0.1, "alpha": 10.0, "strength_col": "strength_raw",
    }
    directory = tmp_path / "config"
    directory.mkdir()
    (directory / "pipeline.yaml").write_text("models:\n  als:\n    rank: 16\n    max_iter: 3\n    reg_param: 0.2\n    alpha: 5.0\n    strength_col: strength_log\n")
    assert asdict(_trial_grid(1)[0]) == {
        "rank": 16, "max_iter": 3, "reg_param": 0.2, "alpha": 5.0, "strength_col": "strength_log",
    }


def test_als_cli_checks_yaml_before_spark_or_output_creation(tmp_path, monkeypatch):
    from src.module4 import cli

    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "pipeline.yaml").write_text("models: [broken")
    monkeypatch.chdir(tmp_path)
    session = MagicMock(side_effect=AssertionError("invalid YAML started Spark"))
    monkeypatch.setattr(cli, "session", session)
    with pytest.raises(ValueError):
        cli.main(["als", "--output", str(tmp_path / "results")])
    assert not (tmp_path / "results").exists()
    session.assert_not_called()


@pytest.mark.parametrize("model", ["als", "hybrid"])
def test_unknown_model_settings_are_not_silently_discarded(tmp_path, monkeypatch, model):
    (tmp_path / "config").mkdir()
    path = tmp_path / "config" / "pipeline.yaml"
    path.write_text(f"models:\n  {model}:\n    synthetic_unknown_option: 10\n")
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ValueError, match="unsupported"):
        _trial_grid(1) if model == "als" else HybridRankingConfig.from_pipeline_config(path)


@pytest.mark.parametrize("field,value", [
    ("alpha", True), ("beta", float("nan")), ("gamma", -0.1),
    ("alpha", float("inf")), ("beta", 2.0),
])
def test_hybrid_weights_are_finite_probability_weights(field, value):
    with pytest.raises(ValueError, match=field):
        HybridRankingConfig(**{field: value})


def test_hybrid_active_three_way_weights_must_sum_to_one():
    with pytest.raises(ValueError, match="sum"):
        HybridRankingConfig(alpha=0.5, beta=0.2, gamma=0.5)


@pytest.mark.parametrize("text", ["models: [broken", "[]", "models: []", "models:\n  hybrid: []", "models:\n  hybrid:\n    output_top_k: true"])
def test_hybrid_explicit_yaml_errors_are_not_converted_to_defaults(tmp_path, text):
    path = tmp_path / "pipeline.yaml"
    path.write_text(text)
    with pytest.raises(ValueError):
        HybridRankingConfig.from_pipeline_config(path)


def test_hybrid_explicit_missing_file_fails_and_optional_default_is_preserved(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert HybridRankingConfig.from_pipeline_config() == HybridRankingConfig()
    with pytest.raises(ValueError):
        HybridRankingConfig.from_pipeline_config(tmp_path / "missing.yaml")


def test_hybrid_yaml_weights_do_not_redefine_lambda_or_activate_cli_scoring(tmp_path):
    path = tmp_path / "pipeline.yaml"
    path.write_text("models:\n  hybrid:\n    alpha: 0.5\n    beta: 0.2\n    gamma: 0.3\n    output_top_k: 10\n")
    config = HybridRankingConfig.from_pipeline_config(path)
    assert (config.alpha, config.beta, config.gamma, config.output_top_k) == (0.5, 0.2, 0.3, 10)
    assert config.lambda_weight == HybridRankingConfig().lambda_weight == 0.7
    assert HybridRankingConfig().beta == 0.0


def test_pipeline_nested_operational_overrides_reach_consumers(tmp_path, monkeypatch):
    path = tmp_path / "pipeline.yaml"
    path.write_text("timeouts:\n  acceptance:\n    checkpoint_recovery_duration: 47\n  infrastructure:\n    image_tag_timeout: 123\nresources:\n  capacity_reserve:\n    cpu_cores: 9\n")
    monkeypatch.setenv("IT4931_PIPELINE_CONFIG", str(path))
    assert pipeline.get_checkpoint_recovery_duration() == 47
    assert pipeline.get_infrastructure_timeout("image_tag_timeout") == 123
    assert pipeline.get_capacity_reserve()["cpu_cores"] == 9
    monkeypatch.setenv("IT4931_CHECKPOINT_RECOVERY_DURATION", "51")
    assert pipeline.get_checkpoint_recovery_duration() == 51


@pytest.mark.parametrize("text", [
    "models: [broken", "[]", "timeouts: []", "resources: []", "models:\n  als: []",
    "timeouts:\n  steps:\n    module2: true", "execution:\n  resume_policy: unrecognized",
])
def test_explicit_pipeline_configuration_fails_closed(tmp_path, text):
    path = tmp_path / "pipeline.yaml"
    path.write_text(text)
    with pytest.raises(ValueError):
        pipeline.load_timeouts_config(path)


def test_pipeline_explicit_missing_file_is_not_replaced_with_defaults(tmp_path):
    with pytest.raises(ValueError):
        pipeline.load_timeouts_config(tmp_path / "missing.yaml")


def test_pipeline_optional_missing_default_returns_independent_builtin_values(tmp_path, monkeypatch):
    monkeypatch.delenv("IT4931_PIPELINE_CONFIG", raising=False)
    monkeypatch.setattr(pipeline, "PIPELINE_CONFIG_DEFAULT", tmp_path / "not-packaged.yaml")
    first = pipeline.load_timeouts_config()
    first["execution"]["resume_policy"] = "disabled"
    assert pipeline.load_timeouts_config()["execution"]["resume_policy"] == "enabled"


@pytest.mark.parametrize("policy", ["enabled", "strict", "disabled"])
def test_pipeline_supported_resume_policies_remain_unchanged(tmp_path, policy):
    path = tmp_path / "pipeline.yaml"
    path.write_text(f"execution:\n  resume_policy: {policy}\n")
    assert pipeline.load_timeouts_config(path)["execution"]["resume_policy"] == policy


def test_pipeline_partial_model_overrides_preserve_existing_consumer_defaults(tmp_path):
    path = tmp_path / "pipeline.yaml"
    path.write_text("models:\n  als:\n    rank: 16\n")
    models = pipeline.load_timeouts_config(path)["models"]
    assert models["als"]["rank"] == 16
    # A partial model section historically leaves defaults to each consumer.
    # Deep-merging would change the fixture runner's established trial budget.
    assert models["als"].get("max_trials", 3) == 3
    assert models["graph"]["backend"] == "graphframes"


@pytest.mark.parametrize("variable,reader", [
    ("IT4931_STEP_TIMEOUT_MODULE2", lambda: pipeline.get_step_timeout("module2")),
    ("IT4931_CHECKPOINT_RECOVERY_DURATION", pipeline.get_checkpoint_recovery_duration),
    ("IT4931_IMAGE_TAG_TIMEOUT", lambda: pipeline.get_infrastructure_timeout("image_tag_timeout")),
    ("IT4931_CAPACITY_RESERVE_CPU_CORES", pipeline.get_capacity_reserve),
])
def test_invalid_operational_environment_is_not_ignored(monkeypatch, variable, reader):
    monkeypatch.setenv(variable, "synthetic-invalid-value")
    with pytest.raises(ValueError, match=variable):
        reader()


def test_yaml_intent_is_not_silently_activated_as_runtime_defaults():
    root = Path(__file__).resolve().parents[1]
    declared = yaml.safe_load((root / "config/pipeline.yaml").read_text())
    actual = StreamConfig()
    assert (actual.window_short, actual.window_long, actual.slide, actual.trigger_interval) == (
        "30 minutes", "120 minutes", "5 minutes", "10 seconds",
    )
    assert (declared["streaming"]["window_short"], declared["streaming"]["window_long"],
            declared["streaming"]["slide"], declared["streaming"]["trigger_interval"]) == (
        "5 minutes", "30 minutes", "1 minute", "2 seconds",
    )
    assert ModelConfig().graph_top_products is None
    assert declared["models"]["graph"]["top_products"] == 20
