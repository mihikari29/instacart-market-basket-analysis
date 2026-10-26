"""Exercise main and the real smoke handlers while stopping before side effects."""

import argparse
from pathlib import Path
from unittest.mock import Mock

import pytest

from scripts import module3_smoke as smoke


class OutputBoundaryReached(Exception):
    """Stop a real handler before it creates output or starts infrastructure."""


@pytest.fixture(autouse=True)
def block_infrastructure(monkeypatch):
    monkeypatch.setattr(smoke, "run_command", Mock(side_effect=AssertionError("unexpected command")))
    monkeypatch.setattr(smoke, "compose_python", Mock(side_effect=AssertionError("unexpected Compose")))
    monkeypatch.setattr(smoke, "archive_checkpoint", Mock(side_effect=AssertionError("unexpected archive")))
    monkeypatch.setattr(smoke, "ensure_output", Mock(side_effect=OutputBoundaryReached))


@pytest.mark.parametrize(
    "argv, handler_name, expected",
    [
        (
            ["live"], "run_live",
            dict(scenario="live", events=50_000, duration_seconds=120,
                 feed=Path("data/synthesized/module3_dev"), output=Path("results/module3/smoke"), top_k=20),
        ),
        (
            ["live", "--events", "7", "--duration-seconds", "20", "--feed", "custom/feed",
             "--output", "custom/live", "--top-k", "3"], "run_live",
            dict(scenario="live", events=7, duration_seconds=20,
                 feed=Path("custom/feed"), output=Path("custom/live"), top_k=3),
        ),
        (
            ["recovery"], "run_recovery",
            dict(scenario="recovery", duration_seconds=25, output=Path("results/module3/recovery")),
        ),
        (
            ["recovery", "--duration-seconds", "15", "--output", "custom/recovery"], "run_recovery",
            dict(scenario="recovery", duration_seconds=15, output=Path("custom/recovery")),
        ),
    ],
    ids=["live-defaults", "live-explicit", "recovery-defaults", "recovery-explicit"],
)
def test_main_dispatches_to_real_handler_once(monkeypatch, argv, handler_name, expected):
    handler = Mock(wraps=vars(smoke)[handler_name])
    monkeypatch.setattr(smoke, handler_name, handler)
    parse_calls = []
    parse_args = argparse.ArgumentParser.parse_args

    def record_parse(parser, args=None, namespace=None):
        parse_calls.append(args)
        return parse_args(parser, args, namespace)

    monkeypatch.setattr(argparse.ArgumentParser, "parse_args", record_parse)

    with pytest.raises(OutputBoundaryReached):
        smoke.main(argv)

    handler.assert_called_once()
    parsed = handler.call_args.args[0]
    assert isinstance(parsed, argparse.Namespace)
    assert vars(parsed) == expected
    assert parse_calls == [argv]
    smoke.ensure_output.assert_called_once_with((smoke.ROOT / expected["output"]).resolve())
    smoke.run_command.assert_not_called()
    smoke.compose_python.assert_not_called()
    smoke.archive_checkpoint.assert_not_called()


@pytest.mark.parametrize(
    "argv, message",
    [
        (["live", "--events", "0"], "Smoke requires positive events and at least 20 seconds"),
        (["live", "--events", "-1"], "Smoke requires positive events and at least 20 seconds"),
        (["live", "--duration-seconds", "19"], "Smoke requires positive events and at least 20 seconds"),
        (["live", "--duration-seconds", "0"], "Smoke requires positive events and at least 20 seconds"),
        (["live", "--duration-seconds", "-1"], "Smoke requires positive events and at least 20 seconds"),
        (["recovery", "--duration-seconds", "14"], "Recovery runs require at least 15 seconds"),
        (["recovery", "--duration-seconds", "0"], "Recovery runs require at least 15 seconds"),
        (["recovery", "--duration-seconds", "-1"], "Recovery runs require at least 15 seconds"),
    ],
)
def test_scenario_validation_precedes_side_effects(argv, message):
    with pytest.raises(ValueError) as caught:
        smoke.main(argv)

    assert str(caught.value) == message
    smoke.ensure_output.assert_not_called()
    smoke.run_command.assert_not_called()


@pytest.mark.parametrize(
    "argv",
    [
        [], ["unknown"], ["live", "--unknown"], ["recovery", "--events", "1"],
        ["live", "--events", "invalid"], ["live", "--duration-seconds", "invalid"],
        ["recovery", "--duration-seconds", "invalid"], ["live", "--top-k", "invalid"],
    ],
)
def test_parser_rejects_unsupported_or_malformed_arguments(argv):
    with pytest.raises(SystemExit) as caught:
        smoke.main(argv)

    assert caught.value.code == 2
    smoke.ensure_output.assert_not_called()
    smoke.run_command.assert_not_called()
