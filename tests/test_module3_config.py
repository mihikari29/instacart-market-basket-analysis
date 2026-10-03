import pytest

from src.module3.config import Config


def test_fail_on_data_loss_is_strict_by_default():
    assert Config().fail_on_data_loss is True


def test_fail_on_data_loss_environment_override(monkeypatch):
    monkeypatch.setenv("MODULE3_FAIL_ON_DATA_LOSS", "false")
    assert Config.environment().fail_on_data_loss is False


def test_fail_on_data_loss_environment_rejects_invalid_value(monkeypatch):
    monkeypatch.setenv("MODULE3_FAIL_ON_DATA_LOSS", "sometimes")
    with pytest.raises(ValueError, match="Boolean value"):
        Config.environment()


def test_fail_on_data_loss_config_rejects_non_boolean():
    with pytest.raises(ValueError, match="must be a boolean"):
        Config(fail_on_data_loss="false")
