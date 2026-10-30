"""Chart readiness regressions; the browser workflow owns actual Chromium runs."""

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
import sys
import types
from unittest.mock import Mock, patch

import pytest


@pytest.fixture
def browser_checks():
    # Exercise the readiness control flow without requiring Playwright in the
    # Spark runtime. Browser integration uses the real pinned Playwright image.
    package = types.ModuleType("playwright")
    api = types.ModuleType("playwright.sync_api")
    api.Page = object
    api.expect = Mock()
    api.sync_playwright = Mock()
    path = Path(__file__).resolve().parents[1] / "scripts/dashboard/browser_e2e.py"
    spec = spec_from_file_location("browser_readiness_test", path)
    module = module_from_spec(spec)
    with patch.dict(sys.modules, {"playwright": package, "playwright.sync_api": api}):
        spec.loader.exec_module(module)
    return module


def test_chart_readiness_survives_rerender_between_visibility_and_count(browser_checks, monkeypatch):
    page = Mock()
    charts = page.locator.return_value
    charts.count.side_effect = [4, 7]
    monkeypatch.setattr(browser_checks, "time", types.SimpleNamespace(monotonic=Mock(side_effect=[0, 0, 1])))

    assert browser_checks._wait_for_chart_minimum(page, 5, "evaluation", "required charts") == 7
    assert charts.nth.call_args_list[0].args == charts.nth.call_args_list[1].args == (4,)
    assert browser_checks.expect.return_value.to_be_visible.call_args_list[1].kwargs == {"timeout": 19000}


def test_chart_readiness_still_fails_if_rerender_never_restores_required_minimum(browser_checks, monkeypatch):
    page = Mock()
    charts = page.locator.return_value
    charts.count.return_value = 4
    monkeypatch.setattr(browser_checks, "time", types.SimpleNamespace(monotonic=Mock(side_effect=[0, 0, 21])))

    with pytest.raises(RuntimeError, match="minimum=5, observed=4"):
        browser_checks._wait_for_chart_minimum(page, 5, "evaluation", "required charts")
