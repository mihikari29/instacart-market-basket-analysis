"""The consolidated Module 4 package exposes canonical implementation paths."""

from importlib.util import find_spec
from pathlib import Path

import src.module4
import src.module4.dashboard
import src.module4.hybrid
from src.module4.dashboard import api as dashboard_api
from src.module4.dashboard.config import ApiConfig
from src.module4.hybrid.service import serve_hybrid_user


def test_module4_packages_resolve_under_one_source_root():
    repository = Path(__file__).resolve().parents[1]
    assert Path(src.module4.__file__).resolve() == repository / "src/module4/__init__.py"
    assert Path(src.module4.hybrid.__file__).resolve() == repository / "src/module4/hybrid/__init__.py"
    assert Path(src.module4.dashboard.__file__).resolve() == repository / "src/module4/dashboard/__init__.py"
    assert Path(dashboard_api.__file__).resolve() == repository / "src/module4/dashboard/api.py"
    assert callable(serve_hybrid_user)
    assert dashboard_api.app is not None

    config = ApiConfig("mongodb://reader:secret@mongo.invalid:27017/test", "test", ("migration",))
    assert config.application_root == repository.resolve()


def test_removed_source_packages_are_not_importable():
    assert find_spec("src.module4c") is None
    assert find_spec("src.module4d") is None
