"""Check actual Docker COPY source closure without Docker or source mounts."""

import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("target,imports", [
    ("ui", "import src.module4.dashboard.ui.api_client; import src.module4.dashboard.ui.pages.overview; import src.module4.dashboard.ui.pages.recommendations"),
    ("k8s-runner", "from src.module4.dashboard.api import create_app; import scripts.k8s.api_readiness; create_app().openapi()"),
])
def test_packaged_python_sources_import_without_repository_mount(tmp_path, target, imports):
    stage = None
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    for line in (ROOT / "Dockerfile").read_text().splitlines():
        if not line.startswith(("FROM ", "COPY ")):
            continue
        words = shlex.split(line, comments=True)
        if not words:
            continue
        if words[0] == "FROM":
            stage = words[-1]
        if stage != target or words[0] != "COPY":
            continue
        destination = words[-1]
        for source in words[1:-1]:
            if not source.startswith(("src/", "scripts/")):
                continue
            source_path = ROOT / source
            relative = destination.removeprefix("/workspace/").removeprefix("./")
            output = workspace / relative
            if source_path.is_dir():
                shutil.copytree(source_path, output, dirs_exist_ok=True, ignore=shutil.ignore_patterns("__pycache__"))
            else:
                if destination.endswith("/"):
                    output = output / source_path.name
                output.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source_path, output)
    environment = dict(os.environ)
    paths = [entry for entry in environment.get("PYTHONPATH", "").split(os.pathsep)
             if entry and not Path(entry).resolve().is_relative_to(ROOT)]
    environment["PYTHONPATH"] = os.pathsep.join([str(workspace), *paths])
    result = subprocess.run([sys.executable, "-B", "-c", imports], cwd=workspace,
                            env=environment, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
