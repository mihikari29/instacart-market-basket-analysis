"""Offline workflow wiring checks; no Docker daemon or external services are used."""

import fnmatch
import json
import os
from pathlib import Path
import subprocess
import sys
import textwrap

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[1]


def _workflow(name):
    # BaseLoader preserves GitHub's literal `on` key rather than YAML 1.1 bools.
    # This is a structural check, not a replacement for actionlint.
    return yaml.load((ROOT / ".github/workflows" / name).read_text(), Loader=yaml.BaseLoader)


def _shell_environment(tmp_path, failure):
    """Record Docker invocations while exercising the real demo orchestration."""
    binary_dir = tmp_path / "bin"
    binary_dir.mkdir()
    docker = binary_dir / "docker"
    docker.write_text(f"#!{sys.executable}\n" + textwrap.dedent('''\
        import json
        import os
        import sys

        args = sys.argv[1:]
        with open(os.environ["DOCKER_CALLS"], "a") as output:
            output.write(json.dumps(args) + "\\n")
        failure = os.environ["DOCKER_FAILURE"]
        if failure == "ui" and "com.codex.resource=ui-regression-tests" in args:
            sys.exit(21)
        if failure == "fixture" and args[-3:] == ["run", "--rm", "fixture"]:
            sys.exit(22)
        if failure == "browser" and "com.codex.resource=browser-e2e" in args:
            sys.exit(23)
        if failure == "down" and args[-2:] == ["down", "--remove-orphans"]:
            sys.exit(24)
    '''))
    docker.chmod(0o755)
    (binary_dir / "python3").symlink_to(sys.executable)
    environment = {
        **os.environ,
        "PATH": f"{binary_dir}{os.pathsep}{os.environ['PATH']}",
        "DOCKER_CALLS": str(tmp_path / "docker.jsonl"),
        "DOCKER_FAILURE": failure,
        "GITHUB_OUTPUT": str(tmp_path / "outputs"),
        "RUNNER_TEMP": str(tmp_path),
        "UI_TEST_IMAGE": "instacart-module4d-ui-dev:ci-700-2",
        "BROWSER_IMAGE": "instacart-module4d-demo-browser:ci-700-2",
    }
    return environment


def _run_shell(step, environment):
    return subprocess.run(
        ["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", step["run"]],
        cwd=ROOT, env=environment, text=True, capture_output=True, timeout=30,
    )


@pytest.mark.parametrize("failure", ["none", "ui", "fixture", "browser", "down"])
def test_browser_workflow_cleanup_owns_resources_and_preserves_failures(tmp_path, failure):
    job = _workflow("ci.yml")["jobs"]["module4d-demo-browser"]
    steps = job["steps"]
    prepare = next(step for step in steps if step.get("id") == "demo")
    ui = next(step for step in steps if "--target ui" in step.get("run", ""))
    browser = next(step for step in steps if "--target browser" in step.get("run", ""))
    cleanup = steps[-1]
    assert cleanup["if"] == "${{ always() && steps.demo.outcome == 'success' }}"
    assert job["needs"] == "fixtures"
    assert not any("docker compose build spark-master" in step.get("run", "") for step in steps)
    for key in ("UI_TEST_IMAGE", "BROWSER_IMAGE"):
        assert "${{ github.run_id }}-${{ github.run_attempt }}" in job["env"][key]
    for step in (browser, cleanup):
        assert step["env"] == {
            "DEMO_RUN_ID": "${{ steps.demo.outputs.run_id }}",
            "DEMO_ENV_FILE": "${{ steps.demo.outputs.env_file }}",
        }

    environment = _shell_environment(tmp_path, failure)
    prepared = _run_shell(prepare, environment)
    assert prepared.returncode == 0, prepared.stderr
    outputs = dict(line.split("=", 1) for line in Path(environment["GITHUB_OUTPUT"]).read_text().splitlines())
    environment.update(DEMO_RUN_ID=outputs["run_id"], DEMO_ENV_FILE=outputs["env_file"])
    ui_result = _run_shell(ui, environment)
    assert ui_result.returncode == (21 if failure == "ui" else 0), ui_result.stderr
    if ui_result.returncode == 0:
        browser_result = _run_shell(browser, environment)
        expected = {"fixture": 1, "browser": 23}.get(failure, 0)
        assert browser_result.returncode == expected, browser_result.stderr
    # GitHub owns cancellation scheduling; exercise the always() cleanup shell.
    cleaned = _run_shell(cleanup, environment)
    assert cleaned.returncode == (1 if failure == "down" else 0), cleaned.stderr

    calls = [json.loads(line) for line in Path(environment["DOCKER_CALLS"]).read_text().splitlines()]
    starts = [call for call in calls if call[0] == "compose" and "up" in call]
    stops = [call for call in calls if call[0] == "compose" and "down" in call]
    if starts:
        assert stops
        projects = {call[call.index("--project-name") + 1] for call in starts + stops}
        assert projects == {f"module4d-demo-{outputs['run_id']}"}
    else:
        assert failure == "ui"
    # No task-wide image discovery or broad removal can touch another run.
    assert not any(call[:2] == ["image", "ls"] for call in calls)
    removed = {call[-1] for call in calls if call[:2] == ["image", "rm"]}
    assert {environment["UI_TEST_IMAGE"], environment["BROWSER_IMAGE"]} <= removed
    assert all(
        image in {environment["UI_TEST_IMAGE"], environment["BROWSER_IMAGE"]}
        or image.startswith(("instacart-module4d-demo-api:", "instacart-module4d-demo-ui:"))
        for image in removed
    )
    private_file = Path(outputs["env_file"])
    assert private_file.exists() == (failure == "down")
    if failure == "down":
        assert private_file.stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize("name,branches,dependencies", [
    ("module2-full.yml", ["main"], [
        "config/pipeline.yaml", "pyproject.toml", "deploy/dashboard/requirements/api.txt",
        "deploy/dashboard/requirements/ui-dev.txt",
    ]),
    ("module3-full.yml", ["main", "feat/**"], [
        "src/common/runtime.py", "src/module1/clean.py", "src/module1/partition_events.py",
        "src/module3/stream.py", "scripts/compose_runner.py", "scripts/fetch_instacart.py",
        "scripts/dashboard/api_validation.py", "config/pipeline.yaml", "pyproject.toml",
        "tests/helpers.py", "tests/test_module3_checkpoint_version.py",
        "tests/test_module3_replay_completion.py", "tests/test_module3_trend_bundle.py",
        "tests/test_module3_bundle_integration.py", "tests/test_streaming_metrics_offline.py",
        "deploy/dashboard/requirements/api.txt", "deploy/dashboard/requirements/ui-dev.txt",
    ]),
])
def test_filtered_acceptance_workflows_cover_shared_dependencies(name, branches, dependencies):
    events = _workflow(name)["on"]
    assert events["push"]["branches"] == branches
    assert "workflow_dispatch" in events
    for path in dependencies:
        assert (ROOT / path).is_file()
        assert any(fnmatch.fnmatchcase(path, pattern) for pattern in events["push"]["paths"]), path
    if name == "module2-full.yml":
        assert events["schedule"] == [{"cron": "17 2 * * 0"}]


def test_workflow_shell_syntax_and_read_only_permissions():
    for path in sorted((ROOT / ".github/workflows").glob("*.yml")):
        workflow = _workflow(path.name)
        assert workflow["on"]
        assert workflow["permissions"] == {"contents": "read"}
        jobs = workflow["jobs"]
        for job in jobs.values():
            assert int(job["timeout-minutes"]) > 0
            needs = job.get("needs", [])
            if isinstance(needs, str):
                needs = [needs]
            assert all(dependency in jobs for dependency in needs)
            for step in job["steps"]:
                if "run" in step:
                    result = subprocess.run(
                        ["bash", "-n"], input=step["run"], text=True, capture_output=True,
                        timeout=10,
                    )
                    assert result.returncode == 0, (path.name, step.get("name"), result.stderr)
