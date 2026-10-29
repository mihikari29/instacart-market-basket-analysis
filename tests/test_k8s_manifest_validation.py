"""Offline contracts for B3 manifest validation and its rendering consumers."""

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
from unittest.mock import MagicMock

import pytest
import yaml

from scripts.k8s import pipeline
from scripts.k8s.ownership import RunIdentity
from tests.test_k8s_pipeline import complete_b3_lock


ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def manifest_sources(tmp_path):
    destination = tmp_path / "manifests"
    shutil.copytree(ROOT / "deploy/k8s/pipeline", destination)
    return destination


def render_fixture(root, monkeypatch):
    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz):
            return datetime(2026, 10, 28, tzinfo=timezone.utc)

    monkeypatch.setattr(pipeline, "datetime", FixedDatetime)
    identity = RunIdentity("0123456789ab")
    root.mkdir(mode=0o700)
    (root / ".it4931-k8s-render-root").touch(mode=0o600)
    (root / "run").mkdir()
    (root / "run/namespace.json").write_text(json.dumps({"metadata": {"name": identity.namespace}}))
    return pipeline.render_pipeline_assets(identity, root, complete_b3_lock(), mode="fixture")


def test_manifest_validation_preserves_exact_summary_without_writes_or_processes(manifest_sources, monkeypatch):
    before = {p.name: p.read_bytes() for p in manifest_sources.iterdir()}
    launch = MagicMock(side_effect=AssertionError("validation launched a process"))
    monkeypatch.setattr(subprocess, "run", launch)
    monkeypatch.setattr(subprocess, "check_output", launch)
    assert pipeline.validate_pipeline_manifest_sources(manifest_sources) == {
        "status": "static-valid", "platform_objects": 13,
        "pipeline_claims": ["it4931-hdfs-datanode", "it4931-hdfs-namenode", "it4931-kafka-data",
                            "it4931-pipeline-checkpoints", "it4931-pipeline-data"],
        "pipeline_policies": ["allow-acceptance-api-egress", "allow-acceptance-to-api",
                              "allow-datanode-egress-to-namenode", "allow-datanode-to-namenode",
                              "allow-kafka-controller-self", "allow-pipeline-egress",
                              "allow-pipeline-to-datanode", "allow-pipeline-to-hdfs",
                              "allow-pipeline-to-kafka", "allow-pipeline-to-mongo"],
    }
    assert {p.name: p.read_bytes() for p in manifest_sources.iterdir()} == before
    launch.assert_not_called()


@pytest.mark.parametrize("name", ["platform.yaml", "network-policies.yaml", "job.yaml"])
def test_missing_manifests_preserve_diagnostic_and_filesystem_cause(manifest_sources, name):
    (manifest_sources / name).unlink()
    with pytest.raises(pipeline.PipelineError) as error:
        pipeline.validate_pipeline_manifest_sources(manifest_sources)
    assert str(error.value) == f"pipeline manifest {name} is unavailable or invalid"
    assert isinstance(error.value.__cause__, FileNotFoundError)


@pytest.mark.parametrize("name", ["platform.yaml", "network-policies.yaml", "job.yaml"])
def test_malformed_manifests_preserve_diagnostic_and_yaml_cause(manifest_sources, name):
    (manifest_sources / name).write_text("[")
    with pytest.raises(pipeline.PipelineError) as error:
        pipeline.validate_pipeline_manifest_sources(manifest_sources)
    assert str(error.value) == f"pipeline manifest {name} is unavailable or invalid"
    assert isinstance(error.value.__cause__, yaml.YAMLError)


@pytest.mark.parametrize("content,detail,cause", [
    (b"", "must contain Kubernetes objects", type(None)),
    (b"[]", "must contain Kubernetes objects", type(None)),
    (b"\xff", "is unavailable or invalid", UnicodeDecodeError),
])
def test_nonobject_and_undecodable_manifest_inputs_fail_explicitly(manifest_sources, content, detail, cause):
    (manifest_sources / "platform.yaml").write_bytes(content)
    with pytest.raises(pipeline.PipelineError) as error:
        pipeline.validate_pipeline_manifest_sources(manifest_sources)
    assert str(error.value) == "pipeline manifest platform.yaml " + detail
    assert isinstance(error.value.__cause__, cause)


@pytest.mark.parametrize("capability", ["hostNetwork", "privileged"])
def test_recursive_host_capability_checks_remain_enforced(manifest_sources, capability):
    path = manifest_sources / "job.yaml"
    document = yaml.safe_load(path.read_text())
    document["spec"]["template"]["spec"][capability] = True
    path.write_text(yaml.safe_dump(document, sort_keys=False))
    with pytest.raises(pipeline.PipelineError) as error:
        pipeline.validate_pipeline_manifest_sources(manifest_sources)
    assert str(error.value) == f"B3 resource uses forbidden host capability {capability}"


def test_policy_validation_rejects_namespace_escape_without_mutating_inputs(manifest_sources):
    path = manifest_sources / "network-policies.yaml"
    documents = list(yaml.safe_load_all(path.read_text()))
    policy = next(d for d in documents if d["metadata"]["name"] == "allow-pipeline-egress")
    policy["spec"]["egress"][0]["to"][0]["namespaceSelector"] = {}
    before = json.dumps(documents, sort_keys=True)
    with pytest.raises(pipeline.PipelineError) as error:
        pipeline.validate_pipeline_policy_inventory(documents)
    assert str(error.value) == "B3 NetworkPolicy allow-pipeline-egress may only select same-namespace Pods"
    assert json.dumps(documents, sort_keys=True) == before


@pytest.mark.parametrize("problem,detail", [
    ("marker", "B3 assets must be rendered beneath an existing private B2-owned output root"),
    ("namespace", "private B2 render namespace does not match the B3 run identity"),
])
def test_render_consumer_rejects_wrong_owner_before_claiming_pipeline_output(tmp_path, problem, detail):
    root = tmp_path / "private"
    root.mkdir(mode=0o700)
    if problem != "marker":
        (root / ".it4931-k8s-render-root").touch(mode=0o600)
        (root / "run").mkdir()
        (root / "run/namespace.json").write_text(json.dumps({"metadata": {"name": "wrong-run"}}))
    with pytest.raises(pipeline.PipelineError) as error:
        pipeline.render_pipeline_assets(RunIdentity("0123456789ab"), root, complete_b3_lock(), mode="fixture")
    assert str(error.value) == detail
    assert not (root / "pipeline").exists()


def test_rendered_assets_preserve_pre_refactor_bytes_and_permissions(tmp_path, monkeypatch):
    output = render_fixture(tmp_path / "private", monkeypatch)
    assert {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in output.iterdir()} == {
        "job-hybrid.yaml": "5b149a136a6a7912639b4d3c692eb8d0709b4c6f92f5a4ba538eb9f53d4d849f",
        "job-input.yaml": "a8d28a425d507d5111ef5adf8227f9f192be0dbfc6a3c420583d3b2e257754b7",
        "job-module1.yaml": "53680e119dce43c3245aea8590ff2ce1df2af0aa92c12cc9b241b7e93698537a",
        "job-module2.yaml": "b0583034b8a55b71d55ac52f559911f76dce04282c0a0dab4526be069545aeef",
        "job-module4.yaml": "14ce7626579bf79ddd21caaa0ddacdcb7c66602d4900f7fead8563a37467e4e6",
        "job-replay.yaml": "c4f9d936846081aaf14defd78e9476c382692f0da7bbfb4b223f2758ea29da9f",
        "network-policies.yaml": "7c360cf44ff99d3f883748f01e38df227bc9609263c677c967473166c10e35d4",
        "plan.json": "cba9d31fb2b8d38a3b544717c1ec57ca3e421a5b498c84969b3443cf561b13e6",
        "platform.yaml": "7f207b05e184f723f80fd9e14c3c12c1c37edfbc983e0c4a63bb6db5100ae189",
    }
    assert output.stat().st_mode & 0o777 == 0o700
    assert all(p.stat().st_mode & 0o777 == 0o600 for p in output.iterdir())


def test_manifest_owner_imports_independently_and_pipeline_reexports_remain_canonical():
    from scripts.k8s import common, render

    assert pipeline.PipelineError is render.PipelineError is common.PipelineError
    assert pipeline.validate_pipeline_manifest_sources is render.validate_pipeline_manifest_sources
    assert pipeline.validate_pipeline_policy_inventory is render.validate_pipeline_policy_inventory
    assert pipeline._load_yaml is render._load_yaml
    assert pipeline.PIPELINE_MANIFESTS == render.PIPELINE_MANIFESTS
    assert pipeline.PIPELINE_POLICY_NAMES is render.PIPELINE_POLICY_NAMES
    result = subprocess.run(
        [sys.executable, "-B", "-c", (
            "import sys; from scripts.k8s import render; "
            "assert 'scripts.k8s.pipeline' not in sys.modules; "
            "assert render.validate_pipeline_manifest_sources()['status']=='static-valid'"
        )], capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stderr
