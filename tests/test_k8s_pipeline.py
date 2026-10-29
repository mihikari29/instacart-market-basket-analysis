"""Offline B3 planner, input receipt, and manifest contract tests."""

import copy
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import replace
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

import yaml

from scripts.k8s.data_receipts import InputReceiptError, inspect_input_directory
from scripts.k8s.fixture_data import write_fixture
from scripts.k8s.ownership import OwnerReferenceIdentity, RunIdentity
from scripts.k8s.pipeline import (
    PipelineError,
    _require_pipeline_namespace_clear,
    execute_pipeline,
    finalize_pipeline_lock,
    load_pipeline_images,
    plan_pipeline,
    render_pipeline_assets,
    validate_full_data_storage_budget,
    validate_pipeline_manifest_sources,
    validate_pipeline_policy_inventory,
)
from scripts.k8s.pipeline_receipts import (
    _CLAIMS,
    _DIRECT_OBJECTS,
    PipelineImage,
    PipelineObject,
    PipelineOwnershipReceipt,
    PipelineReceiptError,
    PipelineStorage,
    pipeline_receipt_document,
    pipeline_receipt_from_document,
    validate_pipeline_receipt,
)
from scripts.k8s import pipeline_task
from tests.test_k8s_ownership import complete_lock


ROOT = Path(__file__).resolve().parents[1]


def complete_b3_lock():
    lock = complete_lock()
    for role, image in lock["images"].items():
        image.setdefault("source", f"synthetic offline lock fixture for {role}")
    component_images = {}
    for index, (role, repository, tag) in enumerate((
        ("kafka", "apache/kafka", "3.7.0"),
        ("hdfs_namenode", "bde2020/hadoop-namenode", "2.0.0-hadoop3.2.1-java8"),
        ("hdfs_datanode", "bde2020/hadoop-datanode", "2.0.0-hadoop3.2.1-java8"),
    ), start=1):
        digest = f"sha256:{index:x}" + "a" * 63
        config = f"sha256:{index:x}" + "b" * 63
        reference = f"{repository}:{tag}"
        lock["images"][role] = {"reference": f"{reference}@{digest}", "immutable_digest": digest, "source": "synthetic offline lock fixture"}
        component_images[role] = {
            "reference": reference, "immutable_digest": digest,
            "config_digest": config, "platform": "linux/amd64",
        }
    lock["images"]["pipeline_application_image"] = {
        "reference": "registry.local/instacart-pipeline-k8s:0123456789ab",
        "immutable_digest": "sha256:" + "c" * 64,
        "source": "synthetic offline lock fixture",
    }
    pipeline_image = {
        "reference": "registry.local/instacart-pipeline-k8s:0123456789ab",
        "image_digest": "sha256:" + "c" * 64,
        "config_digest": "sha256:" + "f" * 64,
        "source_revision": "1" * 40,
        "source_dirty": False,
        "source_tree_sha256": "d" * 64,
        "build_metadata_sha256": "e" * 64,
        "python_environment_sha256": "a" * 64,
        "python_version": "3.10.16",
        "platform": "linux/amd64",
    }
    return finalize_pipeline_lock(lock, component_images=component_images, pipeline_image=pipeline_image)


def valid_pipeline_receipt(identity, lock):
    objects = []
    next_uid = 0

    def uid():
        nonlocal next_uid
        next_uid += 1
        return f"uid-{next_uid:03d}"

    labels = {**identity.labels, "it4931.instacart.edu/component": "pipeline"}
    direct = {}
    job_status = {}
    for kind, name in sorted(_DIRECT_OBJECTS | {("Job", f"it4931-{step}-{identity.run_id}") for step in ("input", "module1", "module2", "module4", "replay", "hybrid")}):
        record_uid = uid()
        direct[(kind, name)] = record_uid
        objects.append(PipelineObject(kind, name, identity.namespace, record_uid, labels))
        if kind == "Job":
            job_status[name] = {"succeeded": 1, "failed": 0, "active": 0}

    pods = {}
    for workload, owner_kind, owner_name, app_name in (
        ("hdfs-namenode", "StatefulSet", "hdfs-namenode", "hdfs-namenode"),
        ("hdfs-datanode", "StatefulSet", "hdfs-datanode", "hdfs-datanode"),
        ("kafka", "StatefulSet", "kafka", "kafka"),
        *((f"it4931-{step}-{identity.run_id}", "Job", f"it4931-{step}-{identity.run_id}", "pipeline-task") for step in ("input", "module1", "module2", "module4", "replay", "hybrid")),
    ):
        pod_name = owner_name + "-0" if owner_kind == "StatefulSet" else owner_name + "-pod"
        pod_uid = uid()
        pods[owner_name] = (pod_name, pod_uid)
        pod_labels = {**labels, "app.kubernetes.io/name": app_name}
        owner = OwnerReferenceIdentity(owner_kind, owner_name, direct[(owner_kind, owner_name)], True)
        objects.append(PipelineObject("Pod", pod_name, identity.namespace, pod_uid, pod_labels, (owner,)))

    storage = []
    for index, (claim_name, requested) in enumerate(_CLAIMS.items(), start=1):
        claim_uid = direct[("PersistentVolumeClaim", claim_name)]
        storage.append(PipelineStorage(
            claim_name=claim_name, claim_uid=claim_uid, requested_storage=requested,
            capacity=requested, access_mode="ReadWriteOnce", volume_name=f"pv-{index}",
            volume_uid=f"pv-uid-{index}", claim_ref_namespace=identity.namespace,
            claim_ref_name=claim_name, claim_ref_uid=claim_uid, storage_class="standard",
            storage_class_uid=lock["storage"]["storage_class_uid"],
            provisioner=lock["storage"]["provisioner"], locality_kind="hostPath",
            locality_path=f"/tmp/hostpath-provisioner/pv-{index}", node_name="minikube",
            node_uid="node-uid-1", profile_local=True, owner_references=(),
        ))

    images = []
    for role, workload, stateful_name in (
        ("hdfs_namenode", "hdfs-namenode", "hdfs-namenode"),
        ("hdfs_datanode", "hdfs-datanode", "hdfs-datanode"),
        ("kafka", "kafka", "kafka"),
    ):
        pod_name, pod_uid = pods[stateful_name]
        container_name = {"hdfs-namenode": "namenode", "hdfs-datanode": "datanode", "kafka": "kafka"}[workload]
        images.append(PipelineImage(
            workload=workload, pod_name=pod_name, pod_uid=pod_uid, container_name=container_name,
            role=role, declared_reference=lock["images"][role]["reference"],
            runtime_image_id=f"containerd://registry.local/image@{lock['images'][role]['immutable_digest']}",
            digest=lock["images"][role]["immutable_digest"],
            config_digest=lock["pipeline_component_image_provenance"][role]["config_digest"],
        ))
    for step in ("input", "module1", "module2", "module4", "replay", "hybrid"):
        workload = f"it4931-{step}-{identity.run_id}"
        pod_name, pod_uid = pods[workload]
        image = lock["images"]["pipeline_application_image"]
        images.append(PipelineImage(
            workload=workload, pod_name=pod_name, pod_uid=pod_uid, container_name="pipeline",
            role="pipeline_application_image", declared_reference=image["reference"],
            runtime_image_id=f"containerd://registry.local/pipeline@{image['immutable_digest']}",
            digest=image["immutable_digest"],
            config_digest=lock["pipeline_image_provenance"]["config_digest"],
        ))
    return PipelineOwnershipReceipt(
        schema_version=1, profile=identity.profile, context=identity.profile,
        cluster_uid=lock["cluster"]["cluster_uid"], namespace=identity.namespace,
        namespace_uid="b2-namespace-uid", namespace_labels=identity.labels,
        objects=tuple(objects), job_status=job_status, storage=tuple(storage), images=tuple(images),
    )


class FakeExecutor:
    def __init__(self):
        self.calls = []

    def run(self, argv, *, timeout, input_text=None):
        self.calls.append(list(argv))
        raise AssertionError("B3 execution must remain inert before explicit authorization")


class InventoryExecutor:
    def __init__(self, inventories):
        self.inventories = list(inventories)
        self.calls = []

    def run(self, argv, *, timeout, input_text=None):
        self.calls.append(list(argv))
        return type("Result", (), {"returncode": 0, "stdout": json.dumps(self.inventories.pop(0))})()


class PipelinePlannerTests(unittest.TestCase):
    def setUp(self):
        self.identity = RunIdentity("0123456789ab")

    def test_fixture_plan_is_bounded_and_dependency_order_is_stable(self):
        plan = plan_pipeline(self.identity, mode="fixture")
        self.assertEqual(
            [step.name for step in plan.steps],
            ["input", "module1", "module2", "module4", "replay", "hybrid"],
        )
        self.assertEqual((plan.fixture_max_users, plan.fixture_max_events), (50, 5000))
        self.assertEqual(plan.topic, f"instacart-purchase-events-{self.identity.run_id}")
        self.assertIn("Spark local[2]", plan.execution_model)
        self.assertEqual(plan.steps[0].backoff_limit, 0)
        self.assertEqual(plan.steps[1].backoff_limit, 0)
        self.assertEqual(plan.steps[4].backoff_limit, 0)
        self.assertIn("checkpoint", plan.steps[4].retry_semantics)

    def test_full_plan_requires_hash_and_host_reserve(self):
        capacity = {"cpu_cores": 6, "memory_gib": 12, "free_disk_gib": 40}
        digest = "a" * 64
        plan = plan_pipeline(self.identity, mode="full", host_capacity=capacity, source_receipt_sha256=digest)
        self.assertIsNone(plan.fixture_max_events)
        self.assertEqual(plan.run_id, self.identity.run_id)
        with self.assertRaisesRegex(PipelineError, "SHA-256"):
            plan_pipeline(self.identity, mode="full", host_capacity=capacity)
        with self.assertRaisesRegex(PipelineError, "memory"):
            plan_pipeline(
                self.identity, mode="full", host_capacity={**capacity, "memory_gib": 11.99},
                source_receipt_sha256=digest,
            )

    def test_static_manifests_use_only_isolated_storage_and_narrow_pipeline_policies(self):
        result = validate_pipeline_manifest_sources()
        self.assertEqual(result["status"], "static-valid")
        self.assertEqual(len(result["pipeline_claims"]), 5)
        docs = list(yaml.safe_load_all((ROOT / "deploy/k8s/pipeline/network-policies.yaml").read_text()))
        policies = [doc for doc in docs if doc]
        invalid = copy.deepcopy(policies)
        invalid[0]["spec"]["egress"][0].pop("ports")
        with self.assertRaisesRegex(PipelineError, "broad"):
            validate_pipeline_policy_inventory(invalid)
        invalid = copy.deepcopy(policies)
        invalid[0]["spec"]["egress"][0]["ports"][0]["port"] = 9
        with self.assertRaisesRegex(PipelineError, "exact approved service flow"):
            validate_pipeline_policy_inventory(invalid)

    def test_pipeline_service_ports_match_hdfs_and_kraft_contracts(self):
        documents = list(yaml.safe_load_all((ROOT / "deploy/k8s/pipeline/platform.yaml").read_text()))
        services = {item["metadata"]["name"]: item for item in documents if item and item["kind"] == "Service"}
        datanode = {item["name"]: item["targetPort"] for item in services["datanode"]["spec"]["ports"]}
        kafka = {item["name"]: item["port"] for item in services["kafka"]["spec"]["ports"]}
        self.assertEqual(datanode, {"transfer": 9866, "web": 9864})
        self.assertEqual(kafka, {"broker": 29092, "controller": 9093})

    def test_kafka_environment_is_validated_after_yaml_parsing(self):
        documents = list(yaml.safe_load_all((ROOT / "deploy/k8s/pipeline/platform.yaml").read_text()))
        kafka = next(item for item in documents if item and item.get("kind") == "StatefulSet" and item["metadata"]["name"] == "kafka")
        environment = kafka["spec"]["template"]["spec"]["containers"][0]["env"]
        values = {item["name"]: item["value"] for item in environment}
        self.assertEqual(values["KAFKA_PROCESS_ROLES"], "controller,broker")
        self.assertEqual(values["KAFKA_LISTENERS"], "INTERNAL://:29092,CONTROLLER://:9093")
        self.assertTrue(all(isinstance(item["value"], str) for item in environment))

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name in ("network-policies.yaml", "job.yaml"):
                (root / name).write_text((ROOT / "deploy/k8s/pipeline" / name).read_text())
            kafka_env = next(item for item in environment if item["name"] == "KAFKA_LISTENERS")
            kafka_env["value"] = ["INTERNAL://:29092", "CONTROLLER://:9093"]
            (root / "platform.yaml").write_text(yaml.safe_dump_all(documents, sort_keys=False))
            with self.assertRaisesRegex(PipelineError, "Kafka environment"):
                validate_pipeline_manifest_sources(root)

    def test_deterministic_fixture_matches_exact_six_file_input_contract(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary) / "input"
            info = write_fixture(directory)
            receipt = inspect_input_directory(directory)
            self.assertEqual(info["users"], 12)
            self.assertEqual(receipt["file_count"], 6)
            self.assertEqual(receipt["files"]["orders.csv"]["rows"], 48)
            self.assertEqual(receipt["files"]["order_products__prior.csv"]["rows"], 72)
            self.assertEqual(len(receipt["manifest_sha256"]), 64)
            self.assertEqual(write_fixture(directory), info)

    def test_input_receipt_rejects_extra_file_and_wrong_header(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary) / "input"
            write_fixture(directory)
            (directory / "extra.csv").write_text("unused\n", encoding="utf-8")
            with self.assertRaisesRegex(InputReceiptError, "exactly six"):
                inspect_input_directory(directory)
            (directory / "extra.csv").unlink()
            (directory / "orders.csv").write_text("order_id,user_id\n1,1\n", encoding="utf-8")
            with self.assertRaisesRegex(InputReceiptError, "header"):
                inspect_input_directory(directory)

    def test_fixture_input_task_writes_a_run_scoped_receipt_without_external_calls(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            env = {
                "PIPELINE_RUN_ID": self.identity.run_id,
                "PIPELINE_MODE": "fixture",
                "PIPELINE_HDFS_ROOT": f"/instacart/{self.identity.run_id}",
                "PIPELINE_HDFS_URI": f"hdfs://namenode:8020/instacart/{self.identity.run_id}",
                "PIPELINE_TOPIC": f"instacart-purchase-events-{self.identity.run_id}",
                "PIPELINE_KAFKA_BOOTSTRAP": "kafka:29092",
            }
            with patch.dict(os.environ, env), patch.object(pipeline_task, "RUNTIME_ROOT", root / "runtime"), patch.object(pipeline_task, "INPUT_ROOT", root / "runtime" / "input"), redirect_stdout(io.StringIO()):
                receipt = pipeline_task.step_input()
            self.assertEqual(receipt["status"], "completed")
            self.assertEqual(receipt["fixture_manifest"]["scope"], "synthetic_fixture")
            saved = json.loads((root / "runtime/evidence/source-receipt.json").read_text())
            self.assertEqual(saved["run_id"], self.identity.run_id)
            self.assertEqual(saved["scope"], "synthetic_fixture")
            self.assertEqual(inspect_input_directory(root / "runtime/input")["manifest_sha256"], saved["manifest_sha256"])

    def test_module1_task_uses_input_root_without_creating_raw_raw(self):
        with tempfile.TemporaryDirectory() as temporary:
            runtime_root = Path(temporary) / "runtime"
            input_root = runtime_root / "input"
            write_fixture(input_root)
            source = inspect_input_directory(input_root)
            (runtime_root / "evidence").mkdir(parents=True)
            (runtime_root / "evidence" / "source-receipt.json").write_text(json.dumps({
                "run_id": self.identity.run_id,
                "scope": "synthetic_fixture",
                "manifest_sha256": source["manifest_sha256"],
                "fixture": {"scope": "synthetic_fixture", "users": 12},
            }), encoding="utf-8")
            base_env = {
                "PIPELINE_RUN_ID": self.identity.run_id,
                "PIPELINE_MODE": "fixture",
                "PIPELINE_HDFS_ROOT": f"/instacart/{self.identity.run_id}",
                "PIPELINE_HDFS_URI": f"hdfs://namenode:8020/instacart/{self.identity.run_id}",
                "PIPELINE_TOPIC": f"instacart-purchase-events-{self.identity.run_id}",
                "PIPELINE_KAFKA_BOOTSTRAP": "kafka:29092",
                "POD_NAME": "module1-offline",
            }
            calls = []

            def fake_subprocess(command, *, env, log_path, timeout):
                calls.append((list(command), dict(env)))
                if "src.module1.generate" in command:
                    feed = Path(command[command.index("--out-dir") + 1])
                    feed.mkdir(parents=True)
                    (feed / "events.parquet").write_bytes(b"offline synthetic parquet placeholder")

            pyarrow = types.ModuleType("pyarrow")
            parquet = types.ModuleType("pyarrow.parquet")
            parquet.ParquetFile = lambda _path: types.SimpleNamespace(metadata=types.SimpleNamespace(num_rows=24))
            pyarrow.parquet = parquet
            with (
                patch.dict(os.environ, base_env, clear=True),
                patch.object(pipeline_task, "RUNTIME_ROOT", runtime_root),
                patch.object(pipeline_task, "INPUT_ROOT", input_root),
                patch.object(pipeline_task, "_subprocess", side_effect=fake_subprocess),
                patch.dict("sys.modules", {"pyarrow": pyarrow, "pyarrow.parquet": parquet}),
            ):
                receipt = pipeline_task.step_module1()
            clean = calls[0][0]
            self.assertEqual(clean[clean.index("--data-dir") + 1], str(runtime_root / "work" / "module1-offline"))
            self.assertEqual(clean[clean.index("--raw-dir") + 1], str(input_root))
            stage = calls[2][0]
            self.assertEqual(stage[stage.index("--raw-dir") + 1], str(input_root))
            self.assertEqual(stage[stage.index("--user") + 1], "k8spipeline")
            self.assertFalse((runtime_root / "work" / "module1-offline" / "raw").exists())
            self.assertEqual(receipt["status"], "completed")

    def test_module4_task_passes_run_scoped_database_to_publisher(self):
        environment = {
            "PIPELINE_RUN_ID": self.identity.run_id,
            "PIPELINE_MODE": "fixture",
            "PIPELINE_HDFS_ROOT": f"/instacart/{self.identity.run_id}",
            "PIPELINE_HDFS_URI": f"hdfs://namenode:8020/instacart/{self.identity.run_id}",
            "PIPELINE_TOPIC": f"instacart-purchase-events-{self.identity.run_id}",
            "PIPELINE_KAFKA_BOOTSTRAP": "kafka:29092",
            "MODULE4D_MONGO_DATABASE": f"module4d_demo_{self.identity.run_id}",
            "MONGODB_URI": "mongodb://publisher:secret@module4d-mongo:27017/admin",
        }
        calls = []
        with (
            patch.dict(os.environ, environment, clear=True),
            patch.object(pipeline_task, "RUNTIME_ROOT", Path("/tmp/offline-pipeline-test")),
            patch.object(pipeline_task, "_subprocess", side_effect=lambda command, **kwargs: calls.append((command, kwargs))),
            patch.object(pipeline_task, "_artifact_inventory", return_value=[]),
        ):
            pipeline_task.step_module4()
        self.assertEqual(calls[0][1]["env"]["MODULE4_MONGO_DATABASE"], f"module4d_demo_{self.identity.run_id}")
        self.assertEqual(calls[0][1]["env"]["MODULE4D_MONGO_DATABASE"], f"module4d_demo_{self.identity.run_id}")

    def test_task_cli_persists_success_receipt_and_redacts_unexpected_exception(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch.object(pipeline_task, "STEPS", {"offline": lambda: {"ok": True}}), patch.object(pipeline_task, "RUNTIME_ROOT", root), redirect_stdout(io.StringIO()):
                self.assertEqual(pipeline_task.main(["offline"]), 0)
            saved = json.loads((root / "evidence/steps/offline.json").read_text())
            self.assertEqual(saved["status"], "completed")

            def sensitive_failure():
                raise RuntimeError("mongodb://user:plaintext-password@example.invalid/database")

            error_output = io.StringIO()
            with patch.object(pipeline_task, "STEPS", {"offline": sensitive_failure}), patch.object(pipeline_task, "RUNTIME_ROOT", root), redirect_stderr(error_output):
                self.assertEqual(pipeline_task.main(["offline"]), 1)
            self.assertNotIn("plaintext-password", error_output.getvalue())
            self.assertIn("sensitive details redacted", error_output.getvalue())

    def test_subprocess_preserves_redacted_output_and_failure_status(self):
        expected = "status=ready token=<redacted>\nmongodb://<redacted>\n"
        for status in (0, 7):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as temporary:
                log = Path(temporary) / "step.log"
                captured = io.StringIO()
                command = [sys.executable, "-c", (
                    "import sys; print('status=ready token=synthetic-token'); "
                    "print('mongodb://user:synthetic-password@mongo.invalid/db'); "
                    f"sys.exit({status})"
                )]
                with patch.object(pipeline_task, "WORKSPACE", Path(temporary)), redirect_stdout(captured):
                    if status:
                        with self.assertRaisesRegex(pipeline_task.TaskError, "exited with status 7"):
                            pipeline_task._subprocess(command, env=dict(os.environ), log_path=log, timeout=10)
                    else:
                        pipeline_task._subprocess(command, env=dict(os.environ), log_path=log, timeout=10)
                self.assertEqual(log.read_text(encoding="utf-8"), expected)
                self.assertEqual(captured.getvalue(), expected)

    def test_rendered_fixture_assets_pin_images_and_isolate_checkpoints(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "private-render"
            root.mkdir(mode=0o700)
            (root / ".it4931-k8s-render-root").touch(mode=0o600)
            (root / "run").mkdir()
            (root / "run" / "namespace.json").write_text(
                json.dumps({"metadata": {"name": self.identity.namespace}}), encoding="utf-8"
            )
            output = render_pipeline_assets(self.identity, root, complete_b3_lock(), mode="fixture")
            plan = json.loads((output / "plan.json").read_text())
            self.assertEqual(len(plan["steps"]), 6)
            self.assertTrue(plan["as_of_event_time"].endswith("Z"))
            self.assertIs(type(plan["as_of_event_time"]), str)
            self.assertEqual((plan["fixture_max_users"], plan["fixture_max_events"]), (50, 5000))
            jobs = {step: yaml.safe_load((output / f"job-{step}.yaml").read_text()) for step in ("input", "module1", "module2", "module4", "replay", "hybrid")}
            for step, job in jobs.items():
                container = job["spec"]["template"]["spec"]["containers"][0]
                self.assertIn("@sha256:", container["image"])
                self.assertIs(type(job["spec"]["backoffLimit"]), int)
                self.assertIs(type(job["spec"]["activeDeadlineSeconds"]), int)
                self.assertEqual("MONGODB_URI" in {env["name"] for env in container.get("env", [])}, step not in {"input", "module1"})
                checkpoint_mount = any(mount["name"] == "pipeline-checkpoints" for mount in container["volumeMounts"])
                self.assertEqual(checkpoint_mount, step == "replay")

    def test_full_data_pvc_budget_bounds_peak_working_set(self):
        self.assertEqual(validate_full_data_storage_budget(4 * 1024**3), 14 * 1024**3)
        with self.assertRaisesRegex(PipelineError, "three working-set copies"):
            validate_full_data_storage_budget(4 * 1024**3 + 1)

    def test_pipeline_image_loader_verifies_all_four_locked_identities(self):
        lock = complete_b3_lock()
        expected_images = []
        for role in ("pipeline_application_image", "kafka", "hdfs_namenode", "hdfs_datanode"):
            image = lock["images"][role]
            provenance = lock.get("pipeline_image_provenance", {}) if role == "pipeline_application_image" else lock["pipeline_component_image_provenance"][role]
            expected_images.append({
                "repoTags": [image["reference"].split("@", 1)[0]],
                "repoDigests": [f"{image['reference'].split('@', 1)[0]}@{image['immutable_digest']}"],
                "id": provenance["config_digest"],
            })

        class LoaderExecutor:
            def __init__(self):
                self.calls = []

            def run(self, argv, *, timeout, input_text=None):
                self.calls.append((list(argv), timeout))
                stdout = json.dumps({"images": expected_images}) if argv[-1:] == ["json"] else ""
                return type("Result", (), {"returncode": 0, "stdout": stdout})()

        executor = LoaderExecutor()
        loaded = load_pipeline_images(executor, self.identity, lock, execute=True)
        self.assertEqual(set(loaded), {"pipeline_application_image", "kafka", "hdfs_namenode", "hdfs_datanode"})
        self.assertEqual(len(executor.calls), 9)
        self.assertTrue(all(call[0][:4] == ["minikube", "image", "load", "-p"] for call in executor.calls[:4]))
        self.assertEqual(executor.calls[-1][0][:3], ["minikube", "-p", self.identity.profile])

    def test_pipeline_executes_verified_image_load_before_workload_apply_without_api_restart(self):
        lock = complete_b3_lock()
        images = []
        for role in ("pipeline_application_image", "kafka", "hdfs_namenode", "hdfs_datanode"):
            item = lock["images"][role]
            provenance = lock.get("pipeline_image_provenance", {}) if role == "pipeline_application_image" else lock["pipeline_component_image_provenance"][role]
            images.append({
                "repoTags": [item["reference"].split("@", 1)[0]],
                "repoDigests": [f"{item['reference'].split('@', 1)[0]}@{item['immutable_digest']}"],
                "id": provenance["config_digest"],
            })

        class FullPathExecutor:
            def __init__(self):
                self.calls = []

            def run(self, argv, *, timeout, input_text=None):
                self.calls.append(list(argv))
                stdout = json.dumps({"images": images}) if argv[-1:] == ["json"] else ""
                return type("Result", (), {"returncode": 0, "stdout": stdout})()

        executor = FullPathExecutor()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "private-render"
            root.mkdir(mode=0o700)
            (root / ".it4931-k8s-render-root").touch(mode=0o600)
            (root / "run").mkdir()
            (root / "run" / "namespace.json").write_text(
                json.dumps({"metadata": {"name": self.identity.namespace}}), encoding="utf-8"
            )
            pipeline_dir = render_pipeline_assets(
                self.identity, root, lock, mode="fixture", host_capacity=lock["host_capacity"]
            )
            b2_path = root / "b2-ownership-receipt.json"
            b2_path.write_text("{}", encoding="utf-8")
            valid_receipt = valid_pipeline_receipt(self.identity, lock)
            with (
                patch("scripts.k8s.receipts.receipt_from_document", return_value=object()),
                patch("scripts.k8s.receipts.validate_collected_receipt"),
                patch("scripts.k8s.receipts.collect_runtime_ownership_receipt", return_value=object()),
                patch("scripts.k8s.receipts.assert_receipt_matches_lock_and_live"),
                patch("scripts.k8s.network_policy.collect_network_policy_inventory", return_value={}),
                patch("scripts.k8s.pipeline._require_pipeline_namespace_clear"),
                patch("scripts.k8s.pipeline_receipts.collect_pipeline_receipt", return_value=valid_receipt),
            ):
                result = execute_pipeline(
                    executor, self.identity, pipeline_dir, lock=lock,
                    serving_receipt_path=b2_path, host_capacity=lock["host_capacity"], execute=True,
                )
        first_apply = next(i for i, call in enumerate(executor.calls) if call[:2] == ["kubectl", "--context"] and "apply" in call)
        first_image_load = next(i for i, call in enumerate(executor.calls) if call[:3] == ["minikube", "image", "load"])
        self.assertLess(first_image_load, first_apply)
        bootstrap = next(i for i, call in enumerate(executor.calls) if "-chown" in call)
        first_job = next(i for i, call in enumerate(executor.calls) if any("job-input.yaml" in part for part in call))
        self.assertLess(bootstrap, first_job)
        self.assertTrue(result["status"] == "completed")
        self.assertEqual(result["completed_steps"], ["platform", "input", "module1", "module2", "module4", "replay", "hybrid"])
        self.assertFalse(any("rollout" in call and "restart" in call for call in executor.calls))
        self.assertFalse(any("api-config.yaml" in call for call in executor.calls))

    def test_renderer_refuses_incomplete_lock_without_claiming_output(self):
        template = yaml.safe_load((ROOT / "deploy/k8s/versions.lock.yaml").read_text(encoding="utf-8"))
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "private-render"
            root.mkdir(mode=0o700)
            (root / ".it4931-k8s-render-root").touch(mode=0o600)
            (root / "run").mkdir()
            (root / "run" / "namespace.json").write_text(
                json.dumps({"metadata": {"name": self.identity.namespace}}), encoding="utf-8"
            )
            with self.assertRaisesRegex(PipelineError, "complete B2"):
                render_pipeline_assets(self.identity, root, template, mode="fixture")
            self.assertFalse((root / "pipeline").exists())

    def test_execution_requires_explicit_opt_in_without_any_command(self):
        executor = FakeExecutor()
        with self.assertRaisesRegex(PipelineError, "explicit execute=True"):
            execute_pipeline(
                executor, self.identity, "/tmp/not-created", lock={},
                serving_receipt_path="/tmp/not-created/b2-ownership-receipt.json",
            )
        self.assertEqual(executor.calls, [])

    def test_namespace_preflight_is_read_only_and_refuses_existing_pipeline_names(self):
        empty = {"items": []}
        executor = InventoryExecutor([empty.copy() for _ in range(8)])
        _require_pipeline_namespace_clear(executor, self.identity)
        self.assertEqual(len(executor.calls), 8)
        self.assertTrue(all(call[:3] == ["kubectl", "--context", self.identity.profile] for call in executor.calls))

        collision = {"items": [{"kind": "PersistentVolumeClaim", "metadata": {"name": "it4931-pipeline-data", "namespace": self.identity.namespace}}]}
        executor = InventoryExecutor([collision, *[empty.copy() for _ in range(7)]])
        with self.assertRaisesRegex(PipelineError, "reserved resource names"):
            _require_pipeline_namespace_clear(executor, self.identity)

    def test_pipeline_receipt_round_trip_binds_jobs_storage_and_images(self):
        lock = complete_b3_lock()
        receipt = valid_pipeline_receipt(self.identity, lock)
        validate_pipeline_receipt(self.identity, receipt, lock)
        decoded = pipeline_receipt_from_document(pipeline_receipt_document(receipt))
        self.assertEqual(decoded, receipt)

        changed = replace(receipt, cluster_uid="other-cluster")
        with self.assertRaisesRegex(PipelineReceiptError, "cluster UID"):
            validate_pipeline_receipt(self.identity, changed, lock)
        changed_storage = replace(receipt.storage[0], claim_uid="other-claim")
        changed = replace(receipt, storage=(changed_storage, *receipt.storage[1:]))
        with self.assertRaisesRegex(PipelineReceiptError, "does not match the lock"):
            validate_pipeline_receipt(self.identity, changed, lock)


if __name__ == "__main__":
    unittest.main()
