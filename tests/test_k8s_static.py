from pathlib import Path
import json
import tempfile
import unittest

from scripts.k8s.render import render_overlay


ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "deploy" / "k8s" / "base"


class StaticFoundationTests(unittest.TestCase):
    def test_render_is_per_run_private_and_never_overwrites(self) -> None:
        with tempfile.TemporaryDirectory(prefix="it4931-k8s-render-test-") as temp:
            output = Path(temp) / "first-render"
            run_dir = render_overlay("0123456789ab", output)
            overlay = json.loads((run_dir / "kustomization.yaml").read_text(encoding="utf-8"))
            namespace = json.loads((run_dir / "namespace.json").read_text(encoding="utf-8"))
            api_patch = json.loads((run_dir / "api-config-patch.json").read_text(encoding="utf-8"))
            self.assertEqual(overlay["namespace"], "it4931-k8s-0123456789ab")
            self.assertEqual(namespace["metadata"]["labels"]["it4931.instacart.edu/run-id"], "0123456789ab")
            self.assertEqual(api_patch["data"]["MODULE4D_MONGO_DATABASE"], "module4d_demo_0123456789ab")
            with self.assertRaises(FileExistsError):
                render_overlay("0123456789ab", output)

    def test_base_defines_only_internal_serving_and_no_live_pipeline_workloads(self) -> None:
        contents = "\n".join(path.read_text(encoding="utf-8") for path in BASE.glob("*.yaml"))
        self.assertIn("type: ClusterIP", contents)
        self.assertIn("module4d-api", contents)
        self.assertIn("module4d-mongo", contents)
        self.assertIn("module4d-ui", contents)
        self.assertNotIn("kind: Job", contents)
        self.assertNotIn("kind: CronJob", contents)
        self.assertRegex(contents, r"(?m)^\s*kind: (?:StatefulSet|Deployment)$")
        self.assertEqual(contents.count("namespace: __K8S_RENDER_REQUIRED__"), 20)
        self.assertFalse((BASE / "kustomization.yaml").exists())

    def test_service_account_and_secret_boundaries_are_explicit(self) -> None:
        mongo = (BASE / "mongo.yaml").read_text(encoding="utf-8")
        serving = (BASE / "serving.yaml").read_text(encoding="utf-8")
        accounts = (BASE / "serviceaccounts.yaml").read_text(encoding="utf-8")
        self.assertEqual(accounts.count("automountServiceAccountToken: false"), 3)
        self.assertIn("name: module4d-mongo-admin", mongo)
        self.assertIn("name: module4d-api-readonly", serving)
        self.assertIn("secretKeyRef:", mongo)
        self.assertIn("secretKeyRef:", serving)
        ui_section = serving.split("name: module4d-ui\n", 1)[1]
        self.assertNotIn("secretKeyRef:", ui_section)
        self.assertNotIn("MONGO_INITDB_ROOT_PASSWORD:", mongo)
        self.assertNotIn("password: ", mongo)

    def test_network_and_storage_guards_are_declared(self) -> None:
        network = (BASE / "network-policies.yaml").read_text(encoding="utf-8")
        mongo = (BASE / "mongo.yaml").read_text(encoding="utf-8")
        contents = "\n".join(path.read_text() for path in BASE.glob("*.yaml"))
        self.assertIn("name: default-deny", network)
        self.assertIn("name: allow-api-to-mongo", network)
        self.assertIn("name: allow-ui-to-api", network)
        self.assertIn("port: 27017", network)
        self.assertIn("port: 8000", network)
        self.assertNotIn("type: NodePort", contents)
        self.assertNotIn("hostPath:", mongo)
        self.assertNotIn("hostNetwork:", mongo)
        self.assertIn("storage: 2Gi", mongo)
        self.assertIn("@sha256:", mongo)

    def test_api_health_is_not_used_as_database_readiness(self) -> None:
        serving = (BASE / "serving.yaml").read_text(encoding="utf-8")
        self.assertIn("path: /health", serving)
        self.assertIn("command: [python3, /workspace/scripts/k8s/api_readiness.py]", serving)
        readiness = (ROOT / "scripts/k8s/api_readiness.py").read_text(encoding="utf-8")
        self.assertIn("list_indexes()", readiness)
        self.assertIn("except Exception:", readiness)
        self.assertIn("config.mongodb_uri", readiness)
        self.assertNotIn("print(exc", readiness)

    def test_docker_runner_preserves_source_digest_inputs(self) -> None:
        dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
        ignore = (ROOT / ".dockerignore").read_text(encoding="utf-8")
        self.assertIn("FROM spark AS k8s-runner", dockerfile)
        self.assertIn('CMD ["python3", "-c", "print(', dockerfile)
        for source_path in (
            "scripts/module4.py",
            "scripts/module4_runtime_smoke.py",
            "scripts/module4_flow_smoke.py",
            "scripts/module4_cross_module_smoke.py",
            "compose.yaml",
        ):
            with self.subTest(source_path=source_path):
                self.assertIn(f"COPY {source_path} ", dockerfile)
                self.assertIn(f"!{source_path}", ignore)
        self.assertIn("**/.env", ignore)
        self.assertIn("**/secrets/**", ignore)
        self.assertNotIn("COPY data/", dockerfile)

    def test_version_lock_is_deliberately_incomplete_until_runtime_evidence_exists(self) -> None:
        lock = (ROOT / "deploy/k8s/versions.lock.yaml").read_text(encoding="utf-8")
        self.assertIn("status: incomplete", lock)
        self.assertIn("unresolved:", lock)
        self.assertIn("Calico component image digests", lock)


if __name__ == "__main__":
    unittest.main()
