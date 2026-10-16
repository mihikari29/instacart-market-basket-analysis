from pathlib import Path
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import yaml

from scripts.k8s import api_readiness
from scripts.k8s.api_readiness import IndexRequirement
from scripts.k8s.render import MANAGED_MARKER, _validate_output_path, render_overlay


ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "deploy" / "k8s" / "base"
OWNER_LABEL = "it4931.instacart.edu/owner"
RUN_LABEL = "it4931.instacart.edu/run-id"
MANAGER_LABEL = "app.kubernetes.io/managed-by"
RENDER_LABELS = {
    OWNER_LABEL: "instacart-market-basket-analysis",
    RUN_LABEL: "0123456789ab",
    MANAGER_LABEL: "it4931-k8s-demo",
}


def _rendered_objects(run_dir: Path) -> list[dict]:
    kubectl = shutil.which("kubectl")
    if kubectl is None:
        raise unittest.SkipTest("kubectl is unavailable for offline Kustomize rendering")
    result = subprocess.run(
        [kubectl, "kustomize", str(run_dir)],
        check=True,
        capture_output=True,
        text=True,
    )
    return [item for item in yaml.safe_load_all(result.stdout) if item]


def _objects_by_name(objects: list[dict], kind: str) -> dict[str, dict]:
    return {
        item["metadata"]["name"]: item
        for item in objects
        if item.get("kind") == kind
    }


def _valid_index_metadata() -> dict[str, list[dict]]:
    return {
        "batch_product_metrics": [
            {
                "name": "idx_batch_product_purchase_count_desc_product_id_asc",
                "key": {"purchase_count": -1, "product_id": 1},
            },
            {
                "name": "idx_batch_product_reorder_rate_desc_product_id_asc",
                "key": {"reorder_rate": -1, "product_id": 1},
            },
        ],
        "realtime_trending_windows": [
            {
                "name": "alternate_supported_trend_index",
                "key": {"source_namespace": 1, "window_end": -1, "product_id": 1},
                "sparse": True,
                "collation": {"locale": "simple"},
            }
        ],
        "final_recommendations": [
            {
                "name": "uq_serving_user_rank",
                "key": {"serving_version": 1, "user_id": 1, "rank": 1},
                "unique": True,
            },
            {
                "name": "uq_serving_user_product",
                "key": {"serving_version": 1, "user_id": 1, "product_id": 1},
                "unique": True,
            },
        ],
    }


class _FakeCollection:
    def __init__(self, indexes: list[dict]) -> None:
        self.indexes = indexes
        self.list_indexes_calls = 0

    def list_indexes(self) -> list[dict]:
        self.list_indexes_calls += 1
        return self.indexes


class _FakeDatabase:
    def __init__(self, indexes: dict[str, list[dict]], auth_status: dict) -> None:
        self.collections = {name: _FakeCollection(items) for name, items in indexes.items()}
        self.auth_status = auth_status
        self.commands: list[dict] = []

    def __getitem__(self, name: str) -> _FakeCollection:
        return self.collections[name]

    def command(self, command: dict) -> dict:
        self.commands.append(command)
        if command == {"connectionStatus": 1}:
            return self.auth_status
        raise AssertionError(f"unexpected database command: {command}")


class _FakeAdmin:
    def __init__(self) -> None:
        self.commands: list[dict] = []

    def command(self, command: dict) -> dict:
        self.commands.append(command)
        if command == "ping":
            return {"ok": 1}
        raise AssertionError(f"unexpected admin command: {command}")


class _FakeClient:
    def __init__(self, database: _FakeDatabase) -> None:
        self.database = database
        self.admin = _FakeAdmin()

    def __getitem__(self, name: str) -> _FakeDatabase:
        self.database_name = name
        return self.database


def _auth_status(username: str = "reader", database: str = "module4d_demo") -> dict:
    return {
        "ok": 1,
        "authInfo": {
            "authenticatedUsers": [{"user": username, "db": database}],
            "authenticatedUserRoles": [{"role": "read", "db": database}],
        },
    }


class StaticFoundationTests(unittest.TestCase):
    def test_offline_kustomize_preserves_dns_selectors_and_owned_labels(self) -> None:
        with tempfile.TemporaryDirectory(prefix="it4931-k8s-render-test-") as temp:
            output = Path(temp) / "first-render"
            run_dir = render_overlay("0123456789ab", output)
            overlay = json.loads((run_dir / "kustomization.yaml").read_text(encoding="utf-8"))
            namespace = json.loads((run_dir / "namespace.json").read_text(encoding="utf-8"))
            api_patch = json.loads((run_dir / "api-config-patch.json").read_text(encoding="utf-8"))
            self.assertEqual(overlay["namespace"], "it4931-k8s-0123456789ab")
            self.assertEqual(overlay["labels"][0]["includeSelectors"], False)
            self.assertEqual(overlay["labels"][0]["includeTemplates"], True)
            self.assertEqual(namespace["metadata"]["labels"][RUN_LABEL], "0123456789ab")
            self.assertEqual(api_patch["data"]["MODULE4D_MONGO_DATABASE"], "module4d_demo_0123456789ab")
            self.assertEqual(output.stat().st_mode & 0o777, 0o700)
            self.assertEqual((output / MANAGED_MARKER).stat().st_mode & 0o777, 0o600)

            objects = _rendered_objects(run_dir)
            namespaces = _objects_by_name(objects, "Namespace")
            self.assertEqual(namespaces["it4931-k8s-0123456789ab"]["metadata"]["labels"], RENDER_LABELS)
            namespaced_objects = [
                item
                for item in objects
                if item.get("metadata", {}).get("namespace") == "it4931-k8s-0123456789ab"
            ]
            self.assertTrue(namespaced_objects)
            for item in namespaced_objects:
                with self.subTest(kind=item["kind"], name=item["metadata"]["name"]):
                    self.assertTrue(
                        all(item["metadata"].get("labels", {}).get(key) == value for key, value in RENDER_LABELS.items())
                    )

            policies = _objects_by_name(objects, "NetworkPolicy")
            dns_peer = policies["allow-dns-egress"]["spec"]["egress"][0]["to"][0]
            self.assertEqual(
                dns_peer["namespaceSelector"]["matchLabels"],
                {"kubernetes.io/metadata.name": "kube-system"},
            )
            self.assertEqual(dns_peer["podSelector"]["matchLabels"], {"k8s-app": "kube-dns"})
            self.assertNotIn(OWNER_LABEL, dns_peer["podSelector"]["matchLabels"])
            self.assertNotIn(RUN_LABEL, dns_peer["podSelector"]["matchLabels"])

            deployments = _objects_by_name(objects, "Deployment")
            statefulsets = _objects_by_name(objects, "StatefulSet")
            for name, workload in {**deployments, **statefulsets}.items():
                with self.subTest(workload=name):
                    self.assertTrue(all(workload["metadata"]["labels"].get(k) == v for k, v in RENDER_LABELS.items()))
                    self.assertTrue(
                        all(workload["spec"]["template"]["metadata"]["labels"].get(k) == v for k, v in RENDER_LABELS.items())
                    )
                    selector = workload["spec"]["selector"]["matchLabels"]
                    pod_labels = workload["spec"]["template"]["metadata"]["labels"]
                    self.assertTrue(all(pod_labels.get(key) == value for key, value in selector.items()))

            mongo_template = statefulsets["module4d-mongo"]["spec"]["volumeClaimTemplates"][0]
            self.assertTrue(all(mongo_template["metadata"]["labels"].get(k) == v for k, v in RENDER_LABELS.items()))

            self.assertEqual(policies["default-deny"]["spec"]["policyTypes"], ["Ingress", "Egress"])
            self.assertEqual(policies["default-deny"]["spec"]["podSelector"], {})
            expected_selector = {
                "allow-api-to-mongo": "module4d-mongo",
                "allow-ui-to-api": "module4d-api",
            }
            for policy_name, pod_name in expected_selector.items():
                selector = policies[policy_name]["spec"]["podSelector"]["matchLabels"]
                self.assertEqual(selector, {"app.kubernetes.io/name": pod_name})
                selected_workload = statefulsets[pod_name] if pod_name == "module4d-mongo" else deployments[pod_name]
                pod_labels = selected_workload["spec"]["template"]["metadata"]["labels"]
                self.assertTrue(all(pod_labels.get(k) == v for k, v in selector.items()))

            source_policy_workloads = {
                "allow-api-to-mongo": (policies["allow-api-to-mongo"]["spec"]["ingress"][0]["from"][0], "module4d-api"),
                "allow-ui-to-api": (policies["allow-ui-to-api"]["spec"]["ingress"][0]["from"][0], "module4d-ui"),
                "allow-api-egress": (policies["allow-api-egress"]["spec"]["egress"][0]["to"][0], "module4d-mongo"),
                "allow-ui-egress": (policies["allow-ui-egress"]["spec"]["egress"][0]["to"][0], "module4d-api"),
            }
            for policy_name, (peer, workload_name) in source_policy_workloads.items():
                selector = peer["podSelector"]["matchLabels"]
                workload = statefulsets.get(workload_name) or deployments[workload_name]
                pod_labels = workload["spec"]["template"]["metadata"]["labels"]
                with self.subTest(policy_peer=policy_name):
                    self.assertTrue(all(pod_labels.get(key) == value for key, value in selector.items()))

    def test_render_rejects_protected_roots_symlinks_and_collisions_without_writes(self) -> None:
        with self.assertRaisesRegex(ValueError, "direct child"):
            _validate_output_path(Path("/__it4931_render_forbidden__"))
        self.assertFalse(Path("/__it4931_render_forbidden__").exists())
        with tempfile.TemporaryDirectory(prefix="it4931-k8s-render-safety-") as temp:
            root = Path(temp)
            unsafe_outputs = (
                ROOT / "would-be-render-output",
                BASE / "would-be-render-output",
                Path("/"),
            )
            for destination in unsafe_outputs:
                with self.subTest(destination=destination), self.assertRaises((ValueError, FileExistsError)):
                    render_overlay("0123456789ab", destination)
                self.assertFalse(destination.name == "would-be-render-output" and destination.exists())

            symlink_parent = root / "source-link"
            symlink_parent.symlink_to(BASE, target_is_directory=True)
            symlink_destination = symlink_parent / "would-be-output"
            with self.assertRaisesRegex(ValueError, "symlink"):
                render_overlay("0123456789ab", symlink_destination)
            self.assertFalse((BASE / "would-be-output").exists())

            collision = root / "existing-output"
            collision.mkdir()
            sentinel = collision / "keep.txt"
            sentinel.write_text("preserve", encoding="utf-8")
            with self.assertRaises(FileExistsError):
                render_overlay("0123456789ab", collision)
            self.assertEqual(sentinel.read_text(encoding="utf-8"), "preserve")

            managed = root / "managed-output"
            render_overlay("0123456789ab", managed)
            nested = managed / "nested-output"
            with self.assertRaisesRegex(ValueError, "managed render"):
                render_overlay("abcdef012345", nested)
            self.assertFalse(nested.exists())

    def test_render_preserves_partial_claim_if_copy_fails(self) -> None:
        with tempfile.TemporaryDirectory(prefix="it4931-k8s-render-partial-") as temp:
            output = Path(temp) / "partial-output"
            with patch("scripts.k8s.render.shutil.copytree", side_effect=OSError("synthetic copy failure")):
                with self.assertRaisesRegex(OSError, "synthetic copy failure"):
                    render_overlay("0123456789ab", output)
            self.assertTrue(output.is_dir())
            self.assertTrue((output / MANAGED_MARKER).is_file())

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

    def test_mongo_requires_nonempty_admin_credentials_and_explicit_auth(self) -> None:
        documents = list(yaml.safe_load_all((BASE / "mongo.yaml").read_text(encoding="utf-8")))
        statefulset = next(item for item in documents if item and item.get("kind") == "StatefulSet")
        pod_spec = statefulset["spec"]["template"]["spec"]
        init = next(item for item in pod_spec["initContainers"] if item["name"] == "validate-admin-credentials")
        self.assertEqual(init["securityContext"]["runAsNonRoot"], True)
        self.assertEqual(init["securityContext"]["runAsUser"], 999)
        self.assertEqual(init["command"], ["sh", "-ec"])
        self.assertIn('test -n "$MONGO_INITDB_ROOT_USERNAME"', init["args"][0])
        self.assertIn('test -n "$MONGO_INITDB_ROOT_PASSWORD"', init["args"][0])
        self.assertNotIn("MONGO_INITDB", init["args"][0].split("echo", 1)[1])
        env_refs = {
            item["name"]: item["valueFrom"]["secretKeyRef"]
            for item in init["env"]
        }
        self.assertEqual(env_refs["MONGO_INITDB_ROOT_USERNAME"], {"name": "module4d-mongo-admin", "key": "username"})
        self.assertEqual(env_refs["MONGO_INITDB_ROOT_PASSWORD"], {"name": "module4d-mongo-admin", "key": "password"})
        mongo = next(item for item in pod_spec["containers"] if item["name"] == "mongodb")
        self.assertIn("--auth", mongo["args"])

    def test_authenticated_readiness_checks_identity_and_index_options(self) -> None:
        expected = {
            "batch_product_metrics": {
                "idx_batch_product_purchase_count_desc_product_id_asc": IndexRequirement(
                    (("purchase_count", -1), ("product_id", 1))
                ),
                "idx_batch_product_reorder_rate_desc_product_id_asc": IndexRequirement(
                    (("reorder_rate", -1), ("product_id", 1))
                ),
            },
            "realtime_trending_windows": {
                "idx_source_namespace_window_end_desc": IndexRequirement(
                    (("source_namespace", 1), ("window_end", -1)),
                    allow_trend_prefix=True,
                    allow_name_fallback=True,
                    reject_sparse=False,
                )
            },
            "final_recommendations": {
                "uq_serving_user_rank": IndexRequirement(
                    (("serving_version", 1), ("user_id", 1), ("rank", 1)), unique=True
                ),
                "uq_serving_user_product": IndexRequirement(
                    (("serving_version", 1), ("user_id", 1), ("product_id", 1)), unique=True
                ),
            },
        }
        valid = _valid_index_metadata()
        database = _FakeDatabase(valid, _auth_status())
        client = _FakeClient(database)
        with patch.object(api_readiness, "_index_specs", return_value=expected):
            api_readiness._verify(client, "module4d_demo", "reader")
        self.assertEqual(database.commands, [{"connectionStatus": 1}])
        self.assertEqual(client.admin.commands, ["ping"])
        self.assertTrue(all(collection.list_indexes_calls == 1 for collection in database.collections.values()))

        invalid_cases = (
            ("product hidden", "batch_product_metrics", 0, {"hidden": True}),
            ("product sparse", "batch_product_metrics", 0, {"sparse": True}),
            ("product partial", "batch_product_metrics", 0, {"partialFilterExpression": {"active": True}}),
            ("product collation", "batch_product_metrics", 0, {"collation": {"locale": "en"}}),
            ("product wrong key order", "batch_product_metrics", 0, {"key": {"product_id": 1, "purchase_count": -1}}),
            ("product wrong name", "batch_product_metrics", 0, {"name": "different_index_name"}),
            ("trend hidden", "realtime_trending_windows", 0, {"hidden": True}),
            ("trend partial", "realtime_trending_windows", 0, {"partialFilterExpression": {"ready": True}}),
            ("trend non-simple collation", "realtime_trending_windows", 0, {"collation": {"locale": "en"}}),
            ("trend wrong prefix", "realtime_trending_windows", 0, {"key": {"window_end": -1, "source_namespace": 1}}),
            ("trend non-integer direction", "realtime_trending_windows", 0, {"key": {"source_namespace": True, "window_end": -1}}),
            ("serving not unique", "final_recommendations", 0, {"unique": False}),
            ("serving wrong name", "final_recommendations", 0, {"name": "different_unique_index"}),
        )
        for label, collection_name, index_number, changes in invalid_cases:
            with self.subTest(index=label):
                indexes = {name: [dict(item) for item in items] for name, items in valid.items()}
                indexes[collection_name][index_number].update(changes)
                fake_db = _FakeDatabase(indexes, _auth_status())
                fake_client = _FakeClient(fake_db)
                with patch.object(api_readiness, "_index_specs", return_value=expected):
                    with self.assertRaisesRegex(RuntimeError, "index is missing or incompatible"):
                        api_readiness._verify(fake_client, "module4d_demo", "reader")

        bad_auth = _FakeClient(_FakeDatabase(valid, {"ok": 1}))
        with patch.object(api_readiness, "_index_specs", return_value=expected):
            with self.assertRaisesRegex(RuntimeError, "authentication metadata"):
                api_readiness._verify(bad_auth, "module4d_demo", "reader")

    def test_mongodb_uri_and_connection_metadata_require_authenticated_database_reader(self) -> None:
        self.assertEqual(
            api_readiness._authenticated_username(
                "mongodb://reader:p%40ss@mongo/module4d_demo?authSource=module4d_demo",
                "module4d_demo",
            ),
            "reader",
        )
        bad_uris = (
            "mongodb://mongo/module4d_demo?authSource=module4d_demo",
            "mongodb://reader@mongo/module4d_demo?authSource=module4d_demo",
            "mongodb://reader:pw@mongo/other?authSource=module4d_demo",
            "mongodb://reader:pw@mongo/module4d_demo?authSource=admin",
            "mongodb://reader:pw@[invalid/module4d_demo?authSource=module4d_demo",
        )
        for uri in bad_uris:
            with self.subTest(uri=uri), self.assertRaises(RuntimeError):
                api_readiness._authenticated_username(uri, "module4d_demo")

        bad_metadata = (
            {"ok": 1},
            {"ok": True, "authInfo": _auth_status()["authInfo"]},
            {"ok": 1, "authInfo": {"authenticatedUsers": [], "authenticatedUserRoles": []}},
            {
                "ok": 1,
                "authInfo": {
                    "authenticatedUsers": [{"user": "other", "db": "module4d_demo"}],
                    "authenticatedUserRoles": [{"role": "read", "db": "module4d_demo"}],
                },
            },
            {
                "ok": 1,
                "authInfo": {
                    "authenticatedUsers": [{"user": "reader", "db": "admin"}],
                    "authenticatedUserRoles": [{"role": "read", "db": "module4d_demo"}],
                },
            },
            {
                "ok": 1,
                "authInfo": {
                    "authenticatedUsers": [{"user": "reader", "db": "module4d_demo"}],
                    "authenticatedUserRoles": [{"role": "readWrite", "db": "module4d_demo"}],
                },
            },
        )
        for metadata in bad_metadata:
            with self.subTest(metadata=metadata), self.assertRaises(RuntimeError):
                api_readiness._verify_authenticated_identity(metadata, "reader", "module4d_demo")

    def test_dockerfile_sha_fragment_rejects_missing_wrong_length_uppercase_and_nonhex(self) -> None:
        dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
        runner = dockerfile.split("FROM spark AS k8s-runner", 1)[1]
        fragment = runner.split("RUN set -eu;", 1)[1].split("groupadd --gid", 1)[0]
        shell = "set -eu;" + fragment.replace("\\\n", " ")
        valid_sha = "a" * 40

        def run(value: str | None) -> subprocess.CompletedProcess[str]:
            env = os.environ.copy()
            if value is None:
                env.pop("MODULE4_COMMIT", None)
            else:
                env["MODULE4_COMMIT"] = value
            return subprocess.run(["/bin/sh", "-c", shell], env=env, capture_output=True, text=True)

        self.assertEqual(run(valid_sha).returncode, 0)
        for invalid in (None, "", "a" * 39, "a" * 41, "A" * 40, "g" * 40, "a" * 39 + "G"):
            with self.subTest(value=invalid):
                self.assertNotEqual(run(invalid).returncode, 0)

    def test_docker_context_excludes_only_the_two_host_kubernetes_tests(self) -> None:
        ignore_lines = (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
        allow_index = ignore_lines.index("!tests/**/*.py")
        ownership_index = ignore_lines.index("tests/test_k8s_ownership.py")
        static_index = ignore_lines.index("tests/test_k8s_static.py")
        self.assertLess(allow_index, ownership_index)
        self.assertLess(allow_index, static_index)
        dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
        self.assertIn("COPY tests/", dockerfile)
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
                self.assertIn(f"!{source_path}", ignore_lines)
        self.assertIn("**/.env", ignore_lines)
        self.assertIn("**/secrets/**", ignore_lines)
        self.assertNotIn("COPY data/", dockerfile)

    def test_ui_readiness_timeout_exceeds_internal_request_timeout(self) -> None:
        documents = [item for item in yaml.safe_load_all((BASE / "serving.yaml").read_text(encoding="utf-8")) if item]
        deployment = next(item for item in documents if item.get("kind") == "Deployment" and item["metadata"]["name"] == "module4d-ui")
        probe = deployment["spec"]["template"]["spec"]["containers"][0]["readinessProbe"]
        command = " ".join(probe["exec"]["command"])
        import re

        match = re.search(r"timeout=(\d+)", command)
        self.assertIsNotNone(match)
        self.assertEqual(int(match.group(1)), 3)
        self.assertGreater(probe["timeoutSeconds"], int(match.group(1)))
        self.assertLessEqual(probe["timeoutSeconds"], 6)

    def test_base_defines_only_internal_network_and_storage_guards(self) -> None:
        network = list(yaml.safe_load_all((BASE / "network-policies.yaml").read_text(encoding="utf-8")))
        policies = {item["metadata"]["name"]: item for item in network if item}
        self.assertIn("default-deny", policies)
        self.assertIn("allow-api-to-mongo", policies)
        self.assertIn("allow-ui-to-api", policies)
        contents = "\n".join(path.read_text(encoding="utf-8") for path in BASE.glob("*.yaml"))
        self.assertNotIn("type: NodePort", contents)
        self.assertNotIn("hostPath:", contents)
        self.assertNotIn("hostNetwork:", contents)
        self.assertIn("storage: 2Gi", contents)
        self.assertIn("@sha256:", contents)

    def test_committed_version_lock_remains_incomplete_until_runtime_evidence_exists(self) -> None:
        lock = yaml.safe_load((ROOT / "deploy/k8s/versions.lock.yaml").read_text(encoding="utf-8"))
        self.assertEqual(lock["status"], "incomplete")
        self.assertTrue(lock["unresolved"])
        self.assertIn("host_capacity", lock)
        self.assertIn("toolchain", lock)
        self.assertIn("storage", lock)
        self.assertIn("Calico component image digests", "\n".join(lock["unresolved"]))
        self.assertEqual(
            lock["cluster"]["cni_compatibility_source"],
            "https://docs.tigera.io/calico/latest/getting-started/kubernetes/requirements",
        )
        docs = (ROOT / "docs/kubernetes.md").read_text(encoding="utf-8")
        self.assertIn(lock["cluster"]["cni_compatibility_source"], docs)


if __name__ == "__main__":
    unittest.main()
