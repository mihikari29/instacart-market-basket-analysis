"""Offline contracts for explicit-profile preflight and identity discovery."""

import json
import unittest
from unittest.mock import patch

from scripts.k8s.ownership import RunIdentity
from scripts.k8s.runtime import (
    CommandResult,
    RuntimePreflightError,
    SubprocessExecutor,
    cluster_creation_plan,
    collect_cluster_identity,
    collect_namespace_identity,
    collect_storage_class_identity,
    preflight_dedicated_target,
    validate_host_capacity,
)


class FakeExecutor:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def run(self, argv, *, timeout, input_text=None):
        self.calls.append(list(argv))
        return self.responses.pop(0)


class RuntimePreflightTests(unittest.TestCase):
    def test_shell_free_executor_accepts_reviewed_long_job_timeouts(self):
        executor = SubprocessExecutor()
        with patch("scripts.k8s.runtime.subprocess.run") as run:
            run.return_value.returncode = 0
            run.return_value.stdout = "ok"
            run.return_value.stderr = ""
            result = executor.run(["python3", "-c", "pass"], timeout=7200)
        self.assertEqual(result.returncode, 0)
        self.assertEqual(run.call_args.kwargs["timeout"], 7200)
        self.assertIs(run.call_args.kwargs["shell"], False)

        with self.assertRaisesRegex(RuntimePreflightError, "10800 seconds"):
            executor.run(["python3", "-c", "pass"], timeout=10801)

    def test_collision_preflight_refuses_existing_profile_or_context(self):
        identity = RunIdentity("0123456789ab")
        executor = FakeExecutor(
            [
                CommandResult(0, json.dumps({"profiles": [{"Name": identity.profile}]})),
                CommandResult(0, "minikube\n"),
            ]
        )
        with self.assertRaisesRegex(RuntimePreflightError, "profile already exists"):
            preflight_dedicated_target(executor, identity)
        self.assertEqual(executor.calls[0], ["minikube", "profile", "list", "-o", "json"])
        self.assertEqual(executor.calls[1][:3], ["kubectl", "config", "get-contexts"])

        executor = FakeExecutor(
            [
                CommandResult(0, json.dumps({"profiles": []})),
                CommandResult(0, identity.profile + "\n"),
            ]
        )
        with self.assertRaisesRegex(RuntimePreflightError, "context already exists"):
            preflight_dedicated_target(executor, identity)

    def test_discovery_uses_explicit_context_and_checks_single_node(self):
        identity = RunIdentity("abcdef012345")
        responses = [
            CommandResult(0, '{"Host":"Running"}'),
            CommandResult(
                0,
                json.dumps(
                    {
                        "valid": [
                            {
                                "Name": identity.profile,
                                "Config": {
                                    "Driver": "docker",
                                    "KubernetesConfig": {
                                        "KubernetesVersion": "v1.37.1",
                                        "ContainerRuntime": "containerd",
                                        "CNI": "calico",
                                    },
                                },
                            }
                        ]
                    }
                ),
            ),
            CommandResult(0, json.dumps({"metadata": {"uid": "cluster-uid"}})),
            CommandResult(0, json.dumps({"serverVersion": {"gitVersion": "v1.37.1"}})),
            CommandResult(
                0,
                json.dumps(
                    {
                        "items": [
                            {
                                "status": {
                                    "nodeInfo": {
                                        "architecture": "amd64",
                                        "containerRuntimeVersion": "containerd://1.7.27",
                                    }
                                }
                            }
                        ]
                    }
                ),
            ),
        ]
        executor = FakeExecutor(responses)
        result = collect_cluster_identity(executor, identity, execute=True)
        self.assertEqual(result["cluster_uid"], "cluster-uid")
        self.assertEqual(result["architecture"], "linux/amd64")
        self.assertEqual(result["node_count"], 1)
        self.assertEqual(result["driver"], "docker")
        self.assertEqual(result["container_runtime"], "containerd")
        self.assertEqual(result["cni"], "calico")
        for call in executor.calls[2:]:
            self.assertEqual(call[:3], ["kubectl", "--context", identity.profile])

        labels = identity.labels
        namespace = {
            "metadata": {"name": identity.namespace, "uid": "namespace-uid", "labels": labels}
        }
        executor = FakeExecutor([CommandResult(0, json.dumps(namespace))])
        self.assertEqual(
            collect_namespace_identity(executor, identity, execute=True)["namespace_uid"],
            "namespace-uid",
        )

    def test_storage_discovery_requires_one_expected_default_class(self):
        identity = RunIdentity("0123456789ab")
        item = {
            "metadata": {
                "name": "standard",
                "uid": "sc-uid",
                "annotations": {"storageclass.kubernetes.io/is-default-class": "true"},
            },
            "provisioner": "k8s.io/minikube-hostpath",
            "volumeBindingMode": "Immediate",
        }
        executor = FakeExecutor([CommandResult(0, json.dumps({"items": [item]}))])
        result = collect_storage_class_identity(executor, identity, execute=True)
        self.assertEqual(result["storage_class_uid"], "sc-uid")
        self.assertIs(result["profile_local"], True)
        self.assertEqual(executor.calls[0][:3], ["kubectl", "--context", identity.profile])
        item["volumeBindingMode"] = "WaitForFirstConsumer"
        executor = FakeExecutor([CommandResult(0, json.dumps({"items": [item]}))])
        with self.assertRaisesRegex(RuntimePreflightError, "Immediate"):
            collect_storage_class_identity(executor, identity, execute=True)

    def test_capacity_and_creation_plan_fail_closed(self):
        with self.assertRaisesRegex(RuntimePreflightError, "below"):
            validate_host_capacity(
                {"cpu_cores": 6, "memory_gib": 5.4, "free_disk_gib": 50, "wsl_guest": False}
            )
        with self.assertRaisesRegex(RuntimePreflightError, "backing-disk"):
            validate_host_capacity(
                {"cpu_cores": 6, "memory_gib": 12, "free_disk_gib": 50, "wsl_guest": True}
            )
        for invalid_backing in (float("nan"), float("inf"), True):
            with self.subTest(windows_backing_disk_gib=invalid_backing):
                with self.assertRaisesRegex(RuntimePreflightError, "backing-disk"):
                    validate_host_capacity({
                        "cpu_cores": 6, "memory_gib": 12, "free_disk_gib": 50,
                        "wsl_guest": True, "windows_backing_disk_gib": invalid_backing,
                    })
        identity = RunIdentity("0123456789ab")
        command = cluster_creation_plan(
            identity, kubernetes_version="v1.37.1", cpus=4, memory_mib=6144, disk_gib=40
        )
        self.assertIn("--profile", command)
        self.assertIn(identity.profile, command)
        self.assertFalse(any("current-context" in value for value in command))


if __name__ == "__main__":
    unittest.main()
