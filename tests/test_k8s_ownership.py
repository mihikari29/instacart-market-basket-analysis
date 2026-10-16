from dataclasses import replace
from copy import deepcopy
from pathlib import Path
import unittest

import yaml

from scripts.k8s.ownership import (
    ALLOWED_RESOURCE_INVENTORY,
    OWNER_LABEL,
    OWNER_VALUE,
    RUN_LABEL,
    MANAGED_BY_LABEL,
    MANAGED_BY_VALUE,
    OwnershipError,
    OwnershipReceipt,
    ResourceIdentity,
    RunIdentity,
    StorageIdentity,
    validate_receipt,
    validate_target,
    validate_version_lock,
)


ROOT = Path(__file__).resolve().parents[1]
_DIGEST_A = "sha256:" + "a" * 64
_DIGEST_B = "sha256:" + "b" * 64


def _complete_lock() -> dict:
    return {
        "schema_version": 1,
        "status": "complete",
        "unresolved": [],
        "cluster": {
            "kubernetes_version": "v1.37.1",
            "minikube_version": "v1.39.0",
            "driver": "docker",
            "container_runtime": "containerd",
            "cni": "calico",
            "cni_version": "v3.33.0",
            "architecture": "linux/amd64",
            "node_count": 1,
        },
        "host_capacity": {
            "cpu_cores": 8,
            "memory_gib": 16,
            "free_disk_gib": 80,
            "measured_at": "2026-10-16T10:00:00Z",
        },
        "images": {
            "mongo": {"reference": f"mongo:7.0.16@{_DIGEST_A}"},
            "spark_base": {"reference": f"apache/spark:3.5.5@{_DIGEST_A}"},
            "ui_base": {"reference": f"python:3.14-slim@{_DIGEST_A}"},
            "api_application_image": {"reference": "registry.local/api:b1", "immutable_digest": _DIGEST_A},
            "ui_application_image": {"reference": "registry.local/ui:b1", "immutable_digest": _DIGEST_A},
        },
        "python_dependencies": {
            "api_python": "3.10.16",
            "ui_python": "3.14.0",
            "spark_parent_python": "3.10.16",
            "resolved_environment_digests": {
                "api": _DIGEST_A,
                "ui": _DIGEST_A,
                "spark_parent": _DIGEST_A,
            },
        },
        "toolchain": {
            "kubectl_version": "v1.36.1",
            "minikube_binary_sha256": "c" * 64,
            "docker_engine_version": "27.5.1",
            "kubernetes_control_plane_images": {
                role: f"registry.k8s.io/{role}:v1.37.1@{_DIGEST_A}"
                for role in (
                    "kube-apiserver",
                    "kube-controller-manager",
                    "kube-scheduler",
                    "kube-proxy",
                    "etcd",
                    "pause",
                    "coredns",
                )
            },
            "calico_images": {
                "cni": f"docker.io/calico/cni:v3.33.0@{_DIGEST_A}",
                "node": f"docker.io/calico/node:v3.33.0@{_DIGEST_A}",
                "kube-controllers": f"docker.io/calico/kube-controllers:v3.33.0@{_DIGEST_A}",
            },
        },
        "storage": {
            "default_storage_class": "standard",
            "provisioner": "k8s.io/minikube-hostpath",
            "access_mode": "ReadWriteOnce",
            "capacity": "2Gi",
            "profile_local": True,
            "volume_binding_mode": "WaitForFirstConsumer",
        },
    }


def _receipt(identity: RunIdentity) -> OwnershipReceipt:
    resources = tuple(
        ResourceIdentity(
            kind=kind,
            name=name,
            namespace=identity.namespace,
            uid=f"uid-{index}",
            labels={
                OWNER_LABEL: OWNER_VALUE,
                RUN_LABEL: identity.run_id,
                MANAGED_BY_LABEL: MANAGED_BY_VALUE,
            },
        )
        for index, (kind, name) in enumerate(sorted(ALLOWED_RESOURCE_INVENTORY), start=1)
    )
    pvc = next(item for item in resources if item.kind == "PersistentVolumeClaim")
    storage = StorageIdentity(
        claim_name="data-module4d-mongo-0",
        namespace=identity.namespace,
        claim_uid=pvc.uid,
        declared_by="StatefulSet/module4d-mongo",
        mount_path="/data/db",
        requested_storage="2Gi",
        access_mode="ReadWriteOnce",
        volume_name="pvc-0123",
        volume_uid="pv-uid-0123",
    )
    return OwnershipReceipt(
        context=identity.profile,
        profile=identity.profile,
        namespace=identity.namespace,
        namespace_uid="namespace-uid-0123",
        namespace_labels=identity.labels,
        resources=resources,
        storage=(storage,),
    )


class OwnershipTests(unittest.TestCase):
    def test_run_identity_is_deterministic_and_unique(self) -> None:
        identity = RunIdentity("0123456789ab")
        self.assertEqual(identity.profile, "it4931-k8s-0123456789ab")
        self.assertEqual(identity.namespace, "it4931-k8s-0123456789ab")
        self.assertEqual(identity.resource_prefix, identity.namespace)
        self.assertEqual(identity.labels[RUN_LABEL], "0123456789ab")
        generated = RunIdentity.generate()
        self.assertRegex(generated.run_id, r"\A[0-9a-f]{12}\Z")

    def test_run_identity_rejects_invalid_or_conflicting_shapes(self) -> None:
        for run_id in ("", "abc", "0123456789aB", "0123456789a-", "0" * 13):
            with self.subTest(run_id=run_id), self.assertRaises(OwnershipError):
                RunIdentity(run_id)

    def test_target_validation_requires_dedicated_context_profile_and_namespace(self) -> None:
        identity = RunIdentity("abcdef012345")
        validate_target(
            identity,
            profile=identity.profile,
            context=identity.profile,
            namespace=identity.namespace,
        )
        invalid_targets = (
            {"profile": "minikube", "context": identity.profile, "namespace": identity.namespace},
            {"profile": identity.profile, "context": "current-context", "namespace": identity.namespace},
            {"profile": identity.profile, "context": identity.profile, "namespace": "default"},
            {"profile": identity.profile, "context": identity.profile, "namespace": ""},
        )
        for target in invalid_targets:
            with self.subTest(target=target), self.assertRaises(OwnershipError):
                validate_target(identity, **target)

    def test_receipt_requires_inventory_uids_labels_and_storage_ownership(self) -> None:
        identity = RunIdentity("0123456789ab")
        receipt = _receipt(identity)
        validate_receipt(identity, receipt)

        invalid_receipts = (
            (replace(receipt, namespace_uid=""), "namespace UID"),
            (replace(receipt, namespace_labels={}), "namespace ownership labels"),
            (replace(receipt, resources=receipt.resources[:-1]), "inventory"),
        )
        for candidate, message in invalid_receipts:
            with self.subTest(message=message), self.assertRaisesRegex(OwnershipError, message):
                validate_receipt(identity, candidate)

        first = receipt.resources[0]
        bad_labels = replace(first, labels={OWNER_LABEL: "another-project"})
        with self.assertRaisesRegex(OwnershipError, "labels"):
            validate_receipt(identity, replace(receipt, resources=(bad_labels, *receipt.resources[1:])))

        bad_storage = replace(receipt.storage[0], requested_storage="20Gi")
        with self.assertRaisesRegex(OwnershipError, "storage receipt field"):
            validate_receipt(identity, replace(receipt, storage=(bad_storage,)))

    def test_version_lock_requires_complete_coherent_deployment_identity(self) -> None:
        validate_version_lock(_complete_lock())
        with self.assertRaisesRegex(OwnershipError, "incomplete"):
            validate_version_lock({"schema_version": 1, "status": "incomplete", "unresolved": ["host capacity"]})

    def test_version_lock_rejects_placeholders_missing_roles_bad_digests_and_conflicts(self) -> None:
        committed = yaml.safe_load((ROOT / "deploy/k8s/versions.lock.yaml").read_text(encoding="utf-8"))
        self.assertEqual(committed["status"], "incomplete")
        with self.assertRaises(OwnershipError):
            validate_version_lock(committed)

        cases: list[tuple[str, dict]] = []
        candidate = deepcopy(_complete_lock())
        candidate["cluster"]["driver"] = "unknown"
        cases.append(("placeholder driver", candidate))

        candidate = deepcopy(_complete_lock())
        candidate["cluster"]["architecture"] = "unknown-host-architecture"
        cases.append(("placeholder architecture", candidate))

        candidate = deepcopy(_complete_lock())
        del candidate["images"]["ui_application_image"]
        cases.append(("missing application image role", candidate))

        candidate = deepcopy(_complete_lock())
        candidate["images"]["mongo"]["reference"] = "mongo:7.0.16@sha256:bad"
        cases.append(("malformed base image digest", candidate))

        candidate = deepcopy(_complete_lock())
        candidate["images"]["api_application_image"]["reference"] = f"registry.local/api:b1@{_DIGEST_A}"
        candidate["images"]["api_application_image"]["immutable_digest"] = _DIGEST_B
        cases.append(("contradictory image identity", candidate))

        candidate = deepcopy(_complete_lock())
        candidate["python_dependencies"]["resolved_environment_digests"]["spark_parent"] = "unresolved"
        cases.append(("unresolved dependency digest", candidate))

        candidate = deepcopy(_complete_lock())
        del candidate["toolchain"]["calico_images"]["node"]
        cases.append(("missing Calico image role", candidate))

        candidate = deepcopy(_complete_lock())
        del candidate["toolchain"]["kubernetes_control_plane_images"]["etcd"]
        cases.append(("missing control-plane image role", candidate))

        candidate = deepcopy(_complete_lock())
        del candidate["storage"]["provisioner"]
        cases.append(("missing storage declaration", candidate))

        candidate = deepcopy(_complete_lock())
        candidate["host_capacity"]["memory_gib"] = float("nan")
        cases.append(("non-finite host measurement", candidate))

        for label, candidate in cases:
            with self.subTest(case=label), self.assertRaises(OwnershipError):
                validate_version_lock(candidate)


if __name__ == "__main__":
    unittest.main()
