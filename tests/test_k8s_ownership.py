from dataclasses import replace
import unittest

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

    def test_version_lock_contract_rejects_incomplete_and_accepts_pinned_lock(self) -> None:
        with self.assertRaisesRegex(OwnershipError, "incomplete"):
            validate_version_lock({"status": "incomplete", "unresolved": ["host capacity"]})

        complete = {
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
            "images": {
                "api": {
                    "reference": "example/api:b1",
                    "immutable_digest": "sha256:" + "a" * 64,
                }
            },
        }
        validate_version_lock(complete)


if __name__ == "__main__":
    unittest.main()
