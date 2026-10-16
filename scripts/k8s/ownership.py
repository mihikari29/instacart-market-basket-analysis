"""Pure identity, ownership-receipt, and lock validation for K8S-B1.

This module never reads kubeconfig, contacts a cluster, or runs a command.
Future execution code must keep those operations separate and validate a
complete receipt before any destructive action.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
import secrets
from typing import Any, Mapping, Sequence


RUN_ID_PATTERN = re.compile(r"[0-9a-f]{12}\Z")
NAMESPACE_PATTERN = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z")
IMAGE_DIGEST_PATTERN = re.compile(r"sha256:[0-9a-f]{64}\Z")
PROTECTED_NAMESPACES = frozenset(
    {"default", "kube-system", "kube-public", "kube-node-lease"}
)
OWNER_LABEL = "it4931.instacart.edu/owner"
RUN_LABEL = "it4931.instacart.edu/run-id"
MANAGED_BY_LABEL = "app.kubernetes.io/managed-by"
OWNER_VALUE = "instacart-market-basket-analysis"
MANAGED_BY_VALUE = "it4931-k8s-demo"


class OwnershipError(ValueError):
    """An identity, target, receipt, or version lock is unsafe or incomplete."""


@dataclass(frozen=True, slots=True)
class RunIdentity:
    """Deterministic, per-run names; the namespace UID is learned at runtime."""

    run_id: str

    def __post_init__(self) -> None:
        if not isinstance(self.run_id, str) or not RUN_ID_PATTERN.fullmatch(self.run_id):
            raise OwnershipError("run_id must be exactly 12 lowercase hexadecimal characters")

    @classmethod
    def generate(cls) -> "RunIdentity":
        """Create a fresh 48-bit run suffix; a live preflight must still check for collisions."""
        return cls(secrets.token_hex(6))

    @property
    def profile(self) -> str:
        return f"it4931-k8s-{self.run_id}"

    @property
    def namespace(self) -> str:
        return f"it4931-k8s-{self.run_id}"

    @property
    def resource_prefix(self) -> str:
        # Stable names inside the unique namespace preserve the required
        # module4d-api Service hostname.
        return self.namespace

    @property
    def labels(self) -> dict[str, str]:
        return {
            OWNER_LABEL: OWNER_VALUE,
            RUN_LABEL: self.run_id,
            MANAGED_BY_LABEL: MANAGED_BY_VALUE,
        }


@dataclass(frozen=True, slots=True)
class ResourceIdentity:
    """One directly managed namespaced object observed from the API server."""

    kind: str
    name: str
    namespace: str
    uid: str
    labels: Mapping[str, str]


@dataclass(frozen=True, slots=True)
class OwnershipReceipt:
    """Runtime receipt fields required before a later scoped teardown."""

    context: str
    profile: str
    namespace: str
    namespace_uid: str
    namespace_labels: Mapping[str, str]
    resources: Sequence[ResourceIdentity]
    storage: Sequence["StorageIdentity"]


@dataclass(frozen=True, slots=True)
class StorageIdentity:
    """PVC/PV identity and the workload contract that owns the claim."""

    claim_name: str
    namespace: str
    claim_uid: str
    declared_by: str
    mount_path: str
    requested_storage: str
    access_mode: str
    volume_name: str
    volume_uid: str


# This is the complete direct-object inventory rendered by deploy/k8s/base.
# Controller-created Pods/ReplicaSets and the bound PV must also be captured
# and validated by a future live receipt before it implements teardown.
ALLOWED_RESOURCE_INVENTORY = frozenset(
    {
        ("ResourceQuota", "it4931-quota"),
        ("LimitRange", "it4931-limits"),
        ("ServiceAccount", "module4d-mongo"),
        ("ServiceAccount", "module4d-api"),
        ("ServiceAccount", "module4d-ui"),
        ("ConfigMap", "module4d-api-config"),
        ("ConfigMap", "module4d-ui-config"),
        ("Service", "module4d-mongo-headless"),
        ("Service", "module4d-mongo"),
        ("StatefulSet", "module4d-mongo"),
        ("Service", "module4d-api"),
        ("Deployment", "module4d-api"),
        ("Service", "module4d-ui"),
        ("Deployment", "module4d-ui"),
        ("NetworkPolicy", "default-deny"),
        ("NetworkPolicy", "allow-dns-egress"),
        ("NetworkPolicy", "allow-api-to-mongo"),
        ("NetworkPolicy", "allow-ui-to-api"),
        ("NetworkPolicy", "allow-api-egress"),
        ("NetworkPolicy", "allow-ui-egress"),
        ("PersistentVolumeClaim", "data-module4d-mongo-0"),
    }
)
DECLARED_STORAGE_OWNERSHIP = {
    "claim_name": "data-module4d-mongo-0",
    "declared_by": "StatefulSet/module4d-mongo",
    "mount_path": "/data/db",
    "requested_storage": "2Gi",
    "access_mode": "ReadWriteOnce",
}


def validate_target(
    identity: RunIdentity,
    *,
    profile: str,
    context: str,
    namespace: str,
) -> None:
    """Reject namespace/profile/context mismatches before a future operation."""
    if not isinstance(namespace, str) or not namespace.strip():
        raise OwnershipError("namespace must be explicitly supplied")
    if not NAMESPACE_PATTERN.fullmatch(namespace):
        raise OwnershipError("namespace is not a valid Kubernetes DNS label")
    if namespace in PROTECTED_NAMESPACES:
        raise OwnershipError("refusing a protected Kubernetes namespace")
    if profile != identity.profile:
        raise OwnershipError("Minikube profile does not match the run identity")
    if context != identity.profile:
        raise OwnershipError("Kubernetes context does not match the dedicated Minikube profile")
    if namespace != identity.namespace:
        raise OwnershipError("namespace does not match the run identity")


def validate_receipt(identity: RunIdentity, receipt: OwnershipReceipt) -> None:
    """Require exact identity, namespace UID, inventory, labels, and PVC owner."""
    validate_target(
        identity,
        profile=receipt.profile,
        context=receipt.context,
        namespace=receipt.namespace,
    )
    if not isinstance(receipt.namespace_uid, str) or not receipt.namespace_uid.strip():
        raise OwnershipError("recorded namespace UID is required after namespace creation")
    if not isinstance(receipt.namespace_labels, Mapping) or any(
        receipt.namespace_labels.get(key) != value for key, value in identity.labels.items()
    ):
        raise OwnershipError("namespace ownership labels do not match the run identity")
    if not isinstance(receipt.resources, Sequence) or isinstance(receipt.resources, (str, bytes)):
        raise OwnershipError("resource inventory must be an explicit sequence")

    actual: dict[tuple[str, str], ResourceIdentity] = {}
    observed_uids = {receipt.namespace_uid}
    for resource in receipt.resources:
        if not isinstance(resource, ResourceIdentity):
            raise OwnershipError("resource inventory contains an invalid record")
        if not all(isinstance(value, str) and value.strip() for value in (resource.kind, resource.name, resource.uid)):
            raise OwnershipError("resource kind, name, and UID must be non-empty strings")
        if resource.uid in observed_uids:
            raise OwnershipError("namespace and resource UIDs must be unique in the receipt")
        observed_uids.add(resource.uid)
        if resource.namespace != identity.namespace:
            raise OwnershipError("resource inventory contains an unrelated namespace")
        key = (resource.kind, resource.name)
        if key in actual:
            raise OwnershipError("resource inventory contains a duplicate object")
        if not resource.uid or not resource.uid.strip():
            raise OwnershipError(f"recorded UID is required for {resource.kind}/{resource.name}")
        if not isinstance(resource.labels, Mapping) or any(
            resource.labels.get(key) != value for key, value in identity.labels.items()
        ):
            raise OwnershipError(f"ownership labels do not match for {resource.kind}/{resource.name}")
        actual[key] = resource

    if frozenset(actual) != ALLOWED_RESOURCE_INVENTORY:
        raise OwnershipError("resource inventory is incomplete or contains an unapproved object")
    if not isinstance(receipt.storage, Sequence) or isinstance(receipt.storage, (str, bytes)):
        raise OwnershipError("storage receipt must be an explicit sequence")
    if len(receipt.storage) != 1:
        raise OwnershipError("storage receipt must identify the single declared MongoDB claim")
    storage = receipt.storage[0]
    if not isinstance(storage, StorageIdentity):
        raise OwnershipError("storage receipt is invalid")
    for field, expected in DECLARED_STORAGE_OWNERSHIP.items():
        if getattr(storage, field) != expected:
            raise OwnershipError(f"storage receipt field {field} does not match the declaration")
    pvc = actual[("PersistentVolumeClaim", DECLARED_STORAGE_OWNERSHIP["claim_name"])]
    if storage.namespace != identity.namespace or storage.claim_uid != pvc.uid:
        raise OwnershipError("PVC namespace or UID does not match the owned inventory")
    if not storage.volume_name or not storage.volume_uid:
        raise OwnershipError("bound PV name and UID are required in the storage receipt")
    if storage.volume_uid in observed_uids:
        raise OwnershipError("bound PV UID conflicts with another owned object")


def validate_version_lock(lock: Mapping[str, Any]) -> None:
    """Fail closed unless a future runtime lock is explicitly complete."""
    if not isinstance(lock, Mapping):
        raise OwnershipError("version lock must be a mapping")
    if lock.get("status") != "complete":
        raise OwnershipError("version lock is incomplete; deployment preflight must stop")
    unresolved = lock.get("unresolved")
    if not isinstance(unresolved, list) or unresolved:
        raise OwnershipError("version lock has unresolved runtime identities")
    cluster = lock.get("cluster")
    if not isinstance(cluster, Mapping):
        raise OwnershipError("version lock is missing the cluster section")
    for field in (
        "kubernetes_version",
        "minikube_version",
        "driver",
        "container_runtime",
        "cni",
        "cni_version",
        "architecture",
    ):
        value = cluster.get(field)
        if not isinstance(value, str) or not value.strip() or value.lower() == "unresolved":
            raise OwnershipError(f"version lock field {field} is incomplete")
    if type(cluster.get("node_count")) is not int or cluster["node_count"] < 1:
        raise OwnershipError("version lock node_count must be a positive integer")
    images = lock.get("images")
    if not isinstance(images, Mapping) or not images:
        raise OwnershipError("version lock must record image identities")
    for name, image in images.items():
        if not isinstance(name, str) or not isinstance(image, Mapping):
            raise OwnershipError("each image lock entry must be an object")
        reference = image.get("reference")
        if not isinstance(reference, str) or not reference.strip() or reference.endswith(":latest"):
            raise OwnershipError(f"image {name} must use an explicit non-latest reference")
        declared_digest = image.get("immutable_digest")
        reference_digest = reference.rsplit("@", 1)[-1] if "@" in reference else None
        digest = declared_digest if isinstance(declared_digest, str) and IMAGE_DIGEST_PATTERN.fullmatch(declared_digest) else reference_digest
        if not isinstance(digest, str) or not IMAGE_DIGEST_PATTERN.fullmatch(digest):
            raise OwnershipError(f"image {name} has no verified immutable digest")
