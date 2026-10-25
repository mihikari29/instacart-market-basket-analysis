"""Pure identity, ownership-receipt, and lock validation for K8S-B1.

This module never reads kubeconfig, contacts a cluster, or runs a command.
Future execution code must keep those operations separate and validate a
complete receipt before any destructive action.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import math
import re
import secrets
from typing import Any, Mapping, Sequence


RUN_ID_PATTERN = re.compile(r"[0-9a-f]{12}\Z")
NAMESPACE_PATTERN = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z")
IMAGE_DIGEST_PATTERN = re.compile(r"sha256:[0-9a-f]{64}\Z")
HEX_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
IMAGE_NAME_TAG_PATTERN = re.compile(
    r"[a-z0-9]+(?:[._-][a-z0-9]+)*(?::[0-9]+)?"
    r"(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)*"
    r"(?::[A-Za-z0-9_][A-Za-z0-9_.-]{0,127})?\Z"
)
KUBERNETES_VERSION_PATTERN = re.compile(r"v\d+\.\d+\.\d+\Z")
ARCHITECTURE_PATTERN = re.compile(r"linux/(?:amd64|arm64|ppc64le|s390x)\Z")
PYTHON_VERSION_PATTERN = re.compile(r"\d+\.\d+\.\d+\Z")
PLACEHOLDER_PATTERN = re.compile(
    r"(?<![A-Za-z0-9])(?:unresolved|placeholder|unknown|pending|incomplete|example|unspecified|dummy|changeme|"
    r"tbd|todo|not[-_ ]?set|n/?a|replace[-_ ]?me)(?![A-Za-z0-9])",
    re.IGNORECASE,
)
REQUIRED_IMAGE_ROLES = frozenset(
    {"mongo", "spark_base", "ui_base", "api_application_image", "ui_application_image"}
)
REQUIRED_SERVING_RUNTIME_IMAGE_ROLES = frozenset(
    {"mongo", "api_application_image", "ui_application_image"}
)
REQUIRED_RESOLVED_ENVIRONMENTS = frozenset({"api", "ui", "spark_parent"})
REQUIRED_CALICO_IMAGES = frozenset({"cni", "node", "kube-controllers"})
REQUIRED_CONTROL_PLANE_IMAGES = frozenset(
    {"kube-apiserver", "kube-controller-manager", "kube-scheduler", "kube-proxy", "etcd", "pause", "coredns"}
)
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


@dataclass(frozen=True, slots=True)
class OwnerReferenceIdentity:
    """Kubernetes owner reference captured from a live object."""

    kind: str
    name: str
    uid: str
    controller: bool


@dataclass(frozen=True, slots=True)
class RuntimeObjectIdentity:
    """A directly declared object or controller-created child observed live."""

    kind: str
    name: str
    namespace: str
    uid: str
    labels: Mapping[str, str]
    owner_references: Sequence[OwnerReferenceIdentity] = ()


@dataclass(frozen=True, slots=True)
class RuntimeStorageIdentity:
    """PVC/PV/StorageClass identity plus evidence that the volume is profile-local."""

    claim_name: str
    claim_uid: str
    volume_name: str
    volume_uid: str
    pv_claim_namespace: str
    pv_claim_name: str
    pv_claim_uid: str
    storage_class: str
    storage_class_uid: str
    provisioner: str
    requested_storage: str
    capacity: str
    access_mode: str
    profile_local: bool
    locality_kind: str
    locality_path: str
    claim_owner_uid: str
    locality_node_name: str
    locality_node_uid: str
    claim_template_name: str
    retention_when_deleted: str
    retention_when_scaled: str


@dataclass(frozen=True, slots=True)
class RuntimeImageIdentity:
    """Declared image role and the immutable digest reported by a running container."""

    role: str
    pod_uid: str
    container_name: str
    declared_reference: str
    runtime_image_id: str
    digest: str
    config_digest: str


@dataclass(frozen=True, slots=True)
class RuntimeOwnershipReceipt:
    """Complete live identity evidence required before a scoped destructive action."""

    profile: str
    context: str
    cluster_uid: str
    namespace: str
    namespace_uid: str
    namespace_labels: Mapping[str, str]
    objects: Sequence[RuntimeObjectIdentity]
    storage: RuntimeStorageIdentity
    images: Sequence[RuntimeImageIdentity]
    cluster_images: Mapping[str, str] = field(default_factory=dict)


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
    for attribute, expected in DECLARED_STORAGE_OWNERSHIP.items():
        if asdict(storage).get(attribute) != expected:
            raise OwnershipError(f"storage receipt field {attribute} does not match the declaration")
    pvc = actual[("PersistentVolumeClaim", DECLARED_STORAGE_OWNERSHIP["claim_name"])]
    if storage.namespace != identity.namespace or storage.claim_uid != pvc.uid:
        raise OwnershipError("PVC namespace or UID does not match the owned inventory")
    if not storage.volume_name or not storage.volume_uid:
        raise OwnershipError("bound PV name and UID are required in the storage receipt")
    if storage.volume_uid in observed_uids:
        raise OwnershipError("bound PV UID conflicts with another owned object")


_EXPECTED_CONTROLLER_CHILDREN = frozenset(
    {
        ("Pod", "module4d-mongo-0", "StatefulSet", "module4d-mongo"),
        ("ReplicaSet", "module4d-api-", "Deployment", "module4d-api"),
        ("Pod", "module4d-api-", "ReplicaSet", "module4d-api-"),
        ("ReplicaSet", "module4d-ui-", "Deployment", "module4d-ui"),
        ("Pod", "module4d-ui-", "ReplicaSet", "module4d-ui-"),
    }
)


def validate_runtime_receipt(
    identity: RunIdentity,
    receipt: RuntimeOwnershipReceipt,
    *,
    expected_images: Mapping[str, str],
    expected_cluster_images: Mapping[str, str],
    expected_storage_class: str,
    expected_provisioner: str,
) -> None:
    """Fail closed unless live objects, owner chains, images, and storage agree."""
    if not isinstance(receipt, RuntimeOwnershipReceipt):
        raise OwnershipError("complete runtime ownership receipt is required")
    validate_target(
        identity,
        profile=receipt.profile,
        context=receipt.context,
        namespace=receipt.namespace,
    )
    if not isinstance(receipt.cluster_uid, str) or not receipt.cluster_uid.strip():
        raise OwnershipError("live cluster UID is required")
    if not isinstance(receipt.namespace_uid, str) or not receipt.namespace_uid.strip():
        raise OwnershipError("live namespace UID is required")
    if not isinstance(receipt.namespace_labels, Mapping) or any(
        receipt.namespace_labels.get(key) != value for key, value in identity.labels.items()
    ):
        raise OwnershipError("live namespace ownership labels do not match")
    if not isinstance(receipt.objects, Sequence) or isinstance(receipt.objects, (str, bytes)):
        raise OwnershipError("live object inventory must be a sequence")

    observed: dict[tuple[str, str], RuntimeObjectIdentity] = {}
    uids = {receipt.cluster_uid, receipt.namespace_uid}
    for item in receipt.objects:
        if not isinstance(item, RuntimeObjectIdentity):
            raise OwnershipError("live object inventory contains an invalid record")
        if not all(isinstance(value, str) and value.strip() for value in (item.kind, item.name, item.uid)):
            raise OwnershipError("live object kind, name, and UID are required")
        if item.namespace != identity.namespace:
            raise OwnershipError("live object belongs to a different namespace")
        if item.uid in uids:
            raise OwnershipError("live object UIDs must be unique")
        uids.add(item.uid)
        if not isinstance(item.labels, Mapping) or any(
            item.labels.get(key) != value for key, value in identity.labels.items()
        ):
            raise OwnershipError(f"live ownership labels do not match for {item.kind}/{item.name}")
        key = (item.kind, item.name)
        if key in observed:
            raise OwnershipError("live object inventory contains a duplicate")
        if not isinstance(item.owner_references, Sequence) or isinstance(item.owner_references, (str, bytes)):
            raise OwnershipError("live owner references must be an explicit sequence")
        observed[key] = item

    expected_direct = ALLOWED_RESOURCE_INVENTORY | frozenset(
        {
            ("ConfigMap", "module4d-mongo-bootstrap"),
            ("Secret", "module4d-mongo-admin"),
            ("Secret", "module4d-mongo-accounts"),
            ("Secret", "module4d-api-readonly"),
            ("Secret", "module4d-mongo-publisher"),
        }
    )
    # A PVC made from the StatefulSet claim template is controller-created.
    direct_actual = set(observed).difference(
        key for key in observed if key[0] in {"Pod", "ReplicaSet"}
    )
    if direct_actual != expected_direct:
        raise OwnershipError("live direct-resource inventory is incomplete or contains an unapproved object")
    matched_child_keys: set[tuple[str, str]] = set()
    for kind, prefix, owner_kind, owner_prefix in _EXPECTED_CONTROLLER_CHILDREN:
        candidates = [
            item for item in observed.values()
            if item.kind == kind and (item.name == prefix if not prefix.endswith("-") else item.name.startswith(prefix))
        ]
        if len(candidates) != 1:
            raise OwnershipError(f"expected exactly one controller child for {owner_kind}/{owner_prefix}")
        child = candidates[0]
        matched_child_keys.add((child.kind, child.name))
        if len(child.owner_references) != 1:
            raise OwnershipError(f"{child.kind}/{child.name} must have exactly one owner reference")
        owner = child.owner_references[0]
        parent_candidates = [
            parent for parent in observed.values()
            if parent.kind == owner_kind
            and (parent.name == owner_prefix if not owner_prefix.endswith("-") else parent.name.startswith(owner_prefix))
        ]
        if (
            not isinstance(owner, OwnerReferenceIdentity)
            or not owner.controller
            or len(parent_candidates) != 1
            or owner.kind != owner_kind
            or owner.name != parent_candidates[0].name
            or owner.uid != parent_candidates[0].uid
        ):
            raise OwnershipError(f"owner reference does not bind {child.kind}/{child.name} to its controller")
    dynamic_child_keys = {key for key in observed if key[0] in {"Pod", "ReplicaSet"}}
    if dynamic_child_keys != matched_child_keys:
        raise OwnershipError("live inventory contains an unapproved controller-created child")

    storage = receipt.storage
    if not isinstance(storage, RuntimeStorageIdentity):
        raise OwnershipError("live PVC/PV/StorageClass receipt is required")
    storage_strings = (
        storage.claim_name,
        storage.claim_uid,
        storage.volume_name,
        storage.volume_uid,
        storage.pv_claim_namespace,
        storage.pv_claim_name,
        storage.pv_claim_uid,
        storage.storage_class,
        storage.storage_class_uid,
        storage.provisioner,
        storage.requested_storage,
        storage.capacity,
        storage.access_mode,
        storage.locality_kind,
        storage.locality_path,
        storage.claim_owner_uid,
        storage.locality_node_name,
        storage.locality_node_uid,
        storage.claim_template_name,
        storage.retention_when_deleted,
        storage.retention_when_scaled,
    )
    if not all(isinstance(value, str) and value.strip() for value in storage_strings):
        raise OwnershipError("live PVC/PV/StorageClass identity fields are incomplete")
    pvc = observed.get(("PersistentVolumeClaim", storage.claim_name))
    sts = observed.get(("StatefulSet", "module4d-mongo"))
    pvc_owners = pvc.owner_references if pvc is not None else ()
    pvc_owner_matches = (
        not pvc_owners
        or (
            len(pvc_owners) == 1
            and isinstance(pvc_owners[0], OwnerReferenceIdentity)
            and pvc_owners[0].kind == "StatefulSet"
            and pvc_owners[0].name == "module4d-mongo"
            and sts is not None
            and pvc_owners[0].uid == sts.uid
            and pvc_owners[0].controller is True
        )
    )
    if (
        pvc is None
        or sts is None
        or not pvc_owner_matches
        or pvc.labels.get("app.kubernetes.io/name") != "module4d-mongo"
        or pvc.labels.get("app.kubernetes.io/component") != "database"
        or storage.claim_name != "data-module4d-mongo-0"
        or storage.claim_uid != pvc.uid
        or storage.pv_claim_namespace != identity.namespace
        or storage.pv_claim_name != storage.claim_name
        or storage.pv_claim_uid != pvc.uid
        or storage.claim_owner_uid != sts.uid
        or storage.claim_template_name != "data"
        or storage.retention_when_deleted != "Retain"
        or storage.retention_when_scaled != "Retain"
        or storage.volume_name.strip() == ""
        or storage.volume_uid.strip() == ""
        or storage.volume_uid in uids
        or storage.storage_class_uid in uids
        or storage.locality_node_uid in uids
        or len({storage.volume_uid, storage.storage_class_uid, storage.locality_node_uid}) != 3
        or storage.storage_class != expected_storage_class
        or storage.provisioner != expected_provisioner
        or storage.storage_class_uid.strip() == ""
        or storage.requested_storage != "2Gi"
        or storage.capacity != "2Gi"
        or storage.access_mode != "ReadWriteOnce"
        or storage.profile_local is not True
        or storage.locality_kind != "hostPath"
        or not storage.locality_path.startswith("/tmp/hostpath-provisioner/")
        or not storage.locality_node_name.strip()
        or not storage.locality_node_uid.strip()
    ):
        raise OwnershipError("live PVC/PV/StorageClass locality contract does not match")

    if not isinstance(expected_images, Mapping) or set(expected_images) != set(REQUIRED_SERVING_RUNTIME_IMAGE_ROLES):
        raise OwnershipError("expected immutable identities for every running serving image role are required")
    images = receipt.images
    if not isinstance(images, Sequence) or isinstance(images, (str, bytes)):
        raise OwnershipError("live image identities must be a sequence")
    if len(images) != len(REQUIRED_SERVING_RUNTIME_IMAGE_ROLES):
        raise OwnershipError("live image receipt is incomplete")
    seen_roles = set()
    role_pod_names = {
        "mongo": ("module4d-mongo-0",),
        "api_application_image": ("module4d-api-",),
        "ui_application_image": ("module4d-ui-",),
    }
    pod_name_by_uid = {item.uid: item.name for item in observed.values() if item.kind == "Pod"}
    for image in images:
        if (
            not isinstance(image, RuntimeImageIdentity)
            or not isinstance(image.role, str)
            or image.role in seen_roles
        ):
            raise OwnershipError("live image receipt contains an invalid or duplicate role")
        seen_roles.add(image.role)
        if (
            image.role not in expected_images
            or not isinstance(image.pod_uid, str)
            or image.pod_uid not in pod_name_by_uid
            or not isinstance(image.container_name, str)
            or not any(
                pod_name_by_uid[image.pod_uid] == name
                if not name.endswith("-")
                else pod_name_by_uid[image.pod_uid].startswith(name)
                for name in role_pod_names.get(image.role, ())
            )
            or not image.container_name.strip()
            or not isinstance(image.declared_reference, str)
            or not isinstance(image.runtime_image_id, str)
            or not isinstance(image.digest, str)
            or not isinstance(image.config_digest, str)
            or image.digest != expected_images[image.role]
            or not IMAGE_DIGEST_PATTERN.fullmatch(image.digest)
            or not IMAGE_DIGEST_PATTERN.fullmatch(image.config_digest)
            or not (
                f"@{image.digest}" in image.runtime_image_id
                or image.runtime_image_id.rstrip("/").endswith(image.config_digest)
            )
            or "@" not in image.declared_reference
            or image.declared_reference.rsplit("@", 1)[-1] != image.digest
        ):
            raise OwnershipError(f"running image identity does not match the lock for {image.role}")
    required_cluster_roles = REQUIRED_CONTROL_PLANE_IMAGES | REQUIRED_CALICO_IMAGES
    if (
        not isinstance(expected_cluster_images, Mapping)
        or set(expected_cluster_images) != set(required_cluster_roles)
        or not isinstance(receipt.cluster_images, Mapping)
        or set(receipt.cluster_images) != set(required_cluster_roles)
    ):
        raise OwnershipError("complete Kubernetes and Calico image identities are required in the receipt")
    if dict(receipt.cluster_images) != dict(expected_cluster_images):
        raise OwnershipError("cluster component image identities do not match the version lock")
    for role, reference in receipt.cluster_images.items():
        _validate_digest_reference(reference, f"receipt.cluster_images.{role}")


def assert_receipt_matches_live(
    identity: RunIdentity,
    recorded: RuntimeOwnershipReceipt,
    live: RuntimeOwnershipReceipt,
    *,
    expected_images: Mapping[str, str],
    expected_cluster_images: Mapping[str, str],
    expected_storage_class: str,
    expected_provisioner: str,
) -> None:
    """Reject teardown or resume when any live UID, owner, image, or volume has drifted."""
    validate_runtime_receipt(
        identity,
        recorded,
        expected_images=expected_images,
        expected_cluster_images=expected_cluster_images,
        expected_storage_class=expected_storage_class,
        expected_provisioner=expected_provisioner,
    )
    validate_runtime_receipt(
        identity,
        live,
        expected_images=expected_images,
        expected_cluster_images=expected_cluster_images,
        expected_storage_class=expected_storage_class,
        expected_provisioner=expected_provisioner,
    )
    if (
        recorded.profile != live.profile
        or recorded.context != live.context
        or recorded.cluster_uid != live.cluster_uid
        or recorded.namespace != live.namespace
        or recorded.namespace_uid != live.namespace_uid
        or dict(recorded.namespace_labels) != dict(live.namespace_labels)
        or {(item.kind, item.name): item for item in recorded.objects}
        != {(item.kind, item.name): item for item in live.objects}
        or recorded.storage != live.storage
        or {item.role: item for item in recorded.images} != {item.role: item for item in live.images}
        or dict(recorded.cluster_images) != dict(live.cluster_images)
    ):
        raise OwnershipError("live identities do not match the recorded ownership receipt")


def validate_version_lock(lock: Mapping[str, Any]) -> None:
    """Require every runtime identity needed by a future B2 deployment lock."""
    if not isinstance(lock, Mapping):
        raise OwnershipError("version lock must be a mapping")
    if type(lock.get("schema_version")) is not int or lock["schema_version"] != 1:
        raise OwnershipError("version lock schema_version must be 1")
    if lock.get("status") != "complete":
        raise OwnershipError("version lock is incomplete; deployment preflight must stop")
    unresolved = lock.get("unresolved")
    if not isinstance(unresolved, list) or unresolved:
        raise OwnershipError("version lock has unresolved runtime identities")
    cluster = lock.get("cluster")
    if not isinstance(cluster, Mapping):
        raise OwnershipError("version lock is missing the cluster section")
    for key in ("kubernetes_version", "minikube_version", "driver", "container_runtime", "cni", "cni_version"):
        _require_concrete_string(cluster, key, f"cluster.{key}")
    if not KUBERNETES_VERSION_PATTERN.fullmatch(cluster["kubernetes_version"]):
        raise OwnershipError("cluster.kubernetes_version must be an exact vMAJOR.MINOR.PATCH version")
    if not KUBERNETES_VERSION_PATTERN.fullmatch(cluster["minikube_version"]):
        raise OwnershipError("cluster.minikube_version must be an exact vMAJOR.MINOR.PATCH version")
    if not KUBERNETES_VERSION_PATTERN.fullmatch(cluster["cni_version"]):
        raise OwnershipError("cluster.cni_version must be an exact vMAJOR.MINOR.PATCH version")
    if cluster["driver"].lower() != "docker" or cluster["container_runtime"].lower() != "containerd":
        raise OwnershipError("cluster driver and container runtime do not match the B1 Docker/containerd contract")
    if cluster["cni"].lower() != "calico":
        raise OwnershipError("cluster.cni must match the required Calico policy-enforcing CNI")
    for key in ("profile", "context", "cluster_uid"):
        _require_concrete_string(cluster, key, f"cluster.{key}")
    if cluster["profile"] != cluster["context"] or not re.fullmatch(
        r"it4931-k8s-[0-9a-f]{12}", cluster["profile"]
    ):
        raise OwnershipError("cluster profile and context must match the dedicated run identity")
    architecture = cluster.get("architecture")
    if not isinstance(architecture, str) or not ARCHITECTURE_PATTERN.fullmatch(architecture):
        raise OwnershipError("cluster.architecture must identify a supported Linux architecture")
    if type(cluster.get("node_count")) is not int or cluster["node_count"] < 1:
        raise OwnershipError("version lock node_count must be a positive integer")

    host = lock.get("host_capacity")
    if not isinstance(host, Mapping):
        raise OwnershipError("version lock is missing measured host_capacity")
    for key in ("cpu_cores", "memory_gib", "free_disk_gib"):
        value = host.get(key)
        try:
            finite = math.isfinite(float(value))
        except (OverflowError, TypeError, ValueError):
            finite = False
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not finite:
            raise OwnershipError(f"host_capacity.{key} must be a measured number")
    if host["cpu_cores"] < 4 or host["memory_gib"] < 6 or host["free_disk_gib"] < 20:
        raise OwnershipError("measured host capacity is below the documented B2 minimum")
    _require_concrete_string(host, "measured_at", "host_capacity.measured_at")

    images = lock.get("images")
    if not isinstance(images, Mapping) or not REQUIRED_IMAGE_ROLES.issubset(images):
        raise OwnershipError("version lock is missing one or more required image roles")
    for name, image in images.items():
        if not isinstance(name, str) or not isinstance(image, Mapping):
            raise OwnershipError("each image lock entry must be an object")
        reference = image.get("reference")
        _require_concrete_string(image, "reference", f"images.{name}.reference")
        if reference.endswith(":latest"):
            raise OwnershipError(f"image {name} must use an explicit non-latest reference")
        declared_digest = image.get("immutable_digest")
        reference_digest = _reference_digest(reference, f"images.{name}.reference")
        if declared_digest is not None:
            if not isinstance(declared_digest, str) or not IMAGE_DIGEST_PATTERN.fullmatch(declared_digest):
                raise OwnershipError(f"image {name} immutable_digest is malformed")
            if reference_digest is not None and declared_digest != reference_digest:
                raise OwnershipError(f"image {name} reference and immutable_digest conflict")
        elif reference_digest is None:
            raise OwnershipError(f"image {name} has no immutable digest")

    provenance = lock.get("image_provenance")
    if not isinstance(provenance, Mapping) or not {"api_application_image", "ui_application_image"}.issubset(provenance):
        raise OwnershipError("version lock is missing application image build provenance")
    for role in ("api_application_image", "ui_application_image"):
        record = provenance.get(role)
        if not isinstance(record, Mapping):
            raise OwnershipError(f"image_provenance.{role} is required")
        _require_concrete_string(record, "source_revision", f"image_provenance.{role}.source_revision")
        if not re.fullmatch(r"[0-9a-f]{40}", record["source_revision"]):
            raise OwnershipError(f"image_provenance.{role}.source_revision must be a full Git SHA")
        if record.get("source_dirty") is not False:
            raise OwnershipError(f"image_provenance.{role}.source_dirty must be false")
        for key in ("source_tree_sha256", "build_metadata_sha256"):
            value = record.get(key)
            if not isinstance(value, str) or not HEX_SHA256_PATTERN.fullmatch(value):
                raise OwnershipError(f"image_provenance.{role}.{key} must be a SHA-256")
        config_digest = record.get("config_digest")
        if not isinstance(config_digest, str) or not IMAGE_DIGEST_PATTERN.fullmatch(config_digest):
            raise OwnershipError(f"image_provenance.{role}.config_digest must be an OCI SHA-256")
        digest = record.get("image_digest")
        if not isinstance(digest, str) or not IMAGE_DIGEST_PATTERN.fullmatch(digest):
            raise OwnershipError(f"image_provenance.{role}.image_digest is malformed")
        if images[role].get("immutable_digest", _reference_digest(images[role]["reference"], f"images.{role}.reference")) != digest:
            raise OwnershipError(f"image_provenance.{role}.image_digest does not match the image lock")
        python_version = record.get("python_version")
        if not isinstance(python_version, str) or not PYTHON_VERSION_PATTERN.fullmatch(python_version):
            raise OwnershipError(f"image_provenance.{role}.python_version must be exact")
        environment_digest = record.get("python_environment_sha256")
        if not isinstance(environment_digest, str) or not HEX_SHA256_PATTERN.fullmatch(environment_digest):
            raise OwnershipError(f"image_provenance.{role}.python_environment_sha256 must be a SHA-256")
        platform = record.get("platform")
        if platform != cluster["architecture"]:
            raise OwnershipError(f"image_provenance.{role}.platform does not match the Kubernetes node")

    dependencies = lock.get("python_dependencies")
    if not isinstance(dependencies, Mapping):
        raise OwnershipError("version lock is missing python_dependencies")
    for key in ("api_python", "ui_python", "spark_parent_python"):
        version = _require_concrete_string(dependencies, key, f"python_dependencies.{key}")
        if not PYTHON_VERSION_PATTERN.fullmatch(version):
            raise OwnershipError(f"python_dependencies.{key} must be an exact Python version")
    environment_digests = dependencies.get("resolved_environment_digests")
    if not isinstance(environment_digests, Mapping) or not REQUIRED_RESOLVED_ENVIRONMENTS.issubset(environment_digests):
        raise OwnershipError("version lock is missing resolved Python environment identities")
    for role in REQUIRED_RESOLVED_ENVIRONMENTS:
        digest = environment_digests[role]
        if not isinstance(digest, str) or not IMAGE_DIGEST_PATTERN.fullmatch(digest):
            raise OwnershipError(f"python_dependencies.{role} environment digest is malformed")
    for role, environment_role in (
        ("api_application_image", "api"),
        ("ui_application_image", "ui"),
    ):
        record = provenance[role]
        if environment_digests[environment_role] != "sha256:" + record["python_environment_sha256"]:
            raise OwnershipError(f"python_dependencies.{environment_role} does not match image provenance")
    api_image_provenance = provenance["api_application_image"]
    ui_image_provenance = provenance["ui_application_image"]
    if (
        api_image_provenance["source_revision"] != ui_image_provenance["source_revision"]
        or api_image_provenance["source_tree_sha256"] != ui_image_provenance["source_tree_sha256"]
    ):
        raise OwnershipError("API and UI images must share one clean source revision and tree")
    api_provenance = api_image_provenance
    if (
        dependencies["spark_parent_python"] != api_provenance["python_version"]
        or environment_digests["spark_parent"] != "sha256:" + api_provenance["python_environment_sha256"]
    ):
        raise OwnershipError("Spark parent Python environment does not match the API runner base")

    toolchain = lock.get("toolchain")
    if not isinstance(toolchain, Mapping):
        raise OwnershipError("version lock is missing the toolchain section")
    for key in ("minikube_version", "kubectl_version", "docker_engine_version"):
        _require_concrete_string(toolchain, key, f"toolchain.{key}")
    if toolchain["minikube_version"] != cluster["minikube_version"]:
        raise OwnershipError("toolchain Minikube version does not match the cluster lock")
    minikube_sha = toolchain.get("minikube_binary_sha256")
    if not isinstance(minikube_sha, str) or not HEX_SHA256_PATTERN.fullmatch(minikube_sha):
        raise OwnershipError("toolchain.minikube_binary_sha256 must be a verified SHA-256 checksum")
    control_plane = toolchain.get("kubernetes_control_plane_images")
    if not isinstance(control_plane, Mapping) or not REQUIRED_CONTROL_PLANE_IMAGES.issubset(control_plane):
        raise OwnershipError("toolchain is missing required Kubernetes control-plane image roles")
    for role, reference in control_plane.items():
        _validate_digest_reference(reference, f"toolchain.kubernetes_control_plane_images.{role}")
    calico_images = toolchain.get("calico_images")
    if not isinstance(calico_images, Mapping) or not REQUIRED_CALICO_IMAGES.issubset(calico_images):
        raise OwnershipError("toolchain is missing required Calico image roles")
    for role in REQUIRED_CALICO_IMAGES:
        _validate_digest_reference(calico_images[role], f"toolchain.calico_images.{role}")

    storage = lock.get("storage")
    if not isinstance(storage, Mapping):
        raise OwnershipError("version lock is missing the storage section")
    for key in ("default_storage_class", "provisioner", "volume_binding_mode"):
        _require_concrete_string(storage, key, f"storage.{key}")
    _require_concrete_string(storage, "storage_class_uid", "storage.storage_class_uid")
    if storage.get("access_mode") != "ReadWriteOnce" or storage.get("capacity") != "2Gi":
        raise OwnershipError("storage contract must match the 2Gi ReadWriteOnce Mongo claim")
    if storage.get("profile_local") is not True:
        raise OwnershipError("storage provisioner must be verified as profile-local")
    if storage["volume_binding_mode"] not in {"Immediate", "WaitForFirstConsumer"}:
        raise OwnershipError("storage.volume_binding_mode is invalid")


def _require_concrete_string(mapping: Mapping[str, Any], key: str, field: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value.strip() or PLACEHOLDER_PATTERN.search(value):
        raise OwnershipError(f"version lock field {field} is incomplete")
    return value


def _reference_digest(reference: str, field: str) -> str | None:
    if "@" not in reference:
        image_name = reference
        digest = None
    else:
        image_name, digest = reference.rsplit("@", 1)
    if (
        not IMAGE_NAME_TAG_PATTERN.fullmatch(image_name)
        or (digest is not None and not IMAGE_DIGEST_PATTERN.fullmatch(digest))
        or (digest is None and ":" not in image_name.rsplit("/", 1)[-1])
    ):
        raise OwnershipError(f"version lock field {field} has a malformed image digest")
    return digest


def _validate_digest_reference(reference: Any, field: str) -> None:
    if not isinstance(reference, str) or PLACEHOLDER_PATTERN.search(reference):
        raise OwnershipError(f"version lock field {field} is incomplete")
    if _reference_digest(reference, field) is None:
        raise OwnershipError(f"version lock field {field} must include an immutable digest")
