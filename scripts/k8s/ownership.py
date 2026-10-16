"""Pure identity, ownership-receipt, and lock validation for K8S-B1.

This module never reads kubeconfig, contacts a cluster, or runs a command.
Future execution code must keep those operations separate and validate a
complete receipt before any destructive action.
"""

from __future__ import annotations

from dataclasses import dataclass
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
    r"(?:\b(?:unresolved|placeholder|unknown|pending|incomplete|example|unspecified|dummy|changeme)\b|"
    r"\btbd\b|\btodo\b|\bnot[-_ ]?set\b|\bn/?a\b|\breplace[-_ ]?me\b)",
    re.IGNORECASE,
)
REQUIRED_IMAGE_ROLES = frozenset(
    {"mongo", "spark_base", "ui_base", "api_application_image", "ui_application_image"}
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
    for field in ("kubernetes_version", "minikube_version", "driver", "container_runtime", "cni", "cni_version"):
        _require_concrete_string(cluster, field, f"cluster.{field}")
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
    architecture = cluster.get("architecture")
    if not isinstance(architecture, str) or not ARCHITECTURE_PATTERN.fullmatch(architecture):
        raise OwnershipError("cluster.architecture must identify a supported Linux architecture")
    if type(cluster.get("node_count")) is not int or cluster["node_count"] < 1:
        raise OwnershipError("version lock node_count must be a positive integer")

    host = lock.get("host_capacity")
    if not isinstance(host, Mapping):
        raise OwnershipError("version lock is missing measured host_capacity")
    for field in ("cpu_cores", "memory_gib", "free_disk_gib"):
        value = host.get(field)
        try:
            finite = math.isfinite(float(value))
        except (OverflowError, TypeError, ValueError):
            finite = False
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not finite:
            raise OwnershipError(f"host_capacity.{field} must be a measured number")
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

    dependencies = lock.get("python_dependencies")
    if not isinstance(dependencies, Mapping):
        raise OwnershipError("version lock is missing python_dependencies")
    for field in ("api_python", "ui_python", "spark_parent_python"):
        version = _require_concrete_string(dependencies, field, f"python_dependencies.{field}")
        if not PYTHON_VERSION_PATTERN.fullmatch(version):
            raise OwnershipError(f"python_dependencies.{field} must be an exact Python version")
    environment_digests = dependencies.get("resolved_environment_digests")
    if not isinstance(environment_digests, Mapping) or not REQUIRED_RESOLVED_ENVIRONMENTS.issubset(environment_digests):
        raise OwnershipError("version lock is missing resolved Python environment identities")
    for role in REQUIRED_RESOLVED_ENVIRONMENTS:
        digest = environment_digests[role]
        if not isinstance(digest, str) or not IMAGE_DIGEST_PATTERN.fullmatch(digest):
            raise OwnershipError(f"python_dependencies.{role} environment digest is malformed")

    toolchain = lock.get("toolchain")
    if not isinstance(toolchain, Mapping):
        raise OwnershipError("version lock is missing the toolchain section")
    for field in ("kubectl_version", "docker_engine_version"):
        _require_concrete_string(toolchain, field, f"toolchain.{field}")
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
    for field in ("default_storage_class", "provisioner", "volume_binding_mode"):
        _require_concrete_string(storage, field, f"storage.{field}")
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
