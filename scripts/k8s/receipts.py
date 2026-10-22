"""Collect sanitized live Kubernetes ownership and storage receipts."""

from __future__ import annotations

from dataclasses import asdict
from collections.abc import Mapping
import json
from typing import Any

from .image_provenance import collect_cluster_component_images, verify_running_image_id
from .lock_lifecycle import validate_static_lock
from .ownership import (
    IMAGE_DIGEST_PATTERN,
    IMAGE_NAME_TAG_PATTERN,
    OwnershipError,
    OwnerReferenceIdentity,
    REQUIRED_SERVING_RUNTIME_IMAGE_ROLES,
    RunIdentity,
    RuntimeImageIdentity,
    RuntimeObjectIdentity,
    RuntimeOwnershipReceipt,
    RuntimeStorageIdentity,
    validate_version_lock,
)
from .runtime import (
    CommandExecutor,
    checked,
    json_output,
    collect_cluster_identity,
    collect_namespace_identity,
    containerd_image_inventory_command,
    kubectl,
)


class ReceiptCollectionError(RuntimeError):
    """Live ownership evidence could not be collected completely."""


_OWNERSHIP_RECEIPT_SCHEMA_VERSION = 2




def pv_spec_capacity(pv: Mapping[str, Any]) -> str:
    """Return the storage capacity a bound persistent volume requests."""
    spec = pv.get("spec")
    capacity = spec.get("capacity") if isinstance(spec, Mapping) else None
    value = capacity.get("storage") if isinstance(capacity, Mapping) else None
    if not isinstance(value, str) or not value:
        raise ReceiptCollectionError("bound PV spec does not report storage capacity")
    return value


def _locked_image_digest(image: Mapping[str, Any]) -> str:
    """Resolve one strict digest from either accepted lock representation."""
    if not isinstance(image, Mapping):
        raise ReceiptCollectionError("locked image record is malformed")
    immutable = image.get("immutable_digest")
    reference = image.get("reference")
    referenced = None
    if reference is not None:
        if not isinstance(reference, str):
            raise ReceiptCollectionError("locked image reference is malformed")
        parts = reference.split("@")
        if len(parts) == 2:
            if not IMAGE_NAME_TAG_PATTERN.fullmatch(parts[0]) or not IMAGE_DIGEST_PATTERN.fullmatch(parts[1]):
                raise ReceiptCollectionError("locked image reference or digest is malformed")
            referenced = parts[1]
        elif len(parts) != 1 or not IMAGE_NAME_TAG_PATTERN.fullmatch(reference):
            raise ReceiptCollectionError("locked image reference is malformed")
    if immutable is not None and (
        not isinstance(immutable, str) or not IMAGE_DIGEST_PATTERN.fullmatch(immutable)
    ):
        raise ReceiptCollectionError("locked immutable image digest is malformed")
    if immutable is not None and referenced is not None and immutable != referenced:
        raise ReceiptCollectionError("locked image reference and immutable digest disagree")
    digest = immutable if immutable is not None else referenced
    if not isinstance(digest, str) or not IMAGE_DIGEST_PATTERN.fullmatch(digest):
        raise ReceiptCollectionError("locked image has no immutable SHA-256 digest")
    return digest


def _locked_role_digest(lock: Mapping[str, Any], role: str) -> str:
    try:
        digest = _locked_image_digest(lock["images"][role])
    except (KeyError, TypeError) as exc:
        raise ReceiptCollectionError(f"lock is missing the image digest for {role}") from exc
    provenance = lock.get("image_provenance", {})
    if not isinstance(provenance, Mapping):
        raise ReceiptCollectionError("image provenance is malformed")
    role_provenance = provenance.get(role, {})
    if not isinstance(role_provenance, Mapping):
        raise ReceiptCollectionError(f"image provenance is malformed for {role}")
    provenance_digest = role_provenance.get("image_digest")
    if provenance_digest is not None and (
        not isinstance(provenance_digest, str)
        or not IMAGE_DIGEST_PATTERN.fullmatch(provenance_digest)
        or provenance_digest != digest
    ):
        raise ReceiptCollectionError(f"image provenance digest conflicts with the lock for {role}")
    return digest


def _validate_mongo_claim_template(
    statefulset: Mapping[str, Any], observed_pvc_labels: Any,
) -> None:
    """Verify the exact Retain PVC template that justifies an ownerless claim."""
    spec = statefulset.get("spec")
    templates = spec.get("volumeClaimTemplates") if isinstance(spec, Mapping) else None
    if not isinstance(templates, list) or len(templates) != 1 or not isinstance(templates[0], Mapping):
        raise ReceiptCollectionError("Mongo StatefulSet claim template inventory is invalid")
    template = templates[0]
    metadata, claim_spec = template.get("metadata"), template.get("spec")
    labels = metadata.get("labels") if isinstance(metadata, Mapping) else None
    retention = spec.get("persistentVolumeClaimRetentionPolicy", {})
    resources = claim_spec.get("resources") if isinstance(claim_spec, Mapping) else None
    requests = resources.get("requests") if isinstance(resources, Mapping) else None
    if not isinstance(retention, Mapping):
        raise ReceiptCollectionError("Mongo StatefulSet claim retention policy is invalid")
    # Kubernetes defaults both StatefulSet PVC retention actions to Retain.
    if (
        not isinstance(metadata, Mapping)
        or metadata.get("name") != "data"
        or not isinstance(labels, Mapping)
        or labels.get("app.kubernetes.io/name") != "module4d-mongo"
        or labels.get("app.kubernetes.io/component") != "database"
        or not isinstance(observed_pvc_labels, Mapping)
        or dict(labels) != dict(observed_pvc_labels)
        or not isinstance(claim_spec, Mapping)
        or claim_spec.get("accessModes") != ["ReadWriteOnce"]
        or not isinstance(requests, Mapping)
        or requests.get("storage") != "2Gi"
        or retention.get("whenDeleted", "Retain") != "Retain"
        or retention.get("whenScaled", "Retain") != "Retain"
    ):
        raise ReceiptCollectionError("Mongo StatefulSet claim template or Retain policy differs from the B2 contract")


_NAMESPACED_RESOURCE_TYPES = (
    "resourcequotas",
    "limitranges",
    "serviceaccounts",
    "configmaps",
    "services",
    "statefulsets",
    "deployments",
    "networkpolicies",
    "persistentvolumeclaims",
    "replicasets",
    "pods",
)

# Kubernetes creates these two namespace support objects automatically. They
# are not workload-owned resources and are intentionally outside the teardown
# inventory; any other unapproved object still fails exact receipt validation.
_SYSTEM_NAMESPACE_OBJECTS = {
    ("ServiceAccount", "default"),
    ("ConfigMap", "kube-root-ca.crt"),
}

_SECRET_METADATA_TEMPLATE = (
    '{{range .items}}{{.metadata.name}}{{"\\t"}}{{.metadata.namespace}}{{"\\t"}}'
    '{{.metadata.uid}}{{"\\t"}}{{index .metadata.labels "it4931.instacart.edu/owner"}}'
    '{{"\\t"}}{{index .metadata.labels "it4931.instacart.edu/run-id"}}'
    '{{"\\t"}}{{index .metadata.labels "app.kubernetes.io/managed-by"}}{{"\\n"}}{{end}}'
)


def _get_json(executor: CommandExecutor, command: list[str], description: str) -> dict[str, Any]:
    payload = json_output(checked(executor, command), description)
    if not isinstance(payload, dict):
        raise ReceiptCollectionError(f"{description} response is invalid")
    return payload


def _owner_references(metadata: Mapping[str, Any]) -> tuple[OwnerReferenceIdentity, ...]:
    values = metadata.get("ownerReferences", [])
    if not isinstance(values, list):
        raise ReceiptCollectionError("live owner reference list is invalid")
    result = []
    for item in values:
        if not isinstance(item, Mapping):
            raise ReceiptCollectionError("live owner reference is invalid")
        result.append(
            OwnerReferenceIdentity(
                kind=item.get("kind", ""),
                name=item.get("name", ""),
                uid=item.get("uid", ""),
                controller=item.get("controller") is True,
            )
        )
    return tuple(result)


def _containerd_config_digest(raw: str, image_reference: str, manifest_digest: str) -> str:
    try:
        payload = json.loads(raw)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ReceiptCollectionError("containerd image inventory is invalid") from exc
    images = payload.get("images") if isinstance(payload, dict) else None
    if not isinstance(images, list):
        raise ReceiptCollectionError("containerd image inventory has an unsupported shape")
    base_reference = image_reference.split("@", 1)[0]
    candidates = []
    for image in images:
        if not isinstance(image, Mapping):
            continue
        repo_digests = image.get("repoDigests") or []
        repo_tags = image.get("repoTags") or []
        if not isinstance(repo_tags, list):
            continue
        contains_digest = any(
            isinstance(value, str) and value.endswith("@" + manifest_digest)
            for value in repo_digests
        )
        image_id = image.get("id")
        if contains_digest and isinstance(image_id, str):
            expected_tail = base_reference.rsplit("/", 1)[-1]
            tag_matches = any(
                isinstance(value, str) and value.rsplit("/", 1)[-1] == expected_tail
                for value in repo_tags
            )
            if not tag_matches:
                expected_repo = expected_tail.split(":", 1)[0]
                tag_matches = any(
                    isinstance(value, str) and value.split("@", 1)[0].rsplit("/", 1)[-1] == expected_repo
                    for value in repo_digests
                )
            if not tag_matches:
                continue
            config = image_id if image_id.startswith("sha256:") else "sha256:" + image_id
            if IMAGE_DIGEST_PATTERN.fullmatch(config):
                candidates.append(config)
    if len(set(candidates)) != 1:
        raise ReceiptCollectionError("containerd config digest does not resolve uniquely")
    return candidates[0]


def collect_runtime_ownership_receipt(
    executor: CommandExecutor,
    identity: RunIdentity,
    lock: Mapping[str, Any],
    *,
    execute: bool = False,
) -> RuntimeOwnershipReceipt:
    """Collect every expected object and live identity from the dedicated profile."""
    if execute is not True:
        raise ReceiptCollectionError("live receipt collection requires explicit execute=True authorization")
    validate_static_lock(lock)
    try:
        validate_version_lock(lock)
    except OwnershipError as exc:
        raise ReceiptCollectionError("runtime receipt collection requires a complete version lock") from exc
    if lock["cluster"].get("profile") != identity.profile or lock["cluster"].get("context") != identity.profile:
        raise ReceiptCollectionError("version lock does not identify the requested profile")

    cluster = collect_cluster_identity(executor, identity, execute=True)
    namespace = collect_namespace_identity(executor, identity, execute=True)
    if cluster["cluster_uid"] != lock["cluster"].get("cluster_uid"):
        raise ReceiptCollectionError("live cluster identity does not match the finalized version lock")
    for field in ("kubernetes_version", "driver", "container_runtime", "cni", "architecture", "node_count"):
        if cluster.get(field) != lock["cluster"].get(field):
            raise ReceiptCollectionError(f"live cluster {field} does not match the finalized version lock")
    component_images = collect_cluster_component_images(executor, identity, execute=True)
    expected_control_plane = lock["toolchain"].get("kubernetes_control_plane_images", {})
    expected_calico = lock["toolchain"].get("calico_images", {})
    if (
        component_images["kubernetes_control_plane_images"] != expected_control_plane
        or component_images["calico_images"] != expected_calico
        or component_images["cni_version"] != lock["cluster"].get("cni_version")
    ):
        raise ReceiptCollectionError("live Kubernetes or Calico image identity does not match the version lock")
    objects: list[RuntimeObjectIdentity] = []
    by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for resource_type in _NAMESPACED_RESOURCE_TYPES:
        payload = _get_json(
            executor,
            kubectl(identity, "--namespace", identity.namespace, "get", resource_type, "-o", "json"),
            f"{resource_type} inventory",
        )
        items = payload.get("items")
        if not isinstance(items, list):
            raise ReceiptCollectionError(f"{resource_type} inventory is invalid")
        for item in items:
            if not isinstance(item, Mapping):
                raise ReceiptCollectionError("live Kubernetes object is invalid")
            metadata = item.get("metadata", {})
            if not isinstance(metadata, Mapping):
                raise ReceiptCollectionError("live Kubernetes metadata is invalid")
            kind, name = item.get("kind"), metadata.get("name")
            uid, labels = metadata.get("uid"), metadata.get("labels", {})
            object_namespace = metadata.get("namespace")
            if (kind, name) in _SYSTEM_NAMESPACE_OBJECTS:
                continue
            if isinstance(labels, Mapping) and labels.get("it4931.instacart.edu/component") == "pipeline":
                # B2's serving receipt remains a stable subset after the
                # independently receipted B3 pipeline is added to the same
                # isolated namespace. B4 teardown must validate both receipts.
                continue
            if not all(isinstance(value, str) and value for value in (kind, name, uid, object_namespace)):
                raise ReceiptCollectionError("live Kubernetes kind, name, namespace, or UID is missing")
            if not isinstance(labels, Mapping):
                raise ReceiptCollectionError("live Kubernetes labels are invalid")
            record = RuntimeObjectIdentity(
                kind=kind,
                name=name,
                namespace=object_namespace,
                uid=uid,
                labels=dict(labels),
                owner_references=_owner_references(metadata),
            )
            key = (kind, name)
            if key in by_key:
                raise ReceiptCollectionError("live Kubernetes inventory contains duplicate objects")
            by_key[key] = dict(item)
            objects.append(record)

    # Secret data is deliberately excluded from the API response; only its
    # metadata is collected for the ownership inventory.
    secret_result = checked(
        executor,
        kubectl(
            identity,
            "--namespace",
            identity.namespace,
            "get",
            "secrets",
            "-o",
            f"go-template={_SECRET_METADATA_TEMPLATE}",
        ),
    )
    for line in secret_result.stdout.splitlines():
        fields = line.split("\t")
        if len(fields) != 6:
            raise ReceiptCollectionError("Secret metadata inventory is invalid")
        name, object_namespace, uid, owner_label, run_label, manager_label = fields
        metadata = {
            "name": name,
            "namespace": object_namespace,
            "uid": uid,
            "labels": {
                "it4931.instacart.edu/owner": owner_label,
                "it4931.instacart.edu/run-id": run_label,
                "app.kubernetes.io/managed-by": manager_label,
            },
        }
        record = RuntimeObjectIdentity(
            kind="Secret",
            name=name,
            namespace=object_namespace,
            uid=uid,
            labels=metadata["labels"],
            owner_references=(),
        )
        key = ("Secret", name)
        if key in by_key:
            raise ReceiptCollectionError("Secret inventory contains a duplicate object")
        by_key[key] = {"kind": "Secret", "metadata": metadata}
        objects.append(record)

    pvc = by_key.get(("PersistentVolumeClaim", "data-module4d-mongo-0"))
    if pvc is None:
        raise ReceiptCollectionError("Mongo PVC is missing from the live inventory")
    pvc_metadata = pvc.get("metadata", {})
    pvc_spec = pvc.get("spec", {})
    pvc_status = pvc.get("status", {})
    volume_name = pvc_spec.get("volumeName")
    storage_class_name = pvc_spec.get("storageClassName")
    if pvc_status.get("phase") != "Bound" or not isinstance(volume_name, str) or not volume_name:
        raise ReceiptCollectionError("Mongo PVC is not bound to a PV")
    pv = _get_json(executor, kubectl(identity, "get", "pv", volume_name, "-o", "json"), "bound PV")
    storage_class = _get_json(
        executor,
        kubectl(identity, "get", "storageclass", storage_class_name, "-o", "json"),
        "bound StorageClass",
    )
    pv_metadata, pv_spec = pv.get("metadata", {}), pv.get("spec", {})
    sc_metadata = storage_class.get("metadata", {})
    provisioner = storage_class.get("provisioner")
    if not isinstance(pvc_metadata.get("labels"), Mapping) or any(
        pvc_metadata["labels"].get(key) != value for key, value in identity.labels.items()
    ):
        raise ReceiptCollectionError("Mongo PVC ownership labels do not match the run")
    mongo_statefulset = by_key.get(("StatefulSet", "module4d-mongo"))
    if mongo_statefulset is None:
        raise ReceiptCollectionError("Mongo StatefulSet is missing from the live inventory")
    _validate_mongo_claim_template(mongo_statefulset, pvc_metadata.get("labels"))
    resources = pvc_spec.get("resources")
    requests = resources.get("requests") if isinstance(resources, Mapping) else None
    pvc_labels = pvc_metadata.get("labels")
    if (
        not isinstance(pvc_spec.get("accessModes"), list)
        or pvc_spec["accessModes"] != ["ReadWriteOnce"]
        or not isinstance(requests, Mapping)
        or requests.get("storage") != "2Gi"
        or not isinstance(pvc_labels, Mapping)
        or pvc_labels.get("app.kubernetes.io/name") != "module4d-mongo"
        or pvc_labels.get("app.kubernetes.io/component") != "database"
    ):
        raise ReceiptCollectionError("Mongo PVC does not match the verified StatefulSet claim template")
    host_path = pv_spec.get("hostPath")
    local = pv_spec.get("local")
    if isinstance(host_path, Mapping):
        locality_kind = "hostPath"
        locality_path = host_path.get("path", "")
    elif isinstance(local, Mapping):
        locality_kind = "local"
        locality_path = local.get("path", "")
    else:
        locality_kind = "unknown"
        locality_path = ""
    claim_ref = pv_spec.get("claimRef", {})
    if (
        claim_ref.get("namespace") != identity.namespace
        or claim_ref.get("name") != "data-module4d-mongo-0"
        or claim_ref.get("uid") != pvc_metadata.get("uid")
    ):
        raise ReceiptCollectionError("bound PV claim reference does not match the owned Mongo PVC")
    node_items = _get_json(executor, kubectl(identity, "get", "nodes", "-o", "json"), "node inventory").get("items")
    if not isinstance(node_items, list) or len(node_items) != 1:
        raise ReceiptCollectionError("profile must have one node for local storage ownership")
    node_metadata = node_items[0].get("metadata", {})
    runtime_storage = RuntimeStorageIdentity(
        claim_name="data-module4d-mongo-0",
        claim_uid=pvc_metadata.get("uid", ""),
        volume_name=volume_name,
        volume_uid=pv_metadata.get("uid", ""),
        pv_claim_namespace=claim_ref.get("namespace", ""),
        pv_claim_name=claim_ref.get("name", ""),
        pv_claim_uid=claim_ref.get("uid", ""),
        storage_class=storage_class_name or "",
        storage_class_uid=sc_metadata.get("uid", ""),
        provisioner=provisioner or "",
        requested_storage=pvc_spec.get("resources", {}).get("requests", {}).get("storage", ""),
        capacity=pv_spec_capacity(pv),
        access_mode=(pvc_spec.get("accessModes") or [""])[0],
        profile_local=provisioner == lock["storage"].get("provisioner") and locality_kind == "hostPath",
        locality_kind=locality_kind,
        locality_path=locality_path,
        # StatefulSet volumeClaimTemplates retain claims by default and do not
        # attach a deletion owner reference. Bind the claim to its template by
        # exact generated name, run labels, and the observed parent UID.
        claim_owner_uid=mongo_statefulset.get("metadata", {}).get("uid", ""),
        locality_node_name=node_metadata.get("name", ""),
        locality_node_uid=node_metadata.get("uid", ""),
        claim_template_name="data",
        retention_when_deleted="Retain",
        retention_when_scaled="Retain",
    )

    cri_inventory = checked(executor, containerd_image_inventory_command(identity), timeout=60).stdout
    provenance = lock.get("image_provenance", {})
    image_records = []
    role_specs = (
        ("mongo", "module4d-mongo-0", "mongodb"),
        ("api_application_image", "module4d-api-", "api"),
        ("ui_application_image", "module4d-ui-", "ui"),
    )
    for role, pod_prefix, container_name in role_specs:
        pod_candidates = [
            item for item in objects
            if item.kind == "Pod" and (
                item.name == pod_prefix if not pod_prefix.endswith("-") else item.name.startswith(pod_prefix)
            )
        ]
        if len(pod_candidates) != 1:
            raise ReceiptCollectionError(f"expected one running Pod for image role {role}")
        pod = by_key[("Pod", pod_candidates[0].name)]
        spec_containers = pod.get("spec", {}).get("containers", [])
        statuses = pod.get("status", {}).get("containerStatuses", [])
        declared = [item for item in spec_containers if item.get("name") == container_name]
        status = [item for item in statuses if item.get("name") == container_name]
        if len(declared) != 1 or len(status) != 1 or status[0].get("ready") is not True:
            raise ReceiptCollectionError(f"running container identity is incomplete for {role}")
        reference = declared[0].get("image")
        runtime_image_id = status[0].get("imageID")
        role_provenance = provenance.get(role, {})
        locked_digest = _locked_role_digest(lock, role)
        provenance_digest = role_provenance.get("image_digest")
        digest = provenance_digest or locked_digest
        if not isinstance(reference, str) or not isinstance(digest, str):
            raise ReceiptCollectionError(f"locked image identity is incomplete for {role}")
        if role == "mongo":
            config_digest = _containerd_config_digest(cri_inventory, reference, digest)
        else:
            config_digest = role_provenance.get("config_digest", "")
        try:
            verify_running_image_id(
                runtime_image_id=runtime_image_id,
                declared_reference=reference,
                image_digest=digest,
                config_digest=config_digest,
            )
        except OwnershipError as exc:
            raise ReceiptCollectionError(f"running image for {role} does not match the lock") from exc
        image_records.append(
            RuntimeImageIdentity(
                role=role,
                pod_uid=pod_candidates[0].uid,
                container_name=container_name,
                declared_reference=reference,
                runtime_image_id=runtime_image_id,
                digest=digest,
                config_digest=config_digest,
            )
        )

    receipt = RuntimeOwnershipReceipt(
        profile=identity.profile,
        context=identity.profile,
        cluster_uid=cluster["cluster_uid"],
        namespace=identity.namespace,
        namespace_uid=namespace["namespace_uid"],
        namespace_labels=namespace["namespace_labels"],
        objects=tuple(objects),
        storage=runtime_storage,
        images=tuple(image_records),
        cluster_images={**expected_control_plane, **expected_calico},
    )
    return receipt


def validate_collected_receipt(receipt: RuntimeOwnershipReceipt, lock: Mapping[str, Any]) -> None:
    """Apply full receipt checks using only identities in the finalized lock."""
    expected_images = {role: _locked_role_digest(lock, role) for role in REQUIRED_SERVING_RUNTIME_IMAGE_ROLES}
    from .ownership import validate_runtime_receipt

    try:
        if (
            receipt.profile != lock["cluster"]["profile"]
            or receipt.context != lock["cluster"]["context"]
            or receipt.cluster_uid != lock["cluster"]["cluster_uid"]
        ):
            raise OwnershipError("receipt cluster does not match the finalized version lock")
        expected_cluster_images = {
            **lock["toolchain"]["kubernetes_control_plane_images"],
            **lock["toolchain"]["calico_images"],
        }
        validate_runtime_receipt(
            RunIdentity(receipt.namespace.removeprefix("it4931-k8s-")),
            receipt,
            expected_images=expected_images,
            expected_cluster_images=expected_cluster_images,
            expected_storage_class=lock["storage"]["default_storage_class"],
            expected_provisioner=lock["storage"]["provisioner"],
        )
    except (KeyError, OwnershipError) as exc:
        raise ReceiptCollectionError("collected runtime receipt failed complete ownership validation") from exc


def assert_receipt_matches_lock_and_live(
    recorded: RuntimeOwnershipReceipt,
    live: RuntimeOwnershipReceipt,
    lock: Mapping[str, Any],
    identity: RunIdentity,
) -> None:
    """Validate and compare the B2 serving subset before B3 or a safe resume."""
    expected_images = {role: _locked_role_digest(lock, role) for role in REQUIRED_SERVING_RUNTIME_IMAGE_ROLES}
    expected_cluster_images = {
        **lock["toolchain"]["kubernetes_control_plane_images"],
        **lock["toolchain"]["calico_images"],
    }
    try:
        from .ownership import assert_receipt_matches_live

        assert_receipt_matches_live(
            identity, recorded, live,
            expected_images=expected_images,
            expected_cluster_images=expected_cluster_images,
            expected_storage_class=lock["storage"]["default_storage_class"],
            expected_provisioner=lock["storage"]["provisioner"],
        )
    except (KeyError, OwnershipError) as exc:
        raise ReceiptCollectionError("recorded B2 serving receipt does not match live identities") from exc


def receipt_document(receipt: RuntimeOwnershipReceipt) -> dict[str, Any]:
    """Return only identity metadata; Secret contents are never part of the model."""
    return {"schema_version": _OWNERSHIP_RECEIPT_SCHEMA_VERSION, **asdict(receipt)}


def receipt_json(receipt: RuntimeOwnershipReceipt) -> str:
    """Serialize a sanitized receipt for a private evidence file."""
    return json.dumps(receipt_document(receipt), sort_keys=True, indent=2) + "\n"


def receipt_from_document(value: Mapping[str, Any]) -> RuntimeOwnershipReceipt:
    """Parse a persisted receipt while rejecting missing or extra schema fields."""
    if (
        not isinstance(value, Mapping)
        or type(value.get("schema_version")) is not int
        or value.get("schema_version") != _OWNERSHIP_RECEIPT_SCHEMA_VERSION
    ):
        raise ReceiptCollectionError("ownership receipt schema is unsupported")
    expected_top = {
        "schema_version", "profile", "context", "cluster_uid", "namespace", "namespace_uid",
        "namespace_labels", "objects", "storage", "images", "cluster_images",
    }
    if set(value) != expected_top:
        raise ReceiptCollectionError("ownership receipt fields are incomplete or unsupported")
    try:
        objects = []
        for item in value["objects"]:
            if set(item) != {"kind", "name", "namespace", "uid", "labels", "owner_references"}:
                raise ValueError
            owners = tuple(OwnerReferenceIdentity(**owner) for owner in item["owner_references"])
            objects.append(
                RuntimeObjectIdentity(
                    kind=item["kind"],
                    name=item["name"],
                    namespace=item["namespace"],
                    uid=item["uid"],
                    labels=item["labels"],
                    owner_references=owners,
                )
            )
        images = tuple(RuntimeImageIdentity(**item) for item in value["images"])
        storage = RuntimeStorageIdentity(**value["storage"])
        receipt = RuntimeOwnershipReceipt(
            profile=value["profile"],
            context=value["context"],
            cluster_uid=value["cluster_uid"],
            namespace=value["namespace"],
            namespace_uid=value["namespace_uid"],
            namespace_labels=value["namespace_labels"],
            objects=tuple(objects),
            storage=storage,
            images=images,
            cluster_images=value["cluster_images"],
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ReceiptCollectionError("ownership receipt document is malformed") from exc
    return receipt
