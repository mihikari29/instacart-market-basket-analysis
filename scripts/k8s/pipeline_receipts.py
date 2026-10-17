"""B3 supplemental ownership, storage, and image receipts for pipeline resources."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from typing import Any, Mapping

from .image_provenance import verify_running_image_id
from .ownership import (
    IMAGE_DIGEST_PATTERN,
    OWNER_LABEL,
    OWNER_VALUE,
    RUN_LABEL,
    OwnerReferenceIdentity,
    RunIdentity,
)
from .pipeline import (
    PIPELINE_IMAGE_ROLES,
    PIPELINE_POLICY_NAMES,
    PIPELINE_STEPS,
    PipelineError,
    _locked_image,
    _validate_pipeline_lock,
)
from .runtime import (
    CommandExecutor,
    RuntimePreflightError,
    _checked,
    _json_output,
    collect_cluster_identity,
    collect_namespace_identity,
    containerd_image_inventory_command,
    kubectl,
)


_CLAIMS = {
    "it4931-hdfs-namenode": "1Gi",
    "it4931-hdfs-datanode": "16Gi",
    "it4931-kafka-data": "2Gi",
    "it4931-pipeline-data": "16Gi",
    "it4931-pipeline-checkpoints": "2Gi",
}
_DIRECT_OBJECTS = {
    ("ConfigMap", "it4931-pipeline-config"),
    ("ServiceAccount", "module4d-pipeline"),
    *( ("PersistentVolumeClaim", name) for name in _CLAIMS ),
    ("Service", "namenode"), ("Service", "datanode"), ("Service", "kafka"),
    ("StatefulSet", "hdfs-namenode"), ("StatefulSet", "hdfs-datanode"), ("StatefulSet", "kafka"),
    *( ("NetworkPolicy", name) for name in PIPELINE_POLICY_NAMES ),
}


@dataclass(frozen=True, slots=True)
class PipelineObject:
    kind: str
    name: str
    namespace: str
    uid: str
    labels: Mapping[str, str]
    owner_references: tuple[OwnerReferenceIdentity, ...] = ()


@dataclass(frozen=True, slots=True)
class PipelineStorage:
    claim_name: str
    claim_uid: str
    requested_storage: str
    capacity: str
    access_mode: str
    volume_name: str
    volume_uid: str
    claim_ref_namespace: str
    claim_ref_name: str
    claim_ref_uid: str
    storage_class: str
    storage_class_uid: str
    provisioner: str
    locality_kind: str
    locality_path: str
    node_name: str
    node_uid: str
    profile_local: bool
    owner_references: tuple[OwnerReferenceIdentity, ...]


@dataclass(frozen=True, slots=True)
class PipelineImage:
    workload: str
    pod_name: str
    pod_uid: str
    container_name: str
    role: str
    declared_reference: str
    runtime_image_id: str
    digest: str
    config_digest: str


@dataclass(frozen=True, slots=True)
class PipelineOwnershipReceipt:
    schema_version: int
    profile: str
    context: str
    cluster_uid: str
    namespace: str
    namespace_uid: str
    namespace_labels: Mapping[str, str]
    objects: tuple[PipelineObject, ...]
    job_status: Mapping[str, Mapping[str, Any]]
    storage: tuple[PipelineStorage, ...]
    images: tuple[PipelineImage, ...]


class PipelineReceiptError(RuntimeError):
    """B3 live identity, owner-chain, storage, or image evidence is incomplete."""


def _parse_owner_references(metadata: Mapping[str, Any]) -> tuple[OwnerReferenceIdentity, ...]:
    values = metadata.get("ownerReferences", [])
    if not isinstance(values, list):
        raise PipelineReceiptError("pipeline owner references are malformed")
    result = []
    for item in values:
        if not isinstance(item, Mapping):
            raise PipelineReceiptError("pipeline owner reference is malformed")
        result.append(OwnerReferenceIdentity(
            kind=item.get("kind", ""), name=item.get("name", ""),
            uid=item.get("uid", ""), controller=item.get("controller") is True,
        ))
    return tuple(result)


def _query_json(executor: CommandExecutor, command: list[str], description: str) -> dict[str, Any]:
    try:
        value = _json_output(_checked(executor, command), description)
    except RuntimePreflightError as exc:
        raise PipelineReceiptError(f"{description} could not be collected") from exc
    if not isinstance(value, dict):
        raise PipelineReceiptError(f"{description} response is malformed")
    return value


def _component_config_digest(raw_inventory: str, reference: str, manifest_digest: str) -> str:
    try:
        payload = json.loads(raw_inventory)
    except (TypeError, json.JSONDecodeError) as exc:
        raise PipelineReceiptError("containerd pipeline image inventory is malformed") from exc
    images = payload.get("images") if isinstance(payload, dict) else None
    if not isinstance(images, list):
        raise PipelineReceiptError("containerd pipeline image inventory has an unsupported shape")
    tail = reference.split("@", 1)[0].rsplit("/", 1)[-1]
    matches = []
    for image in images:
        if not isinstance(image, Mapping):
            continue
        repo_digests = image.get("repoDigests") or []
        repo_tags = image.get("repoTags") or []
        image_id = image.get("id")
        if (
            isinstance(repo_digests, list)
            and any(isinstance(item, str) and item.endswith("@" + manifest_digest) for item in repo_digests)
            and isinstance(repo_tags, list)
            and any(isinstance(item, str) and item.rsplit("/", 1)[-1].split(":", 1)[0] == tail.split(":", 1)[0] for item in repo_tags)
            and isinstance(image_id, str)
        ):
            digest = image_id if image_id.startswith("sha256:") else "sha256:" + image_id
            if IMAGE_DIGEST_PATTERN.fullmatch(digest):
                matches.append(digest)
    if len(set(matches)) != 1:
        raise PipelineReceiptError("containerd pipeline image config digest is missing or ambiguous")
    return matches[0]


def _quantity(value: str) -> int:
    factors = {"Ki": 1024, "Mi": 1024**2, "Gi": 1024**3, "Ti": 1024**4}
    for suffix, factor in factors.items():
        if value.endswith(suffix):
            return int(value[:-len(suffix)]) * factor
    if value.endswith("K"):
        return int(value[:-1]) * 1000
    if value.endswith("M"):
        return int(value[:-1]) * 1000**2
    if value.endswith("G"):
        return int(value[:-1]) * 1000**3
    return int(value)


def collect_pipeline_receipt(
    executor: CommandExecutor,
    identity: RunIdentity,
    lock: Mapping[str, Any],
    *,
    execute: bool = False,
    include_b4_acceptance: bool = False,
) -> PipelineOwnershipReceipt:
    """Collect B3 identities, optionally excluding the separately receipted B4 Job."""
    if execute is not True:
        raise PipelineReceiptError("B3 receipt collection requires explicit execute=True authorization")
    try:
        _validate_pipeline_lock(lock)
        cluster = collect_cluster_identity(executor, identity, execute=True)
        namespace = collect_namespace_identity(executor, identity, execute=True)
    except Exception as exc:
        raise PipelineReceiptError("B3 profile or namespace identity is invalid") from exc
    for field in ("profile", "context", "cluster_uid", "kubernetes_version", "driver", "container_runtime", "cni", "architecture", "node_count"):
        expected = lock["cluster"].get(field)
        if cluster.get(field) != expected:
            raise PipelineReceiptError(f"B3 live cluster {field} does not match the lock")

    selector = f"{OWNER_LABEL}={OWNER_VALUE},{RUN_LABEL}={identity.run_id},it4931.instacart.edu/component=pipeline"
    resource_types = ("configmaps", "serviceaccounts", "services", "persistentvolumeclaims", "statefulsets", "networkpolicies", "jobs", "pods")
    by_key: dict[tuple[str, str], dict[str, Any]] = {}
    objects: list[PipelineObject] = []
    for resource_type in resource_types:
        payload = _query_json(
            executor,
            kubectl(identity, "--namespace", identity.namespace, "get", resource_type, "-l", selector, "-o", "json"),
            f"B3 {resource_type} inventory",
        )
        items = payload.get("items")
        if not isinstance(items, list):
            raise PipelineReceiptError(f"B3 {resource_type} inventory is malformed")
        for item in items:
            if not isinstance(item, Mapping):
                raise PipelineReceiptError("B3 live inventory contains a malformed object")
            metadata = item.get("metadata", {})
            labels = metadata.get("labels", {}) if isinstance(metadata, Mapping) else {}
            kind, name = item.get("kind"), metadata.get("name") if isinstance(metadata, Mapping) else None
            namespace_name, uid = metadata.get("namespace"), metadata.get("uid")
            if not all(isinstance(value, str) and value for value in (kind, name, namespace_name, uid)):
                raise PipelineReceiptError("B3 live inventory is missing object identity fields")
            if not isinstance(labels, Mapping) or any(labels.get(key) != value for key, value in identity.labels.items()):
                raise PipelineReceiptError(f"B3 ownership labels do not match for {kind}/{name}")
            if labels.get("it4931.instacart.edu/component") != "pipeline":
                raise PipelineReceiptError(f"B3 component label is missing for {kind}/{name}")
            acceptance_job = f"it4931-acceptance-{identity.run_id}"
            if include_b4_acceptance and kind == "Job" and name == acceptance_job:
                continue
            if include_b4_acceptance and kind == "Pod":
                owners = _parse_owner_references(metadata)
                if len(owners) == 1 and owners[0].kind == "Job" and owners[0].name == acceptance_job:
                    continue
            record = PipelineObject(kind, name, namespace_name, uid, dict(labels), _parse_owner_references(metadata))
            key = (kind, name)
            if key in by_key:
                continue  # The same object can appear in plural-resource aliases only once in this query plan.
            by_key[key] = dict(item)
            objects.append(record)

    direct_actual = {key for key in by_key if key[0] != "Pod"}
    expected_direct = set(_DIRECT_OBJECTS) | {("Job", f"it4931-{step}-{identity.run_id}") for step in PIPELINE_STEPS}
    if direct_actual != expected_direct:
        raise PipelineReceiptError("B3 direct object inventory is incomplete or contains unapproved resources")
    direct_uids = {key: value.get("metadata", {}).get("uid") for key, value in by_key.items() if key[0] != "Pod"}
    job_status = {}
    for step in PIPELINE_STEPS:
        name = f"it4931-{step}-{identity.run_id}"
        document = by_key.get(("Job", name))
        status = document.get("status", {}) if document is not None else {}
        if status.get("succeeded") != 1 or status.get("active", 0) != 0:
            raise PipelineReceiptError(f"B3 Job {name} is not complete")
        job_status[name] = {
            "succeeded": status.get("succeeded", 0),
            "failed": status.get("failed", 0),
            "active": status.get("active", 0),
        }
    for item in objects:
        if item.kind != "Pod":
            continue
        if len(item.owner_references) != 1:
            raise PipelineReceiptError(f"B3 child Pod {item.name} must have exactly one controller owner")
        owner = item.owner_references[0]
        if (
            not owner.controller or owner.kind not in {"Job", "StatefulSet"}
            or direct_uids.get((owner.kind, owner.name)) != owner.uid
        ):
            raise PipelineReceiptError(f"B3 child Pod {item.name} has an unapproved owner reference")
    if not any(item.kind == "Pod" and item.name.startswith("hdfs-namenode-") for item in objects):
        raise PipelineReceiptError("B3 NameNode child Pod is missing")
    if not any(item.kind == "Pod" and item.name.startswith("hdfs-datanode-") for item in objects):
        raise PipelineReceiptError("B3 DataNode child Pod is missing")
    if not any(item.kind == "Pod" and item.name.startswith("kafka-") for item in objects):
        raise PipelineReceiptError("B3 Kafka child Pod is missing")

    storage: list[PipelineStorage] = []
    node_items = _query_json(executor, kubectl(identity, "get", "nodes", "-o", "json"), "B3 node identity").get("items")
    if not isinstance(node_items, list) or len(node_items) != 1:
        raise PipelineReceiptError("B3 local-volume receipt requires the locked single-node profile")
    node_metadata = node_items[0].get("metadata", {})
    for claim_name, requested in _CLAIMS.items():
        claim = by_key.get(("PersistentVolumeClaim", claim_name))
        if claim is None:
            raise PipelineReceiptError(f"B3 PVC {claim_name} is missing")
        metadata, spec, status = claim.get("metadata", {}), claim.get("spec", {}), claim.get("status", {})
        volume_name = spec.get("volumeName")
        storage_class = spec.get("storageClassName")
        if status.get("phase") != "Bound" or not isinstance(volume_name, str) or not volume_name:
            raise PipelineReceiptError(f"B3 PVC {claim_name} is not bound")
        if spec.get("accessModes") != ["ReadWriteOnce"]:
            raise PipelineReceiptError(f"B3 PVC {claim_name} must be ReadWriteOnce")
        requested_value = spec.get("resources", {}).get("requests", {}).get("storage")
        if requested_value != requested:
            raise PipelineReceiptError(f"B3 PVC {claim_name} request differs from the manifest")
        pv = _query_json(executor, kubectl(identity, "get", "pv", volume_name, "-o", "json"), f"PV for {claim_name}")
        storage_class_doc = _query_json(executor, kubectl(identity, "get", "storageclass", storage_class, "-o", "json"), f"StorageClass for {claim_name}")
        pv_metadata, pv_spec = pv.get("metadata", {}), pv.get("spec", {})
        claim_ref = pv_spec.get("claimRef", {})
        if (
            claim_ref.get("namespace") != identity.namespace
            or claim_ref.get("name") != claim_name
            or claim_ref.get("uid") != metadata.get("uid")
        ):
            raise PipelineReceiptError(f"B3 PV claimRef does not match {claim_name}")
        local = pv_spec.get("hostPath") or pv_spec.get("local")
        locality_kind = "hostPath" if isinstance(pv_spec.get("hostPath"), Mapping) else "local" if isinstance(pv_spec.get("local"), Mapping) else "unknown"
        locality_path = local.get("path", "") if isinstance(local, Mapping) else ""
        capacity = pv_spec.get("capacity", {}).get("storage", "")
        if (
            _quantity(capacity) < _quantity(requested)
            or locality_kind != "hostPath"
            or not locality_path.startswith("/tmp/hostpath-provisioner/")
            or storage_class_doc.get("metadata", {}).get("uid") != lock["storage"].get("storage_class_uid")
            or storage_class_doc.get("provisioner") != lock["storage"].get("provisioner")
        ):
            raise PipelineReceiptError(f"B3 PVC/PV for {claim_name} is not profile-local under the locked StorageClass")
        storage.append(PipelineStorage(
            claim_name=claim_name, claim_uid=metadata.get("uid", ""), requested_storage=requested_value,
            capacity=capacity, access_mode="ReadWriteOnce", volume_name=volume_name,
            volume_uid=pv_metadata.get("uid", ""), claim_ref_namespace=claim_ref.get("namespace", ""),
            claim_ref_name=claim_ref.get("name", ""), claim_ref_uid=claim_ref.get("uid", ""),
            storage_class=storage_class or "", storage_class_uid=storage_class_doc.get("metadata", {}).get("uid", ""),
            provisioner=storage_class_doc.get("provisioner", ""), locality_kind=locality_kind,
            locality_path=locality_path, node_name=node_metadata.get("name", ""),
            node_uid=node_metadata.get("uid", ""), profile_local=True,
            owner_references=_parse_owner_references(metadata),
        ))

    expected_images: dict[str, tuple[str, str]] = {
        "hdfs-namenode": ("hdfs_namenode", "namenode"),
        "hdfs-datanode": ("hdfs_datanode", "datanode"),
        "kafka": ("kafka", "kafka"),
    }
    image_roles = {role: (reference, lock["images"][role]["immutable_digest"]) for role, reference in (
        ("kafka", _locked_image(lock, "kafka")),
        ("hdfs_namenode", _locked_image(lock, "hdfs_namenode")),
        ("hdfs_datanode", _locked_image(lock, "hdfs_datanode")),
        ("pipeline_application_image", _locked_image(lock, "pipeline_application_image")),
    )}
    inventory = _checked(executor, containerd_image_inventory_command(identity), timeout=60).stdout
    images: list[PipelineImage] = []
    by_uid = {item.uid: item for item in objects}
    for item in objects:
        if item.kind != "Pod":
            continue
        pod = by_key[("Pod", item.name)]
        pod_labels = item.labels
        if pod_labels.get("app.kubernetes.io/name") == "hdfs-namenode":
            workload, container, role = "hdfs-namenode", "namenode", "hdfs_namenode"
        elif pod_labels.get("app.kubernetes.io/name") == "hdfs-datanode":
            workload, container, role = "hdfs-datanode", "datanode", "hdfs_datanode"
        elif pod_labels.get("app.kubernetes.io/name") == "kafka":
            workload, container, role = "kafka", "kafka", "kafka"
        elif pod_labels.get("app.kubernetes.io/name") == "pipeline-task":
            owner = item.owner_references[0]
            workload, container, role = owner.name, "pipeline", "pipeline_application_image"
            states = pod.get("status", {}).get("containerStatuses", [])
            if pod.get("status", {}).get("phase") != "Succeeded" or not any(
                value.get("name") == container and value.get("state", {}).get("terminated", {}).get("exitCode") == 0
                for value in states
            ):
                raise PipelineReceiptError(f"B3 Job Pod {item.name} did not exit successfully")
        else:
            raise PipelineReceiptError(f"B3 Pod {item.name} has an unapproved image role")
        containers = pod.get("spec", {}).get("containers", [])
        statuses = pod.get("status", {}).get("containerStatuses", [])
        declared = [value for value in containers if value.get("name") == container]
        status = [value for value in statuses if value.get("name") == container]
        if len(declared) != 1 or len(status) != 1:
            raise PipelineReceiptError(f"B3 Pod {item.name} container identity is incomplete")
        if role != "pipeline_application_image" and (pod.get("status", {}).get("phase") != "Running" or status[0].get("ready") is not True):
            raise PipelineReceiptError(f"B3 service Pod {item.name} is not Ready")
        reference, digest = image_roles[role]
        declared_reference = declared[0].get("image")
        if declared_reference != reference:
            raise PipelineReceiptError(f"B3 Pod {item.name} declared image differs from the lock")
        if role == "pipeline_application_image":
            config_digest = lock["pipeline_image_provenance"]["config_digest"]
        else:
            config_digest = _component_config_digest(inventory, reference, digest)
            if config_digest != lock["pipeline_component_image_provenance"][role]["config_digest"]:
                raise PipelineReceiptError(f"B3 component image {role} config digest differs from the lock")
        runtime_image_id = status[0].get("imageID")
        try:
            verify_running_image_id(
                runtime_image_id=runtime_image_id or "",
                declared_reference=declared_reference,
                image_digest=digest,
                config_digest=config_digest,
            )
        except Exception as exc:
            raise PipelineReceiptError(f"B3 Pod {item.name} image ID does not match the locked digest") from exc
        images.append(PipelineImage(
            workload=workload, pod_name=item.name, pod_uid=item.uid, container_name=container,
            role=role, declared_reference=declared_reference, runtime_image_id=runtime_image_id,
            digest=digest, config_digest=config_digest,
        ))

    if not any(image.role == "pipeline_application_image" and image.workload.endswith("module1-" + identity.run_id) for image in images):
        raise PipelineReceiptError("B3 receipts do not include successful run-scoped pipeline Job images")
    return PipelineOwnershipReceipt(
        schema_version=1, profile=identity.profile, context=identity.profile,
        cluster_uid=cluster["cluster_uid"], namespace=identity.namespace,
        namespace_uid=namespace["namespace_uid"], namespace_labels=namespace["namespace_labels"],
        objects=tuple(sorted(objects, key=lambda value: (value.kind, value.name))),
        job_status=job_status,
        storage=tuple(sorted(storage, key=lambda value: value.claim_name)),
        images=tuple(sorted(images, key=lambda value: (value.role, value.pod_name))),
    )


def pipeline_receipt_document(receipt: PipelineOwnershipReceipt) -> dict[str, Any]:
    return asdict(receipt)


def pipeline_receipt_json(receipt: PipelineOwnershipReceipt) -> str:
    return json.dumps(pipeline_receipt_document(receipt), sort_keys=True, indent=2) + "\n"


def pipeline_receipt_from_document(value: Mapping[str, Any]) -> PipelineOwnershipReceipt:
    expected_top = {
        "schema_version", "profile", "context", "cluster_uid", "namespace", "namespace_uid",
        "namespace_labels", "objects", "job_status", "storage", "images",
    }
    if not isinstance(value, Mapping) or set(value) != expected_top or value.get("schema_version") != 1:
        raise PipelineReceiptError("B3 ownership receipt schema is unsupported")
    try:
        objects = []
        for item in value["objects"]:
            if set(item) != {"kind", "name", "namespace", "uid", "labels", "owner_references"}:
                raise ValueError
            owners = tuple(OwnerReferenceIdentity(**owner) for owner in item["owner_references"])
            objects.append(PipelineObject(
                kind=item["kind"], name=item["name"], namespace=item["namespace"],
                uid=item["uid"], labels=item["labels"], owner_references=owners,
            ))
        storage = []
        for item in value["storage"]:
            expected_fields = set(PipelineStorage.__dataclass_fields__)
            if set(item) != expected_fields:
                raise ValueError
            payload = dict(item)
            payload["owner_references"] = tuple(OwnerReferenceIdentity(**owner) for owner in item["owner_references"])
            storage.append(PipelineStorage(**payload))
        images = []
        for item in value["images"]:
            if set(item) != set(PipelineImage.__dataclass_fields__):
                raise ValueError
            images.append(PipelineImage(**item))
        return PipelineOwnershipReceipt(
            schema_version=1, profile=value["profile"], context=value["context"],
            cluster_uid=value["cluster_uid"], namespace=value["namespace"],
            namespace_uid=value["namespace_uid"], namespace_labels=value["namespace_labels"],
            objects=tuple(objects), job_status=value["job_status"], storage=tuple(storage), images=tuple(images),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise PipelineReceiptError("B3 ownership receipt document is malformed") from exc


def validate_pipeline_receipt(
    identity: RunIdentity,
    receipt: PipelineOwnershipReceipt,
    lock: Mapping[str, Any],
) -> None:
    """Validate exact B3 object set, ownership chains, local claims, and image roles."""
    if not isinstance(receipt, PipelineOwnershipReceipt) or receipt.schema_version != 1:
        raise PipelineReceiptError("B3 ownership receipt schema is unsupported")
    try:
        _validate_pipeline_lock(lock)
    except Exception as exc:
        raise PipelineReceiptError("B3 receipt requires a complete finalized image and cluster lock") from exc
    if (receipt.profile, receipt.context, receipt.namespace) != (identity.profile, identity.profile, identity.namespace):
        raise PipelineReceiptError("B3 receipt run identity does not match")
    if receipt.cluster_uid != lock["cluster"].get("cluster_uid"):
        raise PipelineReceiptError("B3 receipt cluster UID does not match the finalized lock")
    if not receipt.namespace_uid or any(receipt.namespace_labels.get(key) != value for key, value in identity.labels.items()):
        raise PipelineReceiptError("B3 namespace receipt is incomplete or not owned")
    keyed = {(item.kind, item.name): item for item in receipt.objects}
    if len(keyed) != len(receipt.objects):
        raise PipelineReceiptError("B3 receipt contains duplicate objects")
    uids = [receipt.cluster_uid, receipt.namespace_uid, *(item.uid for item in receipt.objects)]
    if any(not isinstance(value, str) or not value for value in uids) or len(set(uids)) != len(uids):
        raise PipelineReceiptError("B3 receipt UIDs are missing or duplicated")
    expected_jobs = {("Job", f"it4931-{step}-{identity.run_id}") for step in PIPELINE_STEPS}
    expected_direct = set(_DIRECT_OBJECTS) | expected_jobs
    actual_direct = set(keyed).difference(key for key in keyed if key[0] == "Pod")
    if actual_direct != expected_direct:
        raise PipelineReceiptError("B3 direct-resource receipt is incomplete or includes unowned resources")
    expected_job_status = {f"it4931-{step}-{identity.run_id}" for step in PIPELINE_STEPS}
    if not isinstance(receipt.job_status, Mapping) or set(receipt.job_status) != expected_job_status:
        raise PipelineReceiptError("B3 completed-Job receipt is incomplete")
    if any(
        not isinstance(value, Mapping) or value.get("succeeded") != 1 or value.get("active") != 0
        for value in receipt.job_status.values()
    ):
        raise PipelineReceiptError("B3 receipt contains an incomplete Job")
    direct_uids = {key: value.uid for key, value in keyed.items() if key[0] != "Pod"}
    expected_job_names = {f"it4931-{step}-{identity.run_id}" for step in PIPELINE_STEPS}
    expected_stateful_names = {"hdfs-namenode", "hdfs-datanode", "kafka"}
    pods_by_owner: dict[tuple[str, str], list[PipelineObject]] = {}
    for item in receipt.objects:
        if item.namespace != identity.namespace or item.uid in {receipt.cluster_uid, receipt.namespace_uid}:
            raise PipelineReceiptError("B3 object receipt has an invalid namespace or duplicated parent UID")
        if any(item.labels.get(key) != value for key, value in identity.labels.items()):
            raise PipelineReceiptError(f"B3 resource labels do not match for {item.kind}/{item.name}")
        if item.labels.get("it4931.instacart.edu/component") != "pipeline":
            raise PipelineReceiptError(f"B3 resource {item.kind}/{item.name} is missing its pipeline component label")
        if item.kind == "Pod":
            if len(item.owner_references) != 1:
                raise PipelineReceiptError(f"B3 Pod {item.name} must have one owner reference")
            owner = item.owner_references[0]
            if not owner.controller or direct_uids.get((owner.kind, owner.name)) != owner.uid:
                raise PipelineReceiptError(f"B3 Pod {item.name} owner reference does not resolve to a direct resource")
            if owner.kind == "Job":
                if owner.name not in expected_job_names or not item.name.startswith(owner.name + "-"):
                    raise PipelineReceiptError(f"B3 Pod {item.name} is not a child of a run-scoped pipeline Job")
            elif owner.kind == "StatefulSet":
                if owner.name not in expected_stateful_names or item.name != owner.name + "-0":
                    raise PipelineReceiptError(f"B3 Pod {item.name} is not an expected single-replica service Pod")
            else:
                raise PipelineReceiptError(f"B3 Pod {item.name} has an unsupported owner kind")
            pods_by_owner.setdefault((owner.kind, owner.name), []).append(item)
    expected_owners = {("Job", name) for name in expected_job_names} | {("StatefulSet", name) for name in expected_stateful_names}
    if set(pods_by_owner) != expected_owners or any(len(pods_by_owner[key]) != 1 for key in expected_owners):
        raise PipelineReceiptError("B3 child Pod receipt must contain exactly one Pod for each Job and StatefulSet")
    pod_records = {item.uid: item for item in receipt.objects if item.kind == "Pod"}
    if set(item.claim_name for item in receipt.storage) != set(_CLAIMS):
        raise PipelineReceiptError("B3 storage receipt must include all five isolated PVCs")
    volume_uids = [item.volume_uid for item in receipt.storage]
    volume_names = [item.volume_name for item in receipt.storage]
    if len(set(volume_uids)) != len(volume_uids) or len(set(volume_names)) != len(volume_names):
        raise PipelineReceiptError("B3 PV identities must be unique across the five run-scoped claims")
    for item in receipt.storage:
        if (
            item.requested_storage != _CLAIMS[item.claim_name]
            or item.claim_uid != keyed[("PersistentVolumeClaim", item.claim_name)].uid
            or _quantity(item.capacity) < _quantity(item.requested_storage)
            or item.access_mode != "ReadWriteOnce"
            or item.claim_ref_namespace != identity.namespace
            or item.claim_ref_name != item.claim_name
            or item.claim_ref_uid != item.claim_uid
            or item.storage_class_uid != lock["storage"]["storage_class_uid"]
            or item.provisioner != lock["storage"]["provisioner"]
            or item.profile_local is not True
            or item.locality_kind != "hostPath"
            or not item.locality_path.startswith("/tmp/hostpath-provisioner/")
            or not all((item.claim_uid, item.volume_uid, item.storage_class, item.storage_class_uid, item.node_name, item.node_uid))
            or item.owner_references
        ):
            raise PipelineReceiptError(f"B3 storage identity for {item.claim_name} does not match the lock")
    roles = {"kafka", "hdfs_namenode", "hdfs_datanode"}
    image_records = receipt.images
    if not all(any(image.role == role for image in image_records) for role in roles):
        raise PipelineReceiptError("B3 receipt is missing a Kafka or HDFS runtime image")
    for role in ("kafka", "hdfs_namenode", "hdfs_datanode", "pipeline_application_image"):
        image = lock["images"][role]
        expected_ref = _locked_image(lock, role)
        expected_digest = image["immutable_digest"]
        matches = [record for record in image_records if record.role == role]
        if not matches:
            raise PipelineReceiptError(f"B3 runtime receipt is missing image role {role}")
        if role != "pipeline_application_image" and len(matches) != 1:
            raise PipelineReceiptError(f"B3 stateful component image role {role} must resolve to one Pod")
        if role == "pipeline_application_image" and len({record.workload for record in matches}) < len(PIPELINE_STEPS):
            raise PipelineReceiptError("B3 pipeline image receipt must cover all six ordered Jobs")
        for record in matches:
            if record.declared_reference != expected_ref or record.digest != expected_digest:
                raise PipelineReceiptError(f"B3 runtime image role {role} differs from the immutable lock")
            if record.pod_uid not in {item.uid for item in receipt.objects if item.kind == "Pod"}:
                raise PipelineReceiptError(f"B3 image role {role} references an unreceipted Pod")
            pod = pod_records[record.pod_uid]
            owner = pod.owner_references[0]
            if role == "pipeline_application_image":
                if owner.kind != "Job" or record.workload != owner.name or owner.name not in expected_job_names:
                    raise PipelineReceiptError("B3 pipeline image record does not match its owning Job")
            else:
                expected_workload = {"kafka": "kafka", "hdfs_namenode": "hdfs-namenode", "hdfs_datanode": "hdfs-datanode"}[role]
                if owner.kind != "StatefulSet" or owner.name != expected_workload or record.workload != expected_workload:
                    raise PipelineReceiptError(f"B3 image role {role} does not match its owning StatefulSet")
            try:
                verify_running_image_id(
                    runtime_image_id=record.runtime_image_id,
                    declared_reference=record.declared_reference,
                    image_digest=record.digest,
                    config_digest=record.config_digest,
                )
            except Exception as exc:
                raise PipelineReceiptError(f"B3 image role {role} does not match its runtime identity") from exc


def assert_pipeline_receipt_matches_live(
    recorded: PipelineOwnershipReceipt,
    live: PipelineOwnershipReceipt,
    identity: RunIdentity,
    lock: Mapping[str, Any],
) -> None:
    validate_pipeline_receipt(identity, recorded, lock)
    validate_pipeline_receipt(identity, live, lock)
    if recorded != live:
        raise PipelineReceiptError("live B3 resources or storage differ from the recorded receipt")
