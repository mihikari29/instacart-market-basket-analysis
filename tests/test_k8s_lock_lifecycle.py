"""Offline staged version-lock lifecycle contracts."""

from copy import deepcopy
from pathlib import Path
import unittest

import yaml

from scripts.k8s.lock_lifecycle import (
    LockLifecycleError,
    finalize_version_lock,
    validate_build_provenance,
    validate_static_lock,
)


ROOT = Path(__file__).resolve().parents[1]
_FAKE_DIGEST_A = "sha256:" + "a" * 64
_FAKE_DIGEST_B = "sha256:" + "b" * 64


def _complete_runtime_evidence(template: dict) -> dict:
    """Create internally consistent fake evidence for offline lock finalization."""
    identity = "it4931-k8s-0123456789ab"
    image_updates = deepcopy(template["images"])
    for index, image in enumerate(image_updates.values(), start=1):
        if "@" not in image["reference"]:
            image["immutable_digest"] = "sha256:" + f"{index:064x}"

    application_images = {
        role: image_updates[role]["immutable_digest"]
        for role in ("api_application_image", "ui_application_image")
    }
    provenance = {
        role: {
            "source_revision": "1" * 40,
            "source_dirty": False,
            "source_tree_sha256": "c" * 64,
            "build_metadata_sha256": "d" * 64,
            "image_digest": image_digest,
            "config_digest": _FAKE_DIGEST_B,
            "python_version": "3.10.16" if role == "api_application_image" else "3.14.0",
            "python_environment_sha256": "e" * 64 if role == "api_application_image" else "f" * 64,
            "platform": "linux/amd64",
        }
        for role, image_digest in application_images.items()
    }
    control_plane_tags = {
        "kube-apiserver": "v1.36.4",
        "kube-controller-manager": "v1.36.4",
        "kube-scheduler": "v1.36.4",
        "kube-proxy": "v1.36.4",
        "etcd": "3.6.8-0",
        "pause": "3.10.2",
        "coredns": "v1.14.2",
    }
    control_plane_images = {
        role: f"registry.k8s.io/{role}:{tag}@sha256:" + f"{index:064x}"
        for index, (role, tag) in enumerate(control_plane_tags.items(), start=1)
    }
    return {
        "cluster": {
            "kubernetes_version": template["cluster"]["kubernetes_version"],
            "minikube_version": template["cluster"]["minikube_version"],
            "driver": template["cluster"]["driver"],
            "container_runtime": template["cluster"]["container_runtime"],
            "cni": template["cluster"]["cni"],
            "cni_version": template["cluster"]["cni_version"],
            "node_count": template["cluster"]["node_count"],
            "profile": identity,
            "context": identity,
            "cluster_uid": "cluster-uid-0123",
            "architecture": "linux/amd64",
        },
        "host_capacity": {
            "cpu_cores": 8,
            "memory_gib": 16,
            "free_disk_gib": 80,
            "measured_at": "2026-10-16T10:00:00Z",
        },
        "images": image_updates,
        "python_dependencies": {
            "api_python": "3.10.16",
            "ui_python": "3.14.0",
            "spark_parent_python": "3.10.16",
            "resolved_environment_digests": {
                "api": "sha256:" + "e" * 64,
                "ui": "sha256:" + "f" * 64,
                "spark_parent": "sha256:" + "e" * 64,
            },
        },
        "toolchain": {
            "minikube_version": template["cluster"]["minikube_version"],
            "kubectl_version": "v1.36.1",
            "minikube_binary_sha256": "a" * 64,
            "docker_engine_version": "27.5.1",
            "kubernetes_control_plane_images": control_plane_images,
            "calico_images": {
                role: f"docker.io/calico/{role}:{template['cluster']['cni_version']}@sha256:"
                + f"{index:064x}"
                for index, role in enumerate(("cni", "node", "kube-controllers"), start=20)
            },
        },
        "storage": {
            "default_storage_class": "standard",
            "storage_class_uid": "storage-class-uid-0123",
            "provisioner": "k8s.io/minikube-hostpath",
            "access_mode": "ReadWriteOnce",
            "capacity": "2Gi",
            "profile_local": True,
            "volume_binding_mode": "Immediate",
        },
        "image_provenance": provenance,
    }


class LockLifecycleTests(unittest.TestCase):
    def test_committed_lock_is_valid_static_input_but_stays_incomplete(self):
        lock = yaml.safe_load((ROOT / "deploy/k8s/versions.lock.yaml").read_text(encoding="utf-8"))
        validate_static_lock(lock)
        self.assertEqual(lock["cluster"]["kubernetes_version"], "v1.36.4")
        self.assertEqual(lock["cluster"]["minikube_version"], "v1.39.0")
        self.assertEqual(lock["cluster"]["cni_version"], "v3.32.2")
        self.assertEqual(lock["status"], "incomplete")
        self.assertTrue(lock["unresolved"])

    def test_matching_synthetic_runtime_evidence_finalizes_a_copy(self):
        template = yaml.safe_load((ROOT / "deploy/k8s/versions.lock.yaml").read_text(encoding="utf-8"))
        original = deepcopy(template)

        result = finalize_version_lock(template, _complete_runtime_evidence(template))

        self.assertEqual(result["status"], "complete")
        self.assertEqual(result["unresolved"], [])
        self.assertEqual(result["cluster"]["kubernetes_version"], "v1.36.4")
        self.assertEqual(result["cluster"]["cni_version"], "v3.32.2")
        self.assertEqual(result["cluster"]["profile"], result["cluster"]["context"])
        self.assertEqual(result["cluster"]["cluster_uid"], "cluster-uid-0123")
        self.assertEqual(
            result["image_provenance"]["api_application_image"]["image_digest"],
            result["images"]["api_application_image"]["immutable_digest"],
        )
        self.assertEqual(template, original)

    def test_finalization_rejects_calico_version_mismatch_without_mutating_template(self):
        template = yaml.safe_load((ROOT / "deploy/k8s/versions.lock.yaml").read_text(encoding="utf-8"))
        original = deepcopy(template)
        evidence = _complete_runtime_evidence(template)
        evidence["cluster"]["cni_version"] = "v3.33.0"

        with self.assertRaisesRegex(LockLifecycleError, "cluster.cni_version"):
            finalize_version_lock(template, evidence)

        self.assertEqual(template, original)
        self.assertEqual(template["status"], "incomplete")
        self.assertTrue(template["unresolved"])

    def test_finalization_rejects_kubernetes_version_mismatch_without_mutating_template(self):
        template = yaml.safe_load((ROOT / "deploy/k8s/versions.lock.yaml").read_text(encoding="utf-8"))
        original = deepcopy(template)
        evidence = _complete_runtime_evidence(template)
        evidence["cluster"]["kubernetes_version"] = "v1.37.1"

        with self.assertRaisesRegex(LockLifecycleError, "cluster.kubernetes_version"):
            finalize_version_lock(template, evidence)

        self.assertEqual(template, original)
        self.assertEqual(template["status"], "incomplete")
        self.assertTrue(template["unresolved"])

    def test_finalization_still_requires_cluster_identity_and_build_provenance(self):
        template = yaml.safe_load((ROOT / "deploy/k8s/versions.lock.yaml").read_text(encoding="utf-8"))
        original = deepcopy(template)
        evidence = _complete_runtime_evidence(template)
        del evidence["cluster"]["cluster_uid"]

        with self.assertRaisesRegex(LockLifecycleError, "cluster identity is incomplete"):
            finalize_version_lock(template, evidence)

        evidence = _complete_runtime_evidence(template)
        del evidence["image_provenance"]["api_application_image"]
        with self.assertRaisesRegex(LockLifecycleError, "application image build provenance"):
            finalize_version_lock(template, evidence)

        self.assertEqual(template, original)

    def test_static_lock_rejects_malformed_digest_reference(self):
        lock = yaml.safe_load((ROOT / "deploy/k8s/versions.lock.yaml").read_text(encoding="utf-8"))
        lock["images"]["mongo"]["reference"] = "mongo:7.0.16@sha256:bad"
        with self.assertRaisesRegex(LockLifecycleError, "digest is malformed"):
            validate_static_lock(lock)

    def test_runtime_provenance_requires_clean_source_and_coherent_digests(self):
        lock = {
            "images": {
                "api_application_image": {"immutable_digest": "sha256:" + "a" * 64},
                "ui_application_image": {"immutable_digest": "sha256:" + "b" * 64},
            },
            "image_provenance": {},
        }
        for role, digest in (
            ("api_application_image", "sha256:" + "a" * 64),
            ("ui_application_image", "sha256:" + "b" * 64),
        ):
            lock["image_provenance"][role] = {
                "source_revision": "1" * 40,
                "source_dirty": False,
                "source_tree_sha256": "c" * 64,
                "build_metadata_sha256": "d" * 64,
                "image_digest": digest,
                "config_digest": "sha256:" + "e" * 64,
                "python_version": "3.10.16",
                "python_environment_sha256": "f" * 64,
                "platform": "linux/amd64",
            }
        validate_build_provenance(lock)
        changed = deepcopy(lock)
        changed["image_provenance"]["api_application_image"]["source_dirty"] = True
        with self.assertRaisesRegex(LockLifecycleError, "source_dirty"):
            validate_build_provenance(changed)
        changed = deepcopy(lock)
        changed["image_provenance"]["api_application_image"]["image_digest"] = "sha256:" + "f" * 64
        with self.assertRaisesRegex(LockLifecycleError, "does not match"):
            validate_build_provenance(changed)


if __name__ == "__main__":
    unittest.main()
