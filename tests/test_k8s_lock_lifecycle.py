"""Offline staged version-lock lifecycle contracts."""

from copy import deepcopy
from pathlib import Path
import unittest

import yaml

from scripts.k8s.lock_lifecycle import (
    LockLifecycleError,
    validate_build_provenance,
    validate_static_lock,
)


ROOT = Path(__file__).resolve().parents[1]


class LockLifecycleTests(unittest.TestCase):
    def test_committed_lock_is_valid_static_input_but_stays_incomplete(self):
        lock = yaml.safe_load((ROOT / "deploy/k8s/versions.lock.yaml").read_text(encoding="utf-8"))
        validate_static_lock(lock)
        self.assertEqual(lock["status"], "incomplete")
        self.assertTrue(lock["unresolved"])

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
