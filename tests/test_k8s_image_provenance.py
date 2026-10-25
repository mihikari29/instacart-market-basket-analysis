"""Offline image provenance and CRI identity verification tests."""

import json
from pathlib import Path
import tempfile
import unittest

from scripts.k8s.image_provenance import (
    ImageProvenanceError,
    build_provenance_record,
    CALICO_REPOSITORIES,
    collect_cluster_component_images,
    CONTROL_PLANE_REPOSITORIES,
    collect_image_environment,
    parse_build_metadata,
    verify_loaded_image_inventory,
    verify_running_image_id,
)
from scripts.k8s.runtime import CommandResult


class FakeDockerExecutor:
    def __init__(self, config_digest):
        self.config_digest = config_digest
        self.calls = []

    def run(self, argv, *, timeout, input_text=None):
        self.calls.append(list(argv))
        if argv[:3] == ["docker", "image", "inspect"] and argv[3] == "--format" and argv[4] == "{{.Id}}":
            return CommandResult(0, self.config_digest)
        if argv[:3] == ["docker", "image", "inspect"]:
            return CommandResult(0, "c" * 40)
        if argv[:2] == ["docker", "create"]:
            return CommandResult(0, "1" * 64)
        if argv[:2] == ["docker", "cp"]:
            Path(argv[-1]).write_text(
                "python_version=3.10.16\nanyio==4.14.2\nfastapi==0.143.0\n",
                encoding="utf-8",
            )
            return CommandResult(0, "")
        if argv[:2] == ["docker", "rm"]:
            return CommandResult(0, "")
        return CommandResult(1, "unexpected command")


class ImageProvenanceTests(unittest.TestCase):
    def test_build_metadata_requires_manifest_and_config_digest(self):
        metadata = {
            "containerimage.digest": "sha256:" + "a" * 64,
            "containerimage.config.digest": "sha256:" + "b" * 64,
        }
        self.assertEqual(parse_build_metadata(metadata), {
            "image_digest": metadata["containerimage.digest"],
            "config_digest": metadata["containerimage.config.digest"],
        })
        with self.assertRaisesRegex(ImageProvenanceError, "config digest"):
            parse_build_metadata({"containerimage.digest": metadata["containerimage.digest"]})

    def test_provenance_record_binds_clean_source_to_build_metadata_file(self):
        image_digest = "sha256:" + "a" * 64
        config_digest = "sha256:" + "b" * 64
        metadata = {
            "containerimage.digest": image_digest,
            "containerimage.config.digest": config_digest,
        }
        with tempfile.TemporaryDirectory() as directory:
            metadata_path = Path(directory) / "build-metadata.json"
            metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
            record = build_provenance_record(
                source={
                    "source_revision": "c" * 40,
                    "source_dirty": False,
                    "source_tree_sha256": "d" * 64,
                },
                metadata_path=metadata_path,
                metadata=metadata,
                python_version="3.10.16",
                python_environment_sha256="e" * 64,
                platform="linux/amd64",
            )
            self.assertEqual(record["image_digest"], image_digest)
            self.assertEqual(record["config_digest"], config_digest)
            self.assertEqual(record["python_version"], "3.10.16")
            self.assertEqual(record["python_environment_sha256"], "e" * 64)
            with self.assertRaisesRegex(ImageProvenanceError, "clean source"):
                build_provenance_record(
                    source={**record, "source_dirty": True},
                    metadata_path=metadata_path,
                    metadata=metadata,
                    python_version="3.10.16",
                    python_environment_sha256="e" * 64,
                    platform="linux/amd64",
                )

    def test_loaded_profile_image_must_match_manifest_or_tag_and_config(self):
        manifest = "sha256:" + "a" * 64
        config = "sha256:" + "b" * 64
        expected = {
            "api_application_image": {
                "reference": "instacart-module4d-k8s-api:b1@" + manifest,
                "image_digest": manifest,
                "config_digest": config,
            }
        }
        inventory = {
            "images": [
                {
                    "id": config,
                    "repoTags": ["instacart-module4d-k8s-api:b1"],
                    "repoDigests": [],
                }
            ]
        }
        self.assertEqual(set(verify_loaded_image_inventory(json.dumps(inventory), expected)), set(expected))
        verify_running_image_id(
            runtime_image_id="containerd://" + config,
            declared_reference=expected["api_application_image"]["reference"],
            image_digest=manifest,
            config_digest=config,
        )
        inventory["images"][0]["repoTags"] = ["unrelated:image"]
        with self.assertRaisesRegex(ImageProvenanceError, "missing or ambiguous"):
            verify_loaded_image_inventory(json.dumps(inventory), expected)

    def test_environment_extraction_uses_only_a_stopped_scoped_container(self):
        config = "sha256:" + "b" * 64
        executor = FakeDockerExecutor(config)
        result = collect_image_environment(
            executor,
            "local/api:b1",
            execute=True,
            expected_config_digest=config,
            expected_source_revision="c" * 40,
        )
        self.assertEqual(result["python_version"], "3.10.16")
        self.assertRegex(result["python_environment_sha256"], r"\A[0-9a-f]{64}\Z")
        self.assertEqual(executor.calls[2][:3], ["docker", "create", "--pull=never"])
        self.assertEqual(executor.calls[-1], ["docker", "rm", "--force", "1" * 64])

    def test_cluster_component_image_inventory_requires_exact_calico_release(self):
        from scripts.k8s.ownership import RunIdentity

        images = []
        for index, repository in enumerate(CONTROL_PLANE_REPOSITORIES.values(), start=1):
            if repository.endswith("/coredns/coredns"):
                tag = "v1.14.2"
            elif repository.endswith("/etcd"):
                tag = "3.6.8-0"
            elif repository.endswith("/pause"):
                tag = "3.10.2"
            else:
                tag = "v1.36.4"
            digest = "sha256:" + f"{index:064x}"
            images.append({"repoTags": [f"{repository}:{tag}"], "repoDigests": [f"{repository}@{digest}"]})
        for index, repository in enumerate(CALICO_REPOSITORIES.values(), start=20):
            digest = "sha256:" + f"{index:064x}"
            images.append({"repoTags": [f"{repository}:v3.32.2"], "repoDigests": [f"{repository}@{digest}"]})

        class Executor:
            def run(self, argv, *, timeout, input_text=None):
                return CommandResult(0, json.dumps({"images": images}))

        result = collect_cluster_component_images(
            Executor(), RunIdentity("0123456789ab"), execute=True
        )
        self.assertEqual(result["cni_version"], "v3.32.2")
        self.assertEqual(len(result["kubernetes_control_plane_images"]), 7)
        self.assertEqual(len(result["calico_images"]), 3)


if __name__ == "__main__":
    unittest.main()
