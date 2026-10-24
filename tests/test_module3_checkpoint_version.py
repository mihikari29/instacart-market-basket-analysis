"""Offline guards for checkpoint state-version compatibility."""

import json
from pathlib import Path
import tempfile
import unittest

from src.module3.progress_log import (
    CHECKPOINT_VERSION_MARKER,
    CheckpointCompatibilityError,
    ensure_checkpoint,
)


class CheckpointVersionTests(unittest.TestCase):
    def test_versioned_checkpoint_marker_is_stable(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = ensure_checkpoint(root, "resume", state_version="ingestion-latency-v1")
            second = ensure_checkpoint(root, "resume", state_version="ingestion-latency-v1")
            checkpoint = root / "checkpoint" / "resume" / "ingestion-latency-v1"
            marker = checkpoint.parent / CHECKPOINT_VERSION_MARKER

            self.assertEqual(first, second)
            self.assertTrue(first.endswith("/checkpoint/resume/ingestion-latency-v1"))
            self.assertEqual(
                json.loads(marker.read_text()),
                {"checkpoint_id": "resume", "state_version": "ingestion-latency-v1"},
            )

    def test_unversioned_checkpoint_is_preserved_and_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            legacy = root / "checkpoint" / "resume"
            legacy.mkdir(parents=True)
            state_file = legacy / "metadata"
            state_file.write_text("old Spark checkpoint")

            with self.assertRaisesRegex(CheckpointCompatibilityError, "unversioned checkpoint"):
                ensure_checkpoint(root, "resume", state_version="ingestion-latency-v1")

            self.assertEqual(state_file.read_text(), "old Spark checkpoint")
            self.assertFalse((legacy / "ingestion-latency-v1").exists())

    def test_checkpoint_root_is_respected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            custom = root / "persistent-checkpoints"
            uri = ensure_checkpoint(
                root, "resume", checkpoint_root=custom, state_version="ingestion-latency-v1"
            )

            self.assertTrue(uri.endswith("/persistent-checkpoints/resume/ingestion-latency-v1"))


if __name__ == "__main__":
    unittest.main()
