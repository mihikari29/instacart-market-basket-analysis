"""Tests for cross-module checkpointing, intermediate data reuse, and tree structure."""

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts.k8s.config import get_execution_config
from scripts.k8s.pipeline_task import _is_resume_allowed
from src.module3.progress_log import ensure_checkpoint, CHECKPOINT_VERSION_MARKER


class PipelineCheckpointResumeTests(unittest.TestCase):
    def test_resume_policy_reads_from_pipeline_config(self):
        exec_cfg = get_execution_config()
        self.assertIn("resume_policy", exec_cfg)
        self.assertIn("checkpoint_dir", exec_cfg)
        self.assertIn("data_dir", exec_cfg)
        self.assertEqual(exec_cfg["checkpoint_dir"], "/run/checkpoints")
        self.assertEqual(exec_cfg["data_dir"], "/run/pipeline")
        self.assertTrue(_is_resume_allowed())

    def test_checkpoint_tree_structure_and_marker(self):
        with tempfile.TemporaryDirectory() as tmp:
            checkpoint_root = Path(tmp) / "run" / "checkpoints" / "module3"
            checkpoint_uri = ensure_checkpoint(
                output_dir=Path(tmp),
                checkpoint_id="run-123456",
                checkpoint_root=checkpoint_root,
                state_version="ingestion-latency-v1",
            )
            checkpoint_dir = checkpoint_root / "run-123456" / "ingestion-latency-v1"
            self.assertTrue(checkpoint_dir.is_dir())
            marker_file = checkpoint_root / "run-123456" / CHECKPOINT_VERSION_MARKER
            self.assertTrue(marker_file.is_file())
            marker_data = json.loads(marker_file.read_text(encoding="utf-8"))
            self.assertEqual(marker_data["checkpoint_id"], "run-123456")
            self.assertEqual(marker_data["state_version"], "ingestion-latency-v1")

    def test_step_input_resumes_cached_receipt(self):
        with tempfile.TemporaryDirectory() as tmp:
            runtime_root = Path(tmp)
            receipt_path = runtime_root / "evidence" / "input-stage-receipt.json"
            source_path = runtime_root / "evidence" / "source-receipt.json"
            receipt_path.parent.mkdir(parents=True, exist_ok=True)

            run_id = "0123456789ab"
            cached_receipt = {"run_id": run_id, "status": "completed", "files": []}
            receipt_path.write_text(json.dumps(cached_receipt), encoding="utf-8")
            source_path.write_text(json.dumps({"manifest_sha256": "fake"}), encoding="utf-8")

            from scripts.k8s import pipeline_task
            with patch.object(pipeline_task, "RUNTIME_ROOT", runtime_root), \
                 patch.object(pipeline_task, "runtime_values", return_value={"PIPELINE_RUN_ID": run_id, "PIPELINE_MODE": "fixture"}):
                result = pipeline_task.step_input()
                self.assertEqual(result["run_id"], run_id)
                self.assertEqual(result["status"], "completed")


if __name__ == "__main__":
    unittest.main()
