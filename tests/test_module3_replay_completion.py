"""Finite replay regressions using fake processes and committed offsets only."""

import json
from pathlib import Path
import tempfile
import types
import unittest

from src.module3.replay import FULL_REPLAY_SECONDS, ReplayCompletion, validate_replay_budget, validate_replay_completion, wait_for_replay


class ReplayCompletionTests(unittest.TestCase):
    def test_consumer_stays_active_until_drain_or_deadline(self):
        now = [0.0]
        query = types.SimpleNamespace(isActive=True)
        def sleep(seconds):
            now[0] += seconds
        result = wait_for_replay(query, lambda q: {"drained": True} if now[0] >= 3 else None,
            5, clock=lambda: now[0], sleep=sleep)
        self.assertEqual(result, {"drained": True})
        with self.assertRaises(TimeoutError):
            wait_for_replay(query, lambda q: None, 1, clock=lambda: now[0], sleep=sleep)
        query.isActive = False
        query.exception = lambda: None
        with self.assertRaisesRegex(RuntimeError, "stopped"):
            wait_for_replay(query, lambda q: {"drained": True}, 1)

    def test_full_feed_pacing_requires_more_than_old_one_hour_window(self):
        with self.assertRaisesRegex(ValueError, "cannot finish"):
            validate_replay_budget(33_819_106, 1000, 3600)
        validate_replay_budget(33_819_106, 1000, FULL_REPLAY_SECONDS)

    def test_waits_for_delivery_and_committed_terminal_offsets(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "delivery.json"
            process = types.SimpleNamespace(code=None)
            process.poll = lambda: process.code
            query = types.SimpleNamespace(lastProgress=None)
            completion = ReplayCompletion(
                expected_events=3, topic="events", start_offsets={"0": 0},
                producer=lambda: process, receipt_path=path, end_offsets=lambda: {"0": 3},
            )
            self.assertIsNone(completion(query))
            path.write_text(json.dumps({"attempted_events": 3, "acked_events": 3, "failed_events": 0, "lifecycle_errors": []}))
            process.code = 0
            query.lastProgress = {"sources": [{"endOffset": '{"events":{"0":2}}'}]}
            self.assertIsNone(completion(query))
            query.lastProgress["sources"][0]["endOffset"] = '{"events":{"0":3}}'
            result = completion(query)
            self.assertEqual(result["consumed_records"], 3)
            validate_replay_completion(result)
            result["acked_events"] = 2
            with self.assertRaisesRegex(ValueError, "reconcile"):
                validate_replay_completion(result)

    def test_failed_producer_cannot_report_success(self):
        completion = ReplayCompletion(expected_events=3, topic="events", start_offsets={"0": 0},
            producer=lambda: types.SimpleNamespace(poll=lambda: 1), receipt_path="unused", end_offsets=lambda: {})
        with self.assertRaisesRegex(RuntimeError, "producer failed"):
            completion(types.SimpleNamespace(lastProgress={}))

    def test_counts_cannot_substitute_for_missing_offset_evidence(self):
        with self.assertRaisesRegex(ValueError, "offset evidence"):
            validate_replay_completion({"expected_events": 3, "attempted_events": 3, "acked_events": 3,
                "consumed_records": 3, "failed_events": 0})
