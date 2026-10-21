"""Pure tests for the Module 3 ingestion-to-output latency reducer."""

import unittest

from src.module3.metrics import summarize_ingestion_to_output


class IngestionToOutputLatencyTests(unittest.TestCase):
    def test_summary_uses_product_window_ingestion_samples_and_output_completion(self):
        stats = {"count": 3, "sum": 3_000, "min": 900, "max": 1_100}

        result = summarize_ingestion_to_output(stats, 2_000)

        self.assertEqual(result["status"], "measured")
        self.assertEqual(result["sample_count"], 3)
        self.assertTrue(result["sample_boundary"].startswith("latest contributing event"))
        self.assertEqual(result["latency_ms"], {"minimum": 900, "mean": 1000.0, "maximum": 1100})
        self.assertFalse(result["clock_skew_detected"])
        self.assertIn("not Kafka acknowledgement latency", result["limitations"])
        self.assertIn("not micro-batch duration or Spark processing rate", result["limitations"])
        self.assertIn("not Mongo sink duration", result["limitations"])

    def test_clock_skew_is_reported_as_a_negative_measurement(self):
        result = summarize_ingestion_to_output(
            {"count": 1, "sum": 2_100, "min": 2_100, "max": 2_100}, 2_000
        )

        self.assertEqual(result["latency_ms"], {"minimum": -100, "mean": -100.0, "maximum": -100})
        self.assertTrue(result["clock_skew_detected"])

    def test_missing_ingestion_samples_are_explicitly_unavailable(self):
        self.assertEqual(
            summarize_ingestion_to_output(
                {"count": 0, "sum": 0, "min": None, "max": None}, 2_000
            ),
            {
                "status": "unavailable",
                "reason": "no_valid_ingestion_timestamps_in_finalized_output",
                "sample_count": 0,
            },
        )

    def test_invalid_or_inconsistent_timestamp_summaries_fail_closed(self):
        invalid = (
            ({"count": 1, "sum": 1, "min": 1, "max": 1}, True),
            ({"count": -1, "sum": 1, "min": 1, "max": 1}, 2),
            ({"count": 2, "sum": 5, "min": 3, "max": 2}, 10),
            ({"count": 1, "sum": 2, "min": 2, "max": 2}, 0),
        )
        for stats, output_epoch_ms in invalid:
            with self.subTest(stats=stats, output=output_epoch_ms), self.assertRaises(ValueError):
                summarize_ingestion_to_output(stats, output_epoch_ms)


if __name__ == "__main__":
    unittest.main()
