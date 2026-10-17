import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

from src.module1.raw_diagnostic import _csv_field, diagnose_orders


class RawDiagnosticTests(unittest.TestCase):
    def test_csv_field_reads_only_the_requested_field(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "orders.csv"
            path.write_text(
                "order_id,order_hour_of_day\n1,24\n",
                encoding="utf-8",
            )
            self.assertEqual(_csv_field(path, "order_hour_of_day", 2), "24")

    @unittest.skipUnless(importlib.util.find_spec("pandas"), "pandas is unavailable")
    def test_diagnostic_reports_provenance_and_only_the_invalid_field(self):
        from contextlib import redirect_stdout
        from io import StringIO

        with tempfile.TemporaryDirectory() as temporary:
            raw_dir = Path(temporary) / "raw"
            raw_dir.mkdir()
            orders_path = raw_dir / "orders.csv"
            orders_path.write_text(
                "order_id,user_id,eval_set,order_number,order_dow,order_hour_of_day,days_since_prior_order\n"
                "1,777,DO_NOT_PRINT_THIS_FIELD,1,0,24,\n",
                encoding="utf-8",
            )
            orders_sha = hashlib.sha256(orders_path.read_bytes()).hexdigest()
            (raw_dir / "source-receipt.json").write_text(
                json.dumps({
                    "url": "https://www.kaggle.com/api/v1/datasets/download/psparks/instacart-market-basket-analysis?datasetVersionNumber=1",
                    "version": 1,
                    "archive_sha256": "a" * 64,
                    "files": {"orders.csv": {"rows": 1, "sha256": orders_sha}},
                }),
                encoding="utf-8",
            )
            output = StringIO()
            with redirect_stdout(output):
                diagnose_orders(raw_dir)

        diagnostic = output.getvalue()
        self.assertIn("dataset_version=1", diagnostic)
        self.assertIn(f"orders_sha256={orders_sha}", diagnostic)
        self.assertIn("orders_sha256_matches_receipt=True", diagnostic)
        self.assertIn("csv_header_valid=True", diagnostic)
        self.assertIn("field=order_hour_of_day row=2", diagnostic)
        self.assertIn("csv_raw_repr='24'", diagnostic)
        self.assertIn("integer_pattern_valid=True", diagnostic)
        self.assertIn("within_domain_0_to_23=False", diagnostic)
        self.assertNotIn("DO_NOT_PRINT_THIS_FIELD", diagnostic)
