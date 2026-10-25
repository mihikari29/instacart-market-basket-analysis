import contextlib
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import unittest

from src.module1.raw_diagnostic import (
    MAX_OUTPUT_BYTES,
    _emit,
    diagnose_orders,
)


ORDERS_COLUMNS = (
    "order_id",
    "user_id",
    "eval_set",
    "order_number",
    "order_dow",
    "order_hour_of_day",
    "days_since_prior_order",
)
HEADER = ",".join(ORDERS_COLUMNS)
EXPECTED_URL = (
    "https://www.kaggle.com/api/v1/datasets/download/"
    "psparks/instacart-market-basket-analysis?datasetVersionNumber=1"
)
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


class RawDiagnosticTests(unittest.TestCase):
    def make_raw_dir(self, temporary: str, content: str, receipt: object | None = None) -> Path:
        raw_dir = Path(temporary) / "raw"
        raw_dir.mkdir()
        (raw_dir / "orders.csv").write_text(content, encoding="utf-8", newline="")
        if receipt is not None:
            receipt_path = raw_dir / "source-receipt.json"
            if isinstance(receipt, bytes):
                receipt_path.write_bytes(receipt)
            else:
                receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
        return raw_dir

    def diagnose(self, raw_dir: Path) -> tuple[str, str]:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            diagnose_orders(raw_dir)
        output = stdout.getvalue()
        self.assertLessEqual(len(output.encode("utf-8")), MAX_OUTPUT_BYTES)
        self.assertEqual(stderr.getvalue(), "")
        return output, stderr.getvalue()

    def make_receipt(self, orders_path: Path, *, url: str = EXPECTED_URL) -> dict[str, object]:
        digest = hashlib.sha256(orders_path.read_bytes()).hexdigest()
        return {
            "url": url,
            "version": 1,
            "archive_sha256": "a" * 64,
            "files": {"orders.csv": {"rows": 1, "sha256": digest}},
        }

    def run_diagnostic_cli(self, raw_dir: Path, startup_hook: str) -> subprocess.CompletedProcess[bytes]:
        with tempfile.TemporaryDirectory() as temporary:
            Path(temporary, "sitecustomize.py").write_text(startup_hook, encoding="utf-8")
            environment = os.environ.copy()
            environment["PYTHONPATH"] = os.pathsep.join((temporary, str(REPOSITORY_ROOT)))
            environment["PYTHONDONTWRITEBYTECODE"] = "1"
            return subprocess.run(
                [
                    sys.executable,
                    "-B",
                    "-m",
                    "src.module1.raw_diagnostic",
                    "--raw-dir",
                    str(raw_dir),
                ],
                cwd=REPOSITORY_ROOT,
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=10,
                check=False,
            )

    def test_optional_pandas_import_failures_are_sanitized_by_cli(self):
        with tempfile.TemporaryDirectory() as temporary:
            raw_dir = self.make_raw_dir(temporary, f"{HEADER}\n1,2,train,1,0,24,7\n")
            for exception_name in ("RuntimeError", "ValueError", "ImportError"):
                with self.subTest(exception_name=exception_name):
                    startup_hook = textwrap.dedent(
                        f"""
                        import importlib.abc
                        import sys

                        class BrokenPandasImport(importlib.abc.MetaPathFinder):
                            def find_spec(self, fullname, path=None, target=None):
                                if fullname == "pandas":
                                    raise {exception_name}(
                                        "SYNTH_IMPORT_FAILURE_" + "x" * 20000
                                    )

                        sys.meta_path.insert(0, BrokenPandasImport())
                        """
                    )
                    result = self.run_diagnostic_cli(raw_dir, startup_hook)
                    combined = result.stdout + result.stderr

                    self.assertEqual(result.returncode, 0)
                    self.assertLessEqual(len(result.stdout) + len(result.stderr), MAX_OUTPUT_BYTES)
                    self.assertNotIn(b"Traceback", combined)
                    self.assertNotIn(b"SYNTH_IMPORT_FAILURE_", combined)
                    self.assertNotIn(b"x" * 256, combined)
                    self.assertNotIn(b"Error:", result.stderr)
                    if exception_name == "ImportError":
                        self.assertIn(b"pandas_scan=unavailable reason=pandas_not_installed", result.stdout)
                    else:
                        self.assertIn(
                            f"pandas_scan=error error_type={exception_name}".encode(),
                            result.stdout,
                        )
                    self.assertEqual(result.stderr, b"")

    def test_successful_pandas_import_uses_normal_scan_path(self):
        with tempfile.TemporaryDirectory() as temporary:
            raw_dir = self.make_raw_dir(temporary, f"{HEADER}\n")
            startup_hook = textwrap.dedent(
                """
                import sys
                import types

                pandas = types.ModuleType("pandas")

                def read_csv(source, **options):
                    assert options == {
                        "dtype": "string",
                        "keep_default_na": False,
                        "chunksize": 4096,
                        "engine": "c",
                    }
                    assert source.getvalue().startswith(b"order_id,user_id,")
                    return iter(())

                pandas.read_csv = read_csv
                sys.modules["pandas"] = pandas
                """
            )
            result = self.run_diagnostic_cli(raw_dir, startup_hook)

        self.assertEqual(result.returncode, 0)
        self.assertLessEqual(len(result.stdout) + len(result.stderr), MAX_OUTPUT_BYTES)
        self.assertIn(b"csv_first_invalid_field=none logical_records_scanned=1", result.stdout)
        self.assertIn(b"pandas_first_invalid_field=none dataframe_rows_scanned=0", result.stdout)
        self.assertEqual(result.stderr, b"")

    def test_workflow_preserves_preflight_status_when_diagnostic_succeeds_or_fails(self):
        workflow = (REPOSITORY_ROOT / ".github/workflows/module3-full.yml").read_text(encoding="utf-8")
        step = workflow.split(
            "      - name: Regenerate bounded Module 1 feed\n        run: |\n", 1
        )[1].split("      - name: Build pinned Spark runtime", 1)[0]
        workflow_script = textwrap.dedent(step)
        command_stub = textwrap.dedent(
            """
            python() {
              case "$*" in
                "scripts/fetch_instacart.py") return 0 ;;
                "-m src.module1.clean --validate") return "$PREFLIGHT_STATUS" ;;
                "-m src.module1.raw_diagnostic --raw-dir data/raw") printf 'diagnostic-ran\\n'; return "$DIAGNOSTIC_STATUS" ;;
                *"src.module1.generate"*) printf 'generation-ran\\n'; return 0 ;;
                *) return 90 ;;
              esac
            }
            """
        )

        for preflight_status, diagnostic_status, expected_status in (
            (0, 0, 0),
            (2, 0, 2),
            (2, 1, 2),
        ):
            with self.subTest(preflight_status=preflight_status, diagnostic_status=diagnostic_status):
                environment = os.environ.copy()
                environment["PREFLIGHT_STATUS"] = str(preflight_status)
                environment["DIAGNOSTIC_STATUS"] = str(diagnostic_status)
                result = subprocess.run(
                    ["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", command_stub + workflow_script],
                    cwd=REPOSITORY_ROOT,
                    env=environment,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    timeout=10,
                    check=False,
                )
                self.assertEqual(result.returncode, expected_status)
                self.assertEqual(b"diagnostic-ran" in result.stdout, preflight_status != 0)
                self.assertEqual(b"generation-ran" in result.stdout, preflight_status == 0)

    def test_receipt_urls_and_control_text_are_never_emitted(self):
        untrusted_values = (
            "https://www.kaggle.com/api/v1/datasets/download/psparks/data?token=SYNTH_TOKEN",
            "https://user:SYNTH_USERINFO@www.kaggle.com/data#SYNTH_FRAGMENT",
            "https://www.kaggle.com/data\nSYNTH_LOG_INJECTION\x1b[31m",
        )
        for untrusted in untrusted_values:
            with self.subTest(url=untrusted[:24]), tempfile.TemporaryDirectory() as temporary:
                raw_dir = self.make_raw_dir(temporary, f"{HEADER}\n1,2,train,1,0,24,7\n")
                receipt = self.make_receipt(raw_dir / "orders.csv", url=untrusted)
                receipt["comment"] = "SYNTH_CONTROL\r\nINJECTED\x00"
                (raw_dir / "source-receipt.json").write_text(json.dumps(receipt), encoding="utf-8")

                output, _ = self.diagnose(raw_dir)
                self.assertIn("source_url_matches_expected=False", output)
                self.assertNotIn("source_url=", output)
                self.assertNotIn("SYNTH_TOKEN", output)
                self.assertNotIn("SYNTH_USERINFO", output)
                self.assertNotIn("SYNTH_FRAGMENT", output)
                self.assertNotIn("SYNTH_LOG_INJECTION", output)
                self.assertNotIn("SYNTH_CONTROL", output)
                self.assertNotIn("INJECTED", output)

    def test_valid_receipt_reports_only_allowlisted_provenance(self):
        with tempfile.TemporaryDirectory() as temporary:
            raw_dir = self.make_raw_dir(temporary, f"{HEADER}\n1,2,train,1,0,24,7\n")
            receipt = self.make_receipt(raw_dir / "orders.csv")
            (raw_dir / "source-receipt.json").write_text(json.dumps(receipt), encoding="utf-8")
            expected_hash = hashlib.sha256((raw_dir / "orders.csv").read_bytes()).hexdigest()
            output, _ = self.diagnose(raw_dir)

        self.assertIn("receipt_valid=True", output)
        self.assertIn("source_url_matches_expected=True", output)
        self.assertIn("dataset_version=1", output)
        self.assertIn(f"orders_sha256={expected_hash}", output)
        self.assertIn("orders_sha256_matches_receipt=True", output)
        self.assertNotIn("source_url=", output)

    def test_extremely_long_receipt_is_bounded_and_hidden(self):
        with tempfile.TemporaryDirectory() as temporary:
            raw_dir = self.make_raw_dir(temporary, f"{HEADER}\n1,2,train,1,0,24,7\n")
            (raw_dir / "source-receipt.json").write_text(
                json.dumps({"url": "SYNTH_LONG_RECEIPT_TOKEN" + "x" * (2 * 1024 * 1024)}),
                encoding="utf-8",
            )
            output, _ = self.diagnose(raw_dir)

        self.assertIn("receipt_status=too_large", output)
        self.assertNotIn("SYNTH_LONG_RECEIPT_TOKEN", output)

    def test_thousands_of_unexpected_header_columns_are_summarized(self):
        extras = [f"private_extra_{index}" for index in range(2000)]
        content = ",".join((*ORDERS_COLUMNS, *extras)) + "\n"
        with tempfile.TemporaryDirectory() as temporary:
            raw_dir = self.make_raw_dir(temporary, content)
            output, _ = self.diagnose(raw_dir)

        self.assertIn("csv_header_valid=False", output)
        self.assertIn("header_column_count=2007", output)
        self.assertIn("header_extra_count=2000", output)
        self.assertNotIn("private_extra_", output)

    def test_headerless_first_record_is_not_reported_as_header_or_data(self):
        sentinel = "SYNTH_PRIVATE_FIRST_RECORD"
        with tempfile.TemporaryDirectory() as temporary:
            raw_dir = self.make_raw_dir(temporary, f"101,202,{sentinel},1,0,24,7\n")
            output, _ = self.diagnose(raw_dir)

        self.assertIn("reason=unrecognized_header", output)
        self.assertIn("header_metadata=unavailable", output)
        self.assertNotIn("header_column_count=", output)
        self.assertNotIn(sentinel, output)

    def test_huge_first_csv_record_stops_at_input_limit(self):
        with tempfile.TemporaryDirectory() as temporary:
            raw_dir = Path(temporary) / "raw"
            raw_dir.mkdir()
            orders_path = raw_dir / "orders.csv"
            with orders_path.open("w", encoding="utf-8", newline="") as output:
                output.write(HEADER + "\n")
                output.write("SYNTH_HUGE_RECORD_TOKEN" + "x" * (9 * 1024 * 1024))
                output.write(",2,train,1,0,24,7\n")
            diagnostic, _ = self.diagnose(raw_dir)

        self.assertIn("csv_scan=stopped reason=physical_line_limit", diagnostic)
        self.assertIn("pandas_scan=skipped reason=input_limit", diagnostic)
        self.assertNotIn("SYNTH_HUGE_RECORD_TOKEN", diagnostic)

    def test_malformed_json_receipt_is_reported_without_contents(self):
        with tempfile.TemporaryDirectory() as temporary:
            raw_dir = self.make_raw_dir(temporary, f"{HEADER}\n1,2,train,1,0,24,7\n")
            (raw_dir / "source-receipt.json").write_text(
                '{"url":"SYNTH_MALFORMED_TOKEN",\n', encoding="utf-8"
            )
            output, _ = self.diagnose(raw_dir)

        self.assertIn("receipt_status=invalid", output)
        self.assertNotIn("SYNTH_MALFORMED_TOKEN", output)

    def test_deeply_nested_receipt_json_is_bounded_and_hidden(self):
        with tempfile.TemporaryDirectory() as temporary:
            raw_dir = self.make_raw_dir(temporary, f"{HEADER}\n1,2,train,1,0,24,7\n")
            (raw_dir / "source-receipt.json").write_text("[" * 7000 + "]" * 7000, encoding="utf-8")
            output, _ = self.diagnose(raw_dir)

        self.assertIn("receipt_status=invalid", output)

    def test_extremely_wide_header_is_stopped_at_header_read_limit(self):
        extras = [f"wide_private_column_{index}" for index in range(6000)]
        content = ",".join((*ORDERS_COLUMNS, *extras)) + "\n"
        with tempfile.TemporaryDirectory() as temporary:
            raw_dir = self.make_raw_dir(temporary, content)
            output, _ = self.diagnose(raw_dir)

        self.assertIn("csv_scan=stopped reason=header_physical_line_limit", output)
        self.assertNotIn("wide_private_column_", output)

    def test_emitter_enforces_total_output_budget_and_marks_truncation(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            _emit(["x" * (MAX_OUTPUT_BYTES * 4)])

        rendered = output.getvalue()
        self.assertLessEqual(len(rendered.encode("utf-8")), MAX_OUTPUT_BYTES)
        self.assertTrue(rendered.endswith("... diagnostic output truncated ...\n"))

    def test_valid_csv_and_invalid_hour_report_csv_coordinates(self):
        content = f"{HEADER}\n1,2,train,1,0,8,7\n2,3,train,2,1,24,7\n"
        with tempfile.TemporaryDirectory() as temporary:
            raw_dir = self.make_raw_dir(temporary, content)
            output, _ = self.diagnose(raw_dir)

        self.assertIn("csv_header_valid=True", output)
        self.assertIn("csv_first_invalid_field=1 table=orders field=order_hour_of_day", output)
        self.assertIn("logical_record_ordinal=3 physical_line_number=3", output)
        self.assertIn("value_kind=out_of_range value_length=2", output)
        self.assertIn("csv_pandas_record_mapping=not_established", output)
        self.assertNotIn("csv_equals_pandas", output)

    def test_ordinary_valid_csv_has_no_csv_hour_issue(self):
        content = f"{HEADER}\n1,2,train,1,0,0,7\n2,3,prior,2,1,23,14\n"
        with tempfile.TemporaryDirectory() as temporary:
            raw_dir = self.make_raw_dir(temporary, content)
            output, _ = self.diagnose(raw_dir)

        self.assertIn("csv_first_invalid_field=none logical_records_scanned=3", output)

    def test_zero_padded_hour_is_a_valid_decimal_hour(self):
        content = f"{HEADER}\n1,2,train,1,0,08,7\n"
        with tempfile.TemporaryDirectory() as temporary:
            raw_dir = self.make_raw_dir(temporary, content)
            output, _ = self.diagnose(raw_dir)

        self.assertIn("csv_first_invalid_field=none logical_records_scanned=2", output)
        self.assertNotIn("field=order_hour_of_day", output)

    def test_non_integer_and_blank_hour_values_are_classified_without_echo(self):
        for invalid, kind in (("nope", "malformed"), ("", "blank")):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as temporary:
                raw_dir = self.make_raw_dir(temporary, f"{HEADER}\n1,2,train,1,0,{invalid},7\n")
                output, _ = self.diagnose(raw_dir)
                self.assertIn(f"value_kind={kind}", output)
                if invalid:
                    self.assertNotIn(invalid, output)

    def test_empty_line_is_counted_by_csv_reader_but_not_mapped_to_pandas(self):
        content = f"{HEADER}\n\n1,2,train,1,0,8,7\n2,3,train,2,1,24,7\n"
        with tempfile.TemporaryDirectory() as temporary:
            raw_dir = self.make_raw_dir(temporary, content)
            output, _ = self.diagnose(raw_dir)

        self.assertIn("logical_record_ordinal=4 physical_line_number=4", output)
        self.assertIn("csv_pandas_record_mapping=not_established", output)

    def test_whitespace_only_line_is_not_mapped_to_pandas_blank_handling(self):
        content = f"{HEADER}\n   \t  \n1,2,train,1,0,24,7\n"
        with tempfile.TemporaryDirectory() as temporary:
            raw_dir = self.make_raw_dir(temporary, content)
            output, _ = self.diagnose(raw_dir)

        self.assertIn("csv_first_issue=wrong_width table=orders", output)
        self.assertIn("logical_record_ordinal=2 physical_line_number=2", output)
        self.assertIn("csv_pandas_record_mapping=not_established", output)

    def test_quoted_multiline_record_keeps_physical_and_logical_coordinates_distinct(self):
        content = (
            f"{HEADER}\n"
            '1,2,"train\ncontinued",1,0,8,7\n'
            "2,3,train,2,1,24,7\n"
        )
        with tempfile.TemporaryDirectory() as temporary:
            raw_dir = self.make_raw_dir(temporary, content)
            output, _ = self.diagnose(raw_dir)

        self.assertIn("logical_record_ordinal=3 physical_line_number=4", output)
        self.assertIn("csv_pandas_record_mapping=not_established", output)

    def test_wrong_width_record_is_reported_without_row_contents(self):
        sentinel = "SYNTH_WRONG_WIDTH_SECRET"
        content = f"{HEADER}\n1,2,{sentinel}\n"
        with tempfile.TemporaryDirectory() as temporary:
            raw_dir = self.make_raw_dir(temporary, content)
            output, _ = self.diagnose(raw_dir)

        self.assertIn("csv_first_issue=wrong_width", output)
        self.assertIn("expected_columns=7 observed_columns=3", output)
        self.assertNotIn(sentinel, output)

    @unittest.skipUnless(importlib.util.find_spec("pandas"), "pandas-specific scan not run: pandas unavailable")
    def test_pandas_position_is_independent_when_whitespace_blank_handling_disagrees(self):
        content = f"{HEADER}\n   \t  \n1,2,train,1,0,24,7\n"
        with tempfile.TemporaryDirectory() as temporary:
            raw_dir = self.make_raw_dir(temporary, content)
            output, _ = self.diagnose(raw_dir)

        self.assertIn("csv_first_issue=wrong_width", output)
        self.assertIn("pandas_first_invalid_field=1", output)
        self.assertIn("dataframe_position_1based=1", output)
        self.assertIn("record_mapping=not_established", output)
        self.assertNotIn("csv_equals_pandas", output)

    @unittest.skipUnless(importlib.util.find_spec("pandas"), "pandas-specific scan not run: pandas unavailable")
    def test_pandas_reports_its_position_for_multiline_input_without_csv_equivalence_claim(self):
        content = f'{HEADER}\n1,2,"train\ncontinued",1,0,8,7\n2,3,train,2,1,24,7\n'
        with tempfile.TemporaryDirectory() as temporary:
            raw_dir = self.make_raw_dir(temporary, content)
            output, _ = self.diagnose(raw_dir)

        self.assertIn("logical_record_ordinal=3 physical_line_number=4", output)
        self.assertIn("dataframe_position_1based=2", output)
        self.assertIn("record_mapping=not_established", output)


if __name__ == "__main__":
    unittest.main()
