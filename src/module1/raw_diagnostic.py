"""Print a bounded, metadata-only diagnostic for an invalid orders.csv file."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
from importlib.metadata import version
from pathlib import Path
import platform
import re
from typing import Any, BinaryIO, Iterator


_EXPECTED_SOURCE_URL = (
    "https://www.kaggle.com/api/v1/datasets/download/"
    "psparks/instacart-market-basket-analysis?datasetVersionNumber=1"
)
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_SAFE_VERSION = re.compile(r"[A-Za-z0-9.+!_-]{1,64}\Z")
_INTEGER = re.compile(r"(?:0|[1-9][0-9]*)\Z")
_ORDER_HOUR = "order_hour_of_day"
_ORDER_COLUMNS = (
    "order_id",
    "user_id",
    "eval_set",
    "order_number",
    "order_dow",
    _ORDER_HOUR,
    "days_since_prior_order",
)
_ORDER_HOUR_MIN = 0
_ORDER_HOUR_MAX = 23
_CHUNK_ROWS = 4096

# The hard output cap includes the truncation marker and newline.
MAX_OUTPUT_BYTES = 8192
MAX_RECEIPT_BYTES = 16 * 1024
MAX_HEADER_BYTES = 64 * 1024
MAX_CSV_SCAN_BYTES = 8 * 1024 * 1024
MAX_PHYSICAL_LINE_BYTES = 64 * 1024
MAX_CSV_FIELD_CHARS = 64 * 1024
MAX_PANDAS_INPUT_BYTES = 8 * 1024 * 1024

_OUTPUT_TRUNCATION_MARKER = "... diagnostic output truncated ...\n"


class _InputLimitReached(Exception):
    """A bounded CSV read reached its configured input limit."""

    def __init__(self, reason: str):
        self.reason = reason


class _BoundedPhysicalLines(Iterator[str]):
    """Decode physical CSV lines without reading past explicit byte limits."""

    def __init__(self, source: BinaryIO, max_bytes: int):
        self._source = source
        self._max_bytes = max_bytes
        self.bytes_read = 0
        self.physical_lines = 0
        self.digest = hashlib.sha256()

    def __iter__(self) -> _BoundedPhysicalLines:
        return self

    def __next__(self) -> str:
        remaining = self._max_bytes - self.bytes_read
        read_limit = min(MAX_PHYSICAL_LINE_BYTES + 1, remaining + 1)
        raw = self._source.readline(read_limit)
        if not raw:
            raise StopIteration
        if len(raw) > MAX_PHYSICAL_LINE_BYTES:
            raise _InputLimitReached("physical_line_limit")
        if len(raw) > remaining:
            raise _InputLimitReached("byte_limit")
        self.bytes_read += len(raw)
        self.physical_lines += 1
        self.digest.update(raw)
        try:
            return raw.decode("utf-8-sig" if self.physical_lines == 1 else "utf-8")
        except UnicodeDecodeError as exc:
            raise _InputLimitReached("invalid_utf8") from exc


def _emit(lines: list[str]) -> None:
    """Write at most MAX_OUTPUT_BYTES, including a visible truncation marker."""
    output = ("\n".join(lines) + "\n").encode("utf-8", errors="replace")
    if len(output) <= MAX_OUTPUT_BYTES:
        print(output.decode("utf-8"), end="")
        return

    marker = _OUTPUT_TRUNCATION_MARKER.encode("ascii")
    prefix = output[: MAX_OUTPUT_BYTES - len(marker)].decode("utf-8", errors="ignore")
    print(prefix + _OUTPUT_TRUNCATION_MARKER, end="")


def _exception_type(exc: BaseException) -> str:
    name = type(exc).__name__
    return name if re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,63}", name) else "OtherError"


def _package_version(distribution: str) -> str:
    try:
        found = version(distribution)
    except Exception:
        return "unavailable"
    return found if _SAFE_VERSION.fullmatch(found) else "unavailable"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _read_receipt(raw_dir: Path) -> tuple[dict[str, Any], str]:
    receipt_path = raw_dir / "source-receipt.json"
    try:
        with receipt_path.open("rb") as source:
            raw = source.read(MAX_RECEIPT_BYTES + 1)
    except FileNotFoundError:
        return {}, "missing"
    except OSError:
        return {}, "unavailable"
    if len(raw) > MAX_RECEIPT_BYTES:
        return {}, "too_large"
    try:
        receipt = json.loads(raw.decode("utf-8"))
    except Exception:
        return {}, "invalid"
    if not isinstance(receipt, dict):
        return {}, "invalid"
    return receipt, "parsed"


def _source_metadata(raw_dir: Path, orders_path: Path) -> list[str]:
    receipt, receipt_status = _read_receipt(raw_dir)
    source_url = receipt.get("url")
    url_matches = source_url == _EXPECTED_SOURCE_URL if isinstance(source_url, str) else False

    dataset_version = receipt.get("version")
    version_is_expected = type(dataset_version) is int and dataset_version == 1
    archive_sha = receipt.get("archive_sha256")
    archive_sha_valid = isinstance(archive_sha, str) and bool(_SHA256.fullmatch(archive_sha))

    files = receipt.get("files")
    orders_receipt = files.get("orders.csv") if isinstance(files, dict) else None
    receipt_sha = orders_receipt.get("sha256") if isinstance(orders_receipt, dict) else None
    receipt_sha_valid = isinstance(receipt_sha, str) and bool(_SHA256.fullmatch(receipt_sha))

    actual_sha: str | None
    actual_sha_error: str | None
    try:
        actual_sha = _sha256(orders_path)
        actual_sha_error = None
    except OSError as exc:
        actual_sha = None
        actual_sha_error = _exception_type(exc)

    receipt_valid = (
        receipt_status == "parsed"
        and url_matches
        and version_is_expected
        and archive_sha_valid
        and receipt_sha_valid
        and actual_sha is not None
        and receipt_sha == actual_sha
    )
    receipt_present = (
        "true"
        if receipt_status in {"parsed", "invalid", "too_large"}
        else "false"
        if receipt_status == "missing"
        else "unknown"
    )

    lines = [
        "source_id=psparks/instacart-market-basket-analysis",
        f"receipt_present={receipt_present}",
        f"receipt_status={receipt_status}",
        f"receipt_valid={receipt_valid}",
        f"source_url_matches_expected={url_matches}",
        f"dataset_version={dataset_version if version_is_expected else 'unavailable'}",
        f"archive_sha256={archive_sha if archive_sha_valid else 'unavailable'}",
        f"orders_receipt_sha256={receipt_sha if receipt_sha_valid else 'unavailable'}",
        f"python_version={platform.python_version()}",
        f"pandas_version={_package_version('pandas')}",
        f"pyarrow_version={_package_version('pyarrow')}",
        "pandas_csv_options=engine=c,dtype=string,keep_default_na=false,skip_blank_lines=true,chunksize=4096",
    ]

    if actual_sha is None:
        lines.append(f"orders_sha256=unavailable error_type={actual_sha_error}")
    else:
        matches = receipt_sha == actual_sha if receipt_sha_valid else "unverifiable"
        lines.extend((f"orders_sha256={actual_sha}", f"orders_sha256_matches_receipt={matches}"))
    return lines


def _csv_field_kind(value: str) -> str | None:
    if value == "":
        return "blank"
    if not _INTEGER.fullmatch(value):
        return "malformed"
    # The accepted domain is two digits at most, so avoid converting attacker-sized integers.
    if len(value) > 2 or not _ORDER_HOUR_MIN <= int(value) <= _ORDER_HOUR_MAX:
        return "out_of_range"
    return None


def _csv_scan(path: Path, expected: tuple[str, ...]) -> tuple[list[str], bool]:
    """Report the CSV reader's first issue with its own record and physical-line coordinates."""
    lines: list[str] = []
    previous_field_limit = csv.field_size_limit()
    csv.field_size_limit(MAX_CSV_FIELD_CHARS)
    try:
        with path.open("rb") as source:
            bounded_lines = _BoundedPhysicalLines(source, MAX_CSV_SCAN_BYTES)
            bounded_lines._max_bytes = MAX_HEADER_BYTES
            reader = csv.reader(bounded_lines, strict=True)
            try:
                header = next(reader)
            except StopIteration:
                return ["csv_scan=unavailable reason=empty_file"], False
            except _InputLimitReached as exc:
                return [f"csv_scan=stopped reason=header_{exc.reason}"], False
            except (csv.Error, UnicodeError) as exc:
                return [f"csv_scan=error error_type={_exception_type(exc)}"], False

            bounded_lines._max_bytes = MAX_CSV_SCAN_BYTES

            expected_set = set(expected)
            recognized = bool(expected_set.intersection(header))
            if not recognized:
                return [
                    "csv_header_valid=False",
                    "header_metadata=unavailable reason=unrecognized_header",
                    "csv_scan=unavailable reason=unrecognized_header",
                ], False

            missing_count = len(expected_set - set(header))
            extra_count = len(set(header) - expected_set)
            duplicate_count = len(header) - len(set(header))
            header_valid = (
                len(header) == len(expected)
                and missing_count == 0
                and extra_count == 0
                and duplicate_count == 0
            )
            lines.extend(
                (
                    f"csv_header_valid={header_valid}",
                    f"header_column_count={len(header)}",
                    f"header_missing_expected_count={missing_count}",
                    f"header_extra_count={extra_count}",
                    f"header_duplicate_count={duplicate_count}",
                    f"header_byte_count={bounded_lines.bytes_read}",
                )
            )
            if not header_valid:
                return lines + ["csv_scan=unavailable reason=invalid_header"], False
            lines.append(f"header_sha256={bounded_lines.digest.hexdigest()}")
            lines.append("logical_record_ordinal_basis=csv_reader_records_including_empty_records")
            lines.append("physical_line_number_basis=csv_reader_line_num_record_end")

            order_hour_index = header.index(_ORDER_HOUR)
            logical_record_ordinal = 1
            try:
                for row in reader:
                    logical_record_ordinal += 1
                    physical_line_number = reader.line_num
                    if not row:
                        continue
                    if len(row) != len(header):
                        lines.append(
                            "csv_first_issue=wrong_width table=orders "
                            f"logical_record_ordinal={logical_record_ordinal} "
                            f"physical_line_number={physical_line_number} "
                            f"expected_columns={len(header)} observed_columns={len(row)}"
                        )
                        return lines, True
                    value = row[order_hour_index]
                    value_kind = _csv_field_kind(value)
                    if value_kind is not None:
                        lines.append(
                            "csv_first_invalid_field=1 table=orders field=order_hour_of_day "
                            f"logical_record_ordinal={logical_record_ordinal} "
                            f"physical_line_number={physical_line_number} "
                            f"value_kind={value_kind} value_length={len(value)}"
                        )
                        return lines, True
            except _InputLimitReached as exc:
                return lines + [
                    f"csv_scan=stopped reason={exc.reason} logical_records_scanned={logical_record_ordinal}"
                ], True
            except (csv.Error, UnicodeError) as exc:
                return lines + [f"csv_scan=error error_type={_exception_type(exc)}"], True
            except OSError as exc:
                return lines + [f"csv_scan=error error_type={_exception_type(exc)}"], True

            return [f"csv_first_invalid_field=none logical_records_scanned={logical_record_ordinal}"], True
    except OSError as exc:
        return [f"csv_scan=error error_type={_exception_type(exc)}"], False
    finally:
        csv.field_size_limit(previous_field_limit)


def _pandas_scan(path: Path) -> list[str]:
    """Report pandas' independent parsed position, within a fixed input-size limit."""
    try:
        with path.open("rb") as source:
            raw_input = source.read(MAX_PANDAS_INPUT_BYTES + 1)
    except OSError as exc:
        return [f"pandas_scan=error error_type={_exception_type(exc)}"]
    if len(raw_input) > MAX_PANDAS_INPUT_BYTES:
        return [
            "pandas_scan=skipped reason=input_limit "
            f"bytes_read_at_least={MAX_PANDAS_INPUT_BYTES + 1}"
        ]

    try:
        import pandas as pd
    except ImportError:
        return ["pandas_scan=unavailable reason=pandas_not_installed"]
    except Exception as exc:
        return [f"pandas_scan=error error_type={_exception_type(exc)}"]

    rows_scanned = 0
    try:
        for chunk in pd.read_csv(
            io.BytesIO(raw_input),
            dtype="string",
            keep_default_na=False,
            chunksize=_CHUNK_ROWS,
            engine="c",
        ):
            values = chunk[_ORDER_HOUR].astype("string")
            valid_text = values.str.fullmatch(_INTEGER.pattern, na=False)
            parsed = pd.to_numeric(values.where(valid_text), errors="coerce")
            invalid = (
                ~valid_text
                | parsed.isna()
                | (parsed < _ORDER_HOUR_MIN)
                | (parsed > _ORDER_HOUR_MAX)
            )
            invalid_positions = invalid.to_numpy().nonzero()[0]
            if invalid_positions.size:
                position = int(invalid_positions[0])
                value = str(values.iloc[position])
                kind = _csv_field_kind(value) or "invalid"
                dataframe_position = rows_scanned + position + 1
                return [
                    "pandas_first_invalid_field=1 table=orders field=order_hour_of_day "
                    f"dataframe_position_1based={dataframe_position} value_kind={kind} "
                    f"value_length={len(value)} record_mapping=not_established"
                ]
            rows_scanned += len(chunk)
    except Exception as exc:
        return [f"pandas_scan=error error_type={_exception_type(exc)}"]
    return [f"pandas_first_invalid_field=none dataframe_rows_scanned={rows_scanned}"]


def diagnose_orders(raw_dir: Path) -> None:
    """Report bounded source metadata and independent CSV and pandas findings."""
    orders_path = raw_dir / "orders.csv"
    lines = _source_metadata(raw_dir, orders_path)
    if not orders_path.is_file():
        lines.append("orders_diagnostic=unavailable reason=orders_csv_missing")
        _emit(lines)
        return

    csv_lines, header_valid = _csv_scan(orders_path, _ORDER_COLUMNS)
    lines.extend(csv_lines)
    if not header_valid:
        # A headerless file's first data record is intentionally never echoed or treated as header data.
        if not any(line.startswith("header_metadata=") for line in csv_lines):
            lines.append("header_metadata=unavailable reason=header_not_validated")
        lines.append("pandas_scan=skipped reason=header_not_validated")
    else:
        lines.extend(_pandas_scan(orders_path))
    lines.append("csv_pandas_record_mapping=not_established")
    _emit(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", type=Path, default=Path("data/raw"))
    args = parser.parse_args()
    diagnose_orders(args.raw_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
