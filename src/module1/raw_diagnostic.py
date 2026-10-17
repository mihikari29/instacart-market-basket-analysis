"""Print a bounded diagnostic for an invalid downloaded orders.csv file."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from importlib.metadata import PackageNotFoundError, version
import platform
from pathlib import Path
import re
from typing import Any


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_ORDER_HOUR = "order_hour_of_day"
_ORDER_HOUR_MIN = 0
_ORDER_HOUR_MAX = 23
_CHUNK_ROWS = 65_536


def _package_version(distribution: str) -> str:
    try:
        return version(distribution)
    except PackageNotFoundError:
        return "unavailable"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _bounded_repr(value: str, limit: int = 80) -> str:
    if len(value) <= limit:
        return repr(value)
    return f"{value[:limit]!r}…(truncated)"


def _csv_field(path: Path, column: str, row_number: int) -> str | None:
    with path.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.reader(source)
        header = next(reader, None)
        if header is None or column not in header:
            return None
        index = header.index(column)
        for current_row, row in enumerate((record for record in reader if record), start=2):
            if current_row == row_number:
                return row[index] if index < len(row) else None
    return None


def _print_source_metadata(raw_dir: Path, orders_path: Path) -> None:
    receipt_path = raw_dir / "source-receipt.json"
    try:
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        receipt = {}
    if not isinstance(receipt, dict):
        receipt = {}

    url = receipt.get("url")
    if not isinstance(url, str) or not url.startswith("https://www.kaggle.com/api/v1/datasets/download/"):
        url = "unavailable"
    dataset_version = receipt.get("version")
    if not isinstance(dataset_version, int) or isinstance(dataset_version, bool):
        dataset_version = "unavailable"
    archive_sha = receipt.get("archive_sha256")
    if not isinstance(archive_sha, str) or not _SHA256.fullmatch(archive_sha):
        archive_sha = "unavailable"

    files = receipt.get("files")
    orders_receipt = files.get("orders.csv") if isinstance(files, dict) else None
    receipt_sha = orders_receipt.get("sha256") if isinstance(orders_receipt, dict) else None
    if not isinstance(receipt_sha, str) or not _SHA256.fullmatch(receipt_sha):
        receipt_sha = "unavailable"

    print(f"source_url={url}")
    print(f"dataset_version={dataset_version}")
    print(f"archive_sha256={archive_sha}")
    print(f"orders_receipt_sha256={receipt_sha}")
    print(f"python_version={platform.python_version()}")
    print(f"pandas_version={_package_version('pandas')}")
    print(f"pyarrow_version={_package_version('pyarrow')}")
    print("pandas_csv_options=engine=c,dtype=string,keep_default_na=false,chunksize=65536")

    try:
        actual_sha = _sha256(orders_path)
    except OSError as exc:
        print(f"orders_sha256=unavailable error_type={type(exc).__name__}")
    else:
        matches = receipt_sha == actual_sha if receipt_sha != "unavailable" else "unverifiable"
        print(f"orders_sha256={actual_sha}")
        print(f"orders_sha256_matches_receipt={matches}")


def _header_result(path: Path, expected: tuple[str, ...]) -> tuple[bool, list[str], list[str], list[str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as source:
        header = next(csv.reader(source))
    duplicates = sorted({name for name in header if header.count(name) > 1})
    missing = sorted(set(expected) - set(header))
    extra = sorted(set(header) - set(expected))
    valid = len(header) == len(expected) and not duplicates and not missing and not extra
    return valid, duplicates, missing, extra


def diagnose_orders(raw_dir: Path) -> None:
    """Report only source identity, schema checks, versions, and the first bad hour field."""
    orders_path = raw_dir / "orders.csv"
    _print_source_metadata(raw_dir, orders_path)
    if not orders_path.is_file():
        print("orders_diagnostic=unavailable reason=orders_csv_missing")
        return

    try:
        import pandas as pd

        from .clean import RAW_COLUMNS, _INTEGER

        header_valid, duplicates, missing, extra = _header_result(orders_path, RAW_COLUMNS["orders"])
        print(f"csv_header_valid={header_valid}")
        print(f"csv_header_duplicates={json.dumps(duplicates)}")
        print(f"csv_header_missing={json.dumps(missing)}")
        print(f"csv_header_extra={json.dumps(extra)}")
        if not header_valid:
            return

        rows_scanned = 0
        for chunk in pd.read_csv(
            orders_path,
            dtype="string",
            keep_default_na=False,
            chunksize=_CHUNK_ROWS,
            engine="c",
        ):
            values = chunk[_ORDER_HOUR].astype("string")
            valid_text = values.str.fullmatch(_INTEGER.pattern, na=False)
            parsed = pd.to_numeric(values.where(valid_text), errors="coerce")
            invalid = ~valid_text | parsed.isna() | (parsed < _ORDER_HOUR_MIN) | (parsed > _ORDER_HOUR_MAX)
            invalid_positions = invalid.to_numpy().nonzero()[0]
            if invalid_positions.size:
                position = int(invalid_positions[0])
                row_number = rows_scanned + position + 2
                pandas_raw = str(values.iloc[position])
                csv_raw = _csv_field(orders_path, _ORDER_HOUR, row_number)
                parsed_value: Any = parsed.iloc[position]
                parsed_text = "null" if pd.isna(parsed_value) else str(parsed_value)
                within_domain = (
                    not pd.isna(parsed_value)
                    and _ORDER_HOUR_MIN <= parsed_value <= _ORDER_HOUR_MAX
                )
                print(f"field={_ORDER_HOUR} row={row_number}")
                print(f"csv_raw_repr={_bounded_repr(csv_raw) if csv_raw is not None else 'unavailable'}")
                print(f"pandas_raw_repr={_bounded_repr(pandas_raw)}")
                print(f"csv_equals_pandas={csv_raw == pandas_raw if csv_raw is not None else 'unverifiable'}")
                print(f"integer_pattern_valid={bool(valid_text.iloc[position])}")
                print(f"pandas_numeric_value={parsed_text}")
                print(f"within_domain_0_to_23={within_domain}")
                return
            rows_scanned += len(chunk)
        print(f"first_invalid_field=none rows_scanned={rows_scanned}")
    except Exception as exc:
        print(f"orders_diagnostic=error error_type={type(exc).__name__}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", type=Path, default=Path("data/raw"))
    args = parser.parse_args()
    diagnose_orders(args.raw_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
