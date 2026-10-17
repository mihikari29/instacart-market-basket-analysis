"""Pure, bounded metadata inspection for the six uploaded Instacart CSV inputs."""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path


REQUIRED_HEADERS = {
    "orders.csv": ("order_id", "user_id", "eval_set", "order_number", "order_dow", "order_hour_of_day", "days_since_prior_order"),
    "order_products__prior.csv": ("order_id", "product_id", "add_to_cart_order", "reordered"),
    "order_products__train.csv": ("order_id", "product_id", "add_to_cart_order", "reordered"),
    "products.csv": ("product_id", "product_name", "aisle_id", "department_id"),
    "aisles.csv": ("aisle_id", "aisle"),
    "departments.csv": ("department_id", "department"),
}


class InputReceiptError(ValueError):
    """The input directory does not contain six readable and compatible CSVs."""


def source_manifest_digest(records: dict) -> str:
    canonical = json.dumps(
        {name: record["sha256"] for name, record in sorted(records.items())},
        sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def inspect_input_directory(directory: Path | str) -> dict:
    """Check exact filenames/headers and stream hashes/counts without loading rows into RAM."""
    root = Path(directory)
    if root.is_symlink() or not root.is_dir():
        raise InputReceiptError("input path must be a non-symlink directory")
    actual = {path.name for path in root.iterdir() if not path.name.startswith(".")}
    expected = set(REQUIRED_HEADERS)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        raise InputReceiptError(f"input directory must contain exactly six required CSV files (missing={missing}, extra={extra})")

    records = {}
    for name, expected_header in REQUIRED_HEADERS.items():
        path = root / name
        if path.is_symlink() or not path.is_file():
            raise InputReceiptError(f"{name} must be a regular file")
        digest = hashlib.sha256()
        with path.open("rb") as binary:
            for block in iter(lambda: binary.read(1024 * 1024), b""):
                digest.update(block)
        try:
            with path.open("r", encoding="utf-8-sig", newline="") as stream:
                rows = csv.reader(stream)
                header = tuple(next(rows))
                if header != expected_header:
                    raise InputReceiptError(f"{name} header must exactly match the declared Instacart schema")
                row_count = 0
                for row_count, row in enumerate(rows, start=1):
                    if len(row) != len(expected_header):
                        raise InputReceiptError(f"{name} row {row_count + 1} has the wrong number of fields")
        except (OSError, UnicodeError, csv.Error, StopIteration) as exc:
            raise InputReceiptError(f"{name} could not be read as a valid UTF-8 CSV") from exc
        records[name] = {
            "rows": row_count,
            "bytes": path.stat().st_size,
            "sha256": digest.hexdigest(),
        }
    return {
        "schema_version": 1,
        "file_count": len(records),
        "total_bytes": sum(item["bytes"] for item in records.values()),
        "files": records,
        "manifest_sha256": source_manifest_digest(records),
    }
