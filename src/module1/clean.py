"""Module 1 - Step 1: Clean raw Instacart CSVs per specification.

Fixes applied:
  1. NaN days_since_prior_order (first-ever order, order_number=1) -> 0.0
  2. Cap-30 reconstruction: minimum weekday-consistent synthetic gap in [30, 36]
  3. Non-breaking space (\\xa0) stripped from product_name
  4. eval_set='test' preserved and flagged (excluded from train/streaming interactions)
  5. Narrow data types applied across all columns for memory efficiency
  6. Unpurchased catalog products retained

Usage:
  python -m src.module1.clean [--data-dir data] [--output-dir data/clean] [--validate]
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import shutil
import sys
import tempfile
import uuid
from pathlib import Path
import pandas as pd

from .config import PARQUET_COMPRESSION

RAW_DATA_TYPES = {
    "orders": {
        "order_id": "int32",
        "user_id": "int32",
        "eval_set": "category",
        "order_number": "int16",
        "order_dow": "int16",
        "order_hour_of_day": "int16",
        "days_since_prior_order": "float32",
    },
    "order_products__prior": {
        "order_id": "int32",
        "product_id": "int32",
        "add_to_cart_order": "int32",
        "reordered": "int8",
    },
    "order_products__train": {
        "order_id": "int32",
        "product_id": "int32",
        "add_to_cart_order": "int32",
        "reordered": "int8",
    },
    "products": {
        "product_id": "int32",
        "aisle_id": "int16",
        "department_id": "int8",
        "product_name": "string",
    },
    "aisles": {"aisle_id": "int16", "aisle": "string"},
    "departments": {"department_id": "int8", "department": "string"},
}

RAW_COLUMNS = {
    "orders": (
        "order_id", "user_id", "eval_set", "order_number", "order_dow",
        "order_hour_of_day", "days_since_prior_order",
    ),
    "order_products__prior": ("order_id", "product_id", "add_to_cart_order", "reordered"),
    "order_products__train": ("order_id", "product_id", "add_to_cart_order", "reordered"),
    "products": ("product_id", "product_name", "aisle_id", "department_id"),
    "aisles": ("aisle_id", "aisle"),
    "departments": ("department_id", "department"),
}
_CLEAN_OUTPUT_MANIFEST = ".it4931-clean-output.json"
_CLEAN_OUTPUT_FILES = frozenset(
    {
        "orders.parquet",
        "order_products__prior.parquet",
        "order_products__train.parquet",
        "products.parquet",
        "aisles.parquet",
        "departments.parquet",
    }
)
_INTEGER = re.compile(r"(?:0|[1-9][0-9]*)\Z")
# Kaggle's version-1 source uses zero-padded hours such as "08". Accept that
# representation only for this field, then apply the same integer domain check.
_ORDER_HOUR_INTEGER = re.compile(r"[0-9]{1,2}\Z")
_NUMBER = re.compile(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)\Z")
_MAX_DIAGNOSTICS = 20


class RawInputValidationError(ValueError):
    """Sanitized, actionable failure in the six raw Instacart source tables."""

    def __init__(self, diagnostics: list[str]):
        self.diagnostics = tuple(diagnostics[:_MAX_DIAGNOSTICS])
        summary = "; ".join(self.diagnostics)
        if len(diagnostics) > _MAX_DIAGNOSTICS:
            summary += f"; and {len(diagnostics) - _MAX_DIAGNOSTICS} more issue(s)"
        super().__init__(f"raw input preflight failed: {summary}")


def _header(path: Path, table: str) -> list[str]:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            return next(csv.reader(handle))
    except (OSError, UnicodeError, csv.Error, StopIteration) as exc:
        raise RawInputValidationError([f"{table}.csv: cannot read a CSV header"]) from exc


def _integer_column(frame: pd.DataFrame, table: str, column: str, low: int, high: int, errors: list[str]) -> None:
    values = frame[column].astype("string")
    pattern = _ORDER_HOUR_INTEGER if (table, column) == ("orders", "order_hour_of_day") else _INTEGER
    valid_text = values.str.fullmatch(pattern.pattern, na=False)
    parsed = pd.to_numeric(values.where(valid_text), errors="coerce")
    invalid = ~valid_text | parsed.isna() | (parsed < low) | (parsed > high)
    if invalid.any():
        row = int(invalid[invalid].index[0]) + 2
        errors.append(f"{table}.csv row {row}: {column} must be an integer in [{low}, {high}]")


def _validate_raw_tables(tables: dict[str, pd.DataFrame]) -> None:
    errors: list[str] = []
    integer_domains = {
        "orders": {
            "order_id": (1, 2**31 - 1), "user_id": (1, 2**31 - 1),
            "order_number": (1, 2**15 - 1), "order_dow": (0, 6),
            "order_hour_of_day": (0, 23),
        },
        "order_products__prior": {
            "order_id": (1, 2**31 - 1), "product_id": (1, 2**31 - 1),
            "add_to_cart_order": (1, 2**31 - 1), "reordered": (0, 1),
        },
        "order_products__train": {
            "order_id": (1, 2**31 - 1), "product_id": (1, 2**31 - 1),
            "add_to_cart_order": (1, 2**31 - 1), "reordered": (0, 1),
        },
        "products": {
            "product_id": (1, 2**31 - 1), "aisle_id": (1, 2**15 - 1),
            "department_id": (1, 2**7 - 1),
        },
        "aisles": {"aisle_id": (1, 2**15 - 1)},
        "departments": {"department_id": (1, 2**7 - 1)},
    }
    for table, frame in tables.items():
        for column, (low, high) in integer_domains[table].items():
            _integer_column(frame, table, column, low, high, errors)

    orders = tables["orders"]
    gaps = orders["days_since_prior_order"].astype("string")
    blank_gap = gaps.eq("").fillna(False)
    numeric_gap = gaps.str.fullmatch(_NUMBER.pattern, na=False)
    gap_values = pd.to_numeric(gaps.where(numeric_gap), errors="coerce")
    # The bounded range excludes infinities; missing parse results fail closed.
    valid_gap = gap_values.between(0, 30).fillna(False)
    invalid_gap = ~blank_gap & (~numeric_gap | ~valid_gap)
    if invalid_gap.any():
        row = int(invalid_gap[invalid_gap].index[0]) + 2
        errors.append(f"orders.csv row {row}: days_since_prior_order must be blank or a finite number in [0, 30]")
    order_number_valid = orders["order_number"].astype("string").str.fullmatch(_INTEGER.pattern, na=False)
    order_number = pd.to_numeric(orders["order_number"].where(order_number_valid), errors="coerce")
    invalid_null_gap = blank_gap & order_number_valid & order_number.ne(1)
    if invalid_null_gap.any():
        row = int(invalid_null_gap[invalid_null_gap].index[0]) + 2
        errors.append(f"orders.csv row {row}: a blank days_since_prior_order is valid only for order_number 1")

    eval_set = orders["eval_set"].astype("string")
    bad_eval = ~eval_set.isin({"prior", "train", "test"})
    if bad_eval.any():
        row = int(bad_eval[bad_eval].index[0]) + 2
        errors.append(f"orders.csv row {row}: eval_set must be prior, train, or test")
    for table, column in (("products", "product_name"), ("aisles", "aisle"), ("departments", "department")):
        names = tables[table][column].astype("string")
        blank = names.str.strip().eq("")
        if blank.any():
            row = int(blank[blank].index[0]) + 2
            errors.append(f"{table}.csv row {row}: {column} must not be blank")

    key_columns = {
        "orders": (("order_id",), ("user_id", "order_number")),
        "order_products__prior": (("order_id", "product_id"), ("order_id", "add_to_cart_order")),
        "order_products__train": (("order_id", "product_id"), ("order_id", "add_to_cart_order")),
        "products": (("product_id",),),
        "aisles": (("aisle_id",),),
        "departments": (("department_id",),),
    }
    for table, keys in key_columns.items():
        frame = tables[table]
        for columns in keys:
            valid_key_rows = pd.Series(True, index=frame.index)
            for column in columns:
                valid_key_rows &= frame[column].astype("string").str.fullmatch(_INTEGER.pattern, na=False)
            duplicate = valid_key_rows & frame.duplicated(list(columns), keep=False)
            if duplicate.any():
                row = int(duplicate[duplicate].index[0]) + 2
                errors.append(f"{table}.csv row {row}: duplicate key ({', '.join(columns)})")

    # Referential checks use validated raw key strings; no coercion or joins have run.
    order_ids = set(orders.loc[orders.order_id.astype("string").str.fullmatch(_INTEGER.pattern, na=False), "order_id"])
    product_ids = set(tables["products"].loc[tables["products"].product_id.astype("string").str.fullmatch(_INTEGER.pattern, na=False), "product_id"])
    aisle_ids = set(tables["aisles"].loc[tables["aisles"].aisle_id.astype("string").str.fullmatch(_INTEGER.pattern, na=False), "aisle_id"])
    department_ids = set(tables["departments"].loc[tables["departments"].department_id.astype("string").str.fullmatch(_INTEGER.pattern, na=False), "department_id"])
    order_kind = orders.set_index("order_id")["eval_set"].to_dict()
    for table, expected_kind in (("order_products__prior", "prior"), ("order_products__train", "train")):
        frame = tables[table]
        valid_order = frame.order_id.astype("string").str.fullmatch(_INTEGER.pattern, na=False)
        valid_product = frame.product_id.astype("string").str.fullmatch(_INTEGER.pattern, na=False)
        missing_order = valid_order & ~frame.order_id.isin(order_ids)
        missing_product = valid_product & ~frame.product_id.isin(product_ids)
        wrong_kind = valid_order & frame.order_id.map(order_kind).notna() & frame.order_id.map(order_kind).ne(expected_kind)
        for mask, label in ((missing_order, "order_id does not exist in orders.csv"), (missing_product, "product_id does not exist in products.csv"), (wrong_kind, f"order_id does not belong to eval_set {expected_kind}")):
            if mask.any():
                row = int(mask[mask].index[0]) + 2
                errors.append(f"{table}.csv row {row}: {label}")
    products = tables["products"]
    for column, valid_ids, dimension in (("aisle_id", aisle_ids, "aisles.csv"), ("department_id", department_ids, "departments.csv")):
        valid = products[column].astype("string").str.fullmatch(_INTEGER.pattern, na=False)
        missing = valid & ~products[column].isin(valid_ids)
        if missing.any():
            row = int(missing[missing].index[0]) + 2
            errors.append(f"products.csv row {row}: {column} does not exist in {dimension}")

    if errors:
        raise RawInputValidationError(errors)

    # Positive, unique positions are insufficient: every basket starts at one
    # and contains exactly the contiguous sequence 1..N.
    for table in ("order_products__prior", "order_products__train"):
        frame = tables[table]
        positions = pd.to_numeric(frame["add_to_cart_order"], errors="raise")
        baskets = positions.groupby(frame["order_id"], sort=False).agg(["min", "max", "count"])
        if ((baskets["min"] != 1) | (baskets["max"] != baskets["count"])).any():
            errors.append(f"{table}.csv: add_to_cart_order must be contiguous 1..N in every order")
    if errors:
        raise RawInputValidationError(errors)


def _coerce_raw_tables(tables: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    for table, dtypes in RAW_DATA_TYPES.items():
        for column, dtype in dtypes.items():
            if dtype == "string":
                tables[table][column] = tables[table][column].astype("string")
            elif dtype == "category":
                tables[table][column] = tables[table][column].astype("category")
            elif dtype.startswith("float"):
                tables[table][column] = pd.to_numeric(tables[table][column].replace("", pd.NA)).astype(dtype)
            else:
                tables[table][column] = pd.to_numeric(tables[table][column], errors="raise").astype(dtype)
    return tables


def _validate_publication_paths(
    data_dir: Path, output_dir: Path, raw_dir: Path | None = None
) -> tuple[Path, Path]:
    """Reject aliases and replacement boundaries that can contain raw inputs."""
    selected_raw_dir = raw_dir if raw_dir is not None else data_dir / "raw"
    for candidate in (data_dir, output_dir.parent, selected_raw_dir):
        absolute = candidate.absolute()
        current = Path(absolute.anchor)
        for part in absolute.parts[1:]:
            current = current / part
            if current.is_symlink():
                raise ValueError("data and output paths must not traverse symlinks")
    if selected_raw_dir.is_symlink():
        raise ValueError("raw input directory must not be a symlink")
    raw_real = selected_raw_dir.resolve(strict=False)
    output_real = output_dir.resolve(strict=False)
    if output_dir.is_symlink():
        raise ValueError("output directory must not be a symlink")
    try:
        output_real.relative_to(raw_real)
        output_inside_raw = True
    except ValueError:
        output_inside_raw = False
    try:
        raw_real.relative_to(output_real)
        raw_inside_output = True
    except ValueError:
        raw_inside_output = False
    if output_real == raw_real or output_inside_raw or raw_inside_output:
        raise ValueError("output directory overlaps the raw input directory")

    # Do not follow symlinked raw files during input validation. Resolve every
    # expected source path as a second defense against a file alias escape.
    for table_name in RAW_COLUMNS:
        source = selected_raw_dir / f"{table_name}.csv"
        if source.is_symlink():
            raise ValueError(f"raw input {table_name}.csv must not be a symlink")
        try:
            source.resolve(strict=False).relative_to(raw_real)
        except ValueError as exc:
            raise ValueError(f"raw input {table_name}.csv escapes the raw input directory") from exc
    return raw_real, output_real


def _validate_existing_output(output_dir: Path) -> None:
    """Allow replacement only for a previously published, fully owned output."""
    if not output_dir.exists():
        return
    if output_dir.is_symlink() or not output_dir.is_dir():
        raise ValueError("output path must be a regular directory")
    entries = list(output_dir.iterdir())
    if not entries:
        return
    if any(entry.is_symlink() or not entry.is_file() for entry in entries):
        raise ValueError("existing output contains a symlink or non-file entry")
    if {entry.name for entry in entries} != _CLEAN_OUTPUT_FILES | {_CLEAN_OUTPUT_MANIFEST}:
        raise ValueError("existing output contains unrecognized or unowned files")
    try:
        manifest = json.loads((output_dir / _CLEAN_OUTPUT_MANIFEST).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("existing output ownership manifest is invalid") from exc
    if manifest != {"schema_version": 1, "files": sorted(_CLEAN_OUTPUT_FILES)}:
        raise ValueError("existing output ownership manifest is invalid")


def _remove_published_output(path: Path) -> None:
    """Remove only the known files of a validated, owned publication."""
    _validate_existing_output(path)
    if not path.exists():
        return
    for filename in (*sorted(_CLEAN_OUTPUT_FILES), _CLEAN_OUTPUT_MANIFEST):
        candidate = path / filename
        if candidate.exists():
            candidate.unlink()
    path.rmdir()


def load_raw(data_dir: Path, raw_dir: Path | None = None) -> dict[str, pd.DataFrame]:
    dataframes: dict[str, pd.DataFrame] = {}
    diagnostics: list[str] = []
    raw_dir = raw_dir if raw_dir is not None else data_dir / "raw"
    for table_name, columns in RAW_COLUMNS.items():
        csv_path = raw_dir / f"{table_name}.csv"
        if not csv_path.exists():
            diagnostics.append(f"{table_name}.csv: required source file is missing from {raw_dir}")
            continue
        header = _header(csv_path, table_name)
        if len(header) != len(set(header)):
            diagnostics.append(f"{table_name}.csv: duplicate column names are not allowed")
            continue
        expected = set(columns)
        actual = set(header)
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        if missing or extra:
            details = []
            if missing:
                details.append("missing columns " + ", ".join(missing))
            if extra:
                details.append("unexpected columns " + ", ".join(extra))
            diagnostics.append(f"{table_name}.csv: header mismatch ({'; '.join(details)})")
            continue
        try:
            dataframes[table_name] = pd.read_csv(csv_path, dtype="string", keep_default_na=False)
            if dataframes[table_name].empty:
                diagnostics.append(f"{table_name}.csv: source table must contain at least one data row")
        except (OSError, UnicodeError, pd.errors.ParserError, ValueError) as exc:
            diagnostics.append(f"{table_name}.csv: CSV rows could not be parsed ({type(exc).__name__})")
    if diagnostics:
        raise RawInputValidationError(diagnostics)
    _validate_raw_tables(dataframes)
    _coerce_raw_tables(dataframes)
    for table, frame in dataframes.items():
        print(f"[load] {table}.csv: {len(frame):,} rows")
    return dataframes


def _write_outputs_atomically(output_dir: Path, outputs: dict[str, pd.DataFrame]) -> None:
    """Stage Parquet files and replace only a verified Module 1 publication."""
    if set(outputs) != _CLEAN_OUTPUT_FILES:
        raise ValueError("clean output set does not match the reviewed publication contract")
    if output_dir.is_symlink():
        raise ValueError("output directory must not be a symlink")
    _validate_existing_output(output_dir)
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.stage-", dir=output_dir.parent))
    backup: Path | None = None
    try:
        for filename, frame in outputs.items():
            frame.to_parquet(stage / filename, index=False, compression=PARQUET_COMPRESSION)
        (stage / _CLEAN_OUTPUT_MANIFEST).write_text(
            json.dumps({"schema_version": 1, "files": sorted(_CLEAN_OUTPUT_FILES)}, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        if output_dir.exists():
            backup = output_dir.parent / f".{output_dir.name}.previous-{os.getpid()}-{uuid.uuid4().hex}"
            os.replace(output_dir, backup)
        try:
            os.replace(stage, output_dir)
        except Exception:
            if backup is not None and backup.exists() and not output_dir.exists():
                os.replace(backup, output_dir)
            raise
    finally:
        if stage.exists():
            shutil.rmtree(stage)
    if backup is not None:
        # Never recursively delete a replacement boundary. If an unexpected
        # entry appeared during publication, leave the private backup intact.
        _remove_published_output(backup)


def clean_orders(orders: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    stats: dict[str, int] = {}
    df = orders.copy()

    first_order_nan_count = int((df["days_since_prior_order"].isna() & (df["order_number"] == 1)).sum())
    stats["first_order_nan_gap"] = first_order_nan_count
    df["dspo_raw"] = df["days_since_prior_order"]
    df["days_since_prior_order"] = df["days_since_prior_order"].fillna(0.0)

    df = df.sort_values(["user_id", "order_number"], kind="mergesort").reset_index(drop=True)
    capped_mask = df["dspo_raw"].eq(30)
    stats["cap30_rows"] = int(capped_mask.sum())
    previous_dow = df.groupby("user_id", sort=False)["order_dow"].shift(1)
    df["prev_dow"] = previous_dow.astype("float64")

    df["cap_recovered"] = False
    df.loc[capped_mask, "days_since_prior_order"] = 30 + (
        (df.loc[capped_mask, "order_dow"] - df.loc[capped_mask, "prev_dow"] - 2) % 7
    )
    df.loc[capped_mask, "cap_recovered"] = True

    recovered_gaps = df.loc[capped_mask, "days_since_prior_order"]
    stats["cap_recovered_range_ok"] = int(bool(((recovered_gaps >= 30) & (recovered_gaps <= 36)).all()))
    stats["cap_recovered_valid_dow"] = int(recovered_gaps.notna().sum())

    valid_mask = df["prev_dow"].notna() & df["dspo_raw"].notna()
    dow_diff = (df.loc[valid_mask, "order_dow"] - df.loc[valid_mask, "prev_dow"]) % 7
    gap_diff = df.loc[valid_mask, "days_since_prior_order"] % 7
    stats["dow_mismatch_after_recovery"] = int((dow_diff != gap_diff).sum())

    if not stats["cap_recovered_range_ok"] or stats["dow_mismatch_after_recovery"]:
        raise RawInputValidationError(["orders.csv: reconstructed gaps must be in [30, 36] for capped rows and agree with weekday progression"])

    stats["eval_set_counts"] = {str(k): int(v) for k, v in df["eval_set"].value_counts().items()}
    df = df.drop(columns=["prev_dow"])
    df["days_since_prior_order"] = df["days_since_prior_order"].astype("float32")
    return df, stats


def clean_products(products: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    stats: dict[str, int] = {}
    df = products.copy()
    has_nbsp = df["product_name"].str.contains("\xa0", regex=False)
    stats["nbsp_rows"] = int(has_nbsp.sum())
    df["product_name"] = (
        df["product_name"]
        .str.replace("\xa0", " ", regex=False)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )
    return df, stats


def clean_order_products(order_products: pd.DataFrame) -> pd.DataFrame:
    df = order_products.copy()
    df["reordered"] = df["reordered"].astype("bool")
    df["add_to_cart_order"] = df["add_to_cart_order"].astype("int32")
    return df


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    default_data_dir = Path(__file__).resolve().parents[2] / "data"
    parser.add_argument("--data-dir", default=str(default_data_dir))
    parser.add_argument("--raw-dir", default=None, help="six-file input directory; defaults to <data-dir>/raw")
    parser.add_argument("--output-dir", "--out-dir", dest="output_dir", default=None)
    parser.add_argument("--validate", action="store_true", help="Print data quality audit statistics")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    raw_dir = Path(args.raw_dir) if args.raw_dir else None
    output_dir = Path(args.output_dir) if args.output_dir else data_dir / "clean"
    try:
        _validate_publication_paths(data_dir, output_dir, raw_dir)
    except (OSError, ValueError) as exc:
        print(f"unsafe cleaned-output path: {exc}", file=sys.stderr)
        return 2
    try:
        dataframes = load_raw(data_dir, raw_dir)
        orders, order_stats = clean_orders(dataframes["orders"])
    except RawInputValidationError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    products, product_stats = clean_products(dataframes["products"])
    prior = clean_order_products(dataframes["order_products__prior"])
    train = clean_order_products(dataframes["order_products__train"])
    aisles = dataframes.get("aisles")
    departments = dataframes.get("departments")

    row_counts = {"orders": len(orders), "prior": len(prior), "train": len(train)}
    try:
        _write_outputs_atomically(
            output_dir,
            {
                "orders.parquet": orders,
                "order_products__prior.parquet": prior,
                "order_products__train.parquet": train,
                "products.parquet": products,
                "aisles.parquet": aisles,
                "departments.parquet": departments,
            },
        )
    except (OSError, ImportError, ValueError) as exc:
        print(f"could not publish cleaned outputs: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    print("\n[clean] outputs written to", output_dir)
    for name, count in row_counts.items():
        print(f"  {name}: {count:,}")
    if args.validate:
        print("\n[validate]")
        print(f"  first-order NaN gaps -> 0            : {order_stats['first_order_nan_gap']:,}")
        print(f"  cap-30 rows reconstructed [30,36]    : {order_stats['cap30_rows']:,}")
        print(f"  reconstructed gap in [30,36]         : {order_stats['cap_recovered_range_ok']}")
        print(f"  DOW mismatches after reconstruction  : {order_stats['dow_mismatch_after_recovery']}")
        print(f"  \\xa0 product names cleaned           : {product_stats['nbsp_rows']}")
        print(f"  eval_set counts                      : {order_stats['eval_set_counts']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
