"""Deterministic, relationally complete subset extractor for Instacart data.

Extracts a valid N-user subset preserving foreign-key referential integrity across:
orders -> order_products__prior / order_products__train -> products -> aisles / departments
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
import sys

from scripts.k8s.data_receipts import REQUIRED_HEADERS, inspect_input_directory


def extract_sample_dataset(
    source_dir: Path | str,
    output_dir: Path | str,
    target_users: int = 500,
) -> dict:
    source = Path(source_dir)
    target = Path(output_dir)
    target.mkdir(parents=True, exist_ok=True)

    # 1. Filter orders.csv for target_users
    selected_users: set[str] = set()
    prior_orders: set[str] = set()
    train_orders: set[str] = set()
    filtered_orders: list[list[str]] = []

    with (source / "orders.csv").open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.reader(stream)
        header = next(reader)
        user_col = header.index("user_id")
        order_col = header.index("order_id")
        eval_col = header.index("eval_set")

        for row in reader:
            uid = row[user_col]
            if uid not in selected_users:
                if len(selected_users) >= target_users:
                    continue
                selected_users.add(uid)
            filtered_orders.append(row)
            oid = row[order_col]
            eval_set = row[eval_col]
            if eval_set == "prior":
                prior_orders.add(oid)
            elif eval_set == "train":
                train_orders.add(oid)

    # 2. Filter order_products__prior.csv and order_products__train.csv
    referenced_products: set[str] = set()

    def filter_order_products(filename: str, valid_orders: set[str]) -> list[list[str]]:
        results = []
        with (source / filename).open("r", encoding="utf-8-sig", newline="") as stream:
            reader = csv.reader(stream)
            next(reader)
            for row in reader:
                if row[0] in valid_orders:
                    results.append(row)
                    referenced_products.add(row[1])
        return results

    filtered_prior = filter_order_products("order_products__prior.csv", prior_orders)
    filtered_train = filter_order_products("order_products__train.csv", train_orders)

    # 3. Filter products.csv
    filtered_products: list[list[str]] = []
    referenced_aisles: set[str] = set()
    referenced_depts: set[str] = set()

    with (source / "products.csv").open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.reader(stream)
        next(reader)
        for row in reader:
            if row[0] in referenced_products:
                filtered_products.append(row)
                referenced_aisles.add(row[2])
                referenced_depts.add(row[3])

    # 4. Filter aisles.csv and departments.csv
    def filter_by_id(filename: str, valid_ids: set[str]) -> list[list[str]]:
        results = []
        with (source / filename).open("r", encoding="utf-8-sig", newline="") as stream:
            reader = csv.reader(stream)
            next(reader)
            for row in reader:
                if row[0] in valid_ids:
                    results.append(row)
        return results

    filtered_aisles = filter_by_id("aisles.csv", referenced_aisles)
    filtered_depts = filter_by_id("departments.csv", referenced_depts)

    # 5. Write out the 6 files with exact required headers
    tables = {
        "orders.csv": (REQUIRED_HEADERS["orders.csv"], filtered_orders),
        "order_products__prior.csv": (REQUIRED_HEADERS["order_products__prior.csv"], filtered_prior),
        "order_products__train.csv": (REQUIRED_HEADERS["order_products__train.csv"], filtered_train),
        "products.csv": (REQUIRED_HEADERS["products.csv"], filtered_products),
        "aisles.csv": (REQUIRED_HEADERS["aisles.csv"], filtered_aisles),
        "departments.csv": (REQUIRED_HEADERS["departments.csv"], filtered_depts),
    }

    for name, (col_headers, rows) in tables.items():
        with (target / name).open("w", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(col_headers)
            writer.writerows(rows)

    receipt = inspect_input_directory(target)
    return {
        "users": len(selected_users),
        "orders": len(filtered_orders),
        "prior_items": len(filtered_prior),
        "train_items": len(filtered_train),
        "products": len(filtered_products),
        "total_bytes": receipt["total_bytes"],
        "manifest_sha256": receipt["manifest_sha256"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract a clean relationally complete Instacart sample")
    parser.add_argument("--source-dir", default="data/raw", help="Directory containing raw Instacart CSVs")
    parser.add_argument("--output-dir", default="data/sample_medium", help="Output directory for sample CSVs")
    parser.add_argument("--users", type=int, default=500, help="Number of users to sample")
    args = parser.parse_args()

    summary = extract_sample_dataset(args.source_dir, args.output_dir, args.users)
    print("Sample generation complete:")
    for k, v in summary.items():
        print(f"  {k}: {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
