"""Deterministic, explicitly synthetic six-file source fixture for B3 offline plans."""

from __future__ import annotations

import csv
from pathlib import Path


TABLES = {
    "orders.csv": ("order_id", "user_id", "eval_set", "order_number", "order_dow", "order_hour_of_day", "days_since_prior_order"),
    "order_products__prior.csv": ("order_id", "product_id", "add_to_cart_order", "reordered"),
    "order_products__train.csv": ("order_id", "product_id", "add_to_cart_order", "reordered"),
    "products.csv": ("product_id", "product_name", "aisle_id", "department_id"),
    "aisles.csv": ("aisle_id", "aisle"),
    "departments.csv": ("department_id", "department"),
}
FIXTURE_USERS = 12


def write_fixture(directory: Path | str) -> dict:
    """Create 12 synthetic users and a small but relationally complete basket set."""
    root = Path(directory)
    root.mkdir(parents=True, exist_ok=True)
    contents: dict[str, list[list[object]]] = {name: [] for name in TABLES}
    orders = contents["orders.csv"]
    prior = contents["order_products__prior.csv"]
    train = contents["order_products__train.csv"]
    for user_id in range(1, FIXTURE_USERS + 1):
        for order_number in range(1, 5):
            order_id = user_id * 10 + order_number
            eval_set = "train" if order_number == 4 else "prior"
            day = ((order_number - 1) * 2) % 7
            gap = "" if order_number == 1 else "2"
            orders.append([order_id, user_id, eval_set, order_number, day, 7 + order_number, gap])
            if order_number == 1:
                products = (1, 2)
            elif order_number == 2:
                products = (1, 4 if user_id <= FIXTURE_USERS // 2 else 3)
            elif order_number == 3:
                products = (1, 2 if user_id <= FIXTURE_USERS // 2 else 4)
            else:
                products = (3, 4)
            target = train if eval_set == "train" else prior
            for position, product_id in enumerate(products, start=1):
                target.append([order_id, product_id, position, int(order_number > 1 and product_id in (1, 2, 3))])
    contents["products.csv"] = [
        [1, "Fixture oat milk", 1, 1], [2, "Fixture yogurt", 2, 1],
        [3, "Fixture bananas", 3, 2], [4, "Fixture berries", 4, 2],
    ]
    contents["aisles.csv"] = [[1, "fixture dairy"], [2, "fixture cultured dairy"], [3, "fixture fruit"], [4, "fixture berries"]]
    contents["departments.csv"] = [[1, "fixture dairy"], [2, "fixture produce"]]

    for name, rows in contents.items():
        path = root / name
        expected = _csv_bytes(TABLES[name], rows)
        if path.exists():
            if path.is_symlink() or not path.is_file():
                raise ValueError(f"fixture path {name} must be a regular non-symlink file")
            if path.read_bytes() != expected:
                raise FileExistsError(f"existing input file {name} differs from the deterministic fixture")
        else:
            path.write_bytes(expected)
    return {
        "scope": "synthetic_fixture",
        "provenance": "Deterministic six-file fixture generated for bounded Kubernetes planning and demonstration.",
        "users": FIXTURE_USERS,
    }


def _csv_bytes(header: tuple[str, ...], rows: list[list[object]]) -> bytes:
    import io

    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(header)
    writer.writerows(rows)
    return output.getvalue().encode("utf-8")
