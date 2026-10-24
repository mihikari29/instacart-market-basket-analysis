"""Fetch public Kaggle mirror version 1 and verify all six source counts."""

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import tempfile
import zipfile
import pyarrow.csv as csv
import requests

URL = "https://www.kaggle.com/api/v1/datasets/download/psparks/instacart-market-basket-analysis?datasetVersionNumber=1"
EXPECTED = {
    "orders.csv": 3421083,
    "order_products__prior.csv": 32434489,
    "order_products__train.csv": 1384617,
    "products.csv": 49688,
    "aisles.csv": 134,
    "departments.csv": 21,
}


def file_digest(path: Path) -> str:
    """Return the hex SHA-256 digest of one file."""
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def verify_existing(root: Path) -> dict:
    """Verify the source CSVs that are already present and write the receipt.

    The archive is not downloaded, so its digest is not available. The per-file
    row counts and digests are, and those are what the Module 4 full-data
    scope cross-checks against the staged tables.
    """
    missing = [name for name in EXPECTED if not (root / name).is_file()]
    if missing:
        raise ValueError(f"source CSVs are missing: {', '.join(sorted(missing))}")
    sources = {}
    for name, expected in EXPECTED.items():
        path = root / name
        actual = sum(batch.num_rows for batch in csv.open_csv(path))
        if actual != expected:
            raise ValueError(f"{name}: expected {expected}, observed {actual}")
        sources[name] = {"rows": actual, "sha256": file_digest(path)}
        print(name, actual, flush=True)
    return {
        "url": URL,
        "mirror": "psparks/instacart-market-basket-analysis",
        "version": 1,
        "archive_sha256": None,
        "archive_note": (
            "the archive was not downloaded; the receipt records the row count and "
            "digest of each source CSV that is present"
        ),
        "files": sources,
    }


def main():
    """Download the source and write the receipt, or verify an existing source."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--receipt-only",
        action="store_true",
        help="verify the source CSVs that are already present and write the receipt",
    )
    arguments = parser.parse_args()
    root = Path("data/raw")
    root.mkdir(parents=True, exist_ok=True)
    if arguments.receipt_only:
        receipt = verify_existing(root)
        (root / "source-receipt.json").write_text(json.dumps(receipt, indent=2), encoding="utf-8")
        print(f"wrote {root / 'source-receipt.json'}")
        return
    if any((root / name).exists() for name in EXPECTED):
        raise ValueError("Refusing to overwrite existing source CSVs")
    digest = hashlib.sha256()
    with tempfile.TemporaryDirectory(dir="data", prefix="download_") as temporary:
        archive = Path(temporary) / "instacart.zip"
        with requests.get(URL, stream=True, timeout=(30, 120)) as response:
            response.raise_for_status()
            with archive.open("wb") as output:
                for chunk in response.iter_content(8 * 1024 * 1024):
                    digest.update(chunk)
                    output.write(chunk)
        sources = {}
        with zipfile.ZipFile(archive) as zipped:
            for name, expected in EXPECTED.items():
                matches = [n for n in zipped.namelist() if Path(n).name == name]
                if len(matches) != 1:
                    raise ValueError(f"Expected exactly one {name}; archive: {zipped.namelist()}")
                path = root / name
                with zipped.open(matches[0]) as source, path.open("wb") as target:
                    shutil.copyfileobj(source, target)
                actual = sum(batch.num_rows for batch in csv.open_csv(path))
                if actual != expected:
                    raise ValueError(f"{name}: expected {expected}, observed {actual}")
                sources[name] = {"rows": actual, "sha256": file_digest(path)}
                print(name, actual, flush=True)
        receipt = {
            "url": URL,
            "mirror": "psparks/instacart-market-basket-analysis",
            "version": 1,
            "archive_sha256": digest.hexdigest(),
            "files": sources,
        }
        (root / "source-receipt.json").write_text(json.dumps(receipt, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
