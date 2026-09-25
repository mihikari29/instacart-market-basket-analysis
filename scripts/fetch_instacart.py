"""Fetch public Kaggle mirror version 1 and verify all six source counts."""

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


def main():
    root = Path("data/raw")
    root.mkdir(parents=True, exist_ok=True)
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
                file_digest = hashlib.sha256()
                with path.open("rb") as stream:
                    for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
                        file_digest.update(block)
                sources[name] = {"rows": actual, "sha256": file_digest.hexdigest()}
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
