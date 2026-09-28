"""Module 1 - Step 3: HDFS Staging Layout.

Deploys raw CSVs, curated Parquet tables, dimensions, partitioned interactions,
and schema placeholders to HDFS via WebHDFS.

Usage:
  python -m src.module1.stage_hdfs
  python -m src.module1.stage_hdfs --feed data/synthesized/scatter_1w
"""

from __future__ import annotations

import argparse
import json
import posixpath
import re
import shutil
import tempfile
import uuid
from pathlib import Path

import pyarrow.parquet as pq
import requests
from hdfs import InsecureClient

from .partition_events import partition_events, file_sha256

DEFAULT_WEBHDFS_URL = "http://localhost:9870"
DEFAULT_HDFS_USER = "root"
HDFS_ROOT = "/instacart"

REPO_DATA_DIR = Path(__file__).resolve().parents[2] / "data"
RAW_DATA_DIR = REPO_DATA_DIR / "raw"
CLEAN_DATA_DIR = REPO_DATA_DIR / "clean"

RAW_TABLES = [
    ("orders", "orders.csv"),
    ("order_products_prior", "order_products__prior.csv"),
    ("order_products_train", "order_products__train.csv"),
    ("products", "products.csv"),
    ("aisles", "aisles.csv"),
    ("departments", "departments.csv"),
]

CURATED_TABLES = [
    ("orders", "orders.parquet"),
    ("order_products_prior", "order_products__prior.parquet"),
    ("order_products_train", "order_products__train.parquet"),
]

DIMENSION_TABLES = [
    ("products", "products.parquet"),
    ("aisles", "aisles.parquet"),
    ("departments", "departments.parquet"),
]


def ensure_dirs(client: InsecureClient, path: str) -> None:
    client.makedirs(path)


def webhdfs_upload(
    hdfs_path: str,
    local_path: Path,
    webhdfs_url: str = DEFAULT_WEBHDFS_URL,
    user: str = DEFAULT_HDFS_USER,
) -> None:
    """Execute two-hop WebHDFS PUT with Docker container redirect rewrite."""
    url = f"{webhdfs_url}/webhdfs/v1{hdfs_path}?op=CREATE&overwrite=true&user.name={user}"
    initial_response = requests.put(url, allow_redirects=False, timeout=30)
    if initial_response.status_code not in (301, 302, 307):
        raise RuntimeError(f"CREATE {hdfs_path}: HTTP {initial_response.status_code} {initial_response.text[:200]}")

    redirect_location = initial_response.headers["Location"]
    redirect_location = re.sub(r"//(datanode|namenode)(:[0-9]+)?", r"//localhost\2", redirect_location)
    with open(local_path, "rb") as stream:
        upload_response = requests.put(redirect_location, data=stream, timeout=3600)
    if upload_response.status_code >= 300:
        raise RuntimeError(f"PUT {hdfs_path}: HTTP {upload_response.status_code} {upload_response.text[:200]}")


def upload_file(client: InsecureClient, hdfs_path: str, local_path: Path, webhdfs_url: str, user: str) -> None:
    ensure_dirs(client, posixpath.dirname(hdfs_path))
    webhdfs_upload(hdfs_path, local_path, webhdfs_url, user)
    print(f"  [up  ] {local_path.name} -> {hdfs_path}", flush=True)


def stage_raw(client: InsecureClient, webhdfs_url: str, user: str) -> None:
    print("[raw  ] uploading 6 original CSVs", flush=True)
    for table, filename in RAW_TABLES:
        upload_file(client, f"{HDFS_ROOT}/raw/{table}/{filename}", RAW_DATA_DIR / filename, webhdfs_url, user)


def stage_curated(client: InsecureClient, webhdfs_url: str, user: str) -> None:
    print("[curated] uploading cleaned parquet tables", flush=True)
    for table, filename in CURATED_TABLES:
        upload_file(client, f"{HDFS_ROOT}/curated/{table}/{filename}", CLEAN_DATA_DIR / filename, webhdfs_url, user)
    for table, filename in DIMENSION_TABLES:
        upload_file(client, f"{HDFS_ROOT}/curated/dimensions/{table}/{filename}", CLEAN_DATA_DIR / filename, webhdfs_url, user)

    sources = {}
    for table, filename in CURATED_TABLES + DIMENSION_TABLES:
        file_path = CLEAN_DATA_DIR / filename
        sources[table] = {
            "rows": pq.ParquetFile(file_path).metadata.num_rows,
            "sha256": file_sha256(file_path),
        }
    with tempfile.TemporaryDirectory(prefix="hdfs_sources_") as temporary_dir:
        manifest_path = Path(temporary_dir) / "_sources.json"
        manifest_path.write_text(json.dumps(sources, indent=2), encoding="utf-8")
        upload_file(client, f"{HDFS_ROOT}/curated/_sources.json", manifest_path, webhdfs_url, user)


def stage_interactions(client: InsecureClient, feed_dir: Path, webhdfs_url: str, user: str) -> None:
    target_path = f"{HDFS_ROOT}/curated/interactions"
    staging_path = f"{target_path}.__staging_{uuid.uuid4().hex}"
    temp_dir = Path(tempfile.mkdtemp(prefix="hdfs_interactions_"))
    try:
        receipt = partition_events(feed_dir / "events.parquet", temp_dir)
        print(f"[interactions] validated {receipt['local_rows']:,} rows in {receipt['files']} files")
        for file_path in sorted(temp_dir.rglob("*.parquet")) + [temp_dir / "_handoff.json"]:
            relative_posix = file_path.relative_to(temp_dir).as_posix()
            upload_file(client, f"{staging_path}/{relative_posix}", file_path, webhdfs_url, user)

        client.delete(target_path, recursive=True)
        client.rename(staging_path, target_path)
        print(f"[done] {target_path}: source/local rows = {receipt['source_rows']:,}")
    finally:
        shutil.rmtree(temp_dir)


def stage_places(client: InsecureClient) -> None:
    for path in (
        f"{HDFS_ROOT}/features/user_features",
        f"{HDFS_ROOT}/features/als_interactions",
        f"{HDFS_ROOT}/models/als",
    ):
        ensure_dirs(client, path)
    print("[place] features/{user_features,als_interactions}, models/als created", flush=True)


def print_tree(client: InsecureClient, path: str, depth: int = 0) -> None:
    status = client.status(path, strict=False)
    if not status or status["type"] == "FILE":
        return
    for name in client.list(path):
        full_path = f"{path}/{name}"
        print("  " * depth + f"- {name}", flush=True)
        print_tree(client, full_path, depth + 1)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    default_feed_dir = REPO_DATA_DIR / "synthesized" / "scatter_3m"
    parser.add_argument("--feed", default=str(default_feed_dir))
    parser.add_argument("--webhdfs", default=DEFAULT_WEBHDFS_URL)
    parser.add_argument("--user", default=DEFAULT_HDFS_USER)
    parser.add_argument("--interactions-only", action="store_true")
    parser.add_argument("--tree-only", action="store_true")
    args = parser.parse_args()

    client = InsecureClient(args.webhdfs, user=args.user)
    print(f"[hdfs ] {args.webhdfs} as {args.user}", flush=True)

    if args.tree_only:
        print_tree(client, HDFS_ROOT)
        return 0
    if not args.interactions_only:
        stage_raw(client, args.webhdfs, args.user)
        stage_curated(client, args.webhdfs, args.user)
        stage_places(client)
    stage_interactions(client, Path(args.feed), args.webhdfs, args.user)

    print("[done ] layout:", flush=True)
    print_tree(client, HDFS_ROOT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
