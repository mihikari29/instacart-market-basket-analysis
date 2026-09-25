"""Module 1 - HDFS staging: materialise Proposal section 10 layout.

Writes the /instacart tree via WebHDFS (bde2020 hadoop-namenode on :9870):

  /instacart/raw/{orders,order_products_prior,order_products_train,products,aisles,departments}
  /instacart/curated/{orders,order_products_prior,order_products_train,dimensions/{products,aisles,departments}}
  /instacart/curated/interactions/synthetic_year=YYYY/synthetic_month=MM/synthetic_day=DD
  /instacart/features/{user_features,als_interactions}
  /instacart/models/als

Usage:
  python src/stage_hdfs.py                                     # scatter_3m interactions
  python src/stage_hdfs.py --feed data/synthesized/scatter_1w  # other feed
"""

from __future__ import annotations

import argparse
import json
import uuid
import posixpath
import re
import shutil
import tempfile
from pathlib import Path

import pyarrow.parquet as pq
import requests
from hdfs import InsecureClient

try:
    from .partition_events import partition_events, file_sha256
except ImportError:
    from partition_events import partition_events, file_sha256

HDFS_START = "http://localhost:9870"
HDFS_USER = "root"
BASE = "/instacart"

LOCAL = Path(__file__).resolve().parent.parent / "data"
RAW = LOCAL / "raw"
CLEAN = LOCAL / "clean"

RAW_TABLES = [
    ("orders", "orders.csv"),
    ("order_products_prior", "order_products__prior.csv"),
    ("order_products_train", "order_products__train.csv"),
    ("products", "products.csv"),
    ("aisles", "aisles.csv"),
    ("departments", "departments.csv"),
]
CURATED = [
    ("orders", "orders.parquet"),
    ("order_products_prior", "order_products__prior.parquet"),
    ("order_products_train", "order_products__train.parquet"),
]
DIMS = [
    ("products", "products.parquet"),
    ("aisles", "aisles.parquet"),
    ("departments", "departments.parquet"),
]


def ensure_dirs(client: InsecureClient, path: str) -> None:
    client.makedirs(path)


def webhdfs_upload(hpath: str, local: Path, webhdfs_url: str = HDFS_START, user: str = HDFS_USER) -> None:
    """Two-hop WebHDFS PUT. The namenode 307-redirects to 'datanode:9864',
    which only resolves inside Docker, so we rewrite its host to localhost."""
    url = f"{webhdfs_url}/webhdfs/v1{hpath}?op=CREATE&overwrite=true&user.name={user}"
    r = requests.put(url, allow_redirects=False, timeout=30)
    if r.status_code not in (301, 302, 307):
        raise RuntimeError(f"CREATE {hpath}: HTTP {r.status_code} {r.text[:200]}")
    loc = r.headers["Location"]
    loc = re.sub(r"//(datanode|namenode)(:[0-9]+)?", r"//localhost\2", loc)
    with open(local, "rb") as f:
        rr = requests.put(loc, data=f, timeout=3600)
    if rr.status_code >= 300:
        raise RuntimeError(f"PUT {hpath}: HTTP {rr.status_code} {rr.text[:200]}")


def upload_file(client: InsecureClient, hpath: str, local: Path, webhdfs_url: str, user: str) -> None:
    ensure_dirs(client, posixpath.dirname(hpath))
    webhdfs_upload(hpath, local, webhdfs_url, user)
    print(f"  [up  ] {local.name} -> {hpath}", flush=True)


def stage_raw(client: InsecureClient, webhdfs_url: str, user: str) -> None:
    print("[raw  ] uploading 6 original CSVs", flush=True)
    for table, fname in RAW_TABLES:
        upload_file(client, f"{BASE}/raw/{table}/{fname}", RAW / fname, webhdfs_url, user)


def stage_curated(client: InsecureClient, webhdfs_url: str, user: str) -> None:
    print("[curated] uploading cleaned parquet tables", flush=True)
    for table, fname in CURATED:
        upload_file(client, f"{BASE}/curated/{table}/{fname}", CLEAN / fname, webhdfs_url, user)
    for table, fname in DIMS:
        upload_file(client, f"{BASE}/curated/dimensions/{table}/{fname}", CLEAN / fname, webhdfs_url, user)

    sources = {}
    for table, fname in CURATED + DIMS:
        path = CLEAN / fname
        sources[table] = {"rows": pq.ParquetFile(path).metadata.num_rows, "sha256": file_sha256(path)}
    with tempfile.TemporaryDirectory(prefix="hdfs_sources_") as temporary:
        manifest = Path(temporary) / "_sources.json"
        manifest.write_text(json.dumps(sources, indent=2), encoding="utf-8")
        upload_file(client, f"{BASE}/curated/_sources.json", manifest, webhdfs_url, user)


def stage_interactions(client: InsecureClient, feed: Path, webhdfs_url: str, user: str) -> None:
    target = f"{BASE}/curated/interactions"
    pending = f"{target}.__staging_{uuid.uuid4().hex}"
    tmp = Path(tempfile.mkdtemp(prefix="hdfs_interactions_"))
    try:
        receipt = partition_events(feed / "events.parquet", tmp)
        print(f"[interactions] validated {receipt['local_rows']:,} rows in {receipt['files']} files")
        for path in sorted(tmp.rglob("*.parquet")) + [tmp / "_handoff.json"]:
            upload_file(client, f"{pending}/{path.relative_to(tmp).as_posix()}", path, webhdfs_url, user)
        # Upload completes before replacing only this subtree. Do not run concurrent stagers.
        # A failed rename leaves the pending copy available for recovery.
        client.delete(target, recursive=True)
        client.rename(pending, target)
        print(f"[done] {target}: source/local rows = {receipt['source_rows']}")
    finally:
        shutil.rmtree(tmp)


def stage_places(client: InsecureClient) -> None:
    for path in (f"{BASE}/features/user_features", f"{BASE}/features/als_interactions", f"{BASE}/models/als"):
        ensure_dirs(client, path)
    print("[place] features/{user_features,als_interactions}, models/als created", flush=True)


def tree(client: InsecureClient, path: str, depth: int = 0) -> None:
    status = client.status(path, strict=False)
    if not status or status["type"] == "FILE":
        return
    for name in client.list(path):
        full = f"{path}/{name}"
        print("  " * depth + f"- {name}", flush=True)
        tree(client, full, depth + 1)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--feed", default=str(LOCAL / "synthesized" / "scatter_3m"))
    ap.add_argument("--webhdfs", default=HDFS_START)
    ap.add_argument("--user", default=HDFS_USER)
    ap.add_argument("--interactions-only", action="store_true")
    ap.add_argument("--tree-only", action="store_true")
    args = ap.parse_args()

    client = InsecureClient(args.webhdfs, user=args.user)
    print(f"[hdfs ] {args.webhdfs} as {args.user}", flush=True)

    if args.tree_only:
        tree(client, BASE)
        return 0
    if not args.interactions_only:
        stage_raw(client, args.webhdfs, args.user)
        stage_curated(client, args.webhdfs, args.user)
        stage_places(client)
    stage_interactions(client, Path(args.feed), args.webhdfs, args.user)

    print("[done ] layout:", flush=True)
    tree(client, BASE)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
