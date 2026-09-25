"""Bounded-memory, single-writer interaction partitioning and staging receipts."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

PARTITIONS = ["synthetic_year", "synthetic_month", "synthetic_day"]


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def partition_events(source: Path, destination: Path) -> dict:
    """Destination must be empty. One writer owns names across ALL input batches.

    At most 512 partition writers buffer 1024-row minimum groups. Larger feeds
    may evict writers (unique names remain safe); standard feeds span 456 days.
    """
    pf = pq.ParquetFile(source)
    if pf.metadata.num_rows == 0:
        raise ValueError("Empty interaction feed")
    if set(PARTITIONS) & set(pf.schema_arrow.names):
        raise ValueError("Source already contains partition columns")
    schema = pf.schema_arrow
    for name in PARTITIONS:
        schema = schema.append(pa.field(name, pa.int32()))

    def batches():
        for batch in pf.iter_batches(batch_size=65536):
            table = pa.Table.from_batches([batch])
            dates = pc.strptime(table["synthetic_date"], format="%Y-%m-%d", unit="s")
            if dates.null_count:
                raise ValueError("Null synthetic_date")
            if not pc.all(pc.equal(pc.strftime(dates, format="%Y-%m-%d"), table["synthetic_date"])).as_py():
                raise ValueError("synthetic_date must be a valid ISO calendar date")
            for name, values in zip(PARTITIONS, (pc.year(dates), pc.month(dates), pc.day(dates))):
                table = table.append_column(name, pc.cast(values, pa.int32()))
            yield from table.to_batches()

    ds.write_dataset(
        batches(),
        str(destination),
        schema=schema,
        format="parquet",
        partitioning=PARTITIONS,
        partitioning_flavor="hive",
        existing_data_behavior="error",
        max_open_files=512,
        max_partitions=1024,
        min_rows_per_group=1024,
        max_rows_per_group=65536,
        max_rows_per_file=1048576,
        use_threads=False,
    )
    files = sorted(destination.rglob("*.parquet"))
    local_rows = sum(pq.ParquetFile(p).metadata.num_rows for p in files)
    if local_rows != pf.metadata.num_rows:
        raise ValueError(f"Lost rows: source={pf.metadata.num_rows}, local={local_rows}")
    receipt = {
        "version": 1,
        "source_rows": pf.metadata.num_rows,
        "local_rows": local_rows,
        "source_sha256": file_sha256(source),
        "source_columns": pf.schema_arrow.names,
        "files": len(files),
        "partitions": len({p.parent for p in files}),
    }
    manifest = source.parent / "manifest.json"
    if manifest.exists():
        feed = json.loads(manifest.read_text(encoding="utf-8"))
        if feed["events"] != receipt["source_rows"]:
            raise ValueError("Feed manifest event count differs from source Parquet")
        receipt["feed_manifest"] = feed
    (destination / "_handoff.json").write_text(json.dumps(receipt, indent=2), encoding="utf-8")
    return receipt
