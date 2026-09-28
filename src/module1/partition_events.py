"""Bounded-memory, single-writer interaction partitioning and staging receipts."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

PARTITION_COLUMNS = ["synthetic_year", "synthetic_month", "synthetic_day"]


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def partition_events(source: Path, destination: Path) -> dict:
    """Partition events by (year, month, day) in a single streaming writer session."""
    parquet_file = pq.ParquetFile(source)
    if parquet_file.metadata.num_rows == 0:
        raise ValueError("Empty interaction feed")
    if set(PARTITION_COLUMNS) & set(parquet_file.schema_arrow.names):
        raise ValueError("Source already contains partition columns")

    schema = parquet_file.schema_arrow
    for col_name in PARTITION_COLUMNS:
        schema = schema.append(pa.field(col_name, pa.int32()))

    def batches():
        for batch in parquet_file.iter_batches(batch_size=65536):
            table = pa.Table.from_batches([batch])
            dates = pc.strptime(table["synthetic_date"], format="%Y-%m-%d", unit="s")
            if dates.null_count:
                raise ValueError("Null synthetic_date found in input")

            synthetic_dates = table["synthetic_date"]
            years, months, days = pc.year(dates), pc.month(dates), pc.day(dates)
            is_valid_date = pc.and_kleene(
                pc.and_kleene(
                    pc.equal(years, pc.cast(pc.utf8_slice_codeunits(synthetic_dates, 0, 4), pa.int32())),
                    pc.equal(months, pc.cast(pc.utf8_slice_codeunits(synthetic_dates, 5, 7), pa.int32())),
                ),
                pc.and_kleene(
                    pc.equal(days, pc.cast(pc.utf8_slice_codeunits(synthetic_dates, 8, 10), pa.int32())),
                    pc.equal(pc.utf8_length(synthetic_dates), pa.scalar(10)),
                ),
            )
            if not pc.all(is_valid_date).as_py():
                raise ValueError("synthetic_date must be a valid ISO calendar date")

            for name, values in zip(PARTITION_COLUMNS, (years, months, days)):
                table = table.append_column(name, pc.cast(values, pa.int32()))
            yield from table.to_batches()

    ds.write_dataset(
        batches(),
        str(destination),
        schema=schema,
        format="parquet",
        partitioning=PARTITION_COLUMNS,
        partitioning_flavor="hive",
        existing_data_behavior="error",
        max_open_files=512,
        max_partitions=1024,
        min_rows_per_group=1024,
        max_rows_per_group=65536,
        max_rows_per_file=1048576,
        use_threads=False,
    )

    partition_files = sorted(destination.rglob("*.parquet"))
    local_rows = sum(pq.ParquetFile(p).metadata.num_rows for p in partition_files)
    if local_rows != parquet_file.metadata.num_rows:
        raise ValueError(f"Row count mismatch: source={parquet_file.metadata.num_rows}, local={local_rows}")

    receipt = {
        "version": 1,
        "source_rows": parquet_file.metadata.num_rows,
        "local_rows": local_rows,
        "source_sha256": file_sha256(source),
        "source_columns": parquet_file.schema_arrow.names,
        "files": len(partition_files),
        "partitions": len({p.parent for p in partition_files}),
    }
    manifest = source.parent / "manifest.json"
    if manifest.exists():
        feed = json.loads(manifest.read_text(encoding="utf-8"))
        if feed["events"] != receipt["source_rows"]:
            raise ValueError("Feed manifest event count differs from source Parquet")
        receipt["feed_manifest"] = feed
    (destination / "_handoff.json").write_text(json.dumps(receipt, indent=2), encoding="utf-8")
    return receipt
