"""Bounded-memory chronological reads for synthesized Parquet event feeds.

Unsorted feeds are transformed into fixed-size sorted runs on local disk, then
merged with a bounded fan-in. Memory is bounded by ``chunk_rows`` while forming
runs and by ``merge_fan_in * reader_batch_rows`` while merging.
"""

from __future__ import annotations

from collections.abc import Iterator
import heapq
import json
from pathlib import Path
import shutil
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq


SCHEMA_FIELDS = [
    "event_id",
    "order_id",
    "user_id",
    "product_id",
    "add_to_cart_order",
    "reordered",
    "aisle_id",
    "department_id",
    "order_dow",
    "order_hour_of_day",
    "event_time_epoch_ms",
    "event_time_iso",
]

SORT_FIELDS = [
    ("event_time_epoch_ms", "ascending"),
    ("user_id", "ascending"),
    ("order_id", "ascending"),
    ("add_to_cart_order", "ascending"),
    ("event_id", "ascending"),
]


def event_sort_key(row: dict) -> tuple:
    """Chronological key with stable business-key tie breakers."""
    return (
        int(row["event_time_epoch_ms"]),
        int(row["user_id"]),
        int(row["order_id"]),
        int(row["add_to_cart_order"]),
        str(row["event_id"]),
    )


def manifest_declares_sorted(event_path: Path) -> bool:
    """Return whether the adjacent feed manifest promises our exact ordering."""
    manifest_path = event_path.parent / "manifest.json"
    if not manifest_path.exists():
        return False
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    return manifest.get("event_sort_order") == [name for name, _ in SORT_FIELDS]


def _iter_parquet_rows(
    path: Path,
    *,
    batch_rows: int,
    limit: int | None = None,
) -> Iterator[dict]:
    emitted = 0
    parquet = pq.ParquetFile(path)
    for batch in parquet.iter_batches(batch_size=batch_rows, columns=SCHEMA_FIELDS):
        for row in batch.to_pylist():
            if limit is not None and emitted >= limit:
                return
            yield row
            emitted += 1


def _write_rows(path: Path, rows: Iterator[dict], schema: pa.Schema, batch_rows: int) -> int:
    writer = pq.ParquetWriter(path, schema, compression="snappy")
    buffer: list[dict] = []
    count = 0
    try:
        for row in rows:
            buffer.append(row)
            if len(buffer) >= batch_rows:
                writer.write_table(pa.Table.from_pylist(buffer, schema=schema))
                count += len(buffer)
                buffer.clear()
        if buffer:
            writer.write_table(pa.Table.from_pylist(buffer, schema=schema))
            count += len(buffer)
    finally:
        writer.close()
    return count


def _merged_rows(paths: list[Path], reader_batch_rows: int) -> Iterator[dict]:
    iterators = [_iter_parquet_rows(path, batch_rows=reader_batch_rows) for path in paths]
    yield from heapq.merge(*iterators, key=event_sort_key)


def _create_sorted_runs(
    event_path: Path,
    temp_dir: Path,
    chunk_rows: int,
    stats: dict,
) -> tuple[list[Path], pa.Schema]:
    parquet = pq.ParquetFile(event_path)
    missing = sorted(set(SCHEMA_FIELDS) - set(parquet.schema_arrow.names))
    if missing:
        raise ValueError(f"Event feed is missing required columns: {missing}")
    schema = pa.schema([parquet.schema_arrow.field(name) for name in SCHEMA_FIELDS])
    runs = []
    for index, batch in enumerate(
        parquet.iter_batches(batch_size=chunk_rows, columns=SCHEMA_FIELDS)
    ):
        table = pa.Table.from_batches([batch]).sort_by(SORT_FIELDS)
        run_path = temp_dir / f"run-0-{index:06d}.parquet"
        pq.write_table(table, run_path, compression="snappy")
        runs.append(run_path)
        stats["source_rows"] += table.num_rows
        stats["max_chunk_rows"] = max(stats["max_chunk_rows"], table.num_rows)
    return runs, schema


def iter_ordered_events(
    event_path: Path,
    *,
    limit: int | None = None,
    input_sorted: bool = False,
    chunk_rows: int = 65_536,
    merge_fan_in: int = 32,
    reader_batch_rows: int = 1_024,
    stats: dict | None = None,
) -> Iterator[dict]:
    """Yield events in deterministic chronological order with bounded memory.

    When ``input_sorted`` is true, rows stream directly from Parquet and
    ``limit`` stops source reads immediately. Otherwise all input must be
    inspected to establish global order; fixed-size sorted runs are spilled to
    disk and hierarchically merged.
    """
    if limit is not None and limit < 1:
        return
    if chunk_rows < 1 or reader_batch_rows < 1 or merge_fan_in < 2:
        raise ValueError("chunk_rows/reader_batch_rows must be positive and merge_fan_in >= 2")

    metrics = stats if stats is not None else {}
    metrics.update(
        {
            "strategy": "direct-sorted" if input_sorted else "external-merge-sort",
            "chunk_rows": chunk_rows,
            "merge_fan_in": merge_fan_in,
            "reader_batch_rows": reader_batch_rows,
            "source_rows": 0,
            "emitted_rows": 0,
            "initial_runs": 0,
            "merge_passes": 0,
            "max_chunk_rows": 0,
        }
    )

    if input_sorted:
        for row in _iter_parquet_rows(event_path, batch_rows=reader_batch_rows, limit=limit):
            metrics["source_rows"] += 1
            metrics["emitted_rows"] += 1
            yield row
        return

    temp_dir = Path(tempfile.mkdtemp(prefix="instacart_event_sort_"))
    try:
        runs, schema = _create_sorted_runs(event_path, temp_dir, chunk_rows, metrics)
        metrics["initial_runs"] = len(runs)
        if not runs:
            return

        pass_number = 0
        while len(runs) > merge_fan_in:
            pass_number += 1
            next_runs = []
            for group_index, start in enumerate(range(0, len(runs), merge_fan_in)):
                group = runs[start : start + merge_fan_in]
                merged_path = temp_dir / f"run-{pass_number}-{group_index:06d}.parquet"
                _write_rows(
                    merged_path,
                    _merged_rows(group, reader_batch_rows),
                    schema,
                    chunk_rows,
                )
                next_runs.append(merged_path)
                for path in group:
                    path.unlink()
            runs = next_runs
            metrics["merge_passes"] = pass_number

        emitted = 0
        for row in _merged_rows(runs, reader_batch_rows):
            if limit is not None and emitted >= limit:
                break
            emitted += 1
            metrics["emitted_rows"] = emitted
            yield row
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
