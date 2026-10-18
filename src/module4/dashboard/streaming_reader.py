"""Read one bounded metrics document through MongoDB's built-in _id index."""

from src.module3.metrics import COLLECTION, FIELDS, canonicalize_snapshot_for_json
from .readers import DatabaseReadFailure, InvalidSnapshot


def read_streaming_metrics(database, source_namespace, *, max_time_ms):
    try:
        document = database[COLLECTION].find_one(
            {"_id": source_namespace}, {"_id": 0, **{key: 1 for key in FIELDS}}, max_time_ms=max_time_ms
        )
    except Exception as exc:
        raise DatabaseReadFailure("streaming metrics query failed") from exc
    if document is not None:
        try:
            document = canonicalize_snapshot_for_json(document)
            if document["source_namespace"] != source_namespace:
                raise ValueError("metrics namespace mismatch")
        except (TypeError, ValueError, KeyError, OverflowError) as exc:
            raise InvalidSnapshot("streaming metrics failed validation") from exc
    return {"available": document is not None, "source_namespace": source_namespace, "metrics": document}
