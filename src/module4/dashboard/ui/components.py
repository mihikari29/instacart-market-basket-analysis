"""Shared, source-aware presentation helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .api_client import ApiClientError


ERROR_GUIDANCE = {
    "recommendation_snapshot_not_found": "No published recommendation snapshot was found for this user ID.",
    "snapshot_not_ready": "The published recommendation snapshot is not ready to read.",
    "snapshot_changed": "The snapshot changed during the read. Submit again to retry once manually.",
    "invalid_snapshot": "The stored recommendation snapshot failed integrity validation.",
    "invalid_source_document": "The stored source data failed validation.",
    "trend_index_unavailable": "The required finalized trend query index is unavailable.",
    "batch_index_unavailable": "A required batch product ranking index is unavailable.",
    "service_unavailable": "The API could not read this source. Check the backend service and its configured data source.",
    "evidence_unavailable": "This registered historical evidence file is unavailable in the API runtime.",
    "invalid_evidence": "This registered evidence file failed validation.",
    "invalid_request": "The API rejected this request. Check the submitted values.",
    "invalid_filter": "The API rejected one or more filter values.",
    "invalid_namespace": "The selected trend namespace is not configured on the API.",
}


def display_time(value: Any) -> str:
    if not isinstance(value, str) or not value:
        return "Not recorded"
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return value
        return parsed.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except (ValueError, OverflowError):
        return value


def render_error(st: Any, error: ApiClientError, *, label: str | None = None) -> None:
    guidance = ERROR_GUIDANCE.get(error.code, error.message)
    heading = f"{label}: " if label else ""
    details = [f"{error.code}"]
    if error.status_code is not None:
        details.append(f"HTTP {error.status_code}")
    if error.request_id:
        details.append(f"request ID {error.request_id}")
    st.error(f"{heading}{guidance} ({'; '.join(details)}).")


def render_source_status(st: Any, title: str, *, state: str, details: str | None = None) -> None:
    st.markdown(f"**{title}**")
    if state == "available":
        st.success("Available")
    elif state == "empty":
        st.info("No rows in the returned source")
    elif state == "unavailable":
        st.warning("Unavailable")
    elif state == "error":
        st.error("Read failed")
    else:
        st.info("Not checked")
    if details:
        st.caption(details)


def show_cached(st: Any, cached: bool) -> None:
    if cached:
        st.caption("Showing this session's last successful response. Use Refresh to read the API again.")


def row_count_text(count: Any, noun: str) -> str:
    if type(count) is not int:
        return "Count unavailable"
    return f"{count:,} {noun}{'' if count == 1 else 's'}"
