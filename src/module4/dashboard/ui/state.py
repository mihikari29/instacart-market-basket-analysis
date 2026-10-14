"""Session-local API client and bounded successful-response cache."""

from __future__ import annotations

import json
import os
from typing import Any, Callable

from .api_client import ApiClient, ApiClientError


_CLIENT_KEY = "_module4d_api_client"
_CACHE_KEY = "_module4d_response_cache"
_MAX_CACHE_ENTRIES = 32
_REFRESH_KEY = "_module4d_refresh_requested"
ANALYTICS_LAST_RUN_KEY = "_module4d_analytics_last_run"


def configured_trend_namespaces() -> tuple[str, ...]:
    raw = os.environ.get("MODULE4D_TREND_SOURCE_NAMESPACES", "")
    values = tuple(value.strip() for value in raw.split(",") if value.strip())
    return tuple(dict.fromkeys(value for value in values if len(value) <= 256 and "\x00" not in value))


def get_api_client(session_state: Any) -> tuple[Any | None, ApiClientError | None]:
    injected = session_state.get(_CLIENT_KEY)
    if injected is not None:
        return injected, None
    try:
        client = ApiClient()
    except ApiClientError as exc:
        return None, exc
    session_state[_CLIENT_KEY] = client
    return client, None


def clear_response_cache(session_state: Any) -> None:
    session_state.pop(_CACHE_KEY, None)
    session_state.pop(ANALYTICS_LAST_RUN_KEY, None)
    session_state[_REFRESH_KEY] = True


def consume_refresh_request(session_state: Any) -> bool:
    return bool(session_state.pop(_REFRESH_KEY, False))


def _cache_key(client: Any, method_name: str, args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
    return json.dumps(
        {"base_url": client.base_url, "method": method_name, "args": args, "kwargs": kwargs},
        sort_keys=True,
        default=str,
        separators=(",", ":"),
    )


def fetch(
    session_state: Any,
    client: Any,
    method_name: str,
    *args: Any,
    refresh: bool = False,
    **kwargs: Any,
) -> tuple[dict[str, Any] | None, ApiClientError | None, bool]:
    """Fetch and cache valid HTTP 200 data for this Streamlit session only."""
    cache = session_state.setdefault(_CACHE_KEY, {})
    key = _cache_key(client, method_name, args, kwargs)
    if not refresh and key in cache:
        return cache[key], None, True
    if refresh:
        cache.pop(key, None)
    try:
        method: Callable[..., dict[str, Any]] = getattr(client, method_name)
        payload = method(*args, **kwargs)
    except ApiClientError as exc:
        return None, exc, False
    except (AttributeError, TypeError, ValueError):
        return (
            None,
            ApiClientError("The dashboard could not prepare this API request.", code="invalid_request"),
            False,
        )
    if not isinstance(payload, dict):
        return (
            None,
            ApiClientError(
                "The dashboard API returned an invalid response.", status_code=502, code="invalid_response"
            ),
            False,
        )
    cache[key] = payload
    while len(cache) > _MAX_CACHE_ENTRIES:
        cache.pop(next(iter(cache)))
    return payload, None, False
