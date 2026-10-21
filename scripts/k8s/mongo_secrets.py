"""Create run-scoped MongoDB Secret documents without writing credentials to disk."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import secrets
from typing import Any
from urllib.parse import quote

from .constants import demo_database
from .ownership import OWNER_LABEL, OWNER_VALUE, RUN_LABEL, RunIdentity
from .runtime import CommandExecutor, RuntimePreflightError, kubectl


@dataclass(frozen=True, slots=True, repr=False)
class MongoSecretBundle:
    """In-memory opaque Secret values; repr deliberately omits all payload data."""

    run_id: str
    _items: tuple[dict[str, Any], ...] = field(repr=False)

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(item["metadata"]["name"] for item in self._items)

    def kubernetes_list_payload(self) -> str:
        """Serialize only for a caller that sends this string directly to kubectl stdin."""
        return json.dumps(
            {"apiVersion": "v1", "kind": "List", "items": self._items},
            separators=(",", ":"),
            ensure_ascii=True,
        )

    def __repr__(self) -> str:
        return f"MongoSecretBundle(run_id={self.run_id!r}, secret_count={len(self._items)})"


def _secret(identity: RunIdentity, name: str, values: dict[str, str]) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": name,
            "namespace": identity.namespace,
            "labels": {
                OWNER_LABEL: OWNER_VALUE,
                RUN_LABEL: identity.run_id,
                "app.kubernetes.io/managed-by": "it4931-k8s-demo",
            },
        },
        "type": "Opaque",
        "immutable": True,
        "stringData": values,
    }


def build_mongo_secret_bundle(identity: RunIdentity) -> MongoSecretBundle:
    """Generate independent admin, application, API-read, and publisher credentials."""
    if not isinstance(identity, RunIdentity):
        raise RuntimePreflightError("a validated run identity is required")
    database = demo_database(identity.run_id)
    admin_user = f"root_{identity.run_id}"
    read_user = f"api_read_{identity.run_id}"
    publisher_user = f"publisher_{identity.run_id}"
    admin_password = secrets.token_urlsafe(32)
    read_password = secrets.token_urlsafe(32)
    publisher_password = secrets.token_urlsafe(32)

    def uri(username: str, password: str) -> str:
        return (
            f"mongodb://{quote(username, safe='')}:{quote(password, safe='')}"
            f"@module4d-mongo:27017/{database}?authSource={database}"
        )

    items = (
        _secret(identity, "module4d-mongo-admin", {"username": admin_user, "password": admin_password}),
        _secret(
            identity,
            "module4d-mongo-accounts",
            {
                "read-username": read_user,
                "read-password": read_password,
                "publisher-username": publisher_user,
                "publisher-password": publisher_password,
            },
        ),
        _secret(identity, "module4d-api-readonly", {"mongodb-uri": uri(read_user, read_password)}),
        _secret(
            identity,
            "module4d-mongo-publisher",
            {"mongodb-uri": uri(publisher_user, publisher_password)},
        ),
    )
    return MongoSecretBundle(identity.run_id, items)


def apply_mongo_secret_bundle(
    executor: CommandExecutor,
    identity: RunIdentity,
    bundle: MongoSecretBundle,
    *,
    execute: bool = False,
    timeout: int = 30,
) -> dict[str, Any]:
    """Apply secrets through explicit-context stdin; never surface subprocess text."""
    if execute is not True:
        raise RuntimePreflightError("Secret apply requires explicit execute=True authorization")
    if not isinstance(bundle, MongoSecretBundle) or bundle.run_id != identity.run_id:
        raise RuntimePreflightError("MongoDB Secret bundle does not match the run identity")
    command = kubectl(identity, "--namespace", identity.namespace, "apply", "-f", "-")
    try:
        result = executor.run(command, timeout=timeout, input_text=bundle.kubernetes_list_payload())
    except Exception as exc:
        raise RuntimePreflightError(f"MongoDB Secret apply failed ({type(exc).__name__})") from exc
    if result.returncode != 0:
        raise RuntimePreflightError(f"kubectl exited with status {result.returncode} while applying Secrets")
    return {"status": "applied", "namespace": identity.namespace, "secret_names": bundle.names}
