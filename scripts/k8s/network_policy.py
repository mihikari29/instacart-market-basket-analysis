"""Static and read-only live inventory checks for the serving NetworkPolicy boundary."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from .ownership import OWNER_LABEL, OWNER_VALUE, RUN_LABEL, RunIdentity
from .runtime import CommandExecutor, RuntimePreflightError, json_output, checked, kubectl
from .pipeline import PIPELINE_POLICY_NAMES, PipelineError, validate_pipeline_policy_inventory


class NetworkPolicyError(ValueError):
    """Serving policy objects are missing, unowned, or inconsistent."""


REQUIRED_POLICY_NAMES = frozenset(
    {
        "default-deny",
        "allow-dns-egress",
        "allow-api-to-mongo",
        "allow-ui-to-api",
        "allow-api-egress",
        "allow-ui-egress",
    }
)


def _selector(policy: Mapping[str, Any]) -> dict[str, Any]:
    spec = policy.get("spec", {})
    value = spec.get("podSelector") if isinstance(spec, Mapping) else None
    return value if isinstance(value, dict) else {}


def validate_network_policy_inventory(
    policies: Sequence[Mapping[str, Any]],
    *,
    identity: RunIdentity | None = None,
) -> None:
    """Require the exact serving deny-first policies and narrowly allowed paths."""
    if not isinstance(policies, Sequence) or isinstance(policies, (str, bytes)):
        raise NetworkPolicyError("NetworkPolicy inventory must be a sequence")
    named: dict[str, Mapping[str, Any]] = {}
    for policy in policies:
        if not isinstance(policy, Mapping):
            raise NetworkPolicyError("NetworkPolicy inventory contains an invalid object")
        metadata = policy.get("metadata", {})
        name = metadata.get("name") if isinstance(metadata, Mapping) else None
        if not isinstance(name, str) or name in named:
            raise NetworkPolicyError("NetworkPolicy inventory contains missing or duplicate names")
        if identity is not None:
            labels = metadata.get("labels", {})
            if (
                metadata.get("namespace") != identity.namespace
                or not isinstance(labels, Mapping)
                or labels.get(OWNER_LABEL) != OWNER_VALUE
                or labels.get(RUN_LABEL) != identity.run_id
            ):
                raise NetworkPolicyError(f"NetworkPolicy {name} is not owned by the requested run")
        named[name] = policy
    if set(named) != REQUIRED_POLICY_NAMES:
        raise NetworkPolicyError("NetworkPolicy inventory is incomplete or contains an unapproved policy")

    deny = named["default-deny"].get("spec", {})
    if (
        _selector(named["default-deny"]) != {}
        or set(deny.get("policyTypes", [])) != {"Ingress", "Egress"}
        or deny.get("ingress") not in (None, [])
        or deny.get("egress") not in (None, [])
    ):
        raise NetworkPolicyError("default-deny must deny ingress and egress for every Pod")
    dns = named["allow-dns-egress"].get("spec", {})
    if _selector(named["allow-dns-egress"]) != {} or "Egress" not in dns.get("policyTypes", []):
        raise NetworkPolicyError("DNS policy must allow only explicit namespace-wide DNS egress")
    dns_rules = dns.get("egress", [])
    dns_ports = {
        (port.get("protocol"), port.get("port"))
        for rule in dns_rules if isinstance(rule, Mapping)
        for port in rule.get("ports", []) if isinstance(port, Mapping)
    }
    dns_targets = [
        target
        for rule in dns_rules if isinstance(rule, Mapping)
        for target in rule.get("to", []) if isinstance(target, Mapping)
    ]
    if len(dns_rules) != 1 or len(dns_targets) != 1 or dns_ports != {("UDP", 53), ("TCP", 53)} or not any(
        target.get("namespaceSelector", {}).get("matchLabels", {}).get("kubernetes.io/metadata.name") == "kube-system"
        and target.get("podSelector", {}).get("matchLabels", {}).get("k8s-app") == "kube-dns"
        for target in dns_targets
    ):
        raise NetworkPolicyError("DNS egress must target kube-system CoreDNS over TCP and UDP 53")

    _require_flow(
        named["allow-api-to-mongo"], target_pod="module4d-mongo", source_pod="module4d-api",
        port=27017, direction="ingress",
    )
    _require_flow(
        named["allow-ui-to-api"], target_pod="module4d-api", source_pod="module4d-ui",
        port=8000, direction="ingress",
    )
    _require_flow(
        named["allow-api-egress"], target_pod="module4d-api", source_pod="module4d-mongo",
        port=27017, direction="egress",
    )
    _require_flow(
        named["allow-ui-egress"], target_pod="module4d-ui", source_pod="module4d-api",
        port=8000, direction="egress",
    )


def _require_flow(
    policy: Mapping[str, Any], *, target_pod: str, source_pod: str, port: int, direction: str
) -> None:
    spec = policy.get("spec", {})
    if not isinstance(spec, Mapping):
        raise NetworkPolicyError("NetworkPolicy spec is invalid")
    selector = spec.get("podSelector", {}).get("matchLabels", {})
    if selector.get("app.kubernetes.io/name") != target_pod:
        raise NetworkPolicyError("NetworkPolicy target selector does not match its allowed flow")
    expected_type = "Ingress" if direction == "ingress" else "Egress"
    if expected_type not in spec.get("policyTypes", []):
        raise NetworkPolicyError("NetworkPolicy type does not match its allowed flow")
    rules = spec.get(direction, [])
    if len(rules) != 1:
        raise NetworkPolicyError("NetworkPolicy must contain exactly one narrow flow rule")
    flow = None
    for rule in rules:
        if not isinstance(rule, Mapping):
            continue
        endpoint_field = "from" if direction == "ingress" else "to"
        endpoints = rule.get(endpoint_field, [])
        if len(endpoints) == 1 and len(rule.get("ports", [])) == 1 and any(
            isinstance(endpoint, Mapping)
            and endpoint.get("podSelector", {}).get("matchLabels", {}).get("app.kubernetes.io/name") == source_pod
            for endpoint in endpoints
        ) and any(
            isinstance(candidate, Mapping)
            and candidate.get("protocol") == "TCP"
            and candidate.get("port") == port
            for candidate in rule.get("ports", [])
        ):
            flow = rule
            break
    if flow is None:
        raise NetworkPolicyError("NetworkPolicy does not express the required one-direction flow")


def collect_network_policy_inventory(
    executor: CommandExecutor, identity: RunIdentity, *, execute: bool = False, include_pipeline: bool = False
) -> dict[str, Any]:
    """Read exact policy objects from the explicit run context without probing flows."""
    if execute is not True:
        raise NetworkPolicyError("live policy discovery requires explicit execute=True authorization")
    try:
        payload = json_output(
            checked(executor, kubectl(identity, "--namespace", identity.namespace, "get", "networkpolicies", "-o", "json")),
            "NetworkPolicy inventory",
        )
    except RuntimePreflightError:
        raise
    items = payload.get("items") if isinstance(payload, Mapping) else None
    if not isinstance(items, list):
        raise NetworkPolicyError("live NetworkPolicy inventory is invalid")
    names = {item.get("metadata", {}).get("name") for item in items if isinstance(item, Mapping)}
    allowed = REQUIRED_POLICY_NAMES | PIPELINE_POLICY_NAMES
    if not names.issubset(allowed):
        raise NetworkPolicyError("live policy inventory contains an unapproved policy")
    base_items = [item for item in items if item.get("metadata", {}).get("name") in REQUIRED_POLICY_NAMES]
    validate_network_policy_inventory(base_items, identity=identity)
    pipeline_items = [item for item in items if item.get("metadata", {}).get("name") in PIPELINE_POLICY_NAMES]
    if pipeline_items:
        if not include_pipeline:
            raise NetworkPolicyError("B3 pipeline policies exist but were not requested in the inventory")
        for item in pipeline_items:
            labels = item.get("metadata", {}).get("labels", {})
            if (
                item.get("metadata", {}).get("namespace") != identity.namespace
                or labels.get(OWNER_LABEL) != OWNER_VALUE
                or labels.get(RUN_LABEL) != identity.run_id
                or labels.get("it4931.instacart.edu/component") != "pipeline"
            ):
                raise NetworkPolicyError("live B3 NetworkPolicy ownership labels do not match")
        try:
            validate_pipeline_policy_inventory(pipeline_items)
        except PipelineError as exc:
            raise NetworkPolicyError("live B3 pipeline NetworkPolicy contract is invalid") from exc
    elif include_pipeline:
        raise NetworkPolicyError("B3 pipeline policies are incomplete")
    return {
        "status": "objects_present",
        "namespace": identity.namespace,
        "names": sorted(names),
        "pipeline_policy_inventory": "present" if pipeline_items else "not_requested",
        "enforcement": "not_proven; packet-flow checks belong to B4 runtime acceptance",
    }
