"""Offline NetworkPolicy shape and read-only inventory checks."""

import json
from pathlib import Path
import unittest

import yaml

from scripts.k8s.network_policy import (
    NetworkPolicyError,
    collect_network_policy_inventory,
    validate_network_policy_inventory,
)
from scripts.k8s.ownership import OWNER_LABEL, OWNER_VALUE, RUN_LABEL, RunIdentity
from scripts.k8s.runtime import CommandResult


ROOT = Path(__file__).resolve().parents[1]


class NetworkPolicyTests(unittest.TestCase):
    def test_base_policy_set_is_deny_first_and_narrow(self):
        policies = list(yaml.safe_load_all((ROOT / "deploy/k8s/base/network-policies.yaml").read_text()))
        validate_network_policy_inventory(policies)

    def test_policy_inventory_rejects_broad_and_missing_flows(self):
        policies = list(yaml.safe_load_all((ROOT / "deploy/k8s/base/network-policies.yaml").read_text()))
        broken = [dict(policy) for policy in policies]
        api_egress = next(item for item in broken if item["metadata"]["name"] == "allow-api-egress")
        api_egress["spec"]["egress"].append({"to": [{}]})
        with self.assertRaisesRegex(NetworkPolicyError, "exactly one"):
            validate_network_policy_inventory(broken)

    def test_live_inventory_uses_explicit_context_and_run_labels(self):
        identity = RunIdentity("0123456789ab")
        labels = {
            OWNER_LABEL: OWNER_VALUE,
            RUN_LABEL: identity.run_id,
            "app.kubernetes.io/managed-by": "it4931-k8s-demo",
        }
        policies = list(yaml.safe_load_all((ROOT / "deploy/k8s/base/network-policies.yaml").read_text()))
        for policy in policies:
            policy["metadata"]["namespace"] = identity.namespace
            policy["metadata"]["labels"] = labels

        class Executor:
            argv = None

            def run(self, argv, *, timeout, input_text=None):
                self.argv = list(argv)
                return CommandResult(0, json.dumps({"items": policies}))

        executor = Executor()
        result = collect_network_policy_inventory(executor, identity, execute=True)
        self.assertEqual(result["status"], "objects_present")
        self.assertIn("not_proven", result["enforcement"])
        self.assertEqual(executor.argv[:3], ["kubectl", "--context", identity.profile])


if __name__ == "__main__":
    unittest.main()
