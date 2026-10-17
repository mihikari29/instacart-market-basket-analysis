"""Offline tests for sanitized, explicit-target Secret creation."""

import json
import unittest
from urllib.parse import parse_qs, unquote, urlsplit

from scripts.k8s.mongo_secrets import apply_mongo_secret_bundle, build_mongo_secret_bundle
from scripts.k8s.ownership import RunIdentity
from scripts.k8s.runtime import CommandResult


class FakeExecutor:
    def __init__(self, result=None):
        self.result = result or CommandResult(0, "secret applied")
        self.argv = None
        self.input_text = None

    def run(self, argv, *, timeout, input_text=None):
        self.argv = list(argv)
        self.input_text = input_text
        return self.result


class MongoSecretTests(unittest.TestCase):
    def test_bundle_has_independent_opaque_database_scoped_credentials(self):
        identity = RunIdentity("0123456789ab")
        first = build_mongo_secret_bundle(identity)
        second = build_mongo_secret_bundle(identity)
        self.assertEqual(
            first.names,
            (
                "module4d-mongo-admin",
                "module4d-mongo-accounts",
                "module4d-api-readonly",
                "module4d-mongo-publisher",
            ),
        )
        self.assertNotEqual(first.kubernetes_list_payload(), second.kubernetes_list_payload())
        items = json.loads(first.kubernetes_list_payload())["items"]
        self.assertTrue(all(item["type"] == "Opaque" and item["immutable"] for item in items))
        credentials = items[0]["stringData"]
        accounts = items[1]["stringData"]
        api_uri = items[2]["stringData"]["mongodb-uri"]
        publisher_uri = items[3]["stringData"]["mongodb-uri"]
        self.assertEqual(credentials["username"], f"root_{identity.run_id}")
        self.assertNotEqual(accounts["read-password"], accounts["publisher-password"])
        self.assertEqual(unquote(urlsplit(api_uri).path), f"/module4d_demo_{identity.run_id}")
        self.assertEqual(unquote(urlsplit(publisher_uri).path), f"/module4d_demo_{identity.run_id}")
        self.assertNotEqual(urlsplit(api_uri).username, urlsplit(publisher_uri).username)
        self.assertEqual(parse_qs(urlsplit(api_uri).query), {"authSource": [f"module4d_demo_{identity.run_id}"]})
        self.assertEqual(parse_qs(urlsplit(publisher_uri).query), {"authSource": [f"module4d_demo_{identity.run_id}"]})
        self.assertNotIn(credentials["password"], repr(first))
        self.assertNotIn(accounts["publisher-password"], repr(first))

    def test_apply_uses_explicit_context_and_stdin_without_echoing_driver_output(self):
        identity = RunIdentity("abcdef012345")
        bundle = build_mongo_secret_bundle(identity)
        executor = FakeExecutor(CommandResult(0, "secret values must not be returned to caller"))
        result = apply_mongo_secret_bundle(executor, identity, bundle, execute=True)
        self.assertEqual(executor.argv[:3], ["kubectl", "--context", identity.profile])
        self.assertIn("--namespace", executor.argv)
        self.assertEqual(executor.argv[-2:], ["-f", "-"])
        self.assertEqual(json.loads(executor.input_text)["kind"], "List")
        self.assertNotIn("secret values", repr(result))
        self.assertEqual(result["secret_names"], bundle.names)

    def test_failed_apply_is_sanitized(self):
        identity = RunIdentity("0123456789ab")
        executor = FakeExecutor(CommandResult(1, "sensitive output", "password in error"))
        with self.assertRaisesRegex(Exception, "kubectl exited with status 1") as raised:
            apply_mongo_secret_bundle(
                executor, identity, build_mongo_secret_bundle(identity), execute=True
            )
        self.assertNotIn("password", str(raised.exception))
        self.assertNotIn("sensitive", str(raised.exception))

    def test_secret_apply_requires_explicit_execute(self):
        identity = RunIdentity("0123456789ab")
        executor = FakeExecutor()
        with self.assertRaisesRegex(Exception, "explicit execute"):
            apply_mongo_secret_bundle(executor, identity, build_mongo_secret_bundle(identity))
        self.assertIsNone(executor.argv)


if __name__ == "__main__":
    unittest.main()
