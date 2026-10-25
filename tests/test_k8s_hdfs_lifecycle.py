"""Offline command contracts for the HDFS writer boundary."""

import unittest

from scripts.k8s.hdfs_lifecycle import bootstrap_hdfs
from scripts.k8s.ownership import RunIdentity
from scripts.k8s.runtime import CommandResult


class HdfsBootstrapTests(unittest.TestCase):
    def test_bootstrap_is_scoped_and_refuses_existing_run(self):
        identity = RunIdentity("0123456789ab")
        calls = []

        class Fake:
            def run(self, argv, **kwargs):
                calls.append(argv)
                return CommandResult(0, "")

        receipt = bootstrap_hdfs(Fake(), identity, execute=True)
        self.assertEqual(receipt["owner"], "k8spipeline")
        self.assertIn("-safemode", calls[0])
        self.assertEqual(calls[2][-4:], ["hdfs", "dfs", "-mkdir", receipt["root"]])
        self.assertEqual(calls[3][-2:], ["k8spipeline", receipt["root"]])
        self.assertTrue(all(identity.profile in call for call in calls))
        self.assertFalse(any("-R" in call for call in calls))

        class Existing:
            def run(self, argv, **kwargs):
                calls.append(argv)
                return CommandResult(1 if argv[-3:] == ["dfs", "-mkdir", receipt["root"]] else 0, "")

        calls.clear()
        with self.assertRaises(RuntimeError):
            bootstrap_hdfs(Existing(), identity, execute=True)
        self.assertEqual(len(calls), 3)

    def test_no_commands_without_authorization(self):
        with self.assertRaises(ValueError):
            bootstrap_hdfs(None, RunIdentity("0123456789ab"))
