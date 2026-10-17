"""Offline opt-in gates for all B2 mutation entry points."""

import unittest

from scripts.k8s.deployment import DeploymentError, deploy_serving
from scripts.k8s.image_operations import ImageOperationError, load_and_verify_application_images
from scripts.k8s.ownership import RunIdentity


class FakeExecutor:
    def __init__(self):
        self.calls = []

    def run(self, argv, *, timeout, input_text=None):
        self.calls.append(list(argv))
        raise AssertionError("the opt-in gate should prevent every external command")


class DeploymentGateTests(unittest.TestCase):
    def test_serving_deployment_is_inert_without_explicit_execute(self):
        executor = FakeExecutor()
        with self.assertRaisesRegex(DeploymentError, "explicit execute"):
            deploy_serving(
                executor,
                RunIdentity("0123456789ab"),
                {},
                host_capacity={},
                run_directory="/tmp/not-created",
                secret_bundle=None,
            )
        self.assertEqual(executor.calls, [])

    def test_image_loading_is_inert_without_explicit_execute(self):
        executor = FakeExecutor()
        with self.assertRaisesRegex(ImageOperationError, "explicit execute"):
            load_and_verify_application_images(
                executor, RunIdentity("0123456789ab"), {},
            )
        self.assertEqual(executor.calls, [])


if __name__ == "__main__":
    unittest.main()
