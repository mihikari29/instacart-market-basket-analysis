from pathlib import Path
import tempfile
import unittest

from scripts.dashboard.api_validation import _copy_context
from tests.build_context import admitted


ROOT = Path(__file__).resolve().parents[1]


class D1BNarrowContextTests(unittest.TestCase):
    def test_narrow_context_contains_dockerfile_input_and_excludes_local_state(self):
        with tempfile.TemporaryDirectory(prefix="d1b-context-test-") as temporary:
            context = Path(temporary) / "context"
            context.mkdir()
            _copy_context(context)

            required = "scripts/k8s/write_python_environment.py"
            self.assertEqual(
                (context / required).read_bytes(),
                (ROOT / required).read_bytes(),
            )
            self.assertIn(
                f"COPY {required} /tmp/write-python-environment.py",
                (context / "Dockerfile").read_text(encoding="utf-8"),
            )
            self.assertEqual(
                {
                    path.relative_to(context).as_posix()
                    for path in (context / "scripts/k8s").rglob("*")
                    if path.is_file()
                },
                {required},
            )
            for excluded in ("data", "results", "checkpoints", ".git", "secrets", "credentials"):
                self.assertFalse((context / excluded).exists())

            dockerignore = (context / ".dockerignore").read_text(encoding="utf-8")
            self.assertTrue(
                admitted(dockerignore.splitlines(), required),
                f"{required} must be admitted into the build context",
            )
            for exclusion in ("**/data/**", "**/results/**", "**/checkpoints/**", "**/.env", "**/credentials/**"):
                self.assertIn(exclusion, dockerignore)


if __name__ == "__main__":
    unittest.main()
