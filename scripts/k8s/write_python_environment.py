"""Write a deterministic inventory of the installed Python runtime packages."""

from __future__ import annotations

import importlib.metadata
from pathlib import Path
import platform
import re


OUTPUT = Path("/opt/it4931/python-environment.txt")


def environment_text() -> str:
    packages = {}
    for distribution in importlib.metadata.distributions():
        name = distribution.metadata.get("Name", "")
        version = distribution.version
        if not name or not version or not re.fullmatch(r"[A-Za-z0-9_.+-]+", name) or not re.fullmatch(
            r"[A-Za-z0-9_.+-]+", version
        ):
            raise RuntimeError("installed Python distribution metadata is invalid")
        canonical_name = re.sub(r"[-_.]+", "-", name).lower()
        if canonical_name in packages:
            raise RuntimeError("installed Python distributions contain a duplicate name")
        packages[canonical_name] = version
    lines = [f"python_version={platform.python_version()}"]
    lines.extend(f"{name}=={version}" for name, version in sorted(packages.items()))
    return "\n".join(lines) + "\n"


def main() -> int:
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(environment_text(), encoding="utf-8")
    OUTPUT.chmod(0o444)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
