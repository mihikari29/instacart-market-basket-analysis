#!/usr/bin/env python3
"""Create a private environment file for the isolated Module 4D demo."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import secrets
import uuid


def create_env(path: Path) -> None:
    tag = uuid.uuid4().hex[:12]
    values = {
        "MODULE4D_DEMO_TAG": tag,
        "MODULE4D_DEMO_DATABASE": f"module4d_demo_{tag}",
        "MODULE4D_DEMO_ROOT_PASSWORD": secrets.token_hex(32),
        "MODULE4D_DEMO_READONLY_USERNAME": "module4d_reader",
        "MODULE4D_DEMO_READONLY_PASSWORD": secrets.token_hex(32),
        "MODULE4D_DEMO_TREND_NAMESPACE": "module4d-demo",
        "MODULE4D_DEMO_API_HOST_PORT": "18000",
        "MODULE4D_DEMO_UI_HOST_PORT": "18501",
    }
    path = path.expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as output:
        output.writelines(f"{key}={value}\n" for key, value in values.items())
    path.chmod(0o600)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    arguments = parser.parse_args()
    create_env(arguments.output)
    print(f"Created private demo environment: {arguments.output.expanduser().resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
