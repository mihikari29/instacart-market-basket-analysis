#!/usr/bin/env python3
"""Build and validate D1B in a narrow, disposable Docker context."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import tempfile
import uuid


ROOT = Path(__file__).resolve().parents[1]
BASE_IMAGE = "instacart-spark:3.5.5"
MONGO_IMAGE = "mongo:7.0.16"
UNIT_TESTS = (
    "tests/test_module4d_readers.py",
    "tests/test_module4d_api.py",
    "tests/test_module4d_batch_readers.py",
    "tests/test_module4d_evidence.py",
    "tests/test_module4d_batch_api.py",
    "tests/test_module4c_data.py",
    "tests/test_module4c_ranking.py",
    "tests/test_module4c_service.py",
    "tests/test_module4c_sink_mongo.py",
    "tests/test_module4c_trend_reader.py",
)
def _run(command: list[str], *, check: bool = True, env=None) -> subprocess.CompletedProcess:
    print("$ " + " ".join(command), flush=True)
    result = subprocess.run(command, cwd=ROOT, check=False, env=env)
    print(f"exit_code={result.returncode}", flush=True)
    if check and result.returncode:
        raise subprocess.CalledProcessError(result.returncode, command)
    return result


def _capture(command: list[str]) -> subprocess.CompletedProcess:
    result = subprocess.run(command, cwd=ROOT, check=False, capture_output=True, text=True)
    if result.returncode:
        print(result.stderr.strip(), flush=True)
    return result


def _copy_context(destination: Path) -> None:
    shutil.copy2(ROOT / "Dockerfile.module4d-api", destination / "Dockerfile.module4d-api")
    shutil.copy2(ROOT / "requirements-module4d-api.txt", destination / "requirements-module4d-api.txt")
    shutil.copy2(ROOT / "pyproject.toml", destination / "pyproject.toml")
    shutil.copytree(ROOT / "src", destination / "src", ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    tests_dir = destination / "tests"
    tests_dir.mkdir()
    for relative in (
        "tests/__init__.py",
        "tests/module4d_helpers.py",
        *UNIT_TESTS,
        "tests/test_module4d_api_mongo.py",
        "tests/test_module4d_batch_api_mongo.py",
    ):
        source = ROOT / relative
        target = tests_dir / source.name
        shutil.copy2(source, target)
    scripts_dir = destination / "scripts"
    scripts_dir.mkdir()
    shutil.copy2(ROOT / "scripts/module4d_api_validation.py", scripts_dir)
    for relative in (
        "docs/evidence/full/analytics.json",
        "docs/evidence/full/current-hardening-2026-10-03.json",
        "docs/evidence/module4/full-ab-20261010-fixed-first/evaluation.json",
        "docs/evidence/module4c/c8a-receipt.json",
        "docs/evidence/module4c/c8b-receipt.json",
    ):
        source = ROOT / relative
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)


def _build(image_tag: str) -> None:
    if shutil.which("docker") is None:
        raise RuntimeError("Docker CLI is required for the reproducible D1B image")
    _run(["docker", "image", "inspect", "--format", "{{.Id}}", BASE_IMAGE])
    with tempfile.TemporaryDirectory(prefix="d1b-api-context-") as temporary:
        context = Path(temporary)
        _copy_context(context)
        context_bytes = sum(path.stat().st_size for path in context.rglob("*") if path.is_file())
        print(f"narrow_context_bytes={context_bytes}", flush=True)
        _run(
            [
                "docker",
                "build",
                "--pull=false",
                "--tag",
                image_tag,
                "--file",
                "Dockerfile.module4d-api",
                str(context),
            ],
            env={**os.environ, "DOCKER_BUILDKIT": "1"},
        )


def _unit(image_tag: str, session_id: str) -> None:
    _run(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            "none",
            "--label",
            "com.codex.task=d1b",
            "--label",
            f"com.codex.session={session_id}",
            image_tag,
            "python3",
            "-m",
            "pytest",
            "-q",
            *UNIT_TESTS,
        ]
    )


def _ruff(image_tag: str, session_id: str) -> None:
    _run(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            "none",
            "--volume",
            f"{ROOT}:/repo:ro",
            "--workdir",
            "/repo",
            "--env",
            "RUFF_CACHE_DIR=/tmp/module4d-ruff-cache",
            "--label",
            "com.codex.task=d1b",
            "--label",
            f"com.codex.session={session_id}",
            image_tag,
            "python3",
            "-m",
            "ruff",
            "check",
            ".",
        ]
    )


def _runtime_smoke(image_tag: str, session_id: str) -> None:
    _run(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            "none",
            "--label",
            "com.codex.task=d1b",
            "--label",
            f"com.codex.session={session_id}",
            image_tag,
            "python3",
            "-c",
            "from src.module4d.api import app; paths=app.openapi()['paths']; required={'/api/v1/batch/products','/api/v1/batch/departments','/api/v1/evidence','/api/v1/evidence/{evidence_id}','/api/v1/analytics/orders-distribution','/api/v1/evaluation/als','/api/v1/recommendations/{user_id}','/api/v1/trends'}; assert required <= set(paths); print('API import and OpenAPI runtime smoke passed')",
        ]
    )


def _write_env(path: Path, values: dict[str, str]) -> None:
    path.write_text("".join(f"{key}={value}\n" for key, value in values.items()), encoding="utf-8")
    path.chmod(0o600)


def _mongo(image_tag: str, session_id: str) -> None:
    suffix = uuid.uuid4().hex[:12]
    network = f"d1b-net-{suffix}"
    mongo_name = f"d1b-mongo-{suffix}"
    root_password = secrets.token_urlsafe(32)
    with tempfile.TemporaryDirectory(prefix="d1b-mongo-env-") as temporary:
        temp_dir = Path(temporary)
        mongo_env = temp_dir / "mongo.env"
        test_env = temp_dir / "test.env"
        _write_env(
            mongo_env,
            {"MONGO_INITDB_ROOT_USERNAME": "root", "MONGO_INITDB_ROOT_PASSWORD": root_password},
        )
        _write_env(
            test_env,
            {
                "D1A_MONGODB_TEST_URI": f"mongodb://root:{root_password}@d1a-mongo:27017/?authSource=admin",
                "D1B_MONGODB_TEST_URI": f"mongodb://root:{root_password}@d1b-mongo:27017/?authSource=admin",
            },
        )
        try:
            _run(
                [
                    "docker",
                    "network",
                    "create",
                    "--label",
                    "com.codex.task=d1b",
                    "--label",
                    f"com.codex.session={session_id}",
                    network,
                ]
            )
            _run(
                [
                    "docker",
                    "run",
                    "--detach",
                    "--name",
                    mongo_name,
                    "--network",
                    network,
                    "--network-alias",
                    "d1a-mongo",
                    "--network-alias",
                    "d1b-mongo",
                    "--tmpfs",
                    "/data/db:rw,noexec,nosuid,size=768m",
                    "--tmpfs",
                    "/data/configdb:rw,noexec,nosuid,size=64m",
                    "--label",
                    "com.codex.task=d1b",
                    "--label",
                    "com.codex.resource=mongodb",
                    "--label",
                    f"com.codex.session={session_id}",
                    "--env-file",
                    str(mongo_env),
                    MONGO_IMAGE,
                ]
            )
            _run(
                [
                    "docker",
                    "run",
                    "--rm",
                    "--network",
                    network,
                    "--label",
                    "com.codex.task=d1b",
                    "--label",
                    "com.codex.resource=live-tests",
                    "--label",
                    f"com.codex.session={session_id}",
                    "--env-file",
                    str(test_env),
                    image_tag,
                    "python3",
                    "-m",
                    "pytest",
                    "-q",
                    "tests/test_module4d_api_mongo.py",
                    "tests/test_module4d_batch_api_mongo.py",
                ]
            )
        finally:
            _run(["docker", "rm", "--force", "--volumes", mongo_name], check=False)
            _run(["docker", "network", "rm", network], check=False)


def _verify_cleanup(session_id: str, image_tag: str) -> None:
    containers = _capture(
        ["docker", "ps", "--all", "--quiet", "--filter", f"label=com.codex.session={session_id}"],
    )
    networks = _capture(
        ["docker", "network", "ls", "--quiet", "--filter", f"label=com.codex.session={session_id}"],
    )
    if containers.returncode or networks.returncode or containers.stdout.strip() or networks.stdout.strip():
        raise RuntimeError("D1B task-labeled Docker containers or networks remain after cleanup")
    image = subprocess.run(
        ["docker", "image", "inspect", "--format", "{{.Id}}", image_tag],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if image.returncode == 0:
        raise RuntimeError("D1B validation image remains after cleanup")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("all", "unit", "mongo"), default="all")
    args = parser.parse_args()
    session_id = uuid.uuid4().hex
    image_tag = f"module4d-api-test:{session_id[:12]}"
    try:
        print(f"base_image={BASE_IMAGE} mongo_image={MONGO_IMAGE} mode={args.mode}", flush=True)
        _build(image_tag)
        _runtime_smoke(image_tag, session_id)
        if args.mode in {"all", "unit"}:
            _ruff(image_tag, session_id)
            _unit(image_tag, session_id)
        if args.mode in {"all", "mongo"}:
            _mongo(image_tag, session_id)
        print("D1B validation stages completed.", flush=True)
        return 0
    finally:
        _run(["docker", "image", "rm", "--force", image_tag], check=False)
        _verify_cleanup(session_id, image_tag)


if __name__ == "__main__":
    raise SystemExit(main())
