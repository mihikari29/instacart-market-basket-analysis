"""Pure contract tests for the bounded C8B streaming demonstration."""

from __future__ import annotations

import json
import math
from pathlib import Path
import subprocess
from types import SimpleNamespace

import pytest

from scripts.module4.module4c_streaming_demo import (
    FIXTURE_INGESTION_MS,
    REQUIRED_EVENT_FIELDS,
    SOURCE_SCOPE,
    TARGET_A_END,
    TARGET_A_START,
    TARGET_B_END,
    TARGET_B_START,
    WATERMARK_DELAY,
    build_event_fixture,
    cleanup_task_docker_resources,
    expected_source_provenance,
    finalize_report,
    independent_hybrid_oracle,
    independent_trend_oracle,
    parse_timestamp,
    redact_secrets,
    resource_names,
    target_window_events,
    timestamp,
    validate_disposable_database_name,
    validate_event,
    validate_fixture,
    validate_mongodb_target,
    validate_report,
    validate_run_id,
    wait_bounded,
)
from src.module3.schemas import KAFKA_EVENT_SCHEMA


RUN_ID = "c8b-20261013t151900z-abc123ef"


def _mock_container_id(index: int) -> str:
    return f"{index + 1:064x}"


def _report() -> dict:
    return {
        "experiment_id": RUN_ID,
        "status": "passed",
        "git_commit": "a" * 40,
        "git_dirty": True,
        "source_scope": SOURCE_SCOPE,
        "source_namespace": "c8b:controlled-synthetic:test",
        "dataset_type": "controlled synthetic Kafka fixture",
        "recommendation_accuracy_measured": False,
        "production_latency_benchmark": False,
        "kafka_topic": "i3-c8b-test",
        "event_counts": {"planned": 48, "acknowledged": 48, "consumed": 48},
        "spark_version": "3.5.5",
        "kafka_version": "3.7.0",
        "mongodb_version": "7.0.16",
        "window_configuration": {"window_long": "120 minutes", "slide": "5 minutes"},
        "watermark_configuration": {"delay": "10 minutes"},
        "checkpoint_identity": RUN_ID,
        "target_windows": {},
        "spark_progress_summary": {},
        "bundle_identities": [],
        "bundle_content_hashes": [],
        "serving_requests": [],
        "serving_versions": [],
        "ranking_before": {},
        "ranking_after": {},
        "readiness_checks": {},
        "retry_checks": {},
        "timing_observations": {
            "kafka_producer_ack_wall_ms": [],
            "spark_microbatch_duration_ms": {},
            "processing_time_to_finalized_publication_ms": {},
            "module4c_cli_wall_ms": [],
        },
        "cleanup_status": {},
        "limitations": [],
    }


def test_fixture_generation_is_deterministic_and_has_48_unique_events():
    first = build_event_fixture(RUN_ID)
    second = build_event_fixture(RUN_ID)
    assert first == second
    assert sum(map(len, first.values())) == 48
    all_events = [event for phase in first.values() for event in phase]
    assert len({event["event_id"] for event in all_events}) == 48
    assert len({event["order_id"] for event in all_events}) == 48
    assert all(event["ingestion_time_epoch_ms"] == FIXTURE_INGESTION_MS for event in all_events)


def test_fixture_schema_matches_module3_and_each_event_is_valid():
    phases = build_event_fixture(RUN_ID)
    assert tuple(KAFKA_EVENT_SCHEMA.fieldNames()) == REQUIRED_EVENT_FIELDS
    for phase in phases.values():
        for event in phase:
            validate_event(event)
            assert set(event) == set(KAFKA_EVENT_SCHEMA.fieldNames())
            assert timestamp(parse_timestamp(event["event_time_iso"])) == event["event_time_iso"]


def test_fixture_phases_and_event_times_are_chronological():
    phases = build_event_fixture(RUN_ID)
    assert tuple(phases) == ("window_a", "advance_a", "window_b", "advance_b")
    maxima = []
    for phase in phases.values():
        event_times = [event["event_time_epoch_ms"] for event in phase]
        assert event_times == sorted(event_times)
        maxima.append(max(event_times))
    assert maxima == sorted(maxima)
    validate_fixture(phases)


@pytest.mark.parametrize(
    ("start", "end", "phase", "favored"),
    [
        (TARGET_A_START, TARGET_A_END, "window_a", 1),
        (TARGET_B_START, TARGET_B_END, "window_b", 2),
    ],
)
def test_target_events_have_expected_window_membership_and_favored_counts(start, end, phase, favored):
    phases = build_event_fixture(RUN_ID)
    events = target_window_events(phases, start, end)
    assert len(events) == 22
    assert all(start <= parse_timestamp(event["event_time_iso"]) < end for event in events)
    counts = {product_id: 0 for product_id in range(1, 21)}
    for event in events:
        counts[event["product_id"]] += 1
    assert counts[favored] == 3
    assert all(count == 1 for product_id, count in counts.items() if product_id != favored)
    assert all(event in phases[phase] for event in events)


def test_watermark_advancement_requires_strictly_passing_target_end():
    phases = build_event_fixture(RUN_ID)
    max_a = max(parse_timestamp(event["event_time_iso"]) for event in phases["advance_a"])
    max_b = max(parse_timestamp(event["event_time_iso"]) for event in phases["advance_b"])
    assert max_a - WATERMARK_DELAY > TARGET_A_END
    assert max_b - WATERMARK_DELAY > TARGET_B_END

    invalid = {name: [dict(event) for event in events] for name, events in phases.items()}
    exact_threshold = TARGET_A_END + WATERMARK_DELAY
    for candidate in invalid["advance_a"]:
        candidate["event_time_epoch_ms"] = int(exact_threshold.timestamp() * 1000)
        candidate["event_time_iso"] = timestamp(exact_threshold)
    with pytest.raises(ValueError, match="strictly beyond"):
        validate_fixture(invalid)


def test_independent_module3_oracle_ranks_p1_then_p2_with_exact_counts():
    phases = build_event_fixture(RUN_ID)
    a = independent_trend_oracle(
        target_window_events(phases, TARGET_A_START, TARGET_A_END),
        start=TARGET_A_START,
        end=TARGET_A_END,
    )
    b = independent_trend_oracle(
        target_window_events(phases, TARGET_B_START, TARGET_B_END),
        start=TARGET_B_START,
        end=TARGET_B_END,
    )
    assert len(a) == len(b) == 20
    assert a[0]["product_id"] == 1
    assert b[0]["product_id"] == 2
    assert a[0]["purchase_count_30m"] == a[0]["purchase_count_120m"] == 3
    assert b[1]["product_id"] == 1
    assert all(row["trend_score"] == 0.0 for row in a[1:])
    assert all(row["trend_score"] == 0.0 for row in b if row["product_id"] != 2)
    assert a[0]["trend_score"] == b[0]["trend_score"] == 1.0


def test_independent_hybrid_oracle_changes_top1_at_lambda_half():
    phases = build_event_fixture(RUN_ID)
    a_trend = independent_trend_oracle(
        target_window_events(phases, TARGET_A_START, TARGET_A_END),
        start=TARGET_A_START,
        end=TARGET_A_END,
    )
    b_trend = independent_trend_oracle(
        target_window_events(phases, TARGET_B_START, TARGET_B_END),
        start=TARGET_B_START,
        end=TARGET_B_END,
    )
    a = independent_hybrid_oracle(a_trend, lambda_weight=0.5)
    b = independent_hybrid_oracle(b_trend, lambda_weight=0.5)
    assert a[0]["product_id"] == 1
    assert b[0]["product_id"] == 2
    assert math.isclose(a[0]["final_score"], 1.0, abs_tol=1e-12)
    assert math.isclose(b[0]["final_score"], 0.75, abs_tol=1e-12)


def test_fixture_uses_explicit_controlled_synthetic_scope():
    assert SOURCE_SCOPE == "controlled_synthetic"
    report = _report()
    validate_report(report)
    report["source_scope"] = "prior_only"
    with pytest.raises(ValueError, match="controlled_synthetic"):
        validate_report(report)


def test_readiness_provenance_maps_to_cli_source_fields():
    readiness = {
        "run_id": "c8b-source-run",
        "model_version": "c8b-als-model",
        "graph_version": "c8b-graph",
    }
    assert expected_source_provenance(readiness) == {
        "source_run_id": "c8b-source-run",
        "model_version": "c8b-als-model",
        "graph_version": "c8b-graph",
    }


def test_evidence_must_disclaim_accuracy_and_production_latency_claims():
    report = _report()
    report["recommendation_accuracy_measured"] = True
    with pytest.raises(ValueError, match="accuracy was not measured"):
        validate_report(report)
    report["recommendation_accuracy_measured"] = False
    report["production_latency_benchmark"] = True
    with pytest.raises(ValueError, match="production latency benchmark"):
        validate_report(report)


def test_evidence_schema_requires_distinct_timing_categories_and_finite_values():
    report = _report()
    validate_report(report)
    report["timing_observations"]["module4c_cli_wall_ms"] = [math.inf]
    with pytest.raises(ValueError, match="non-finite"):
        validate_report(report)


def test_evidence_rejects_fabricated_event_to_serving_latency():
    report = _report()
    report["timing_observations"]["event_to_serving_latency_ms"] = 3600000
    with pytest.raises(ValueError, match="event-to-serving latency"):
        validate_report(report)


def test_timeout_is_bounded_and_includes_last_observation():
    now = [0.0]
    observations = iter((None, None))

    def clock():
        return now[0]

    def sleep(duration):
        now[0] += duration

    with pytest.raises(TimeoutError, match="last=None"):
        wait_bounded(
            lambda: next(observations),
            description="a final bundle",
            timeout_seconds=0.5,
            poll_seconds=0.25,
            monotonic=clock,
            sleep=sleep,
        )
    assert now[0] == 0.5


def test_wait_returns_observation_without_extra_polling():
    calls = []
    result = wait_bounded(
        lambda: calls.append(1) or {"ready": True},
        description="ready document",
        timeout_seconds=1,
        sleep=lambda _duration: pytest.fail("ready predicate must not sleep"),
    )
    assert result == {"ready": True}
    assert len(calls) == 1


def test_resource_names_are_isolated_by_validated_run_id():
    first = resource_names(RUN_ID)
    second = resource_names("c8b-20261013t151901z-abc123ef")
    assert first["network"] != second["network"]
    assert first["kafka_container"] != second["kafka_container"]
    assert first["mongo_container"] != second["mongo_container"]
    assert first["spark_container"] != second["spark_container"]
    assert first["topic"] != second["topic"]
    assert first["database"] != second["database"]
    assert first["owner_helper_container"] != second["owner_helper_container"]
    assert first["checkpoint_id"] == RUN_ID
    validate_disposable_database_name(first["database"])
    with pytest.raises(ValueError, match="run ID"):
        validate_run_id("c8b-../../instacart")


@pytest.mark.parametrize("database", ["instacart", "admin", "config", "local", "i3_c8b_bad"])
def test_database_guard_refuses_production_or_malformed_database(database):
    with pytest.raises(ValueError, match="disposable"):
        validate_disposable_database_name(database)


@pytest.mark.parametrize(
    "uri",
    [
        "mongodb://localhost:27017",
        "mongodb://127.0.0.1:27017",
        "mongodb://c8b-mongo:27017/instacart",
        "mongodb://user:secret@c8b-mongo:27017",
        "mongodb://c8b-mongo:27017/?authSource=admin",
        "mongodb://c8b-mongo:27017,mongodb:27017",
        "not-a-mongodb-uri",
    ],
)
def test_mongodb_guard_refuses_external_production_or_credentialed_targets(uri):
    with pytest.raises(ValueError, match="MongoDB URI"):
        validate_mongodb_target(uri)


def test_mongodb_guard_allows_only_dedicated_internal_service():
    validate_mongodb_target("mongodb://c8b-mongo:27017")


def test_credential_redaction_covers_uri_and_inline_secrets():
    result = redact_secrets(
        "failed mongodb://fixture:topsecret@c8b-mongo:27017 password=anothersecret"
    )
    assert "topsecret" not in result
    assert "anothersecret" not in result
    assert "mongodb://<redacted>" in result
    assert "password=<redacted>" in result


def test_event_validation_rejects_timestamp_mismatch_and_extra_fields():
    event = build_event_fixture(RUN_ID)["window_a"][0]
    invalid_time = dict(event, event_time_iso="2024-01-01T00:00:00Z")
    with pytest.raises(ValueError, match="does not agree"):
        validate_event(invalid_time)
    with pytest.raises(ValueError, match="event schema"):
        validate_event(dict(event, hidden_ground_truth=True))


def test_event_fixture_covers_only_the_seeded_twenty_product_catalog():
    events = [event for phase in build_event_fixture(RUN_ID).values() for event in phase]
    assert {event["product_id"] for event in events} == set(range(1, 21))
    assert {event["user_id"] for event in events} == {9401}
    assert all("label" not in event and "ground_truth" not in event for event in events)


class _DockerSubprocess:
    """Small Docker CLI model for exercising the production cleanup function."""

    def __init__(self, run_id=RUN_ID, *, named_volume=False, leaked_volumes=(), leaked_containers=(), leaked_networks=()):
        names = resource_names(run_id)
        labels = {"codex.task": "c8b", "codex.run": run_id}
        self.containers = {}
        self.networks = {
            names["network"]: {"Id": "network-id", "Name": names["network"], "Labels": labels}
        }
        self.volumes = set()
        self.named_volumes = set()
        self.leaked_volumes = set(leaked_volumes)
        self.leaked_containers = set(leaked_containers)
        self.leaked_networks = set(leaked_networks)
        self.commands = []

        for index, role in enumerate(("spark_container", "kafka_container", "mongo_container", "owner_helper_container")):
            name = names[role]
            container_id = _mock_container_id(index)
            mounts = []
            config_volumes = {}
            host_mounts = []
            binds = []
            if role == "kafka_container":
                destinations = (
                    "/etc/kafka/secrets",
                    "/mnt/shared/config",
                    "/var/lib/kafka/data",
                )
                for volume_index, destination in enumerate(destinations):
                    volume_name = f"{volume_index + 1:064x}"
                    config_volumes[destination] = {}
                    mounts.append(
                        {"Type": "volume", "Name": volume_name, "Destination": destination, "RW": True}
                    )
                    self.volumes.add(volume_name)
                    if named_volume and volume_index == 2:
                        self.volumes.discard(volume_name)
                        volume_name = "project-kafka-data"
                        mounts[-1]["Name"] = volume_name
                        host_mounts.append({"Type": "volume", "Source": volume_name, "Target": destination})
                        self.volumes.add(volume_name)
                        self.named_volumes.add(volume_name)
            elif role == "spark_container":
                mounts.append({"Type": "bind", "Source": "/repo", "Destination": "/workspace", "RW": False})
                mounts.append({"Type": "bind", "Source": "/out", "Destination": "/demo-output", "RW": True})
                binds = ["/repo:/workspace:ro", "/out:/demo-output:rw"]
            self.containers[name] = {
                "Id": container_id,
                "Name": f"/{name}",
                "Config": {"Labels": labels, "Volumes": config_volumes},
                "HostConfig": {"Mounts": host_mounts, "Binds": binds},
                "Mounts": mounts,
                "State": {"Status": "exited" if role == "kafka_container" else "running"},
            }
        self.project_container = {
            "Id": "9" * 64,
            "Name": "/instacart-kafka-1",
            "Config": {"Labels": {"com.docker.compose.project": "instacart"}, "Volumes": {}},
            "HostConfig": {"Mounts": [], "Binds": []},
            "Mounts": [],
            "State": {"Status": "exited"},
        }
        self.project_volume = "instacart_mongodb_data"
        self.volumes.add(self.project_volume)
        self.containers[self.project_container["Name"].lstrip("/")] = self.project_container

    @staticmethod
    def _done(command, returncode=0, stdout="", stderr=""):
        return subprocess.CompletedProcess(command, returncode, stdout, stderr)

    def __call__(self, command, **_kwargs):
        assert command[0] == "docker"
        args = command[1:]
        self.commands.append(args)
        if args[:2] in (["container", "inspect"], ["network", "inspect"], ["volume", "inspect"]):
            kind, _, name = args[:3]
            mapping = {"container": self.containers, "network": self.networks, "volume": {v: {"Name": v} for v in self.volumes}}[kind]
            value = mapping.get(name)
            if value is None:
                return self._done(command, 1, stderr=f"Error: No such object: {name}")
            return self._done(command, stdout=json.dumps([value]))
        if args[0] == "logs":
            return self._done(command, stdout="synthetic log output\n")
        if args[0] == "ps" and "-aq" in args:
            filters = [args[index + 1] for index, arg in enumerate(args[:-1]) if arg == "--filter"]
            volume_filter = next((item.split("=", 1)[1] for item in filters if item.startswith("volume=")), None)
            if volume_filter:
                ids = [
                    container["Id"][:12]
                    for container in self.containers.values()
                    if any(mount.get("Name") == volume_filter for mount in container.get("Mounts", []))
                ]
                return self._done(command, stdout="\n".join(ids))
            labels = dict(item.split("=", 1)[1].split("=", 1) for item in filters if item.startswith("label="))
            ids = [
                container["Id"][:12]
                for container in self.containers.values()
                if all((container.get("Config", {}).get("Labels") or {}).get(key) == value for key, value in labels.items())
            ]
            return self._done(command, stdout="\n".join(ids))
        if args[:2] == ["network", "ls"]:
            filters = [args[index + 1] for index, arg in enumerate(args[:-1]) if arg == "--filter"]
            labels = dict(item.split("=", 1)[1].split("=", 1) for item in filters if item.startswith("label="))
            ids = [
                network["Id"] for network in self.networks.values()
                if all((network.get("Labels") or {}).get(key) == value for key, value in labels.items())
            ]
            return self._done(command, stdout="\n".join(ids))
        if args[0] == "rm":
            container_id = args[-1]
            container = next((value for value in self.containers.values() if value["Id"] == container_id), None)
            if container is None:
                return self._done(command, 1, stderr=f"Error: No such container: {container_id}")
            if container_id not in self.leaked_containers:
                if "-v" in args:
                    for mount in container.get("Mounts", []):
                        volume_name = mount.get("Name")
                        if volume_name and volume_name not in self.named_volumes and volume_name not in self.leaked_volumes:
                            self.volumes.discard(volume_name)
                del self.containers[container["Name"].lstrip("/")]
            return self._done(command, stdout=container_id)
        if args[:2] == ["network", "rm"]:
            network_id = args[2]
            network = next((value for value in self.networks.values() if value["Id"] == network_id), None)
            if network is None:
                return self._done(command, 1, stderr=f"Error: No such network: {network_id}")
            if network_id not in self.leaked_networks:
                del self.networks[network["Name"]]
            return self._done(command, stdout=network_id)
        raise AssertionError(f"unexpected Docker command: {args}")


def _run_mocked_docker_cleanup(monkeypatch, tmp_path, fake, run_id=RUN_ID):
    monkeypatch.setattr("scripts.module4.module4c_streaming_demo.subprocess.run", fake)
    return cleanup_task_docker_resources(run_id, evidence_dir=Path(tmp_path))


def test_docker_cleanup_captures_anonymous_volume_ids_before_container_removal(monkeypatch, tmp_path):
    fake = _DockerSubprocess()
    receipt = _run_mocked_docker_cleanup(monkeypatch, tmp_path, fake)
    assert receipt["status"] == "passed"
    assert len(receipt["captured_anonymous_volumes"]) == 3
    for volume in receipt["captured_anonymous_volumes"]:
        assert volume["absent_after_cleanup"] is True
        inspect_index = next(
            index for index, args in enumerate(fake.commands)
            if args[:3] == ["container", "inspect", resource_names(RUN_ID)["kafka_container"]]
        )
        remove_index = next(
            index for index, args in enumerate(fake.commands)
            if args[:2] == ["rm", "-f"] and args[-1] == _mock_container_id(1)
        )
        assert inspect_index < remove_index
    kafka_remove = next(args for args in fake.commands if args[:2] == ["rm", "-f"] and args[-1] == _mock_container_id(1))
    assert kafka_remove == ["rm", "-f", "-v", _mock_container_id(1)]
    assert json.loads((Path(tmp_path) / "docker-cleanup.json").read_text())["cleanup_complete"] is True


def test_docker_cleanup_preserves_named_volumes_and_fails_safely(monkeypatch, tmp_path):
    fake = _DockerSubprocess(named_volume=True)
    receipt = _run_mocked_docker_cleanup(monkeypatch, tmp_path, fake)
    assert receipt["status"] == "failed"
    assert "project-kafka-data" in fake.volumes
    kafka_remove = next(args for args in fake.commands if args[:2] == ["rm", "-f"] and args[-1] == _mock_container_id(1))
    assert kafka_remove == ["rm", "-f", _mock_container_id(1)]
    assert any("named, external, or unexpected" in error for error in receipt["errors"])


def test_docker_cleanup_handles_containers_without_anonymous_volumes(monkeypatch, tmp_path):
    fake = _DockerSubprocess()
    kafka_name = resource_names(RUN_ID)["kafka_container"]
    kafka = fake.containers[kafka_name]
    for mount in kafka["Mounts"]:
        fake.volumes.discard(mount["Name"])
    kafka["Mounts"] = []
    kafka["Config"]["Volumes"] = {}
    receipt = _run_mocked_docker_cleanup(monkeypatch, tmp_path, fake)
    assert receipt["status"] == "passed"
    assert receipt["captured_anonymous_volumes"] == []


def test_docker_cleanup_requires_exact_task_ownership_before_deleting(monkeypatch, tmp_path):
    fake = _DockerSubprocess()
    kafka_name = resource_names(RUN_ID)["kafka_container"]
    fake.containers[kafka_name]["Config"]["Labels"]["codex.run"] = "c8b-20261013t151901z-abc123ef"
    receipt = _run_mocked_docker_cleanup(monkeypatch, tmp_path, fake)
    assert receipt["status"] == "failed"
    assert _mock_container_id(1) in receipt["remaining"]["containers"]
    assert not any(args[0] == "rm" and args[-1] == _mock_container_id(1) for args in fake.commands)


def test_docker_cleanup_requires_network_ownership_before_deleting(monkeypatch, tmp_path):
    fake = _DockerSubprocess()
    fake.networks[resource_names(RUN_ID)["network"]]["Labels"]["codex.run"] = "c8b-20261013t151901z-abc123ef"
    receipt = _run_mocked_docker_cleanup(monkeypatch, tmp_path, fake)
    assert receipt["status"] == "failed"
    assert "network-id" in receipt["remaining"]["networks"]
    assert not any(args[:2] == ["network", "rm"] for args in fake.commands)


def test_docker_cleanup_removes_stopped_container_after_startup_failure(monkeypatch, tmp_path):
    fake = _DockerSubprocess()
    fake.containers[resource_names(RUN_ID)["kafka_container"]]["State"]["Status"] = "exited"
    receipt = _run_mocked_docker_cleanup(monkeypatch, tmp_path, fake)
    assert receipt["status"] == "passed"
    assert all(container["removed"] for container in receipt["containers"] if container["present_before_cleanup"])


def test_docker_cleanup_is_idempotent_when_called_twice(monkeypatch, tmp_path):
    fake = _DockerSubprocess()
    first = _run_mocked_docker_cleanup(monkeypatch, tmp_path, fake)
    rm_count = sum(args[0] == "rm" for args in fake.commands)
    second = cleanup_task_docker_resources(RUN_ID, evidence_dir=Path(tmp_path))
    assert first["status"] == second["status"] == "passed"
    assert sum(args[0] == "rm" for args in fake.commands) == rm_count
    assert len(second["captured_anonymous_volumes"]) == 3


def test_second_cleanup_still_detects_a_volume_leaked_by_the_first(monkeypatch, tmp_path):
    fake = _DockerSubprocess(leaked_volumes={f"{3:064x}"})
    first = _run_mocked_docker_cleanup(monkeypatch, tmp_path, fake)
    second = cleanup_task_docker_resources(RUN_ID, evidence_dir=Path(tmp_path))
    assert first["status"] == second["status"] == "failed"
    assert f"{3:064x}" in second["remaining"]["anonymous_volumes"]


@pytest.mark.parametrize("leak_kind", ["volume", "container", "network"])
def test_docker_cleanup_verification_detects_remaining_resources(monkeypatch, tmp_path, leak_kind):
    kwargs = {
        "leaked_volumes": {f"{3:064x}"} if leak_kind == "volume" else (),
        "leaked_containers": {_mock_container_id(1)} if leak_kind == "container" else (),
        "leaked_networks": {"network-id"} if leak_kind == "network" else (),
    }
    fake = _DockerSubprocess(**kwargs)
    receipt = _run_mocked_docker_cleanup(monkeypatch, tmp_path, fake)
    assert receipt["status"] == "failed"
    if leak_kind == "volume":
        assert f"{3:064x}" in receipt["remaining"]["anonymous_volumes"]
    elif leak_kind == "container":
        assert _mock_container_id(1) in receipt["remaining"]["containers"]
    else:
        assert "network-id" in receipt["remaining"]["networks"]


def test_docker_cleanup_handles_previously_removed_resources(monkeypatch, tmp_path):
    fake = _DockerSubprocess()
    fake.containers.clear()
    fake.networks.clear()
    fake.volumes.intersection_update({fake.project_volume})
    receipt = _run_mocked_docker_cleanup(monkeypatch, tmp_path, fake)
    assert receipt["status"] == "passed"
    assert receipt["captured_anonymous_volumes"] == []


def test_docker_cleanup_does_not_reuse_volume_ids_from_another_run(monkeypatch, tmp_path):
    fake = _DockerSubprocess()
    fake.containers.clear()
    fake.networks.clear()
    fake.volumes.intersection_update({fake.project_volume})
    (Path(tmp_path) / "docker-cleanup.json").write_text(
        json.dumps(
            {
                "run_id": "c8b-20261013t151901z-abc123ef",
                "captured_anonymous_volumes": [
                    {"name": f"{7:064x}", "container_ids": [_mock_container_id(7)]}
                ],
            }
        )
    )
    receipt = _run_mocked_docker_cleanup(monkeypatch, tmp_path, fake)
    assert receipt["status"] == "failed"
    assert receipt["captured_anonymous_volumes"] == []
    assert any("different run ID" in error for error in receipt["errors"])


def test_docker_cleanup_preserves_shared_anonymous_volume_and_other_project_resources(monkeypatch, tmp_path):
    fake = _DockerSubprocess()
    kafka = fake.containers[resource_names(RUN_ID)["kafka_container"]]
    shared = kafka["Mounts"][0]["Name"]
    fake.project_container["Mounts"].append(
        {"Type": "volume", "Name": shared, "Destination": "/external", "RW": True}
    )
    before_project = json.dumps(fake.project_container, sort_keys=True)
    before_named_volume = fake.project_volume
    receipt = _run_mocked_docker_cleanup(monkeypatch, tmp_path, fake)
    assert receipt["status"] == "failed"
    assert shared in fake.volumes
    assert json.dumps(fake.project_container, sort_keys=True) == before_project
    assert before_named_volume in fake.volumes
    kafka_remove = next(args for args in fake.commands if args[:2] == ["rm", "-f"] and args[-1] == _mock_container_id(1))
    assert kafka_remove == ["rm", "-f", _mock_container_id(1)]


def test_cleanup_failure_cannot_finalize_a_pass_report(tmp_path):
    report = _report()
    report["cleanup_status"] = {"mongodb_database_dropped": True, "spark_query_stopped": True}
    report_path = Path(tmp_path) / "report.json"
    cleanup_path = Path(tmp_path) / "docker-cleanup.json"
    report_path.write_text(json.dumps(report))
    cleanup_path.write_text(json.dumps({"run_id": RUN_ID, "status": "failed", "cleanup_complete": False, "remaining": {"anonymous_volumes": ["leaked"]}, "errors": ["leaked volume"]}))
    rc = finalize_report(
        SimpleNamespace(
            run_id=RUN_ID,
            report=str(report_path),
            docker_cleanup_report=str(cleanup_path),
            cleaned="yes",
            experiment_exit_code=0,
        )
    )
    final_report = json.loads(report_path.read_text())
    assert rc == 1
    assert final_report["status"] == "failed"
    assert final_report["cleanup_status"]["docker_anonymous_volumes_removed"] is False


def test_failed_experiment_cannot_finalize_a_pass_report_even_after_clean_teardown(tmp_path):
    report = _report()
    report["cleanup_status"] = {"mongodb_database_dropped": True, "spark_query_stopped": True}
    report_path = Path(tmp_path) / "report.json"
    cleanup_path = Path(tmp_path) / "docker-cleanup.json"
    report_path.write_text(json.dumps(report))
    cleanup_path.write_text(
        json.dumps(
            {
                "run_id": RUN_ID,
                "status": "passed",
                "cleanup_complete": True,
                "captured_anonymous_volumes": [{"name": f"{1:064x}", "absent_after_cleanup": True}],
                "remaining": {"containers": [], "networks": [], "anonymous_volumes": []},
                "errors": [],
            }
        )
    )
    rc = finalize_report(
        SimpleNamespace(
            run_id=RUN_ID,
            report=str(report_path),
            docker_cleanup_report=str(cleanup_path),
            cleaned="yes",
            experiment_exit_code=42,
        )
    )
    assert rc == 1
    assert json.loads(report_path.read_text())["status"] == "failed"


def test_cleanup_commands_never_use_global_pruning(monkeypatch, tmp_path):
    fake = _DockerSubprocess()
    receipt = _run_mocked_docker_cleanup(monkeypatch, tmp_path, fake)
    assert receipt["status"] == "passed"
    commands = [" ".join(args) for args in fake.commands]
    assert not any("volume prune" in command or "system prune" in command or "compose down" in command for command in commands)
