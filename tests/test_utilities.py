import json
import pytest
from src.module2.config import Config
from src.module2.benchmarks import summarize, verify_join, verify_pruning
from src.module2.eventlog import parse_event_logs, attach_metrics


def test_paths_and_protocol():
    assert (
        Config(root="hdfs://namenode:8020/instacart/").table("products")
        == "hdfs://namenode:8020/instacart/curated/dimensions/products"
    )
    assert Config(root="file:///tmp/fixture").features() == "file:///tmp/fixture/features/user_features"
    with pytest.raises(ValueError):
        Config(repetitions=1)
    with pytest.raises(ValueError):
        Config(warmups=0)


def test_benchmark_checks():
    assert summarize([{"seconds": n, "checksum": "equal"} for n in [9, 1, 2]])["median_seconds"] == 2
    with pytest.raises(ValueError, match="differ"):
        summarize([{"seconds": 1, "checksum": str(i)} for i in range(2)])
    verify_join("SortMergeJoin SortMergeJoin", "SortMergeJoin")
    with pytest.raises(ValueError):
        verify_join("BroadcastHashJoin", "SortMergeJoin")
    verify_pruning("PartitionFilters: [], PushedFilters: []", False)
    with pytest.raises(ValueError):
        verify_pruning("PartitionFilters: [], PushedFilters: []", True)


def test_event_metrics_retry_speculation_and_attribution(tmp_path):
    events = [
        {
            "Event": "SparkListenerJobStart",
            "Job ID": 0,
            "Stage IDs": [1, 2],
            "Properties": {"spark.jobGroup.id": "one"},
        }
    ]
    for attempt in [0, 1]:
        events.append(
            {
                "Event": "SparkListenerStageSubmitted",
                "Stage Info": {"Stage ID": 1, "Stage Attempt ID": attempt},
                "Properties": {"spark.jobGroup.id": "one"},
            }
        )
        events.append(
            {
                "Event": "SparkListenerStageCompleted",
                "Stage Info": {"Stage ID": 1, "Stage Attempt ID": attempt},
            }
        )
        for index in [0, 0, 1]:
            events.append(
                {
                    "Event": "SparkListenerTaskEnd",
                    "Stage ID": 1,
                    "Stage Attempt ID": attempt,
                    "Task Info": {"Index": index},
                    "Task End Reason": {"Reason": "Success"},
                    "Task Metrics": {
                        "Input Metrics": {"Bytes Read": 10, "Records Read": 2},
                        "Shuffle Read Metrics": {"Remote Bytes Read": 3, "Local Bytes Read": 4},
                        "Shuffle Write Metrics": {"Shuffle Bytes Written": 5},
                        "Executor Run Time": 6,
                    },
                }
            )
    events.append({"Event": "SparkListenerJobEnd", "Job ID": 0, "Job Result": {"Result": "JobSucceeded"}})
    events.append({"Event": "SparkListenerApplicationEnd"})
    (tmp_path / "eventlog").write_text("\n".join(map(json.dumps, events)), encoding="utf-8")
    metrics = parse_event_logs(tmp_path)
    assert metrics["one"] == {
        "stages": 1,
        "referenced_stages": 2,
        "skipped_stages": 1,
        "discarded_task_attempts": 4,
        "tasks": 2,
        "failed_task_attempts": 0,
        "input_bytes": 20,
        "input_records": 4,
        "shuffle_read_bytes": 14,
        "shuffle_write_bytes": 10,
        "executor_run_time_ms": 12,
    }
    report = {"trials": [{"job_group": "one"}]}
    attach_metrics(report, metrics)
    assert report["trials"][0]["task_metrics"]["tasks"] == 2
    with pytest.raises(ValueError):
        attach_metrics({"job_group": "missing"}, metrics)


def test_incomplete_log_rejected(tmp_path):
    (tmp_path / "log.inprogress").write_text("")
    with pytest.raises(ValueError, match="stop"):
        parse_event_logs(tmp_path)
