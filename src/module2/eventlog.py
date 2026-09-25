"""Parse completed Spark 3.5 JSON logs with job-group attribution.

JobStart includes skipped cached dependencies. Only StageSubmitted events attribute
executed work; referenced-only stages are reported separately. Useful I/O uses
successful task indices from the final successful attempt. Failed/discarded task
attempts are counted separately, without charging their bytes as useful I/O.
"""

import json
from collections import defaultdict


def parse_event_logs(directory):
    groups = defaultdict(set)
    submitted = defaultdict(set)
    tasks = defaultdict(list)
    stages, jobs, outcomes = {}, {}, {}
    completed = False
    for path in sorted(directory.iterdir()):
        if not path.is_file() or path.name.startswith("."):
            continue
        if path.name.endswith(".inprogress"):
            raise ValueError("Spark must stop before parsing event logs")
        with path.open(encoding="utf-8") as stream:
            for line in stream:
                event = json.loads(line)
                kind = event.get("Event")
                if kind == "SparkListenerApplicationEnd":
                    completed = True
                elif kind == "SparkListenerJobStart":
                    group = (event.get("Properties") or {}).get("spark.jobGroup.id")
                    if group:
                        groups[group].update(event["Stage IDs"])
                        jobs[event["Job ID"]] = group
                elif kind == "SparkListenerJobEnd":
                    outcomes[event["Job ID"]] = event["Job Result"]["Result"]
                elif kind == "SparkListenerStageSubmitted":
                    stage = event["Stage Info"]
                    group = (event.get("Properties") or {}).get("spark.jobGroup.id")
                    if group:
                        submitted[group].add((stage["Stage ID"], stage["Stage Attempt ID"]))
                elif kind == "SparkListenerStageCompleted":
                    stage = event["Stage Info"]
                    if not stage.get("Failure Reason"):
                        stages[stage["Stage ID"]] = max(
                            stages.get(stage["Stage ID"], -1), stage["Stage Attempt ID"]
                        )
                elif kind == "SparkListenerTaskEnd":
                    tasks[(event["Stage ID"], event["Stage Attempt ID"])].append(event)
    if not completed:
        raise ValueError("No completed Spark application in event log")
    for job, group in jobs.items():
        if outcomes.get(job) != "JobSucceeded":
            raise ValueError(f"Group {group}: job {job} did not succeed")
    output = {}
    for group, references in groups.items():
        executed = submitted[group]
        executed_ids = {stage for stage, _ in executed}
        result = dict(
            stages=len(executed_ids),
            referenced_stages=len(references),
            skipped_stages=len(references - executed_ids),
            tasks=0,
            failed_task_attempts=0,
            discarded_task_attempts=0,
            input_bytes=0,
            input_records=0,
            shuffle_read_bytes=0,
            shuffle_write_bytes=0,
            executor_run_time_ms=0,
        )
        for stage in executed_ids:
            if stage not in stages:
                raise ValueError(f"Group {group}: no successful attempt for submitted stage {stage}")
        seen = set()
        for stage, attempt in sorted(executed):
            for event in tasks[(stage, attempt)]:
                success = event.get("Task End Reason", {}).get("Reason") == "Success"
                if not success:
                    result["failed_task_attempts"] += 1
                index = (stage, event["Task Info"]["Index"])
                if not success or attempt != stages[stage] or index in seen:
                    result["discarded_task_attempts"] += 1
                    continue
                seen.add(index)
                metrics = event.get("Task Metrics", {})
                inp = metrics.get("Input Metrics", {})
                read = metrics.get("Shuffle Read Metrics", {})
                write = metrics.get("Shuffle Write Metrics", {})
                result["tasks"] += 1
                result["input_bytes"] += inp.get("Bytes Read", 0)
                result["input_records"] += inp.get("Records Read", 0)
                result["shuffle_read_bytes"] += read.get("Remote Bytes Read", 0) + read.get(
                    "Local Bytes Read", 0
                )
                result["shuffle_write_bytes"] += write.get("Shuffle Bytes Written", 0)
                result["executor_run_time_ms"] += metrics.get("Executor Run Time", 0)
        output[group] = result
    return output


def attach_metrics(value, metrics):
    if isinstance(value, dict):
        if "job_group" in value:
            group = value["job_group"]
            if group not in metrics:
                raise ValueError(f"No Spark task evidence for {group}")
            value["task_metrics"] = metrics[group]
        for child in list(value.values()):
            attach_metrics(child, metrics)
    elif isinstance(value, list):
        for child in value:
            attach_metrics(child, metrics)
