from __future__ import annotations

from typing import TYPE_CHECKING, TypedDict

from dsctl.upstream.runtime_enums import (
    TASK_EXECUTION_FAILED_STATES,
    TASK_EXECUTION_PAUSED_STATES,
    TASK_EXECUTION_QUEUED_STATES,
    TASK_EXECUTION_RUNNING_STATES,
    TASK_EXECUTION_SUCCESS_STATES,
)

if TYPE_CHECKING:
    from dsctl.services._serialization import TaskInstanceData, WorkflowInstanceData


class WorkflowInstanceDigestTaskData(TypedDict):
    """Compact task-instance entry emitted by `workflow-instance digest`."""

    id: int
    taskCode: int
    name: str | None
    taskType: str | None
    state: str | None
    retryTimes: int
    host: str | None
    startTime: str | None
    endTime: str | None
    duration: str | None


class WorkflowInstanceDigestProgressData(TypedDict):
    """Aggregated progress buckets derived from DS task states."""

    running: int
    queued: int
    paused: int
    failed: int
    success: int
    other: int
    finished: int
    active: int


class WorkflowInstanceDigestData(TypedDict):
    """Compact runtime digest for one workflow instance."""

    workflowInstance: WorkflowInstanceData
    taskCount: int
    taskStateCounts: dict[str, int]
    taskTypeCounts: dict[str, int]
    progress: WorkflowInstanceDigestProgressData
    runningTasks: list[WorkflowInstanceDigestTaskData]
    queuedTasks: list[WorkflowInstanceDigestTaskData]
    failedTasks: list[WorkflowInstanceDigestTaskData]
    retriedTasks: list[WorkflowInstanceDigestTaskData]


def digest_workflow_instance(
    *,
    workflow_instance: WorkflowInstanceData,
    tasks: list[TaskInstanceData],
) -> WorkflowInstanceDigestData:
    """Build one compact runtime digest from instance and task-instance payloads."""
    progress = _progress_data(tasks)
    return {
        "workflowInstance": workflow_instance,
        "taskCount": len(tasks),
        "taskStateCounts": _task_state_counts(tasks),
        "taskTypeCounts": _task_type_counts(tasks),
        "progress": progress,
        "runningTasks": _bucket_tasks(tasks, bucket="running"),
        "queuedTasks": _bucket_tasks(tasks, bucket="queued"),
        "failedTasks": _bucket_tasks(tasks, bucket="failed"),
        "retriedTasks": [
            _compact_task(task) for task in tasks if task["retryTimes"] > 0
        ],
    }


def _task_state_counts(tasks: list[TaskInstanceData]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for task in tasks:
        state = task["state"] or "UNKNOWN"
        counts[state] = counts.get(state, 0) + 1
    return dict(sorted(counts.items()))


def _task_type_counts(tasks: list[TaskInstanceData]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for task in tasks:
        task_type = task["taskType"] or "UNKNOWN"
        counts[task_type] = counts.get(task_type, 0) + 1
    return dict(sorted(counts.items()))


def _progress_data(tasks: list[TaskInstanceData]) -> WorkflowInstanceDigestProgressData:
    running = 0
    queued = 0
    paused = 0
    failed = 0
    success = 0
    other = 0
    for task in tasks:
        bucket = _task_bucket(task["state"])
        if bucket == "running":
            running += 1
        elif bucket == "queued":
            queued += 1
        elif bucket == "paused":
            paused += 1
        elif bucket == "failed":
            failed += 1
        elif bucket == "success":
            success += 1
        else:
            other += 1
    finished = failed + success
    active = len(tasks) - finished
    return {
        "running": running,
        "queued": queued,
        "paused": paused,
        "failed": failed,
        "success": success,
        "other": other,
        "finished": finished,
        "active": active,
    }


def _bucket_tasks(
    tasks: list[TaskInstanceData],
    *,
    bucket: str,
) -> list[WorkflowInstanceDigestTaskData]:
    return [
        _compact_task(task) for task in tasks if _task_bucket(task["state"]) == bucket
    ]


def _compact_task(task: TaskInstanceData) -> WorkflowInstanceDigestTaskData:
    return {
        "id": task["id"],
        "taskCode": task["taskCode"],
        "name": task["name"],
        "taskType": task["taskType"],
        "state": task["state"],
        "retryTimes": task["retryTimes"],
        "host": task["host"],
        "startTime": task["startTime"],
        "endTime": task["endTime"],
        "duration": task["duration"],
    }


def _task_bucket(state: str | None) -> str:
    if state in TASK_EXECUTION_RUNNING_STATES:
        return "running"
    if state in TASK_EXECUTION_QUEUED_STATES:
        return "queued"
    if state in TASK_EXECUTION_PAUSED_STATES:
        return "paused"
    if state in TASK_EXECUTION_FAILED_STATES:
        return "failed"
    if state in TASK_EXECUTION_SUCCESS_STATES:
        return "success"
    return "other"
