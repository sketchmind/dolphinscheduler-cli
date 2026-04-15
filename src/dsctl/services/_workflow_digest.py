from __future__ import annotations

import heapq
from typing import TYPE_CHECKING, TypedDict

if TYPE_CHECKING:
    from dsctl.services._serialization import TaskData
    from dsctl.services._workflow_render import ScheduleData, WorkflowDescribeData


class WorkflowDigestTaskRefData(TypedDict):
    """Compact task reference used inside workflow digest payloads."""

    code: int
    name: str | None


class WorkflowDigestTaskData(TypedDict):
    """One compact task entry emitted by `workflow digest`."""

    code: int
    name: str | None
    taskType: str | None
    upstreamTasks: list[WorkflowDigestTaskRefData]
    downstreamTasks: list[WorkflowDigestTaskRefData]
    isRoot: bool
    isLeaf: bool


class WorkflowDigestWorkflowData(TypedDict):
    """Compact workflow metadata emitted by `workflow digest`."""

    code: int
    name: str | None
    version: int | None
    projectCode: int
    projectName: str | None
    description: str | None
    releaseState: str | None
    scheduleReleaseState: str | None
    executionType: str | None
    timeout: int
    schedule: ScheduleData | None


class WorkflowDigestData(TypedDict):
    """Compact workflow graph summary for AI-friendly inspection."""

    workflow: WorkflowDigestWorkflowData
    taskCount: int
    relationCount: int
    taskTypeCounts: dict[str, int]
    globalParamNames: list[str]
    rootTasks: list[WorkflowDigestTaskRefData]
    leafTasks: list[WorkflowDigestTaskRefData]
    isolatedTasks: list[WorkflowDigestTaskRefData]
    tasks: list[WorkflowDigestTaskData]


def digest_workflow(description: WorkflowDescribeData) -> WorkflowDigestData:
    """Build one compact workflow graph summary from a full DAG description."""
    workflow = description["workflow"]
    tasks = description["tasks"]
    relations = description["relations"]
    task_by_code: dict[int, TaskData] = {task["code"]: task for task in tasks}
    original_order = {task["code"]: index for index, task in enumerate(tasks)}
    upstream_codes: dict[int, set[int]] = {task["code"]: set() for task in tasks}
    downstream_codes: dict[int, set[int]] = {task["code"]: set() for task in tasks}
    relation_count = 0

    for relation in relations:
        pre_code = relation["preTaskCode"]
        post_code = relation["postTaskCode"]
        if pre_code not in task_by_code or post_code not in task_by_code:
            continue
        upstream_codes[post_code].add(pre_code)
        downstream_codes[pre_code].add(post_code)
        relation_count += 1

    ordered_codes = _ordered_task_codes(
        task_by_code=task_by_code,
        upstream_codes=upstream_codes,
        downstream_codes=downstream_codes,
        original_order=original_order,
    )
    ordered_index = {code: index for index, code in enumerate(ordered_codes)}
    task_type_counts = _task_type_counts(tasks)
    root_codes = [code for code in ordered_codes if not upstream_codes.get(code)]
    leaf_codes = [code for code in ordered_codes if not downstream_codes.get(code)]
    isolated_codes = [
        code
        for code in ordered_codes
        if not upstream_codes.get(code) and not downstream_codes.get(code)
    ]
    root_code_set = set(root_codes)
    leaf_code_set = set(leaf_codes)

    return {
        "workflow": {
            "code": workflow["code"],
            "name": workflow["name"],
            "version": workflow["version"],
            "projectCode": workflow["projectCode"],
            "projectName": workflow["projectName"],
            "description": workflow["description"],
            "releaseState": workflow["releaseState"],
            "scheduleReleaseState": workflow["scheduleReleaseState"],
            "executionType": workflow["executionType"],
            "timeout": workflow["timeout"],
            "schedule": workflow["schedule"],
        },
        "taskCount": len(tasks),
        "relationCount": relation_count,
        "taskTypeCounts": task_type_counts,
        "globalParamNames": sorted((workflow["globalParamMap"] or {}).keys()),
        "rootTasks": [_task_ref(task_by_code[code]) for code in root_codes],
        "leafTasks": [_task_ref(task_by_code[code]) for code in leaf_codes],
        "isolatedTasks": [_task_ref(task_by_code[code]) for code in isolated_codes],
        "tasks": [
            {
                "code": task_by_code[code]["code"],
                "name": task_by_code[code]["name"],
                "taskType": task_by_code[code]["taskType"],
                "upstreamTasks": [
                    _task_ref(task_by_code[upstream_code])
                    for upstream_code in sorted(
                        upstream_codes[code],
                        key=lambda task_code: ordered_index[task_code],
                    )
                ],
                "downstreamTasks": [
                    _task_ref(task_by_code[downstream_code])
                    for downstream_code in sorted(
                        downstream_codes[code],
                        key=lambda task_code: ordered_index[task_code],
                    )
                ],
                "isRoot": code in root_code_set,
                "isLeaf": code in leaf_code_set,
            }
            for code in ordered_codes
        ],
    }


def _ordered_task_codes(
    *,
    task_by_code: dict[int, TaskData],
    upstream_codes: dict[int, set[int]],
    downstream_codes: dict[int, set[int]],
    original_order: dict[int, int],
) -> list[int]:
    remaining_upstream = {
        code: set(upstream_codes.get(code, set())) for code in task_by_code
    }
    ready: list[tuple[int, int]] = []
    queued: set[int] = set()
    visited: set[int] = set()
    for code, upstream in remaining_upstream.items():
        if not upstream:
            heapq.heappush(ready, (original_order[code], code))
            queued.add(code)
    ordered: list[int] = []
    while ready:
        _, code = heapq.heappop(ready)
        queued.discard(code)
        if code in visited:
            continue
        visited.add(code)
        ordered.append(code)
        for downstream_code in sorted(
            downstream_codes.get(code, set()),
            key=lambda task_code: original_order[task_code],
        ):
            upstream = remaining_upstream[downstream_code]
            upstream.discard(code)
            if (
                not upstream
                and downstream_code not in visited
                and downstream_code not in queued
            ):
                heapq.heappush(
                    ready,
                    (original_order[downstream_code], downstream_code),
                )
                queued.add(downstream_code)

    if len(ordered) != len(task_by_code):
        return sorted(task_by_code, key=lambda task_code: original_order[task_code])
    return ordered


def _task_ref(task: TaskData) -> WorkflowDigestTaskRefData:
    return {
        "code": task["code"],
        "name": task["name"],
    }


def _task_type_counts(tasks: list[TaskData]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for task in tasks:
        task_type = task["taskType"]
        key = task_type if isinstance(task_type, str) and task_type else "UNKNOWN"
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))
