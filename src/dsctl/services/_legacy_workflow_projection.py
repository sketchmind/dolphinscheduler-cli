from __future__ import annotations

import heapq
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from dsctl.support.yaml_io import compact_yaml_mapping, dump_yaml_document

if TYPE_CHECKING:
    from dsctl.support.yaml_io import JsonObject, JsonValue
    from dsctl.upstream.definition_models import ScheduleView, WorkflowScope
    from dsctl.upstream.legacy_workflow_graph import (
        DecodedLegacyTask,
        DecodedLegacyWorkflowGraph,
    )
    from dsctl.upstream.workflows import LegacyWorkflowDefinitionSnapshot


@dataclass(frozen=True, slots=True)
class LegacyWorkflowReadProjection:
    """Pure user-facing projections of one decoded DS 1.3 workflow graph."""

    scope: WorkflowScope
    snapshot: LegacyWorkflowDefinitionSnapshot
    graph: DecodedLegacyWorkflowGraph
    attached_schedule: ScheduleView | None = None

    def yaml_text(self) -> str:
        """Render canonical YAML without exposing legacy string task ids."""
        document = self.graph.workflow_document(
            name=self._workflow_name(),
            project=self.scope.project.name,
            description=self.snapshot.description,
            release_state=self.snapshot.release_state or "OFFLINE",
        )
        schedule = _schedule_document(self.attached_schedule)
        if schedule is not None:
            document["schedule"] = schedule
        return dump_yaml_document(document)

    def describe(self) -> JsonObject:
        """Describe the graph with only the identities native to DS 1.3."""
        task_by_name = {task.name: task for task in self.graph.tasks}
        return {
            "workflow": self.scope.view.to_data(
                attached_schedule=self.attached_schedule
            ),
            "tasks": [
                {
                    "id": task.id,
                    "name": task.name,
                    "type": task.type,
                    "taskParams": _thaw(task.document["task_params"]),
                    "dependsOn": list(task.depends_on),
                }
                for task in self.graph.tasks
            ],
            "relations": [
                {
                    "preTaskId": task_by_name[predecessor].id,
                    "preTaskName": predecessor,
                    "postTaskId": task_by_name[successor].id,
                    "postTaskName": successor,
                }
                for predecessor, successor in self.graph.edges
            ],
        }

    def digest(self) -> JsonObject:
        """Build a compact graph summary using native string task ids."""
        workflow = self.scope.view.to_data(attached_schedule=self.attached_schedule)
        task_by_name = {task.name: task for task in self.graph.tasks}
        upstream = {task.name: set(task.depends_on) for task in self.graph.tasks}
        downstream: dict[str, set[str]] = {
            task.name: set() for task in self.graph.tasks
        }
        for predecessor, successor in self.graph.edges:
            downstream[predecessor].add(successor)
        ordered_names = _ordered_task_names(
            task_names=tuple(task_by_name),
            upstream=upstream,
            downstream=downstream,
        )
        order_index = {
            task_name: index for index, task_name in enumerate(ordered_names)
        }
        roots = [name for name in ordered_names if not upstream[name]]
        leaves = [name for name in ordered_names if not downstream[name]]
        isolated = [
            name
            for name in ordered_names
            if not upstream[name] and not downstream[name]
        ]
        return {
            "workflow": {
                key: workflow[key]
                for key in (
                    "id",
                    "name",
                    "version",
                    "projectId",
                    "projectName",
                    "description",
                    "releaseState",
                    "scheduleReleaseState",
                    "timeout",
                    "schedule",
                )
            },
            "taskCount": len(self.graph.tasks),
            "relationCount": len(self.graph.edges),
            "taskTypeCounts": _task_type_counts(self.graph),
            "globalParamNames": _global_param_names(self.graph),
            "rootTasks": [_task_ref(task_by_name[name]) for name in roots],
            "leafTasks": [_task_ref(task_by_name[name]) for name in leaves],
            "isolatedTasks": [_task_ref(task_by_name[name]) for name in isolated],
            "tasks": [
                {
                    "id": task_by_name[name].id,
                    "name": name,
                    "taskType": task_by_name[name].type,
                    "upstreamTasks": [
                        _task_ref(task_by_name[upstream_name])
                        for upstream_name in sorted(
                            upstream[name], key=order_index.__getitem__
                        )
                    ],
                    "downstreamTasks": [
                        _task_ref(task_by_name[downstream_name])
                        for downstream_name in sorted(
                            downstream[name], key=order_index.__getitem__
                        )
                    ],
                    "isRoot": name in roots,
                    "isLeaf": name in leaves,
                }
                for name in ordered_names
            ],
        }

    def _workflow_name(self) -> str:
        name = self.snapshot.name or self.scope.workflow.name
        if name is None:
            msg = "Legacy workflow detail did not contain a workflow name"
            raise ValueError(msg)
        return name


def _thaw(value: JsonValue) -> JsonValue:
    if isinstance(value, Mapping):
        return {key: _thaw(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_thaw(item) for item in value]
    return value


def _schedule_document(schedule: ScheduleView | None) -> JsonObject | None:
    if schedule is None:
        return None
    cron = _non_empty_text(schedule.crontab)
    timezone = _non_empty_text(schedule.timezone_id)
    start = _non_empty_text(schedule.start_time)
    end = _non_empty_text(schedule.end_time)
    if cron is None or start is None or end is None:
        return None
    return compact_yaml_mapping(
        {
            "cron": cron,
            "timezone": timezone,
            "start": start,
            "end": end,
            "failure_strategy": schedule.failure_strategy,
            "priority": schedule.workflow_instance_priority,
            "release_state": schedule.release_state,
        }
    )


def _non_empty_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def _ordered_task_names(
    *,
    task_names: tuple[str, ...],
    upstream: Mapping[str, set[str]],
    downstream: Mapping[str, set[str]],
) -> list[str]:
    original_order = {name: index for index, name in enumerate(task_names)}
    remaining = {name: set(upstream[name]) for name in task_names}
    ready = [(original_order[name], name) for name in task_names if not remaining[name]]
    heapq.heapify(ready)
    ordered: list[str] = []
    while ready:
        _, name = heapq.heappop(ready)
        ordered.append(name)
        for successor in sorted(downstream[name], key=original_order.__getitem__):
            remaining[successor].discard(name)
            if not remaining[successor]:
                heapq.heappush(ready, (original_order[successor], successor))
    return ordered if len(ordered) == len(task_names) else list(task_names)


def _task_ref(task: DecodedLegacyTask) -> JsonObject:
    return {"id": task.id, "name": task.name}


def _task_type_counts(graph: DecodedLegacyWorkflowGraph) -> dict[str, int]:
    counts: dict[str, int] = {}
    for task in graph.tasks:
        key = task.type or "UNKNOWN"
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def _global_param_names(graph: DecodedLegacyWorkflowGraph) -> list[str]:
    names = {
        prop
        for value in graph.global_params
        if isinstance(value, Mapping)
        and isinstance((prop := value.get("prop")), str)
        and prop
    }
    return sorted(names)


__all__ = ["LegacyWorkflowReadProjection"]
