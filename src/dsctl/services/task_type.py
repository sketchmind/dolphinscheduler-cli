from __future__ import annotations

from typing import TYPE_CHECKING, TypedDict

from dsctl.cli_surface import TASK_TYPE_RESOURCE
from dsctl.models.task_spec import canonical_task_type, supported_typed_task_types
from dsctl.output import CommandResult, require_json_object
from dsctl.services._serialization import require_resource_text
from dsctl.services.runtime import ServiceRuntime, run_with_service_runtime
from dsctl.services.task_authoring import (
    task_type_schema_result,
    task_type_summary_result,
)
from dsctl.services.template import (
    generic_task_template_types,
    supported_task_template_types,
)

if TYPE_CHECKING:
    from collections.abc import Iterable

    from dsctl.upstream.protocol import TaskTypeRecord


class TaskTypeData(TypedDict):
    """DS task type payload projected from `FavTaskDto`."""

    taskType: str
    isCollection: bool
    taskCategory: str


class TaskTypeCoverageData(TypedDict):
    """CLI authoring coverage against the current remote task type list."""

    taskTemplateTypes: list[str]
    typedTaskSpecs: list[str]
    genericTaskTemplateTypes: list[str]
    untemplatedTaskTypes: list[str]


class TaskTypeListData(TypedDict):
    """Stable discovery payload for `dsctl task-type list`."""

    taskTypes: list[TaskTypeData]
    count: int
    taskTypesByCategory: dict[str, list[str]]
    cliCoverage: TaskTypeCoverageData


def list_task_types_result(*, env_file: str | None = None) -> CommandResult:
    """List DS task types visible to the current user/runtime."""
    return run_with_service_runtime(
        env_file,
        _list_task_types_result,
    )


def _list_task_types_result(runtime: ServiceRuntime) -> CommandResult:
    task_types = [
        _serialize_task_type(task_type)
        for task_type in runtime.upstream.task_types.list()
    ]
    return CommandResult(
        data=require_json_object(
            _task_type_list_data(task_types),
            label="task type list data",
        ),
        resolved={"source": "favourite/taskTypes"},
    )


def _serialize_task_type(task_type: TaskTypeRecord) -> TaskTypeData:
    return {
        "taskType": require_resource_text(
            task_type.taskType,
            resource=TASK_TYPE_RESOURCE,
            field_name="taskType",
        ),
        "isCollection": bool(task_type.isCollection),
        "taskCategory": require_resource_text(
            task_type.taskCategory,
            resource=TASK_TYPE_RESOURCE,
            field_name="taskCategory",
        ),
    }


def _task_type_list_data(task_types: list[TaskTypeData]) -> TaskTypeListData:
    task_types_by_category: dict[str, list[str]] = {}
    for task_type in task_types:
        category = task_type["taskCategory"]
        task_types_by_category.setdefault(category, []).append(task_type["taskType"])

    template_types = list(supported_task_template_types())
    typed_task_specs = list(supported_typed_task_types())
    generic_template_types = list(generic_task_template_types())
    supported_template_types = set(template_types)
    untemplated_task_types = _unique_task_types(
        task_type["taskType"]
        for task_type in task_types
        if canonical_task_type(task_type["taskType"]) not in supported_template_types
    )

    return {
        "taskTypes": task_types,
        "count": len(task_types),
        "taskTypesByCategory": task_types_by_category,
        "cliCoverage": {
            "taskTemplateTypes": template_types,
            "typedTaskSpecs": typed_task_specs,
            "genericTaskTemplateTypes": generic_template_types,
            "untemplatedTaskTypes": untemplated_task_types,
        },
    }


def _unique_task_types(task_types: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    unique_task_types: list[str] = []
    for task_type in task_types:
        if task_type in seen:
            continue
        seen.add(task_type)
        unique_task_types.append(task_type)
    return unique_task_types


__all__ = [
    "list_task_types_result",
    "task_type_schema_result",
    "task_type_summary_result",
]
