"""Pure DS-native workflow graph encoding below authoring orchestration."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, TypedDict

from dsctl.cli_surface import WORKFLOW_RESOURCE
from dsctl.errors import ApiTransportError
from dsctl.models.common import DataType, Direct
from dsctl.output import require_json_object, require_json_value
from dsctl.upstream.runtime_enums import (
    TASK_EXECUTE_TYPE_BATCH_VALUE,
    task_execute_type_value,
)
from dsctl.upstream.task_parameter_projection import (
    TaskRefIndex,
    encode_task_parameters,
)
from dsctl.upstream.task_settings import (
    encode_task_node_fields,
    task_timeout_settings,
)

if TYPE_CHECKING:
    from dsctl.models.workflow_spec import WorkflowSpec, WorkflowTaskSpec
    from dsctl.support.json_types import JsonObject, JsonValue
    from dsctl.upstream.task_parameter_projection import (
        ProjectionSource,
        TaskResourceRefIndex,
        TaskWorkflowRefIndex,
    )


class WorkflowCreatePayload(TypedDict):
    """Compiled legacy workflow-definition form payload."""

    name: str
    description: str | None
    globalParams: str
    locations: str
    timeout: int
    taskRelationJson: str
    taskDefinitionJson: str
    executionType: str | None


class WorkflowUpdatePayload(WorkflowCreatePayload):
    """Compiled legacy workflow-definition form payload for whole-definition edits."""

    releaseState: str | None


def render_workflow_graph(
    spec: WorkflowSpec,
    *,
    task_codes: Mapping[str, int],
    task_versions: Mapping[str, int],
    main_task_ids: Mapping[int, int] | None = None,
    task_cache_by_name: Mapping[str, str | None],
    edges: list[tuple[str, str]],
    levels: Mapping[str, int],
    task_cache_requested: bool,
    profile_version: str,
    projection_sources: Mapping[str, ProjectionSource],
    resource_refs: TaskResourceRefIndex,
    workflow_refs: TaskWorkflowRefIndex,
) -> WorkflowCreatePayload:
    """Render one prepared workflow graph with fully bound task identities."""
    return {
        "name": spec.workflow.name,
        "description": spec.workflow.description,
        "globalParams": _global_params_json(spec),
        "locations": _workflow_locations_json(spec.tasks, task_codes, levels),
        "timeout": spec.workflow.timeout,
        "taskRelationJson": _task_relations_json(
            spec.tasks,
            task_codes,
            task_versions,
            edges=edges,
        ),
        "taskDefinitionJson": _task_definitions_json(
            spec.tasks,
            task_codes,
            task_versions,
            main_task_ids=main_task_ids,
            task_cache_by_name=task_cache_by_name,
            task_cache_requested=task_cache_requested,
            profile_version=profile_version,
            projection_sources=projection_sources,
            resource_refs=resource_refs,
            workflow_refs=workflow_refs,
        ),
        "executionType": spec.workflow.execution_type.value,
    }


def _global_params_json(spec: WorkflowSpec) -> str:
    global_params = spec.workflow.global_params
    if global_params is None:
        return "[]"
    if isinstance(global_params, Mapping):
        properties = [
            _global_param_json_object(
                prop=key,
                value=value,
                direct=Direct.IN,
                data_type=DataType.VARCHAR,
            )
            for key, value in global_params.items()
        ]
    else:
        properties = [
            _global_param_json_object(
                prop=parameter.prop,
                value=parameter.value,
                direct=parameter.direct,
                data_type=parameter.type,
            )
            for parameter in global_params
        ]
    return _json_text(properties)


def _global_param_json_object(
    *,
    prop: str,
    value: str | None,
    direct: Direct,
    data_type: DataType,
) -> JsonObject:
    property_data: JsonObject = {
        "prop": prop,
        "direct": direct.value,
        "type": data_type.value,
    }
    if value is not None:
        property_data["value"] = value
    return property_data


def _workflow_locations_json(
    tasks: list[WorkflowTaskSpec],
    task_codes: Mapping[str, int],
    levels: Mapping[str, int],
) -> str:
    rows_by_level: dict[int, int] = {}
    locations: list[dict[str, int]] = []
    for task in tasks:
        level = levels[task.name]
        row = rows_by_level.get(level, 0)
        rows_by_level[level] = row + 1
        locations.append(
            {
                "taskCode": task_codes[task.name],
                "x": 80 + (level * 260),
                "y": 80 + (row * 140),
            }
        )
    return _json_text(locations)


def _task_relations_json(
    tasks: list[WorkflowTaskSpec],
    task_codes: Mapping[str, int],
    task_versions: Mapping[str, int],
    *,
    edges: list[tuple[str, str]],
) -> str:
    indegree: dict[str, int] = {task.name: 0 for task in tasks}
    for _, successor in edges:
        indegree[successor] += 1
    relations: list[dict[str, int | str]] = [
        {
            "name": "",
            "preTaskCode": 0,
            "preTaskVersion": 0,
            "postTaskCode": task_codes[task.name],
            "postTaskVersion": task_versions[task.name],
            "conditionType": 0,
            "conditionParams": "{}",
        }
        for task in tasks
        if indegree[task.name] == 0
    ]
    relations.extend(
        {
            "name": "",
            "preTaskCode": task_codes[predecessor],
            "preTaskVersion": task_versions[predecessor],
            "postTaskCode": task_codes[successor],
            "postTaskVersion": task_versions[successor],
            "conditionType": 0,
            "conditionParams": "{}",
        }
        for predecessor, successor in edges
    )
    return _json_text(relations)


def _task_definitions_json(
    tasks: list[WorkflowTaskSpec],
    task_codes: Mapping[str, int],
    task_versions: Mapping[str, int],
    main_task_ids: Mapping[int, int] | None = None,
    *,
    task_cache_by_name: Mapping[str, str | None],
    task_cache_requested: bool,
    profile_version: str,
    projection_sources: Mapping[str, ProjectionSource],
    resource_refs: TaskResourceRefIndex,
    workflow_refs: TaskWorkflowRefIndex,
) -> str:
    task_refs = TaskRefIndex.from_code_by_name(task_codes)
    definitions = [
        _task_definition_payload(
            task,
            code=task_codes[task.name],
            version=task_versions[task.name],
            main_task_id=(main_task_ids or {}).get(task_codes[task.name]),
            task_refs=task_refs,
            task_cache_by_name=task_cache_by_name,
            task_cache_requested=task_cache_requested,
            profile_version=profile_version,
            projection_source=projection_sources[task.name],
            resource_refs=resource_refs,
            workflow_refs=workflow_refs,
        )
        for task in tasks
    ]
    return _json_text(definitions)


def _task_definition_payload(
    task: WorkflowTaskSpec,
    *,
    code: int,
    version: int,
    main_task_id: int | None = None,
    task_refs: TaskRefIndex,
    task_cache_by_name: Mapping[str, str | None],
    task_cache_requested: bool,
    profile_version: str,
    projection_source: ProjectionSource,
    resource_refs: TaskResourceRefIndex,
    workflow_refs: TaskWorkflowRefIndex,
) -> dict[str, int | str | None]:
    timeout_flag, _ = task_timeout_settings(
        task.timeout,
        notify_strategy=task.timeout_notify_strategy,
    )
    projected = encode_task_parameters(
        version=profile_version,
        task_type=task.type,
        task_params=_task_params_payload(task),
        refs=task_refs,
        source=projection_source,
        resource_refs=resource_refs,
        workflow_refs=workflow_refs,
    )
    payload: dict[str, int | str | None] = {
        "code": code,
        "version": version,
        "name": task.name,
        **encode_task_node_fields(task, ("description",)),
        "taskType": projected.task_type,
        "taskParams": _json_text(projected.task_params),
        **encode_task_node_fields(
            task,
            (
                "flag",
                "priority",
                "worker_group",
                "environment_code",
                "task_group_id",
                "task_group_priority",
                "retry.times",
                "retry.interval",
            ),
        ),
        "timeoutFlag": timeout_flag,
        **encode_task_node_fields(
            task, ("timeout_notify_strategy", "timeout", "delay")
        ),
        "resourceIds": "",
        **encode_task_node_fields(task, ("cpu_quota", "memory_max")),
        "taskExecuteType": _task_definition_execute_type(projected.task_type),
    }
    if main_task_id is not None:
        if profile_version != "3.1.0" or main_task_id <= 0:
            message = "Main task id is only valid for DS 3.1.0 existing tasks"
            raise ApiTransportError(message, details={"resource": WORKFLOW_RESOURCE})
        payload["id"] = main_task_id
    if task_cache_requested:
        if task.name not in task_cache_by_name:
            payload["isCache"] = "NO"
        elif task_cache_by_name[task.name] in {"YES", "NO"}:
            payload["isCache"] = task_cache_by_name[task.name]
        else:
            message = "Workflow DAG payload was missing required task cache state"
            raise ApiTransportError(
                message,
                details={"resource": WORKFLOW_RESOURCE, "task": task.name},
            )
    return payload


def _task_definition_execute_type(task_type: str) -> str:
    """Select the DS-native execute type implied by one exact task family."""
    if task_type == "FLINK_STREAM":
        return task_execute_type_value("STREAM")
    return TASK_EXECUTE_TYPE_BATCH_VALUE


def _task_params_payload(
    task: WorkflowTaskSpec,
) -> JsonObject:
    if task.task_params is not None:
        return require_json_object(
            task.task_params,
            label=f"workflow task params for '{task.name}'",
        )
    message = task.command
    if message is None:
        fallback = f"Task '{task.name}' was missing task params"
        raise ApiTransportError(fallback, details={"resource": WORKFLOW_RESOURCE})
    return {
        "rawScript": message,
        "localParams": [],
        "resourceList": [],
    }


def _json_text(value: JsonObject | Sequence[Mapping[str, JsonValue]]) -> str:
    return json.dumps(
        require_json_value(value, label="workflow JSON text"),
        ensure_ascii=False,
        separators=(",", ":"),
    )
