from __future__ import annotations

import json
from collections import deque
from collections.abc import Mapping
from typing import TYPE_CHECKING, TypedDict

from dsctl.cli_surface import WORKFLOW_RESOURCE
from dsctl.errors import ApiTransportError, UserInputError
from dsctl.models.common import DataType, Direct
from dsctl.output import require_json_object, require_json_value
from dsctl.services._task_settings import (
    task_environment_code_value,
    task_flag_value,
    task_group_values,
    task_resource_limit_value,
    task_timeout_settings,
)
from dsctl.support.ds_code import gen_code
from dsctl.upstream.runtime_enums import TASK_EXECUTE_TYPE_BATCH_VALUE

if TYPE_CHECKING:
    from dsctl.models.workflow_spec import WorkflowSpec, WorkflowTaskSpec
    from dsctl.services._workflow_identity import WorkflowTaskIdentity
    from dsctl.support.yaml_io import JsonObject


_WORKFLOW_COMPILE_REVIEW_SUGGESTION = (
    "Review task names and task references, then retry with workflow dry-run "
    "before applying the change."
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


def compile_workflow_create_payload(
    spec: WorkflowSpec,
    *,
    task_identities: Mapping[str, WorkflowTaskIdentity] | None = None,
) -> WorkflowCreatePayload:
    """Compile one workflow spec into the legacy DS create/update payload."""
    task_codes, task_versions = _task_identity_maps(
        spec.tasks,
        task_identities=task_identities,
    )
    edges = workflow_edges(spec.tasks)
    levels = _task_levels(spec.tasks, edges=edges)
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
        ),
        "executionType": spec.workflow.execution_type.value,
    }


def compile_workflow_update_payload(
    spec: WorkflowSpec,
    *,
    release_state: str | None,
    task_identities: Mapping[str, WorkflowTaskIdentity] | None = None,
) -> WorkflowUpdatePayload:
    """Compile one workflow spec into the legacy update payload."""
    return {
        **compile_workflow_create_payload(spec, task_identities=task_identities),
        "releaseState": release_state,
    }


def _task_identity_maps(
    tasks: list[WorkflowTaskSpec],
    *,
    task_identities: Mapping[str, WorkflowTaskIdentity] | None,
) -> tuple[dict[str, int], dict[str, int]]:
    task_codes: dict[str, int] = {}
    task_versions: dict[str, int] = {}
    used_codes = (
        {identity.code for identity in task_identities.values()}
        if task_identities is not None
        else set()
    )
    for task in tasks:
        identity = None if task_identities is None else task_identities.get(task.name)
        if identity is None:
            task_codes[task.name] = _next_generated_task_code(used_codes)
            task_versions[task.name] = 1
            continue
        task_codes[task.name] = identity.code
        task_versions[task.name] = identity.version
    return task_codes, task_versions


def _next_generated_task_code(used_codes: set[int]) -> int:
    while True:
        code = gen_code()
        if code in used_codes:
            continue
        used_codes.add(code)
        return code


def workflow_edges(tasks: list[WorkflowTaskSpec]) -> list[tuple[str, str]]:
    """Build the DAG edge list implied by task dependencies and logic branches."""
    task_names = {task.name for task in tasks}
    edges: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    def add_edge(
        *,
        predecessor: str,
        successor: str,
        task_name: str,
        label: str,
    ) -> None:
        if predecessor not in task_names:
            message = f"Task '{task_name}' depends on unknown task '{predecessor}'"
            raise UserInputError(
                message,
                suggestion=_WORKFLOW_COMPILE_REVIEW_SUGGESTION,
            )
        if successor not in task_names:
            message = (
                f"Task '{task_name}' references unknown task '{successor}' in {label}"
            )
            raise UserInputError(
                message,
                suggestion=_WORKFLOW_COMPILE_REVIEW_SUGGESTION,
            )
        if predecessor == successor:
            message = f"Task '{task_name}' cannot reference itself in {label}"
            raise UserInputError(
                message,
                suggestion=_WORKFLOW_COMPILE_REVIEW_SUGGESTION,
            )
        edge = (predecessor, successor)
        if edge in seen:
            return
        seen.add(edge)
        edges.append(edge)

    for task in tasks:
        for dependency in task.depends_on:
            add_edge(
                predecessor=dependency,
                successor=task.name,
                task_name=task.name,
                label="depends_on",
            )
        for label, successor in _logical_successor_targets(task):
            add_edge(
                predecessor=task.name,
                successor=successor,
                task_name=task.name,
                label=label,
            )
    return edges


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


def _task_levels(
    tasks: list[WorkflowTaskSpec],
    *,
    edges: list[tuple[str, str]],
) -> dict[str, int]:
    dependents: dict[str, list[str]] = {task.name: [] for task in tasks}
    indegree: dict[str, int] = {task.name: 0 for task in tasks}
    for predecessor, successor in edges:
        dependents[predecessor].append(successor)
        indegree[successor] += 1

    order: list[str] = []
    queue = deque(task.name for task in tasks if indegree[task.name] == 0)
    while queue:
        current = queue.popleft()
        order.append(current)
        for dependent in dependents[current]:
            indegree[dependent] -= 1
            if indegree[dependent] == 0:
                queue.append(dependent)

    if len(order) != len(tasks):
        message = "Workflow tasks contain a dependency cycle"
        raise UserInputError(
            message,
            suggestion=_WORKFLOW_COMPILE_REVIEW_SUGGESTION,
        )

    levels: dict[str, int] = {}
    predecessors_by_task: dict[str, list[str]] = {task.name: [] for task in tasks}
    for predecessor, successor in edges:
        predecessors_by_task[successor].append(predecessor)
    for name in order:
        dependencies = predecessors_by_task[name]
        if not dependencies:
            levels[name] = 0
            continue
        levels[name] = max(levels[dependency] + 1 for dependency in dependencies)
    return levels


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


def _logical_successor_targets(task: WorkflowTaskSpec) -> list[tuple[str, str]]:
    if task.task_params is None:
        return []
    payload = require_json_object(
        task.task_params,
        label=f"workflow task params for '{task.name}'",
    )
    task_type = task.type.upper()
    if task_type == "SWITCH":
        return _switch_successor_targets(payload)
    if task_type == "CONDITIONS":
        return _conditions_successor_targets(payload)
    return []


def _switch_successor_targets(payload: JsonObject) -> list[tuple[str, str]]:
    targets: list[tuple[str, str]] = []
    switch_result = payload.get("switchResult")
    if not isinstance(switch_result, Mapping):
        return targets
    depend_task_list = switch_result.get("dependTaskList")
    if isinstance(depend_task_list, list):
        for index, branch in enumerate(depend_task_list):
            if not isinstance(branch, Mapping):
                continue
            next_node = branch.get("nextNode")
            if isinstance(next_node, str):
                targets.append(
                    (
                        f"task_params.switchResult.dependTaskList[{index}].nextNode",
                        next_node,
                    )
                )
    default_next_node = switch_result.get("nextNode")
    if isinstance(default_next_node, str):
        targets.append(("task_params.switchResult.nextNode", default_next_node))
    return targets


def _conditions_successor_targets(payload: JsonObject) -> list[tuple[str, str]]:
    targets: list[tuple[str, str]] = []
    condition_result = payload.get("conditionResult")
    if not isinstance(condition_result, Mapping):
        return targets
    for key in ("successNode", "failedNode"):
        nodes = condition_result.get(key)
        if not isinstance(nodes, list):
            continue
        for index, node in enumerate(nodes):
            if isinstance(node, str):
                targets.append((f"task_params.conditionResult.{key}[{index}]", node))
    return targets


def _task_definitions_json(
    tasks: list[WorkflowTaskSpec],
    task_codes: Mapping[str, int],
    task_versions: Mapping[str, int],
) -> str:
    definitions = [
        _task_definition_payload(
            task,
            code=task_codes[task.name],
            version=task_versions[task.name],
            task_codes=task_codes,
        )
        for task in tasks
    ]
    return _json_text(definitions)


def _task_definition_payload(
    task: WorkflowTaskSpec,
    *,
    code: int,
    version: int,
    task_codes: Mapping[str, int],
) -> dict[str, int | str | None]:
    timeout_flag, timeout_notify_strategy = task_timeout_settings(
        task.timeout,
        notify_strategy=task.timeout_notify_strategy,
    )
    task_group_id, task_group_priority = task_group_values(
        task.task_group_id,
        task.task_group_priority,
    )
    payload: dict[str, int | str | None] = {
        "code": code,
        "version": version,
        "name": task.name,
        "description": task.description or "",
        "taskType": task.type.upper(),
        "taskParams": _json_text(_task_params_payload(task, task_codes=task_codes)),
        "flag": task_flag_value(task.flag),
        "taskPriority": task.priority.value,
        "workerGroup": task.worker_group or "default",
        "environmentCode": task_environment_code_value(task.environment_code),
        "taskGroupId": 0 if task_group_id is None else task_group_id,
        "taskGroupPriority": 0 if task_group_priority is None else task_group_priority,
        "failRetryTimes": task.retry.times,
        "failRetryInterval": task.retry.interval,
        "timeoutFlag": timeout_flag,
        "timeoutNotifyStrategy": timeout_notify_strategy,
        "timeout": task.timeout,
        "delayTime": task.delay,
        "resourceIds": "",
        "cpuQuota": task_resource_limit_value(task.cpu_quota),
        "memoryMax": task_resource_limit_value(task.memory_max),
        "taskExecuteType": TASK_EXECUTE_TYPE_BATCH_VALUE,
    }
    return payload


def _task_params_payload(
    task: WorkflowTaskSpec,
    *,
    task_codes: Mapping[str, int],
) -> JsonObject:
    if task.task_params is not None:
        payload = require_json_object(
            task.task_params,
            label=f"workflow task params for '{task.name}'",
        )
        return _compiled_task_params_payload(
            task=task,
            payload=payload,
            task_codes=task_codes,
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


def _compiled_task_params_payload(
    *,
    task: WorkflowTaskSpec,
    payload: JsonObject,
    task_codes: Mapping[str, int],
) -> JsonObject:
    task_type = task.type.upper()
    if task_type == "SWITCH":
        return _compiled_switch_task_params_payload(
            task_name=task.name,
            payload=payload,
            task_codes=task_codes,
        )
    if task_type == "CONDITIONS":
        return _compiled_conditions_task_params_payload(
            task_name=task.name,
            payload=payload,
            task_codes=task_codes,
        )
    return payload


def _compiled_switch_task_params_payload(
    *,
    task_name: str,
    payload: JsonObject,
    task_codes: Mapping[str, int],
) -> JsonObject:
    compiled: JsonObject = dict(payload)
    switch_result = payload.get("switchResult")
    if isinstance(switch_result, Mapping):
        compiled_switch_result: JsonObject = dict(switch_result)
        depend_task_list = switch_result.get("dependTaskList")
        if isinstance(depend_task_list, list):
            compiled_depend_task_list: list[JsonObject] = []
            for index, branch in enumerate(depend_task_list):
                if not isinstance(branch, Mapping):
                    continue
                compiled_branch: JsonObject = dict(branch)
                next_node = branch.get("nextNode")
                if next_node is not None:
                    compiled_branch["nextNode"] = _resolve_local_task_name_ref(
                        next_node,
                        task_name=task_name,
                        task_codes=task_codes,
                        label=(
                            f"task_params.switchResult.dependTaskList[{index}].nextNode"
                        ),
                    )
                compiled_depend_task_list.append(compiled_branch)
            compiled_switch_result["dependTaskList"] = compiled_depend_task_list
        default_next_node = switch_result.get("nextNode")
        if default_next_node is not None:
            compiled_switch_result["nextNode"] = _resolve_local_task_name_ref(
                default_next_node,
                task_name=task_name,
                task_codes=task_codes,
                label="task_params.switchResult.nextNode",
            )
        compiled["switchResult"] = compiled_switch_result
    next_branch = payload.get("nextBranch")
    if next_branch is not None:
        compiled["nextBranch"] = _resolve_local_task_name_ref(
            next_branch,
            task_name=task_name,
            task_codes=task_codes,
            label="task_params.nextBranch",
        )
    return compiled


def _compiled_conditions_task_params_payload(
    *,
    task_name: str,
    payload: JsonObject,
    task_codes: Mapping[str, int],
) -> JsonObject:
    compiled: JsonObject = dict(payload)
    condition_result = payload.get("conditionResult")
    if isinstance(condition_result, Mapping):
        compiled_condition_result: JsonObject = dict(condition_result)
        for key in ("successNode", "failedNode"):
            nodes = condition_result.get(key)
            if isinstance(nodes, list):
                compiled_condition_result[key] = [
                    _resolve_local_task_name_ref(
                        node,
                        task_name=task_name,
                        task_codes=task_codes,
                        label=f"task_params.conditionResult.{key}[{index}]",
                    )
                    for index, node in enumerate(nodes)
                ]
        compiled["conditionResult"] = compiled_condition_result
    return compiled


def _resolve_local_task_name_ref(
    value: object,
    *,
    task_name: str,
    task_codes: Mapping[str, int],
    label: str,
) -> int:
    if not isinstance(value, str):
        message = f"Task '{task_name}' expects a task name string in {label}"
        raise UserInputError(
            message,
            suggestion=_WORKFLOW_COMPILE_REVIEW_SUGGESTION,
        )
    candidate = value.strip()
    if candidate not in task_codes:
        message = f"Task '{task_name}' references unknown task '{candidate}' in {label}"
        raise UserInputError(
            message,
            suggestion=_WORKFLOW_COMPILE_REVIEW_SUGGESTION,
        )
    return task_codes[candidate]


def _json_text(value: object) -> str:
    return json.dumps(
        require_json_value(value, label="workflow JSON text"),
        ensure_ascii=False,
        separators=(",", ":"),
    )
