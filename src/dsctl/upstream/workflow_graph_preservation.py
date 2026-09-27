"""Preserve a reviewed code-native graph around one task-field overlay."""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from typing import TYPE_CHECKING, TypedDict, cast

from dsctl.cli_surface import TASK_RESOURCE
from dsctl.errors import ApiTransportError
from dsctl.output import require_json_object
from dsctl.upstream.serialization import enum_value, serialize_task
from dsctl.upstream.task_settings import task_node_native_name
from dsctl.upstream.workflow_graph_requests import workflow_update_arguments

if TYPE_CHECKING:
    from collections.abc import Sequence

    from dsctl.support.json_types import JsonObject, JsonValue
    from dsctl.upstream.protocol import WorkflowDagRecord
    from dsctl.upstream.workflow_graph import WorkflowUpdatePayload
    from dsctl.upstream.workflow_graph_requests import WorkflowUpdateArguments


# Exact overlay authorization remains independent of encodable fields.
_TASK_OVERLAY_FIELDS = (
    "delay",
    "description",
    "environment_code",
    "flag",
    "priority",
    "retry.interval",
    "retry.times",
    "timeout",
    "timeout_notify_strategy",
    "worker_group",
)


def _task_overlay_fields(path: str) -> frozenset[str]:
    if path == "command":
        return frozenset({"taskParams"})
    if path == "timeout":
        return frozenset(
            {
                task_node_native_name(path),
                "timeoutFlag",
                task_node_native_name("timeout_notify_strategy"),
            }
        )
    if path == "task_group_id":
        return frozenset(
            {
                task_node_native_name("task_group_id"),
                task_node_native_name("task_group_priority"),
            }
        )
    if path in _TASK_OVERLAY_FIELDS or path in {
        "task_group_priority",
        "cpu_quota",
        "memory_max",
    }:
        return frozenset({task_node_native_name(path)})
    return frozenset()


_RELATION_IDENTITY_FIELDS = frozenset(
    {
        "preTaskCode",
        "preTaskVersion",
        "postTaskCode",
        "postTaskVersion",
    }
)


WHOLE_WORKFLOW_TASK_REQUEST_FIELDS = frozenset().union(
    *(_task_overlay_fields(path) for path in (*_TASK_OVERLAY_FIELDS, "command"))
)


def workflow_graph_fingerprint(
    dag: WorkflowDagRecord,
    *,
    dag_raw: JsonObject,
    workflow_field: str = "processDefinition",
    relation_field: str = "processTaskRelationList",
) -> str:
    """Hash the lossless raw DAG, falling back only for local test doubles."""
    snapshot: JsonObject
    if dag_raw:
        _require_raw_components(
            dag_raw, workflow_field=workflow_field, relation_field=relation_field
        )
        snapshot = deepcopy(dag_raw)
    else:
        snapshot = _fallback_dag_raw(
            dag, workflow_field=workflow_field, relation_field=relation_field
        )
    encoded = json.dumps(
        snapshot,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


class _WholeWorkflowPayload(TypedDict):
    """One fully materialized exact whole-workflow update payload."""

    name: str
    description: str | None
    globalParams: str
    locations: str
    timeout: int
    taskRelationJson: str
    taskDefinitionJson: str
    executionType: str | None
    releaseState: str | None
    tenantCode: str | None


def preserved_task_update_arguments(
    compiled: WorkflowUpdatePayload,
    *,
    dag: WorkflowDagRecord,
    dag_raw: JsonObject,
    task_name: str,
    requested_fields: Sequence[str],
    workflow_field: str = "processDefinition",
    relation_field: str = "processTaskRelationList",
) -> WorkflowUpdateArguments:
    """Overlay the selected task fields while retaining unowned native graph state."""
    raw_workflow, raw_tasks, raw_relations = _graph_components(
        dag,
        dag_raw=dag_raw,
        workflow_field=workflow_field,
        relation_field=relation_field,
    )
    compiled_tasks = _json_object_list(
        compiled["taskDefinitionJson"],
        label="compiled task definitions",
    )
    compiled_relations = _json_object_list(
        compiled["taskRelationJson"],
        label="compiled task relations",
    )
    task_definitions = _merge_task_definitions(
        raw_tasks,
        compiled_tasks=compiled_tasks,
        task_name=task_name,
        requested_fields=requested_fields,
    )
    task_relations = _merge_task_relations(
        raw_relations,
        compiled_relations=compiled_relations,
    )
    payload: _WholeWorkflowPayload = {
        "name": _text_or(compiled["name"], raw_workflow.get("name")),
        "description": _optional_text_or(
            raw_workflow.get("description"),
            compiled.get("description"),
        ),
        "globalParams": _text_or(
            raw_workflow.get("globalParams"),
            compiled["globalParams"],
        ),
        "locations": _text_or(
            raw_workflow.get("locations"),
            compiled["locations"],
        ),
        "timeout": _int_or(raw_workflow.get("timeout"), compiled["timeout"]),
        "taskRelationJson": _json_text(task_relations),
        "taskDefinitionJson": _json_text(task_definitions),
        "executionType": _optional_text(compiled.get("executionType")),
        "releaseState": _optional_text(compiled.get("releaseState")),
        "tenantCode": _optional_text(raw_workflow.get("tenantCode")),
    }
    return workflow_update_arguments(payload, tenant_code=payload["tenantCode"])


def _merge_task_definitions(
    current: list[JsonObject],
    *,
    compiled_tasks: list[JsonObject],
    task_name: str,
    requested_fields: Sequence[str],
) -> list[JsonObject]:
    current_by_code = _unique_objects_by_int(current, field_name="code")
    compiled_by_code = _unique_objects_by_int(compiled_tasks, field_name="code")
    if current_by_code.keys() != compiled_by_code.keys():
        message = "Whole-workflow task compilation changed task membership"
        raise ApiTransportError(
            message,
            details={"resource": TASK_RESOURCE, "mutation_applied": False},
        )
    target_codes = [
        code for code, task in current_by_code.items() if task.get("name") == task_name
    ]
    if len(target_codes) != 1:
        message = "Whole-workflow task compilation lost the selected task"
        raise ApiTransportError(
            message,
            details={"resource": TASK_RESOURCE, "mutation_applied": False},
        )
    target_code = target_codes[0]
    fields_to_update = frozenset().union(
        *(_task_overlay_fields(field_name) for field_name in requested_fields)
    )
    merged: list[JsonObject] = []
    for current_task in current:
        code = _positive_int(current_task.get("code"), label="task code")
        item = deepcopy(current_task)
        if code == target_code:
            compiled_task = compiled_by_code[code]
            for field_name in fields_to_update:
                if field_name not in compiled_task:
                    message = (
                        "Whole-workflow task compilation omitted a requested field"
                    )
                    raise ApiTransportError(
                        message,
                        details={
                            "resource": TASK_RESOURCE,
                            "field": field_name,
                            "mutation_applied": False,
                        },
                    )
                if field_name == "taskParams":
                    item[field_name] = _merge_task_params(
                        current_task.get(field_name),
                        compiled_task[field_name],
                    )
                else:
                    item[field_name] = deepcopy(compiled_task[field_name])
        merged.append(item)
    return merged


def _merge_task_params(current: JsonValue, compiled: JsonValue) -> JsonValue:
    """Overlay reviewed task parameters without dropping opaque siblings."""
    current_params = _task_params_object(
        current,
        label="current taskParams",
    )
    compiled_params = _task_params_object(
        compiled,
        label="compiled taskParams",
    )
    merged = _overlay_json_objects(current_params, compiled_params)
    if isinstance(compiled, str):
        return _json_text(merged)
    return merged


def _task_params_object(value: JsonValue, *, label: str) -> JsonObject:
    decoded = value
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError as exc:
            message = f"{label} was not valid JSON"
            raise ApiTransportError(
                message,
                details={"resource": TASK_RESOURCE, "mutation_applied": False},
            ) from exc
    return require_json_object(decoded, label=label)


def _overlay_json_objects(
    current: JsonObject,
    compiled: JsonObject,
) -> JsonObject:
    merged = deepcopy(current)
    for field_name, compiled_value in compiled.items():
        current_value = merged.get(field_name)
        if isinstance(current_value, dict) and isinstance(compiled_value, dict):
            merged[field_name] = _overlay_json_objects(
                current_value,
                compiled_value,
            )
        else:
            merged[field_name] = deepcopy(compiled_value)
    return merged


def _merge_task_relations(
    current: list[JsonObject],
    *,
    compiled_relations: list[JsonObject],
) -> list[JsonObject]:
    current_by_edge = _unique_relations(current)
    merged: list[JsonObject] = []
    seen_edges: set[tuple[int, int]] = set()
    for compiled in compiled_relations:
        edge = _relation_edge(compiled)
        if edge in seen_edges:
            message = "Whole-workflow task compilation produced duplicate relations"
            raise ApiTransportError(
                message,
                details={"resource": TASK_RESOURCE, "mutation_applied": False},
            )
        seen_edges.add(edge)
        item = deepcopy(current_by_edge.get(edge, compiled))
        for field_name in _RELATION_IDENTITY_FIELDS:
            item[field_name] = deepcopy(compiled[field_name])
        merged.append(item)
    return merged


def _graph_components(
    dag: WorkflowDagRecord,
    *,
    dag_raw: JsonObject,
    workflow_field: str,
    relation_field: str,
) -> tuple[JsonObject, list[JsonObject], list[JsonObject]]:
    if dag_raw:
        return _require_raw_components(
            dag_raw, workflow_field=workflow_field, relation_field=relation_field
        )
    fallback = _fallback_dag_raw(
        dag, workflow_field=workflow_field, relation_field=relation_field
    )
    return _require_raw_components(
        fallback, workflow_field=workflow_field, relation_field=relation_field
    )


def _require_raw_components(
    value: JsonObject,
    *,
    workflow_field: str,
    relation_field: str,
) -> tuple[JsonObject, list[JsonObject], list[JsonObject]]:
    workflow = require_json_object(
        value.get(workflow_field),
        label=f"native {workflow_field}",
    )
    tasks = _object_list(
        value.get("taskDefinitionList"),
        label="native taskDefinitionList",
    )
    relations = _object_list(
        value.get(relation_field),
        label=f"native {relation_field}",
    )
    return workflow, tasks, relations


def _fallback_dag_raw(
    dag: WorkflowDagRecord, *, workflow_field: str, relation_field: str
) -> JsonObject:
    workflow = dag.workflowDefinition
    if workflow is None:
        message = "Workflow DAG payload was missing processDefinition"
        raise ApiTransportError(
            message,
            details={"resource": TASK_RESOURCE, "mutation_applied": False},
        )
    return {
        workflow_field: {
            "id": workflow.id,
            "code": workflow.code,
            "name": workflow.name,
            "version": workflow.version,
            "releaseState": enum_value(workflow.releaseState),
            "projectCode": workflow.projectCode,
            "description": workflow.description,
            "globalParams": workflow.globalParams,
            "globalParamMap": None
            if workflow.globalParamMap is None
            else dict(workflow.globalParamMap),
            "timeout": workflow.timeout,
        },
        "taskDefinitionList": [
            cast("JsonObject", serialize_task(task))
            for task in dag.taskDefinitionList or ()
        ],
        relation_field: [
            {
                "preTaskCode": relation.preTaskCode,
                "preTaskVersion": relation.preTaskVersion,
                "postTaskCode": relation.postTaskCode,
                "postTaskVersion": relation.postTaskVersion,
                "conditionParams": relation.conditionParams,
            }
            for relation in dag.workflowTaskRelationList or ()
        ],
    }


def _json_object_list(value: JsonValue, *, label: str) -> list[JsonObject]:
    if not isinstance(value, str):
        message = f"{label} must be serialized JSON text"
        raise ApiTransportError(
            message,
            details={"resource": TASK_RESOURCE, "mutation_applied": False},
        )
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError as exc:
        message = f"{label} was not valid JSON"
        raise ApiTransportError(
            message,
            details={"resource": TASK_RESOURCE, "mutation_applied": False},
        ) from exc
    return _object_list(decoded, label=label)


def _object_list(value: JsonValue, *, label: str) -> list[JsonObject]:
    if not isinstance(value, list):
        message = f"{label} must be a list"
        raise ApiTransportError(
            message,
            details={"resource": TASK_RESOURCE, "mutation_applied": False},
        )
    return [require_json_object(item, label=f"{label} item") for item in value]


def _unique_objects_by_int(
    values: list[JsonObject],
    *,
    field_name: str,
) -> dict[int, JsonObject]:
    result: dict[int, JsonObject] = {}
    for value in values:
        key = _positive_int(value.get(field_name), label=field_name)
        if key in result:
            message = f"Whole-workflow payload duplicated {field_name} {key}"
            raise ApiTransportError(
                message,
                details={"resource": TASK_RESOURCE, "mutation_applied": False},
            )
        result[key] = value
    return result


def _unique_relations(values: list[JsonObject]) -> dict[tuple[int, int], JsonObject]:
    result: dict[tuple[int, int], JsonObject] = {}
    for value in values:
        edge = _relation_edge(value)
        if edge in result:
            message = "Whole-workflow payload contained duplicate task relations"
            raise ApiTransportError(
                message,
                details={"resource": TASK_RESOURCE, "mutation_applied": False},
            )
        result[edge] = value
    return result


def _relation_edge(value: JsonObject) -> tuple[int, int]:
    pre_task_code = value.get("preTaskCode")
    post_task_code = value.get("postTaskCode")
    if (
        isinstance(pre_task_code, bool)
        or not isinstance(pre_task_code, int)
        or pre_task_code < 0
    ):
        message = "Whole-workflow relation has an invalid preTaskCode"
        raise ApiTransportError(
            message,
            details={"resource": TASK_RESOURCE, "mutation_applied": False},
        )
    return (
        pre_task_code,
        _positive_int(post_task_code, label="postTaskCode"),
    )


def _positive_int(value: JsonValue, *, label: str) -> int:
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return value
    message = f"Whole-workflow payload has an invalid {label}"
    raise ApiTransportError(
        message,
        details={"resource": TASK_RESOURCE, "mutation_applied": False},
    )


def _text_or(first: JsonValue, second: JsonValue) -> str:
    for value in (first, second):
        if isinstance(value, str):
            return value
    message = "Whole-workflow payload was missing required text"
    raise ApiTransportError(
        message,
        details={"resource": TASK_RESOURCE, "mutation_applied": False},
    )


def _optional_text_or(first: JsonValue, second: JsonValue) -> str | None:
    for value in (first, second):
        if value is None or isinstance(value, str):
            return value
    message = "Whole-workflow payload has an invalid optional text field"
    raise ApiTransportError(
        message,
        details={"resource": TASK_RESOURCE, "mutation_applied": False},
    )


def _optional_text(value: JsonValue) -> str | None:
    if value is None or isinstance(value, str):
        return value
    message = "Whole-workflow payload has an invalid optional text field"
    raise ApiTransportError(
        message,
        details={"resource": TASK_RESOURCE, "mutation_applied": False},
    )


def _int_or(first: JsonValue, second: JsonValue) -> int:
    for value in (first, second):
        if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
            return value
    message = "Whole-workflow payload was missing a non-negative timeout"
    raise ApiTransportError(
        message,
        details={"resource": TASK_RESOURCE, "mutation_applied": False},
    )


def _json_text(value: JsonValue) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
