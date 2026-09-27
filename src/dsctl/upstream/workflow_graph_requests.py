"""Project compiled graphs into existing exact domain request interfaces."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypedDict

if TYPE_CHECKING:
    from dsctl.upstream.legacy_workflow_graph import LegacyWorkflowGraphPayload
    from dsctl.upstream.workflow_graph import (
        WorkflowCreatePayload,
        WorkflowUpdatePayload,
    )


class LegacyWorkflowArguments(TypedDict):
    """The three separate graph strings accepted by DS 1.3.9 definitions."""

    process_definition_json: str
    locations: str
    connects: str


class WorkflowCreateArguments(TypedDict):
    """Bound graph arguments accepted by the workflow definition domain."""

    name: str
    description: str | None
    global_params: str
    locations: str
    timeout: int
    task_relation_json: str
    task_definition_json: str
    execution_type: str | None


class LegacyWorkflowInstanceUpdateArguments(TypedDict):
    """The legacy runtime-instance graph and definition-sync arguments."""

    process_instance_json: str
    locations: str
    connects: str
    sync_define: bool


class WorkflowUpdateArguments(WorkflowCreateArguments):
    """Definition update arguments including preserved release and tenant state."""

    release_state: str | None
    tenant_code: str | None


class WorkflowInstanceUpdateArguments(TypedDict):
    """Compiled graph arguments accepted by the runtime-instance domain."""

    task_relation_json: str
    task_definition_json: str
    sync_define: bool
    global_params: str
    locations: str
    timeout: int


@dataclass(frozen=True)
class WorkflowGraphCounts:
    """Structural counts from the actual compiled graph, without revalidation."""

    task_definitions: int
    task_relations: int
    global_parameters: int


def legacy_workflow_arguments(
    payload: LegacyWorkflowGraphPayload,
) -> LegacyWorkflowArguments:
    """Bind the legacy graph without parsing or translating its string identities."""
    return {
        "process_definition_json": payload["processDefinitionJson"],
        "locations": payload["locations"],
        "connects": payload["connects"],
    }


def workflow_create_arguments(
    payload: WorkflowCreatePayload,
) -> WorkflowCreateArguments:
    """Bind one compiled graph to the existing definition-create interface."""
    return {
        "name": payload["name"],
        "description": payload["description"],
        "global_params": payload["globalParams"],
        "locations": payload["locations"],
        "timeout": payload["timeout"],
        "task_relation_json": payload["taskRelationJson"],
        "task_definition_json": payload["taskDefinitionJson"],
        "execution_type": payload["executionType"],
    }


def legacy_workflow_instance_update_arguments(
    payload: LegacyWorkflowGraphPayload,
    *,
    sync_definition: bool,
) -> LegacyWorkflowInstanceUpdateArguments:
    """Bind the legacy instance graph without introducing modern task identities."""
    return {
        "process_instance_json": payload["processDefinitionJson"],
        "locations": payload["locations"],
        "connects": payload["connects"],
        "sync_define": sync_definition,
    }


def workflow_update_arguments(
    payload: WorkflowUpdatePayload,
    *,
    tenant_code: str | None = None,
) -> WorkflowUpdateArguments:
    """Bind a compiled update without changing its selected native graph epoch."""
    return {
        **workflow_create_arguments(payload),
        "release_state": payload["releaseState"],
        "tenant_code": tenant_code,
    }


def workflow_instance_update_arguments(
    payload: WorkflowUpdatePayload,
    *,
    sync_definition: bool,
    preserved_global_params: str | None = None,
) -> WorkflowInstanceUpdateArguments:
    """Bind only the graph fields supported by the runtime-instance update."""
    return {
        "task_relation_json": payload["taskRelationJson"],
        "task_definition_json": payload["taskDefinitionJson"],
        "sync_define": sync_definition,
        "global_params": payload["globalParams"]
        if preserved_global_params is None
        else preserved_global_params,
        "locations": payload["locations"],
        "timeout": payload["timeout"],
    }


def workflow_graph_counts(payload: WorkflowCreatePayload) -> WorkflowGraphCounts:
    """Count the rendered arrays consumed by lint, preserving malformed-data errors."""
    return WorkflowGraphCounts(
        task_definitions=_json_array_length(
            payload["taskDefinitionJson"], label="taskDefinitionJson"
        ),
        task_relations=_json_array_length(
            payload["taskRelationJson"], label="taskRelationJson"
        ),
        global_parameters=_json_array_length(
            payload["globalParams"], label="globalParams"
        ),
    )


def _json_array_length(value: str | int | None, *, label: str) -> int:
    if not isinstance(value, str):
        message = f"{label} did not compile to JSON text"
        raise TypeError(message)
    parsed = json.loads(value)
    if not isinstance(parsed, list):
        message = f"{label} did not compile to a JSON array"
        raise TypeError(message)
    return len(parsed)
