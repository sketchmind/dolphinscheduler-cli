from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from dsctl.upstream.parameter_semantics import (
    NestedWorkflowTaskType,
    get_parameter_semantics,
)

NestedWorkflowNativeCodeField = Literal[
    "processDefinitionId",
    "processDefinitionCode",
    "workflowDefinitionCode",
]


@dataclass(frozen=True, slots=True)
class NestedWorkflowAuthoringSurface:
    """Stable CLI name plus the exact native nested-workflow wire identity."""

    cli_task_type: Literal["SUB_WORKFLOW"]
    native_task_type: NestedWorkflowTaskType
    native_code_field: NestedWorkflowNativeCodeField


def _nested_workflow_surface(version: str) -> NestedWorkflowAuthoringSurface:
    native_task_type = get_parameter_semantics(version).nested_workflow.task_type
    native_code_field: NestedWorkflowNativeCodeField = (
        "processDefinitionId"
        if version == "1.3.9"
        else (
            "processDefinitionCode"
            if native_task_type == "SUB_PROCESS"
            else "workflowDefinitionCode"
        )
    )
    return NestedWorkflowAuthoringSurface(
        cli_task_type="SUB_WORKFLOW",
        native_task_type=native_task_type,
        native_code_field=native_code_field,
    )
