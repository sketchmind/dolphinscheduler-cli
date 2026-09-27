from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.command_contract import COMMAND_CATALOG
from dsctl.services.version_resolution import selected_target_globals
from dsctl.upstream.serialization import (
    optional_text,
)

if TYPE_CHECKING:
    from pathlib import Path

    from dsctl.services.workflow_instance._types import (
        WorkflowInstanceEditInputMode,
    )
    from dsctl.upstream.runtime_instances import (
        WorkflowInstanceSnapshot,
    )


def _wait_for_final_state_suggestion(command: str) -> str:
    return (
        "Wait for the workflow instance to reach a final state, then retry "
        f"`{command}`."
    )


def _workflow_instance_command(
    action: str,
    *,
    workflow_instance_id: int,
    project_selector: str,
    task: int | str | None = None,
) -> str:
    values: dict[str, str | int] = {
        "workflow_instance": workflow_instance_id,
        "project": project_selector,
    }
    if task is not None:
        values["task"] = str(task)
    return COMMAND_CATALOG.render(
        f"workflow-instance.{action}",
        values=values,
        global_values=selected_target_globals(),
    )


def _workflow_instance_action_command(
    action: str,
    *,
    workflow_instance_id: int,
    project_selector: str,
    task_code: int | None = None,
) -> str:
    return _workflow_instance_command(
        action,
        workflow_instance_id=workflow_instance_id,
        project_selector=project_selector,
        task=task_code,
    )


def _workflow_instance_edit_retry_command(
    input_mode: WorkflowInstanceEditInputMode,
    *,
    workflow_instance_id: int,
    project_selector: str,
    input_path: Path,
) -> str:
    return COMMAND_CATALOG.render(
        "workflow-instance.edit",
        global_values=selected_target_globals(),
        values={
            "workflow_instance": workflow_instance_id,
            "project": project_selector,
            "file" if input_mode == "file" else "patch": str(input_path),
        },
    )


def _instance_workflow_run_command(payload: WorkflowInstanceSnapshot) -> str:
    """Build the safest fresh-instance alternative for replay-unsafe tasks."""
    dag = payload.dagData
    definition = None if dag is None else dag.workflowDefinition
    workflow_selector = (
        None if definition is None else optional_text(definition.name)
    ) or "WORKFLOW"
    project_selector = payload.project.name or str(payload.project.native.value)
    return COMMAND_CATALOG.render(
        "workflow.run",
        global_values=selected_target_globals(),
        values={
            "workflow": workflow_selector,
            "project": project_selector,
        },
    )
