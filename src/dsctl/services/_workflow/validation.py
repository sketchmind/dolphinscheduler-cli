from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.errors import UserInputError

if TYPE_CHECKING:
    from dsctl.models.workflow_spec import WorkflowSpec


def require_schedule_block_create_compatible(spec: WorkflowSpec) -> None:
    """Ensure one workflow YAML schedule block is compatible with create flow."""
    schedule = spec.schedule
    if schedule is None:
        return
    if spec.workflow.release_state.value == "ONLINE":
        return
    message = (
        "Workflow YAML schedule blocks require workflow.release_state=ONLINE "
        "because DolphinScheduler only allows schedules on online workflows."
    )
    raise UserInputError(
        message,
        suggestion=(
            "Set workflow.release_state=ONLINE or remove the schedule block "
            "before retrying."
        ),
    )


__all__ = ["require_schedule_block_create_compatible"]
