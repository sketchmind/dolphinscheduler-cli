from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from dsctl.generated.versions.ds_3_4_1.common.enums.task_execute_type import (
    TaskExecuteType,
)
from dsctl.generated.versions.ds_3_4_1.common.enums.workflow_execution_status import (
    WorkflowExecutionStatus,
)
from dsctl.generated.versions.ds_3_4_1.plugin.task_api.enums import (
    task_execution_status,
)

if TYPE_CHECKING:
    from dsctl.upstream.protocol import StringEnumValue


@dataclass(frozen=True)
class WorkflowExecutionStatusInfo:
    """Version-stable workflow execution-state facts used by services."""

    value: str
    can_stop: bool
    final_state: bool


def _task_execution_status_value(name: str) -> str:
    return task_execution_status.TaskExecutionStatus[name].value


TASK_EXECUTE_TYPE_BATCH_VALUE = TaskExecuteType.BATCH.value
WORKFLOW_EXECUTION_STOP_STATE = WorkflowExecutionStatus.STOP.value
WORKFLOW_EXECUTION_FAILURE_STATE = WorkflowExecutionStatus.FAILURE.value

TASK_EXECUTION_FORCE_SUCCESS_ALLOWED_STATES = frozenset(
    {
        _task_execution_status_value("FAILURE"),
        _task_execution_status_value("NEED_FAULT_TOLERANCE"),
        _task_execution_status_value("KILL"),
    }
)
TASK_EXECUTION_FINISHED_STATES = frozenset(
    {
        _task_execution_status_value("SUCCESS"),
        _task_execution_status_value("FORCED_SUCCESS"),
        _task_execution_status_value("KILL"),
        _task_execution_status_value("FAILURE"),
        _task_execution_status_value("NEED_FAULT_TOLERANCE"),
        _task_execution_status_value("PAUSE"),
    }
)
TASK_EXECUTION_RUNNING_STATES = frozenset(
    {_task_execution_status_value("RUNNING_EXECUTION")}
)
TASK_EXECUTION_QUEUED_STATES = frozenset(
    {
        _task_execution_status_value("SUBMITTED_SUCCESS"),
        _task_execution_status_value("DISPATCH"),
        _task_execution_status_value("DELAY_EXECUTION"),
    }
)
TASK_EXECUTION_PAUSED_STATES = frozenset({_task_execution_status_value("PAUSE")})
TASK_EXECUTION_FAILED_STATES = frozenset(
    {
        _task_execution_status_value("FAILURE"),
        _task_execution_status_value("NEED_FAULT_TOLERANCE"),
        _task_execution_status_value("KILL"),
    }
)
TASK_EXECUTION_SUCCESS_STATES = frozenset(
    {
        _task_execution_status_value("SUCCESS"),
        _task_execution_status_value("FORCED_SUCCESS"),
    }
)


def workflow_execution_status_info(
    value: StringEnumValue | str | None,
) -> WorkflowExecutionStatusInfo | None:
    """Return workflow execution-state facts for a DS enum-like value."""
    wire_value = _enum_wire_value(value)
    if wire_value is None:
        return None
    try:
        status = WorkflowExecutionStatus[wire_value]
    except KeyError:
        return None
    return WorkflowExecutionStatusInfo(
        value=status.value,
        can_stop=status.canStop,
        final_state=status.finalState,
    )


def workflow_execution_status_value(name: str) -> str:
    """Return the DS workflow execution-status wire value for one enum name."""
    return WorkflowExecutionStatus[name].value


def workflow_execution_status_is_final(state_name: str | None) -> bool:
    """Return whether one workflow execution-status name is final."""
    if state_name is None:
        return False
    try:
        return WorkflowExecutionStatus[state_name].finalState
    except KeyError:
        return False


def task_execution_status_value(name: str) -> str:
    """Return the DS task execution-status wire value for one enum name."""
    return _task_execution_status_value(name)


def _enum_wire_value(value: StringEnumValue | str | None) -> str | None:
    if isinstance(value, str):
        return value
    wire_value = getattr(value, "value", None)
    return wire_value if isinstance(wire_value, str) else None


__all__ = [
    "TASK_EXECUTE_TYPE_BATCH_VALUE",
    "TASK_EXECUTION_FAILED_STATES",
    "TASK_EXECUTION_FINISHED_STATES",
    "TASK_EXECUTION_FORCE_SUCCESS_ALLOWED_STATES",
    "TASK_EXECUTION_PAUSED_STATES",
    "TASK_EXECUTION_QUEUED_STATES",
    "TASK_EXECUTION_RUNNING_STATES",
    "TASK_EXECUTION_SUCCESS_STATES",
    "WORKFLOW_EXECUTION_FAILURE_STATE",
    "WORKFLOW_EXECUTION_STOP_STATE",
    "WorkflowExecutionStatusInfo",
    "task_execution_status_value",
    "workflow_execution_status_info",
    "workflow_execution_status_is_final",
    "workflow_execution_status_value",
]
