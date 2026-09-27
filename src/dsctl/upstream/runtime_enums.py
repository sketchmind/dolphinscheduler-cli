from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from dsctl.execution_states import (
    TASK_EXECUTION_FAILED_STATES,
    TASK_EXECUTION_FINISHED_STATES,
    TASK_EXECUTION_FORCE_SUCCESS_ALLOWED_STATES,
    TASK_EXECUTION_PAUSED_STATES,
    TASK_EXECUTION_QUEUED_STATES,
    TASK_EXECUTION_RUNNING_STATES,
    TASK_EXECUTION_SUCCESS_STATES,
    WORKFLOW_EXECUTION_STATUS_FACTS,
)

if TYPE_CHECKING:
    from dsctl.upstream.protocol import StringEnumValue


@dataclass(frozen=True)
class WorkflowExecutionStatusInfo:
    """Version-stable workflow execution-state facts used by services."""

    value: str
    can_stop: bool
    final_state: bool


_TASK_EXECUTE_TYPE_NAMES = frozenset({"BATCH", "STREAM"})
_TASK_EXECUTION_STATUS_NAMES = (
    TASK_EXECUTION_FINISHED_STATES
    | TASK_EXECUTION_RUNNING_STATES
    | TASK_EXECUTION_QUEUED_STATES
)


def _task_execution_status_value(name: str) -> str:
    if name not in _TASK_EXECUTION_STATUS_NAMES:
        raise KeyError(name)
    return name


TASK_EXECUTE_TYPE_BATCH_VALUE = "BATCH"
WORKFLOW_EXECUTION_STOP_STATE = "STOP"
WORKFLOW_EXECUTION_FAILURE_STATE = "FAILURE"


def workflow_execution_status_info(
    value: StringEnumValue | str | None,
) -> WorkflowExecutionStatusInfo | None:
    """Return workflow execution-state facts for a DS enum-like value."""
    wire_value = _enum_wire_value(value)
    if wire_value is None:
        return None
    facts = WORKFLOW_EXECUTION_STATUS_FACTS.get(wire_value)
    if facts is None:
        return None
    can_stop, final_state = facts
    return WorkflowExecutionStatusInfo(
        value=wire_value,
        can_stop=can_stop,
        final_state=final_state,
    )


def workflow_execution_status_value(name: str) -> str:
    """Return the DS workflow execution-status wire value for one enum name."""
    if name not in WORKFLOW_EXECUTION_STATUS_FACTS:
        raise KeyError(name)
    return name


def workflow_execution_status_is_final(state_name: str | None) -> bool:
    """Return whether one workflow execution-status name is final."""
    if state_name is None:
        return False
    facts = WORKFLOW_EXECUTION_STATUS_FACTS.get(state_name)
    return facts is not None and facts[1]


def task_execution_status_value(name: str) -> str:
    """Return the DS task execution-status wire value for one enum name."""
    return _task_execution_status_value(name)


def task_execute_type_value(name: str) -> str:
    """Return the DS task execute-type wire value for one enum name."""
    if name not in _TASK_EXECUTE_TYPE_NAMES:
        raise KeyError(name)
    return name


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
    "task_execute_type_value",
    "task_execution_status_value",
    "workflow_execution_status_info",
    "workflow_execution_status_is_final",
    "workflow_execution_status_value",
]
