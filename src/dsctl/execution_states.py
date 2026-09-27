"""Reviewed DS state semantics shared by runtime validation and navigation.

Exact profiles still determine which actions exist. These facts describe only
the meaning of a returned state, never permissions or worker prerequisites.
"""

WORKFLOW_EXECUTION_STATUS_FACTS = {
    "SUBMITTED_SUCCESS": (False, False),
    "RUNNING_EXECUTION": (True, False),
    "READY_PAUSE": (True, False),
    "PAUSE": (False, True),
    "READY_STOP": (True, False),
    "STOP": (False, True),
    "FAILURE": (False, True),
    "SUCCESS": (False, True),
    "SERIAL_WAIT": (True, False),
    "FAILOVER": (False, False),
}
WORKFLOW_EXECUTION_FINISHED_STATES = frozenset(
    state for state, (_, final) in WORKFLOW_EXECUTION_STATUS_FACTS.items() if final
)
WORKFLOW_EXECUTION_STOPPABLE_STATES = frozenset(
    state
    for state, (can_stop, _) in WORKFLOW_EXECUTION_STATUS_FACTS.items()
    if can_stop
)
TASK_EXECUTION_FAILED_STATES = frozenset({"FAILURE", "NEED_FAULT_TOLERANCE", "KILL"})
TASK_EXECUTION_SUCCESS_STATES = frozenset({"SUCCESS", "FORCED_SUCCESS"})
TASK_EXECUTION_PAUSED_STATES = frozenset({"PAUSE"})
TASK_EXECUTION_RUNNING_STATES = frozenset({"RUNNING_EXECUTION"})
TASK_EXECUTION_QUEUED_STATES = frozenset(
    {"SUBMITTED_SUCCESS", "DISPATCH", "DELAY_EXECUTION"}
)
TASK_EXECUTION_FINISHED_STATES = (
    TASK_EXECUTION_FAILED_STATES
    | TASK_EXECUTION_SUCCESS_STATES
    | TASK_EXECUTION_PAUSED_STATES
)
TASK_EXECUTION_FORCE_SUCCESS_ALLOWED_STATES = TASK_EXECUTION_FAILED_STATES
