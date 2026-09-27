"""Reviewed same-instance execution markers, independent of response timing."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject

# ProcessService[Impl]'s REPEAT_RUNNING and START_FAILURE_TASK_PROCESS cases;
# Since 3.3.1: ReRunWorkflowCommandHandler and
# WorkflowInstanceRecoverFailureTaskTrigger.
# EXECUTE_TASK increments only in the reviewed 3.2.x service implementation.
_REPLAY_VERSIONS = frozenset(
    {
        "1.3.9",
        "2.0.0",
        "2.0.1",
        "2.0.2",
        "2.0.3",
        "2.0.4",
        "2.0.5",
        "2.0.6",
        "2.0.7",
        "2.0.8",
        "2.0.9",
        "3.0.0",
        "3.0.1",
        "3.0.2",
        "3.0.3",
        "3.0.4",
        "3.0.5",
        "3.0.6",
        "3.1.0",
        "3.1.1",
        "3.1.2",
        "3.1.3",
        "3.1.4",
        "3.1.5",
        "3.1.6",
        "3.1.7",
        "3.1.8",
        "3.1.9",
        "3.2.0",
        "3.2.1",
        "3.2.2",
        "3.3.1",
        "3.3.2",
        "3.4.0",
        "3.4.1",
        "3.4.2",
        "3.4.3",
    }
)


def replay_baseline(
    *, ds_version: str, action: str, run_times: int
) -> JsonObject | None:
    """Return a baseline only when this action advances a known positive marker."""
    supported = (
        action in {"rerun", "recover-failed"} and ds_version in _REPLAY_VERSIONS
    ) or (action == "execute-task" and ds_version in {"3.2.0", "3.2.1", "3.2.2"})
    return {"run_times": run_times} if supported and run_times > 0 else None
