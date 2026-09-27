from __future__ import annotations

import time

from dsctl.cli_surface import WORKFLOW_INSTANCE_RESOURCE
from dsctl.errors import (
    ExecutionFailedError,
    WaitTimeoutError,
)
from dsctl.output import CommandResult, require_json_object
from dsctl.services._validation import (
    require_non_negative_int,
    require_positive_int,
)
from dsctl.services.runtime import (
    run_with_bound_domain_service_runtime,
)
from dsctl.services.selection import (
    require_project_selection,
)
from dsctl.services.workflow_instance._selection import (
    _workflow_execution_status,
    _workflow_instance_resolved,
    get_workflow_instance,
)
from dsctl.services.workflow_instance._types import (
    DEFAULT_WATCH_INTERVAL_SECONDS,
    DEFAULT_WATCH_TIMEOUT_SECONDS,
    RuntimeInstanceServiceRuntime,
)
from dsctl.upstream.runtime_instances import (
    RUNTIME_INSTANCE_DOMAIN,
)
from dsctl.upstream.serialization import (
    enum_value,
    optional_text,
)


def watch_workflow_instance_result(
    workflow_instance_id: int,
    *,
    project: str | None = None,
    interval_seconds: int = DEFAULT_WATCH_INTERVAL_SECONDS,
    timeout_seconds: int = DEFAULT_WATCH_TIMEOUT_SECONDS,
    after_run_times: int | None = None,
    exit_status: bool = False,
    env_file: str | None = None,
) -> CommandResult:
    """Poll one workflow instance until it reaches a final state."""
    normalized_workflow_instance_id = require_positive_int(
        workflow_instance_id,
        label="workflow_instance_id",
    )
    normalized_interval_seconds = require_positive_int(
        interval_seconds,
        label="interval_seconds",
        input_hint="--interval-seconds",
    )
    normalized_timeout_seconds = require_non_negative_int(
        timeout_seconds,
        label="timeout_seconds",
        input_hint="--timeout-seconds",
    )
    if after_run_times is not None:
        require_non_negative_int(
            after_run_times, label="after_run_times", input_hint="--after-run-times"
        )
    return run_with_bound_domain_service_runtime(
        env_file,
        RUNTIME_INSTANCE_DOMAIN,
        _watch_workflow_instance_result,
        workflow_instance_id=normalized_workflow_instance_id,
        project=optional_text(project),
        interval_seconds=normalized_interval_seconds,
        timeout_seconds=normalized_timeout_seconds,
        after_run_times=after_run_times,
        exit_status=exit_status,
    )


def _watch_workflow_instance_result(
    runtime: RuntimeInstanceServiceRuntime,
    *,
    workflow_instance_id: int,
    project: str | None,
    interval_seconds: int,
    timeout_seconds: int,
    after_run_times: int | None = None,
    exit_status: bool = False,
) -> CommandResult:
    selected_project = require_project_selection(project, runtime=runtime)
    started_at = time.monotonic()
    while True:
        payload = get_workflow_instance(
            runtime,
            project_selector=selected_project.value,
            workflow_instance_id=workflow_instance_id,
        )
        status = _workflow_execution_status(payload.state)
        current_execution_observed = (
            after_run_times is None or payload.runTimes > after_run_times
        )
        if current_execution_observed and status is not None and status.final_state:
            state_name = enum_value(payload.state)
            return CommandResult(
                data=require_json_object(
                    payload.to_data(),
                    label="workflow-instance data",
                ),
                resolved=require_json_object(
                    _workflow_instance_resolved(
                        workflow_instance_id,
                        project=payload.project,
                        selected_project=selected_project,
                    ),
                    label="workflow-instance resolved",
                ),
                failure=(
                    ExecutionFailedError(
                        f"Workflow instance {workflow_instance_id} "
                        f"finished in {state_name}.",
                        details={"state": state_name, "id": workflow_instance_id},
                        suggestion=(
                            "Inspect the instance digest and failed task logs "
                            "before retrying."
                        ),
                    )
                    if exit_status and state_name != "SUCCESS"
                    else None
                ),
            )
        if timeout_seconds > 0 and (time.monotonic() - started_at) >= timeout_seconds:
            message = (
                "Timed out waiting for the workflow instance to reach a final state."
            )
            raise WaitTimeoutError(
                message,
                details={
                    "resource": WORKFLOW_INSTANCE_RESOURCE,
                    "id": workflow_instance_id,
                    "last_state": enum_value(payload.state),
                    "timeout_seconds": timeout_seconds,
                    "last_run_times": payload.runTimes,
                    "after_run_times": after_run_times,
                    "new_execution_observed": current_execution_observed,
                },
                suggestion=(
                    "Retry with a larger --timeout-seconds value or inspect the "
                    "current state with "
                    f"`dsctl workflow-instance get {workflow_instance_id}`."
                ),
            )
        time.sleep(interval_seconds)
