from __future__ import annotations

import shlex
from typing import NoReturn

from dsctl.cli_surface import TASK_RESOURCE, WORKFLOW_INSTANCE_RESOURCE
from dsctl.errors import (
    ApiResultError,
    InvalidStateError,
    MutationOutcomeUnknownError,
    NotFoundError,
    UserInputError,
)
from dsctl.services._discovery_commands import render_discovery_command
from dsctl.services._runtime_support import master_unavailable_error
from dsctl.services.workflow_instance._commands import (
    _wait_for_final_state_suggestion,
    _workflow_instance_action_command,
    _workflow_instance_command,
)
from dsctl.services.workflow_instance._types import (
    CHECK_WORKFLOW_TASK_RELATION_ERROR,
    DATA_IS_NOT_VALID,
    EXECUTE_NOT_DEFINE_TASK,
    EXECUTE_WORKFLOW_INSTANCE_ERROR,
    QUERY_WORKFLOW_INSTANCE_LIST_PAGING_ERROR,
    SUB_WORKFLOW_INSTANCE_NOT_EXIST,
    WORKFLOW_DEFINITION_NOT_RELEASE,
    WORKFLOW_INSTANCE_EXECUTING_COMMAND,
    WORKFLOW_INSTANCE_NOT_FINISHED,
    WORKFLOW_INSTANCE_NOT_SUB_WORKFLOW_INSTANCE,
    WORKFLOW_NODE_HAS_CYCLE,
    WORKFLOW_NODE_S_PARAMETER_INVALID,
)
from dsctl.upstream.runtime_instances import (
    WorkflowInstanceSnapshot,
    workflow_instance_stop_result_may_be_unknown,
)
from dsctl.upstream.serialization import enum_value


def _raise_workflow_instance_action_error(
    exc: ApiResultError,
    *,
    baseline: WorkflowInstanceSnapshot,
    workflow_instance_id: int,
    action: str,
    project_selector: str,
    task_code: int | None = None,
) -> None:
    command = _workflow_instance_action_command(
        action,
        workflow_instance_id=workflow_instance_id,
        project_selector=project_selector,
        task_code=task_code,
    )
    if (
        action == "stop"
        and exc.result_code == EXECUTE_WORKFLOW_INSTANCE_ERROR
        and workflow_instance_stop_result_may_be_unknown(baseline.ds_version)
    ):
        get_command = _workflow_instance_command(
            "get",
            workflow_instance_id=workflow_instance_id,
            project_selector=project_selector,
        )
        watch_command = _workflow_instance_command(
            "watch",
            workflow_instance_id=workflow_instance_id,
            project_selector=project_selector,
        )
        project_data = baseline.project.to_data()
        message = (
            "DolphinScheduler returned an ambiguous workflow-instance stop "
            "result; the requested stop may already have taken effect."
        )
        raise MutationOutcomeUnknownError(
            message,
            details={
                "resource": WORKFLOW_INSTANCE_RESOURCE,
                "id": workflow_instance_id,
                "action": action,
                "operation": "workflow-instance.stop",
                "ds_version": baseline.ds_version,
                "state_before": enum_value(baseline.state),
                "phase": "control_response",
                "mutation_may_have_applied": True,
                "request_replay_safe": False,
                "completed_stages": [],
                "failed_stage": "control_request",
                "project": project_data,
                "known_resources": {
                    "project": project_data,
                    "workflowInstance": {"id": workflow_instance_id},
                },
            },
            source=exc.source,
            suggestion=(
                f"Use `{get_command}` or `{watch_command}` to reconcile the "
                "current state. Do not blindly repeat the stop command."
            ),
        ) from exc
    if action in {"rerun", "recover-failed"}:
        unavailable = master_unavailable_error(
            exc,
            operation=f"workflow-instance.{action}",
            details={
                "resource": WORKFLOW_INSTANCE_RESOURCE,
                "id": workflow_instance_id,
                "action": action,
            },
            suggestion=(
                "Run `dsctl monitor server master` and wait until at least one "
                f"master is listed, then retry `{command}`."
            ),
        )
        if unavailable is not None:
            raise unavailable from exc
    if exc.result_code == WORKFLOW_INSTANCE_EXECUTING_COMMAND:
        get_command = _workflow_instance_command(
            "get",
            workflow_instance_id=workflow_instance_id,
            project_selector=project_selector,
        )
        watch_command = _workflow_instance_command(
            "watch",
            workflow_instance_id=workflow_instance_id,
            project_selector=project_selector,
        )
        message = (
            "This workflow instance is already executing another runtime control "
            "command."
        )
        raise InvalidStateError(
            message,
            details={
                "resource": WORKFLOW_INSTANCE_RESOURCE,
                "id": workflow_instance_id,
                "action": action,
            },
            suggestion=(
                f"Use `{get_command}` or `{watch_command}` to inspect "
                f"the current state, wait for the active runtime control command "
                f"to finish, "
                f"then retry `{command}`."
            ),
        ) from exc
    if exc.result_code == WORKFLOW_INSTANCE_NOT_FINISHED:
        message = (
            "This workflow instance must be in a final state before this action "
            "can proceed."
        )
        raise InvalidStateError(
            message,
            details={
                "resource": WORKFLOW_INSTANCE_RESOURCE,
                "id": workflow_instance_id,
                "action": action,
            },
            suggestion=_wait_for_final_state_suggestion(command),
        ) from exc
    if exc.result_code == WORKFLOW_DEFINITION_NOT_RELEASE:
        get_command = _workflow_instance_command(
            "get",
            workflow_instance_id=workflow_instance_id,
            project_selector=project_selector,
        )
        message = (
            "The workflow definition must be online before this action can proceed."
        )
        raise InvalidStateError(
            message,
            details={
                "resource": WORKFLOW_INSTANCE_RESOURCE,
                "id": workflow_instance_id,
                "action": action,
            },
            suggestion=(
                f"Use `{get_command}` to inspect "
                "the referenced workflow definition, bring that workflow online with "
                "`dsctl workflow online`, then retry the runtime action."
            ),
        ) from exc
    if task_code is not None and exc.result_code == EXECUTE_NOT_DEFINE_TASK:
        message = f"Task code {task_code} was not found"
        task_list_command = render_discovery_command(
            "task-instance.list",
            values={
                "project": project_selector,
                "workflow-instance": workflow_instance_id,
            },
        )
        raise NotFoundError(
            message,
            details={
                "resource": TASK_RESOURCE,
                "code": task_code,
                "workflow_instance_id": workflow_instance_id,
            },
            suggestion=(
                f"Run `{task_list_command}` to inspect tasks in this workflow "
                "instance, then retry with one returned task name or code."
            ),
        ) from exc
    raise exc


def _translate_workflow_instance_list_error(
    exc: ApiResultError,
    *,
    project: str,
    workflow: str | None,
    search: str | None,
    executor: str | None,
    host: str | None,
    start: str | None,
    end: str | None,
    state: str | None,
) -> ApiResultError | UserInputError:
    if exc.result_code != QUERY_WORKFLOW_INSTANCE_LIST_PAGING_ERROR:
        return exc
    filters = {
        key: value
        for key, value in {
            "project": project,
            "workflow": workflow,
            "search": search,
            "executor": executor,
            "host": host,
            "start": start,
            "end": end,
            "state": state,
        }.items()
        if value is not None
    }
    message = "DolphinScheduler rejected the workflow-instance list filters."
    example_command = shlex.join(
        (
            "dsctl",
            "workflow-instance",
            "list",
            "--project",
            project,
        )
    )
    return UserInputError(
        message,
        details={"filters": filters},
        suggestion=(
            f"Start from `{example_command}`, then add one filter at a time. "
            "Datetime filters use `YYYY-MM-DD HH:MM:SS`."
        ),
    )


def _parent_workflow_instance_not_found(
    *,
    sub_workflow_instance_id: int,
) -> NotFoundError:
    return NotFoundError(
        (
            "Parent workflow instance for sub-workflow instance id "
            f"{sub_workflow_instance_id} was not found."
        ),
        details={
            "resource": WORKFLOW_INSTANCE_RESOURCE,
            "id": sub_workflow_instance_id,
            "relation": "parent",
        },
    )


def _raise_parent_workflow_instance_lookup_error(
    exc: ApiResultError,
    *,
    sub_workflow_instance_id: int,
    project_selector: str,
) -> None:
    details = {
        "resource": WORKFLOW_INSTANCE_RESOURCE,
        "id": sub_workflow_instance_id,
        "relation": "parent",
    }
    if exc.result_code == WORKFLOW_INSTANCE_NOT_SUB_WORKFLOW_INSTANCE:
        get_command = _workflow_instance_command(
            "get",
            workflow_instance_id=sub_workflow_instance_id,
            project_selector=project_selector,
        )
        message = (
            f"Workflow instance id {sub_workflow_instance_id} is not a "
            "sub-workflow instance."
        )
        raise InvalidStateError(
            message,
            details=details,
            suggestion=(
                f"Use `{get_command}` for "
                "regular workflow instances; `parent` only applies to "
                "sub-workflow instances."
            ),
        ) from exc
    if exc.result_code == SUB_WORKFLOW_INSTANCE_NOT_EXIST:
        raise _parent_workflow_instance_not_found(
            sub_workflow_instance_id=sub_workflow_instance_id,
        ) from exc
    raise exc


def _raise_workflow_instance_edit_error(
    exc: ApiResultError,
    *,
    workflow_instance_id: int,
) -> NoReturn:
    if exc.result_code == WORKFLOW_NODE_HAS_CYCLE:
        message = "This workflow instance patch would introduce a dependency cycle."
        raise UserInputError(
            message,
            details={
                "resource": WORKFLOW_INSTANCE_RESOURCE,
                "id": workflow_instance_id,
            },
            suggestion=(
                "Inspect the compiled diff with `workflow-instance edit --dry-run`."
            ),
        ) from exc
    if exc.result_code in {
        DATA_IS_NOT_VALID,
        WORKFLOW_NODE_S_PARAMETER_INVALID,
        CHECK_WORKFLOW_TASK_RELATION_ERROR,
    }:
        message = (
            "This workflow instance patch compiled to an invalid DolphinScheduler "
            "runtime payload."
        )
        raise UserInputError(
            message,
            details={
                "resource": WORKFLOW_INSTANCE_RESOURCE,
                "id": workflow_instance_id,
                "result_code": exc.result_code,
                "result_message": exc.result_message,
            },
            suggestion=(
                "Inspect the compiled diff with `workflow-instance edit --dry-run`."
            ),
        ) from exc
    raise exc
