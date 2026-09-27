from __future__ import annotations

import shlex
from typing import TYPE_CHECKING, Literal, NoReturn

from dsctl.cli_surface import (
    WORKFLOW_RESOURCE,
)
from dsctl.command_contract import COMMAND_CATALOG
from dsctl.errors import (
    ApiHttpError,
    ApiResultError,
    ApiTransportError,
    ConflictError,
    DsctlError,
    InvalidStateError,
    NotFoundError,
    PermissionDeniedError,
    UserInputError,
)
from dsctl.output import (
    require_json_object,
)
from dsctl.services._discovery_commands import render_discovery_command
from dsctl.services._runtime_support import master_unavailable_error
from dsctl.services.version_resolution import selected_target_globals
from dsctl.upstream.resolver import (
    ResolvedProject,
    ResolvedWorkflow,
)

if TYPE_CHECKING:
    from dsctl.support.yaml_io import JsonObject
    from dsctl.upstream.definition_models import (
        ProjectRef,
        WorkflowRef,
    )
    from dsctl.upstream.workflows import WorkflowDeleteLineageError

from dsctl.services.workflow._commands import (
    _scoped_related_command,
    _scoped_workflow_command,
)
from dsctl.services.workflow._selection import (
    _identity_field,
)
from dsctl.services.workflow._types import (
    _USER_NO_OPERATION_PERMISSION,
    _WORKFLOW_CREATE_REVIEW_SUGGESTION,
    _WORKFLOW_EDIT_DRY_RUN_SUGGESTION,
    CHECK_WORKFLOW_TASK_RELATION_ERROR,
    DATA_IS_NOT_VALID,
    INTERNAL_SERVER_ERROR_ARGS,
    PROJECT_NOT_FOUND,
    START_WORKFLOW_INSTANCE_ERROR,
    TASK_DEFINE_NOT_EXIST,
    TASK_NAME_DUPLICATE_ERROR,
    WORKFLOW_DEFINITION_NAME_EXIST,
    WORKFLOW_DEFINITION_NOT_ALLOWED_EDIT,
    WORKFLOW_DEFINITION_NOT_EXIST,
    WORKFLOW_DEFINITION_NOT_RELEASE,
    WORKFLOW_NODE_HAS_CYCLE,
    WORKFLOW_NODE_S_PARAMETER_INVALID,
    WorkflowEditConstraintData,
)


def _raise_workflow_release_error(
    error: ApiResultError,
    *,
    project: ProjectRef,
    workflow: WorkflowRef,
    action: Literal["online", "offline"],
) -> None:
    if error.result_code == WORKFLOW_DEFINITION_NOT_EXIST:
        message = f"Workflow '{workflow.name}' does not exist."
        raise NotFoundError(
            message,
            details={
                "resource": WORKFLOW_RESOURCE,
                _identity_field(workflow): workflow.native.value,
                "name": workflow.name,
            },
        ) from error
    if action == "online" and _is_subworkflow_not_online_release_error(error):
        lineage_command = shlex.join(
            (
                "dsctl",
                "workflow",
                "lineage",
                "dependent-tasks",
                str(workflow.native.value),
                "--project",
                str(project.native.value),
            )
        )
        message = (
            "This workflow cannot be brought online until all referenced "
            "sub-workflows are already online."
        )
        raise InvalidStateError(
            message,
            details={
                "resource": WORKFLOW_RESOURCE,
                _identity_field(workflow): workflow.native.value,
                "name": workflow.name,
                "action": action,
            },
            suggestion=(
                f"Run `{lineage_command}` to inspect sub-workflow references, "
                "bring those sub-workflows online, then retry the original "
                "workflow online command."
            ),
        ) from error
    raise error


def _raise_workflow_release_refresh_error(
    error: DsctlError,
    *,
    project: ProjectRef,
    workflow: WorkflowRef,
    action: Literal["online", "offline"],
) -> NoReturn:
    details: JsonObject = {
        **error.details,
        "resource": WORKFLOW_RESOURCE,
        "project": project.name,
        _identity_field(project, prefix="project_"): project.native.value,
        _identity_field(workflow): workflow.native.value,
        "name": workflow.name,
        "action": action,
        "operation": f"workflow.{action}",
        "phase": "post_mutation_refresh",
        "mutation_applied": True,
    }
    if isinstance(error, ApiResultError):
        details["upstream_result_code"] = error.result_code
        details["upstream_result_message"] = error.result_message
    else:
        details["upstream_error_type"] = error.error_type
        details["upstream_error_details"] = error.details
    suggestion = (
        f"The workflow {action} mutation completed. Do not retry it before running "
        f"`dsctl workflow get {workflow.native.value} --project "
        f"{project.native.value}` and `dsctl schedule list --project "
        f"{project.native.value} --workflow {workflow.native.value}` "
        "to verify current state."
    )
    if isinstance(error, NotFoundError) or (
        isinstance(error, ApiResultError)
        and error.result_code == WORKFLOW_DEFINITION_NOT_EXIST
    ):
        message = (
            f"Workflow {action} completed, but workflow '{workflow.name}' could "
            "not be found during result refresh."
        )
        raise NotFoundError(
            message,
            details=details,
            source=error.source,
            suggestion=suggestion,
        ) from error
    if isinstance(error, PermissionDeniedError) or (
        isinstance(error, ApiResultError)
        and error.result_code == _USER_NO_OPERATION_PERMISSION
    ):
        message = (
            f"Workflow {action} completed, but current user cannot refresh "
            f"workflow '{workflow.name}'."
        )
        raise PermissionDeniedError(
            message,
            details=details,
            source=error.source,
            suggestion=suggestion,
        ) from error
    message = (
        f"Workflow {action} completed, but the CLI could not refresh the resulting "
        "workflow state."
    )
    if isinstance(error, ApiHttpError):
        raise ApiHttpError(
            message,
            status_code=error.status_code,
            body=error.body,
            details=details,
            suggestion=suggestion,
        ) from error
    raise ApiTransportError(
        message,
        details=details,
        suggestion=suggestion,
    ) from error


def _is_subworkflow_not_online_release_error(error: ApiResultError) -> bool:
    """Return whether one release error means a referenced sub-workflow is offline."""
    if error.result_code == WORKFLOW_DEFINITION_NOT_RELEASE:
        return True
    if error.result_code != INTERNAL_SERVER_ERROR_ARGS:
        return False
    normalized_message = error.result_message.casefold()
    return (
        "subworkflowdefinition" in normalized_message
        and "is not online" in normalized_message
    )


def _raise_workflow_run_error(
    error: ApiResultError,
    *,
    project: ProjectRef,
    workflow: WorkflowRef,
    operation: str = "workflow.run",
    partial_dispatch_possible: bool = False,
    retry_command: str | None = None,
) -> None:
    details: JsonObject = {
        "resource": WORKFLOW_RESOURCE,
        "project": project.name,
        _identity_field(project, prefix="project_"): project.native.value,
        _identity_field(workflow): workflow.native.value,
        "name": workflow.name,
    }
    if partial_dispatch_possible:
        details["partial_dispatch_possible"] = True
        instance_list_command = _scoped_related_command(
            "workflow-instance",
            "list",
            project=project,
            workflow=workflow,
            extra=("--all",),
        )
        suggestion = (
            "Run `dsctl monitor server master` and wait until at least one master "
            "is listed. Part of this parallel backfill may already have been "
            f"dispatched; use `{instance_list_command}` and compare `scheduleTime` "
            "with the "
            "requested range before deciding whether to retry the original "
            "backfill command."
        )
    else:
        retry_target = (
            "the original command" if retry_command is None else f"`{retry_command}`"
        )
        suggestion = (
            "Run `dsctl monitor server master` and wait until at least one "
            f"master is listed, then retry {retry_target}."
        )
    unavailable = master_unavailable_error(
        error,
        operation=operation,
        details=details,
        suggestion=suggestion,
    )
    if unavailable is not None:
        raise unavailable from error
    if (
        error.result_code == START_WORKFLOW_INSTANCE_ERROR
        and "call method to " in error.result_message.casefold()
        and error.result_message.casefold().rstrip().endswith(" failed")
    ):
        # Upstream NettyRemotingClient wraps both send and response failures.
        # A failed RPC reply therefore does not prove that dispatch did not occur.
        instance_list_command = _scoped_related_command(
            "workflow-instance",
            "list",
            project=project,
            workflow=workflow,
            extra=("--all",),
        )
        message = (
            "DolphinScheduler's API-to-master RPC failed; workflow execution "
            "may already have been dispatched."
        )
        raise ApiTransportError(
            message,
            details={
                **details,
                "operation": operation,
                "phase": "master_rpc",
                "mutation_may_have_applied": True,
                "request_replay_safe": False,
            },
            source=error.source,
            suggestion=(
                f"Inspect `{instance_list_command}` before retrying; for backfill, "
                "compare `scheduleTime` with the requested range. Run "
                "`dsctl monitor server master` and have the operator verify that "
                "the API can reach the registered master address. Do not blindly "
                "repeat the execution command."
            ),
        ) from error
    if _is_workflow_definition_not_online_run_error(error):
        online_command = _scoped_workflow_command(
            "online",
            project=project,
            workflow=workflow,
        )
        message = f"Workflow '{workflow.name}' must be online before it can be run."
        raise InvalidStateError(
            message,
            details={
                "resource": WORKFLOW_RESOURCE,
                "project": project.name,
                _identity_field(project, prefix="project_"): project.native.value,
                _identity_field(workflow): workflow.native.value,
                "name": workflow.name,
                "required_release_state": "ONLINE",
            },
            suggestion=(f"Run `{online_command}`, then retry the original command."),
        ) from error
    raise error


def _is_workflow_definition_not_online_run_error(error: ApiResultError) -> bool:
    if error.result_code != START_WORKFLOW_INSTANCE_ERROR:
        return False
    compacted_message = "".join(
        character
        for character in error.result_message.casefold()
        if character.isalnum()
    )
    return (
        "workflowdefinitionshouldbeonline" in compacted_message
        or "workflowdefinitionnotonline" in compacted_message
    )


def _raise_workflow_create_error(
    error: ApiResultError,
    *,
    project: ProjectRef,
    workflow_name: str,
) -> NoReturn:
    details: JsonObject = {
        "resource": WORKFLOW_RESOURCE,
        "project": project.name,
        _identity_field(project, prefix="project_"): project.native.value,
        "name": workflow_name,
        "mutation_applied": False,
    }
    if error.result_code == PROJECT_NOT_FOUND:
        message = f"Project '{project.name}' was not found."
        raise NotFoundError(
            message,
            details=details,
            suggestion=(
                f"Run `{render_discovery_command('project.list')}` to find an "
                "existing project, select it with --project or workflow.project, "
                "then retry create."
            ),
        ) from error
    if error.result_code == WORKFLOW_DEFINITION_NAME_EXIST:
        message = (
            f"Workflow '{workflow_name}' already exists in project '{project.name}'."
        )
        edit_command = COMMAND_CATALOG.render(
            "workflow.edit",
            global_values=selected_target_globals(),
            values={
                "workflow": workflow_name,
                "file": "FILE",
                "project": str(project.native.value),
                "dry-run": True,
            },
        )
        raise ConflictError(
            message,
            details=details,
            suggestion=(
                "Choose a unique workflow.name and retry create. To update the "
                f"existing workflow instead, run `{edit_command}` after replacing "
                "FILE with the intended full YAML path."
            ),
        ) from error
    if error.result_code == _USER_NO_OPERATION_PERMISSION:
        message = f"Current user cannot create workflow '{workflow_name}'."
        raise PermissionDeniedError(
            message,
            details=details,
            suggestion=(
                "Ask a project owner or administrator for workflow create "
                f"permission in project '{project.name}', then retry create."
            ),
        ) from error
    if error.result_code in {
        TASK_NAME_DUPLICATE_ERROR,
        DATA_IS_NOT_VALID,
        WORKFLOW_NODE_HAS_CYCLE,
        WORKFLOW_NODE_S_PARAMETER_INVALID,
        TASK_DEFINE_NOT_EXIST,
        CHECK_WORKFLOW_TASK_RELATION_ERROR,
    }:
        raise UserInputError(
            error.result_message,
            details=details,
            suggestion=_WORKFLOW_CREATE_REVIEW_SUGGESTION,
        ) from error
    raise error


def _raise_workflow_update_error(
    error: ApiResultError,
    *,
    project: ResolvedProject | ProjectRef,
    workflow: ResolvedWorkflow | WorkflowRef,
    workflow_name: str,
) -> NoReturn:
    project_identity = (
        ("project_code", project.code)
        if isinstance(project, ResolvedProject)
        else (_identity_field(project, prefix="project_"), project.native.value)
    )
    workflow_identity = (
        ("code", workflow.code)
        if isinstance(workflow, ResolvedWorkflow)
        else (_identity_field(workflow), workflow.native.value)
    )
    details: JsonObject = {
        "resource": WORKFLOW_RESOURCE,
        "project": project.name,
        project_identity[0]: project_identity[1],
        workflow_identity[0]: workflow_identity[1],
        "name": workflow_name,
        "mutation_applied": False,
    }
    if error.result_code == WORKFLOW_DEFINITION_NOT_EXIST:
        message = f"Workflow '{workflow.name}' does not exist."
        raise NotFoundError(message, details=details) from error
    if error.result_code == WORKFLOW_DEFINITION_NAME_EXIST:
        message = (
            f"Workflow '{workflow_name}' already exists in project '{project.name}'."
        )
        raise ConflictError(message, details=details) from error
    if error.result_code == _USER_NO_OPERATION_PERMISSION:
        message = f"Current user cannot edit workflow '{workflow.name}'."
        raise PermissionDeniedError(message, details=details) from error
    if error.result_code == WORKFLOW_DEFINITION_NOT_ALLOWED_EDIT:
        message = f"Workflow '{workflow.name}' must be offline before it can be edited."
        raise InvalidStateError(
            message,
            details=details,
            suggestion=(
                "Run `dsctl workflow offline WORKFLOW --project PROJECT` first, "
                "then retry `dsctl workflow edit`."
            ),
        ) from error
    if error.result_code in {
        TASK_NAME_DUPLICATE_ERROR,
        DATA_IS_NOT_VALID,
        WORKFLOW_NODE_HAS_CYCLE,
        WORKFLOW_NODE_S_PARAMETER_INVALID,
        TASK_DEFINE_NOT_EXIST,
        CHECK_WORKFLOW_TASK_RELATION_ERROR,
    }:
        raise UserInputError(
            error.result_message,
            details=details,
            suggestion=_WORKFLOW_EDIT_DRY_RUN_SUGGESTION,
        ) from error
    raise error


def _raise_workflow_delete_lineage_conflict(
    error: WorkflowDeleteLineageError,
    *,
    ds_version: str,
    project: ProjectRef,
    workflow: WorkflowRef,
) -> NoReturn:
    selectors = {
        "workflow": str(workflow.native.value),
        "project": str(project.native.value),
    }
    lineage_command = render_discovery_command(
        "workflow.lineage.get",
        values=selectors,
    )
    offline_command = render_discovery_command(
        "workflow.offline",
        values=selectors,
    )
    message = (
        f"Workflow '{workflow.name}' owns dependency lineage that DS {ds_version} "
        "does not clean up during deletion. No delete request was sent."
    )
    raise ConflictError(
        message,
        details={
            "resource": WORKFLOW_RESOURCE,
            "ds_version": ds_version,
            "reason": "upstream_lineage_cleanup_missing",
            "project": project.name,
            _identity_field(project, prefix="project_"): project.native.value,
            _identity_field(workflow): workflow.native.value,
            "name": workflow.name,
        },
        suggestion=(
            f"Inspect `{lineage_command}`. Keep the definition offline with "
            f"`{offline_command}` until an administrator addresses the upstream "
            "cleanup defect (fixed in DS 3.4.0). Removing DEPENDENT tasks or "
            "retrying --force does not clear existing lineage."
        ),
    ) from error


def _raise_workflow_delete_error(
    error: ApiResultError,
    *,
    project: ProjectRef,
    workflow: WorkflowRef,
) -> None:
    details: JsonObject = {
        "resource": WORKFLOW_RESOURCE,
        "project": project.name,
        _identity_field(project, prefix="project_"): project.native.value,
        _identity_field(workflow): workflow.native.value,
        "name": workflow.name,
    }
    if error.result_code == WORKFLOW_DEFINITION_NOT_EXIST:
        message = f"Workflow '{workflow.name}' does not exist."
        raise NotFoundError(message, details=details) from error
    if error.result_code == _USER_NO_OPERATION_PERMISSION:
        message = f"Current user cannot delete workflow '{workflow.name}'."
        raise PermissionDeniedError(message, details=details) from error
    if error.result_code == 50021:
        message = f"Workflow '{workflow.name}' must be offline before deletion."
        raise InvalidStateError(
            message,
            details=details,
            suggestion=(
                "Run `dsctl workflow offline WORKFLOW --project PROJECT` first, "
                "then retry `dsctl workflow delete --force`."
            ),
        ) from error
    if error.result_code == 50023:
        message = (
            f"Workflow '{workflow.name}' still has an online schedule and cannot "
            "be deleted."
        )
        raise InvalidStateError(
            message,
            details=details,
            suggestion=(
                "Run `dsctl schedule list --workflow WORKFLOW --project PROJECT` "
                "to find the attached schedule, take it offline with "
                "`dsctl schedule offline SCHEDULE_ID`, then retry "
                "`dsctl workflow delete --force`."
            ),
        ) from error
    if error.result_code == 10163:
        message = (
            f"Workflow '{workflow.name}' still has running workflow instances and "
            "cannot be deleted."
        )
        raise InvalidStateError(
            message,
            details=details,
            suggestion=(
                "Run `dsctl workflow-instance list --workflow WORKFLOW --project "
                "PROJECT` to inspect active instances, stop or wait for them to "
                "finish, then retry deletion."
            ),
        ) from error
    if error.result_code == 10193:
        message = (
            f"Workflow '{workflow.name}' is still referenced by other tasks and "
            "cannot be deleted."
        )
        raise ConflictError(
            message,
            details=details,
            suggestion=(
                "Run `dsctl workflow lineage dependent-tasks WORKFLOW --project "
                "PROJECT` to inspect references before retrying deletion."
            ),
        ) from error
    raise error


def _raise_workflow_edit_online_error(
    *,
    workflow: ResolvedWorkflow | WorkflowRef,
    workflow_state_constraint_details: list[WorkflowEditConstraintData],
) -> None:
    raise _workflow_edit_online_error(
        workflow=workflow,
        workflow_state_constraint_details=workflow_state_constraint_details,
    )


def _workflow_edit_online_error(
    *,
    workflow: ResolvedWorkflow | WorkflowRef,
    workflow_state_constraint_details: list[WorkflowEditConstraintData],
) -> InvalidStateError:
    """Build the shared apply and dry-run error for an online edit target."""
    primary_constraint = workflow_state_constraint_details[0]
    identity = (
        ("code", workflow.code)
        if isinstance(workflow, ResolvedWorkflow)
        else (_identity_field(workflow), workflow.native.value)
    )
    details: JsonObject = {
        "resource": WORKFLOW_RESOURCE,
        identity[0]: identity[1],
        "name": workflow.name,
        "required_release_state": "OFFLINE",
        "current_release_state": "ONLINE",
        "constraint_detail": require_json_object(
            primary_constraint,
            label="workflow edit constraint detail",
        ),
    }
    if len(workflow_state_constraint_details) > 1:
        schedule_constraint = workflow_state_constraint_details[1]
        details["schedule_impact"] = schedule_constraint["message"]
        details["schedule_impact_detail"] = require_json_object(
            schedule_constraint,
            label="workflow edit schedule impact detail",
        )
    message = f"Workflow '{workflow.name}' must be offline before it can be edited."
    return InvalidStateError(
        message,
        details=details,
        suggestion=(
            "Run `dsctl workflow offline WORKFLOW --project PROJECT` first, then "
            "retry `dsctl workflow edit`. Review `schedule_impact_detail` before "
            "taking an attached schedule offline."
        ),
    )
