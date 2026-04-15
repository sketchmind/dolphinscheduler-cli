from __future__ import annotations

import time
from typing import TYPE_CHECKING, TypedDict

from dsctl.cli_surface import TASK_RESOURCE, WORKFLOW_INSTANCE_RESOURCE
from dsctl.errors import (
    ApiResultError,
    ApiTransportError,
    InvalidStateError,
    NotFoundError,
    UserInputError,
    WaitTimeoutError,
)
from dsctl.output import CommandResult, dry_run_result, require_json_object
from dsctl.services._runtime_support import (
    get_workflow_instance,
    require_workflow_definition_code,
    require_workflow_instance_project_code,
)
from dsctl.services._serialization import (
    WorkflowInstanceData,
    enum_value,
    optional_text,
    serialize_task_instance,
    serialize_workflow_instance,
)
from dsctl.services._validation import (
    require_non_empty_text,
    require_non_negative_int,
    require_positive_int,
)
from dsctl.services._workflow_instance_digest import (
    digest_workflow_instance as _digest_workflow_instance,
)
from dsctl.services._workflow_mutation import (
    compile_workflow_mutation_plan,
    load_workflow_patch_or_error,
)
from dsctl.services.pagination import (
    DEFAULT_PAGE_SIZE,
    MAX_AUTO_EXHAUST_PAGES,
    PageData,
    collect_all_pages,
    requested_page_data,
)
from dsctl.services.resolver import ResolvedProjectData, ResolvedTaskData
from dsctl.services.resolver import project as resolve_project
from dsctl.services.resolver import task as resolve_task
from dsctl.services.runtime import ServiceRuntime, run_with_service_runtime
from dsctl.upstream.runtime_enums import (
    WORKFLOW_EXECUTION_FAILURE_STATE,
    WORKFLOW_EXECUTION_STOP_STATE,
    WorkflowExecutionStatusInfo,
    workflow_execution_status_info,
    workflow_execution_status_value,
)

if TYPE_CHECKING:
    from pathlib import Path

    from dsctl.models.workflow_patch import WorkflowPatchSpec
    from dsctl.services._workflow_compile import WorkflowUpdatePayload
    from dsctl.services._workflow_patch import WorkflowPatchDiffData
    from dsctl.support.yaml_io import JsonObject
    from dsctl.upstream.protocol import StringEnumValue


class WorkflowInstanceSelectionData(TypedDict):
    """Resolved workflow-instance selector emitted in JSON envelopes."""

    id: int


WorkflowInstancePageData = PageData[WorkflowInstanceData]
DEFAULT_WATCH_INTERVAL_SECONDS = 5
DEFAULT_WATCH_TIMEOUT_SECONDS = 600
WORKFLOW_INSTANCE_EXECUTING_COMMAND = 50009
WORKFLOW_DEFINITION_NOT_RELEASE = 50004
WORKFLOW_INSTANCE_NOT_FINISHED = 50071
WORKFLOW_INSTANCE_NOT_SUB_WORKFLOW_INSTANCE = 50010
SUB_WORKFLOW_INSTANCE_NOT_EXIST = 50007
EXECUTE_NOT_DEFINE_TASK = 10206
DATA_IS_NOT_VALID = 50017
WORKFLOW_NODE_HAS_CYCLE = 50019
WORKFLOW_NODE_S_PARAMETER_INVALID = 50020
CHECK_WORKFLOW_TASK_RELATION_ERROR = 50036
WORKFLOW_INSTANCE_PATCH_SUPPORTED_WORKFLOW_FIELDS = frozenset(
    {"global_params", "timeout"}
)


class WorkflowInstanceExecuteTaskResolved(TypedDict):
    """Resolved execute-task metadata emitted in JSON envelopes."""

    workflowInstance: WorkflowInstanceSelectionData
    task: ResolvedTaskData
    scope: str


class WorkflowInstanceActionWarningDetail(TypedDict):
    """Structured warning emitted after one runtime action request."""

    code: str
    action: str
    message: str
    current_state: str
    expect_non_final: bool
    target_state: str | None


class WorkflowInstanceParentData(TypedDict):
    """DS-native parent relation payload emitted for one sub-workflow instance."""

    parentWorkflowInstance: int


class WorkflowInstanceUpdateResolved(TypedDict):
    """Resolved metadata emitted for one workflow-instance edit request."""

    workflowInstance: WorkflowInstanceSelectionData
    project: ResolvedProjectData
    workflow: JsonObject
    patch_file: str
    syncDefine: bool


class WorkflowInstanceUpdateNoChangeWarningDetail(TypedDict):
    """Structured warning emitted when one instance patch changes nothing."""

    code: str
    message: str
    no_change: bool
    request_sent: bool


def list_workflow_instances_result(
    *,
    page_no: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
    all_pages: bool = False,
    project: str | None = None,
    workflow: str | None = None,
    state: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """List workflow instances using explicit runtime filters."""
    normalized_project = optional_text(project)
    normalized_workflow = optional_text(workflow)
    normalized_state = _normalized_workflow_instance_state(state)
    normalized_page_no = require_positive_int(page_no, label="page_no")
    normalized_page_size = require_positive_int(page_size, label="page_size")
    return run_with_service_runtime(
        env_file,
        _list_workflow_instances_result,
        page_no=normalized_page_no,
        page_size=normalized_page_size,
        all_pages=all_pages,
        project=normalized_project,
        workflow=normalized_workflow,
        state=normalized_state,
    )


def get_workflow_instance_result(
    workflow_instance_id: int,
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Get one workflow instance by id."""
    normalized_workflow_instance_id = require_positive_int(
        workflow_instance_id,
        label="workflow_instance_id",
    )
    return run_with_service_runtime(
        env_file,
        _get_workflow_instance_result,
        workflow_instance_id=normalized_workflow_instance_id,
    )


def digest_workflow_instance_result(
    workflow_instance_id: int,
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Return one compact workflow-instance runtime digest."""
    normalized_workflow_instance_id = require_positive_int(
        workflow_instance_id,
        label="workflow_instance_id",
    )
    return run_with_service_runtime(
        env_file,
        _digest_workflow_instance_result,
        workflow_instance_id=normalized_workflow_instance_id,
    )


def get_parent_workflow_instance_result(
    sub_workflow_instance_id: int,
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Return the parent workflow instance for one sub-workflow instance."""
    normalized_sub_workflow_instance_id = require_positive_int(
        sub_workflow_instance_id,
        label="sub_workflow_instance_id",
    )
    return run_with_service_runtime(
        env_file,
        _get_parent_workflow_instance_result,
        sub_workflow_instance_id=normalized_sub_workflow_instance_id,
    )


def stop_workflow_instance_result(
    workflow_instance_id: int,
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Request stop for one workflow instance and return the refreshed payload."""
    normalized_workflow_instance_id = require_positive_int(
        workflow_instance_id,
        label="workflow_instance_id",
    )
    return run_with_service_runtime(
        env_file,
        _stop_workflow_instance_result,
        workflow_instance_id=normalized_workflow_instance_id,
    )


def watch_workflow_instance_result(
    workflow_instance_id: int,
    *,
    interval_seconds: int = DEFAULT_WATCH_INTERVAL_SECONDS,
    timeout_seconds: int = DEFAULT_WATCH_TIMEOUT_SECONDS,
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
    )
    normalized_timeout_seconds = require_non_negative_int(
        timeout_seconds,
        label="timeout_seconds",
    )
    return run_with_service_runtime(
        env_file,
        _watch_workflow_instance_result,
        workflow_instance_id=normalized_workflow_instance_id,
        interval_seconds=normalized_interval_seconds,
        timeout_seconds=normalized_timeout_seconds,
    )


def rerun_workflow_instance_result(
    workflow_instance_id: int,
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Request rerun for one finished workflow instance."""
    normalized_workflow_instance_id = require_positive_int(
        workflow_instance_id,
        label="workflow_instance_id",
    )
    return run_with_service_runtime(
        env_file,
        _rerun_workflow_instance_result,
        workflow_instance_id=normalized_workflow_instance_id,
    )


def recover_failed_workflow_instance_result(
    workflow_instance_id: int,
    *,
    env_file: str | None = None,
) -> CommandResult:
    """Recover one failed workflow instance from failed tasks."""
    normalized_workflow_instance_id = require_positive_int(
        workflow_instance_id,
        label="workflow_instance_id",
    )
    return run_with_service_runtime(
        env_file,
        _recover_failed_workflow_instance_result,
        workflow_instance_id=normalized_workflow_instance_id,
    )


def execute_task_in_workflow_instance_result(
    workflow_instance_id: int,
    *,
    task: str,
    scope: str = "self",
    env_file: str | None = None,
) -> CommandResult:
    """Execute one task within one finished workflow instance."""
    normalized_workflow_instance_id = require_positive_int(
        workflow_instance_id,
        label="workflow_instance_id",
    )
    normalized_task = require_non_empty_text(task, label="task")
    normalized_scope = _normalized_execute_task_scope(scope)
    return run_with_service_runtime(
        env_file,
        _execute_task_in_workflow_instance_result,
        workflow_instance_id=normalized_workflow_instance_id,
        task_identifier=normalized_task,
        scope=normalized_scope,
    )


def update_workflow_instance_result(
    workflow_instance_id: int,
    *,
    patch: Path,
    sync_definition: bool = False,
    dry_run: bool = False,
    env_file: str | None = None,
) -> CommandResult:
    """Edit one finished workflow instance DAG from a YAML patch file."""
    normalized_workflow_instance_id = require_positive_int(
        workflow_instance_id,
        label="workflow_instance_id",
    )
    workflow_patch = load_workflow_patch_or_error(patch)
    _validate_workflow_instance_patch_support(workflow_patch)
    return run_with_service_runtime(
        env_file,
        _update_workflow_instance_result,
        workflow_instance_id=normalized_workflow_instance_id,
        patch_file=patch,
        patch=workflow_patch,
        sync_definition=sync_definition,
        dry_run=dry_run,
    )


def _list_workflow_instances_result(
    runtime: ServiceRuntime,
    *,
    page_no: int,
    page_size: int,
    all_pages: bool,
    project: str | None,
    workflow: str | None,
    state: str | None,
) -> CommandResult:
    data = require_json_object(
        requested_page_data(
            lambda current_page_no, current_page_size: (
                runtime.upstream.workflow_instances.list(
                    page_no=current_page_no,
                    page_size=current_page_size,
                    project_name=project,
                    workflow_name=workflow,
                    state=state,
                )
            ),
            page_no=page_no,
            page_size=page_size,
            all_pages=all_pages,
            resource=WORKFLOW_INSTANCE_RESOURCE,
            serialize_item=serialize_workflow_instance,
            max_pages=MAX_AUTO_EXHAUST_PAGES,
        ),
        label="workflow-instance list data",
    )
    resolved: dict[str, int | str | None] = {
        "page_no": page_no,
        "page_size": page_size,
        "all": all_pages,
    }
    if project is not None:
        resolved["project"] = project
    if workflow is not None:
        resolved["workflow"] = workflow
    if state is not None:
        resolved["state"] = state
    return CommandResult(
        data=data,
        resolved=require_json_object(
            resolved,
            label="workflow-instance list resolved",
        ),
    )


def _get_workflow_instance_result(
    runtime: ServiceRuntime,
    *,
    workflow_instance_id: int,
) -> CommandResult:
    payload = get_workflow_instance(
        runtime,
        workflow_instance_id=workflow_instance_id,
    )
    return CommandResult(
        data=require_json_object(
            serialize_workflow_instance(payload),
            label="workflow-instance data",
        ),
        resolved=require_json_object(
            _workflow_instance_resolved(workflow_instance_id),
            label="workflow-instance resolved",
        ),
    )


def _digest_workflow_instance_result(
    runtime: ServiceRuntime,
    *,
    workflow_instance_id: int,
) -> CommandResult:
    payload = get_workflow_instance(
        runtime,
        workflow_instance_id=workflow_instance_id,
    )
    project_code = require_workflow_instance_project_code(
        payload.projectCode,
    )
    task_pages = collect_all_pages(
        lambda page_no, page_size: runtime.upstream.task_instances.list(
            workflow_instance_id=workflow_instance_id,
            project_code=project_code,
            page_no=page_no,
            page_size=page_size,
        ),
        page_no=1,
        page_size=DEFAULT_PAGE_SIZE,
        max_pages=MAX_AUTO_EXHAUST_PAGES,
    )
    workflow_instance_data = serialize_workflow_instance(payload)
    task_data = [serialize_task_instance(task) for task in task_pages.items]
    return CommandResult(
        data=require_json_object(
            _digest_workflow_instance(
                workflow_instance=workflow_instance_data,
                tasks=task_data,
            ),
            label="workflow-instance digest data",
        ),
        resolved=require_json_object(
            _workflow_instance_resolved(workflow_instance_id),
            label="workflow-instance resolved",
        ),
    )


def _get_parent_workflow_instance_result(
    runtime: ServiceRuntime,
    *,
    sub_workflow_instance_id: int,
) -> CommandResult:
    payload = get_workflow_instance(
        runtime,
        workflow_instance_id=sub_workflow_instance_id,
    )
    project_code = require_workflow_instance_project_code(payload.projectCode)
    try:
        relation = runtime.upstream.workflow_instances.parent_instance_by_sub_workflow(
            project_code=project_code,
            sub_workflow_instance_id=sub_workflow_instance_id,
        )
    except ApiResultError as exc:
        _raise_parent_workflow_instance_lookup_error(
            exc,
            sub_workflow_instance_id=sub_workflow_instance_id,
        )
    parent_workflow_instance_id = relation.parentWorkflowInstance
    if (
        not isinstance(parent_workflow_instance_id, int)
        or parent_workflow_instance_id <= 0
    ):
        raise _parent_workflow_instance_not_found(
            sub_workflow_instance_id=sub_workflow_instance_id,
        )
    return CommandResult(
        data=require_json_object(
            WorkflowInstanceParentData(
                parentWorkflowInstance=parent_workflow_instance_id
            ),
            label="workflow-instance parent data",
        ),
        resolved=require_json_object(
            {
                "subWorkflowInstance": WorkflowInstanceSelectionData(
                    id=sub_workflow_instance_id
                )
            },
            label="workflow-instance parent resolved",
        ),
    )


def _stop_workflow_instance_result(
    runtime: ServiceRuntime,
    *,
    workflow_instance_id: int,
) -> CommandResult:
    payload = get_workflow_instance(
        runtime,
        workflow_instance_id=workflow_instance_id,
    )
    status = _workflow_execution_status(payload.state)
    state_name = enum_value(payload.state)
    if status is None or not status.can_stop:
        message = "This workflow instance cannot be stopped in its current state."
        raise InvalidStateError(
            message,
            details={
                "resource": WORKFLOW_INSTANCE_RESOURCE,
                "id": workflow_instance_id,
                "state": state_name,
            },
            suggestion=(
                "Use `dsctl workflow-instance get ID` or "
                "`dsctl workflow-instance watch ID` to inspect the current "
                "state before retrying stop."
            ),
        )
    runtime.upstream.workflow_instances.stop(workflow_instance_id=workflow_instance_id)
    refreshed_payload = get_workflow_instance(
        runtime,
        workflow_instance_id=workflow_instance_id,
    )
    warnings = _action_warning(
        "stop",
        refreshed_payload.state,
        expect_non_final=False,
        target_state=WORKFLOW_EXECUTION_STOP_STATE,
    )
    return CommandResult(
        data=require_json_object(
            serialize_workflow_instance(refreshed_payload),
            label="workflow-instance data",
        ),
        resolved=require_json_object(
            _workflow_instance_resolved(workflow_instance_id),
            label="workflow-instance resolved",
        ),
        warnings=warnings,
        warning_details=_action_warning_details(
            "stop",
            refreshed_payload.state,
            expect_non_final=False,
            target_state=WORKFLOW_EXECUTION_STOP_STATE,
        ),
    )


def _wait_for_final_state_suggestion(command: str) -> str:
    return (
        "Wait for the workflow instance to reach a final state, then retry "
        f"`{command}`."
    )


def _workflow_instance_action_command(action: str) -> str:
    if action == "execute-task":
        return "dsctl workflow-instance execute-task ID --task TASK"
    return f"dsctl workflow-instance {action} ID"


def _rerun_workflow_instance_result(
    runtime: ServiceRuntime,
    *,
    workflow_instance_id: int,
) -> CommandResult:
    payload = get_workflow_instance(
        runtime,
        workflow_instance_id=workflow_instance_id,
    )
    status = _workflow_execution_status(payload.state)
    if status is None or not status.final_state:
        message = "This workflow instance must be in a final state before rerun."
        raise InvalidStateError(
            message,
            details={
                "resource": WORKFLOW_INSTANCE_RESOURCE,
                "id": workflow_instance_id,
                "state": enum_value(payload.state),
            },
            suggestion=_wait_for_final_state_suggestion(
                "dsctl workflow-instance rerun ID"
            ),
        )
    try:
        runtime.upstream.workflow_instances.rerun(
            workflow_instance_id=workflow_instance_id
        )
    except ApiResultError as exc:
        _raise_workflow_instance_action_error(
            exc,
            workflow_instance_id=workflow_instance_id,
            action="rerun",
        )
    refreshed_payload = get_workflow_instance(
        runtime,
        workflow_instance_id=workflow_instance_id,
    )
    return CommandResult(
        data=require_json_object(
            serialize_workflow_instance(refreshed_payload),
            label="workflow-instance data",
        ),
        resolved=require_json_object(
            _workflow_instance_resolved(workflow_instance_id),
            label="workflow-instance resolved",
        ),
        warnings=_action_warning(
            "rerun",
            refreshed_payload.state,
            expect_non_final=True,
        ),
        warning_details=_action_warning_details(
            "rerun",
            refreshed_payload.state,
            expect_non_final=True,
        ),
    )


def _recover_failed_workflow_instance_result(
    runtime: ServiceRuntime,
    *,
    workflow_instance_id: int,
) -> CommandResult:
    payload = get_workflow_instance(
        runtime,
        workflow_instance_id=workflow_instance_id,
    )
    status = _workflow_execution_status(payload.state)
    if status is None or status.value != WORKFLOW_EXECUTION_FAILURE_STATE:
        message = (
            "This workflow instance must be in FAILURE state before recover-failed."
        )
        raise InvalidStateError(
            message,
            details={
                "resource": WORKFLOW_INSTANCE_RESOURCE,
                "id": workflow_instance_id,
                "state": enum_value(payload.state),
            },
            suggestion=(
                "Use `dsctl workflow-instance get ID` or "
                "`dsctl workflow-instance watch ID` to confirm the instance is in "
                "FAILURE before retrying `recover-failed`."
            ),
        )
    try:
        runtime.upstream.workflow_instances.recover_failed(
            workflow_instance_id=workflow_instance_id
        )
    except ApiResultError as exc:
        _raise_workflow_instance_action_error(
            exc,
            workflow_instance_id=workflow_instance_id,
            action="recover-failed",
        )
    refreshed_payload = get_workflow_instance(
        runtime,
        workflow_instance_id=workflow_instance_id,
    )
    return CommandResult(
        data=require_json_object(
            serialize_workflow_instance(refreshed_payload),
            label="workflow-instance data",
        ),
        resolved=require_json_object(
            _workflow_instance_resolved(workflow_instance_id),
            label="workflow-instance resolved",
        ),
        warnings=_action_warning(
            "recover-failed",
            refreshed_payload.state,
            expect_non_final=True,
        ),
        warning_details=_action_warning_details(
            "recover-failed",
            refreshed_payload.state,
            expect_non_final=True,
        ),
    )


def _execute_task_in_workflow_instance_result(
    runtime: ServiceRuntime,
    *,
    workflow_instance_id: int,
    task_identifier: str,
    scope: str,
) -> CommandResult:
    payload = get_workflow_instance(
        runtime,
        workflow_instance_id=workflow_instance_id,
    )
    status = _workflow_execution_status(payload.state)
    if status is None or not status.final_state:
        message = "This workflow instance must be in a final state before execute-task."
        raise InvalidStateError(
            message,
            details={
                "resource": WORKFLOW_INSTANCE_RESOURCE,
                "id": workflow_instance_id,
                "state": enum_value(payload.state),
            },
            suggestion=_wait_for_final_state_suggestion(
                "dsctl workflow-instance execute-task ID --task TASK"
            ),
        )
    project_code = require_workflow_instance_project_code(
        payload.projectCode,
    )
    workflow_code = require_workflow_definition_code(
        payload.workflowDefinitionCode,
    )
    resolved_task = resolve_task(
        task_identifier,
        adapter=runtime.upstream.tasks,
        project_code=project_code,
        workflow_code=workflow_code,
    )
    try:
        runtime.upstream.workflow_instances.execute_task(
            project_code=project_code,
            workflow_instance_id=workflow_instance_id,
            task_code=resolved_task.code,
            scope=scope,
        )
    except ApiResultError as exc:
        _raise_workflow_instance_action_error(
            exc,
            workflow_instance_id=workflow_instance_id,
            action="execute-task",
            task_code=resolved_task.code,
        )
    refreshed_payload = get_workflow_instance(
        runtime,
        workflow_instance_id=workflow_instance_id,
    )
    return CommandResult(
        data=require_json_object(
            serialize_workflow_instance(refreshed_payload),
            label="workflow-instance data",
        ),
        resolved=require_json_object(
            WorkflowInstanceExecuteTaskResolved(
                workflowInstance=WorkflowInstanceSelectionData(id=workflow_instance_id),
                task=resolved_task.to_data(),
                scope=scope,
            ),
            label="workflow-instance execute-task resolved",
        ),
        warnings=_action_warning(
            "execute-task",
            refreshed_payload.state,
            expect_non_final=True,
        ),
        warning_details=_action_warning_details(
            "execute-task",
            refreshed_payload.state,
            expect_non_final=True,
        ),
    )


def _update_workflow_instance_result(
    runtime: ServiceRuntime,
    *,
    workflow_instance_id: int,
    patch_file: Path,
    patch: WorkflowPatchSpec,
    sync_definition: bool,
    dry_run: bool,
) -> CommandResult:
    payload = get_workflow_instance(
        runtime,
        workflow_instance_id=workflow_instance_id,
    )
    status = _workflow_execution_status(payload.state)
    if status is None or not status.final_state:
        message = "This workflow instance must be in a final state before update."
        raise InvalidStateError(
            message,
            details={
                "resource": WORKFLOW_INSTANCE_RESOURCE,
                "id": workflow_instance_id,
                "state": enum_value(payload.state),
            },
            suggestion=_wait_for_final_state_suggestion(
                "dsctl workflow-instance update ID --patch PATCH"
            ),
        )
    dag = payload.dagData
    if dag is None:
        message = "Workflow instance payload was missing dagData"
        raise ApiTransportError(
            message,
            details={
                "resource": WORKFLOW_INSTANCE_RESOURCE,
                "id": workflow_instance_id,
            },
        )
    project_code = require_workflow_instance_project_code(payload.projectCode)
    resolved_project = resolve_project(
        str(project_code),
        adapter=runtime.upstream.projects,
    )
    mutation = compile_workflow_mutation_plan(
        dag,
        project=resolved_project,
        patch=patch,
        release_state=None,
    )
    compiled_payload = mutation.payload
    merged_spec = mutation.merged_spec
    diff = mutation.diff
    form_data = _workflow_instance_update_form_data(
        compiled_payload,
        sync_definition=sync_definition,
    )
    resolved = _workflow_instance_update_resolved(
        workflow_instance_id=workflow_instance_id,
        project=resolved_project.to_data(),
        workflow=_workflow_instance_resolved_workflow(
            workflow_code=require_workflow_definition_code(
                payload.workflowDefinitionCode
            ),
            workflow_name=merged_spec.workflow.name,
            workflow_version=payload.workflowDefinitionVersion,
        ),
        patch_file=str(patch_file),
        sync_definition=sync_definition,
    )
    has_changes = mutation.has_changes

    if dry_run:
        return _workflow_instance_update_dry_run_result(
            project_code=project_code,
            workflow_instance_id=workflow_instance_id,
            form_data=form_data,
            resolved=resolved,
            diff=diff,
            no_change=not has_changes,
        )

    if not has_changes:
        no_change_warning = (
            "patch produced no persistent workflow instance change; no update "
            "request was sent"
        )
        return CommandResult(
            data=require_json_object(
                serialize_workflow_instance(payload),
                label="workflow-instance data",
            ),
            resolved=require_json_object(
                resolved,
                label="workflow-instance update resolved",
            ),
            warnings=[no_change_warning],
            warning_details=[
                require_json_object(
                    WorkflowInstanceUpdateNoChangeWarningDetail(
                        code="workflow_instance_update_no_persistent_change",
                        message=no_change_warning,
                        no_change=True,
                        request_sent=False,
                    ),
                    label="workflow-instance update warning detail",
                )
            ],
        )

    try:
        saved_workflow = runtime.upstream.workflow_instances.update(
            project_code=project_code,
            workflow_instance_id=workflow_instance_id,
            task_relation_json=compiled_payload["taskRelationJson"],
            task_definition_json=compiled_payload["taskDefinitionJson"],
            sync_define=sync_definition,
            global_params=compiled_payload["globalParams"],
            locations=compiled_payload["locations"],
            timeout=compiled_payload["timeout"],
        )
    except ApiResultError as exc:
        _raise_workflow_instance_update_error(
            exc,
            workflow_instance_id=workflow_instance_id,
        )

    refreshed_payload = get_workflow_instance(
        runtime,
        workflow_instance_id=workflow_instance_id,
    )
    updated_resolved = _workflow_instance_update_resolved(
        workflow_instance_id=workflow_instance_id,
        project=resolved_project.to_data(),
        workflow=_workflow_instance_resolved_workflow(
            workflow_code=saved_workflow.code,
            workflow_name=saved_workflow.name,
            workflow_version=saved_workflow.version,
        ),
        patch_file=str(patch_file),
        sync_definition=sync_definition,
    )
    return CommandResult(
        data=require_json_object(
            serialize_workflow_instance(refreshed_payload),
            label="workflow-instance data",
        ),
        resolved=require_json_object(
            updated_resolved,
            label="workflow-instance update resolved",
        ),
    )


def _watch_workflow_instance_result(
    runtime: ServiceRuntime,
    *,
    workflow_instance_id: int,
    interval_seconds: int,
    timeout_seconds: int,
) -> CommandResult:
    started_at = time.monotonic()
    while True:
        payload = get_workflow_instance(
            runtime,
            workflow_instance_id=workflow_instance_id,
        )
        status = _workflow_execution_status(payload.state)
        if status is not None and status.final_state:
            return CommandResult(
                data=require_json_object(
                    serialize_workflow_instance(payload),
                    label="workflow-instance data",
                ),
                resolved=require_json_object(
                    _workflow_instance_resolved(workflow_instance_id),
                    label="workflow-instance resolved",
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
                },
                suggestion=(
                    "Retry with a larger --timeout-seconds value or inspect the "
                    "current state with "
                    f"`workflow-instance get {workflow_instance_id}`."
                ),
            )
        time.sleep(interval_seconds)


def _workflow_instance_resolved(
    workflow_instance_id: int,
) -> dict[str, WorkflowInstanceSelectionData]:
    return {"workflowInstance": WorkflowInstanceSelectionData(id=workflow_instance_id)}


def _workflow_execution_status(
    value: StringEnumValue | None,
) -> WorkflowExecutionStatusInfo | None:
    return workflow_execution_status_info(value)


def _action_warning(
    action: str,
    state: StringEnumValue | None,
    *,
    expect_non_final: bool,
    target_state: str | None = None,
) -> list[str]:
    detail = _action_warning_detail(
        action,
        state,
        expect_non_final=expect_non_final,
        target_state=target_state,
    )
    if detail is None:
        return []
    return [detail["message"]]


def _action_warning_details(
    action: str,
    state: StringEnumValue | None,
    *,
    expect_non_final: bool,
    target_state: str | None = None,
) -> list[WorkflowInstanceActionWarningDetail]:
    detail = _action_warning_detail(
        action,
        state,
        expect_non_final=expect_non_final,
        target_state=target_state,
    )
    if detail is None:
        return []
    return [detail]


def _action_warning_detail(
    action: str,
    state: StringEnumValue | None,
    *,
    expect_non_final: bool,
    target_state: str | None,
) -> WorkflowInstanceActionWarningDetail | None:
    status = _workflow_execution_status(state)
    current_state = "UNKNOWN" if state is None else state.value
    if expect_non_final:
        if status is not None and not status.final_state:
            return None
    elif target_state is not None and current_state == target_state:
        return None
    message = f"{action} requested; current workflow instance state is {current_state}"
    return WorkflowInstanceActionWarningDetail(
        code="workflow_instance_action_state_after_request",
        action=action,
        message=message,
        current_state=current_state,
        expect_non_final=expect_non_final,
        target_state=target_state,
    )


def _raise_workflow_instance_action_error(
    exc: ApiResultError,
    *,
    workflow_instance_id: int,
    action: str,
    task_code: int | None = None,
) -> None:
    command = _workflow_instance_action_command(action)
    if exc.result_code == WORKFLOW_INSTANCE_EXECUTING_COMMAND:
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
                "Use `dsctl workflow-instance get ID` or "
                "`dsctl workflow-instance watch ID` to inspect the current "
                f"state, wait for the active runtime control command to finish, "
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
                "Use `dsctl workflow-instance get ID` to inspect the referenced "
                "workflow definition, bring that workflow online with "
                "`dsctl workflow online`, then retry the runtime action."
            ),
        ) from exc
    if task_code is not None and exc.result_code == EXECUTE_NOT_DEFINE_TASK:
        message = f"Task code {task_code} was not found"
        raise NotFoundError(
            message,
            details={
                "resource": TASK_RESOURCE,
                "code": task_code,
                "workflow_instance_id": workflow_instance_id,
            },
        ) from exc
    raise exc


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
) -> None:
    details = {
        "resource": WORKFLOW_INSTANCE_RESOURCE,
        "id": sub_workflow_instance_id,
        "relation": "parent",
    }
    if exc.result_code == WORKFLOW_INSTANCE_NOT_SUB_WORKFLOW_INSTANCE:
        message = (
            f"Workflow instance id {sub_workflow_instance_id} is not a "
            "sub-workflow instance."
        )
        raise InvalidStateError(
            message,
            details=details,
            suggestion=(
                "Use `dsctl workflow-instance get ID` for regular workflow "
                "instances; `parent` only applies to sub-workflow instances."
            ),
        ) from exc
    if exc.result_code == SUB_WORKFLOW_INSTANCE_NOT_EXIST:
        raise _parent_workflow_instance_not_found(
            sub_workflow_instance_id=sub_workflow_instance_id,
        ) from exc
    raise exc


def _normalized_execute_task_scope(value: str) -> str:
    normalized = value.strip().lower()
    if normalized in {"self", "pre", "post"}:
        return normalized
    message = "Task execution scope must be one of: self, pre, post"
    raise UserInputError(
        message,
        details={"scope": value},
        suggestion="Pass `--scope self`, `--scope pre`, or `--scope post`.",
    )


def _validate_workflow_instance_patch_support(patch: WorkflowPatchSpec) -> None:
    workflow_patch = patch.workflow
    if workflow_patch is None:
        return
    unsupported_fields = sorted(
        field_name
        for field_name in workflow_patch.set.model_fields_set
        if field_name not in WORKFLOW_INSTANCE_PATCH_SUPPORTED_WORKFLOW_FIELDS
    )
    if not unsupported_fields:
        return
    message = (
        "workflow-instance update only supports workflow.set.global_params and "
        "workflow.set.timeout"
    )
    raise UserInputError(
        message,
        details={
            "unsupported_fields": unsupported_fields,
            "supported_fields": sorted(
                WORKFLOW_INSTANCE_PATCH_SUPPORTED_WORKFLOW_FIELDS
            ),
        },
        suggestion=(
            "Use `dsctl workflow edit --patch ...` for definition-level fields "
            "such as name, description, or release_state."
        ),
    )


def _workflow_instance_update_form_data(
    compiled_payload: WorkflowUpdatePayload,
    *,
    sync_definition: bool,
) -> JsonObject:
    return {
        "taskRelationJson": compiled_payload["taskRelationJson"],
        "taskDefinitionJson": compiled_payload["taskDefinitionJson"],
        "globalParams": compiled_payload["globalParams"],
        "locations": compiled_payload["locations"],
        "timeout": compiled_payload["timeout"],
        "syncDefine": sync_definition,
    }


def _workflow_instance_resolved_workflow(
    *,
    workflow_code: int,
    workflow_name: str | None,
    workflow_version: int | None,
) -> JsonObject:
    return require_json_object(
        {
            "code": workflow_code,
            "name": workflow_name,
            "version": workflow_version,
        },
        label="workflow-instance resolved workflow",
    )


def _workflow_instance_update_resolved(
    *,
    workflow_instance_id: int,
    project: ResolvedProjectData,
    workflow: JsonObject,
    patch_file: str,
    sync_definition: bool,
) -> WorkflowInstanceUpdateResolved:
    return WorkflowInstanceUpdateResolved(
        workflowInstance=WorkflowInstanceSelectionData(id=workflow_instance_id),
        project=project,
        workflow=workflow,
        patch_file=patch_file,
        syncDefine=sync_definition,
    )


def _workflow_instance_update_dry_run_result(
    *,
    project_code: int,
    workflow_instance_id: int,
    form_data: JsonObject,
    resolved: WorkflowInstanceUpdateResolved,
    diff: WorkflowPatchDiffData,
    no_change: bool,
) -> CommandResult:
    warnings: list[str] = []
    warning_details: list[JsonObject] = []
    if no_change:
        no_change_warning = (
            "patch produced no persistent workflow instance change; no update "
            "request was sent"
        )
        warnings.append(no_change_warning)
        warning_details.append(
            require_json_object(
                WorkflowInstanceUpdateNoChangeWarningDetail(
                    code="workflow_instance_update_no_persistent_change",
                    message=no_change_warning,
                    no_change=True,
                    request_sent=False,
                ),
                label="workflow-instance update dry-run warning detail",
            )
        )
    return dry_run_result(
        method="PUT",
        path=f"/projects/{project_code}/workflow-instances/{workflow_instance_id}",
        form_data=require_json_object(
            form_data,
            label="workflow-instance update dry-run form data",
        ),
        resolved=require_json_object(
            resolved,
            label="workflow-instance update dry-run resolved",
        ),
        warnings=warnings,
        warning_details=warning_details,
        extra_data={
            "diff": require_json_object(
                diff,
                label="workflow-instance update dry-run diff",
            ),
            "no_change": no_change,
            "syncDefine": resolved["syncDefine"],
        },
    )


def _raise_workflow_instance_update_error(
    exc: ApiResultError,
    *,
    workflow_instance_id: int,
) -> None:
    if exc.result_code == WORKFLOW_NODE_HAS_CYCLE:
        message = "This workflow instance patch would introduce a dependency cycle."
        raise UserInputError(
            message,
            details={
                "resource": WORKFLOW_INSTANCE_RESOURCE,
                "id": workflow_instance_id,
            },
            suggestion=(
                "Inspect the compiled diff with `workflow-instance update --dry-run`."
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
                "Inspect the compiled diff with `workflow-instance update --dry-run`."
            ),
        ) from exc
    raise exc


def _normalized_workflow_instance_state(value: str | None) -> str | None:
    normalized = optional_text(value)
    if normalized is None:
        return None
    candidate = normalized.upper()
    try:
        return workflow_execution_status_value(candidate)
    except KeyError as exc:
        message = "Workflow instance state must be one of the DS execution status names"
        raise UserInputError(
            message,
            details={"state": value},
            suggestion=(
                "Run `dsctl enum list workflow_execution_status` to inspect "
                "the supported state names."
            ),
        ) from exc
