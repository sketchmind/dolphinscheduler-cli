from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from dsctl.errors import (
    ApiResultError,
    DsctlError,
)
from dsctl.output import (
    CommandResult,
    require_json_object,
)
from dsctl.services._validation import (
    require_delete_force,
)
from dsctl.services._workflow import authoring
from dsctl.services._workflow.compile import (
    preflight_datax_runtime_activation,
    preflight_kubeflow_runtime_activation,
    preflight_seatunnel_runtime_activation,
)
from dsctl.services._workflow.render import (
    workflow_live_baseline,
)
from dsctl.services.runtime import (
    run_with_bound_domain_service_runtime,
)
from dsctl.upstream.serialization import (
    enum_value,
    optional_text,
)
from dsctl.upstream.workflows import WORKFLOW_DOMAIN, WorkflowDeleteLineageError

if TYPE_CHECKING:
    from dsctl.support.yaml_io import JsonObject
    from dsctl.upstream.definition_models import (
        WorkflowScope,
    )
    from dsctl.upstream.protocol import (
        ScheduleRecord,
    )

from dsctl.services.workflow._errors import (
    _raise_workflow_delete_error,
    _raise_workflow_delete_lineage_conflict,
    _raise_workflow_release_error,
    _raise_workflow_release_refresh_error,
)
from dsctl.services.workflow._graph import (
    _load_legacy_workflow_graph,
    _load_workflow_dag,
)
from dsctl.services.workflow._schedules import (
    _load_target_attached_schedule,
    _workflow_scope_data,
)
from dsctl.services.workflow._selection import (
    _resolve_workflow_target,
    _resolved_project_selection,
    _resolved_workflow_selection,
    _ResolvedWorkflowTarget,
)
from dsctl.services.workflow._types import (
    WorkflowReleaseWarningDetail,
    WorkflowServiceRuntime,
)


def delete_workflow_result(
    workflow: str | None,
    *,
    project: str | None = None,
    force: bool,
    env_file: str | None = None,
) -> CommandResult:
    """Delete one workflow definition after explicit confirmation."""
    require_delete_force(force=force, resource_label="Workflow")
    return run_with_bound_domain_service_runtime(
        env_file,
        WORKFLOW_DOMAIN,
        _delete_workflow_result,
        workflow=workflow,
        project=project,
    )


def online_workflow_result(
    workflow: str | None,
    *,
    project: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Bring one workflow definition online and return the refreshed payload."""
    return run_with_bound_domain_service_runtime(
        env_file,
        WORKFLOW_DOMAIN,
        _set_workflow_release_state_result,
        workflow=workflow,
        project=project,
        action="online",
    )


def offline_workflow_result(
    workflow: str | None,
    *,
    project: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Bring one workflow definition offline and return the refreshed payload."""
    return run_with_bound_domain_service_runtime(
        env_file,
        WORKFLOW_DOMAIN,
        _set_workflow_release_state_result,
        workflow=workflow,
        project=project,
        action="offline",
    )


def _delete_workflow_result(
    runtime: WorkflowServiceRuntime,
    *,
    workflow: str | None,
    project: str | None,
) -> CommandResult:
    target = _resolve_workflow_target(
        runtime,
        workflow=workflow,
        project=project,
        action="workflow.delete",
    )
    operations = runtime.domain.workflows
    attached_schedule = _load_target_attached_schedule(
        runtime,
        target=target,
        action="workflow.delete",
        phase="pre_mutation",
    )
    current = _workflow_scope_data(target.scope, attached_schedule)
    try:
        operations.delete(target.scope)
    except WorkflowDeleteLineageError as error:
        _raise_workflow_delete_lineage_conflict(
            error,
            ds_version=runtime.profile.ds_version,
            project=target.project,
            workflow=target.workflow,
        )
    except ApiResultError as error:
        _raise_workflow_delete_error(
            error,
            project=target.project,
            workflow=target.workflow,
        )
    return CommandResult(
        data=require_json_object(
            {"deleted": True, "workflow": current},
            label="workflow delete data",
        ),
        resolved={
            "project": _resolved_project_selection(
                target.project,
                target.selected_project,
            ),
            "workflow": _resolved_workflow_selection(
                target.workflow,
                target.selected_workflow,
            ),
        },
    )


def _set_workflow_release_state_result(
    runtime: WorkflowServiceRuntime,
    *,
    workflow: str | None,
    project: str | None,
    action: Literal["online", "offline"],
) -> CommandResult:
    target = _resolve_workflow_target(
        runtime,
        workflow=workflow,
        project=project,
        action=f"workflow.{action}",
    )
    workflow_release_state = target.scope.view.release_state
    if action == "online":
        _preflight_guarded_workflow_online(runtime, target=target)
    attached_schedule = _load_target_attached_schedule(
        runtime,
        target=target,
        action=f"workflow.{action}",
        phase="pre_mutation",
    )
    try:
        runtime.domain.workflows.release(
            target.scope,
            state="ONLINE" if action == "online" else "OFFLINE",
        )
    except ApiResultError as exc:
        _raise_workflow_release_error(
            exc,
            project=target.project,
            workflow=target.workflow,
            action=action,
        )
    refreshed_scope = _refresh_workflow_after_release(
        runtime,
        target=target,
        action=action,
    )
    refreshed_attached_schedule = attached_schedule
    if action == "offline" and attached_schedule is not None:
        refreshed_attached_schedule = _load_target_attached_schedule(
            runtime,
            target=target,
            action="workflow.offline",
            phase="post_mutation_refresh",
        )
    return CommandResult(
        data=require_json_object(
            _workflow_scope_data(refreshed_scope, refreshed_attached_schedule),
            label="workflow data",
        ),
        resolved={
            "project": _resolved_project_selection(
                target.project,
                target.selected_project,
            ),
            "workflow": _resolved_workflow_selection(
                target.workflow,
                target.selected_workflow,
            ),
        },
        warnings=_workflow_release_warnings(
            attached_schedule,
            action=action,
        ),
        warning_details=_workflow_release_warning_details(
            workflow_release_state,
            attached_schedule=attached_schedule,
            action=action,
        ),
    )


def _preflight_guarded_workflow_online(
    runtime: WorkflowServiceRuntime,
    *,
    target: _ResolvedWorkflowTarget,
) -> None:
    """Fail before release when a guarded typed DAG is unsafe to activate."""
    catalog = authoring.workflow_authoring_catalog_for_version(
        runtime.profile.ds_version
    )
    guarded_types = {
        task_type
        for task_type in ("DATAX", "KUBEFLOW", "SEATUNNEL")
        if catalog.supports_typed_authoring(task_type)
    }
    if not guarded_types:
        return
    if runtime.domain.workflows.workflow_graph_family == "legacy-json":
        snapshot, graph = _load_legacy_workflow_graph(
            runtime,
            target=target,
            action="workflow.online",
        )
        present_types = {task.type.upper() for task in graph.tasks}
        if not guarded_types.intersection(present_types):
            return
        spec = graph.to_workflow_spec(
            name=(
                optional_text(snapshot.name)
                or optional_text(target.workflow.name)
                or target.selected_workflow.value
            ),
            project=(
                optional_text(target.project.name) or target.selected_project.value
            ),
            description=snapshot.description,
            release_state=snapshot.release_state or "OFFLINE",
        )
        projection_sources = {
            task.name: task.task_params_reencode_source for task in graph.tasks
        }
    else:
        dag = _load_workflow_dag(
            runtime,
            target=target,
            action="workflow.online",
        )
        present_types = {
            (optional_text(task.taskType) or "").upper()
            for task in dag.taskDefinitionList or ()
        }
        if not guarded_types.intersection(present_types):
            return
        baseline = workflow_live_baseline(
            dag,
            project=target.resolved_project,
            catalog=catalog,
        )
        spec = baseline.spec
        projection_sources = baseline.projection_sources
    if "DATAX" in present_types:
        preflight_datax_runtime_activation(
            spec,
            profile_version=catalog.profile_version,
        )
    if "KUBEFLOW" in present_types:
        preflight_kubeflow_runtime_activation(
            spec,
            projection_sources=projection_sources,
        )
    if "SEATUNNEL" in present_types:
        preflight_seatunnel_runtime_activation(
            spec,
            profile_version=catalog.profile_version,
            projection_sources=projection_sources,
        )


def _refresh_workflow_after_release(
    runtime: WorkflowServiceRuntime,
    *,
    target: _ResolvedWorkflowTarget,
    action: Literal["online", "offline"],
) -> WorkflowScope:
    try:
        return runtime.domain.workflows.resolve_workflow(
            target.selected_project.value,
            target.selected_workflow.value,
        )
    except DsctlError as error:
        _raise_workflow_release_refresh_error(
            error,
            project=target.project,
            workflow=target.workflow,
            action=action,
        )


def _workflow_release_warnings(
    attached_schedule: ScheduleRecord | None,
    *,
    action: Literal["online", "offline"],
) -> list[str]:
    schedule_release_state = (
        None
        if attached_schedule is None
        else enum_value(attached_schedule.releaseState)
    )
    if action == "online":
        if attached_schedule is not None and schedule_release_state != "ONLINE":
            return [
                "workflow brought online; any attached schedule remains offline "
                "until `schedule online` is requested"
            ]
        return []
    if schedule_release_state == "ONLINE":
        return ["workflow brought offline; any attached schedule is also taken offline"]
    return []


def _workflow_release_warning_details(
    workflow_release_state: str | None,
    *,
    attached_schedule: ScheduleRecord | None,
    action: Literal["online", "offline"],
) -> list[JsonObject]:
    schedule_release_state = (
        None
        if attached_schedule is None
        else enum_value(attached_schedule.releaseState)
    )
    if action == "online":
        if attached_schedule is not None and schedule_release_state != "ONLINE":
            return [
                require_json_object(
                    WorkflowReleaseWarningDetail(
                        code="workflow_online_leaves_schedule_offline",
                        message=(
                            "workflow brought online; any attached schedule remains "
                            "offline until `schedule online` is requested"
                        ),
                        action=action,
                        workflow_release_state=workflow_release_state,
                        schedule_release_state=schedule_release_state,
                    ),
                    label="workflow release warning detail",
                )
            ]
        return []
    if schedule_release_state == "ONLINE":
        return [
            require_json_object(
                WorkflowReleaseWarningDetail(
                    code="workflow_offline_also_offlines_schedule",
                    message=(
                        "workflow brought offline; any attached schedule is also "
                        "taken offline"
                    ),
                    action=action,
                    workflow_release_state=workflow_release_state,
                    schedule_release_state=schedule_release_state,
                ),
                label="workflow release warning detail",
            )
        ]
    return []
