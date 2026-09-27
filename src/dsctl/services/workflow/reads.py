from __future__ import annotations

from typing import TYPE_CHECKING, Literal, cast

from dsctl.output import (
    CommandResult,
    require_json_object,
)
from dsctl.services._dynamic_workflow_references import (
    resolve_read_dynamic_workflow_refs,
)
from dsctl.services._legacy_workflow_projection import LegacyWorkflowReadProjection
from dsctl.services._task_resource_refs import (
    resolve_dag_read_task_resource_refs,
)
from dsctl.services._validation import (
    require_positive_int,
)
from dsctl.services._workflow import authoring
from dsctl.services._workflow.digest import (
    digest_workflow as _digest_workflow,
)
from dsctl.services._workflow.render import (
    serialize_workflow_dag as _serialize_workflow_dag,
)
from dsctl.services._workflow.render import (
    workflow_yaml_document as _workflow_yaml_document,
)
from dsctl.services.runtime import (
    ReadServiceRuntime,
    run_with_bound_domain_service_runtime,
    run_with_read_service_runtime,
)
from dsctl.services.selection import (
    require_project_selection,
    require_workflow_selection,
    with_selection_source,
)
from dsctl.upstream import Availability, get_action_capability
from dsctl.upstream.dynamic_workflow_references import dynamic_workflow_codes_from_dag
from dsctl.upstream.pagination import (
    DEFAULT_PAGE_SIZE,
)
from dsctl.upstream.serialization import (
    optional_text,
)
from dsctl.upstream.workflows import WORKFLOW_DOMAIN

if TYPE_CHECKING:
    from dsctl.services.selection import SelectionData
    from dsctl.support.yaml_io import JsonObject

from dsctl.services.workflow._graph import (
    _load_legacy_workflow_graph,
    _load_workflow_dag,
)
from dsctl.services.workflow._schedules import (
    _load_target_attached_schedule,
    _schedule_view,
)
from dsctl.services.workflow._selection import (
    _resolve_workflow_target,
    _resolved_project_selection,
    _resolved_workflow_selection,
    _ResolvedWorkflowTarget,
)
from dsctl.services.workflow._types import (
    WorkflowServiceRuntime,
    WorkflowYamlExportData,
)


def list_workflows_result(
    *,
    project: str | None = None,
    search: str | None = None,
    page_no: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
    all_pages: bool = False,
    env_file: str | None = None,
) -> CommandResult:
    """List workflows inside one resolved project with paging controls."""
    normalized_search = optional_text(search)
    require_positive_int(page_no, label="page_no")
    require_positive_int(page_size, label="page_size")
    return run_with_read_service_runtime(
        env_file,
        _list_workflows_result,
        project=project,
        search=normalized_search,
        page_no=page_no,
        page_size=page_size,
        all_pages=all_pages,
    )


def get_workflow_result(
    workflow: str | None,
    *,
    project: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Get one workflow by context-aware name or code."""
    return run_with_read_service_runtime(
        env_file,
        _get_workflow_result,
        workflow=workflow,
        project=project,
    )


def export_workflow_yaml_result(
    workflow: str | None,
    *,
    project: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Export one workflow as a stable YAML authoring document."""
    return run_with_bound_domain_service_runtime(
        env_file,
        WORKFLOW_DOMAIN,
        _workflow_definition_read_result,
        view="export",
        workflow=workflow,
        project=project,
    )


def describe_workflow_result(
    workflow: str | None,
    *,
    project: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Describe one workflow with tasks and task relations."""
    return run_with_bound_domain_service_runtime(
        env_file,
        WORKFLOW_DOMAIN,
        _workflow_definition_read_result,
        view="describe",
        workflow=workflow,
        project=project,
    )


def digest_workflow_result(
    workflow: str | None,
    *,
    project: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Return one compact workflow graph summary."""
    return run_with_bound_domain_service_runtime(
        env_file,
        WORKFLOW_DOMAIN,
        _workflow_definition_read_result,
        view="digest",
        workflow=workflow,
        project=project,
    )


def _list_workflows_result(
    runtime: ReadServiceRuntime,
    *,
    project: str | None,
    search: str | None,
    page_no: int,
    page_size: int,
    all_pages: bool,
) -> CommandResult:
    selected_project = require_project_selection(project, runtime=runtime)
    listing = runtime.upstream.definitions.list_workflows(
        selected_project.value,
        page_no=page_no,
        page_size=page_size,
        search=search,
        all_pages=all_pages,
    )
    data = listing.page.to_data(lambda workflow: workflow.to_data())
    return CommandResult(
        data=require_json_object(data, label="workflow list data"),
        resolved={
            "project": with_selection_source(
                cast("SelectionData", listing.project.to_data()),
                selected_project,
            ),
            "search": search,
            "page_no": page_no,
            "page_size": page_size,
            "all": all_pages,
        },
    )


def _get_workflow_result(
    runtime: ReadServiceRuntime,
    *,
    workflow: str | None,
    project: str | None,
) -> CommandResult:
    selected_project = require_project_selection(project, runtime=runtime)
    selected_workflow = require_workflow_selection(
        workflow,
        input_form="argument",
    )
    read = runtime.upstream.definitions.get_workflow(
        selected_project.value,
        selected_workflow.value,
        schedule_list_supported=(
            get_action_capability(
                runtime.profile.ds_version, "schedule.list"
            ).availability
            is Availability.SUPPORTED
        ),
    )
    data = require_json_object(
        read.view.to_data(attached_schedule=read.attached_schedule),
        label="workflow data",
    )

    return CommandResult(
        data=data,
        resolved={
            "project": with_selection_source(
                cast("SelectionData", read.project.to_data()),
                selected_project,
            ),
            "workflow": with_selection_source(
                cast("SelectionData", read.workflow.to_data()),
                selected_workflow,
            ),
        },
    )


def _workflow_definition_read_result(
    runtime: WorkflowServiceRuntime,
    *,
    workflow: str | None,
    project: str | None,
    view: Literal["export", "describe", "digest"],
) -> CommandResult:
    """Read one selected definition with its dialect's hydration and schedule order."""
    action = f"workflow.{view}"
    target = _resolve_workflow_target(
        runtime, workflow=workflow, project=project, action=action
    )
    label = "workflow yaml export" if view == "export" else f"workflow {view} data"
    if runtime.domain.workflows.workflow_graph_family == "legacy-json":
        snapshot, graph = _load_legacy_workflow_graph(
            runtime,
            target=target,
            action=action,
            hydrate_workflow_refs=True,
            hydrate_resource_refs=view == "export",
        )
        attached_schedule = _load_target_attached_schedule(
            runtime, target=target, action=action, phase="read"
        )
        projection = LegacyWorkflowReadProjection(
            target.scope,
            snapshot,
            graph,
            _schedule_view(target.scope, attached_schedule),
        )
        data = require_json_object(
            WorkflowYamlExportData(yaml=projection.yaml_text())
            if view == "export"
            else projection.describe()
            if view == "describe"
            else projection.digest(),
            label=label,
        )
        resolved = _legacy_workflow_read_resolved(target)
    else:
        dag = _load_workflow_dag(runtime, target=target, action=action)
        attached_schedule = _load_target_attached_schedule(
            runtime, target=target, action=action, phase="read"
        )
        if view == "export":
            export_catalog = authoring.workflow_authoring_catalog_for_version(
                runtime.profile.ds_version
            )
            read_resource_refs = resolve_dag_read_task_resource_refs(
                runtime.domain.task_resource_resolver,
                dag,
                profile_version=runtime.profile.ds_version,
            )
            read_workflow_refs = resolve_read_dynamic_workflow_refs(
                runtime.domain.workflows,
                project=target.project,
                workflow_codes=dynamic_workflow_codes_from_dag(dag),
            )
            data = require_json_object(
                WorkflowYamlExportData(
                    yaml=_workflow_yaml_document(
                        dag,
                        project=target.resolved_project,
                        attached_schedule=attached_schedule,
                        catalog=export_catalog,
                        resource_refs=read_resource_refs,
                        workflow_refs=read_workflow_refs,
                    )
                ),
                label=label,
            )
        else:
            described = _serialize_workflow_dag(
                dag, attached_schedule=attached_schedule
            )
            data = require_json_object(
                described if view == "describe" else _digest_workflow(described),
                label=label,
            )
        resolved = {
            "project": _resolved_project_selection(
                target.resolved_project, target.selected_project
            ),
            "workflow": _resolved_workflow_selection(
                target.resolved_workflow, target.selected_workflow
            ),
        }
    return CommandResult(data=data, resolved=resolved)


def _legacy_workflow_read_resolved(
    target: _ResolvedWorkflowTarget,
) -> JsonObject:
    return require_json_object(
        {
            "project": _resolved_project_selection(
                target.project,
                target.selected_project,
            ),
            "workflow": _resolved_workflow_selection(
                target.workflow,
                target.selected_workflow,
            ),
        },
        label="legacy workflow read resolved",
    )
