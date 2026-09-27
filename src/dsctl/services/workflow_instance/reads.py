from __future__ import annotations

from typing import TYPE_CHECKING, cast

from dsctl.cli_surface import WORKFLOW_INSTANCE_RESOURCE
from dsctl.errors import (
    ApiResultError,
    ApiTransportError,
    UserInputError,
)
from dsctl.output import CommandResult, require_json_object
from dsctl.services._dynamic_workflow_references import (
    resolve_read_dynamic_workflow_refs,
)
from dsctl.services._legacy_workflow_instance_projection import (
    LegacyWorkflowInstanceReadProjection,
)
from dsctl.services._task_resource_refs import (
    mr_resource_ids_from_legacy_graph,
    resolve_dag_read_task_resource_refs,
    resolve_read_task_resource_refs,
)
from dsctl.services._validation import (
    optional_ds_datetime,
    require_positive_int,
    validate_ds_datetime_range,
)
from dsctl.services._workflow import authoring
from dsctl.services._workflow.instance_digest import (
    digest_workflow_instance as _digest_workflow_instance,
)
from dsctl.services._workflow.render import workflow_yaml_document
from dsctl.services.runtime import (
    run_with_bound_domain_service_runtime,
)
from dsctl.services.selection import (
    SelectedValue,
    require_project_selection,
)
from dsctl.upstream.definition_models import (
    NativeCode,
    ProjectRef,
    WorkflowRef,
)
from dsctl.upstream.dynamic_workflow_references import dynamic_workflow_codes_from_dag
from dsctl.upstream.instance_time_filters import instance_time_filter_contract
from dsctl.upstream.pagination import (
    DEFAULT_PAGE_SIZE,
    MAX_AUTO_EXHAUST_PAGES,
    collect_all_pages,
    observation_time,
    requested_page_data,
)
from dsctl.upstream.runtime_enums import (
    workflow_execution_status_value,
)
from dsctl.upstream.runtime_instances import (
    RUNTIME_INSTANCE_DOMAIN,
    LocatedWorkflowInstance,
    WorkflowInstanceListing,
    WorkflowInstanceSnapshot,
)
from dsctl.upstream.serialization import (
    optional_text,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from dsctl.services._workflow.instance_digest import TaskInstanceDigestRecord
    from dsctl.upstream.read_models import ReadPage


from dsctl.services.workflow_instance._errors import (
    _parent_workflow_instance_not_found,
    _raise_parent_workflow_instance_lookup_error,
    _translate_workflow_instance_list_error,
)
from dsctl.services.workflow_instance._selection import (
    _is_legacy_workflow_instance,
    _legacy_workflow_instance_graph,
    _legacy_workflow_instance_name,
    _legacy_workflow_instance_project_name,
    _resolved_project,
    _selected_project_data,
    _selected_workflow_instance,
    _workflow_instance_resolved,
)
from dsctl.services.workflow_instance._types import (
    RuntimeInstanceServiceRuntime,
    WorkflowInstanceListResolvedValue,
    WorkflowInstanceParentData,
    WorkflowInstanceSelectionData,
    WorkflowInstanceYamlExportData,
)


def list_workflow_instances_result(
    *,
    page_no: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
    all_pages: bool = False,
    project: str | None = None,
    workflow: str | None = None,
    search: str | None = None,
    executor: str | None = None,
    host: str | None = None,
    start: str | None = None,
    end: str | None = None,
    state: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """List workflow instances using explicit runtime filters."""
    normalized_project = optional_text(project)
    normalized_workflow = optional_text(workflow)
    normalized_search = optional_text(search)
    normalized_executor = optional_text(executor)
    normalized_host = optional_text(host)
    normalized_start = optional_ds_datetime(start, label="start")
    normalized_end = optional_ds_datetime(end, label="end")
    validate_ds_datetime_range(normalized_start, normalized_end)
    normalized_state = _normalized_workflow_instance_state(state)
    normalized_page_no = require_positive_int(page_no, label="page_no")
    normalized_page_size = require_positive_int(page_size, label="page_size")
    return run_with_bound_domain_service_runtime(
        env_file,
        RUNTIME_INSTANCE_DOMAIN,
        _list_workflow_instances_result,
        page_no=normalized_page_no,
        page_size=normalized_page_size,
        all_pages=all_pages,
        project=normalized_project,
        workflow=normalized_workflow,
        search=normalized_search,
        executor=normalized_executor,
        host=normalized_host,
        start=normalized_start,
        end=normalized_end,
        state=normalized_state,
    )


def get_workflow_instance_result(
    workflow_instance_id: int,
    *,
    project: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Get one workflow instance by id."""
    normalized_workflow_instance_id = require_positive_int(
        workflow_instance_id,
        label="workflow_instance_id",
    )
    return run_with_bound_domain_service_runtime(
        env_file,
        RUNTIME_INSTANCE_DOMAIN,
        _get_workflow_instance_result,
        workflow_instance_id=normalized_workflow_instance_id,
        project=optional_text(project),
    )


def export_workflow_instance_yaml_result(
    workflow_instance_id: int,
    *,
    project: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Export one workflow instance DAG as a stable YAML authoring document."""
    normalized_workflow_instance_id = require_positive_int(
        workflow_instance_id,
        label="workflow_instance_id",
    )
    return run_with_bound_domain_service_runtime(
        env_file,
        RUNTIME_INSTANCE_DOMAIN,
        _export_workflow_instance_yaml_result,
        workflow_instance_id=normalized_workflow_instance_id,
        project=optional_text(project),
    )


def digest_workflow_instance_result(
    workflow_instance_id: int,
    *,
    project: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Return one compact workflow-instance runtime digest."""
    normalized_workflow_instance_id = require_positive_int(
        workflow_instance_id,
        label="workflow_instance_id",
    )
    return run_with_bound_domain_service_runtime(
        env_file,
        RUNTIME_INSTANCE_DOMAIN,
        _digest_workflow_instance_result,
        workflow_instance_id=normalized_workflow_instance_id,
        project=optional_text(project),
    )


def get_parent_workflow_instance_result(
    sub_workflow_instance_id: int,
    *,
    project: str | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Return the parent workflow instance for one sub-workflow instance."""
    normalized_sub_workflow_instance_id = require_positive_int(
        sub_workflow_instance_id,
        label="sub_workflow_instance_id",
    )
    return run_with_bound_domain_service_runtime(
        env_file,
        RUNTIME_INSTANCE_DOMAIN,
        _get_parent_workflow_instance_result,
        sub_workflow_instance_id=normalized_sub_workflow_instance_id,
        project=optional_text(project),
    )


def _list_workflow_instances_result(
    runtime: RuntimeInstanceServiceRuntime,
    *,
    page_no: int,
    page_size: int,
    all_pages: bool,
    project: str | None,
    workflow: str | None,
    search: str | None,
    executor: str | None,
    host: str | None,
    start: str | None,
    end: str | None,
    state: str | None,
) -> CommandResult:
    time_filter = instance_time_filter_contract(
        runtime.profile.ds_version, "workflow-instance.list"
    )
    time_filter.validate(start=start, end=end)
    instances = runtime.domain.instances
    selected_project = require_project_selection(project, runtime=runtime)

    def load(
        current_page_no: int,
        current_page_size: int,
    ) -> WorkflowInstanceListing:
        return instances.list_workflow_instances(
            project_selector=selected_project.value,
            workflow_selector=workflow,
            page_no=current_page_no,
            page_size=current_page_size,
            search=search,
            executor=executor,
            host=host,
            start_time=start,
            end_time=end,
            state=state,
        )

    observation_started_at = observation_time()
    try:
        initial = load(page_no, page_size)
    except ApiResultError as exc:
        raise _translate_workflow_instance_list_error(
            exc,
            project=selected_project.value,
            workflow=workflow,
            search=search,
            executor=executor,
            host=host,
            start=start,
            end=end,
            state=state,
        ) from exc

    def fetch_page(
        current_page_no: int,
        current_page_size: int,
    ) -> ReadPage[WorkflowInstanceSnapshot]:
        if current_page_no == page_no and current_page_size == page_size:
            return initial.page
        return load(current_page_no, current_page_size).page

    data = require_json_object(
        requested_page_data(
            fetch_page,
            page_no=page_no,
            page_size=page_size,
            all_pages=all_pages,
            observation_started_at=observation_started_at,
            resource=WORKFLOW_INSTANCE_RESOURCE,
            serialize_item=lambda item: item.to_data(),
            max_pages=MAX_AUTO_EXHAUST_PAGES,
            translate_error=lambda exc: _translate_workflow_instance_list_error(
                exc,
                project=selected_project.value,
                workflow=workflow,
                search=search,
                executor=executor,
                host=host,
                start=start,
                end=end,
                state=state,
            ),
        ),
        label="workflow-instance list data",
    )
    resolved = _workflow_instance_list_resolved(
        page_no=page_no,
        page_size=page_size,
        all_pages=all_pages,
        selected_project=selected_project,
        resolved_project=initial.project,
        workflow=workflow,
        resolved_workflow=initial.workflow,
        search=search,
        executor=executor,
        host=host,
        start=start,
        end=end,
        state=state,
    )
    return CommandResult(
        data=data,
        resolved=require_json_object(
            {**resolved, "time_filter": time_filter.to_data()},
            label="workflow-instance list resolved",
        ),
    )


def _workflow_instance_list_resolved(
    *,
    page_no: int,
    page_size: int,
    all_pages: bool,
    selected_project: SelectedValue,
    resolved_project: ProjectRef,
    workflow: str | None,
    resolved_workflow: WorkflowRef | None,
    search: str | None,
    executor: str | None,
    host: str | None,
    start: str | None,
    end: str | None,
    state: str | None,
) -> dict[str, WorkflowInstanceListResolvedValue]:
    resolved: dict[str, WorkflowInstanceListResolvedValue] = {
        "page_no": page_no,
        "page_size": page_size,
        "all": all_pages,
    }
    optional_values: dict[str, WorkflowInstanceListResolvedValue | None] = {
        "workflow": workflow,
        "search": search,
        "executor": executor,
        "host": host,
        "start": start,
        "end": end,
        "state": state,
    }
    project_identity = (
        "project_code"
        if isinstance(resolved_project.native, NativeCode)
        else "project_id"
    )
    optional_values["project"] = selected_project.value
    optional_values["project_source"] = selected_project.source
    optional_values[project_identity] = resolved_project.native.value
    if resolved_workflow is not None:
        workflow_identity = (
            "workflow_code"
            if isinstance(resolved_workflow.native, NativeCode)
            else "workflow_id"
        )
        optional_values[workflow_identity] = resolved_workflow.native.value
    resolved.update(
        {key: value for key, value in optional_values.items() if value is not None}
    )
    return resolved


def _get_workflow_instance_result(
    runtime: RuntimeInstanceServiceRuntime,
    *,
    workflow_instance_id: int,
    project: str | None,
) -> CommandResult:
    selected_project, located = _selected_workflow_instance(
        runtime,
        project=project,
        workflow_instance_id=workflow_instance_id,
    )
    payload = located.instance
    data = require_json_object(
        payload.to_data(),
        label="workflow-instance data",
    )
    return CommandResult(
        data=data,
        resolved=require_json_object(
            _workflow_instance_resolved(
                workflow_instance_id,
                project=located.project,
                selected_project=selected_project,
            ),
            label="workflow-instance resolved",
        ),
    )


def _export_workflow_instance_yaml_result(
    runtime: RuntimeInstanceServiceRuntime,
    *,
    workflow_instance_id: int,
    project: str | None,
) -> CommandResult:
    selected_project, located = _selected_workflow_instance(
        runtime,
        project=project,
        workflow_instance_id=workflow_instance_id,
    )
    payload = located.instance
    if _is_legacy_workflow_instance(payload):
        return _export_legacy_workflow_instance_yaml_result(
            runtime=runtime,
            workflow_instance_id=workflow_instance_id,
            selected_project=selected_project,
            located=located,
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
    resolved_project = _resolved_project(payload.project)
    read_resource_refs = resolve_dag_read_task_resource_refs(
        runtime.domain.task_resource_resolver,
        dag,
        profile_version=runtime.profile.ds_version,
    )
    read_workflow_refs = resolve_read_dynamic_workflow_refs(
        runtime.domain.workflows,
        project=located.project,
        workflow_codes=dynamic_workflow_codes_from_dag(dag),
    )
    data = require_json_object(
        WorkflowInstanceYamlExportData(
            yaml=workflow_yaml_document(
                dag,
                project=resolved_project,
                attached_schedule=None,
                catalog=authoring.workflow_authoring_catalog_for_version(
                    runtime.profile.ds_version
                ),
                resource_refs=read_resource_refs,
                workflow_refs=read_workflow_refs,
            )
        ),
        label="workflow-instance yaml export",
    )
    return CommandResult(
        data=data,
        resolved=require_json_object(
            _workflow_instance_resolved(
                workflow_instance_id,
                project=located.project,
                selected_project=selected_project,
            ),
            label="workflow-instance resolved",
        ),
    )


def _export_legacy_workflow_instance_yaml_result(
    *,
    runtime: RuntimeInstanceServiceRuntime,
    workflow_instance_id: int,
    selected_project: SelectedValue,
    located: LocatedWorkflowInstance,
) -> CommandResult:
    payload = located.instance
    graph = _legacy_workflow_instance_graph(payload)
    read_resource_refs = resolve_read_task_resource_refs(
        runtime.domain.task_resource_resolver,
        mr_resource_ids_from_legacy_graph(graph),
    )
    graph = _legacy_workflow_instance_graph(
        payload,
        resource_refs=read_resource_refs,
    )
    projection = LegacyWorkflowInstanceReadProjection(
        graph=graph,
        workflow_name=_legacy_workflow_instance_name(payload),
        project_name=_legacy_workflow_instance_project_name(payload),
    )
    return CommandResult(
        data=require_json_object(
            WorkflowInstanceYamlExportData(yaml=projection.yaml_text()),
            label="workflow-instance yaml export",
        ),
        resolved=require_json_object(
            _workflow_instance_resolved(
                workflow_instance_id,
                project=located.project,
                selected_project=selected_project,
            ),
            label="workflow-instance resolved",
        ),
    )


def _digest_workflow_instance_result(
    runtime: RuntimeInstanceServiceRuntime,
    *,
    workflow_instance_id: int,
    project: str | None,
) -> CommandResult:
    selected_project, located = _selected_workflow_instance(
        runtime,
        project=project,
        workflow_instance_id=workflow_instance_id,
    )
    payload = located.instance
    task_pages = collect_all_pages(
        lambda page_no, page_size: runtime.domain.instances.task_page_for_workflow(
            located,
            page_no=page_no,
            page_size=page_size,
        ),
        page_no=1,
        page_size=DEFAULT_PAGE_SIZE,
        max_pages=MAX_AUTO_EXHAUST_PAGES,
    )
    return CommandResult(
        data=require_json_object(
            {
                "coverage": task_pages.coverage,
                **_digest_workflow_instance(
                    workflow_instance=payload,
                    tasks=cast("Sequence[TaskInstanceDigestRecord]", task_pages.items),
                ),
            },
            label="workflow-instance digest data",
        ),
        resolved=require_json_object(
            _workflow_instance_resolved(
                workflow_instance_id,
                project=located.project,
                selected_project=selected_project,
            ),
            label="workflow-instance resolved",
        ),
    )


def _get_parent_workflow_instance_result(
    runtime: RuntimeInstanceServiceRuntime,
    *,
    sub_workflow_instance_id: int,
    project: str | None,
) -> CommandResult:
    selected_project, located = _selected_workflow_instance(
        runtime,
        project=project,
        workflow_instance_id=sub_workflow_instance_id,
    )
    try:
        parent_workflow_instance_id = (
            runtime.domain.instances.parent_workflow_instance_id(located)
        )
    except ApiResultError as exc:
        _raise_parent_workflow_instance_lookup_error(
            exc,
            sub_workflow_instance_id=sub_workflow_instance_id,
            project_selector=selected_project.value,
        )
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
                ),
                "project": _selected_project_data(located.project, selected_project),
            },
            label="workflow-instance parent resolved",
        ),
    )


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
                "Run `dsctl enum list workflow-execution-status` to inspect "
                "the supported state names."
            ),
        ) from exc
