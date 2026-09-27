from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, NoReturn

from dsctl.cli_surface import (
    ALERT_GROUP_RESOURCE,
    ALERT_PLUGIN_RESOURCE,
    CLUSTER_RESOURCE,
    DATASOURCE_RESOURCE,
    ENV_RESOURCE,
    NAMESPACE_RESOURCE,
    PROJECT_PARAMETER_RESOURCE,
    PROJECT_RESOURCE,
    QUEUE_RESOURCE,
    TASK_GROUP_RESOURCE,
    TASK_RESOURCE,
    TENANT_RESOURCE,
    USER_RESOURCE,
    WORKER_GROUP_RESOURCE,
    WORKFLOW_RESOURCE,
)
from dsctl.errors import (
    ApiResultError,
    NotFoundError,
    PermissionDeniedError,
    UserInputError,
)
from dsctl.upstream._resolver_models import (
    ResolvedAlertGroup,
    ResolvedAlertGroupData,
    ResolvedAlertPlugin,
    ResolvedAlertPluginData,
    ResolvedCluster,
    ResolvedClusterData,
    ResolvedDataSource,
    ResolvedDataSourceData,
    ResolvedEnvironment,
    ResolvedEnvironmentData,
    ResolvedNamespace,
    ResolvedNamespaceData,
    ResolvedProject,
    ResolvedProjectData,
    ResolvedProjectParameter,
    ResolvedProjectParameterData,
    ResolvedQueue,
    ResolvedQueueData,
    ResolvedTask,
    ResolvedTaskData,
    ResolvedTaskGroup,
    ResolvedTaskGroupData,
    ResolvedTenant,
    ResolvedTenantData,
    ResolvedUser,
    ResolvedUserData,
    ResolvedWorkerGroup,
    ResolvedWorkerGroupData,
    ResolvedWorkflow,
    ResolvedWorkflowData,
    resolved_alert_group,
    resolved_alert_plugin,
    resolved_cluster,
    resolved_datasource,
    resolved_datasource_payload,
    resolved_environment,
    resolved_namespace,
    resolved_project,
    resolved_project_parameter,
    resolved_queue,
    resolved_task,
    resolved_task_group,
    resolved_tenant,
    resolved_user,
    resolved_worker_group,
    resolved_workflow,
)
from dsctl.upstream._selector_resolution import (
    collect_resolution_page_items,
    normalize_identifier,
    parse_numeric_identifier,
    require_single_match,
    resolve_direct,
    resolve_exact_matches,
)

if TYPE_CHECKING:
    from dsctl.upstream.protocol import (
        AlertGroupOperations,
        AlertPluginListItemRecord,
        AlertPluginOperations,
        ClusterOperations,
        DataSourceOperations,
        EnvironmentOperations,
        NamespaceOperations,
        ProjectParameterOperations,
        ProjectReadOperations,
        QueueLookupOperations,
        TaskGroupOperations,
        TaskOperations,
        TenantOperations,
        UserReadOperations,
        WorkerGroupOperations,
        WorkflowPayloadRecord,
        WorkflowReadOperations,
    )

DEFAULT_RESOLUTION_PAGE_SIZE = 100
MAX_RESOLUTION_PAGES = 20
PROJECT_NOT_FOUND = 10018
PROJECT_NOT_EXIST = 10190
USER_NO_OPERATION_PROJECT_PERM = 30002
PROJECT_PARAMETER_NOT_EXISTS = 10219
QUERY_ENVIRONMENT_BY_CODE_ERROR = 1200009
QUERY_CLUSTER_BY_CODE_ERROR = 1200028
RESOURCE_NOT_EXIST = 20004
ALERT_GROUP_NOT_EXIST = 10011
USER_NOT_EXIST = 10010
QUEUE_NOT_EXIST = 10128
TENANT_NOT_EXIST = 10017
WORKER_GROUP_NOT_EXIST = 1402001
WORKFLOW_DEFINITION_NOT_EXIST = 50003

__all__ = [
    "DEFAULT_RESOLUTION_PAGE_SIZE",
    "MAX_RESOLUTION_PAGES",
    "ResolvedAlertGroup",
    "ResolvedAlertGroupData",
    "ResolvedAlertPlugin",
    "ResolvedAlertPluginData",
    "ResolvedCluster",
    "ResolvedClusterData",
    "ResolvedDataSource",
    "ResolvedDataSourceData",
    "ResolvedEnvironment",
    "ResolvedEnvironmentData",
    "ResolvedNamespace",
    "ResolvedNamespaceData",
    "ResolvedProject",
    "ResolvedProjectData",
    "ResolvedProjectParameter",
    "ResolvedProjectParameterData",
    "ResolvedQueue",
    "ResolvedQueueData",
    "ResolvedTask",
    "ResolvedTaskData",
    "ResolvedTaskGroup",
    "ResolvedTaskGroupData",
    "ResolvedTenant",
    "ResolvedTenantData",
    "ResolvedUser",
    "ResolvedUserData",
    "ResolvedWorkerGroup",
    "ResolvedWorkerGroupData",
    "ResolvedWorkflow",
    "ResolvedWorkflowData",
    "WorkflowResolution",
    "alert_group",
    "alert_plugin",
    "cluster",
    "datasource",
    "datasource_name",
    "environment",
    "namespace",
    "project",
    "project_parameter",
    "queue",
    "raise_workflow_read_api_error",
    "task",
    "task_group",
    "tenant",
    "user",
    "worker_group",
    "workflow",
    "workflow_resolution",
]


@dataclass(frozen=True)
class WorkflowResolution:
    """Resolved identity plus a detail payload already loaded during lookup."""

    identity: ResolvedWorkflow
    payload: WorkflowPayloadRecord | None


def project(
    identifier: str,
    *,
    adapter: ProjectReadOperations,
) -> ResolvedProject:
    """Resolve a project name-or-code into a stable code/name pair."""
    normalized_identifier = normalize_identifier(identifier, label="Project")
    project_code = parse_numeric_identifier(normalized_identifier)
    if project_code is not None:
        try:
            return resolve_direct(
                project_code,
                load=lambda code: adapter.get(code=code),
                project=resolved_project,
                not_found_result_codes=(PROJECT_NOT_FOUND, PROJECT_NOT_EXIST),
                not_found_message=f"Project code {project_code} was not found",
                not_found_details={
                    "resource": PROJECT_RESOURCE,
                    "code": project_code,
                },
            )
        except ApiResultError as error:
            if error.result_code != USER_NO_OPERATION_PROJECT_PERM:
                raise
            message = "Current user does not have permission to access this project"
            raise PermissionDeniedError(
                message,
                details={"resource": PROJECT_RESOURCE, "code": project_code},
                suggestion=(
                    "Ask a DolphinScheduler administrator or project owner to "
                    "grant access to the project, then retry."
                ),
            ) from error

    matches = resolve_exact_matches(
        collect_resolution_page_items(
            fetch_page=lambda page_no, page_size: adapter.list(
                page_no=page_no,
                page_size=page_size,
                search=normalized_identifier,
            ),
            page_size=DEFAULT_RESOLUTION_PAGE_SIZE,
            max_pages=MAX_RESOLUTION_PAGES,
            safety_message=(
                f"Project search for {normalized_identifier!r} exceeded the "
                "resolver safety limit"
            ),
            safety_details={
                "resource": PROJECT_RESOURCE,
                "name": normalized_identifier,
            },
        ),
        matches=lambda candidate: (
            candidate.name == normalized_identifier and candidate.code is not None
        ),
        project=resolved_project,
        dedupe_key=lambda match: match.code,
    )
    return require_single_match(
        matches,
        not_found_message=f"Project {normalized_identifier!r} was not found",
        not_found_details={
            "resource": PROJECT_RESOURCE,
            "name": normalized_identifier,
        },
        ambiguous_message=f"Project name {normalized_identifier!r} is ambiguous",
        ambiguous_details={
            "resource": PROJECT_RESOURCE,
            "name": normalized_identifier,
            "codes": [match.code for match in matches],
        },
    )


def workflow(
    identifier: str,
    *,
    adapter: WorkflowReadOperations,
    project_code: int,
) -> ResolvedWorkflow:
    """Resolve a workflow name-or-code within one project."""
    return workflow_resolution(
        identifier,
        adapter=adapter,
        project_code=project_code,
    ).identity


def workflow_resolution(
    identifier: str,
    *,
    adapter: WorkflowReadOperations,
    project_code: int,
) -> WorkflowResolution:
    """Resolve a workflow and retain any detail loaded to complete identity."""
    normalized_identifier = normalize_identifier(identifier, label="Workflow")
    workflow_code = parse_numeric_identifier(normalized_identifier)
    if workflow_code is not None:
        return _workflow_by_code(
            adapter=adapter,
            project_code=project_code,
            workflow_code=workflow_code,
        )

    try:
        workflow_refs = adapter.list_refs(project_code=project_code)
    except ApiResultError as error:
        raise_workflow_read_api_error(
            error,
            project_code=project_code,
        )

    matches = resolve_exact_matches(
        workflow_refs,
        matches=lambda candidate: (
            candidate.name == normalized_identifier and candidate.code is not None
        ),
        project=resolved_workflow,
        dedupe_key=lambda match: match.code,
    )
    match = require_single_match(
        matches,
        not_found_message=f"Workflow {normalized_identifier!r} was not found",
        not_found_details={
            "resource": WORKFLOW_RESOURCE,
            "project_code": project_code,
            "name": normalized_identifier,
        },
        ambiguous_message=f"Workflow name {normalized_identifier!r} is ambiguous",
        ambiguous_details={
            "resource": WORKFLOW_RESOURCE,
            "project_code": project_code,
            "name": normalized_identifier,
            "codes": [match.code for match in matches],
        },
    )
    if match.version is not None:
        return WorkflowResolution(identity=match, payload=None)
    return _workflow_by_code(
        adapter=adapter,
        project_code=project_code,
        workflow_code=match.code,
    )


def _workflow_by_code(
    *,
    adapter: WorkflowReadOperations,
    project_code: int,
    workflow_code: int,
) -> WorkflowResolution:
    """Load one workflow detail and verify its project-scoped identity."""
    try:
        payload = resolve_direct(
            workflow_code,
            load=lambda code: adapter.get(project_code=project_code, code=code),
            project=lambda record: record,
            not_found_result_codes=(WORKFLOW_DEFINITION_NOT_EXIST,),
            not_found_message=f"Workflow code {workflow_code} was not found",
            not_found_details={
                "resource": WORKFLOW_RESOURCE,
                "code": workflow_code,
                "project_code": project_code,
            },
        )
    except ApiResultError as error:
        raise_workflow_read_api_error(
            error,
            project_code=project_code,
            workflow_code=workflow_code,
        )
    if payload.projectCode != project_code:
        message = f"Workflow code {workflow_code} was not found in the selected project"
        raise NotFoundError(
            message,
            details={
                "resource": WORKFLOW_RESOURCE,
                "code": workflow_code,
                "project_code": project_code,
            },
        )
    return WorkflowResolution(
        identity=resolved_workflow(payload),
        payload=payload,
    )


def raise_workflow_read_api_error(
    error: ApiResultError,
    *,
    project_code: int,
    workflow_code: int | None = None,
) -> NoReturn:
    """Translate exact project-scoped workflow read failures and raise them."""
    if error.result_code == PROJECT_NOT_EXIST:
        message = f"Project code {project_code} was not found"
        raise NotFoundError(
            message,
            details={"resource": PROJECT_RESOURCE, "code": project_code},
        ) from error

    workflow_details: dict[str, str | int] = {
        "resource": WORKFLOW_RESOURCE,
        "project_code": project_code,
    }
    if workflow_code is not None:
        workflow_details["code"] = workflow_code

    if error.result_code == USER_NO_OPERATION_PROJECT_PERM:
        message = (
            "Current user does not have permission to access workflows in the "
            f"selected project {project_code}"
        )
        raise PermissionDeniedError(
            message,
            details=workflow_details,
            suggestion=(
                "Ask a DolphinScheduler administrator or project owner to grant "
                "workflow-definition access in the project, then retry."
            ),
        ) from error
    if error.result_code == WORKFLOW_DEFINITION_NOT_EXIST and workflow_code is not None:
        message = f"Workflow code {workflow_code} was not found"
        raise NotFoundError(
            message,
            details=workflow_details,
        ) from error
    raise error


def project_parameter(
    identifier: str,
    *,
    adapter: ProjectParameterOperations,
    project_code: int,
) -> ResolvedProjectParameter:
    """Resolve a project parameter name-or-code within one selected project."""
    normalized_identifier = normalize_identifier(
        identifier,
        label="Project parameter",
    )
    parameter_code = parse_numeric_identifier(normalized_identifier)
    if parameter_code is not None:
        return resolve_direct(
            parameter_code,
            load=lambda code: adapter.get(project_code=project_code, code=code),
            project=resolved_project_parameter,
            not_found_result_codes=(PROJECT_PARAMETER_NOT_EXISTS,),
            not_found_message=(
                f"Project parameter code {parameter_code} was not found"
            ),
            not_found_details={
                "resource": PROJECT_PARAMETER_RESOURCE,
                "project_code": project_code,
                "code": parameter_code,
            },
        )

    matches = resolve_exact_matches(
        collect_resolution_page_items(
            fetch_page=lambda page_no, page_size: adapter.list(
                project_code=project_code,
                page_no=page_no,
                page_size=page_size,
                search=normalized_identifier,
            ),
            page_size=DEFAULT_RESOLUTION_PAGE_SIZE,
            max_pages=MAX_RESOLUTION_PAGES,
            safety_message=(
                f"Project parameter search for {normalized_identifier!r} "
                "exceeded the resolver safety limit"
            ),
            safety_details={
                "resource": PROJECT_PARAMETER_RESOURCE,
                "project_code": project_code,
                "name": normalized_identifier,
            },
        ),
        matches=lambda candidate: (
            candidate.paramName == normalized_identifier and candidate.code is not None
        ),
        project=resolved_project_parameter,
        dedupe_key=lambda match: match.code,
    )
    return require_single_match(
        matches,
        not_found_message=(
            f"Project parameter {normalized_identifier!r} was not found"
        ),
        not_found_details={
            "resource": PROJECT_PARAMETER_RESOURCE,
            "project_code": project_code,
            "name": normalized_identifier,
        },
        ambiguous_message=(
            f"Project parameter name {normalized_identifier!r} is ambiguous"
        ),
        ambiguous_details={
            "resource": PROJECT_PARAMETER_RESOURCE,
            "project_code": project_code,
            "name": normalized_identifier,
            "codes": [match.code for match in matches],
        },
    )


def environment(
    identifier: str,
    *,
    adapter: EnvironmentOperations,
) -> ResolvedEnvironment:
    """Resolve an environment name-or-code into a stable code/name pair."""
    normalized_identifier = normalize_identifier(identifier, label="Environment")
    environment_code = parse_numeric_identifier(normalized_identifier)
    if environment_code is not None:
        return resolve_direct(
            environment_code,
            load=lambda code: adapter.get(code=code),
            project=resolved_environment,
            not_found_result_codes=(QUERY_ENVIRONMENT_BY_CODE_ERROR,),
            not_found_message=f"Environment code {environment_code} was not found",
            not_found_details={"resource": ENV_RESOURCE, "code": environment_code},
        )

    matches = resolve_exact_matches(
        collect_resolution_page_items(
            fetch_page=lambda page_no, page_size: adapter.list(
                page_no=page_no,
                page_size=page_size,
                search=normalized_identifier,
            ),
            page_size=DEFAULT_RESOLUTION_PAGE_SIZE,
            max_pages=MAX_RESOLUTION_PAGES,
            safety_message=(
                f"Environment search for {normalized_identifier!r} exceeded the "
                "resolver safety limit"
            ),
            safety_details={"resource": ENV_RESOURCE, "name": normalized_identifier},
        ),
        matches=lambda candidate: (
            candidate.name == normalized_identifier and candidate.code is not None
        ),
        project=resolved_environment,
        dedupe_key=lambda match: match.code,
    )
    return require_single_match(
        matches,
        not_found_message=f"Environment {normalized_identifier!r} was not found",
        not_found_details={"resource": ENV_RESOURCE, "name": normalized_identifier},
        ambiguous_message=f"Environment name {normalized_identifier!r} is ambiguous",
        ambiguous_details={
            "resource": ENV_RESOURCE,
            "name": normalized_identifier,
            "codes": [match.code for match in matches],
        },
    )


def cluster(
    identifier: str,
    *,
    adapter: ClusterOperations,
) -> ResolvedCluster:
    """Resolve a cluster name-or-code into a stable code/name pair."""
    normalized_identifier = normalize_identifier(identifier, label="Cluster")
    cluster_code = parse_numeric_identifier(normalized_identifier)
    if cluster_code is not None:
        return resolve_direct(
            cluster_code,
            load=lambda code: adapter.get(code=code),
            project=resolved_cluster,
            not_found_result_codes=(QUERY_CLUSTER_BY_CODE_ERROR,),
            not_found_message=f"Cluster code {cluster_code} was not found",
            not_found_details={"resource": CLUSTER_RESOURCE, "code": cluster_code},
        )

    matches = resolve_exact_matches(
        collect_resolution_page_items(
            fetch_page=lambda page_no, page_size: adapter.list(
                page_no=page_no,
                page_size=page_size,
                search=normalized_identifier,
            ),
            page_size=DEFAULT_RESOLUTION_PAGE_SIZE,
            max_pages=MAX_RESOLUTION_PAGES,
            safety_message=(
                f"Cluster search for {normalized_identifier!r} exceeded the "
                "resolver safety limit"
            ),
            safety_details={
                "resource": CLUSTER_RESOURCE,
                "name": normalized_identifier,
            },
        ),
        matches=lambda candidate: (
            candidate.name == normalized_identifier and candidate.code is not None
        ),
        project=resolved_cluster,
        dedupe_key=lambda match: match.code,
    )
    return require_single_match(
        matches,
        not_found_message=f"Cluster {normalized_identifier!r} was not found",
        not_found_details={
            "resource": CLUSTER_RESOURCE,
            "name": normalized_identifier,
        },
        ambiguous_message=f"Cluster name {normalized_identifier!r} is ambiguous",
        ambiguous_details={
            "resource": CLUSTER_RESOURCE,
            "name": normalized_identifier,
            "codes": [match.code for match in matches],
        },
    )


def alert_plugin(
    identifier: str,
    *,
    adapter: AlertPluginOperations,
) -> ResolvedAlertPlugin:
    """Resolve an alert-plugin instance name-or-id into a stable identity."""
    normalized_identifier = normalize_identifier(identifier, label="Alert-plugin")
    alert_plugin_id = parse_numeric_identifier(normalized_identifier)
    if alert_plugin_id is not None:
        alert_plugin_record = _alert_plugin_list_item_by_id(
            adapter,
            alert_plugin_id=alert_plugin_id,
        )
        if alert_plugin_record is None:
            message = f"Alert-plugin id {alert_plugin_id} was not found"
            raise NotFoundError(
                message,
                details={"resource": ALERT_PLUGIN_RESOURCE, "id": alert_plugin_id},
            )
        return resolved_alert_plugin(
            alert_plugin_record,
            alert_plugin_name=alert_plugin_record.alertPluginName,
        )

    matches = resolve_exact_matches(
        collect_resolution_page_items(
            fetch_page=lambda page_no, page_size: adapter.list(
                page_no=page_no,
                page_size=page_size,
                search=normalized_identifier,
            ),
            page_size=DEFAULT_RESOLUTION_PAGE_SIZE,
            max_pages=MAX_RESOLUTION_PAGES,
            safety_message=(
                f"Alert-plugin search for {normalized_identifier!r} exceeded the "
                "resolver safety limit"
            ),
            safety_details={
                "resource": ALERT_PLUGIN_RESOURCE,
                "name": normalized_identifier,
            },
        ),
        matches=lambda candidate: candidate.instanceName == normalized_identifier,
        project=lambda candidate: resolved_alert_plugin(
            candidate,
            alert_plugin_name=candidate.alertPluginName,
        ),
        dedupe_key=lambda match: match.id,
    )
    return require_single_match(
        matches,
        not_found_message=f"Alert-plugin {normalized_identifier!r} was not found",
        not_found_details={
            "resource": ALERT_PLUGIN_RESOURCE,
            "name": normalized_identifier,
        },
        ambiguous_message=(f"Alert-plugin name {normalized_identifier!r} is ambiguous"),
        ambiguous_details={
            "resource": ALERT_PLUGIN_RESOURCE,
            "name": normalized_identifier,
            "ids": [match.id for match in matches],
        },
    )


def task_group(
    identifier: str,
    *,
    adapter: TaskGroupOperations,
) -> ResolvedTaskGroup:
    """Resolve a task-group name-or-id into a stable id/name pair."""
    normalized_identifier = normalize_identifier(identifier, label="Task-group")
    task_group_id = parse_numeric_identifier(normalized_identifier)
    if task_group_id is not None:
        return resolve_direct(
            task_group_id,
            load=lambda group_id: adapter.get(task_group_id=group_id),
            project=resolved_task_group,
            not_found_result_codes=(),
            not_found_message=f"Task-group id {task_group_id} was not found",
            not_found_details={
                "resource": TASK_GROUP_RESOURCE,
                "id": task_group_id,
            },
        )

    matches = resolve_exact_matches(
        collect_resolution_page_items(
            fetch_page=lambda page_no, page_size: adapter.list(
                page_no=page_no,
                page_size=page_size,
                search=normalized_identifier,
            ),
            page_size=DEFAULT_RESOLUTION_PAGE_SIZE,
            max_pages=MAX_RESOLUTION_PAGES,
            safety_message=(
                f"Task-group search for {normalized_identifier!r} exceeded the "
                "resolver safety limit"
            ),
            safety_details={
                "resource": TASK_GROUP_RESOURCE,
                "name": normalized_identifier,
            },
        ),
        matches=lambda candidate: (
            candidate.name == normalized_identifier and candidate.id is not None
        ),
        project=resolved_task_group,
        dedupe_key=lambda match: match.id,
    )
    return require_single_match(
        matches,
        not_found_message=f"Task-group {normalized_identifier!r} was not found",
        not_found_details={
            "resource": TASK_GROUP_RESOURCE,
            "name": normalized_identifier,
        },
        ambiguous_message=f"Task-group name {normalized_identifier!r} is ambiguous",
        ambiguous_details={
            "resource": TASK_GROUP_RESOURCE,
            "name": normalized_identifier,
            "ids": [match.id for match in matches],
        },
    )


def datasource(
    identifier: str,
    *,
    adapter: DataSourceOperations,
) -> ResolvedDataSource:
    """Resolve a datasource name-or-id into a stable id/name pair."""
    normalized_identifier = normalize_identifier(identifier, label="Datasource")
    datasource_id = parse_numeric_identifier(normalized_identifier)
    if datasource_id is not None:
        return resolve_direct(
            datasource_id,
            load=lambda resolved_id: adapter.get(datasource_id=resolved_id),
            project=lambda payload: resolved_datasource_payload(
                payload,
                fallback_id=datasource_id,
            ),
            not_found_result_codes=(RESOURCE_NOT_EXIST,),
            not_found_message=f"Datasource id {datasource_id} was not found",
            not_found_details={"resource": DATASOURCE_RESOURCE, "id": datasource_id},
        )

    return datasource_name(normalized_identifier, adapter=adapter)


def datasource_name(
    name: str,
    *,
    adapter: DataSourceOperations,
) -> ResolvedDataSource:
    """Resolve an exact datasource name without interpreting numeric-looking text."""
    if not name.strip():
        message = "Datasource name must not be empty"
        raise UserInputError(
            message,
            suggestion="Pass one non-empty exact datasource name.",
        )
    matches = resolve_exact_matches(
        collect_resolution_page_items(
            fetch_page=lambda page_no, page_size: adapter.list(
                page_no=page_no,
                page_size=page_size,
                search=name,
            ),
            page_size=DEFAULT_RESOLUTION_PAGE_SIZE,
            max_pages=MAX_RESOLUTION_PAGES,
            safety_message=(
                f"Datasource search for {name!r} exceeded the resolver safety limit"
            ),
            safety_details={
                "resource": DATASOURCE_RESOURCE,
                "name": name,
            },
        ),
        matches=lambda candidate: candidate.name == name and candidate.id is not None,
        project=resolved_datasource,
        dedupe_key=lambda match: match.id,
    )
    return require_single_match(
        matches,
        not_found_message=f"Datasource {name!r} was not found",
        not_found_details={
            "resource": DATASOURCE_RESOURCE,
            "name": name,
        },
        ambiguous_message=f"Datasource name {name!r} is ambiguous",
        ambiguous_details={
            "resource": DATASOURCE_RESOURCE,
            "name": name,
            "ids": sorted(match.id for match in matches),
        },
    )


def _alert_plugin_list_item_by_id(
    adapter: AlertPluginOperations,
    *,
    alert_plugin_id: int,
) -> AlertPluginListItemRecord | None:
    for alert_plugin_record in adapter.list_all():
        if alert_plugin_record.id == alert_plugin_id:
            return alert_plugin_record
    return None


def namespace(
    identifier: str,
    *,
    adapter: NamespaceOperations,
) -> ResolvedNamespace:
    """Resolve a namespace name-or-id into a stable id/name pair."""
    normalized_identifier = normalize_identifier(identifier, label="Namespace")
    namespace_id = parse_numeric_identifier(normalized_identifier)
    if namespace_id is not None:
        searched_items = collect_resolution_page_items(
            fetch_page=lambda page_no, page_size: adapter.list(
                page_no=page_no,
                page_size=page_size,
            ),
            page_size=DEFAULT_RESOLUTION_PAGE_SIZE,
            max_pages=MAX_RESOLUTION_PAGES,
            safety_message=(
                f"Namespace search for id {namespace_id} exceeded the resolver "
                "safety limit"
            ),
            safety_details={"resource": NAMESPACE_RESOURCE, "id": namespace_id},
        )
        for candidate in searched_items:
            if candidate.id == namespace_id:
                return resolved_namespace(candidate)
        message = f"Namespace id {namespace_id} was not found"
        raise NotFoundError(
            message,
            details={"resource": NAMESPACE_RESOURCE, "id": namespace_id},
        )

    matches = resolve_exact_matches(
        collect_resolution_page_items(
            fetch_page=lambda page_no, page_size: adapter.list(
                page_no=page_no,
                page_size=page_size,
                search=normalized_identifier,
            ),
            page_size=DEFAULT_RESOLUTION_PAGE_SIZE,
            max_pages=MAX_RESOLUTION_PAGES,
            safety_message=(
                f"Namespace search for {normalized_identifier!r} exceeded the "
                "resolver safety limit"
            ),
            safety_details={
                "resource": NAMESPACE_RESOURCE,
                "name": normalized_identifier,
            },
        ),
        matches=lambda candidate: (
            candidate.namespace == normalized_identifier and candidate.id is not None
        ),
        project=resolved_namespace,
        dedupe_key=lambda match: match.id,
    )
    return require_single_match(
        matches,
        not_found_message=f"Namespace {normalized_identifier!r} was not found",
        not_found_details={
            "resource": NAMESPACE_RESOURCE,
            "name": normalized_identifier,
        },
        ambiguous_message=f"Namespace name {normalized_identifier!r} is ambiguous",
        ambiguous_details={
            "resource": NAMESPACE_RESOURCE,
            "name": normalized_identifier,
            "ids": [match.id for match in matches],
            "clusterCodes": [match.cluster_code for match in matches],
        },
    )


def alert_group(
    identifier: str,
    *,
    adapter: AlertGroupOperations,
) -> ResolvedAlertGroup:
    """Resolve an alert-group name-or-id into a stable id/name pair."""
    normalized_identifier = normalize_identifier(identifier, label="Alert group")
    alert_group_id = parse_numeric_identifier(normalized_identifier)
    if alert_group_id is not None:
        return resolve_direct(
            alert_group_id,
            load=lambda resolved_id: adapter.get(alert_group_id=resolved_id),
            project=resolved_alert_group,
            not_found_result_codes=(ALERT_GROUP_NOT_EXIST,),
            not_found_message=f"Alert group id {alert_group_id} was not found",
            not_found_details={"resource": ALERT_GROUP_RESOURCE, "id": alert_group_id},
        )

    matches = resolve_exact_matches(
        collect_resolution_page_items(
            fetch_page=lambda page_no, page_size: adapter.list(
                page_no=page_no,
                page_size=page_size,
                search=normalized_identifier,
            ),
            page_size=DEFAULT_RESOLUTION_PAGE_SIZE,
            max_pages=MAX_RESOLUTION_PAGES,
            safety_message=(
                f"Alert group search for {normalized_identifier!r} exceeded the "
                "resolver safety limit"
            ),
            safety_details={
                "resource": ALERT_GROUP_RESOURCE,
                "name": normalized_identifier,
            },
        ),
        matches=lambda candidate: (
            candidate.groupName == normalized_identifier and candidate.id is not None
        ),
        project=resolved_alert_group,
        dedupe_key=lambda match: match.id,
    )
    return require_single_match(
        matches,
        not_found_message=f"Alert group {normalized_identifier!r} was not found",
        not_found_details={
            "resource": ALERT_GROUP_RESOURCE,
            "name": normalized_identifier,
        },
        ambiguous_message=f"Alert group name {normalized_identifier!r} is ambiguous",
        ambiguous_details={
            "resource": ALERT_GROUP_RESOURCE,
            "name": normalized_identifier,
            "ids": [match.id for match in matches],
        },
    )


def user(
    identifier: str,
    *,
    adapter: UserReadOperations,
) -> ResolvedUser:
    """Resolve a user name-or-id into a stable id/name pair."""
    normalized_identifier = normalize_identifier(identifier, label="User")
    user_id = parse_numeric_identifier(normalized_identifier)
    if user_id is not None:
        return resolve_direct(
            user_id,
            load=lambda resolved_id: adapter.get(user_id=resolved_id),
            project=resolved_user,
            not_found_result_codes=(USER_NOT_EXIST,),
            not_found_message=f"User id {user_id} was not found",
            not_found_details={"resource": USER_RESOURCE, "id": user_id},
        )

    matches = resolve_exact_matches(
        collect_resolution_page_items(
            fetch_page=lambda page_no, page_size: adapter.list(
                page_no=page_no,
                page_size=page_size,
                search=normalized_identifier,
            ),
            page_size=DEFAULT_RESOLUTION_PAGE_SIZE,
            max_pages=MAX_RESOLUTION_PAGES,
            safety_message=(
                f"User search for {normalized_identifier!r} exceeded the resolver "
                "safety limit"
            ),
            safety_details={
                "resource": USER_RESOURCE,
                "userName": normalized_identifier,
            },
        ),
        matches=lambda candidate: (
            candidate.userName == normalized_identifier and candidate.id is not None
        ),
        project=resolved_user,
        dedupe_key=lambda match: match.id,
    )
    return require_single_match(
        matches,
        not_found_message=f"User {normalized_identifier!r} was not found",
        not_found_details={
            "resource": USER_RESOURCE,
            "userName": normalized_identifier,
        },
        ambiguous_message=f"User name {normalized_identifier!r} is ambiguous",
        ambiguous_details={
            "resource": USER_RESOURCE,
            "userName": normalized_identifier,
            "ids": [match.id for match in matches],
        },
    )


def queue(
    identifier: str,
    *,
    adapter: QueueLookupOperations,
) -> ResolvedQueue:
    """Resolve a queue name-or-id into a stable id/name pair."""
    normalized_identifier = normalize_identifier(identifier, label="Queue")
    queue_id = parse_numeric_identifier(normalized_identifier)
    if queue_id is not None:
        return resolve_direct(
            queue_id,
            load=lambda resolved_id: adapter.get(queue_id=resolved_id),
            project=resolved_queue,
            not_found_result_codes=(QUEUE_NOT_EXIST,),
            not_found_message=f"Queue id {queue_id} was not found",
            not_found_details={"resource": QUEUE_RESOURCE, "id": queue_id},
        )

    matches = resolve_exact_matches(
        collect_resolution_page_items(
            fetch_page=lambda page_no, page_size: adapter.list(
                page_no=page_no,
                page_size=page_size,
                search=normalized_identifier,
            ),
            page_size=DEFAULT_RESOLUTION_PAGE_SIZE,
            max_pages=MAX_RESOLUTION_PAGES,
            safety_message=(
                f"Queue search for {normalized_identifier!r} exceeded the resolver "
                "safety limit"
            ),
            safety_details={"resource": QUEUE_RESOURCE, "name": normalized_identifier},
        ),
        matches=lambda candidate: (
            candidate.queueName == normalized_identifier and candidate.id is not None
        ),
        project=resolved_queue,
        dedupe_key=lambda match: match.id,
    )
    return require_single_match(
        matches,
        not_found_message=f"Queue {normalized_identifier!r} was not found",
        not_found_details={"resource": QUEUE_RESOURCE, "name": normalized_identifier},
        ambiguous_message=f"Queue name {normalized_identifier!r} is ambiguous",
        ambiguous_details={
            "resource": QUEUE_RESOURCE,
            "name": normalized_identifier,
            "ids": [match.id for match in matches],
        },
    )


def tenant(
    identifier: str,
    *,
    adapter: TenantOperations,
) -> ResolvedTenant:
    """Resolve a tenant code-or-id into a stable id/code pair."""
    normalized_identifier = normalize_identifier(identifier, label="Tenant")
    tenant_id = parse_numeric_identifier(normalized_identifier)
    if tenant_id is not None:
        return resolve_direct(
            tenant_id,
            load=lambda resolved_id: adapter.get(tenant_id=resolved_id),
            project=resolved_tenant,
            not_found_result_codes=(TENANT_NOT_EXIST,),
            not_found_message=f"Tenant id {tenant_id} was not found",
            not_found_details={"resource": TENANT_RESOURCE, "id": tenant_id},
        )

    matches = resolve_exact_matches(
        collect_resolution_page_items(
            fetch_page=lambda page_no, page_size: adapter.list(
                page_no=page_no,
                page_size=page_size,
                search=normalized_identifier,
            ),
            page_size=DEFAULT_RESOLUTION_PAGE_SIZE,
            max_pages=MAX_RESOLUTION_PAGES,
            safety_message=(
                f"Tenant search for {normalized_identifier!r} exceeded the resolver "
                "safety limit"
            ),
            safety_details={
                "resource": TENANT_RESOURCE,
                "tenantCode": normalized_identifier,
            },
        ),
        matches=lambda candidate: (
            candidate.tenantCode == normalized_identifier and candidate.id is not None
        ),
        project=resolved_tenant,
        dedupe_key=lambda match: match.id,
    )
    return require_single_match(
        matches,
        not_found_message=f"Tenant {normalized_identifier!r} was not found",
        not_found_details={
            "resource": TENANT_RESOURCE,
            "tenantCode": normalized_identifier,
        },
        ambiguous_message=f"Tenant code {normalized_identifier!r} is ambiguous",
        ambiguous_details={
            "resource": TENANT_RESOURCE,
            "tenantCode": normalized_identifier,
            "ids": [match.id for match in matches],
        },
    )


def worker_group(
    identifier: str,
    *,
    adapter: WorkerGroupOperations,
) -> ResolvedWorkerGroup:
    """Resolve a worker-group name-or-id into a stable payload snapshot."""
    normalized_identifier = normalize_identifier(identifier, label="Worker group")
    worker_group_id = parse_numeric_identifier(normalized_identifier)
    if worker_group_id is not None:
        return resolve_direct(
            worker_group_id,
            load=lambda resolved_id: adapter.get(worker_group_id=resolved_id),
            project=resolved_worker_group,
            not_found_result_codes=(WORKER_GROUP_NOT_EXIST,),
            not_found_message=f"Worker group id {worker_group_id} was not found",
            not_found_details={
                "resource": WORKER_GROUP_RESOURCE,
                "id": worker_group_id,
            },
        )

    matches = resolve_exact_matches(
        collect_resolution_page_items(
            fetch_page=lambda page_no, page_size: adapter.list(
                page_no=page_no,
                page_size=page_size,
                search=normalized_identifier,
            ),
            page_size=DEFAULT_RESOLUTION_PAGE_SIZE,
            max_pages=MAX_RESOLUTION_PAGES,
            safety_message=(
                f"Worker group search for {normalized_identifier!r} exceeded the "
                "resolver safety limit"
            ),
            safety_details={
                "resource": WORKER_GROUP_RESOURCE,
                "name": normalized_identifier,
            },
        ),
        matches=lambda candidate: candidate.name == normalized_identifier,
        project=resolved_worker_group,
        dedupe_key=lambda match: (match.id, match.name, match.system_default),
    )
    if not matches:
        # Config-derived rows are not guaranteed to participate in filtered UI
        # pages, so keep a full-snapshot fallback for exact-name resolution.
        matches = resolve_exact_matches(
            adapter.list_all(),
            matches=lambda candidate: candidate.name == normalized_identifier,
            project=resolved_worker_group,
            dedupe_key=lambda match: (match.id, match.name, match.system_default),
        )
    return require_single_match(
        matches,
        not_found_message=f"Worker group {normalized_identifier!r} was not found",
        not_found_details={
            "resource": WORKER_GROUP_RESOURCE,
            "name": normalized_identifier,
        },
        ambiguous_message=f"Worker group name {normalized_identifier!r} is ambiguous",
        ambiguous_details={
            "resource": WORKER_GROUP_RESOURCE,
            "name": normalized_identifier,
            "matches": [match.to_details() for match in matches],
        },
    )


def task(
    identifier: str,
    *,
    adapter: TaskOperations,
    project_code: int,
    workflow_code: int,
) -> ResolvedTask:
    """Resolve a task name-or-code within one workflow."""
    normalized_identifier = normalize_identifier(identifier, label="Task")
    tasks = adapter.list(project_code=project_code, workflow_code=workflow_code)
    task_code = parse_numeric_identifier(normalized_identifier)
    if task_code is not None:
        matches = resolve_exact_matches(
            tasks,
            matches=lambda candidate: (
                candidate.code == task_code and candidate.name is not None
            ),
            project=resolved_task,
            dedupe_key=lambda match: match.code,
        )
        return require_single_match(
            matches,
            not_found_message=f"Task code {task_code} was not found",
            not_found_details={
                "resource": TASK_RESOURCE,
                "project_code": project_code,
                "workflow_code": workflow_code,
                "code": task_code,
            },
            ambiguous_message=f"Task code {task_code} is ambiguous",
            ambiguous_details={
                "resource": TASK_RESOURCE,
                "project_code": project_code,
                "workflow_code": workflow_code,
                "code": task_code,
                "codes": [match.code for match in matches],
            },
        )

    matches = resolve_exact_matches(
        tasks,
        matches=lambda candidate: (
            candidate.name == normalized_identifier and candidate.code is not None
        ),
        project=resolved_task,
        dedupe_key=lambda match: match.code,
    )
    return require_single_match(
        matches,
        not_found_message=f"Task {normalized_identifier!r} was not found",
        not_found_details={
            "resource": TASK_RESOURCE,
            "project_code": project_code,
            "workflow_code": workflow_code,
            "name": normalized_identifier,
        },
        ambiguous_message=f"Task name {normalized_identifier!r} is ambiguous",
        ambiguous_details={
            "resource": TASK_RESOURCE,
            "project_code": project_code,
            "workflow_code": workflow_code,
            "name": normalized_identifier,
            "codes": [match.code for match in matches],
        },
    )
