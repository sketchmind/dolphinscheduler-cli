from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, TypedDict

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
from dsctl.errors import ResolutionError

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dsctl.upstream.protocol import (
        AlertGroupRecord,
        AlertPluginPayloadRecord,
        ClusterRecord,
        DataSourceRecord,
        EnvironmentRecord,
        NamespaceRecord,
        ProjectParameterRecord,
        ProjectRecord,
        QueueRecord,
        TaskGroupRecord,
        TaskRecord,
        TenantRecord,
        UserListRecord,
        UserRecord,
        WorkerGroupRecord,
        WorkflowRecord,
    )


class ResolvedProjectData(TypedDict):
    """Stable JSON shape for resolved project metadata."""

    code: int
    name: str
    description: str | None


class ResolvedEnvironmentData(TypedDict):
    """Stable JSON shape for resolved environment metadata."""

    code: int
    name: str
    description: str | None


class ResolvedClusterData(TypedDict):
    """Stable JSON shape for resolved cluster metadata."""

    code: int
    name: str
    description: str | None


class ResolvedProjectParameterData(TypedDict):
    """Stable JSON shape for resolved project-parameter metadata."""

    code: int
    paramName: str
    paramDataType: str | None


class ResolvedDataSourceData(TypedDict):
    """Stable JSON shape for resolved datasource metadata."""

    id: int
    name: str
    note: str | None
    type: str | None


class ResolvedNamespaceData(TypedDict):
    """Stable JSON shape for resolved namespace metadata."""

    id: int
    namespace: str
    clusterCode: int | None
    clusterName: str | None


class ResolvedAlertGroupData(TypedDict):
    """Stable JSON shape for resolved alert-group metadata."""

    id: int
    groupName: str
    description: str | None


class ResolvedAlertPluginData(TypedDict):
    """Stable JSON shape for resolved alert-plugin metadata."""

    id: int
    instanceName: str
    pluginDefineId: int
    alertPluginName: str | None


class ResolvedQueueData(TypedDict):
    """Stable JSON shape for resolved queue metadata."""

    id: int
    queueName: str
    queue: str | None


class ResolvedTaskGroupData(TypedDict):
    """Stable JSON shape for resolved task-group metadata."""

    id: int
    name: str
    projectCode: int


class ResolvedUserData(TypedDict):
    """Stable JSON shape for resolved user metadata."""

    id: int
    userName: str
    email: str | None
    tenantId: int
    tenantCode: str | None
    state: int


class ResolvedTenantData(TypedDict):
    """Stable JSON shape for resolved tenant metadata."""

    id: int
    tenantCode: str
    description: str | None
    queueId: int
    queueName: str | None
    queue: str | None


class ResolvedWorkerGroupData(TypedDict):
    """Stable JSON shape for resolved worker-group metadata."""

    id: int | None
    name: str
    addrList: str | None
    systemDefault: bool


class ResolvedWorkflowData(TypedDict):
    """Stable JSON shape for resolved workflow metadata."""

    code: int
    name: str
    version: int | None


class ResolvedTaskData(TypedDict):
    """Stable JSON shape for resolved task metadata."""

    code: int
    name: str
    version: int | None


@dataclass(frozen=True)
class ResolvedProject:
    """Stable resolved project identity used across service calls."""

    code: int
    name: str
    description: str | None

    def to_data(self) -> ResolvedProjectData:
        """Render the resolved project as JSON-safe metadata."""
        return {
            "code": self.code,
            "name": self.name,
            "description": self.description,
        }


@dataclass(frozen=True)
class ResolvedEnvironment:
    """Stable resolved environment identity used across service calls."""

    code: int
    name: str
    description: str | None

    def to_data(self) -> ResolvedEnvironmentData:
        """Render the resolved environment as JSON-safe metadata."""
        return {
            "code": self.code,
            "name": self.name,
            "description": self.description,
        }


@dataclass(frozen=True)
class ResolvedCluster:
    """Stable resolved cluster identity used across service calls."""

    code: int
    name: str
    description: str | None

    def to_data(self) -> ResolvedClusterData:
        """Render the resolved cluster as JSON-safe metadata."""
        return {
            "code": self.code,
            "name": self.name,
            "description": self.description,
        }


@dataclass(frozen=True)
class ResolvedAlertPlugin:
    """Stable resolved alert-plugin identity used across service calls."""

    id: int
    instance_name: str
    plugin_define_id: int
    alert_plugin_name: str | None

    def to_data(self) -> ResolvedAlertPluginData:
        """Render the resolved alert-plugin as JSON-safe metadata."""
        return {
            "id": self.id,
            "instanceName": self.instance_name,
            "pluginDefineId": self.plugin_define_id,
            "alertPluginName": self.alert_plugin_name,
        }


@dataclass(frozen=True)
class ResolvedProjectParameter:
    """Stable resolved project-parameter identity used across service calls."""

    code: int
    param_name: str
    param_data_type: str | None

    def to_data(self) -> ResolvedProjectParameterData:
        """Render the resolved project parameter as JSON-safe metadata."""
        return {
            "code": self.code,
            "paramName": self.param_name,
            "paramDataType": self.param_data_type,
        }


@dataclass(frozen=True)
class ResolvedDataSource:
    """Stable resolved datasource identity used across service calls."""

    id: int
    name: str
    note: str | None
    type: str | None

    def to_data(self) -> ResolvedDataSourceData:
        """Render the resolved datasource as JSON-safe metadata."""
        return {
            "id": self.id,
            "name": self.name,
            "note": self.note,
            "type": self.type,
        }


@dataclass(frozen=True)
class ResolvedNamespace:
    """Stable resolved namespace identity used across service calls."""

    id: int
    namespace_name: str
    cluster_code: int | None
    cluster_name: str | None

    @property
    def namespace(self) -> str:
        """Expose the upstream namespace field for structural serializers."""
        return self.namespace_name

    @property
    def clusterCode(self) -> int | None:  # noqa: N802
        """Expose the upstream clusterCode field for structural serializers."""
        return self.cluster_code

    @property
    def clusterName(self) -> str | None:  # noqa: N802
        """Expose the upstream clusterName field for structural serializers."""
        return self.cluster_name

    def to_data(self) -> ResolvedNamespaceData:
        """Render the resolved namespace as JSON-safe metadata."""
        return {
            "id": self.id,
            "namespace": self.namespace_name,
            "clusterCode": self.cluster_code,
            "clusterName": self.cluster_name,
        }


@dataclass(frozen=True)
class ResolvedAlertGroup:
    """Stable resolved alert-group identity used across service calls."""

    id: int
    group_name: str
    description: str | None

    @property
    def groupName(self) -> str:  # noqa: N802
        """Expose the upstream groupName field for structural serializers."""
        return self.group_name

    def to_data(self) -> ResolvedAlertGroupData:
        """Render the resolved alert-group as JSON-safe metadata."""
        return {
            "id": self.id,
            "groupName": self.group_name,
            "description": self.description,
        }


@dataclass(frozen=True)
class ResolvedQueue:
    """Stable resolved queue identity used across service calls."""

    id: int
    queue_name: str
    queue: str | None

    def to_data(self) -> ResolvedQueueData:
        """Render the resolved queue as JSON-safe metadata."""
        return {
            "id": self.id,
            "queueName": self.queue_name,
            "queue": self.queue,
        }


@dataclass(frozen=True)
class ResolvedTaskGroup:
    """Stable resolved task-group identity used across service calls."""

    id: int
    name: str
    project_code: int

    @property
    def projectCode(self) -> int:  # noqa: N802
        """Expose the upstream projectCode field for structural serializers."""
        return self.project_code

    def to_data(self) -> ResolvedTaskGroupData:
        """Render the resolved task-group as JSON-safe metadata."""
        return {
            "id": self.id,
            "name": self.name,
            "projectCode": self.project_code,
        }


@dataclass(frozen=True)
class ResolvedUser:
    """Stable resolved user identity used across service calls."""

    id: int
    user_name: str
    email: str | None
    tenant_id: int
    tenant_code: str | None
    state: int

    @property
    def userName(self) -> str:  # noqa: N802
        """Expose the upstream userName field for structural serializers."""
        return self.user_name

    @property
    def tenantId(self) -> int:  # noqa: N802
        """Expose the upstream tenantId field for structural serializers."""
        return self.tenant_id

    @property
    def tenantCode(self) -> str | None:  # noqa: N802
        """Expose the upstream tenantCode field for structural serializers."""
        return self.tenant_code

    def to_data(self) -> ResolvedUserData:
        """Render the resolved user as JSON-safe metadata."""
        return {
            "id": self.id,
            "userName": self.user_name,
            "email": self.email,
            "tenantId": self.tenant_id,
            "tenantCode": self.tenant_code,
            "state": self.state,
        }


@dataclass(frozen=True)
class ResolvedTenant:
    """Stable resolved tenant identity used across service calls."""

    id: int
    tenant_code: str
    description: str | None
    queue_id: int
    queue_name: str | None
    queue: str | None

    @property
    def tenantCode(self) -> str:  # noqa: N802
        """Expose the upstream tenantCode field for structural serializers."""
        return self.tenant_code

    @property
    def queueId(self) -> int:  # noqa: N802
        """Expose the upstream queueId field for structural serializers."""
        return self.queue_id

    @property
    def queueName(self) -> str | None:  # noqa: N802
        """Expose the upstream queueName field for structural serializers."""
        return self.queue_name

    def to_data(self) -> ResolvedTenantData:
        """Render the resolved tenant as JSON-safe metadata."""
        return {
            "id": self.id,
            "tenantCode": self.tenant_code,
            "description": self.description,
            "queueId": self.queue_id,
            "queueName": self.queue_name,
            "queue": self.queue,
        }


@dataclass(frozen=True)
class ResolvedWorkerGroup:
    """Stable resolved worker-group payload used across service calls."""

    id: int | None
    name: str
    addr_list: str | None
    description: str | None
    create_time: str | None
    update_time: str | None
    system_default: bool

    @property
    def addrList(self) -> str | None:  # noqa: N802
        """Expose the upstream addrList field for structural serializers."""
        return self.addr_list

    @property
    def createTime(self) -> str | None:  # noqa: N802
        """Expose the upstream createTime field for structural serializers."""
        return self.create_time

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        """Expose the upstream updateTime field for structural serializers."""
        return self.update_time

    @property
    def systemDefault(self) -> bool:  # noqa: N802
        """Expose the upstream systemDefault field for structural serializers."""
        return self.system_default

    def to_data(self) -> ResolvedWorkerGroupData:
        """Render the resolved worker group as JSON-safe metadata."""
        return {
            "id": self.id,
            "name": self.name,
            "addrList": self.addr_list,
            "systemDefault": self.system_default,
        }

    def to_details(self) -> dict[str, str | int | bool | None]:
        """Render the resolved worker group as JSON-safe error details."""
        return {
            "id": self.id,
            "name": self.name,
            "addrList": self.addr_list,
            "systemDefault": self.system_default,
        }


@dataclass(frozen=True)
class ResolvedWorkflow:
    """Stable resolved workflow identity used across service calls."""

    code: int
    name: str
    version: int | None

    def to_data(self) -> ResolvedWorkflowData:
        """Render the resolved workflow as JSON-safe metadata."""
        return {
            "code": self.code,
            "name": self.name,
            "version": self.version,
        }


@dataclass(frozen=True)
class ResolvedTask:
    """Stable resolved task identity used across service calls."""

    code: int
    name: str
    version: int | None

    def to_data(self) -> ResolvedTaskData:
        """Render the resolved task as JSON-safe metadata."""
        return {
            "code": self.code,
            "name": self.name,
            "version": self.version,
        }


def resolved_project(project_record: ProjectRecord) -> ResolvedProject:
    """Project one upstream project record into the stable resolver model."""
    if project_record.code is None or project_record.name is None:
        message = "Resolved project payload was missing required identity fields"
        raise ResolutionError(message, details={"resource": PROJECT_RESOURCE})
    return ResolvedProject(
        code=project_record.code,
        name=project_record.name,
        description=project_record.description,
    )


def resolved_environment(environment_record: EnvironmentRecord) -> ResolvedEnvironment:
    """Project one upstream environment record into the stable resolver model."""
    if environment_record.code is None or environment_record.name is None:
        message = "Resolved environment payload was missing required identity fields"
        raise ResolutionError(message, details={"resource": ENV_RESOURCE})
    return ResolvedEnvironment(
        code=environment_record.code,
        name=environment_record.name,
        description=environment_record.description,
    )


def resolved_cluster(cluster_record: ClusterRecord) -> ResolvedCluster:
    """Project one upstream cluster record into the stable resolver model."""
    if cluster_record.code is None or cluster_record.name is None:
        message = "Resolved cluster payload was missing required identity fields"
        raise ResolutionError(message, details={"resource": CLUSTER_RESOURCE})
    return ResolvedCluster(
        code=cluster_record.code,
        name=cluster_record.name,
        description=cluster_record.description,
    )


def resolved_alert_plugin(
    alert_plugin_record: AlertPluginPayloadRecord,
    *,
    alert_plugin_name: str | None = None,
) -> ResolvedAlertPlugin:
    """Project one alert-plugin instance into the stable resolver model."""
    if alert_plugin_record.id is None or alert_plugin_record.instanceName is None:
        message = "Resolved alert-plugin payload was missing required identity fields"
        raise ResolutionError(message, details={"resource": ALERT_PLUGIN_RESOURCE})
    return ResolvedAlertPlugin(
        id=alert_plugin_record.id,
        instance_name=alert_plugin_record.instanceName,
        plugin_define_id=alert_plugin_record.pluginDefineId,
        alert_plugin_name=alert_plugin_name,
    )


def resolved_project_parameter(
    project_parameter_record: ProjectParameterRecord,
) -> ResolvedProjectParameter:
    """Project one upstream project-parameter record into the resolver model."""
    if (
        project_parameter_record.code is None
        or project_parameter_record.paramName is None
    ):
        message = (
            "Resolved project parameter payload was missing required identity fields"
        )
        raise ResolutionError(
            message,
            details={"resource": PROJECT_PARAMETER_RESOURCE},
        )
    return ResolvedProjectParameter(
        code=project_parameter_record.code,
        param_name=project_parameter_record.paramName,
        param_data_type=project_parameter_record.paramDataType,
    )


def resolved_datasource(datasource_record: DataSourceRecord) -> ResolvedDataSource:
    """Project one upstream datasource record into the stable resolver model."""
    if datasource_record.id is None or datasource_record.name is None:
        message = "Resolved datasource payload was missing required identity fields"
        raise ResolutionError(message, details={"resource": DATASOURCE_RESOURCE})
    return ResolvedDataSource(
        id=datasource_record.id,
        name=datasource_record.name,
        note=datasource_record.note,
        type=enum_value(datasource_record.type),
    )


def resolved_namespace(namespace_record: NamespaceRecord) -> ResolvedNamespace:
    """Project one upstream namespace record into the stable resolver model."""
    if namespace_record.id is None or namespace_record.namespace is None:
        message = "Resolved namespace payload was missing required identity fields"
        raise ResolutionError(message, details={"resource": NAMESPACE_RESOURCE})
    return ResolvedNamespace(
        id=namespace_record.id,
        namespace_name=namespace_record.namespace,
        cluster_code=namespace_record.clusterCode,
        cluster_name=namespace_record.clusterName,
    )


def resolved_alert_group(alert_group_record: AlertGroupRecord) -> ResolvedAlertGroup:
    """Project one upstream alert-group record into the stable resolver model."""
    if alert_group_record.id is None or alert_group_record.groupName is None:
        message = "Resolved alert-group payload was missing required identity fields"
        raise ResolutionError(message, details={"resource": ALERT_GROUP_RESOURCE})
    return ResolvedAlertGroup(
        id=alert_group_record.id,
        group_name=alert_group_record.groupName,
        description=alert_group_record.description,
    )


def resolved_datasource_payload(
    datasource_payload: dict[str, object] | Mapping[str, object],
    *,
    fallback_id: int | None = None,
) -> ResolvedDataSource:
    """Project one datasource detail payload into the stable resolver model."""
    datasource_id = datasource_payload.get("id")
    datasource_name = datasource_payload.get("name")
    if not isinstance(datasource_id, int):
        datasource_id = fallback_id
    if datasource_id is None or not isinstance(datasource_name, str):
        message = "Resolved datasource detail payload was missing required fields"
        raise ResolutionError(message, details={"resource": DATASOURCE_RESOURCE})
    datasource_note = datasource_payload.get("note")
    datasource_type = datasource_payload.get("type")
    return ResolvedDataSource(
        id=datasource_id,
        name=datasource_name,
        note=datasource_note if isinstance(datasource_note, str) else None,
        type=datasource_type if isinstance(datasource_type, str) else None,
    )


def resolved_queue(queue_record: QueueRecord) -> ResolvedQueue:
    """Project one upstream queue record into the stable resolver model."""
    if queue_record.id is None or queue_record.queueName is None:
        message = "Resolved queue payload was missing required identity fields"
        raise ResolutionError(message, details={"resource": QUEUE_RESOURCE})
    return ResolvedQueue(
        id=queue_record.id,
        queue_name=queue_record.queueName,
        queue=queue_record.queue,
    )


def resolved_task_group(task_group_record: TaskGroupRecord) -> ResolvedTaskGroup:
    """Project one upstream task-group record into the stable resolver model."""
    if task_group_record.id is None or task_group_record.name is None:
        message = "Resolved task-group payload was missing required identity fields"
        raise ResolutionError(message, details={"resource": TASK_GROUP_RESOURCE})
    return ResolvedTaskGroup(
        id=task_group_record.id,
        name=task_group_record.name,
        project_code=task_group_record.projectCode,
    )


def resolved_user(user_record: UserListRecord | UserRecord) -> ResolvedUser:
    """Project one upstream user record into the stable resolver model."""
    if user_record.id is None or user_record.userName is None:
        message = "Resolved user payload was missing required identity fields"
        raise ResolutionError(message, details={"resource": USER_RESOURCE})
    return ResolvedUser(
        id=user_record.id,
        user_name=user_record.userName,
        email=user_record.email,
        tenant_id=user_record.tenantId,
        tenant_code=user_record.tenantCode,
        state=user_record.state,
    )


def resolved_tenant(tenant_record: TenantRecord) -> ResolvedTenant:
    """Project one upstream tenant record into the stable resolver model."""
    if tenant_record.id is None or tenant_record.tenantCode is None:
        message = "Resolved tenant payload was missing required identity fields"
        raise ResolutionError(message, details={"resource": TENANT_RESOURCE})
    return ResolvedTenant(
        id=tenant_record.id,
        tenant_code=tenant_record.tenantCode,
        description=tenant_record.description,
        queue_id=tenant_record.queueId,
        queue_name=tenant_record.queueName,
        queue=tenant_record.queue,
    )


def resolved_worker_group(
    worker_group_record: WorkerGroupRecord,
) -> ResolvedWorkerGroup:
    """Project one upstream worker-group record into the stable resolver model."""
    if worker_group_record.name is None:
        message = "Resolved worker-group payload was missing required identity fields"
        raise ResolutionError(message, details={"resource": WORKER_GROUP_RESOURCE})
    return ResolvedWorkerGroup(
        id=worker_group_record.id,
        name=worker_group_record.name,
        addr_list=worker_group_record.addrList,
        description=worker_group_record.description,
        create_time=worker_group_record.createTime,
        update_time=worker_group_record.updateTime,
        system_default=worker_group_record.systemDefault,
    )


def resolved_workflow(workflow_record: WorkflowRecord) -> ResolvedWorkflow:
    """Project one upstream workflow record into the stable resolver model."""
    if workflow_record.code is None or workflow_record.name is None:
        message = "Resolved workflow payload was missing required identity fields"
        raise ResolutionError(message, details={"resource": WORKFLOW_RESOURCE})
    return ResolvedWorkflow(
        code=workflow_record.code,
        name=workflow_record.name,
        version=workflow_record.version,
    )


def resolved_task(task_record: TaskRecord) -> ResolvedTask:
    """Project one upstream task record into the stable resolver model."""
    if task_record.code is None or task_record.name is None:
        message = "Resolved task payload was missing required identity fields"
        raise ResolutionError(message, details={"resource": TASK_RESOURCE})
    return ResolvedTask(
        code=task_record.code,
        name=task_record.name,
        version=task_record.version,
    )


def enum_value(value: object) -> str | None:
    """Project one enum-like payload value into a string when possible."""
    if value is None:
        return None
    enum_like_value = getattr(value, "value", None)
    if isinstance(enum_like_value, str):
        return enum_like_value
    if isinstance(value, str):
        return value
    return None
