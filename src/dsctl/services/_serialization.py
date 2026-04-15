from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, TypeAlias, TypedDict

from dsctl.cli_surface import (
    ACCESS_TOKEN_RESOURCE,
    ALERT_GROUP_RESOURCE,
    ALERT_PLUGIN_RESOURCE,
    AUDIT_RESOURCE,
    CLUSTER_RESOURCE,
    DATASOURCE_RESOURCE,
    ENV_RESOURCE,
    NAMESPACE_RESOURCE,
    PROJECT_PARAMETER_RESOURCE,
    PROJECT_PREFERENCE_RESOURCE,
    PROJECT_WORKER_GROUP_RESOURCE,
    QUEUE_RESOURCE,
    SCHEDULE_RESOURCE,
    TASK_GROUP_RESOURCE,
    TASK_INSTANCE_RESOURCE,
    TASK_RESOURCE,
    TENANT_RESOURCE,
    USER_RESOURCE,
    WORKFLOW_INSTANCE_RESOURCE,
    resource_label,
)
from dsctl.errors import ApiTransportError

if TYPE_CHECKING:
    from dsctl.upstream.protocol import (
        AccessTokenRecord,
        AlertGroupRecord,
        AlertPluginListItemRecord,
        AlertPluginPayloadRecord,
        AuditModelTypeRecord,
        AuditOperationTypeRecord,
        AuditRecord,
        ClusterPayloadRecord,
        DataSourceRecord,
        DependentLineageTaskRecord,
        EnvironmentPayloadRecord,
        MonitorDatabaseRecord,
        MonitorServerRecord,
        NamespaceRecord,
        PluginDefineRecord,
        ProjectParameterRecord,
        ProjectPreferenceRecord,
        ProjectWorkerGroupRecord,
        QueueRecord,
        ResourceItemRecord,
        SchedulePayloadRecord,
        StringEnumValue,
        TaskGroupQueueRecord,
        TaskGroupRecord,
        TaskInstanceRecord,
        TaskPayloadRecord,
        TaskRecord,
        TenantRecord,
        UserListRecord,
        UserRecord,
        WorkerGroupRecord,
        WorkflowInstanceRecord,
        WorkflowLineageDetailRecord,
        WorkflowLineageRecord,
        WorkflowLineageRelationRecord,
    )


StructuredDataValue: TypeAlias = (
    None
    | bool
    | int
    | float
    | str
    | Sequence["StructuredDataValue"]
    | Mapping[str, "StructuredDataValue"]
)


class ScheduleData(TypedDict):
    """JSON object emitted for one schedule payload."""

    id: int
    workflowDefinitionCode: int
    workflowDefinitionName: str | None
    projectName: str | None
    definitionDescription: str | None
    startTime: str | None
    endTime: str | None
    timezoneId: str | None
    crontab: str | None
    failureStrategy: str | None
    warningType: str | None
    createTime: str | None
    updateTime: str | None
    userId: int
    userName: str | None
    releaseState: str | None
    warningGroupId: int
    workflowInstancePriority: str | None
    workerGroup: str | None
    tenantCode: str | None
    environmentCode: int | None
    environmentName: str | None


class EnvironmentData(TypedDict):
    """JSON object emitted for one environment payload."""

    id: int | None
    code: int
    name: str | None
    config: str | None
    description: str | None
    workerGroups: list[str] | None
    operator: int | None
    createTime: str | None
    updateTime: str | None


class ClusterData(TypedDict):
    """JSON object emitted for one cluster payload."""

    id: int
    code: int
    name: str | None
    config: str | None
    description: str | None
    workflowDefinitions: list[str] | None
    operator: int | None
    createTime: str | None
    updateTime: str | None


class DataSourceData(TypedDict):
    """JSON object emitted for one datasource summary payload."""

    id: int
    name: str | None
    note: str | None
    type: str | None
    userId: int
    userName: str | None
    createTime: str | None
    updateTime: str | None


class AlertGroupData(TypedDict):
    """JSON object emitted for one alert-group payload."""

    id: int
    groupName: str | None
    alertInstanceIds: str | None
    description: str | None
    createTime: str | None
    updateTime: str | None
    createUserId: int


class AlertPluginData(TypedDict):
    """JSON object emitted for one alert-plugin payload."""

    id: int
    pluginDefineId: int
    instanceName: str | None
    pluginInstanceParams: str | None
    createTime: str | None
    updateTime: str | None
    instanceType: str | None
    warningType: str | None
    alertPluginName: str | None


class AccessTokenData(TypedDict):
    """JSON object emitted for one access-token payload."""

    id: int
    userId: int
    token: str | None
    expireTime: str | None
    createTime: str | None
    updateTime: str | None
    userName: str | None


class AuditData(TypedDict):
    """JSON object emitted for one audit-log row."""

    userName: str | None
    modelType: str | None
    modelName: str | None
    operation: str | None
    createTime: str | None
    description: str | None
    detail: str | None
    latency: str | None


class NamespaceData(TypedDict):
    """JSON object emitted for one namespace payload."""

    id: int
    code: int | None
    namespace: str | None
    clusterCode: int | None
    clusterName: str | None
    userId: int
    userName: str | None
    createTime: str | None
    updateTime: str | None


class QueueData(TypedDict):
    """JSON object emitted for one queue payload."""

    id: int
    queueName: str | None
    queue: str | None
    createTime: str | None
    updateTime: str | None


class TaskGroupData(TypedDict):
    """JSON object emitted for one task-group payload."""

    id: int
    name: str | None
    projectCode: int
    description: str | None
    groupSize: int
    useSize: int
    userId: int
    status: str | None
    createTime: str | None
    updateTime: str | None


class TaskGroupQueueData(TypedDict):
    """JSON object emitted for one task-group queue payload."""

    id: int
    taskId: int
    taskName: str | None
    projectName: str | None
    projectCode: str | None
    workflowInstanceName: str | None
    groupId: int
    workflowInstanceId: int | None
    priority: int
    forceStart: int
    inQueue: int
    status: str | None
    createTime: str | None
    updateTime: str | None


class WorkflowLineageRelationData(TypedDict):
    """JSON object emitted for one workflow-lineage edge."""

    sourceWorkFlowCode: int
    targetWorkFlowCode: int


class WorkflowLineageDetailData(TypedDict):
    """JSON object emitted for one workflow-lineage node/detail row."""

    workFlowCode: int
    workFlowName: str | None
    workFlowPublishStatus: str | None
    scheduleStartTime: str | None
    scheduleEndTime: str | None
    crontab: str | None
    schedulePublishStatus: int
    sourceWorkFlowCode: str | None


class WorkflowLineageData(TypedDict):
    """JSON object emitted for one workflow-lineage graph."""

    workFlowRelationList: list[WorkflowLineageRelationData]
    workFlowRelationDetailList: list[WorkflowLineageDetailData]


class DependentLineageTaskData(TypedDict):
    """JSON object emitted for one dependent-task lineage row."""

    projectCode: int
    workflowDefinitionCode: int
    workflowDefinitionName: str | None
    taskDefinitionCode: int
    taskDefinitionName: str | None


class ProjectParameterData(TypedDict):
    """JSON object emitted for one project-parameter payload."""

    id: int | None
    userId: int | None
    operator: int | None
    code: int
    projectCode: int
    paramName: str | None
    paramValue: str | None
    paramDataType: str | None
    createTime: str | None
    updateTime: str | None
    createUser: str | None
    modifyUser: str | None


class ProjectWorkerGroupData(TypedDict):
    """JSON object emitted for one project worker-group payload."""

    id: int | None
    projectCode: int
    workerGroup: str | None
    createTime: str | None
    updateTime: str | None


class ProjectPreferenceData(TypedDict):
    """JSON object emitted for one project-preference payload."""

    id: int | None
    code: int
    projectCode: int
    preferences: str | None
    userId: int | None
    state: int
    createTime: str | None
    updateTime: str | None


class ResourceItemData(TypedDict):
    """JSON object emitted for one resource list item."""

    alias: str | None
    userName: str | None
    fileName: str | None
    fullName: str | None
    isDirectory: bool
    type: str | None
    size: int
    createTime: str | None
    updateTime: str | None


class WorkerGroupData(TypedDict):
    """JSON object emitted for one worker-group payload."""

    id: int | None
    name: str | None
    addrList: str | None
    createTime: str | None
    updateTime: str | None
    description: str | None
    systemDefault: bool


class TenantData(TypedDict):
    """JSON object emitted for one tenant payload."""

    id: int
    tenantCode: str | None
    description: str | None
    queueId: int
    queueName: str | None
    queue: str | None
    createTime: str | None
    updateTime: str | None


class UserListItemData(TypedDict):
    """JSON object emitted for one user in `user list`."""

    id: int
    userName: str | None
    email: str | None
    phone: str | None
    userType: str | None
    tenantId: int
    tenantCode: str | None
    queueName: str | None
    queue: str | None
    state: int
    createTime: str | None
    updateTime: str | None


class UserData(UserListItemData):
    """JSON object emitted for one user payload."""

    timeZone: str | None


class MonitorServerData(TypedDict):
    """JSON object emitted for one monitor server payload."""

    id: int
    host: str | None
    port: int
    serverDirectory: str | None
    heartBeatInfo: str | None
    createTime: str | None
    lastHeartbeatTime: str | None


class AuditModelTypeData(TypedDict):
    """JSON object emitted for one audit model-type tree node."""

    name: str
    child: list[AuditModelTypeData] | None


class PluginDefineData(TypedDict):
    """JSON object emitted for one UI-plugin definition payload."""

    id: int
    pluginName: str | None
    pluginType: str | None
    pluginParams: str | None
    createTime: str | None
    updateTime: str | None


class MonitorDatabaseData(TypedDict):
    """JSON object emitted for one monitor database metrics payload."""

    dbType: str | None
    state: str | None
    maxConnections: int
    maxUsedConnections: int
    threadsConnections: int
    threadsRunningConnections: int
    date: str | None


class AuditOperationTypeData(TypedDict):
    """JSON object emitted for one audit operation-type node."""

    name: str


class TaskListItem(TypedDict):
    """JSON object emitted for one task in `task list`."""

    code: int
    name: str | None
    version: int | None


class TaskData(TypedDict):
    """JSON object emitted for one task payload."""

    id: int | None
    code: int
    name: str | None
    version: int | None
    projectCode: int
    description: str | None
    taskType: str | None
    taskParams: StructuredDataValue
    userName: str | None
    projectName: str | None
    workerGroup: str | None
    failRetryTimes: int
    failRetryInterval: int
    timeout: int
    delayTime: int
    resourceIds: str | None
    createTime: str | None
    updateTime: str | None
    modifyBy: str | None
    taskGroupId: int
    taskGroupPriority: int
    environmentCode: int
    taskPriority: str | None
    timeoutFlag: str | None
    timeoutNotifyStrategy: str | None
    taskExecuteType: str | None
    flag: str | None
    cpuQuota: int | None
    memoryMax: int | None


class WorkflowInstanceData(TypedDict):
    """JSON object emitted for one workflow-instance payload."""

    id: int
    workflowDefinitionCode: int | None
    workflowDefinitionVersion: int
    projectCode: int
    state: str | None
    recovery: str | None
    startTime: str | None
    endTime: str | None
    runTimes: int
    name: str | None
    host: str | None
    commandType: str | None
    taskDependType: str | None
    failureStrategy: str | None
    warningType: str | None
    scheduleTime: str | None
    executorId: int
    executorName: str | None
    tenantCode: str | None
    queue: str | None
    duration: str | None
    workflowInstancePriority: str | None
    workerGroup: str | None
    environmentCode: int | None
    timeout: int
    dryRun: int
    restartTime: str | None


class TaskInstanceData(TypedDict):
    """JSON object emitted for one task-instance payload."""

    id: int
    name: str | None
    taskType: str | None
    workflowInstanceId: int
    workflowInstanceName: str | None
    projectCode: int
    taskCode: int
    taskDefinitionVersion: int
    processDefinitionName: str | None
    state: str | None
    firstSubmitTime: str | None
    submitTime: str | None
    startTime: str | None
    endTime: str | None
    host: str | None
    logPath: str | None
    retryTimes: int
    duration: str | None
    executorName: str | None
    workerGroup: str | None
    environmentCode: int | None
    delayTime: int
    taskParams: str | None
    dryRun: int
    taskGroupId: int
    taskExecuteType: str | None


class TaskLogData(TypedDict):
    """JSON object emitted for one task log view."""

    text: str
    lineCount: int


def serialize_schedule(schedule: SchedulePayloadRecord) -> ScheduleData:
    """Serialize one schedule payload for schedule and workflow commands."""
    return {
        "id": require_resource_int(
            schedule.id,
            resource=SCHEDULE_RESOURCE,
            field_name="schedule.id",
        ),
        "workflowDefinitionCode": schedule.workflowDefinitionCode,
        "workflowDefinitionName": schedule.workflowDefinitionName,
        "projectName": schedule.projectName,
        "definitionDescription": schedule.definitionDescription,
        "startTime": schedule.startTime,
        "endTime": schedule.endTime,
        "timezoneId": schedule.timezoneId,
        "crontab": schedule.crontab,
        "failureStrategy": enum_value(schedule.failureStrategy),
        "warningType": enum_value(schedule.warningType),
        "createTime": schedule.createTime,
        "updateTime": schedule.updateTime,
        "userId": schedule.userId,
        "userName": schedule.userName,
        "releaseState": enum_value(schedule.releaseState),
        "warningGroupId": schedule.warningGroupId,
        "workflowInstancePriority": enum_value(schedule.workflowInstancePriority),
        "workerGroup": schedule.workerGroup,
        "tenantCode": schedule.tenantCode,
        "environmentCode": schedule.environmentCode,
        "environmentName": schedule.environmentName,
    }


def serialize_environment(environment: EnvironmentPayloadRecord) -> EnvironmentData:
    """Serialize one environment payload for env commands."""
    return {
        "id": environment.id,
        "code": require_resource_int(
            environment.code,
            resource=ENV_RESOURCE,
            field_name="environment.code",
        ),
        "name": environment.name,
        "config": environment.config,
        "description": environment.description,
        "workerGroups": (
            None if environment.workerGroups is None else list(environment.workerGroups)
        ),
        "operator": environment.operator,
        "createTime": environment.createTime,
        "updateTime": environment.updateTime,
    }


def serialize_cluster(cluster: ClusterPayloadRecord) -> ClusterData:
    """Serialize one cluster payload for cluster commands."""
    return {
        "id": require_resource_int(
            cluster.id,
            resource=CLUSTER_RESOURCE,
            field_name="cluster.id",
        ),
        "code": require_resource_int(
            cluster.code,
            resource=CLUSTER_RESOURCE,
            field_name="cluster.code",
        ),
        "name": cluster.name,
        "config": cluster.config,
        "description": cluster.description,
        "workflowDefinitions": (
            None
            if cluster.workflowDefinitions is None
            else list(cluster.workflowDefinitions)
        ),
        "operator": cluster.operator,
        "createTime": cluster.createTime,
        "updateTime": cluster.updateTime,
    }


def serialize_datasource(datasource: DataSourceRecord) -> DataSourceData:
    """Serialize one datasource summary payload for datasource commands."""
    return {
        "id": require_resource_int(
            datasource.id,
            resource=DATASOURCE_RESOURCE,
            field_name="datasource.id",
        ),
        "name": datasource.name,
        "note": datasource.note,
        "type": enum_value(datasource.type),
        "userId": datasource.userId,
        "userName": datasource.userName,
        "createTime": datasource.createTime,
        "updateTime": datasource.updateTime,
    }


def serialize_alert_group(alert_group: AlertGroupRecord) -> AlertGroupData:
    """Serialize one alert-group payload for alert-group commands."""
    return {
        "id": require_resource_int(
            alert_group.id,
            resource=ALERT_GROUP_RESOURCE,
            field_name="alert_group.id",
        ),
        "groupName": alert_group.groupName,
        "alertInstanceIds": alert_group.alertInstanceIds,
        "description": alert_group.description,
        "createTime": alert_group.createTime,
        "updateTime": alert_group.updateTime,
        "createUserId": alert_group.createUserId,
    }


def serialize_alert_plugin_payload(
    alert_plugin: AlertPluginPayloadRecord,
    *,
    instance_type: str | None = None,
    warning_type: str | None = None,
    alert_plugin_name: str | None = None,
) -> AlertPluginData:
    """Serialize one alert-plugin payload for alert-plugin commands."""
    return {
        "id": require_resource_int(
            alert_plugin.id,
            resource=ALERT_PLUGIN_RESOURCE,
            field_name="alert_plugin.id",
        ),
        "pluginDefineId": alert_plugin.pluginDefineId,
        "instanceName": alert_plugin.instanceName,
        "pluginInstanceParams": alert_plugin.pluginInstanceParams,
        "createTime": alert_plugin.createTime,
        "updateTime": alert_plugin.updateTime,
        "instanceType": instance_type,
        "warningType": warning_type,
        "alertPluginName": alert_plugin_name,
    }


def serialize_alert_plugin_list_item(
    alert_plugin: AlertPluginListItemRecord,
) -> AlertPluginData:
    """Serialize one alert-plugin list item for alert-plugin commands."""
    return serialize_alert_plugin_payload(
        alert_plugin,
        instance_type=alert_plugin.instanceType,
        warning_type=alert_plugin.warningType,
        alert_plugin_name=alert_plugin.alertPluginName,
    )


def serialize_audit_log(audit_log: AuditRecord) -> AuditData:
    """Serialize one audit-log row for audit commands."""
    return {
        "userName": audit_log.userName,
        "modelType": audit_log.modelType,
        "modelName": audit_log.modelName,
        "operation": audit_log.operation,
        "createTime": audit_log.createTime,
        "description": audit_log.description,
        "detail": audit_log.detail,
        "latency": audit_log.latency,
    }


def serialize_audit_model_type(
    model_type: AuditModelTypeRecord,
) -> AuditModelTypeData:
    """Serialize one audit model-type tree node."""
    child = model_type.child
    if isinstance(child, list):
        serialized_child = [
            serialize_audit_model_type(child_item) for child_item in child
        ]
    else:
        serialized_child = None
    return {
        "name": require_resource_text(
            model_type.name,
            resource=AUDIT_RESOURCE,
            field_name="audit_model_type.name",
        ),
        "child": serialized_child,
    }


def serialize_audit_operation_type(
    operation_type: AuditOperationTypeRecord,
) -> AuditOperationTypeData:
    """Serialize one audit operation-type node."""
    return {
        "name": require_resource_text(
            operation_type.name,
            resource=AUDIT_RESOURCE,
            field_name="audit_operation_type.name",
        )
    }


def serialize_namespace(namespace: NamespaceRecord) -> NamespaceData:
    """Serialize one namespace payload for namespace and user commands."""
    return {
        "id": require_resource_int(
            namespace.id,
            resource=NAMESPACE_RESOURCE,
            field_name="namespace.id",
        ),
        "code": namespace.code,
        "namespace": namespace.namespace,
        "clusterCode": namespace.clusterCode,
        "clusterName": namespace.clusterName,
        "userId": namespace.userId,
        "userName": namespace.userName,
        "createTime": namespace.createTime,
        "updateTime": namespace.updateTime,
    }


def serialize_queue(queue: QueueRecord) -> QueueData:
    """Serialize one queue payload for queue commands."""
    return {
        "id": require_resource_int(
            queue.id,
            resource=QUEUE_RESOURCE,
            field_name="queue.id",
        ),
        "queueName": queue.queueName,
        "queue": queue.queue,
        "createTime": queue.createTime,
        "updateTime": queue.updateTime,
    }


def serialize_task_group(task_group: TaskGroupRecord) -> TaskGroupData:
    """Serialize one task-group payload for task-group commands."""
    return {
        "id": require_resource_int(
            task_group.id,
            resource=TASK_GROUP_RESOURCE,
            field_name="task_group.id",
        ),
        "name": task_group.name,
        "projectCode": task_group.projectCode,
        "description": task_group.description,
        "groupSize": task_group.groupSize,
        "useSize": task_group.useSize,
        "userId": task_group.userId,
        "status": enum_value(task_group.status),
        "createTime": task_group.createTime,
        "updateTime": task_group.updateTime,
    }


def serialize_task_group_queue(
    task_group_queue: TaskGroupQueueRecord,
) -> TaskGroupQueueData:
    """Serialize one task-group queue payload for task-group commands."""
    return {
        "id": require_resource_int(
            task_group_queue.id,
            resource=TASK_GROUP_RESOURCE,
            field_name="task_group_queue.id",
        ),
        "taskId": task_group_queue.taskId,
        "taskName": task_group_queue.taskName,
        "projectName": task_group_queue.projectName,
        "projectCode": task_group_queue.projectCode,
        "workflowInstanceName": task_group_queue.workflowInstanceName,
        "groupId": task_group_queue.groupId,
        "workflowInstanceId": task_group_queue.workflowInstanceId,
        "priority": task_group_queue.priority,
        "forceStart": task_group_queue.forceStart,
        "inQueue": task_group_queue.inQueue,
        "status": enum_value(task_group_queue.status),
        "createTime": task_group_queue.createTime,
        "updateTime": task_group_queue.updateTime,
    }


def serialize_workflow_lineage_relation(
    relation: WorkflowLineageRelationRecord,
) -> WorkflowLineageRelationData:
    """Serialize one workflow-lineage edge for lineage commands."""
    return {
        "sourceWorkFlowCode": relation.sourceWorkFlowCode,
        "targetWorkFlowCode": relation.targetWorkFlowCode,
    }


def serialize_workflow_lineage_detail(
    detail: WorkflowLineageDetailRecord,
) -> WorkflowLineageDetailData:
    """Serialize one workflow-lineage node/detail row for lineage commands."""
    return {
        "workFlowCode": detail.workFlowCode,
        "workFlowName": detail.workFlowName,
        "workFlowPublishStatus": detail.workFlowPublishStatus,
        "scheduleStartTime": detail.scheduleStartTime,
        "scheduleEndTime": detail.scheduleEndTime,
        "crontab": detail.crontab,
        "schedulePublishStatus": detail.schedulePublishStatus,
        "sourceWorkFlowCode": detail.sourceWorkFlowCode,
    }


def serialize_workflow_lineage(
    lineage: WorkflowLineageRecord | None,
) -> WorkflowLineageData:
    """Serialize one workflow-lineage graph for lineage commands."""
    if lineage is None:
        return {
            "workFlowRelationList": [],
            "workFlowRelationDetailList": [],
        }
    return {
        "workFlowRelationList": [
            serialize_workflow_lineage_relation(relation)
            for relation in lineage.workFlowRelationList or []
        ],
        "workFlowRelationDetailList": [
            serialize_workflow_lineage_detail(detail)
            for detail in lineage.workFlowRelationDetailList or []
        ],
    }


def serialize_dependent_lineage_task(
    task: DependentLineageTaskRecord,
) -> DependentLineageTaskData:
    """Serialize one dependent-task lineage row for lineage commands."""
    return {
        "projectCode": task.projectCode,
        "workflowDefinitionCode": task.workflowDefinitionCode,
        "workflowDefinitionName": task.workflowDefinitionName,
        "taskDefinitionCode": task.taskDefinitionCode,
        "taskDefinitionName": task.taskDefinitionName,
    }


def serialize_project_parameter(
    project_parameter: ProjectParameterRecord,
) -> ProjectParameterData:
    """Serialize one project-parameter payload for project-parameter commands."""
    return {
        "id": project_parameter.id,
        "userId": project_parameter.userId,
        "operator": project_parameter.operator,
        "code": require_resource_int(
            project_parameter.code,
            resource=PROJECT_PARAMETER_RESOURCE,
            field_name="project_parameter.code",
        ),
        "projectCode": require_resource_int(
            project_parameter.projectCode,
            resource=PROJECT_PARAMETER_RESOURCE,
            field_name="project_parameter.projectCode",
        ),
        "paramName": project_parameter.paramName,
        "paramValue": project_parameter.paramValue,
        "paramDataType": project_parameter.paramDataType,
        "createTime": project_parameter.createTime,
        "updateTime": project_parameter.updateTime,
        "createUser": project_parameter.createUser,
        "modifyUser": project_parameter.modifyUser,
    }


def serialize_project_worker_group(
    project_worker_group: ProjectWorkerGroupRecord,
) -> ProjectWorkerGroupData:
    """Serialize one project worker-group payload for project worker-group commands."""
    return {
        "id": project_worker_group.id,
        "projectCode": require_resource_int(
            project_worker_group.projectCode,
            resource=PROJECT_WORKER_GROUP_RESOURCE,
            field_name="project_worker_group.projectCode",
        ),
        "workerGroup": project_worker_group.workerGroup,
        "createTime": project_worker_group.createTime,
        "updateTime": project_worker_group.updateTime,
    }


def serialize_project_preference(
    project_preference: ProjectPreferenceRecord,
) -> ProjectPreferenceData:
    """Serialize one project-preference payload for project-preference commands."""
    return {
        "id": project_preference.id,
        "code": require_resource_int(
            project_preference.code,
            resource=PROJECT_PREFERENCE_RESOURCE,
            field_name="project_preference.code",
        ),
        "projectCode": require_resource_int(
            project_preference.projectCode,
            resource=PROJECT_PREFERENCE_RESOURCE,
            field_name="project_preference.projectCode",
        ),
        "preferences": project_preference.preferences,
        "userId": project_preference.userId,
        "state": project_preference.state,
        "createTime": project_preference.createTime,
        "updateTime": project_preference.updateTime,
    }


def serialize_resource_item(resource: ResourceItemRecord) -> ResourceItemData:
    """Serialize one resource item payload for resource commands."""
    return {
        "alias": resource.alias,
        "userName": resource.userName,
        "fileName": resource.fileName,
        "fullName": resource.fullName,
        "isDirectory": resource.isDirectory,
        "type": enum_value(resource.type),
        "size": resource.size,
        "createTime": resource.createTime,
        "updateTime": resource.updateTime,
    }


def serialize_worker_group(worker_group: WorkerGroupRecord) -> WorkerGroupData:
    """Serialize one worker-group payload for worker-group commands."""
    return {
        "id": worker_group.id,
        "name": worker_group.name,
        "addrList": worker_group.addrList,
        "createTime": worker_group.createTime,
        "updateTime": worker_group.updateTime,
        "description": worker_group.description,
        "systemDefault": worker_group.systemDefault,
    }


def serialize_tenant(tenant: TenantRecord) -> TenantData:
    """Serialize one tenant payload for tenant commands."""
    return {
        "id": require_resource_int(
            tenant.id,
            resource=TENANT_RESOURCE,
            field_name="tenant.id",
        ),
        "tenantCode": tenant.tenantCode,
        "description": tenant.description,
        "queueId": tenant.queueId,
        "queueName": tenant.queueName,
        "queue": tenant.queue,
        "createTime": tenant.createTime,
        "updateTime": tenant.updateTime,
    }


def serialize_plugin_define(plugin_define: PluginDefineRecord) -> PluginDefineData:
    """Serialize one UI-plugin definition payload."""
    return {
        "id": require_resource_int(
            plugin_define.id,
            resource=ALERT_PLUGIN_RESOURCE,
            field_name="plugin_define.id",
        ),
        "pluginName": plugin_define.pluginName,
        "pluginType": plugin_define.pluginType,
        "pluginParams": plugin_define.pluginParams,
        "createTime": plugin_define.createTime,
        "updateTime": plugin_define.updateTime,
    }


def serialize_access_token(access_token: AccessTokenRecord) -> AccessTokenData:
    """Serialize one access-token payload for access-token commands."""
    return {
        "id": require_resource_int(
            access_token.id,
            resource=ACCESS_TOKEN_RESOURCE,
            field_name="access_token.id",
        ),
        "userId": require_resource_int(
            access_token.userId,
            resource=ACCESS_TOKEN_RESOURCE,
            field_name="access_token.userId",
        ),
        "token": access_token.token,
        "expireTime": access_token.expireTime,
        "createTime": access_token.createTime,
        "updateTime": access_token.updateTime,
        "userName": access_token.userName,
    }


def serialize_user_list_item(user: UserListRecord) -> UserListItemData:
    """Serialize one paged user list item for user commands."""
    return {
        "id": require_resource_int(
            user.id,
            resource=USER_RESOURCE,
            field_name="user.id",
        ),
        "userName": user.userName,
        "email": user.email,
        "phone": user.phone,
        "userType": enum_value(user.userType),
        "tenantId": user.tenantId,
        "tenantCode": user.tenantCode,
        "queueName": user.queueName,
        "queue": user.queue,
        "state": user.state,
        "createTime": user.createTime,
        "updateTime": user.updateTime,
    }


def serialize_user(user: UserRecord) -> UserData:
    """Serialize one full user payload for user commands."""
    data: UserData = {
        **serialize_user_list_item(user),
        "timeZone": user.timeZone,
    }
    return data


def serialize_monitor_server(server: MonitorServerRecord) -> MonitorServerData:
    """Serialize one monitor server payload for monitor commands."""
    return {
        "id": server.id,
        "host": server.host,
        "port": server.port,
        "serverDirectory": server.serverDirectory,
        "heartBeatInfo": server.heartBeatInfo,
        "createTime": server.createTime,
        "lastHeartbeatTime": server.lastHeartbeatTime,
    }


def serialize_monitor_database(
    database: MonitorDatabaseRecord,
) -> MonitorDatabaseData:
    """Serialize one monitor database metrics payload for monitor commands."""
    return {
        "dbType": enum_value(database.dbType),
        "state": enum_value(database.state),
        "maxConnections": database.maxConnections,
        "maxUsedConnections": database.maxUsedConnections,
        "threadsConnections": database.threadsConnections,
        "threadsRunningConnections": database.threadsRunningConnections,
        "date": database.date,
    }


def serialize_task_ref(task: TaskRecord) -> TaskListItem:
    """Serialize one task summary for list output."""
    return {
        "code": require_resource_int(
            task.code,
            resource=TASK_RESOURCE,
            field_name="task.code",
        ),
        "name": task.name,
        "version": task.version,
    }


def serialize_task(task: TaskPayloadRecord) -> TaskData:
    """Serialize one task payload for task and workflow commands."""
    return {
        "id": task.id,
        "code": task.code,
        "name": task.name,
        "version": task.version,
        "projectCode": task.projectCode,
        "description": task.description,
        "taskType": task.taskType,
        "taskParams": task.taskParams,
        "userName": task.userName,
        "projectName": task.projectName,
        "workerGroup": task.workerGroup,
        "failRetryTimes": task.failRetryTimes,
        "failRetryInterval": task.failRetryInterval,
        "timeout": task.timeout,
        "delayTime": task.delayTime,
        "resourceIds": task.resourceIds,
        "createTime": task.createTime,
        "updateTime": task.updateTime,
        "modifyBy": task.modifyBy,
        "taskGroupId": task.taskGroupId,
        "taskGroupPriority": task.taskGroupPriority,
        "environmentCode": task.environmentCode,
        "taskPriority": enum_value(task.taskPriority),
        "timeoutFlag": enum_value(task.timeoutFlag),
        "timeoutNotifyStrategy": enum_value(task.timeoutNotifyStrategy),
        "taskExecuteType": enum_value(task.taskExecuteType),
        "flag": enum_value(task.flag),
        "cpuQuota": task.cpuQuota,
        "memoryMax": task.memoryMax,
    }


def serialize_workflow_instance(
    workflow_instance: WorkflowInstanceRecord,
) -> WorkflowInstanceData:
    """Serialize one workflow-instance payload for runtime commands."""
    return {
        "id": require_resource_int(
            workflow_instance.id,
            resource=WORKFLOW_INSTANCE_RESOURCE,
            field_name="id",
        ),
        "workflowDefinitionCode": workflow_instance.workflowDefinitionCode,
        "workflowDefinitionVersion": workflow_instance.workflowDefinitionVersion,
        "projectCode": require_resource_int(
            workflow_instance.projectCode,
            resource=WORKFLOW_INSTANCE_RESOURCE,
            field_name="projectCode",
        ),
        "state": enum_value(workflow_instance.state),
        "recovery": enum_value(workflow_instance.recovery),
        "startTime": workflow_instance.startTime,
        "endTime": workflow_instance.endTime,
        "runTimes": workflow_instance.runTimes,
        "name": workflow_instance.name,
        "host": workflow_instance.host,
        "commandType": enum_value(workflow_instance.commandType),
        "taskDependType": enum_value(workflow_instance.taskDependType),
        "failureStrategy": enum_value(workflow_instance.failureStrategy),
        "warningType": enum_value(workflow_instance.warningType),
        "scheduleTime": workflow_instance.scheduleTime,
        "executorId": workflow_instance.executorId,
        "executorName": workflow_instance.executorName,
        "tenantCode": workflow_instance.tenantCode,
        "queue": workflow_instance.queue,
        "duration": workflow_instance.duration,
        "workflowInstancePriority": enum_value(
            workflow_instance.workflowInstancePriority
        ),
        "workerGroup": workflow_instance.workerGroup,
        "environmentCode": workflow_instance.environmentCode,
        "timeout": workflow_instance.timeout,
        "dryRun": workflow_instance.dryRun,
        "restartTime": workflow_instance.restartTime,
    }


def serialize_task_instance(task_instance: TaskInstanceRecord) -> TaskInstanceData:
    """Serialize one task-instance payload for runtime commands."""
    return {
        "id": require_resource_int(
            task_instance.id,
            resource=TASK_INSTANCE_RESOURCE,
            field_name="id",
        ),
        "name": task_instance.name,
        "taskType": task_instance.taskType,
        "workflowInstanceId": task_instance.workflowInstanceId,
        "workflowInstanceName": task_instance.workflowInstanceName,
        "projectCode": require_resource_int(
            task_instance.projectCode,
            resource=TASK_INSTANCE_RESOURCE,
            field_name="projectCode",
        ),
        "taskCode": task_instance.taskCode,
        "taskDefinitionVersion": task_instance.taskDefinitionVersion,
        "processDefinitionName": task_instance.processDefinitionName,
        "state": enum_value(task_instance.state),
        "firstSubmitTime": task_instance.firstSubmitTime,
        "submitTime": task_instance.submitTime,
        "startTime": task_instance.startTime,
        "endTime": task_instance.endTime,
        "host": task_instance.host,
        "logPath": task_instance.logPath,
        "retryTimes": task_instance.retryTimes,
        "duration": task_instance.duration,
        "executorName": task_instance.executorName,
        "workerGroup": task_instance.workerGroup,
        "environmentCode": task_instance.environmentCode,
        "delayTime": task_instance.delayTime,
        "taskParams": task_instance.taskParams,
        "dryRun": task_instance.dryRun,
        "taskGroupId": task_instance.taskGroupId,
        "taskExecuteType": enum_value(task_instance.taskExecuteType),
    }


def optional_text(value: str | None) -> str | None:
    """Normalize optional CLI text input to trimmed text or `None`."""
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def enum_value(value: StringEnumValue | str | None) -> str | None:
    """Normalize enum-like values to one stable string representation."""
    if value is None:
        return None
    enum_string = getattr(value, "value", None)
    if isinstance(enum_string, str):
        return enum_string
    return str(value)


def require_resource_int(
    value: int | None,
    *,
    resource: str,
    field_name: str,
) -> int:
    """Require one integer field from an upstream payload."""
    if value is None:
        message = (
            f"{resource_label(resource)} payload was missing required field "
            f"{field_name!r}"
        )
        raise ApiTransportError(
            message,
            details={"resource": resource, "field": field_name},
        )
    return value


def require_resource_text(
    value: str | None,
    *,
    resource: str,
    field_name: str,
) -> str:
    """Require one non-empty text field from an upstream payload."""
    normalized = optional_text(value)
    if normalized is None:
        message = (
            f"{resource_label(resource)} payload was missing required field "
            f"{field_name!r}"
        )
        raise ApiTransportError(
            message,
            details={"resource": resource, "field": field_name},
        )
    return normalized
