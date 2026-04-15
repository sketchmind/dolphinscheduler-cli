from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import IO, TYPE_CHECKING, TypeGuard, cast
from urllib.parse import urlsplit

from pydantic import TypeAdapter

from dsctl.client import (
    BinaryResponse,
    DolphinSchedulerClient,
    HttpFormValue,
    HttpQueryParams,
    HttpQueryValue,
    HttpRequestData,
    MultipartFiles,
)
from dsctl.errors import ApiResultError, ApiTransportError
from dsctl.generated.versions.ds_3_4_1 import DS341Client
from dsctl.generated.versions.ds_3_4_1.api.contracts.project import (
    ProjectCreateRequest,
    ProjectQueryRequest,
    ProjectUpdateRequest,
)
from dsctl.generated.versions.ds_3_4_1.api.contracts.schedule import (
    ScheduleCreateRequest,
    ScheduleUpdateRequest,
)
from dsctl.generated.versions.ds_3_4_1.api.contracts.workflow_instance import (
    workflow_instance_query_request as workflow_instance_contracts,
)
from dsctl.generated.versions.ds_3_4_1.api.enums.execute_type import ExecuteType
from dsctl.generated.versions.ds_3_4_1.api.operations.access_token import (
    CreateTokenParams,
    GenerateTokenParams,
    QueryAccessTokenListParams,
    UpdateTokenParams,
)
from dsctl.generated.versions.ds_3_4_1.api.operations.alert_group import (
    CreateAlertGroupParams,
    QueryAlertGroupByIdParams,
    UpdateAlertGroupByIdParams,
)
from dsctl.generated.versions.ds_3_4_1.api.operations.alert_group import (
    ListPagingParams as AlertGroupListPagingParams,
)
from dsctl.generated.versions.ds_3_4_1.api.operations.alert_plugin_instance import (
    CreateAlertPluginInstanceParams,
    TestSendAlertPluginInstanceParams,
    UpdateAlertPluginInstanceByIdParams,
)
from dsctl.generated.versions.ds_3_4_1.api.operations.alert_plugin_instance import (
    ListPagingParams as AlertPluginListPagingParams,
)
from dsctl.generated.versions.ds_3_4_1.api.operations.audit_log import (
    QueryAuditLogListPagingParams,
)
from dsctl.generated.versions.ds_3_4_1.api.operations.cluster import (
    CreateClusterParams,
    DeleteClusterParams,
    QueryClusterByCodeParams,
    QueryClusterListPagingParams,
    UpdateClusterParams,
)
from dsctl.generated.versions.ds_3_4_1.api.operations.data_source import (
    AuthedDatasourceParams,
    QueryDataSourceListPagingParams,
)
from dsctl.generated.versions.ds_3_4_1.api.operations.environment import (
    CreateEnvironmentParams,
    DeleteEnvironmentParams,
    QueryEnvironmentByCodeParams,
    QueryEnvironmentListPagingParams,
    UpdateEnvironmentParams,
)
from dsctl.generated.versions.ds_3_4_1.api.operations.executor import (
    ExecuteTaskParams,
    TriggerWorkflowDefinitionParams,
)
from dsctl.generated.versions.ds_3_4_1.api.operations.k8s_namespace import (
    CreateNamespaceParams,
    DelNamespaceByIdParams,
    QueryAuthorizedNamespaceParams,
    QueryNamespaceListPagingParams,
)
from dsctl.generated.versions.ds_3_4_1.api.operations.logger import (
    QueryLogGetLogDetailParams,
)
from dsctl.generated.versions.ds_3_4_1.api.operations.project_parameter import (
    CreateProjectParameterParams,
    DeleteProjectParametersByCodeParams,
    QueryProjectParameterListPagingParams,
    UpdateProjectParameterParams,
)
from dsctl.generated.versions.ds_3_4_1.api.operations.project_preference import (
    EnableProjectPreferenceParams,
    UpdateProjectPreferenceParams,
)
from dsctl.generated.versions.ds_3_4_1.api.operations.queue import (
    CreateQueueParams,
    QueryQueueListPagingParams,
    UpdateQueueParams,
)
from dsctl.generated.versions.ds_3_4_1.api.operations.resources import (
    CreateDirectoryParams,
    CreateFileFromContentParams,
    DeleteResourceParams,
    PagingResourceItemParams,
    QueryResourceBaseDirParams,
)
from dsctl.generated.versions.ds_3_4_1.api.operations.scheduler import (
    PreviewScheduleParams,
    QueryScheduleListPagingParams,
)
from dsctl.generated.versions.ds_3_4_1.api.operations.task_definition import (
    UpdateTaskWithUpstreamParams,
)
from dsctl.generated.versions.ds_3_4_1.api.operations.task_group import (
    CloseTaskGroupParams,
    CreateTaskGroupParams,
    ForceStartParams,
    ModifyPriorityParams,
    QueryAllTaskGroupParams,
    QueryTaskGroupByCodeParams,
    QueryTaskGroupQueuesParams,
    StartTaskGroupParams,
    UpdateTaskGroupParams,
)
from dsctl.generated.versions.ds_3_4_1.api.operations.tenant import (
    CreateTenantParams,
    QueryTenantListPagingParams,
    UpdateTenantParams,
)
from dsctl.generated.versions.ds_3_4_1.api.operations.ui_plugin import (
    QueryUiPluginsByTypeParams,
)
from dsctl.generated.versions.ds_3_4_1.api.operations.users import (
    CreateUserParams,
    DelUserByIdParams,
    GrantDataSourceParams,
    GrantNamespaceParams,
    GrantProjectByCodeParams,
    QueryUserListParams,
    RevokeProjectParams,
    UpdateUserParams,
)
from dsctl.generated.versions.ds_3_4_1.api.operations.worker_group import (
    QueryAllWorkerGroupsPagingParams,
    SaveWorkerGroupParams,
)
from dsctl.generated.versions.ds_3_4_1.api.operations.workflow_definition import (
    CreateWorkflowDefinitionParams,
    GetTaskListByWorkflowDefinitionCodeParams,
    ReleaseWorkflowDefinitionParams,
    UpdateWorkflowDefinitionParams,
)
from dsctl.generated.versions.ds_3_4_1.api.operations.workflow_instance import (
    QueryParentInstanceBySubIdParams,
    QuerySubWorkflowInstanceByTaskIdParams,
    UpdateWorkflowInstanceParams,
)
from dsctl.generated.versions.ds_3_4_1.api.operations.workflow_lineage import (
    QueryDependentTasksParams,
)
from dsctl.generated.versions.ds_3_4_1.api.views.resources import (
    FetchFileContentResponse,
)
from dsctl.generated.versions.ds_3_4_1.common.enums import (
    workflow_execution_status as workflow_execution_status_enums,
)
from dsctl.generated.versions.ds_3_4_1.common.enums.command_type import CommandType
from dsctl.generated.versions.ds_3_4_1.common.enums.complement_dependent_mode import (
    ComplementDependentMode,
)
from dsctl.generated.versions.ds_3_4_1.common.enums.execution_order import (
    ExecutionOrder,
)
from dsctl.generated.versions.ds_3_4_1.common.enums.failure_strategy import (
    FailureStrategy,
)
from dsctl.generated.versions.ds_3_4_1.common.enums.plugin_type import PluginType
from dsctl.generated.versions.ds_3_4_1.common.enums.priority import Priority
from dsctl.generated.versions.ds_3_4_1.common.enums.release_state import ReleaseState
from dsctl.generated.versions.ds_3_4_1.common.enums.run_mode import RunMode
from dsctl.generated.versions.ds_3_4_1.common.enums.task_depend_type import (
    TaskDependType,
)
from dsctl.generated.versions.ds_3_4_1.common.enums.warning_type import WarningType
from dsctl.generated.versions.ds_3_4_1.common.enums.workflow_execution_type_enum import (  # noqa: E501
    WorkflowExecutionTypeEnum,
)
from dsctl.generated.versions.ds_3_4_1.dao.entities.project_preference import (
    ProjectPreference as ProjectPreferenceEntity,
)
from dsctl.generated.versions.ds_3_4_1.plugin.task_api.enums import (
    task_execution_status as task_execution_status_enums,
)
from dsctl.generated.versions.ds_3_4_1.registry.api.enums.registry_node_type import (
    RegistryNodeType,
)
from dsctl.generated.versions.ds_3_4_1.spi.enums.resource_type import ResourceType
from dsctl.upstream.protocol import StringEnumValue, UpstreamAdapter, UserListRecord

if TYPE_CHECKING:
    from typing import Unpack

    import httpx

    from dsctl.config import ClusterProfile
    from dsctl.generated.versions.ds_3_4_1.api.contracts.audit_log import (
        AuditModelTypeDto,
        AuditOperationTypeDto,
    )
    from dsctl.generated.versions.ds_3_4_1.api.contracts.cluster_dto import (
        ClusterDto,
    )
    from dsctl.generated.versions.ds_3_4_1.api.contracts.environment_dto import (
        EnvironmentDto,
    )
    from dsctl.generated.versions.ds_3_4_1.api.contracts.fav_task_dto import FavTaskDto
    from dsctl.generated.versions.ds_3_4_1.api.contracts.page_info import (
        PageInfoAccessToken,
        PageInfoAlertGroup,
        PageInfoAlertPluginInstanceVO,
        PageInfoAuditDto,
        PageInfoClusterDto,
        PageInfoDataSource,
        PageInfoEnvironmentDto,
        PageInfoK8sNamespace,
        PageInfoProject,
        PageInfoProjectParameter,
        PageInfoQueue,
        PageInfoResourceItemVO,
        PageInfoScheduleVO,
        PageInfoTaskGroup,
        PageInfoTaskGroupQueue,
        PageInfoTenant,
        PageInfoUser,
        PageInfoWorkerGroupPageDetail,
        PageInfoWorkflowInstance,
    )
    from dsctl.generated.versions.ds_3_4_1.api.operations._base import (
        RequestKwargs,
        SessionLike,
    )
    from dsctl.generated.versions.ds_3_4_1.api.views.alert_plugin_instance import (
        AlertPluginInstanceVO,
    )
    from dsctl.generated.versions.ds_3_4_1.api.views.workflow_instance import (
        WorkflowInstanceParentInstanceView,
        WorkflowInstanceSubWorkflowInstanceView,
    )
    from dsctl.generated.versions.ds_3_4_1.common.model.server import Server
    from dsctl.generated.versions.ds_3_4_1.dao.entities import (
        dependent_simplify_definition as dependent_simplify_definition_models,
    )
    from dsctl.generated.versions.ds_3_4_1.dao.entities.access_token import (
        AccessToken,
    )
    from dsctl.generated.versions.ds_3_4_1.dao.entities.alert_group import AlertGroup
    from dsctl.generated.versions.ds_3_4_1.dao.entities.alert_plugin_instance import (
        AlertPluginInstance,
    )
    from dsctl.generated.versions.ds_3_4_1.dao.entities.dag_data import DagData
    from dsctl.generated.versions.ds_3_4_1.dao.entities.data_source import DataSource
    from dsctl.generated.versions.ds_3_4_1.dao.entities.dependent_lineage_task import (
        DependentLineageTask,
    )
    from dsctl.generated.versions.ds_3_4_1.dao.entities.k8s_namespace import (
        K8sNamespace,
    )
    from dsctl.generated.versions.ds_3_4_1.dao.entities.plugin_define import (
        PluginDefine,
    )
    from dsctl.generated.versions.ds_3_4_1.dao.entities.project import Project
    from dsctl.generated.versions.ds_3_4_1.dao.entities.project_parameter import (
        ProjectParameter,
    )
    from dsctl.generated.versions.ds_3_4_1.dao.entities.project_worker_group import (
        ProjectWorkerGroup,
    )
    from dsctl.generated.versions.ds_3_4_1.dao.entities.queue import Queue
    from dsctl.generated.versions.ds_3_4_1.dao.entities.response_task_log import (
        ResponseTaskLog,
    )
    from dsctl.generated.versions.ds_3_4_1.dao.entities.schedule import Schedule
    from dsctl.generated.versions.ds_3_4_1.dao.entities.task_definition import (
        TaskDefinition,
    )
    from dsctl.generated.versions.ds_3_4_1.dao.entities.task_group import TaskGroup
    from dsctl.generated.versions.ds_3_4_1.dao.entities.task_instance import (
        TaskInstance,
    )
    from dsctl.generated.versions.ds_3_4_1.dao.entities.task_instance_dependent_details import (  # noqa: E501
        TaskInstanceDependentDetailsAbstractTaskInstanceContext,
    )
    from dsctl.generated.versions.ds_3_4_1.dao.entities.user import User
    from dsctl.generated.versions.ds_3_4_1.dao.entities.work_flow_lineage import (
        WorkFlowLineage,
    )
    from dsctl.generated.versions.ds_3_4_1.dao.entities.worker_group import (
        WorkerGroup,
    )
    from dsctl.generated.versions.ds_3_4_1.dao.entities.workflow_definition import (
        WorkflowDefinition,
    )
    from dsctl.generated.versions.ds_3_4_1.dao.entities.workflow_instance import (
        WorkflowInstance,
    )
    from dsctl.generated.versions.ds_3_4_1.dao.plugin_api.monitor.database_metrics import (  # noqa: E501
        DatabaseMetrics,
    )
    from dsctl.support.json_types import JsonValue
    from dsctl.upstream.protocol import (
        AccessTokenOperations,
        AlertGroupOperations,
        AlertPluginOperations,
        AuditOperations,
        ClusterOperations,
        DataSourceOperations,
        EnvironmentOperations,
        MonitorOperations,
        NamespaceOperations,
        ProjectOperations,
        ProjectParameterOperations,
        ProjectPreferenceOperations,
        ProjectWorkerGroupOperations,
        QueueOperations,
        ResourceOperations,
        ScheduleOperations,
        TaskGroupOperations,
        TaskGroupPageRecord,
        TaskGroupQueuePageRecord,
        TaskGroupRecord,
        TaskInstanceOperations,
        TaskInstancePageRecord,
        TaskOperations,
        TaskTypeOperations,
        TenantOperations,
        TenantPageRecord,
        TenantRecord,
        UiPluginOperations,
        UpstreamSession,
        UserOperations,
        UserPageRecord,
        UserRecord,
        WorkerGroupOperations,
        WorkerGroupPageRecord,
        WorkerGroupRecord,
        WorkflowInstanceOperations,
        WorkflowLineageOperations,
        WorkflowOperations,
    )

    DependentSimplifyDefinition = (
        dependent_simplify_definition_models.DependentSimplifyDefinition
    )


class _GeneratedSessionAdapter:
    """Adapt the shared `DolphinSchedulerClient` to the generated session protocol."""

    def __init__(self, client: DolphinSchedulerClient, *, base_url: str) -> None:
        self._client = client
        self._base_url = base_url.rstrip("/")

    def request(
        self,
        method: str,
        url: str,
        headers: dict[str, str],
        **kwargs: Unpack[RequestKwargs],
    ) -> JsonValue:
        """Route a generated operation call through the shared HTTP client."""
        try:
            payload = self._client.request_payload(
                method,
                _relative_path(url, base_url=self._base_url),
                params=_query_params_or_none(kwargs.pop("params", None)),
                json_body=_json_value_or_none(kwargs.pop("json", None)),
                form_data=_request_data_or_none(kwargs.pop("data", None)),
                content=kwargs.pop("content", None),
                files=_multipart_files_or_none(kwargs.pop("files", None)),
                headers=headers,
            )
            _reject_unexpected_request_kwargs(kwargs)
        except TypeError as exc:
            message = f"Generated request shape did not match adapter contract: {exc}"
            raise ApiTransportError(
                message,
                details={
                    "method": method.upper(),
                    "url": url,
                },
            ) from exc
        return payload


@dataclass(frozen=True)
class DS341Adapter(UpstreamAdapter[DS341Client]):
    """Adapter for Apache DolphinScheduler 3.4.1."""

    ds_version: str = "3.4.1"
    version_slug: str = "ds_3_4_1"
    client_class: type[DS341Client] = DS341Client

    def create_client(
        self,
        profile: ClusterProfile,
        *,
        transport: httpx.BaseTransport | None = None,
        client: DolphinSchedulerClient | None = None,
    ) -> DS341Client:
        """Create a generated 3.4.1 client using the shared HTTP transport."""
        if client is not None and transport is not None:
            message = "create_client() accepts either transport or client, not both"
            raise ValueError(message)
        transport_client = client or DolphinSchedulerClient(
            profile,
            transport=transport,
        )
        generated_session = cast(
            "SessionLike",
            _GeneratedSessionAdapter(
                transport_client,
                base_url=profile.api_url,
            ),
        )
        return self.client_class(
            profile.api_url,
            profile.api_token,
            session=generated_session,
        )

    def bind(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> UpstreamSession:
        """Bind 3.4.1 operations to one profile and HTTP client."""
        client = self.create_client(profile, client=http_client)
        return _DS341Session(
            task_types=_DS341TaskTypeOperations(client=client),
            projects=_DS341ProjectOperations(
                client=client,
            ),
            project_parameters=_DS341ProjectParameterOperations(client=client),
            project_preferences=_DS341ProjectPreferenceOperations(
                client=client,
                http_client=http_client,
            ),
            project_worker_groups=_DS341ProjectWorkerGroupOperations(
                client=client,
                http_client=http_client,
            ),
            access_tokens=_DS341AccessTokenOperations(client=client),
            clusters=_DS341ClusterOperations(client=client),
            environments=_DS341EnvironmentOperations(client=client),
            datasources=_DS341DataSourceOperations(
                client=client,
            ),
            namespaces=_DS341NamespaceOperations(
                client=client,
            ),
            ui_plugins=_DS341UiPluginOperations(client=client),
            alert_plugins=_DS341AlertPluginOperations(client=client),
            alert_groups=_DS341AlertGroupOperations(client=client),
            queues=_DS341QueueOperations(
                client=client,
            ),
            worker_groups=_DS341WorkerGroupOperations(
                client=client,
            ),
            task_groups=_DS341TaskGroupOperations(
                client=client,
            ),
            tenants=_DS341TenantOperations(
                client=client,
            ),
            users=_DS341UserOperations(
                client=client,
            ),
            audits=_DS341AuditOperations(client=client),
            resources=_DS341ResourceOperations(
                client=client,
                http_client=http_client,
            ),
            monitor=_DS341MonitorOperations(client=client),
            workflows=_DS341WorkflowOperations(client=client),
            workflow_lineages=_DS341WorkflowLineageOperations(client=client),
            tasks=_DS341TaskOperations(client=client),
            schedules=_DS341ScheduleOperations(client=client),
            workflow_instances=_DS341WorkflowInstanceOperations(client=client),
            task_instances=_DS341TaskInstanceOperations(client=client),
        )


@dataclass(frozen=True)
class _DS341ProjectOperations:
    """Bound project operations backed by the generated v2 project client."""

    client: DS341Client

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> PageInfoProject:
        return self.client.project_v2.query_project_list_paging(
            ProjectQueryRequest(
                pageNo=page_no,
                pageSize=page_size,
                searchVal=search,
            )
        )

    def get(self, *, code: int) -> Project:
        return self.client.project_v2.query_project_by_code(code)

    def create(self, *, name: str, description: str | None = None) -> Project:
        return self.client.project_v2.create_project(
            ProjectCreateRequest(
                projectName=name,
                description=description,
            )
        )

    def update(
        self,
        *,
        code: int,
        name: str,
        description: str | None = None,
    ) -> Project:
        return self.client.project_v2.update_project(
            code,
            ProjectUpdateRequest(
                projectName=name,
                description=description,
            ),
        )

    def delete(self, *, code: int) -> bool:
        return self.client.project_v2.delete_project(code)


@dataclass(frozen=True)
class _DS341ProjectParameterOperations:
    """Bound project-parameter operations backed by generated clients."""

    client: DS341Client

    def list(
        self,
        *,
        project_code: int,
        page_no: int,
        page_size: int,
        search: str | None = None,
        data_type: str | None = None,
    ) -> PageInfoProjectParameter:
        return self.client.project_parameter.query_project_parameter_list_paging(
            project_code,
            QueryProjectParameterListPagingParams(
                searchVal=search,
                projectParameterDataType=data_type,
                pageNo=page_no,
                pageSize=page_size,
            ),
        )

    def get(self, *, project_code: int, code: int) -> ProjectParameter:
        return self.client.project_parameter.query_project_parameter_by_code(
            project_code,
            code,
        )

    def create(
        self,
        *,
        project_code: int,
        name: str,
        value: str,
        data_type: str,
    ) -> ProjectParameter:
        return self.client.project_parameter.create_project_parameter(
            project_code,
            CreateProjectParameterParams(
                projectParameterName=name,
                projectParameterValue=value,
                projectParameterDataType=data_type,
            ),
        )

    def update(
        self,
        *,
        project_code: int,
        code: int,
        name: str,
        value: str,
        data_type: str,
    ) -> ProjectParameter:
        return self.client.project_parameter.update_project_parameter(
            project_code,
            code,
            UpdateProjectParameterParams(
                projectParameterName=name,
                projectParameterValue=value,
                projectParameterDataType=data_type,
            ),
        )

    def delete(self, *, project_code: int, code: int) -> bool:
        self.client.project_parameter.delete_project_parameters_by_code(
            project_code,
            DeleteProjectParametersByCodeParams(code=code),
        )
        return True


@dataclass(frozen=True)
class _DS341ProjectWorkerGroupOperations:
    """Bound project worker-group operations backed by generated clients."""

    client: DS341Client
    http_client: DolphinSchedulerClient

    def list(self, *, project_code: int) -> Sequence[ProjectWorkerGroup]:
        return self.client.project_worker_group.query_assigned_worker_groups(
            project_code
        )

    def set(
        self,
        *,
        project_code: int,
        worker_groups: Sequence[str],
    ) -> None:
        # DS UI sends one comma-joined workerGroups form field and uses the empty
        # string to clear assignments. Reuse that wire contract so clear works.
        self.http_client.request_result(
            "POST",
            f"projects/{project_code}/worker-group",
            form_data={"workerGroups": ",".join(worker_groups)},
        )


@dataclass(frozen=True)
class _DS341ProjectPreferenceOperations:
    """Bound project-preference operations backed by generated clients."""

    client: DS341Client
    http_client: DolphinSchedulerClient

    def get(self, *, project_code: int) -> ProjectPreferenceEntity | None:
        payload = self.http_client.request_payload(
            "GET",
            f"projects/{project_code}/project-preference",
        )
        if payload is None:
            return None
        return TypeAdapter(ProjectPreferenceEntity).validate_python(payload)

    def update(
        self,
        *,
        project_code: int,
        preferences: str,
    ) -> ProjectPreferenceEntity:
        return self.client.project_preference.update_project_preference(
            project_code,
            UpdateProjectPreferenceParams(projectPreferences=preferences),
        )

    def set_state(self, *, project_code: int, state: int) -> None:
        self.client.project_preference.enable_project_preference(
            project_code,
            EnableProjectPreferenceParams(state=state),
        )


@dataclass(frozen=True)
class _DS341AccessTokenOperations:
    """Bound access-token operations backed by generated clients."""

    client: DS341Client

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> PageInfoAccessToken:
        return self.client.access_token.query_access_token_list(
            QueryAccessTokenListParams(
                pageNo=page_no,
                pageSize=page_size,
                searchVal=search,
            )
        )

    def create(
        self,
        *,
        user_id: int,
        expire_time: str,
        token: str | None = None,
    ) -> AccessToken:
        return self.client.access_token.create_token(
            CreateTokenParams(
                userId=user_id,
                expireTime=expire_time,
                token=token,
            )
        )

    def generate(
        self,
        *,
        user_id: int,
        expire_time: str,
    ) -> str:
        return self.client.access_token.generate_token(
            GenerateTokenParams(
                userId=user_id,
                expireTime=expire_time,
            )
        )

    def update(
        self,
        *,
        token_id: int,
        user_id: int,
        expire_time: str,
        token: str | None = None,
    ) -> AccessToken:
        return self.client.access_token.update_token(
            token_id,
            UpdateTokenParams(
                userId=user_id,
                expireTime=expire_time,
                token=token,
            ),
        )

    def delete(self, *, token_id: int) -> bool:
        self.client.access_token.del_access_token_by_id(token_id)
        return True


@dataclass(frozen=True)
class _DS341ClusterOperations:
    """Bound cluster operations backed by generated cluster clients."""

    client: DS341Client

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> PageInfoClusterDto:
        return self.client.cluster.query_cluster_list_paging(
            QueryClusterListPagingParams(
                searchVal=search,
                pageNo=page_no,
                pageSize=page_size,
            )
        )

    def get(self, *, code: int) -> ClusterDto:
        return self.client.cluster.query_cluster_by_code(
            QueryClusterByCodeParams(clusterCode=code)
        )

    def create(
        self,
        *,
        name: str,
        config: str,
        description: str | None = None,
    ) -> ClusterDto:
        cluster_code = self.client.cluster.create_cluster(
            CreateClusterParams(
                name=name,
                config=config,
                description=description,
            )
        )
        return self.get(code=cluster_code)

    def update(
        self,
        *,
        code: int,
        name: str,
        config: str,
        description: str | None = None,
    ) -> ClusterDto:
        self.client.cluster.update_cluster(
            UpdateClusterParams(
                code=code,
                name=name,
                config=config,
                description=description,
            )
        )
        return self.get(code=code)

    def delete(self, *, code: int) -> bool:
        return self.client.cluster.delete_cluster(DeleteClusterParams(clusterCode=code))


@dataclass(frozen=True)
class _DS341Session:
    """Bound upstream session for DS 3.4.1."""

    task_types: TaskTypeOperations
    projects: ProjectOperations
    project_parameters: ProjectParameterOperations
    project_preferences: ProjectPreferenceOperations
    project_worker_groups: ProjectWorkerGroupOperations
    access_tokens: AccessTokenOperations
    clusters: ClusterOperations
    environments: EnvironmentOperations
    datasources: DataSourceOperations
    namespaces: NamespaceOperations
    ui_plugins: UiPluginOperations
    alert_plugins: AlertPluginOperations
    alert_groups: AlertGroupOperations
    queues: QueueOperations
    worker_groups: WorkerGroupOperations
    task_groups: TaskGroupOperations
    tenants: TenantOperations
    users: UserOperations
    audits: AuditOperations
    resources: ResourceOperations
    monitor: MonitorOperations
    workflows: WorkflowOperations
    workflow_lineages: WorkflowLineageOperations
    tasks: TaskOperations
    schedules: ScheduleOperations
    workflow_instances: WorkflowInstanceOperations
    task_instances: TaskInstanceOperations


@dataclass(frozen=True)
class _DS341TaskTypeOperations:
    """Bound task-type discovery backed by generated favourite-task clients."""

    client: DS341Client

    def list(self) -> Sequence[FavTaskDto]:
        return self.client.fav_task.list_task_type()


@dataclass(frozen=True)
class _DS341EnvironmentOperations:
    """Bound environment operations backed by generated environment clients."""

    client: DS341Client

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> PageInfoEnvironmentDto:
        return self.client.environment.query_environment_list_paging(
            QueryEnvironmentListPagingParams(
                searchVal=search,
                pageNo=page_no,
                pageSize=page_size,
            )
        )

    def list_all(self) -> Sequence[EnvironmentDto]:
        return self.client.environment.query_all_environment_list()

    def get(self, *, code: int) -> EnvironmentDto:
        return self.client.environment.query_environment_by_code(
            QueryEnvironmentByCodeParams(environmentCode=code)
        )

    def create(
        self,
        *,
        name: str,
        config: str,
        description: str | None = None,
        worker_groups: Sequence[str] | None = None,
    ) -> EnvironmentDto:
        environment_code = self.client.environment.create_environment(
            CreateEnvironmentParams(
                name=name,
                config=config,
                description=description,
                workerGroups=_worker_groups_json(worker_groups),
            )
        )
        return self.get(code=environment_code)

    def update(
        self,
        *,
        code: int,
        name: str,
        config: str,
        description: str | None = None,
        worker_groups: Sequence[str],
    ) -> EnvironmentDto:
        self.client.environment.update_environment(
            UpdateEnvironmentParams(
                code=code,
                name=name,
                config=config,
                description=description,
                workerGroups=_worker_groups_json(worker_groups),
            )
        )
        return self.get(code=code)

    def delete(self, *, code: int) -> bool:
        self.client.environment.delete_environment(
            DeleteEnvironmentParams(environmentCode=code)
        )
        return True


@dataclass(frozen=True)
class _DS341DataSourceOperations:
    """Bound datasource operations backed by generated paging/detail clients."""

    client: DS341Client

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> PageInfoDataSource:
        return self.client.data_source.query_data_source_list_paging(
            QueryDataSourceListPagingParams(
                searchVal=search,
                pageNo=page_no,
                pageSize=page_size,
            )
        )

    def get(self, *, datasource_id: int) -> Mapping[str, object]:
        payload = self.client.data_source.query_data_source(datasource_id).model_dump(
            mode="json",
            by_alias=True,
            exclude_none=False,
        )
        return _json_mapping(payload, label="datasource detail payload")

    def authorized_for_user(self, *, user_id: int) -> Sequence[DataSource]:
        return self.client.data_source.authed_datasource(
            AuthedDatasourceParams(userId=user_id)
        )

    def create(self, *, payload_json: str) -> DataSource:
        return self.client.data_source.create_data_source(payload_json)

    def update(self, *, datasource_id: int, payload_json: str) -> DataSource:
        return self.client.data_source.update_data_source(
            datasource_id,
            payload_json,
        )

    def delete(self, *, datasource_id: int) -> bool:
        self.client.data_source.delete_data_source(datasource_id)
        return True

    def connection_test(self, *, datasource_id: int) -> bool:
        return self.client.data_source.connection_test(datasource_id)


@dataclass(frozen=True)
class _DS341NamespaceOperations:
    """Bound namespace operations backed by generated k8s namespace clients."""

    client: DS341Client

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> PageInfoK8sNamespace:
        return self.client.k8s_namespace.query_namespace_list_paging(
            QueryNamespaceListPagingParams(
                searchVal=search,
                pageNo=page_no,
                pageSize=page_size,
            )
        )

    def available(self) -> Sequence[K8sNamespace]:
        return self.client.k8s_namespace.query_available_namespace_list()

    def create(
        self,
        *,
        namespace: str,
        cluster_code: int,
    ) -> K8sNamespace:
        return self.client.k8s_namespace.create_namespace(
            CreateNamespaceParams(
                namespace=namespace,
                clusterCode=cluster_code,
            )
        )

    def delete(self, *, namespace_id: int) -> bool:
        self.client.k8s_namespace.del_namespace_by_id(
            DelNamespaceByIdParams(id=namespace_id)
        )
        return True

    def authorized_for_user(self, *, user_id: int) -> Sequence[K8sNamespace]:
        return self.client.k8s_namespace.query_authorized_namespace(
            QueryAuthorizedNamespaceParams(userId=user_id)
        )


@dataclass(frozen=True)
class _DS341UiPluginOperations:
    """Bound UI-plugin discovery backed by generated UI-plugin clients."""

    client: DS341Client

    def list(self, *, plugin_type: str) -> Sequence[PluginDefine]:
        return self.client.ui_plugin.query_ui_plugins_by_type(
            QueryUiPluginsByTypeParams(pluginType=_plugin_type_member(plugin_type))
        )

    def get(self, *, plugin_id: int) -> PluginDefine:
        return self.client.ui_plugin.query_ui_plugin_detail_by_id(plugin_id)


@dataclass(frozen=True)
class _DS341AlertPluginOperations:
    """Bound alert-plugin instance operations backed by generated clients."""

    client: DS341Client

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> PageInfoAlertPluginInstanceVO:
        return self.client.alert_plugin_instance.list_paging(
            AlertPluginListPagingParams(
                searchVal=search,
                pageNo=page_no,
                pageSize=page_size,
            )
        )

    def list_all(self) -> Sequence[AlertPluginInstanceVO]:
        api = self.client.alert_plugin_instance
        payload = api.get_alert_plugin_instance_get_alert_plugin_instances_list()
        if payload is None:
            return ()
        return payload

    def create(
        self,
        *,
        plugin_define_id: int,
        instance_name: str,
        plugin_instance_params: str,
    ) -> AlertPluginInstance:
        return self.client.alert_plugin_instance.create_alert_plugin_instance(
            CreateAlertPluginInstanceParams(
                pluginDefineId=plugin_define_id,
                instanceName=instance_name,
                pluginInstanceParams=plugin_instance_params,
            )
        )

    def update(
        self,
        *,
        alert_plugin_id: int,
        instance_name: str,
        plugin_instance_params: str,
    ) -> AlertPluginInstance:
        return self.client.alert_plugin_instance.update_alert_plugin_instance_by_id(
            alert_plugin_id,
            UpdateAlertPluginInstanceByIdParams(
                instanceName=instance_name,
                pluginInstanceParams=plugin_instance_params,
            ),
        )

    def delete(self, *, alert_plugin_id: int) -> bool:
        return self.client.alert_plugin_instance.delete_alert_plugin_instance(
            alert_plugin_id
        )

    def test_send(
        self,
        *,
        plugin_define_id: int,
        plugin_instance_params: str,
    ) -> bool:
        return self.client.alert_plugin_instance.test_send_alert_plugin_instance(
            TestSendAlertPluginInstanceParams(
                pluginDefineId=plugin_define_id,
                pluginInstanceParams=plugin_instance_params,
            )
        )


@dataclass(frozen=True)
class _DS341AlertGroupOperations:
    """Bound alert-group operations backed by generated alert-group clients."""

    client: DS341Client

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> PageInfoAlertGroup:
        return self.client.alert_group.list_paging(
            AlertGroupListPagingParams(
                searchVal=search,
                pageNo=page_no,
                pageSize=page_size,
            )
        )

    def get(self, *, alert_group_id: int) -> AlertGroup:
        return self.client.alert_group.query_alert_group_by_id(
            QueryAlertGroupByIdParams(id=alert_group_id)
        )

    def create(
        self,
        *,
        group_name: str,
        description: str | None,
        alert_instance_ids: str,
    ) -> AlertGroup:
        return self.client.alert_group.create_alert_group(
            CreateAlertGroupParams(
                groupName=group_name,
                description=description,
                alertInstanceIds=alert_instance_ids,
            )
        )

    def update(
        self,
        *,
        alert_group_id: int,
        group_name: str,
        description: str | None,
        alert_instance_ids: str,
    ) -> AlertGroup:
        return self.client.alert_group.update_alert_group_by_id(
            alert_group_id,
            UpdateAlertGroupByIdParams(
                groupName=group_name,
                description=description,
                alertInstanceIds=alert_instance_ids,
            ),
        )

    def delete(self, *, alert_group_id: int) -> bool:
        return self.client.alert_group.delete_alert_group_by_id(alert_group_id)


@dataclass(frozen=True)
class _DS341AuditOperations:
    """Bound audit-log discovery backed by generated audit clients."""

    client: DS341Client

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        model_types: Sequence[str] | None = None,
        operation_types: Sequence[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        user_name: str | None = None,
        model_name: str | None = None,
    ) -> PageInfoAuditDto:
        return self.client.audit_log.query_audit_log_list_paging(
            QueryAuditLogListPagingParams(
                pageNo=page_no,
                pageSize=page_size,
                modelTypes=_comma_join(model_types),
                operationTypes=_comma_join(operation_types),
                startDate=start_date,
                endDate=end_date,
                userName=user_name,
                modelName=model_name,
            )
        )

    def list_model_types(self) -> Sequence[AuditModelTypeDto]:
        return self.client.audit_log.query_audit_model_type_list()

    def list_operation_types(self) -> Sequence[AuditOperationTypeDto]:
        return self.client.audit_log.query_audit_operation_type_list()


@dataclass(frozen=True)
class _DS341QueueOperations:
    """Bound queue operations backed by generated paging and CRUD clients."""

    client: DS341Client

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> PageInfoQueue:
        return self.client.queue.query_queue_list_paging(
            QueryQueueListPagingParams(
                searchVal=search,
                pageNo=page_no,
                pageSize=page_size,
            )
        )

    def list_all(self) -> Sequence[Queue]:
        return self.client.queue.query_list()

    def get(self, *, queue_id: int) -> Queue:
        return _queue_by_id(self.list_all(), queue_id=queue_id)

    def create(self, *, queue: str, queue_name: str) -> Queue:
        self.client.queue.create_queue(
            CreateQueueParams(queue=queue, queueName=queue_name)
        )
        return _queue_by_name(self.list_all(), queue_name=queue_name)

    def update(self, *, queue_id: int, queue: str, queue_name: str) -> Queue:
        self.client.queue.update_queue(
            queue_id,
            UpdateQueueParams(queue=queue, queueName=queue_name),
        )
        return self.get(queue_id=queue_id)

    def delete(self, *, queue_id: int) -> bool:
        return self.client.queue.delete_queue_by_id(queue_id)


@dataclass(frozen=True)
class _DS341WorkerGroupOperations:
    """Bound worker-group operations backed by generated worker-group clients."""

    client: DS341Client

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> PageInfoWorkerGroupPageDetail:
        return self.client.worker_group.query_all_worker_groups_paging(
            QueryAllWorkerGroupsPagingParams(
                searchVal=search,
                pageNo=page_no,
                pageSize=page_size,
            )
        )

    def list_all(self) -> Sequence[WorkerGroupRecord]:
        return _collect_worker_group_pages(
            lambda page_no, page_size: self.list(
                page_no=page_no,
                page_size=page_size,
            )
        )

    def get(self, *, worker_group_id: int) -> WorkerGroupRecord:
        for worker_group in self.list_all():
            if worker_group.id == worker_group_id:
                return worker_group
        raise ApiResultError(
            result_code=1402001,
            result_message=f"worker group {worker_group_id} not exists",
        )

    def create(
        self,
        *,
        name: str,
        addr_list: str,
        description: str | None = None,
    ) -> WorkerGroup:
        return self.client.worker_group.save_worker_group(
            SaveWorkerGroupParams(
                name=name,
                addrList=addr_list,
                description=description,
            )
        )

    def update(
        self,
        *,
        worker_group_id: int,
        name: str,
        addr_list: str,
        description: str | None = None,
    ) -> WorkerGroup:
        return self.client.worker_group.save_worker_group(
            SaveWorkerGroupParams(
                id=worker_group_id,
                name=name,
                addrList=addr_list,
                description=description,
            )
        )

    def delete(self, *, worker_group_id: int) -> bool:
        self.client.worker_group.delete_worker_group_by_id(worker_group_id)
        return True


@dataclass(frozen=True)
class _DS341TaskGroupOperations:
    """Bound task-group operations backed by generated task-group clients."""

    client: DS341Client

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
        status: int | None = None,
    ) -> PageInfoTaskGroup:
        return self.client.task_group.query_all_task_group(
            QueryAllTaskGroupParams(
                name=search,
                status=status,
                pageNo=page_no,
                pageSize=page_size,
            )
        )

    def list_by_project(
        self,
        *,
        project_code: int,
        page_no: int,
        page_size: int,
    ) -> PageInfoTaskGroup:
        return self.client.task_group.query_task_group_by_code(
            QueryTaskGroupByCodeParams(
                projectCode=project_code,
                pageNo=page_no,
                pageSize=page_size,
            )
        )

    def list_all(self) -> Sequence[TaskGroupRecord]:
        return _collect_task_group_pages(
            lambda page_no, page_size: self.list(
                page_no=page_no,
                page_size=page_size,
            )
        )

    def get(self, *, task_group_id: int) -> TaskGroupRecord:
        return _task_group_by_id(self.list_all(), task_group_id=task_group_id)

    def create(
        self,
        *,
        project_code: int,
        name: str,
        description: str,
        group_size: int,
    ) -> TaskGroup:
        return self.client.task_group.create_task_group(
            CreateTaskGroupParams(
                name=name,
                projectCode=project_code,
                description=description,
                groupSize=group_size,
            )
        )

    def update(
        self,
        *,
        task_group_id: int,
        name: str,
        description: str,
        group_size: int,
    ) -> TaskGroup:
        return self.client.task_group.update_task_group(
            UpdateTaskGroupParams(
                id=task_group_id,
                name=name,
                description=description,
                groupSize=group_size,
            )
        )

    def close(self, *, task_group_id: int) -> None:
        self.client.task_group.close_task_group(CloseTaskGroupParams(id=task_group_id))

    def start(self, *, task_group_id: int) -> None:
        self.client.task_group.start_task_group(StartTaskGroupParams(id=task_group_id))

    def list_queues(
        self,
        *,
        group_id: int,
        page_no: int,
        page_size: int,
        task_instance_name: str | None = None,
        workflow_instance_name: str | None = None,
        status: int | None = None,
    ) -> PageInfoTaskGroupQueue:
        return self.client.task_group.query_task_group_queues(
            QueryTaskGroupQueuesParams(
                groupId=group_id,
                taskInstanceName=task_instance_name,
                workflowInstanceName=workflow_instance_name,
                status=status,
                pageNo=page_no,
                pageSize=page_size,
            )
        )

    def force_start(self, *, queue_id: int) -> None:
        self.client.task_group.force_start(ForceStartParams(queueId=queue_id))

    def set_queue_priority(self, *, queue_id: int, priority: int) -> None:
        self.client.task_group.modify_priority(
            ModifyPriorityParams(queueId=queue_id, priority=priority)
        )


@dataclass(frozen=True)
class _DS341TenantOperations:
    """Bound tenant operations backed by generated tenant clients."""

    client: DS341Client

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> PageInfoTenant:
        return self.client.tenant.query_tenant_list_paging(
            QueryTenantListPagingParams(
                searchVal=search,
                pageNo=page_no,
                pageSize=page_size,
            )
        )

    def list_all(self) -> Sequence[TenantRecord]:
        return _collect_tenant_pages(
            lambda page_no, page_size: self.list(
                page_no=page_no,
                page_size=page_size,
            )
        )

    def get(self, *, tenant_id: int) -> TenantRecord:
        return _tenant_by_id(self.list_all(), tenant_id=tenant_id)

    def create(
        self,
        *,
        tenant_code: str,
        queue_id: int,
        description: str | None = None,
    ) -> TenantRecord:
        created = self.client.tenant.create_tenant(
            CreateTenantParams(
                tenantCode=tenant_code,
                queueId=queue_id,
                description=description,
            )
        )
        if created.id is not None:
            return self.get(tenant_id=created.id)
        return _tenant_by_code(
            _collect_tenant_pages(
                lambda page_no, page_size: self.list(
                    page_no=page_no,
                    page_size=page_size,
                    search=tenant_code,
                )
            ),
            tenant_code=tenant_code,
        )

    def update(
        self,
        *,
        tenant_id: int,
        tenant_code: str,
        queue_id: int,
        description: str | None = None,
    ) -> TenantRecord:
        self.client.tenant.update_tenant(
            tenant_id,
            UpdateTenantParams(
                tenantCode=tenant_code,
                queueId=queue_id,
                description=description,
            ),
        )
        return self.get(tenant_id=tenant_id)

    def delete(self, *, tenant_id: int) -> bool:
        return self.client.tenant.delete_tenant_by_id(tenant_id)


@dataclass(frozen=True)
class _DS341UserSnapshot:
    """Merged user snapshot that combines raw and paging-only user views."""

    id_value: int | None
    user_name_value: str | None
    email_value: str | None
    phone_value: str | None
    user_type_value: StringEnumValue | None
    tenant_id_value: int
    tenant_code_value: str | None
    queue_name_value: str | None
    queue_value: str | None
    state_value: int
    time_zone_value: str | None
    stored_queue_value: str | None
    create_time_value: str | None
    update_time_value: str | None

    @property
    def id(self) -> int | None:
        return self.id_value

    @property
    def userName(self) -> str | None:  # noqa: N802
        return self.user_name_value

    @property
    def email(self) -> str | None:
        return self.email_value

    @property
    def phone(self) -> str | None:
        return self.phone_value

    @property
    def userType(self) -> StringEnumValue | None:  # noqa: N802
        return self.user_type_value

    @property
    def tenantId(self) -> int:  # noqa: N802
        return self.tenant_id_value

    @property
    def tenantCode(self) -> str | None:  # noqa: N802
        return self.tenant_code_value

    @property
    def queueName(self) -> str | None:  # noqa: N802
        return self.queue_name_value

    @property
    def queue(self) -> str | None:
        return self.queue_value

    @property
    def state(self) -> int:
        return self.state_value

    @property
    def timeZone(self) -> str | None:  # noqa: N802
        return self.time_zone_value

    @property
    def storedQueue(self) -> str | None:  # noqa: N802
        return self.stored_queue_value

    @property
    def createTime(self) -> str | None:  # noqa: N802
        return self.create_time_value

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        return self.update_time_value


@dataclass(frozen=True)
class _DS341UserOperations:
    """Bound user operations backed by generated user clients."""

    client: DS341Client

    def current(self) -> UserRecord:
        return _merge_user_snapshot(
            raw_user=self.client.users.get_user_info(),
            summary_user=None,
        )

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> PageInfoUser:
        return self.client.users.query_user_list(
            QueryUserListParams(
                searchVal=search,
                pageNo=page_no,
                pageSize=page_size,
            )
        )

    def list_all(self) -> Sequence[UserRecord]:
        return _collect_user_snapshots(self.client)

    def get(self, *, user_id: int) -> UserRecord:
        return _user_detail_by_id(self.client, user_id=user_id)

    def create(
        self,
        *,
        user_name: str,
        password: str,
        email: str,
        tenant_id: int,
        phone: str | None = None,
        queue: str | None = None,
        state: int,
    ) -> UserRecord:
        created = self.client.users.create_user(
            CreateUserParams(
                userName=user_name,
                userPassword=password,
                email=email,
                tenantId=tenant_id,
                phone=phone,
                queue=queue,
                state=state,
            )
        )
        if created is not None and created.id is not None:
            return self.get(user_id=created.id)
        return _user_detail_by_name(self.client, user_name=user_name)

    def update(
        self,
        *,
        user_id: int,
        user_name: str,
        password: str,
        email: str,
        tenant_id: int,
        phone: str | None,
        queue: str,
        state: int,
        time_zone: str | None = None,
    ) -> UserRecord:
        updated = self.client.users.update_user(
            UpdateUserParams(
                id=user_id,
                userName=user_name,
                userPassword=password,
                email=email,
                tenantId=tenant_id,
                phone=phone,
                queue=queue,
                state=state,
                timeZone=time_zone,
            )
        )
        if updated.id is not None:
            return self.get(user_id=updated.id)
        return self.get(user_id=user_id)

    def delete(self, *, user_id: int) -> bool:
        self.client.users.del_user_by_id(
            DelUserByIdParams(id=user_id),
        )
        return True

    def grant_project_by_code(self, *, user_id: int, project_code: int) -> bool:
        self.client.users.grant_project_by_code(
            GrantProjectByCodeParams(
                userId=user_id,
                projectCode=project_code,
            )
        )
        return True

    def revoke_project(self, *, user_id: int, project_code: int) -> bool:
        self.client.users.revoke_project(
            RevokeProjectParams(
                userId=user_id,
                projectCode=project_code,
            )
        )
        return True

    def grant_datasources(
        self,
        *,
        user_id: int,
        datasource_ids: Sequence[int],
    ) -> bool:
        self.client.users.grant_data_source(
            GrantDataSourceParams(
                userId=user_id,
                datasourceIds=",".join(
                    str(datasource_id) for datasource_id in datasource_ids
                ),
            )
        )
        return True

    def grant_namespaces(
        self,
        *,
        user_id: int,
        namespace_ids: Sequence[int],
    ) -> bool:
        self.client.users.grant_namespace(
            GrantNamespaceParams(
                userId=user_id,
                namespaceIds=",".join(
                    str(namespace_id) for namespace_id in namespace_ids
                ),
            )
        )
        return True


@dataclass(frozen=True)
class _DS341ResourceOperations:
    """Bound resource operations backed by generated and raw transport paths."""

    client: DS341Client
    http_client: DolphinSchedulerClient

    def base_dir(self) -> str:
        return self.client.resources.query_resource_base_dir(
            QueryResourceBaseDirParams(type=ResourceType.FILE)
        )

    def list(
        self,
        *,
        directory: str,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> PageInfoResourceItemVO:
        return self.client.resources.paging_resource_item_request(
            PagingResourceItemParams(
                fullName=directory,
                type=ResourceType.FILE,
                searchVal=_resource_search_value(search),
                pageNo=page_no,
                pageSize=page_size,
            )
        )

    def view(
        self,
        *,
        full_name: str,
        skip_line_num: int,
        limit: int,
    ) -> FetchFileContentResponse:
        downloaded = self.download(full_name=full_name)
        return FetchFileContentResponse(
            content=_resource_view_content(
                downloaded,
                skip_line_num=skip_line_num,
                limit=limit,
            )
        )

    def upload(
        self,
        *,
        current_dir: str,
        name: str,
        file: IO[bytes],
    ) -> None:
        form_data: dict[str, HttpFormValue] = {
            "type": ResourceType.FILE.value,
            "name": name,
            "currentDir": current_dir,
        }
        files: MultipartFiles = {"file": (name, file)}
        self.http_client.request_result(
            "POST",
            "resources",
            form_data=form_data,
            files=files,
            retryable=False,
        )

    def create_from_content(
        self,
        *,
        current_dir: str,
        file_name: str,
        suffix: str,
        content: str,
    ) -> None:
        self.client.resources.create_file_from_content(
            CreateFileFromContentParams(
                type=ResourceType.FILE,
                fileName=file_name,
                suffix=suffix,
                content=content,
                currentDir=current_dir,
            )
        )

    def create_directory(
        self,
        *,
        current_dir: str,
        name: str,
    ) -> None:
        self.client.resources.create_directory(
            CreateDirectoryParams(
                type=ResourceType.FILE,
                name=name,
                currentDir=current_dir,
            )
        )

    def delete(self, *, full_name: str) -> bool:
        self.client.resources.delete_resource(DeleteResourceParams(fullName=full_name))
        return True

    def download(self, *, full_name: str) -> BinaryResponse:
        return self.http_client.get_binary(
            "resources/download",
            params={"fullName": full_name},
        )


@dataclass(frozen=True)
class _DS341MonitorOperations:
    """Bound monitor operations backed by generated monitor clients."""

    client: DS341Client

    def list_databases(self) -> Sequence[DatabaseMetrics]:
        return self.client.monitor.query_database_state()

    def list_servers(
        self,
        *,
        node_type: str,
    ) -> Sequence[Server]:
        return self.client.monitor.list_server(_registry_node_type(node_type))


@dataclass(frozen=True)
class _DS341WorkflowOperations:
    """Bound workflow operations backed by generated workflow clients."""

    client: DS341Client

    def list(self, *, project_code: int) -> list[DependentSimplifyDefinition]:
        return self.client.workflow_definition.get_workflow_list_by_project_code(
            project_code
        )

    def get(self, *, code: int) -> WorkflowDefinition:
        return self.client.workflow_v2.get_workflow(code)

    def describe(self, *, project_code: int, code: int) -> DagData:
        return self.client.workflow_definition.query_workflow_definition_by_code(
            project_code,
            code,
        )

    def create(
        self,
        *,
        project_code: int,
        name: str,
        description: str | None,
        global_params: str,
        locations: str,
        timeout: int,
        task_relation_json: str,
        task_definition_json: str,
        execution_type: str | None,
    ) -> None:
        self.client.workflow_definition.create_workflow_definition(
            project_code,
            CreateWorkflowDefinitionParams(
                name=name,
                description=description,
                globalParams=global_params,
                locations=locations,
                timeout=timeout,
                taskRelationJson=task_relation_json,
                taskDefinitionJson=task_definition_json,
                executionType=(
                    None
                    if execution_type is None
                    else WorkflowExecutionTypeEnum[execution_type]
                ),
            ),
        )

    def update(
        self,
        *,
        project_code: int,
        workflow_code: int,
        name: str,
        description: str | None,
        global_params: str,
        locations: str,
        timeout: int,
        task_relation_json: str,
        task_definition_json: str,
        execution_type: str | None,
        release_state: str | None,
    ) -> None:
        self.client.workflow_definition.update_workflow_definition(
            project_code,
            workflow_code,
            UpdateWorkflowDefinitionParams(
                name=name,
                description=description,
                globalParams=global_params,
                locations=locations,
                timeout=timeout,
                taskRelationJson=task_relation_json,
                taskDefinitionJson=task_definition_json,
                executionType=(
                    None
                    if execution_type is None
                    else WorkflowExecutionTypeEnum[execution_type]
                ),
                releaseState=(
                    None if release_state is None else ReleaseState[release_state]
                ),
            ),
        )

    def delete(self, *, project_code: int, workflow_code: int) -> None:
        self.client.workflow_definition.delete_workflow_definition_by_code(
            project_code,
            workflow_code,
        )

    def online(self, *, project_code: int, workflow_code: int) -> None:
        self.client.workflow_definition.release_workflow_definition(
            project_code,
            workflow_code,
            ReleaseWorkflowDefinitionParams(releaseState=ReleaseState.ONLINE),
        )

    def offline(self, *, project_code: int, workflow_code: int) -> None:
        self.client.workflow_definition.release_workflow_definition(
            project_code,
            workflow_code,
            ReleaseWorkflowDefinitionParams(releaseState=ReleaseState.OFFLINE),
        )

    def run(
        self,
        *,
        project_code: int,
        workflow_code: int,
        worker_group: str,
        tenant_code: str,
        start_node_list: Sequence[int] | None = None,
        task_scope: str | None = None,
        failure_strategy: str = "CONTINUE",
        warning_type: str = "NONE",
        workflow_instance_priority: str = "MEDIUM",
        warning_group_id: int | None = None,
        environment_code: int | None = None,
        start_params: str | None = None,
        dry_run: bool = False,
    ) -> Sequence[int]:
        return self.client.executor.trigger_workflow_definition(
            project_code,
            TriggerWorkflowDefinitionParams(
                workflowDefinitionCode=workflow_code,
                scheduleTime=_start_process_schedule_time(),
                failureStrategy=FailureStrategy[failure_strategy],
                execType=CommandType.START_PROCESS,
                startNodeList=_start_node_list(start_node_list),
                taskDependType=(
                    TaskDependType.TASK_POST
                    if task_scope is None
                    else _task_depend_type(task_scope)
                ),
                warningType=WarningType[warning_type],
                warningGroupId=warning_group_id,
                workflowInstancePriority=Priority[workflow_instance_priority],
                workerGroup=worker_group,
                tenantCode=tenant_code,
                environmentCode=environment_code,
                startParams=start_params,
                dryRun=1 if dry_run else 0,
            ),
        )

    def backfill(
        self,
        *,
        project_code: int,
        workflow_code: int,
        schedule_time: str,
        run_mode: str,
        expected_parallelism_number: int,
        complement_dependent_mode: str,
        all_level_dependent: bool,
        execution_order: str,
        worker_group: str,
        tenant_code: str,
        start_node_list: Sequence[int] | None = None,
        task_scope: str | None = None,
        failure_strategy: str = "CONTINUE",
        warning_type: str = "NONE",
        workflow_instance_priority: str = "MEDIUM",
        warning_group_id: int | None = None,
        environment_code: int | None = None,
        start_params: str | None = None,
        dry_run: bool = False,
    ) -> Sequence[int]:
        return self.client.executor.trigger_workflow_definition(
            project_code,
            TriggerWorkflowDefinitionParams(
                workflowDefinitionCode=workflow_code,
                scheduleTime=schedule_time,
                failureStrategy=FailureStrategy[failure_strategy],
                execType=CommandType.COMPLEMENT_DATA,
                startNodeList=_start_node_list(start_node_list),
                taskDependType=(
                    TaskDependType.TASK_POST
                    if task_scope is None
                    else _task_depend_type(task_scope)
                ),
                warningType=WarningType[warning_type],
                warningGroupId=warning_group_id,
                runMode=RunMode[run_mode],
                workflowInstancePriority=Priority[workflow_instance_priority],
                workerGroup=worker_group,
                tenantCode=tenant_code,
                environmentCode=environment_code,
                startParams=start_params,
                expectedParallelismNumber=expected_parallelism_number,
                dryRun=1 if dry_run else 0,
                complementDependentMode=ComplementDependentMode[
                    complement_dependent_mode
                ],
                allLevelDependent=all_level_dependent,
                executionOrder=ExecutionOrder[execution_order],
            ),
        )


@dataclass(frozen=True)
class _DS341WorkflowLineageOperations:
    """Bound workflow-lineage operations backed by generated lineage clients."""

    client: DS341Client

    def list(self, *, project_code: int) -> WorkFlowLineage | None:
        return self.client.workflow_lineage.query_work_flow_lineage(project_code)

    def get(
        self,
        *,
        project_code: int,
        workflow_code: int,
    ) -> WorkFlowLineage | None:
        return self.client.workflow_lineage.query_work_flow_lineage_by_code(
            project_code,
            workflow_code,
        )

    def query_dependent_tasks(
        self,
        *,
        project_code: int,
        workflow_code: int,
        task_code: int | None = None,
    ) -> Sequence[DependentLineageTask]:
        return self.client.workflow_lineage.query_dependent_tasks(
            project_code,
            QueryDependentTasksParams(
                workFlowCode=workflow_code,
                taskCode=task_code,
            ),
        )


@dataclass(frozen=True)
class _DS341TaskOperations:
    """Bound task operations backed by generated task clients."""

    client: DS341Client

    def list(
        self,
        *,
        project_code: int,
        workflow_code: int,
    ) -> list[DependentSimplifyDefinition]:
        return (
            self.client.workflow_definition.get_task_list_by_workflow_definition_code(
                project_code,
                GetTaskListByWorkflowDefinitionCodeParams(
                    workflowDefinitionCode=workflow_code,
                ),
            )
        )

    def get(self, *, code: int) -> TaskDefinition:
        return self.client.task_definition_v2.get_task_definition(code)

    def update(
        self,
        *,
        project_code: int,
        code: int,
        task_definition_json: str,
        upstream_codes: Sequence[int],
    ) -> None:
        self.client.task_definition.update_task_with_upstream(
            project_code,
            code,
            UpdateTaskWithUpstreamParams(
                taskDefinitionJsonObj=task_definition_json,
                upstreamCodes=",".join(str(item) for item in upstream_codes) or None,
            ),
        )


@dataclass(frozen=True)
class _DS341WorkflowInstanceOperations:
    """Bound workflow-instance operations backed by generated runtime clients."""

    client: DS341Client

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        project_name: str | None = None,
        workflow_name: str | None = None,
        state: str | None = None,
    ) -> PageInfoWorkflowInstance:
        return self.client.workflow_instance_v2.query_workflow_instance_list_paging(
            workflow_instance_contracts.WorkflowInstanceQueryRequest(
                pageNo=page_no,
                pageSize=page_size,
                projectName=project_name,
                workflowName=workflow_name,
                state=_workflow_execution_state_code(state),
            )
        )

    def get(self, *, workflow_instance_id: int) -> WorkflowInstance:
        return self.client.workflow_instance_v2.query_workflow_instance_by_id(
            workflow_instance_id
        )

    def update(
        self,
        *,
        project_code: int,
        workflow_instance_id: int,
        task_relation_json: str,
        task_definition_json: str,
        sync_define: bool,
        global_params: str | None = None,
        locations: str | None = None,
        timeout: int | None = None,
        schedule_time: str | None = None,
    ) -> WorkflowDefinition:
        return self.client.workflow_instance.update_workflow_instance(
            project_code,
            workflow_instance_id,
            UpdateWorkflowInstanceParams(
                taskRelationJson=task_relation_json,
                taskDefinitionJson=task_definition_json,
                scheduleTime=schedule_time,
                syncDefine=sync_define,
                globalParams=global_params,
                locations=locations,
                timeout=timeout,
            ),
        )

    def parent_instance_by_sub_workflow(
        self,
        *,
        project_code: int,
        sub_workflow_instance_id: int,
    ) -> WorkflowInstanceParentInstanceView:
        return self.client.workflow_instance.query_parent_instance_by_sub_id(
            project_code,
            QueryParentInstanceBySubIdParams(subId=sub_workflow_instance_id),
        )

    def sub_workflow_instance_by_task(
        self,
        *,
        project_code: int,
        task_instance_id: int,
    ) -> WorkflowInstanceSubWorkflowInstanceView:
        return self.client.workflow_instance.query_sub_workflow_instance_by_task_id(
            project_code,
            QuerySubWorkflowInstanceByTaskIdParams(taskId=task_instance_id),
        )

    def stop(self, *, workflow_instance_id: int) -> None:
        self.client.workflow_instance_v2.execute(
            workflow_instance_id,
            ExecuteType.STOP,
        )

    def rerun(self, *, workflow_instance_id: int) -> None:
        self.client.workflow_instance_v2.execute(
            workflow_instance_id,
            ExecuteType.REPEAT_RUNNING,
        )

    def recover_failed(self, *, workflow_instance_id: int) -> None:
        self.client.workflow_instance_v2.execute(
            workflow_instance_id,
            ExecuteType.START_FAILURE_TASK_PROCESS,
        )

    def execute_task(
        self,
        *,
        project_code: int,
        workflow_instance_id: int,
        task_code: int,
        scope: str,
    ) -> None:
        self.client.executor.execute_task(
            project_code,
            ExecuteTaskParams(
                workflowInstanceId=workflow_instance_id,
                startNodeList=str(task_code),
                taskDependType=_task_depend_type(scope),
            ),
        )


@dataclass(frozen=True)
class _DS341TaskInstanceOperations:
    """Bound task-instance operations backed by generated runtime clients."""

    client: DS341Client

    def list(
        self,
        *,
        workflow_instance_id: int,
        project_code: int,
        page_no: int,
        page_size: int,
        search: str | None = None,
        state: str | None = None,
    ) -> TaskInstancePageRecord:
        task_list = (
            self.client.workflow_instance.query_task_list_by_workflow_instance_id(
                project_code,
                workflow_instance_id,
            )
        )
        items = _filtered_task_instance_items(
            task_list.taskList or [],
            search=search,
            state=state,
        )
        return _TaskInstanceListPage.from_items(
            items,
            page_no=page_no,
            page_size=page_size,
        )

    def get(
        self,
        *,
        project_code: int,
        task_instance_id: int,
    ) -> TaskInstance:
        return self.client.task_instance_v2.query_task_instance_by_code(
            project_code,
            task_instance_id,
        )

    def log_chunk(
        self,
        *,
        task_instance_id: int,
        skip_line_num: int,
        limit: int,
    ) -> ResponseTaskLog:
        return self.client.logger.query_log_get_log_detail(
            QueryLogGetLogDetailParams(
                taskInstanceId=task_instance_id,
                skipLineNum=skip_line_num,
                limit=limit,
            )
        )

    def force_success(
        self,
        *,
        project_code: int,
        task_instance_id: int,
    ) -> None:
        self.client.task_instance.force_task_success(project_code, task_instance_id)

    def savepoint(
        self,
        *,
        project_code: int,
        task_instance_id: int,
    ) -> None:
        self.client.task_instance.task_save_point(project_code, task_instance_id)

    def stop(
        self,
        *,
        project_code: int,
        task_instance_id: int,
    ) -> None:
        self.client.task_instance.stop_task(project_code, task_instance_id)


@dataclass(frozen=True)
class _DS341ScheduleOperations:
    """Bound schedule operations backed by generated schedule clients."""

    client: DS341Client

    def list(
        self,
        *,
        project_code: int,
        page_no: int,
        page_size: int,
        workflow_code: int | None = None,
        search: str | None = None,
    ) -> PageInfoScheduleVO:
        return self.client.scheduler.query_schedule_list_paging(
            project_code,
            QueryScheduleListPagingParams(
                workflowDefinitionCode=workflow_code,
                searchVal=search,
                pageNo=page_no,
                pageSize=page_size,
            ),
        )

    def get(self, *, schedule_id: int) -> Schedule:
        return self.client.schedule_v2.get_schedule(schedule_id)

    def preview(
        self,
        *,
        project_code: int,
        crontab: str,
        start_time: str,
        end_time: str,
        timezone_id: str,
    ) -> Sequence[str]:
        return self.client.scheduler.preview_schedule(
            project_code,
            PreviewScheduleParams(
                schedule=_schedule_preview_expression(
                    crontab=crontab,
                    end_time=end_time,
                    start_time=start_time,
                    timezone_id=timezone_id,
                ),
            ),
        )

    def create(
        self,
        *,
        workflow_code: int,
        crontab: str,
        start_time: str,
        end_time: str,
        timezone_id: str,
        failure_strategy: str | None = None,
        warning_type: str | None = None,
        warning_group_id: int = 0,
        workflow_instance_priority: str | None = None,
        worker_group: str | None = None,
        tenant_code: str | None = None,
        environment_code: int = 0,
    ) -> Schedule:
        return self.client.schedule_v2.create_schedule(
            ScheduleCreateRequest(
                workflowDefinitionCode=workflow_code,
                crontab=crontab,
                startTime=start_time,
                endTime=end_time,
                timezoneId=timezone_id,
                failureStrategy=failure_strategy,
                warningType=warning_type,
                warningGroupId=warning_group_id,
                workflowInstancePriority=workflow_instance_priority,
                workerGroup=worker_group,
                tenantCode=tenant_code,
                environmentCode=environment_code,
            )
        )

    def update(
        self,
        *,
        schedule_id: int,
        crontab: str,
        start_time: str,
        end_time: str,
        timezone_id: str,
        failure_strategy: str | None = None,
        warning_type: str | None = None,
        warning_group_id: int = 0,
        workflow_instance_priority: str | None = None,
        worker_group: str | None = None,
        environment_code: int = 0,
    ) -> Schedule:
        return self.client.schedule_v2.update_schedule(
            schedule_id,
            ScheduleUpdateRequest(
                crontab=crontab,
                startTime=start_time,
                endTime=end_time,
                timezoneId=timezone_id,
                failureStrategy=failure_strategy,
                warningType=warning_type,
                warningGroupId=warning_group_id,
                workflowInstancePriority=workflow_instance_priority,
                workerGroup=worker_group,
                environmentCode=environment_code,
            ),
        )

    def delete(self, *, schedule_id: int) -> bool:
        self.client.schedule_v2.delete_schedule(schedule_id)
        return True

    def online(self, *, schedule_id: int) -> Schedule:
        project_code = _schedule_project_code(self.client, schedule_id=schedule_id)
        self.client.scheduler.publish_schedule_online(project_code, schedule_id)
        return self.get(schedule_id=schedule_id)

    def offline(self, *, schedule_id: int) -> Schedule:
        project_code = _schedule_project_code(self.client, schedule_id=schedule_id)
        self.client.scheduler.offline_schedule(project_code, schedule_id)
        return self.get(schedule_id=schedule_id)


def _schedule_project_code(client: DS341Client, *, schedule_id: int) -> int:
    schedule = client.schedule_v2.get_schedule(schedule_id)
    workflow = client.workflow_v2.get_workflow(schedule.workflowDefinitionCode)
    return workflow.projectCode


def _start_process_schedule_time() -> str:
    timestamp = datetime.now(tz=UTC).strftime("%Y-%m-%d %H:%M:%S")
    return json.dumps(
        {
            "complementStartDate": timestamp,
            "complementEndDate": timestamp,
        },
        ensure_ascii=True,
        separators=(",", ":"),
    )


def _workflow_execution_state_code(value: str | None) -> int | None:
    if value is None:
        return None
    return workflow_execution_status_enums.WorkflowExecutionStatus[value].code


def _task_execution_state(
    value: str | None,
) -> task_execution_status_enums.TaskExecutionStatus | None:
    if value is None:
        return None
    return task_execution_status_enums.TaskExecutionStatus[value]


@dataclass(frozen=True)
class _TaskInstanceListPage:
    """Client-side page wrapper for workflow-instance task lists."""

    total_list_value: list[TaskInstanceDependentDetailsAbstractTaskInstanceContext]
    total: int
    total_page_value: int
    page_size_value: int
    current_page_value: int

    @classmethod
    def from_items(
        cls,
        items: Sequence[TaskInstanceDependentDetailsAbstractTaskInstanceContext],
        *,
        page_no: int,
        page_size: int,
    ) -> _TaskInstanceListPage:
        total = len(items)
        total_pages = 0 if total == 0 else ((total - 1) // page_size) + 1
        start = (page_no - 1) * page_size
        stop = start + page_size
        return cls(
            total_list_value=list(items[start:stop]),
            total=total,
            total_page_value=total_pages,
            page_size_value=page_size,
            current_page_value=page_no,
        )

    @property
    def totalList(  # noqa: N802
        self,
    ) -> list[TaskInstanceDependentDetailsAbstractTaskInstanceContext] | None:
        return self.total_list_value

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        return self.total_page_value

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        return self.page_size_value

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        return self.current_page_value

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        return self.current_page_value


def _filtered_task_instance_items(
    items: Sequence[TaskInstanceDependentDetailsAbstractTaskInstanceContext],
    *,
    search: str | None,
    state: str | None,
) -> list[TaskInstanceDependentDetailsAbstractTaskInstanceContext]:
    filtered: list[TaskInstanceDependentDetailsAbstractTaskInstanceContext] = []
    normalized_search = None if search is None else search.lower()
    for item in items:
        if normalized_search is not None:
            name = item.name
            if name is None or normalized_search not in name.lower():
                continue
        if state is not None and _enum_member_value(item.state) != state:
            continue
        filtered.append(item)
    return filtered


def _enum_member_value(value: object) -> str | None:
    member_value = getattr(value, "value", None)
    if isinstance(member_value, str):
        return member_value
    return None


def _task_depend_type(scope: str) -> TaskDependType:
    normalized_scope = scope.strip().lower()
    if normalized_scope == "self":
        return TaskDependType.TASK_ONLY
    if normalized_scope == "pre":
        return TaskDependType.TASK_PRE
    if normalized_scope == "post":
        return TaskDependType.TASK_POST
    message = f"Unsupported task execution scope: {scope!r}"
    raise ValueError(message)


def _start_node_list(start_node_list: Sequence[int] | None) -> str | None:
    if start_node_list is None:
        return None
    normalized = [str(code) for code in start_node_list]
    if not normalized:
        return None
    return ",".join(normalized)


def _schedule_preview_expression(
    *,
    crontab: str,
    start_time: str,
    end_time: str,
    timezone_id: str,
) -> str:
    return json.dumps(
        {
            "crontab": crontab,
            "endTime": end_time,
            "startTime": start_time,
            "timezoneId": timezone_id,
        },
        ensure_ascii=True,
        separators=(",", ":"),
    )


def _relative_path(url: str, *, base_url: str) -> str:
    if url == base_url:
        return ""
    prefix = f"{base_url}/"
    if url.startswith(prefix):
        return url.removeprefix(prefix)
    return urlsplit(url).path.lstrip("/")


def _query_params_or_none(value: object) -> HttpQueryParams | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        message = f"Expected query param mapping, got {type(value)!r}"
        raise TypeError(message)
    cleaned: dict[str, HttpQueryValue] = {}
    for key, item in value.items():
        if not isinstance(key, str):
            message = f"Expected string query param key, got {type(key)!r}"
            raise TypeError(message)
        if _is_http_query_scalar(item):
            cleaned[key] = item
            continue
        if isinstance(item, Sequence) and not isinstance(
            item,
            (str, bytes, bytearray),
        ):
            sequence = list(item)
            if all(_is_http_query_scalar(entry) for entry in sequence):
                cleaned[key] = sequence
                continue
        message = f"Unsupported query param value type for {key!r}: {type(item)!r}"
        raise TypeError(message)
    return cleaned


def _request_data_or_none(
    value: object,
) -> HttpRequestData | None:
    if value is None:
        return None
    if isinstance(value, Mapping):
        cleaned: dict[str, HttpFormValue] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                message = f"Expected string form field key, got {type(key)!r}"
                raise TypeError(message)
            if _is_http_form_scalar(item):
                cleaned[key] = item
                continue
            if isinstance(item, Sequence) and not isinstance(
                item,
                (str, bytes, bytearray),
            ):
                sequence = list(item)
                if all(_is_http_form_scalar(entry) for entry in sequence):
                    cleaned[key] = sequence
                    continue
            message = f"Unsupported form field value type for {key!r}: {type(item)!r}"
            raise TypeError(message)
        return cleaned
    if isinstance(value, str | bytes | bytearray):
        return value
    message = f"Expected request data payload, got {type(value)!r}"
    raise TypeError(message)


def _multipart_files_or_none(value: object) -> MultipartFiles | None:
    if value is None:
        return None
    if isinstance(value, Mapping):
        return cast("MultipartFiles", value)
    message = f"Expected multipart file mapping, got {type(value)!r}"
    raise TypeError(message)


def _reject_unexpected_request_kwargs(kwargs: RequestKwargs) -> None:
    if not kwargs:
        return
    keys = ", ".join(sorted(kwargs))
    message = f"Unsupported generated request arguments: {keys}"
    raise TypeError(message)


def _plugin_type_member(plugin_type: str) -> PluginType:
    try:
        return PluginType[plugin_type]
    except KeyError as error:
        message = f"Unsupported plugin type {plugin_type!r}"
        raise ValueError(message) from error


def _comma_join(values: Sequence[str] | None) -> str | None:
    if not values:
        return None
    return ",".join(values)


def _json_value_or_none(value: object) -> JsonValue | None:
    if value is None:
        return None
    if _is_json_value(value):
        return value
    message = f"Expected JSON payload, got {type(value)!r}"
    raise TypeError(message)


def _worker_groups_json(worker_groups: Sequence[str] | None) -> str | None:
    if worker_groups is None:
        return None
    return json.dumps(list(worker_groups), ensure_ascii=False, separators=(",", ":"))


def _resource_search_value(search: str | None) -> str:
    """DS 3.4.1 resource paging crashes when searchVal is omitted."""
    return "" if search is None else search


def _resource_view_content(
    response: BinaryResponse,
    *,
    skip_line_num: int,
    limit: int,
) -> str:
    """Work around the DS 3.4.1 `/resources/view` limit bug."""
    text = _decode_resource_text(response)
    lines = text.splitlines()
    window = lines[skip_line_num : skip_line_num + limit]
    return "\n".join(window)


def _decode_resource_text(response: BinaryResponse) -> str:
    encoding = _content_type_charset(response.content_type) or "utf-8"
    return response.content.decode(encoding, errors="replace")


def _content_type_charset(content_type: str | None) -> str | None:
    if content_type is None:
        return None
    for part in content_type.split(";"):
        name, separator, value = part.strip().partition("=")
        if separator and name.lower() == "charset":
            normalized = value.strip()
            return normalized or None
    return None


def _collect_worker_group_pages(
    fetch_page: Callable[[int, int], WorkerGroupPageRecord],
    *,
    page_size: int = 100,
    max_pages: int = 100,
) -> list[WorkerGroupRecord]:
    first_page = fetch_page(1, page_size)
    first_page_no = _worker_group_page_number(first_page, fallback=1)
    last_page = _worker_group_total_pages(first_page, page_size=page_size) or 0
    page_count = (last_page or first_page_no) - first_page_no + 1
    if page_count > max_pages:
        message = "Worker-group auto-resolution exceeded the paging safety limit"
        raise ApiTransportError(
            message,
            details={
                "page_no": first_page_no,
                "total_pages": last_page or first_page_no,
                "max_pages": max_pages,
            },
        )

    seen: set[str] = set()
    collected: list[WorkerGroupRecord] = []
    _append_unique_worker_groups(
        collected,
        seen,
        first_page.totalList or [],
    )
    for current_page_no in range(first_page_no + 1, (last_page or first_page_no) + 1):
        page = fetch_page(current_page_no, page_size)
        _append_unique_worker_groups(
            collected,
            seen,
            page.totalList or [],
        )
    return collected


def _collect_task_group_pages(
    fetch_page: Callable[[int, int], TaskGroupPageRecord],
    *,
    page_size: int = 100,
    max_pages: int = 100,
) -> list[TaskGroupRecord]:
    first_page = fetch_page(1, page_size)
    first_page_no = _page_number(first_page, fallback=1)
    last_page = _total_pages(first_page, page_size=page_size) or 0
    page_count = (last_page or first_page_no) - first_page_no + 1
    if page_count > max_pages:
        message = "Task-group auto-resolution exceeded the paging safety limit"
        raise ApiTransportError(
            message,
            details={
                "page_no": first_page_no,
                "total_pages": last_page or first_page_no,
                "max_pages": max_pages,
            },
        )

    collected = list(first_page.totalList or [])
    for current_page_no in range(first_page_no + 1, (last_page or first_page_no) + 1):
        page = fetch_page(current_page_no, page_size)
        collected.extend(page.totalList or [])
    return collected


def _collect_tenant_pages(
    fetch_page: Callable[[int, int], TenantPageRecord],
    *,
    page_size: int = 100,
    max_pages: int = 100,
) -> list[TenantRecord]:
    first_page = fetch_page(1, page_size)
    first_page_no = _page_number(first_page, fallback=1)
    last_page = _total_pages(first_page, page_size=page_size) or 0
    page_count = (last_page or first_page_no) - first_page_no + 1
    if page_count > max_pages:
        message = "Tenant auto-resolution exceeded the paging safety limit"
        raise ApiTransportError(
            message,
            details={
                "page_no": first_page_no,
                "total_pages": last_page or first_page_no,
                "max_pages": max_pages,
            },
        )

    collected = list(first_page.totalList or [])
    for current_page_no in range(first_page_no + 1, (last_page or first_page_no) + 1):
        page = fetch_page(current_page_no, page_size)
        collected.extend(page.totalList or [])
    return collected


def _collect_user_pages(
    fetch_page: Callable[[int, int], UserPageRecord],
    *,
    page_size: int = 100,
    max_pages: int = 100,
) -> list[UserListRecord]:
    first_page = fetch_page(1, page_size)
    first_page_no = _page_number(first_page, fallback=1)
    last_page = _total_pages(first_page, page_size=page_size) or 0
    page_count = (last_page or first_page_no) - first_page_no + 1
    if page_count > max_pages:
        message = "User auto-resolution exceeded the paging safety limit"
        raise ApiTransportError(
            message,
            details={
                "page_no": first_page_no,
                "total_pages": last_page or first_page_no,
                "max_pages": max_pages,
            },
        )

    collected = list(first_page.totalList or [])
    for current_page_no in range(first_page_no + 1, (last_page or first_page_no) + 1):
        page = fetch_page(current_page_no, page_size)
        collected.extend(page.totalList or [])
    return collected


def _collect_user_snapshots(client: DS341Client) -> list[_DS341UserSnapshot]:
    raw_users = _dedupe_users(
        [*client.users.list_user(), *client.users.list_all()],
    )
    raw_users_by_id = {user.id: user for user in raw_users if user.id is not None}
    summary_users = _collect_user_pages(
        lambda page_no, page_size: client.users.query_user_list(
            QueryUserListParams(
                pageNo=page_no,
                pageSize=page_size,
            )
        )
    )
    summary_users_by_id = {
        user.id: user for user in summary_users if user.id is not None
    }

    ordered_ids: list[int] = []
    seen_ids: set[int] = set()
    for user in raw_users:
        if user.id is None or user.id in seen_ids:
            continue
        seen_ids.add(user.id)
        ordered_ids.append(user.id)
    for summary_user in summary_users:
        if summary_user.id is None or summary_user.id in seen_ids:
            continue
        seen_ids.add(summary_user.id)
        ordered_ids.append(summary_user.id)

    return [
        _merge_user_snapshot(
            raw_user=raw_users_by_id.get(user_id),
            summary_user=summary_users_by_id.get(user_id),
        )
        for user_id in ordered_ids
    ]


def _user_detail_by_id(client: DS341Client, *, user_id: int) -> _DS341UserSnapshot:
    raw_users = _dedupe_users(
        [*client.users.list_user(), *client.users.list_all()],
    )
    raw_user = _raw_user_by_id_or_none(raw_users, user_id=user_id)
    searched_users = _collect_user_pages(
        lambda page_no, page_size: client.users.query_user_list(
            QueryUserListParams(
                searchVal=None if raw_user is None else raw_user.userName,
                pageNo=page_no,
                pageSize=page_size,
            )
        )
    )
    summary_user = _paged_user_by_id_or_none(searched_users, user_id=user_id)
    if summary_user is None and raw_user is not None and raw_user.userName is not None:
        summary_user = _paged_user_by_id_or_none(
            _collect_user_pages(
                lambda page_no, page_size: client.users.query_user_list(
                    QueryUserListParams(
                        pageNo=page_no,
                        pageSize=page_size,
                    )
                )
            ),
            user_id=user_id,
        )
    if raw_user is None and summary_user is None:
        raise ApiResultError(
            result_code=10010,
            result_message=f"user {user_id} not exists",
        )
    return _merge_user_snapshot(
        raw_user=raw_user,
        summary_user=summary_user,
    )


def _user_detail_by_name(
    client: DS341Client,
    *,
    user_name: str,
) -> _DS341UserSnapshot:
    raw_user = _raw_user_by_name_or_none(
        _dedupe_users([*client.users.list_user(), *client.users.list_all()]),
        user_name=user_name,
    )
    summary_user = _paged_user_by_name_or_none(
        _collect_user_pages(
            lambda page_no, page_size: client.users.query_user_list(
                QueryUserListParams(
                    searchVal=user_name,
                    pageNo=page_no,
                    pageSize=page_size,
                )
            )
        ),
        user_name=user_name,
    )
    if raw_user is None and summary_user is None:
        raise ApiResultError(
            result_code=10010,
            result_message=f"user {user_name} not exists",
        )
    return _merge_user_snapshot(
        raw_user=raw_user,
        summary_user=summary_user,
    )


def _merge_user_snapshot(
    *,
    raw_user: User | None,
    summary_user: UserListRecord | None,
) -> _DS341UserSnapshot:
    effective_queue = None
    if summary_user is not None:
        effective_queue = summary_user.queue
    elif raw_user is not None:
        effective_queue = (
            raw_user.queueName
            if raw_user.queue == "" and raw_user.queueName is not None
            else raw_user.queue
        )

    return _DS341UserSnapshot(
        id_value=(
            None
            if raw_user is None and summary_user is None
            else (
                summary_user.id
                if summary_user is not None and summary_user.id is not None
                else None
                if raw_user is None
                else raw_user.id
            )
        ),
        user_name_value=(
            summary_user.userName
            if summary_user is not None and summary_user.userName is not None
            else None
            if raw_user is None
            else raw_user.userName
        ),
        email_value=(
            summary_user.email
            if summary_user is not None and summary_user.email is not None
            else None
            if raw_user is None
            else raw_user.email
        ),
        phone_value=(
            summary_user.phone
            if summary_user is not None and summary_user.phone is not None
            else None
            if raw_user is None
            else raw_user.phone
        ),
        user_type_value=(
            summary_user.userType
            if summary_user is not None and summary_user.userType is not None
            else None
            if raw_user is None
            else raw_user.userType
        ),
        tenant_id_value=(
            summary_user.tenantId
            if summary_user is not None
            else 0
            if raw_user is None
            else raw_user.tenantId
        ),
        tenant_code_value=(
            summary_user.tenantCode
            if summary_user is not None and summary_user.tenantCode is not None
            else None
            if raw_user is None
            else raw_user.tenantCode
        ),
        queue_name_value=(
            summary_user.queueName
            if summary_user is not None and summary_user.queueName is not None
            else None
            if raw_user is None
            else raw_user.queueName
        ),
        queue_value=effective_queue,
        state_value=(
            summary_user.state
            if summary_user is not None
            else 0
            if raw_user is None
            else raw_user.state
        ),
        time_zone_value=None if raw_user is None else raw_user.timeZone,
        stored_queue_value=None if raw_user is None else raw_user.queue,
        create_time_value=(
            summary_user.createTime
            if summary_user is not None and summary_user.createTime is not None
            else None
            if raw_user is None
            else raw_user.createTime
        ),
        update_time_value=(
            summary_user.updateTime
            if summary_user is not None and summary_user.updateTime is not None
            else None
            if raw_user is None
            else raw_user.updateTime
        ),
    )


def _dedupe_users(users: Sequence[User]) -> list[User]:
    seen_ids: set[int] = set()
    deduped: list[User] = []
    for user in users:
        if user.id is None or user.id in seen_ids:
            continue
        seen_ids.add(user.id)
        deduped.append(user)
    return deduped


def _raw_user_by_id_or_none(
    users: Sequence[User],
    *,
    user_id: int,
) -> User | None:
    for user in users:
        if user.id == user_id:
            return user
    return None


def _raw_user_by_name_or_none(
    users: Sequence[User],
    *,
    user_name: str,
) -> User | None:
    for user in users:
        if user.userName == user_name:
            return user
    return None


def _paged_user_by_id_or_none(
    users: Sequence[UserListRecord],
    *,
    user_id: int,
) -> UserListRecord | None:
    for user in users:
        if user.id == user_id:
            return user
    return None


def _paged_user_by_name_or_none(
    users: Sequence[UserListRecord],
    *,
    user_name: str,
) -> UserListRecord | None:
    for user in users:
        if user.userName == user_name:
            return user
    return None


def _append_unique_worker_groups(
    collected: list[WorkerGroupRecord],
    seen: set[str],
    worker_groups: Sequence[WorkerGroupRecord],
) -> None:
    for worker_group in worker_groups:
        identity = _worker_group_identity(worker_group)
        if identity in seen:
            continue
        seen.add(identity)
        collected.append(worker_group)


def _worker_group_identity(worker_group: WorkerGroupRecord) -> str:
    if worker_group.id is not None:
        return f"id:{worker_group.id}"
    if worker_group.addrList is not None:
        return f"addr:{worker_group.addrList}"
    if worker_group.name is not None:
        return f"name:{worker_group.name}"
    message = "Worker-group payload was missing both id and fallback identity fields"
    raise ApiTransportError(message)


def _worker_group_page_number(
    page: WorkerGroupPageRecord,
    *,
    fallback: int,
) -> int:
    return _page_number(page, fallback=fallback)


def _worker_group_total_pages(
    page: WorkerGroupPageRecord,
    *,
    page_size: int,
) -> int | None:
    return _total_pages(page, page_size=page_size)


def _page_number(
    page: (
        WorkerGroupPageRecord
        | TaskGroupPageRecord
        | TaskGroupQueuePageRecord
        | TenantPageRecord
        | UserPageRecord
    ),
    *,
    fallback: int,
) -> int:
    current_page = page.currentPage
    if isinstance(current_page, int):
        return current_page
    page_no = page.pageNo
    if isinstance(page_no, int):
        return page_no
    return fallback


def _total_pages(
    page: (
        WorkerGroupPageRecord
        | TaskGroupPageRecord
        | TaskGroupQueuePageRecord
        | TenantPageRecord
        | UserPageRecord
    ),
    *,
    page_size: int,
) -> int | None:
    total_pages = page.totalPage
    if isinstance(total_pages, int):
        return total_pages
    total = page.total
    if not isinstance(total, int) or page_size < 1:
        return None
    quotient, remainder = divmod(total, page_size)
    return quotient if remainder == 0 else quotient + 1


def _registry_node_type(node_type: str) -> RegistryNodeType:
    return RegistryNodeType[node_type]


def _queue_by_id(queues: Sequence[Queue], *, queue_id: int) -> Queue:
    for queue in queues:
        if queue.id == queue_id:
            return queue
    message = f"Queue id {queue_id} was not returned by the upstream list endpoint"
    raise ApiTransportError(message, details={"queue_id": queue_id})


def _queue_by_name(queues: Sequence[Queue], *, queue_name: str) -> Queue:
    for queue in queues:
        if queue.queueName == queue_name:
            return queue
    message = f"Queue {queue_name!r} was not returned by the upstream list endpoint"
    raise ApiTransportError(message, details={"queue_name": queue_name})


def _task_group_by_id(
    task_groups: Sequence[TaskGroupRecord],
    *,
    task_group_id: int,
) -> TaskGroupRecord:
    for task_group in task_groups:
        if task_group.id == task_group_id:
            return task_group
    message = (
        f"Task-group id {task_group_id} was not returned by the upstream list endpoint"
    )
    raise ApiTransportError(message, details={"task_group_id": task_group_id})


def _tenant_by_id(tenants: Sequence[TenantRecord], *, tenant_id: int) -> TenantRecord:
    for tenant in tenants:
        if tenant.id == tenant_id:
            return tenant
    message = f"Tenant id {tenant_id} was not returned by the upstream list endpoint"
    raise ApiTransportError(message, details={"tenant_id": tenant_id})


def _tenant_by_code(
    tenants: Sequence[TenantRecord],
    *,
    tenant_code: str,
) -> TenantRecord:
    for tenant in tenants:
        if tenant.tenantCode == tenant_code:
            return tenant
    message = f"Tenant {tenant_code!r} was not returned by the upstream list endpoint"
    raise ApiTransportError(message, details={"tenant_code": tenant_code})


def _json_mapping(value: object, *, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        message = f"{label} must be a JSON object"
        raise ApiTransportError(message)
    if not all(isinstance(key, str) for key in value):
        message = f"{label} must use string keys"
        raise ApiTransportError(message)
    return cast("Mapping[str, object]", value)


def _is_json_value(value: object) -> TypeGuard[JsonValue]:
    if value is None or isinstance(value, str | int | float | bool):
        return True
    if isinstance(value, Mapping):
        return all(
            isinstance(key, str) and _is_json_value(item) for key, item in value.items()
        )
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return all(_is_json_value(item) for item in value)
    return False


def _is_http_query_scalar(value: object) -> bool:
    return value is None or isinstance(value, str | int | float | bool)


def _is_http_form_scalar(value: object) -> bool:
    return value is None or isinstance(value, str | bytes | int | float | bool)


__all__ = ["DS341Adapter"]
