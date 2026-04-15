from __future__ import annotations

from typing import IO, TYPE_CHECKING, Protocol, TypeVar

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    import httpx

    from dsctl.client import BinaryResponse, DolphinSchedulerClient
    from dsctl.config import ClusterProfile
    from dsctl.support.json_types import JsonValue

ClientT = TypeVar("ClientT")


class StringEnumValue(Protocol):
    """Structural enum-like value that serializes to one stable string."""

    @property
    def value(self) -> str:
        """Return the wire/value representation."""


class TaskTypeRecord(Protocol):
    """Structural task-type payload returned by favourite task discovery."""

    @property
    def taskType(self) -> str | None:  # noqa: N802
        """DS task type name."""

    @property
    def isCollection(self) -> bool:  # noqa: N802
        """Whether the current user marked the task type as favourite."""

    @property
    def taskCategory(self) -> str | None:  # noqa: N802
        """DS task category label."""


class TaskTypeOperations(Protocol):
    """Bound task-type discovery operations exposed to the service layer."""

    def list(self) -> Sequence[TaskTypeRecord]:
        """Return DS default task types plus the user's favourite flags."""


class ProjectRecord(Protocol):
    """Structural project identity shared across services and adapters."""

    @property
    def code(self) -> int | None:
        """Project code used for stable API addressing."""

    @property
    def name(self) -> str | None:
        """Human-facing project name."""

    @property
    def description(self) -> str | None:
        """Optional project description."""


class ProjectPayloadRecord(ProjectRecord, Protocol):
    """Structural project payload returned by upstream project operations."""

    @property
    def code(self) -> int:
        """Project code used for stable API addressing."""

    @property
    def id(self) -> int | None:
        """Project id."""

    @property
    def userId(self) -> int | None:  # noqa: N802
        """Project owner user id."""

    @property
    def userName(self) -> str | None:  # noqa: N802
        """Project owner user name."""

    @property
    def createTime(self) -> str | None:  # noqa: N802
        """Project creation time."""

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        """Project update time."""

    @property
    def perm(self) -> int:
        """Permission bitset."""

    @property
    def defCount(self) -> int:  # noqa: N802
        """Workflow definition count."""


class ProjectPageRecord(Protocol):
    """Structural DS paging payload for project list operations."""

    @property
    def totalList(self) -> Sequence[ProjectPayloadRecord] | None:  # noqa: N802
        """Page items."""

    @property
    def total(self) -> int | None:
        """Total remote item count."""

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        """Remote total page count."""

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        """Remote page size."""

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        """Remote current page number."""

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        """Alternate remote page number field."""


class ProjectOperations(Protocol):
    """Bound project operations exposed to the service layer."""

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> ProjectPageRecord:
        """Return one page of projects visible to the configured user."""

    def get(self, *, code: int) -> ProjectPayloadRecord:
        """Fetch a single project by code."""

    def create(
        self,
        *,
        name: str,
        description: str | None = None,
    ) -> ProjectPayloadRecord:
        """Create a project and return the created entity."""

    def update(
        self,
        *,
        code: int,
        name: str,
        description: str | None = None,
    ) -> ProjectPayloadRecord:
        """Update a project and return the updated entity."""

    def delete(self, *, code: int) -> bool:
        """Delete a project by code and return the remote deletion flag."""


class ProjectParameterRecord(Protocol):
    """Structural project-parameter payload used by project-scoped services."""

    @property
    def id(self) -> int | None:
        """Project-parameter row id."""

    @property
    def userId(self) -> int | None:  # noqa: N802
        """Owning user id."""

    @property
    def operator(self) -> int | None:
        """Last operator user id."""

    @property
    def code(self) -> int | None:
        """Stable project-parameter code."""

    @property
    def projectCode(self) -> int | None:  # noqa: N802
        """Owning project code."""

    @property
    def paramName(self) -> str | None:  # noqa: N802
        """Parameter name."""

    @property
    def paramValue(self) -> str | None:  # noqa: N802
        """Parameter value."""

    @property
    def paramDataType(self) -> str | None:  # noqa: N802
        """Parameter data type label."""

    @property
    def createTime(self) -> str | None:  # noqa: N802
        """Creation time."""

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        """Update time."""

    @property
    def createUser(self) -> str | None:  # noqa: N802
        """Creation user display name."""

    @property
    def modifyUser(self) -> str | None:  # noqa: N802
        """Last modifier display name."""


class ProjectParameterPageRecord(Protocol):
    """Structural DS paging payload for project-parameter list operations."""

    @property
    def totalList(self) -> Sequence[ProjectParameterRecord] | None:  # noqa: N802
        """Page items."""

    @property
    def total(self) -> int | None:
        """Total remote item count."""

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        """Remote total page count."""

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        """Remote page size."""

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        """Remote current page number."""

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        """Alternate remote page number field."""


class ProjectParameterOperations(Protocol):
    """Bound project-parameter operations exposed to the service layer."""

    def list(
        self,
        *,
        project_code: int,
        page_no: int,
        page_size: int,
        search: str | None = None,
        data_type: str | None = None,
    ) -> ProjectParameterPageRecord:
        """Return one page of project parameters for the selected project."""

    def get(self, *, project_code: int, code: int) -> ProjectParameterRecord:
        """Fetch a single project parameter by code."""

    def create(
        self,
        *,
        project_code: int,
        name: str,
        value: str,
        data_type: str,
    ) -> ProjectParameterRecord:
        """Create one project parameter and return the created entity."""

    def update(
        self,
        *,
        project_code: int,
        code: int,
        name: str,
        value: str,
        data_type: str,
    ) -> ProjectParameterRecord:
        """Update one project parameter and return the updated entity."""

    def delete(self, *, project_code: int, code: int) -> bool:
        """Delete one project parameter by code."""


class ProjectPreferenceRecord(Protocol):
    """Structural project-preference payload used by project-scoped services."""

    @property
    def id(self) -> int | None:
        """Project-preference row id."""

    @property
    def code(self) -> int:
        """Project-preference code."""

    @property
    def projectCode(self) -> int:  # noqa: N802
        """Owning project code."""

    @property
    def preferences(self) -> str | None:
        """Stored DS project preference JSON string."""

    @property
    def userId(self) -> int | None:  # noqa: N802
        """Updating user id when available."""

    @property
    def state(self) -> int:
        """Project-preference enabled state."""

    @property
    def createTime(self) -> str | None:  # noqa: N802
        """Creation time."""

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        """Update time."""


class ProjectPreferenceOperations(Protocol):
    """Bound project-preference operations exposed to the service layer."""

    def get(self, *, project_code: int) -> ProjectPreferenceRecord | None:
        """Fetch the singleton project preference for one selected project."""

    def update(
        self,
        *,
        project_code: int,
        preferences: str,
    ) -> ProjectPreferenceRecord:
        """Create or update the singleton project preference."""

    def set_state(self, *, project_code: int, state: int) -> None:
        """Set the singleton project-preference enabled state."""


class ProjectWorkerGroupRecord(Protocol):
    """Structural project worker-group payload used by project-scoped services."""

    @property
    def id(self) -> int | None:
        """Project worker-group row id."""

    @property
    def projectCode(self) -> int | None:  # noqa: N802
        """Owning project code."""

    @property
    def workerGroup(self) -> str | None:  # noqa: N802
        """Assigned or implied worker-group name."""

    @property
    def createTime(self) -> str | None:  # noqa: N802
        """Creation time."""

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        """Update time."""


class ProjectWorkerGroupOperations(Protocol):
    """Bound project worker-group operations exposed to the service layer."""

    def list(self, *, project_code: int) -> Sequence[ProjectWorkerGroupRecord]:
        """Return the current worker groups reported for one selected project."""

    def set(
        self,
        *,
        project_code: int,
        worker_groups: Sequence[str],
    ) -> None:
        """Replace the explicit worker-group assignment set for one project."""


class AccessTokenRecord(Protocol):
    """Structural access-token payload used by governance services."""

    @property
    def id(self) -> int | None:
        """Access-token id."""

    @property
    def userId(self) -> int | None:  # noqa: N802
        """Owning user id."""

    @property
    def token(self) -> str | None:
        """Token string."""

    @property
    def expireTime(self) -> str | None:  # noqa: N802
        """Expiration time."""

    @property
    def createTime(self) -> str | None:  # noqa: N802
        """Creation time."""

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        """Update time."""

    @property
    def userName(self) -> str | None:  # noqa: N802
        """Owning user name."""


class AccessTokenPageRecord(Protocol):
    """Structural DS paging payload for access-token list operations."""

    @property
    def totalList(self) -> Sequence[AccessTokenRecord] | None:  # noqa: N802
        """Page items."""

    @property
    def total(self) -> int | None:
        """Total remote item count."""

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        """Remote total page count."""

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        """Remote page size."""

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        """Remote current page number."""

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        """Alternate remote page number field."""


class AccessTokenOperations(Protocol):
    """Bound access-token operations exposed to the service layer."""

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> AccessTokenPageRecord:
        """Return one page of access tokens visible to the configured user."""

    def create(
        self,
        *,
        user_id: int,
        expire_time: str,
        token: str | None = None,
    ) -> AccessTokenRecord:
        """Create one access token and return the created entity."""

    def generate(
        self,
        *,
        user_id: int,
        expire_time: str,
    ) -> str:
        """Generate one token string without persisting it."""

    def update(
        self,
        *,
        token_id: int,
        user_id: int,
        expire_time: str,
        token: str | None = None,
    ) -> AccessTokenRecord:
        """Update one access token and return the updated entity."""

    def delete(self, *, token_id: int) -> bool:
        """Delete one access token by id."""


class ClusterRecord(Protocol):
    """Structural cluster identity used by service resolvers and lists."""

    @property
    def code(self) -> int | None:
        """Cluster code used for stable API addressing."""

    @property
    def name(self) -> str | None:
        """Human-facing cluster name."""

    @property
    def description(self) -> str | None:
        """Optional cluster description."""


class ClusterPayloadRecord(ClusterRecord, Protocol):
    """Structural cluster payload returned by upstream cluster operations."""

    @property
    def id(self) -> int:
        """Cluster id."""

    @property
    def code(self) -> int | None:
        """Cluster code used for stable API addressing."""

    @property
    def config(self) -> str | None:
        """Cluster config payload."""

    @property
    def workflowDefinitions(self) -> Sequence[str] | None:  # noqa: N802
        """Workflow definitions associated with this cluster."""

    @property
    def operator(self) -> int | None:
        """Operator user id."""

    @property
    def createTime(self) -> str | None:  # noqa: N802
        """Creation time."""

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        """Update time."""


class ClusterPageRecord(Protocol):
    """Structural DS paging payload for cluster list operations."""

    @property
    def totalList(self) -> Sequence[ClusterPayloadRecord] | None:  # noqa: N802
        """Page items."""

    @property
    def total(self) -> int | None:
        """Total remote item count."""

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        """Remote total page count."""

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        """Remote page size."""

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        """Remote current page number."""

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        """Alternate remote page number field."""


class ClusterOperations(Protocol):
    """Bound cluster operations exposed to the service layer."""

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> ClusterPageRecord:
        """Return one page of clusters visible to the configured user."""

    def get(self, *, code: int) -> ClusterPayloadRecord:
        """Fetch a single cluster by code."""

    def create(
        self,
        *,
        name: str,
        config: str,
        description: str | None = None,
    ) -> ClusterPayloadRecord:
        """Create one cluster and return the created entity."""

    def update(
        self,
        *,
        code: int,
        name: str,
        config: str,
        description: str | None = None,
    ) -> ClusterPayloadRecord:
        """Update one cluster and return the updated entity."""

    def delete(self, *, code: int) -> bool:
        """Delete one cluster by code."""


class EnvironmentRecord(Protocol):
    """Structural environment identity used by service resolvers and lists."""

    @property
    def code(self) -> int | None:
        """Environment code used for stable API addressing."""

    @property
    def name(self) -> str | None:
        """Human-facing environment name."""

    @property
    def description(self) -> str | None:
        """Optional environment description."""


class EnvironmentPayloadRecord(EnvironmentRecord, Protocol):
    """Structural environment payload returned by upstream environment ops."""

    @property
    def code(self) -> int | None:
        """Environment code used for stable API addressing."""

    @property
    def id(self) -> int | None:
        """Environment id."""

    @property
    def config(self) -> str | None:
        """Environment config payload."""

    @property
    def workerGroups(self) -> Sequence[str] | None:  # noqa: N802
        """Bound worker groups."""

    @property
    def operator(self) -> int | None:
        """Operator user id."""

    @property
    def createTime(self) -> str | None:  # noqa: N802
        """Creation time."""

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        """Update time."""


class EnvironmentPageRecord(Protocol):
    """Structural DS paging payload for environment list operations."""

    @property
    def totalList(self) -> Sequence[EnvironmentPayloadRecord] | None:  # noqa: N802
        """Page items."""

    @property
    def total(self) -> int | None:
        """Total remote item count."""

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        """Remote total page count."""

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        """Remote page size."""

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        """Remote current page number."""

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        """Alternate remote page number field."""


class EnvironmentOperations(Protocol):
    """Bound environment operations exposed to the service layer."""

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> EnvironmentPageRecord:
        """Return one page of environments visible to the configured user."""

    def list_all(self) -> Sequence[EnvironmentRecord]:
        """Return all environments visible to the configured user."""

    def get(self, *, code: int) -> EnvironmentPayloadRecord:
        """Fetch a single environment by code."""

    def create(
        self,
        *,
        name: str,
        config: str,
        description: str | None = None,
        worker_groups: Sequence[str] | None = None,
    ) -> EnvironmentPayloadRecord:
        """Create one environment and return the refreshed entity payload."""

    def update(
        self,
        *,
        code: int,
        name: str,
        config: str,
        description: str | None = None,
        worker_groups: Sequence[str],
    ) -> EnvironmentPayloadRecord:
        """Update one environment and return the refreshed entity payload."""

    def delete(self, *, code: int) -> bool:
        """Delete one environment by code and return the remote deletion flag."""


class DataSourceRecord(Protocol):
    """Structural datasource summary exposed to services and resolvers."""

    @property
    def id(self) -> int | None:
        """Datasource id used for stable API addressing."""

    @property
    def name(self) -> str | None:
        """Human-facing datasource name."""

    @property
    def note(self) -> str | None:
        """Optional datasource note."""

    @property
    def type(self) -> StringEnumValue | None:
        """Datasource type enum."""

    @property
    def userId(self) -> int:  # noqa: N802
        """Owner user id."""

    @property
    def userName(self) -> str | None:  # noqa: N802
        """Owner user name."""

    @property
    def createTime(self) -> str | None:  # noqa: N802
        """Creation time."""

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        """Update time."""


class DataSourcePageRecord(Protocol):
    """Structural DS paging payload for datasource list operations."""

    @property
    def totalList(self) -> Sequence[DataSourceRecord] | None:  # noqa: N802
        """Page items."""

    @property
    def total(self) -> int | None:
        """Total remote item count."""

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        """Remote total page count."""

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        """Remote page size."""

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        """Remote current page number."""

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        """Alternate remote page number field."""


class DataSourceOperations(Protocol):
    """Bound datasource operations exposed to the service layer."""

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> DataSourcePageRecord:
        """Return one page of datasources visible to the configured user."""

    def get(self, *, datasource_id: int) -> Mapping[str, object]:
        """Fetch one datasource detail payload by id."""

    def authorized_for_user(self, *, user_id: int) -> Sequence[DataSourceRecord]:
        """Return datasources currently authorized for one user."""

    def create(self, *, payload_json: str) -> DataSourceRecord:
        """Create one datasource and return the created datasource summary."""

    def update(
        self,
        *,
        datasource_id: int,
        payload_json: str,
    ) -> DataSourceRecord:
        """Update one datasource and return the updated datasource summary."""

    def delete(self, *, datasource_id: int) -> bool:
        """Delete one datasource by id and return the remote deletion flag."""

    def connection_test(self, *, datasource_id: int) -> bool:
        """Run one datasource connection test by id."""


class NamespaceRecord(Protocol):
    """Structural k8s namespace payload exposed to services and resolvers."""

    @property
    def id(self) -> int | None:
        """Namespace id used for stable API addressing."""

    @property
    def code(self) -> int | None:
        """Namespace code."""

    @property
    def namespace(self) -> str | None:
        """Namespace name."""

    @property
    def clusterCode(self) -> int | None:  # noqa: N802
        """Owning cluster code."""

    @property
    def clusterName(self) -> str | None:  # noqa: N802
        """Owning cluster name."""

    @property
    def userId(self) -> int:  # noqa: N802
        """Owner user id."""

    @property
    def userName(self) -> str | None:  # noqa: N802
        """Owner user name."""

    @property
    def createTime(self) -> str | None:  # noqa: N802
        """Creation time."""

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        """Update time."""


class NamespacePageRecord(Protocol):
    """Structural DS paging payload for namespace list operations."""

    @property
    def totalList(self) -> Sequence[NamespaceRecord] | None:  # noqa: N802
        """Page items."""

    @property
    def total(self) -> int | None:
        """Total remote item count."""

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        """Remote total page count."""

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        """Remote page size."""

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        """Remote current page number."""

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        """Alternate remote page number field."""


class NamespaceOperations(Protocol):
    """Bound namespace operations exposed to the service layer."""

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> NamespacePageRecord:
        """Return one page of namespaces visible to the configured user."""

    def available(self) -> Sequence[NamespaceRecord]:
        """Return namespaces available to the configured login user."""

    def create(
        self,
        *,
        namespace: str,
        cluster_code: int,
    ) -> NamespaceRecord:
        """Create one namespace and return the created namespace payload."""

    def delete(self, *, namespace_id: int) -> bool:
        """Delete one namespace by id and return the remote deletion flag."""

    def authorized_for_user(self, *, user_id: int) -> Sequence[NamespaceRecord]:
        """Return namespaces currently authorized for one user."""


class PluginDefineRecord(Protocol):
    """Structural UI-plugin definition payload used by alert-plugin flows."""

    @property
    def id(self) -> int | None:
        """Plugin definition id."""

    @property
    def pluginName(self) -> str | None:  # noqa: N802
        """Plugin definition name."""

    @property
    def pluginType(self) -> str | None:  # noqa: N802
        """Plugin definition type."""

    @property
    def pluginParams(self) -> str | None:  # noqa: N802
        """Plugin definition dynamic form schema."""

    @property
    def createTime(self) -> str | None:  # noqa: N802
        """Creation time."""

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        """Update time."""


class UiPluginOperations(Protocol):
    """Bound UI-plugin discovery operations used by alert-plugin services."""

    def list(self, *, plugin_type: str) -> Sequence[PluginDefineRecord]:
        """Return UI plugin definitions for one plugin type."""

    def get(self, *, plugin_id: int) -> PluginDefineRecord:
        """Fetch one UI plugin definition by id."""


class AlertPluginPayloadRecord(Protocol):
    """Structural alert-plugin instance payload returned by CRUD mutations."""

    @property
    def id(self) -> int | None:
        """Alert-plugin instance id used for stable API addressing."""

    @property
    def pluginDefineId(self) -> int:  # noqa: N802
        """Alert-plugin definition id."""

    @property
    def instanceName(self) -> str | None:  # noqa: N802
        """Alert-plugin instance name."""

    @property
    def pluginInstanceParams(self) -> str | None:  # noqa: N802
        """Serialized alert-plugin UI params."""

    @property
    def createTime(self) -> str | None:  # noqa: N802
        """Creation time."""

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        """Update time."""


class AlertPluginListItemRecord(AlertPluginPayloadRecord, Protocol):
    """Structural alert-plugin list item returned by list endpoints."""

    @property
    def id(self) -> int:
        """Alert-plugin instance id used for stable API addressing."""

    @property
    def instanceType(self) -> str | None:  # noqa: N802
        """Alert-plugin instance type label."""

    @property
    def warningType(self) -> str | None:  # noqa: N802
        """Alert-plugin warning type label."""

    @property
    def alertPluginName(self) -> str | None:  # noqa: N802
        """Alert-plugin definition display name."""


class AlertPluginPageRecord(Protocol):
    """Structural DS paging payload for alert-plugin list operations."""

    @property
    def totalList(self) -> Sequence[AlertPluginListItemRecord] | None:  # noqa: N802
        """Page items."""

    @property
    def total(self) -> int | None:
        """Total remote item count."""

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        """Remote total page count."""

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        """Remote page size."""

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        """Remote current page number."""

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        """Alternate remote page number field."""


class AlertPluginOperations(Protocol):
    """Bound alert-plugin instance operations exposed to the service layer."""

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> AlertPluginPageRecord:
        """Return one page of visible alert-plugin instances."""

    def list_all(self) -> Sequence[AlertPluginListItemRecord]:
        """Return all alert-plugin instances using the DS VO projection."""

    def create(
        self,
        *,
        plugin_define_id: int,
        instance_name: str,
        plugin_instance_params: str,
    ) -> AlertPluginPayloadRecord:
        """Create one alert-plugin instance and return the created payload."""

    def update(
        self,
        *,
        alert_plugin_id: int,
        instance_name: str,
        plugin_instance_params: str,
    ) -> AlertPluginPayloadRecord:
        """Update one alert-plugin instance and return the updated payload."""

    def delete(self, *, alert_plugin_id: int) -> bool:
        """Delete one alert-plugin instance by id."""

    def test_send(
        self,
        *,
        plugin_define_id: int,
        plugin_instance_params: str,
    ) -> bool:
        """Send one test alert using one plugin definition and UI params."""


class AlertGroupRecord(Protocol):
    """Structural alert-group payload exposed to services and resolvers."""

    @property
    def id(self) -> int | None:
        """Alert-group id used for stable API addressing."""

    @property
    def groupName(self) -> str | None:  # noqa: N802
        """Alert-group name."""

    @property
    def alertInstanceIds(self) -> str | None:  # noqa: N802
        """Comma-separated alert plugin instance ids."""

    @property
    def description(self) -> str | None:
        """Optional alert-group description."""

    @property
    def createTime(self) -> str | None:  # noqa: N802
        """Creation time."""

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        """Update time."""

    @property
    def createUserId(self) -> int:  # noqa: N802
        """Creator user id."""


class AlertGroupPageRecord(Protocol):
    """Structural DS paging payload for alert-group list operations."""

    @property
    def totalList(self) -> Sequence[AlertGroupRecord] | None:  # noqa: N802
        """Page items."""

    @property
    def total(self) -> int | None:
        """Total remote item count."""

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        """Remote total page count."""

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        """Remote page size."""

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        """Remote current page number."""

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        """Alternate remote page number field."""


class AlertGroupOperations(Protocol):
    """Bound alert-group operations exposed to the service layer."""

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> AlertGroupPageRecord:
        """Return one page of visible alert groups."""

    def get(self, *, alert_group_id: int) -> AlertGroupRecord:
        """Fetch one alert group by id."""

    def create(
        self,
        *,
        group_name: str,
        description: str | None,
        alert_instance_ids: str,
    ) -> AlertGroupRecord:
        """Create one alert group and return the created payload."""

    def update(
        self,
        *,
        alert_group_id: int,
        group_name: str,
        description: str | None,
        alert_instance_ids: str,
    ) -> AlertGroupRecord:
        """Update one alert group and return the updated payload."""

    def delete(self, *, alert_group_id: int) -> bool:
        """Delete one alert group by id and return the remote deletion flag."""


class AuditRecord(Protocol):
    """Structural audit-log payload exposed to runtime services."""

    @property
    def userName(self) -> str | None:  # noqa: N802
        """Audit actor user name."""

    @property
    def modelType(self) -> str | None:  # noqa: N802
        """Audit model type display name."""

    @property
    def modelName(self) -> str | None:  # noqa: N802
        """Audit model display name."""

    @property
    def operation(self) -> str | None:
        """Audit operation display name."""

    @property
    def createTime(self) -> str | None:  # noqa: N802
        """Audit timestamp."""

    @property
    def description(self) -> str | None:
        """Audit summary description."""

    @property
    def detail(self) -> str | None:
        """Audit detail payload."""

    @property
    def latency(self) -> str | None:
        """Recorded request latency string."""


class AuditPageRecord(Protocol):
    """Structural DS paging payload for audit-log list operations."""

    @property
    def totalList(self) -> Sequence[AuditRecord] | None:  # noqa: N802
        """Page items."""

    @property
    def total(self) -> int | None:
        """Total remote item count."""

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        """Remote total page count."""

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        """Remote page size."""

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        """Remote current page number."""

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        """Alternate remote page number field."""


class AuditModelTypeRecord(Protocol):
    """Structural audit model-type tree node returned by DS."""

    @property
    def name(self) -> str | None:
        """Audit model-type name."""

    @property
    def child(self) -> Sequence[AuditModelTypeRecord] | None:
        """Nested audit model-type children."""


class AuditOperationTypeRecord(Protocol):
    """Structural audit operation-type node returned by DS."""

    @property
    def name(self) -> str | None:
        """Audit operation-type name."""


class AuditOperations(Protocol):
    """Bound audit-log discovery operations exposed to runtime services."""

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
    ) -> AuditPageRecord:
        """Return one page of audit-log rows with optional filters."""

    def list_model_types(self) -> Sequence[AuditModelTypeRecord]:
        """Return the audit model-type tree."""

    def list_operation_types(self) -> Sequence[AuditOperationTypeRecord]:
        """Return the audit operation-type list."""


class QueueRecord(Protocol):
    """Structural queue payload exposed to services and resolvers."""

    @property
    def id(self) -> int | None:
        """Queue id used for stable API addressing."""

    @property
    def queueName(self) -> str | None:  # noqa: N802
        """Human-facing queue name."""

    @property
    def queue(self) -> str | None:
        """Underlying DS queue value."""

    @property
    def createTime(self) -> str | None:  # noqa: N802
        """Creation time."""

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        """Update time."""


class QueuePageRecord(Protocol):
    """Structural DS paging payload for queue list operations."""

    @property
    def totalList(self) -> Sequence[QueueRecord] | None:  # noqa: N802
        """Page items."""

    @property
    def total(self) -> int | None:
        """Total remote item count."""

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        """Remote total page count."""

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        """Remote page size."""

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        """Remote current page number."""

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        """Alternate remote page number field."""


class QueueOperations(Protocol):
    """Bound queue operations exposed to the service layer."""

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> QueuePageRecord:
        """Return one page of queues visible to the configured user."""

    def list_all(self) -> Sequence[QueueRecord]:
        """Return all queues visible to the configured user."""

    def get(self, *, queue_id: int) -> QueueRecord:
        """Fetch one queue by id."""

    def create(self, *, queue: str, queue_name: str) -> QueueRecord:
        """Create one queue and return the refreshed queue payload."""

    def update(
        self,
        *,
        queue_id: int,
        queue: str,
        queue_name: str,
    ) -> QueueRecord:
        """Update one queue and return the refreshed queue payload."""

    def delete(self, *, queue_id: int) -> bool:
        """Delete one queue by id and return the remote deletion flag."""


class WorkerGroupRecord(Protocol):
    """Structural worker-group payload exposed to services and resolvers."""

    @property
    def id(self) -> int | None:
        """Worker-group id used for UI-backed CRUD operations."""

    @property
    def name(self) -> str | None:
        """Human-facing worker-group name."""

    @property
    def addrList(self) -> str | None:  # noqa: N802
        """Comma-separated upstream worker address list."""

    @property
    def createTime(self) -> str | None:  # noqa: N802
        """Creation time."""

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        """Update time."""

    @property
    def description(self) -> str | None:
        """Optional worker-group description."""

    @property
    def systemDefault(self) -> bool:  # noqa: N802
        """Whether this row is derived from config/system state."""


class WorkerGroupPageRecord(Protocol):
    """Structural DS paging payload for worker-group list operations."""

    @property
    def totalList(self) -> Sequence[WorkerGroupRecord] | None:  # noqa: N802
        """Page items."""

    @property
    def total(self) -> int | None:
        """Total remote item count."""

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        """Remote total page count."""

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        """Remote page size."""

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        """Remote current page number."""

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        """Alternate remote page number field."""


class WorkerGroupOperations(Protocol):
    """Bound worker-group operations exposed to the service layer."""

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> WorkerGroupPageRecord:
        """Return one page of worker groups visible to the configured user."""

    def list_all(self) -> Sequence[WorkerGroupRecord]:
        """Return all worker groups visible to the configured user."""

    def get(self, *, worker_group_id: int) -> WorkerGroupRecord:
        """Fetch one UI-backed worker group by id."""

    def create(
        self,
        *,
        name: str,
        addr_list: str,
        description: str | None = None,
    ) -> WorkerGroupRecord:
        """Create one worker group and return the created payload."""

    def update(
        self,
        *,
        worker_group_id: int,
        name: str,
        addr_list: str,
        description: str | None = None,
    ) -> WorkerGroupRecord:
        """Update one worker group and return the updated payload."""

    def delete(self, *, worker_group_id: int) -> bool:
        """Delete one worker group by id and return the remote deletion flag."""


class TaskGroupRecord(Protocol):
    """Structural task-group payload exposed to services and resolvers."""

    @property
    def id(self) -> int | None:
        """Task-group id used for stable API addressing."""

    @property
    def name(self) -> str | None:
        """Human-facing task-group name."""

    @property
    def projectCode(self) -> int:  # noqa: N802
        """Owning project code."""

    @property
    def description(self) -> str | None:
        """Optional task-group description."""

    @property
    def groupSize(self) -> int:  # noqa: N802
        """Configured task-group capacity."""

    @property
    def useSize(self) -> int:  # noqa: N802
        """Current used capacity."""

    @property
    def userId(self) -> int:  # noqa: N802
        """Owner user id."""

    @property
    def status(self) -> StringEnumValue | str | None:
        """Current task-group status."""

    @property
    def createTime(self) -> str | None:  # noqa: N802
        """Creation time."""

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        """Update time."""


class TaskGroupPageRecord(Protocol):
    """Structural DS paging payload for task-group list operations."""

    @property
    def totalList(self) -> Sequence[TaskGroupRecord] | None:  # noqa: N802
        """Page items."""

    @property
    def total(self) -> int | None:
        """Total remote item count."""

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        """Remote total page count."""

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        """Remote page size."""

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        """Remote current page number."""

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        """Alternate remote page number field."""


class TaskGroupQueueRecord(Protocol):
    """Structural task-group queue payload exposed to services."""

    @property
    def id(self) -> int | None:
        """Task-group queue id."""

    @property
    def taskId(self) -> int:  # noqa: N802
        """Task-instance task id."""

    @property
    def taskName(self) -> str | None:  # noqa: N802
        """Task-instance display name."""

    @property
    def projectName(self) -> str | None:  # noqa: N802
        """Owning project name."""

    @property
    def projectCode(self) -> str | None:  # noqa: N802
        """Owning project code rendered by DS."""

    @property
    def workflowInstanceName(self) -> str | None:  # noqa: N802
        """Workflow-instance display name."""

    @property
    def groupId(self) -> int:  # noqa: N802
        """Owning task-group id."""

    @property
    def workflowInstanceId(self) -> int | None:  # noqa: N802
        """Workflow-instance id."""

    @property
    def priority(self) -> int:
        """Queue priority."""

    @property
    def forceStart(self) -> int:  # noqa: N802
        """Whether force-start was requested."""

    @property
    def inQueue(self) -> int:  # noqa: N802
        """Whether the task is still waiting in queue."""

    @property
    def status(self) -> StringEnumValue | str | None:
        """Current queue status."""

    @property
    def createTime(self) -> str | None:  # noqa: N802
        """Creation time."""

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        """Update time."""


class TaskGroupQueuePageRecord(Protocol):
    """Structural DS paging payload for task-group queue list operations."""

    @property
    def totalList(self) -> Sequence[TaskGroupQueueRecord] | None:  # noqa: N802
        """Page items."""

    @property
    def total(self) -> int | None:
        """Total remote item count."""

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        """Remote total page count."""

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        """Remote page size."""

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        """Remote current page number."""

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        """Alternate remote page number field."""


class TaskGroupOperations(Protocol):
    """Bound task-group operations exposed to the service layer."""

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
        status: int | None = None,
    ) -> TaskGroupPageRecord:
        """Return one page of task groups."""

    def list_by_project(
        self,
        *,
        project_code: int,
        page_no: int,
        page_size: int,
    ) -> TaskGroupPageRecord:
        """Return one page of task groups for one project."""

    def list_all(self) -> Sequence[TaskGroupRecord]:
        """Return all task groups visible to the configured user."""

    def get(self, *, task_group_id: int) -> TaskGroupRecord:
        """Fetch one task group by id."""

    def create(
        self,
        *,
        project_code: int,
        name: str,
        description: str,
        group_size: int,
    ) -> TaskGroupRecord:
        """Create one task group and return the created payload."""

    def update(
        self,
        *,
        task_group_id: int,
        name: str,
        description: str,
        group_size: int,
    ) -> TaskGroupRecord:
        """Update one task group and return the updated payload."""

    def close(self, *, task_group_id: int) -> None:
        """Close one task group."""

    def start(self, *, task_group_id: int) -> None:
        """Start one task group."""

    def list_queues(
        self,
        *,
        group_id: int,
        page_no: int,
        page_size: int,
        task_instance_name: str | None = None,
        workflow_instance_name: str | None = None,
        status: int | None = None,
    ) -> TaskGroupQueuePageRecord:
        """Return one page of task-group queue rows for one task group."""

    def force_start(self, *, queue_id: int) -> None:
        """Force-start one waiting task-group queue row."""

    def set_queue_priority(self, *, queue_id: int, priority: int) -> None:
        """Update one task-group queue priority."""


class TenantRecord(Protocol):
    """Structural tenant payload exposed to services and resolvers."""

    @property
    def id(self) -> int | None:
        """Tenant id used for CRUD addressing."""

    @property
    def tenantCode(self) -> str | None:  # noqa: N802
        """Tenant code."""

    @property
    def description(self) -> str | None:
        """Optional tenant description."""

    @property
    def queueId(self) -> int:  # noqa: N802
        """Bound queue id."""

    @property
    def queueName(self) -> str | None:  # noqa: N802
        """Bound queue name when the upstream payload provides it."""

    @property
    def queue(self) -> str | None:
        """Bound queue value when the upstream payload provides it."""

    @property
    def createTime(self) -> str | None:  # noqa: N802
        """Creation time."""

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        """Update time."""


class TenantPageRecord(Protocol):
    """Structural DS paging payload for tenant list operations."""

    @property
    def totalList(self) -> Sequence[TenantRecord] | None:  # noqa: N802
        """Page items."""

    @property
    def total(self) -> int | None:
        """Total remote item count."""

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        """Remote total page count."""

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        """Remote page size."""

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        """Remote current page number."""

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        """Alternate remote page number field."""


class TenantOperations(Protocol):
    """Bound tenant operations exposed to the service layer."""

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> TenantPageRecord:
        """Return one page of tenants visible to the configured user."""

    def list_all(self) -> Sequence[TenantRecord]:
        """Return all tenants visible to the configured user."""

    def get(self, *, tenant_id: int) -> TenantRecord:
        """Fetch one tenant by id."""

    def create(
        self,
        *,
        tenant_code: str,
        queue_id: int,
        description: str | None = None,
    ) -> TenantRecord:
        """Create one tenant and return the refreshed payload."""

    def update(
        self,
        *,
        tenant_id: int,
        tenant_code: str,
        queue_id: int,
        description: str | None = None,
    ) -> TenantRecord:
        """Update one tenant and return the refreshed payload."""

    def delete(self, *, tenant_id: int) -> bool:
        """Delete one tenant by id and return the remote deletion flag."""


class UserListRecord(Protocol):
    """Structural user list item returned by upstream paging operations."""

    @property
    def id(self) -> int | None:
        """User id used for CRUD addressing."""

    @property
    def userName(self) -> str | None:  # noqa: N802
        """Human-facing user name."""

    @property
    def email(self) -> str | None:
        """User email."""

    @property
    def phone(self) -> str | None:
        """User phone."""

    @property
    def userType(self) -> StringEnumValue | None:  # noqa: N802
        """User type."""

    @property
    def tenantId(self) -> int:  # noqa: N802
        """Bound tenant id."""

    @property
    def tenantCode(self) -> str | None:  # noqa: N802
        """Bound tenant code when the upstream view provides it."""

    @property
    def queueName(self) -> str | None:  # noqa: N802
        """Tenant queue name when the upstream view provides it."""

    @property
    def queue(self) -> str | None:
        """Effective queue value surfaced by the upstream view."""

    @property
    def state(self) -> int:
        """User state."""

    @property
    def createTime(self) -> str | None:  # noqa: N802
        """Creation time."""

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        """Update time."""


class UserRecord(UserListRecord, Protocol):
    """Structural full user snapshot used for get and patch-preserving update."""

    @property
    def timeZone(self) -> str | None:  # noqa: N802
        """User time zone when available."""

    @property
    def storedQueue(self) -> str | None:  # noqa: N802
        """Raw queue override stored on the user record."""


class UserPageRecord(Protocol):
    """Structural DS paging payload for user list operations."""

    @property
    def totalList(self) -> Sequence[UserListRecord] | None:  # noqa: N802
        """Page items."""

    @property
    def total(self) -> int | None:
        """Total remote item count."""

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        """Remote total page count."""

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        """Remote page size."""

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        """Remote current page number."""

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        """Alternate remote page number field."""


class UserOperations(Protocol):
    """Bound user operations exposed to the service layer."""

    def current(self) -> UserRecord:
        """Fetch the current authenticated user snapshot."""

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> UserPageRecord:
        """Return one page of users visible to the configured user."""

    def list_all(self) -> Sequence[UserRecord]:
        """Return full user snapshots available for resolution and updates."""

    def get(self, *, user_id: int) -> UserRecord:
        """Fetch one user by id."""

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
        """Create one user and return the refreshed payload."""

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
        """Update one user and return the refreshed payload."""

    def delete(self, *, user_id: int) -> bool:
        """Delete one user by id and return the remote deletion flag."""

    def grant_project_by_code(self, *, user_id: int, project_code: int) -> bool:
        """Grant one project to one user and return the remote status flag."""

    def revoke_project(self, *, user_id: int, project_code: int) -> bool:
        """Revoke one project from one user and return the remote status flag."""

    def grant_datasources(
        self,
        *,
        user_id: int,
        datasource_ids: Sequence[int],
    ) -> bool:
        """Apply the full datasource grant set for one user."""

    def grant_namespaces(
        self,
        *,
        user_id: int,
        namespace_ids: Sequence[int],
    ) -> bool:
        """Apply the full namespace grant set for one user."""


class ResourceItemRecord(Protocol):
    """Structural resource item payload exposed to resource services."""

    @property
    def alias(self) -> str | None:
        """Display alias exposed by DS."""

    @property
    def userName(self) -> str | None:  # noqa: N802
        """Owning user or tenant display name when available."""

    @property
    def fileName(self) -> str | None:  # noqa: N802
        """Leaf file or directory name."""

    @property
    def fullName(self) -> str | None:  # noqa: N802
        """DS full resource path."""

    @property
    def isDirectory(self) -> bool:  # noqa: N802
        """Whether this resource row is a directory."""

    @property
    def type(self) -> StringEnumValue | None:
        """DS resource type enum."""

    @property
    def size(self) -> int:
        """Remote resource size."""

    @property
    def createTime(self) -> str | None:  # noqa: N802
        """Creation time."""

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        """Update time."""


class ResourcePageRecord(Protocol):
    """Structural DS paging payload for resource list operations."""

    @property
    def totalList(self) -> Sequence[ResourceItemRecord] | None:  # noqa: N802
        """Page items."""

    @property
    def total(self) -> int | None:
        """Total remote item count."""

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        """Remote total page count."""

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        """Remote page size."""

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        """Remote current page number."""

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        """Alternate remote page number field."""


class ResourceContentRecord(Protocol):
    """Structural resource content payload returned by resource view calls."""

    @property
    def content(self) -> str | None:
        """Fetched file content chunk."""


class ResourceOperations(Protocol):
    """Bound resource operations exposed to the service layer."""

    def base_dir(self) -> str:
        """Return the effective DS base directory for file resources."""

    def list(
        self,
        *,
        directory: str,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> ResourcePageRecord:
        """Return one page of resources inside one DS directory."""

    def view(
        self,
        *,
        full_name: str,
        skip_line_num: int,
        limit: int,
    ) -> ResourceContentRecord:
        """Return one text content window for one resource file."""

    def upload(
        self,
        *,
        current_dir: str,
        name: str,
        file: IO[bytes],
    ) -> None:
        """Upload one local file into one DS directory."""

    def create_from_content(
        self,
        *,
        current_dir: str,
        file_name: str,
        suffix: str,
        content: str,
    ) -> None:
        """Create one DS file from inline text content."""

    def create_directory(
        self,
        *,
        current_dir: str,
        name: str,
    ) -> None:
        """Create one DS directory."""

    def delete(self, *, full_name: str) -> bool:
        """Delete one resource by full path."""

    def download(self, *, full_name: str) -> BinaryResponse:
        """Download one resource as a binary response payload."""


class MonitorServerRecord(Protocol):
    """Structural monitor server payload returned by upstream monitor ops."""

    @property
    def id(self) -> int:
        """Stable remote server id."""

    @property
    def host(self) -> str | None:
        """Server host."""

    @property
    def port(self) -> int:
        """Server port."""

    @property
    def serverDirectory(self) -> str | None:  # noqa: N802
        """Server working directory."""

    @property
    def heartBeatInfo(self) -> str | None:  # noqa: N802
        """Opaque upstream heartbeat payload."""

    @property
    def createTime(self) -> str | None:  # noqa: N802
        """Registration time."""

    @property
    def lastHeartbeatTime(self) -> str | None:  # noqa: N802
        """Last heartbeat time."""


class MonitorDatabaseRecord(Protocol):
    """Structural database metrics payload returned by upstream monitor ops."""

    @property
    def dbType(self) -> StringEnumValue | None:  # noqa: N802
        """Database type enum."""

    @property
    def state(self) -> StringEnumValue | None:
        """Database health state."""

    @property
    def maxConnections(self) -> int:  # noqa: N802
        """Configured max connections."""

    @property
    def maxUsedConnections(self) -> int:  # noqa: N802
        """Observed max used connections."""

    @property
    def threadsConnections(self) -> int:  # noqa: N802
        """Current thread connections."""

    @property
    def threadsRunningConnections(self) -> int:  # noqa: N802
        """Current running thread connections."""

    @property
    def date(self) -> str | None:
        """Metric snapshot time."""


class MonitorOperations(Protocol):
    """Bound monitor operations exposed to the service layer."""

    def list_servers(
        self,
        *,
        node_type: str,
    ) -> Sequence[MonitorServerRecord]:
        """Return one registry-backed server list for one monitor node type."""

    def list_databases(self) -> Sequence[MonitorDatabaseRecord]:
        """Return the current database health metrics payload."""


class WorkflowRecord(Protocol):
    """Structural workflow identity used by service resolvers and lists."""

    @property
    def code(self) -> int | None:
        """Workflow code used for stable API addressing."""

    @property
    def name(self) -> str | None:
        """Human-facing workflow name."""

    @property
    def version(self) -> int | None:
        """Workflow version."""


class ScheduleRecord(Protocol):
    """Structural schedule subset exposed in workflow payloads."""

    @property
    def startTime(self) -> str | None:  # noqa: N802
        """Schedule start time."""

    @property
    def endTime(self) -> str | None:  # noqa: N802
        """Schedule end time."""

    @property
    def timezoneId(self) -> str | None:  # noqa: N802
        """Schedule timezone id."""

    @property
    def crontab(self) -> str | None:
        """Cron expression."""

    @property
    def failureStrategy(self) -> StringEnumValue | None:  # noqa: N802
        """Failure strategy."""

    @property
    def workflowInstancePriority(self) -> StringEnumValue | None:  # noqa: N802
        """Workflow instance priority."""

    @property
    def releaseState(self) -> StringEnumValue | None:  # noqa: N802
        """Schedule release state."""


class SchedulePayloadRecord(ScheduleRecord, Protocol):
    """Structural schedule payload returned by upstream schedule operations."""

    @property
    def id(self) -> int | None:
        """Schedule id."""

    @property
    def workflowDefinitionCode(self) -> int:  # noqa: N802
        """Bound workflow definition code."""

    @property
    def workflowDefinitionName(self) -> str | None:  # noqa: N802
        """Bound workflow definition name."""

    @property
    def projectName(self) -> str | None:  # noqa: N802
        """Owning project name."""

    @property
    def definitionDescription(self) -> str | None:  # noqa: N802
        """Workflow definition description."""

    @property
    def warningType(self) -> StringEnumValue | None:  # noqa: N802
        """Warning type."""

    @property
    def createTime(self) -> str | None:  # noqa: N802
        """Creation time."""

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        """Update time."""

    @property
    def userId(self) -> int:  # noqa: N802
        """Owner user id."""

    @property
    def userName(self) -> str | None:  # noqa: N802
        """Owner user name."""

    @property
    def warningGroupId(self) -> int:  # noqa: N802
        """Warning group id."""

    @property
    def workerGroup(self) -> str | None:  # noqa: N802
        """Worker group."""

    @property
    def tenantCode(self) -> str | None:  # noqa: N802
        """Tenant code."""

    @property
    def environmentCode(self) -> int | None:  # noqa: N802
        """Environment code."""

    @property
    def environmentName(self) -> str | None:  # noqa: N802
        """Environment name."""


class SchedulePageRecord(Protocol):
    """Structural DS paging payload for schedule list operations."""

    @property
    def totalList(self) -> Sequence[SchedulePayloadRecord] | None:  # noqa: N802
        """Page items."""

    @property
    def total(self) -> int | None:
        """Total remote item count."""

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        """Remote total page count."""

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        """Remote page size."""

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        """Remote current page number."""

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        """Alternate remote page number field."""


class WorkflowPayloadRecord(WorkflowRecord, Protocol):
    """Structural workflow payload returned by upstream workflow operations."""

    @property
    def code(self) -> int:
        """Workflow code used for stable API addressing."""

    @property
    def id(self) -> int | None:
        """Workflow id."""

    @property
    def projectCode(self) -> int:  # noqa: N802
        """Owning project code."""

    @property
    def description(self) -> str | None:
        """Workflow description."""

    @property
    def globalParams(self) -> str | None:  # noqa: N802
        """Serialized global params."""

    @property
    def globalParamMap(self) -> dict[str, str] | None:  # noqa: N802
        """Global params map."""

    @property
    def createTime(self) -> str | None:  # noqa: N802
        """Creation time."""

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        """Update time."""

    @property
    def userId(self) -> int:  # noqa: N802
        """Owner user id."""

    @property
    def userName(self) -> str | None:  # noqa: N802
        """Owner user name."""

    @property
    def projectName(self) -> str | None:  # noqa: N802
        """Owning project name."""

    @property
    def timeout(self) -> int:
        """Workflow timeout in minutes."""

    @property
    def releaseState(self) -> StringEnumValue | None:  # noqa: N802
        """Workflow release state."""

    @property
    def scheduleReleaseState(self) -> StringEnumValue | None:  # noqa: N802
        """Schedule release state for this workflow."""

    @property
    def executionType(self) -> StringEnumValue | None:  # noqa: N802
        """Workflow execution type."""

    @property
    def schedule(self) -> ScheduleRecord | None:
        """Attached schedule data when available."""


class WorkflowTaskRelationRecord(Protocol):
    """Structural task relation used by workflow DAG exports."""

    @property
    def preTaskCode(self) -> int:  # noqa: N802
        """Upstream task code."""

    @property
    def postTaskCode(self) -> int:  # noqa: N802
        """Downstream task code."""

    @property
    def conditionParams(self) -> JsonValue | None:  # noqa: N802
        """Relation condition payload projected from the DS wire response."""


class TaskRecord(Protocol):
    """Structural task identity used by service resolvers and lists."""

    @property
    def code(self) -> int | None:
        """Task code used for stable API addressing."""

    @property
    def name(self) -> str | None:
        """Human-facing task name."""

    @property
    def version(self) -> int | None:
        """Task version."""


class TaskPayloadRecord(TaskRecord, Protocol):
    """Structural task payload returned by upstream task operations."""

    @property
    def code(self) -> int:
        """Task code used for stable API addressing."""

    @property
    def projectCode(self) -> int:  # noqa: N802
        """Owning project code."""

    @property
    def id(self) -> int | None:
        """Task id."""

    @property
    def description(self) -> str | None:
        """Task description."""

    @property
    def taskType(self) -> str | None:  # noqa: N802
        """Task type."""

    @property
    def taskParams(self) -> JsonValue | None:  # noqa: N802
        """Task params projected from the DS wire response."""

    @property
    def userName(self) -> str | None:  # noqa: N802
        """Owner user name."""

    @property
    def projectName(self) -> str | None:  # noqa: N802
        """Owning project name."""

    @property
    def workerGroup(self) -> str | None:  # noqa: N802
        """Worker group."""

    @property
    def failRetryTimes(self) -> int:  # noqa: N802
        """Retry count."""

    @property
    def failRetryInterval(self) -> int:  # noqa: N802
        """Retry interval in minutes."""

    @property
    def timeout(self) -> int:
        """Task timeout in minutes."""

    @property
    def delayTime(self) -> int:  # noqa: N802
        """Delay before execution in minutes."""

    @property
    def resourceIds(self) -> str | None:  # noqa: N802
        """Serialized resource ids."""

    @property
    def createTime(self) -> str | None:  # noqa: N802
        """Creation time."""

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        """Update time."""

    @property
    def modifyBy(self) -> str | None:  # noqa: N802
        """Modifier name."""

    @property
    def taskGroupId(self) -> int:  # noqa: N802
        """Task group id."""

    @property
    def taskGroupPriority(self) -> int:  # noqa: N802
        """Task group priority."""

    @property
    def environmentCode(self) -> int:  # noqa: N802
        """Environment code."""

    @property
    def taskPriority(self) -> StringEnumValue | None:  # noqa: N802
        """Task priority."""

    @property
    def timeoutFlag(self) -> StringEnumValue | None:  # noqa: N802
        """Timeout flag."""

    @property
    def timeoutNotifyStrategy(self) -> StringEnumValue | None:  # noqa: N802
        """Timeout notify strategy."""

    @property
    def taskExecuteType(self) -> StringEnumValue | None:  # noqa: N802
        """Task execute type."""

    @property
    def flag(self) -> StringEnumValue | None:
        """Task validity flag."""

    @property
    def cpuQuota(self) -> int | None:  # noqa: N802
        """Task CPU quota."""

    @property
    def memoryMax(self) -> int | None:  # noqa: N802
        """Task max memory."""


class WorkflowDagRecord(Protocol):
    """Structural DAG payload used by workflow describe/export operations."""

    @property
    def workflowDefinition(self) -> WorkflowPayloadRecord | None:  # noqa: N802
        """Workflow definition payload."""

    @property
    def workflowTaskRelationList(  # noqa: N802
        self,
    ) -> Sequence[WorkflowTaskRelationRecord] | None:
        """Workflow task relations."""

    @property
    def taskDefinitionList(self) -> Sequence[TaskPayloadRecord] | None:  # noqa: N802
        """Task definitions."""


class WorkflowLineageRelationRecord(Protocol):
    """Structural workflow-lineage edge payload exposed to services."""

    @property
    def sourceWorkFlowCode(self) -> int:  # noqa: N802
        """Source workflow code."""

    @property
    def targetWorkFlowCode(self) -> int:  # noqa: N802
        """Target workflow code."""


class WorkflowLineageDetailRecord(Protocol):
    """Structural workflow-lineage node/detail payload exposed to services."""

    @property
    def workFlowCode(self) -> int:  # noqa: N802
        """Workflow code."""

    @property
    def workFlowName(self) -> str | None:  # noqa: N802
        """Workflow name."""

    @property
    def workFlowPublishStatus(self) -> str | None:  # noqa: N802
        """Workflow publish status."""

    @property
    def scheduleStartTime(self) -> str | None:  # noqa: N802
        """Schedule start time."""

    @property
    def scheduleEndTime(self) -> str | None:  # noqa: N802
        """Schedule end time."""

    @property
    def crontab(self) -> str | None:
        """Schedule crontab."""

    @property
    def schedulePublishStatus(self) -> int:  # noqa: N802
        """Schedule publish status."""

    @property
    def sourceWorkFlowCode(self) -> str | None:  # noqa: N802
        """Immediate upstream workflow code rendered by DS."""


class WorkflowLineageRecord(Protocol):
    """Structural workflow-lineage graph payload exposed to services."""

    @property
    def workFlowRelationList(  # noqa: N802
        self,
    ) -> Sequence[WorkflowLineageRelationRecord] | None:
        """Workflow lineage edges."""

    @property
    def workFlowRelationDetailList(  # noqa: N802
        self,
    ) -> Sequence[WorkflowLineageDetailRecord] | None:
        """Workflow lineage node/detail rows."""


class DependentLineageTaskRecord(Protocol):
    """Structural dependent-task lineage payload exposed to services."""

    @property
    def projectCode(self) -> int:  # noqa: N802
        """Owning project code."""

    @property
    def workflowDefinitionCode(self) -> int:  # noqa: N802
        """Dependent workflow definition code."""

    @property
    def workflowDefinitionName(self) -> str | None:  # noqa: N802
        """Dependent workflow definition name."""

    @property
    def taskDefinitionCode(self) -> int:  # noqa: N802
        """Dependent task definition code."""

    @property
    def taskDefinitionName(self) -> str | None:  # noqa: N802
        """Dependent task definition name."""


class WorkflowLineageOperations(Protocol):
    """Bound workflow-lineage operations exposed to the service layer."""

    def list(self, *, project_code: int) -> WorkflowLineageRecord | None:
        """Return the project-wide workflow lineage graph."""

    def get(
        self,
        *,
        project_code: int,
        workflow_code: int,
    ) -> WorkflowLineageRecord | None:
        """Return the lineage graph anchored on one workflow."""

    def query_dependent_tasks(
        self,
        *,
        project_code: int,
        workflow_code: int,
        task_code: int | None = None,
    ) -> Sequence[DependentLineageTaskRecord]:
        """Return workflows/tasks that depend on one workflow or task."""


class WorkflowOperations(Protocol):
    """Bound workflow operations exposed to the service layer."""

    def list(self, *, project_code: int) -> Sequence[WorkflowRecord]:
        """Return workflows visible inside one project."""

    def get(self, *, code: int) -> WorkflowPayloadRecord:
        """Fetch one workflow by code."""

    def describe(self, *, project_code: int, code: int) -> WorkflowDagRecord:
        """Fetch one workflow DAG payload by project and workflow code."""

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
        """Create one full workflow definition from a compiled DAG payload."""

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
        """Update one whole workflow definition from a compiled DAG payload."""

    def delete(self, *, project_code: int, workflow_code: int) -> None:
        """Delete one workflow definition from one selected project."""

    def online(self, *, project_code: int, workflow_code: int) -> None:
        """Bring one workflow definition online."""

    def offline(self, *, project_code: int, workflow_code: int) -> None:
        """Bring one workflow definition offline."""

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
        """Trigger one workflow definition and return created instance ids."""

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
        """Backfill one workflow definition and return created instance ids."""


class WorkflowInstanceRecord(Protocol):
    """Structural workflow-instance payload exposed to runtime services."""

    @property
    def id(self) -> int | None:
        """Workflow instance id."""

    @property
    def workflowDefinitionCode(self) -> int | None:  # noqa: N802
        """Source workflow definition code."""

    @property
    def workflowDefinitionVersion(self) -> int:  # noqa: N802
        """Source workflow definition version."""

    @property
    def projectCode(self) -> int | None:  # noqa: N802
        """Owning project code."""

    @property
    def state(self) -> StringEnumValue | None:
        """Execution state."""

    @property
    def recovery(self) -> StringEnumValue | None:
        """Recovery flag."""

    @property
    def startTime(self) -> str | None:  # noqa: N802
        """Execution start time."""

    @property
    def endTime(self) -> str | None:  # noqa: N802
        """Execution end time."""

    @property
    def runTimes(self) -> int:  # noqa: N802
        """Total run attempts."""

    @property
    def name(self) -> str | None:
        """Workflow instance name."""

    @property
    def host(self) -> str | None:
        """Master host."""

    @property
    def commandType(self) -> StringEnumValue | None:  # noqa: N802
        """Trigger command type."""

    @property
    def taskDependType(self) -> StringEnumValue | None:  # noqa: N802
        """Task dependency scope."""

    @property
    def failureStrategy(self) -> StringEnumValue | None:  # noqa: N802
        """Failure strategy."""

    @property
    def warningType(self) -> StringEnumValue | None:  # noqa: N802
        """Warning policy."""

    @property
    def scheduleTime(self) -> str | None:  # noqa: N802
        """Scheduler/complement schedule payload."""

    @property
    def executorId(self) -> int:  # noqa: N802
        """Executor user id."""

    @property
    def executorName(self) -> str | None:  # noqa: N802
        """Executor user name."""

    @property
    def tenantCode(self) -> str | None:  # noqa: N802
        """Tenant code."""

    @property
    def queue(self) -> str | None:
        """Execution queue."""

    @property
    def duration(self) -> str | None:
        """Rendered duration string."""

    @property
    def workflowInstancePriority(self) -> StringEnumValue | None:  # noqa: N802
        """Workflow instance priority."""

    @property
    def workerGroup(self) -> str | None:  # noqa: N802
        """Worker group."""

    @property
    def environmentCode(self) -> int | None:  # noqa: N802
        """Environment code."""

    @property
    def timeout(self) -> int:
        """Workflow timeout."""

    @property
    def dryRun(self) -> int:  # noqa: N802
        """Dry-run flag."""

    @property
    def restartTime(self) -> str | None:  # noqa: N802
        """Restart time."""

    @property
    def dagData(self) -> WorkflowDagRecord | None:  # noqa: N802
        """Embedded workflow DAG payload when the DS endpoint includes it."""


class WorkflowInstanceSubWorkflowRecord(Protocol):
    """Structural sub-workflow relation payload exposed to runtime services."""

    @property
    def subWorkflowInstanceId(self) -> int | None:  # noqa: N802
        """Sub-workflow instance id linked from one SUB_WORKFLOW task instance."""


class WorkflowInstanceParentRecord(Protocol):
    """Structural parent-workflow relation payload exposed to runtime services."""

    @property
    def parentWorkflowInstance(self) -> int | None:  # noqa: N802
        """Parent workflow instance id linked from one sub-workflow instance."""


class WorkflowInstancePageRecord(Protocol):
    """Structural DS paging payload for workflow-instance list operations."""

    @property
    def totalList(self) -> Sequence[WorkflowInstanceRecord] | None:  # noqa: N802
        """Page items."""

    @property
    def total(self) -> int | None:
        """Total remote item count."""

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        """Remote total page count."""

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        """Remote page size."""

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        """Remote current page number."""

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        """Alternate remote page number field."""


class WorkflowInstanceOperations(Protocol):
    """Bound workflow-instance operations exposed to runtime services."""

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        project_name: str | None = None,
        workflow_name: str | None = None,
        state: str | None = None,
    ) -> WorkflowInstancePageRecord:
        """Return one page of workflow instances."""

    def get(self, *, workflow_instance_id: int) -> WorkflowInstanceRecord:
        """Fetch one workflow instance by id."""

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
    ) -> WorkflowPayloadRecord:
        """Update one finished workflow instance DAG and return the saved definition."""

    def parent_instance_by_sub_workflow(
        self,
        *,
        project_code: int,
        sub_workflow_instance_id: int,
    ) -> WorkflowInstanceParentRecord:
        """Return the parent workflow instance linked to one sub-workflow instance."""

    def sub_workflow_instance_by_task(
        self,
        *,
        project_code: int,
        task_instance_id: int,
    ) -> WorkflowInstanceSubWorkflowRecord:
        """Return the child workflow instance linked to one SUB_WORKFLOW task."""

    def stop(self, *, workflow_instance_id: int) -> None:
        """Request stop for one workflow instance."""

    def rerun(self, *, workflow_instance_id: int) -> None:
        """Request rerun for one workflow instance."""

    def recover_failed(self, *, workflow_instance_id: int) -> None:
        """Recover one failed workflow instance from failed tasks."""

    def execute_task(
        self,
        *,
        project_code: int,
        workflow_instance_id: int,
        task_code: int,
        scope: str,
    ) -> None:
        """Execute one task inside one existing workflow instance."""


class TaskOperations(Protocol):
    """Bound task operations exposed to the service layer."""

    def list(self, *, project_code: int, workflow_code: int) -> Sequence[TaskRecord]:
        """Return tasks belonging to one workflow."""

    def get(self, *, code: int) -> TaskPayloadRecord:
        """Fetch one task definition by code."""

    def update(
        self,
        *,
        project_code: int,
        code: int,
        task_definition_json: str,
        upstream_codes: Sequence[int],
    ) -> None:
        """Update one task definition and its upstream relations."""


class TaskInstanceRecord(Protocol):
    """Structural task-instance payload exposed to runtime services."""

    @property
    def id(self) -> int | None:
        """Task instance id."""

    @property
    def name(self) -> str | None:
        """Task instance name."""

    @property
    def taskType(self) -> str | None:  # noqa: N802
        """Task type."""

    @property
    def workflowInstanceId(self) -> int:  # noqa: N802
        """Owning workflow instance id."""

    @property
    def workflowInstanceName(self) -> str | None:  # noqa: N802
        """Owning workflow instance name."""

    @property
    def projectCode(self) -> int | None:  # noqa: N802
        """Owning project code."""

    @property
    def taskCode(self) -> int:  # noqa: N802
        """Task definition code."""

    @property
    def taskDefinitionVersion(self) -> int:  # noqa: N802
        """Task definition version."""

    @property
    def processDefinitionName(self) -> str | None:  # noqa: N802
        """Workflow definition name."""

    @property
    def state(self) -> StringEnumValue | None:
        """Execution state."""

    @property
    def firstSubmitTime(self) -> str | None:  # noqa: N802
        """First submit time."""

    @property
    def submitTime(self) -> str | None:  # noqa: N802
        """Submit time."""

    @property
    def startTime(self) -> str | None:  # noqa: N802
        """Execution start time."""

    @property
    def endTime(self) -> str | None:  # noqa: N802
        """Execution end time."""

    @property
    def host(self) -> str | None:
        """Worker host."""

    @property
    def logPath(self) -> str | None:  # noqa: N802
        """Task log path."""

    @property
    def retryTimes(self) -> int:  # noqa: N802
        """Retry attempts."""

    @property
    def duration(self) -> str | None:
        """Rendered duration string."""

    @property
    def executorName(self) -> str | None:  # noqa: N802
        """Executor user name."""

    @property
    def workerGroup(self) -> str | None:  # noqa: N802
        """Worker group."""

    @property
    def environmentCode(self) -> int | None:  # noqa: N802
        """Environment code."""

    @property
    def delayTime(self) -> int:  # noqa: N802
        """Delay time."""

    @property
    def taskParams(self) -> str | None:  # noqa: N802
        """Serialized task params."""

    @property
    def dryRun(self) -> int:  # noqa: N802
        """Dry-run flag."""

    @property
    def taskGroupId(self) -> int:  # noqa: N802
        """Task group id."""

    @property
    def taskExecuteType(self) -> StringEnumValue | None:  # noqa: N802
        """Task execute type."""


class TaskInstancePageRecord(Protocol):
    """Structural DS paging payload for task-instance list operations."""

    @property
    def totalList(self) -> Sequence[TaskInstanceRecord] | None:  # noqa: N802
        """Page items."""

    @property
    def total(self) -> int | None:
        """Total remote item count."""

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        """Remote total page count."""

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        """Remote page size."""

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        """Remote current page number."""

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        """Alternate remote page number field."""


class TaskLogRecord(Protocol):
    """Structural task-log chunk returned by upstream logger operations."""

    @property
    def lineNum(self) -> int:  # noqa: N802
        """Number of lines returned in this chunk."""

    @property
    def message(self) -> str | None:
        """Chunk payload text."""


class TaskInstanceOperations(Protocol):
    """Bound task-instance operations exposed to runtime services."""

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
        """Return one page of task instances inside one workflow instance."""

    def get(
        self,
        *,
        project_code: int,
        task_instance_id: int,
    ) -> TaskInstanceRecord:
        """Fetch one task instance by id within one project."""

    def log_chunk(
        self,
        *,
        task_instance_id: int,
        skip_line_num: int,
        limit: int,
    ) -> TaskLogRecord:
        """Fetch one incremental log chunk for a task instance."""

    def force_success(
        self,
        *,
        project_code: int,
        task_instance_id: int,
    ) -> None:
        """Force one finished failed task instance into FORCED_SUCCESS."""

    def savepoint(
        self,
        *,
        project_code: int,
        task_instance_id: int,
    ) -> None:
        """Trigger one savepoint request for a running task instance."""

    def stop(
        self,
        *,
        project_code: int,
        task_instance_id: int,
    ) -> None:
        """Request stop for one task instance."""


class ScheduleOperations(Protocol):
    """Bound schedule operations exposed to the service layer."""

    def list(
        self,
        *,
        project_code: int,
        page_no: int,
        page_size: int,
        workflow_code: int | None = None,
        search: str | None = None,
    ) -> SchedulePageRecord:
        """Return one page of schedules inside one project."""

    def get(self, *, schedule_id: int) -> SchedulePayloadRecord:
        """Fetch one schedule by id."""

    def preview(
        self,
        *,
        project_code: int,
        crontab: str,
        start_time: str,
        end_time: str,
        timezone_id: str,
    ) -> Sequence[str]:
        """Preview the next fire times for one schedule expression."""

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
    ) -> SchedulePayloadRecord:
        """Create one schedule bound to a workflow."""

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
    ) -> SchedulePayloadRecord:
        """Update one schedule by id."""

    def delete(self, *, schedule_id: int) -> bool:
        """Delete one schedule by id."""

    def online(self, *, schedule_id: int) -> SchedulePayloadRecord:
        """Bring one schedule online and return the refreshed payload."""

    def offline(self, *, schedule_id: int) -> SchedulePayloadRecord:
        """Bring one schedule offline and return the refreshed payload."""


class UpstreamSession(Protocol):
    """Bound version adapter operations for one profile/client runtime."""

    @property
    def task_types(self) -> TaskTypeOperations:
        """Return the bound task-type discovery group."""

    @property
    def projects(self) -> ProjectOperations:
        """Return the bound project operation group."""

    @property
    def project_parameters(self) -> ProjectParameterOperations:
        """Return the bound project-parameter operation group."""

    @property
    def project_preferences(self) -> ProjectPreferenceOperations:
        """Return the bound project-preference operation group."""

    @property
    def project_worker_groups(self) -> ProjectWorkerGroupOperations:
        """Return the bound project worker-group operation group."""

    @property
    def access_tokens(self) -> AccessTokenOperations:
        """Return the bound access-token operation group."""

    @property
    def clusters(self) -> ClusterOperations:
        """Return the bound cluster operation group."""

    @property
    def environments(self) -> EnvironmentOperations:
        """Return the bound environment operation group."""

    @property
    def datasources(self) -> DataSourceOperations:
        """Return the bound datasource operation group."""

    @property
    def namespaces(self) -> NamespaceOperations:
        """Return the bound namespace operation group."""

    @property
    def ui_plugins(self) -> UiPluginOperations:
        """Return the bound UI-plugin discovery group."""

    @property
    def alert_plugins(self) -> AlertPluginOperations:
        """Return the bound alert-plugin instance operation group."""

    @property
    def alert_groups(self) -> AlertGroupOperations:
        """Return the bound alert-group operation group."""

    @property
    def queues(self) -> QueueOperations:
        """Return the bound queue operation group."""

    @property
    def worker_groups(self) -> WorkerGroupOperations:
        """Return the bound worker-group operation group."""

    @property
    def task_groups(self) -> TaskGroupOperations:
        """Return the bound task-group operation group."""

    @property
    def tenants(self) -> TenantOperations:
        """Return the bound tenant operation group."""

    @property
    def users(self) -> UserOperations:
        """Return the bound user operation group."""

    @property
    def audits(self) -> AuditOperations:
        """Return the bound audit-log discovery group."""

    @property
    def resources(self) -> ResourceOperations:
        """Return the bound resource operation group."""

    @property
    def monitor(self) -> MonitorOperations:
        """Return the bound monitor operation group."""

    @property
    def workflows(self) -> WorkflowOperations:
        """Return the bound workflow operation group."""

    @property
    def workflow_lineages(self) -> WorkflowLineageOperations:
        """Return the bound workflow-lineage operation group."""

    @property
    def tasks(self) -> TaskOperations:
        """Return the bound task operation group."""

    @property
    def schedules(self) -> ScheduleOperations:
        """Return the bound schedule operation group."""

    @property
    def workflow_instances(self) -> WorkflowInstanceOperations:
        """Return the bound workflow-instance operation group."""

    @property
    def task_instances(self) -> TaskInstanceOperations:
        """Return the bound task-instance operation group."""


class UpstreamAdapter(Protocol[ClientT]):
    """Protocol for DS-version-specific generated client adapters."""

    ds_version: str
    version_slug: str
    client_class: type[ClientT]

    def create_client(
        self,
        profile: ClusterProfile,
        *,
        transport: httpx.BaseTransport | None = None,
        client: DolphinSchedulerClient | None = None,
    ) -> ClientT:
        """Create a versioned generated client bound to a cluster profile."""

    def bind(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> UpstreamSession:
        """Bind versioned operations to a concrete profile and HTTP client."""
