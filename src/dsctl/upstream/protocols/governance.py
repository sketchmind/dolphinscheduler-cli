from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from typing import IO

    from dsctl.client import BinaryResponse
    from dsctl.upstream.protocols.base import StringEnumValue


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
