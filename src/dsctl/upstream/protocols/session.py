from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

from dsctl.upstream.protocols.base import ClientT

if TYPE_CHECKING:
    import httpx

    from dsctl.client import DolphinSchedulerClient
    from dsctl.config import ClusterProfile
    from dsctl.upstream.protocols.base import TaskTypeOperations
    from dsctl.upstream.protocols.design import (
        ScheduleOperations,
        WorkflowLineageOperations,
        WorkflowOperations,
    )
    from dsctl.upstream.protocols.governance import (
        AccessTokenOperations,
        AlertGroupOperations,
        AlertPluginOperations,
        AuditOperations,
        ClusterOperations,
        DataSourceOperations,
        EnvironmentOperations,
        MonitorOperations,
        NamespaceOperations,
        QueueOperations,
        ResourceOperations,
        TaskGroupOperations,
        TenantOperations,
        UiPluginOperations,
        UserOperations,
        WorkerGroupOperations,
    )
    from dsctl.upstream.protocols.project import (
        ProjectOperations,
        ProjectParameterOperations,
        ProjectPreferenceOperations,
        ProjectWorkerGroupOperations,
    )
    from dsctl.upstream.protocols.runtime import (
        TaskInstanceOperations,
        TaskOperations,
        WorkflowInstanceOperations,
    )


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
