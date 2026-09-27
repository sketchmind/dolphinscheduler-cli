"""Explicit shared fake collaborators for service and command tests."""

from tests.fakes.alerts import (
    FakeAlertGroup,
    FakeAlertGroupAdapter,
    FakeAlertPlugin,
    FakeAlertPluginAdapter,
    FakePluginDefine,
    FakeUiPluginAdapter,
)
from tests.fakes.audit import (
    FakeAudit,
    FakeAuditAdapter,
    FakeAuditModelType,
    FakeAuditOperationType,
)
from tests.fakes.common import (
    FakeEnumValue,
    FakeHttpClient,
)
from tests.fakes.datasources import (
    FakeDataSource,
    FakeDataSourceAdapter,
)
from tests.fakes.definitions import (
    FakeDag,
    FakeTaskDefinition,
    FakeWorkflow,
    FakeWorkflowPage,
    FakeWorkflowTaskRelation,
)
from tests.fakes.lineage import (
    FakeDependentLineageTask,
    FakeWorkflowLineage,
    FakeWorkflowLineageAdapter,
    FakeWorkflowLineageDetail,
    FakeWorkflowLineageRelation,
)
from tests.fakes.monitoring import (
    FakeMonitorAdapter,
    FakeMonitorDatabase,
    FakeMonitorServer,
)
from tests.fakes.namespaces import (
    FakeNamespace,
    FakeNamespaceAdapter,
)
from tests.fakes.projects import (
    FakeNativeProjectMutations,
    FakeProject,
    FakeProjectAdapter,
    FakeProjectPage,
    FakeProjectParameter,
    FakeProjectParameterAdapter,
    FakeProjectPreference,
    FakeProjectPreferenceAdapter,
    FakeProjectWorkerGroup,
    FakeProjectWorkerGroupAdapter,
)
from tests.fakes.queues import (
    FakeQueue,
    FakeQueueAdapter,
)
from tests.fakes.resources import (
    FakeResourceAdapter,
    FakeResourceItem,
)
from tests.fakes.runtime import (
    fake_bound_domain_service_runtime,
    fake_project_definitions,
    fake_read_service_runtime,
    fake_task_definition_service_runtime,
)
from tests.fakes.schedules import (
    FakeSchedule,
    FakeScheduleAdapter,
)
from tests.fakes.task_definitions import (
    FakeTaskAdapter,
    empty_task_adapter,
)
from tests.fakes.task_groups import (
    FakeTaskGroup,
    FakeTaskGroupAdapter,
    FakeTaskGroupQueue,
)
from tests.fakes.task_instances import (
    FakeTaskInstance,
    FakeTaskInstanceAdapter,
    empty_task_instance_adapter,
)
from tests.fakes.task_types import (
    FakeTaskType,
    FakeTaskTypeAdapter,
)
from tests.fakes.users import (
    FakeAccessToken,
    FakeAccessTokenAdapter,
    FakeUser,
    FakeUserAdapter,
)
from tests.fakes.workers import (
    FakeCluster,
    FakeClusterAdapter,
    FakeEnvironment,
    FakeEnvironmentAdapter,
    FakeTenant,
    FakeTenantAdapter,
    FakeWorkerGroup,
    FakeWorkerGroupAdapter,
)
from tests.fakes.workflow_instances import (
    FakeWorkflowInstance,
    FakeWorkflowInstanceAdapter,
    empty_workflow_instance_adapter,
)
from tests.fakes.workflows import (
    FakeWorkflowAdapter,
    empty_workflow_adapter,
)

__all__ = [
    "FakeAccessToken",
    "FakeAccessTokenAdapter",
    "FakeAlertGroup",
    "FakeAlertGroupAdapter",
    "FakeAlertPlugin",
    "FakeAlertPluginAdapter",
    "FakeAudit",
    "FakeAuditAdapter",
    "FakeAuditModelType",
    "FakeAuditOperationType",
    "FakeCluster",
    "FakeClusterAdapter",
    "FakeDag",
    "FakeDataSource",
    "FakeDataSourceAdapter",
    "FakeDependentLineageTask",
    "FakeEnumValue",
    "FakeEnvironment",
    "FakeEnvironmentAdapter",
    "FakeHttpClient",
    "FakeMonitorAdapter",
    "FakeMonitorDatabase",
    "FakeMonitorServer",
    "FakeNamespace",
    "FakeNamespaceAdapter",
    "FakeNativeProjectMutations",
    "FakePluginDefine",
    "FakeProject",
    "FakeProjectAdapter",
    "FakeProjectPage",
    "FakeProjectParameter",
    "FakeProjectParameterAdapter",
    "FakeProjectPreference",
    "FakeProjectPreferenceAdapter",
    "FakeProjectWorkerGroup",
    "FakeProjectWorkerGroupAdapter",
    "FakeQueue",
    "FakeQueueAdapter",
    "FakeResourceAdapter",
    "FakeResourceItem",
    "FakeSchedule",
    "FakeScheduleAdapter",
    "FakeTaskAdapter",
    "FakeTaskDefinition",
    "FakeTaskGroup",
    "FakeTaskGroupAdapter",
    "FakeTaskGroupQueue",
    "FakeTaskInstance",
    "FakeTaskInstanceAdapter",
    "FakeTaskType",
    "FakeTaskTypeAdapter",
    "FakeTenant",
    "FakeTenantAdapter",
    "FakeUiPluginAdapter",
    "FakeUser",
    "FakeUserAdapter",
    "FakeWorkerGroup",
    "FakeWorkerGroupAdapter",
    "FakeWorkflow",
    "FakeWorkflowAdapter",
    "FakeWorkflowInstance",
    "FakeWorkflowInstanceAdapter",
    "FakeWorkflowLineage",
    "FakeWorkflowLineageAdapter",
    "FakeWorkflowLineageDetail",
    "FakeWorkflowLineageRelation",
    "FakeWorkflowPage",
    "FakeWorkflowTaskRelation",
    "empty_task_adapter",
    "empty_task_instance_adapter",
    "empty_workflow_adapter",
    "empty_workflow_instance_adapter",
    "fake_bound_domain_service_runtime",
    "fake_project_definitions",
    "fake_read_service_runtime",
    "fake_task_definition_service_runtime",
]
