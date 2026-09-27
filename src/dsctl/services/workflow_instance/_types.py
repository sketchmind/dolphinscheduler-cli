from __future__ import annotations

from typing import TYPE_CHECKING, Literal, NotRequired, TypeAlias, TypedDict

from dsctl.services.runtime import (
    BoundDomainServiceRuntime,
)
from dsctl.upstream.pagination import (
    PageData,
)
from dsctl.upstream.runtime_instances import (
    RuntimeInstanceDomain,
)
from dsctl.upstream.serialization import (
    WorkflowInstanceData,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from dsctl.services._task_datasource_refs import TaskDatasourceResolutionData
    from dsctl.support.yaml_io import JsonObject
    from dsctl.upstream.protocol import (
        TaskRecord,
        WorkflowDagRecord,
    )
    from dsctl.upstream.resolver import (
        ResolvedTaskData,
    )


RuntimeInstanceServiceRuntime = BoundDomainServiceRuntime[RuntimeInstanceDomain]


class _InstanceDagTaskOperations:
    """Resolve execute-task selectors from the already fetched instance DAG."""

    def __init__(self, dag: WorkflowDagRecord) -> None:
        self._dag = dag

    def list(
        self,
        *,
        project_code: int,
        workflow_code: int,
    ) -> Sequence[TaskRecord]:
        del project_code, workflow_code
        return tuple(self._dag.taskDefinitionList or ())

    def generate_codes(self, *, project_code: int, count: int) -> Sequence[int]:
        del project_code, count
        message = "Instance DAG task resolver cannot allocate task codes"
        raise RuntimeError(message)


class WorkflowInstanceSelectionData(TypedDict):
    """Resolved workflow-instance selector emitted in JSON envelopes."""

    id: int


WorkflowInstancePageData = PageData[WorkflowInstanceData]


DEFAULT_WATCH_INTERVAL_SECONDS = 5


DEFAULT_WATCH_TIMEOUT_SECONDS = 600


WORKFLOW_INSTANCE_EXECUTING_COMMAND = 50009


EXECUTE_WORKFLOW_INSTANCE_ERROR = 50015


WORKFLOW_DEFINITION_NOT_RELEASE = 50004


WORKFLOW_INSTANCE_NOT_FINISHED = 50071


WORKFLOW_INSTANCE_NOT_SUB_WORKFLOW_INSTANCE = 50010


SUB_WORKFLOW_INSTANCE_NOT_EXIST = 50007


WORKFLOW_INSTANCE_NOT_EXIST = 50001


USER_NO_OPERATION_PERM = 30001


USER_NO_OPERATION_PROJECT_PERM = 30002


EXECUTE_NOT_DEFINE_TASK = 10206


DATA_IS_NOT_VALID = 50017


WORKFLOW_NODE_HAS_CYCLE = 50019


WORKFLOW_NODE_S_PARAMETER_INVALID = 50020


CHECK_WORKFLOW_TASK_RELATION_ERROR = 50036


QUERY_WORKFLOW_INSTANCE_LIST_PAGING_ERROR = 10113


WorkflowInstanceEditInputMode = Literal["patch", "file"]


class WorkflowInstanceExecuteTaskResolved(TypedDict):
    """Resolved execute-task metadata emitted in JSON envelopes."""

    workflowInstance: WorkflowInstanceSelectionData
    project: JsonObject
    task: ResolvedTaskData
    scope: str


class WorkflowInstanceActionWarningDetail(TypedDict):
    """Structured warning emitted after one runtime action request."""

    code: str
    action: str
    message: str
    current_state: str
    expect_non_final: bool
    target_state: str | None


class WorkflowInstanceParentData(TypedDict):
    """DS-native parent relation payload emitted for one sub-workflow instance."""

    parentWorkflowInstance: int


class WorkflowInstanceEditResolved(TypedDict):
    """Resolved metadata emitted for one workflow-instance edit request."""

    workflowInstance: WorkflowInstanceSelectionData
    project: JsonObject
    workflow: JsonObject
    input_mode: WorkflowInstanceEditInputMode
    patch_file: NotRequired[str]
    file: NotRequired[str]
    syncDefine: bool
    native_definition_release_effect: NotRequired[str]
    task_datasources: list[TaskDatasourceResolutionData]


class WorkflowInstanceYamlExportData(TypedDict):
    """YAML workflow document exported from one workflow instance DAG."""

    yaml: str


class WorkflowInstanceEditNoChangeWarningDetail(TypedDict):
    """Structured warning emitted when one instance patch changes nothing."""

    code: str
    message: str
    no_change: bool
    request_sent: bool


WorkflowInstanceListResolvedValue: TypeAlias = int | str | bool
