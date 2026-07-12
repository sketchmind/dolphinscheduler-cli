from __future__ import annotations

from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Generic,
    Literal,
    NotRequired,
    Protocol,
    TypedDict,
    TypeVar,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from dsctl.support.json_types import JsonObject, JsonValue
    from dsctl.upstream.protocols.base import StringEnumValue


WorkflowCodeT_co = TypeVar("WorkflowCodeT_co", covariant=True)


@dataclass(frozen=True)
class ScheduleCreateSpec(Generic[WorkflowCodeT_co]):
    """Version-stable input for one schedule create request."""

    project_code: int
    workflow_code: WorkflowCodeT_co
    crontab: str
    start_time: str
    end_time: str
    timezone_id: str
    failure_strategy: str | None = None
    warning_type: str | None = None
    warning_group_id: int = 0
    workflow_instance_priority: str | None = None
    worker_group: str | None = None
    tenant_code: str | None = None
    environment_code: int | None = None


class ScheduleCreateRequestPlan(TypedDict):
    """Exact REST descriptor selected by one version adapter."""

    method: Literal["POST"]
    path: str
    form: NotRequired[JsonObject]
    json: NotRequired[JsonObject]


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
    def id(self) -> int | None:
        """Schedule id when the payload includes one."""

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
    def executionType(self) -> StringEnumValue | None:  # noqa: N802
        """Workflow execution type."""


class WorkflowListRecord(WorkflowRecord, Protocol):
    """Rich workflow row returned by the public paging endpoint."""

    @property
    def releaseState(self) -> StringEnumValue | None:  # noqa: N802
        """Workflow release state."""

    @property
    def scheduleReleaseState(self) -> StringEnumValue | None:  # noqa: N802
        """Attached schedule release state when available."""

    @property
    def schedule(self) -> ScheduleRecord | None:
        """Attached schedule when available."""


class WorkflowPageRecord(Protocol):
    """Structural DS paging payload for public workflow lists."""

    @property
    def totalList(self) -> Sequence[WorkflowListRecord] | None:  # noqa: N802
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

    def list_refs(self, *, project_code: int) -> Sequence[WorkflowRecord]:
        """Return inexpensive workflow identities for selector resolution."""

    def list_page(
        self,
        *,
        project_code: int,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> WorkflowPageRecord:
        """Return one rich public page of workflows inside one project."""

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
        spec: ScheduleCreateSpec[int],
    ) -> SchedulePayloadRecord:
        """Create one schedule bound to a workflow."""

    def plan_create(
        self,
        *,
        spec: ScheduleCreateSpec[int | str],
    ) -> ScheduleCreateRequestPlan:
        """Describe the exact version-selected REST request without sending it."""

    def update(
        self,
        *,
        project_code: int,
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
        tenant_code: str | None = None,
        environment_code: int | None = None,
    ) -> SchedulePayloadRecord:
        """Update one schedule by id."""

    def delete(self, *, schedule_id: int) -> bool:
        """Delete one schedule by id."""

    def online(self, *, schedule_id: int) -> SchedulePayloadRecord:
        """Bring one schedule online and return the refreshed payload."""

    def offline(self, *, schedule_id: int) -> SchedulePayloadRecord:
        """Bring one schedule offline and return the refreshed payload."""
