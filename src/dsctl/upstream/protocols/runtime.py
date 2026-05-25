from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from collections.abc import Sequence

    from dsctl.upstream.protocols.base import StringEnumValue
    from dsctl.upstream.protocols.design import (
        TaskPayloadRecord,
        TaskRecord,
        WorkflowDagRecord,
        WorkflowPayloadRecord,
    )


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
        project_code: int | None = None,
        workflow_code: int | None = None,
        project_name: str | None = None,
        workflow_name: str | None = None,
        search: str | None = None,
        executor: str | None = None,
        host: str | None = None,
        start_time: str | None = None,
        end_time: str | None = None,
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
        project_code: int,
        page_no: int,
        page_size: int,
        workflow_instance_id: int | None = None,
        workflow_instance_name: str | None = None,
        workflow_definition_name: str | None = None,
        search: str | None = None,
        task_name: str | None = None,
        task_code: int | None = None,
        executor: str | None = None,
        state: str | None = None,
        host: str | None = None,
        start_time: str | None = None,
        end_time: str | None = None,
        task_execute_type: str | None = None,
    ) -> TaskInstancePageRecord:
        """Return one project-scoped page of task instances."""

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
