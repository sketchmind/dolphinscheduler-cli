from __future__ import annotations

from pydantic import Field
from ..._models import BaseEntityModel

from ...common.enums.command_type import CommandType
from ...common.enums.failure_strategy import FailureStrategy
from ...common.enums.flag import Flag
from ...common.enums.priority import Priority
from ...common.enums.task_depend_type import TaskDependType
from ...common.enums.warning_type import WarningType
from ...common.enums.workflow_execution_status import WorkflowExecutionStatus
from .dag_data import DagData
from .workflow_definition import WorkflowDefinition

class WorkflowInstance(BaseEntityModel):
    id: int | None = Field(default=None)
    workflowDefinitionCode: int | None = Field(default=None)
    workflowDefinitionVersion: int = Field(default=0)
    projectCode: int | None = Field(default=None)
    state: WorkflowExecutionStatus | None = Field(default=None)
    stateHistory: str | None = Field(default=None)
    stateDescList: list[WorkflowInstanceStateDesc] | None = Field(default=None)
    recovery: Flag | None = Field(default=None)
    startTime: str | None = Field(default=None)
    endTime: str | None = Field(default=None)
    runTimes: int = Field(default=0)
    name: str | None = Field(default=None)
    host: str | None = Field(default=None)
    workflowDefinition: WorkflowDefinition | None = Field(default=None)
    commandType: CommandType | None = Field(default=None)
    commandParam: str | None = Field(default=None)
    taskDependType: TaskDependType | None = Field(default=None)
    maxTryTimes: int = Field(default=0)
    failureStrategy: FailureStrategy | None = Field(default=None)
    warningType: WarningType | None = Field(default=None)
    warningGroupId: int | None = Field(default=None)
    scheduleTime: str | None = Field(default=None)
    commandStartTime: str | None = Field(default=None)
    globalParams: str | None = Field(default=None, description='user define parameters string')
    dagData: DagData | None = Field(default=None)
    executorId: int = Field(default=0)
    executorName: str | None = Field(default=None)
    tenantCode: str | None = Field(default=None)
    queue: str | None = Field(default=None)
    isSubWorkflow: Flag | None = Field(default=None)
    locations: str | None = Field(default=None, description='task locations for web')
    historyCmd: str | None = Field(default=None)
    dependenceScheduleTimes: str | None = Field(default=None)
    duration: str | None = Field(default=None, description='workflow execution duration')
    workflowInstancePriority: Priority | None = Field(default=None)
    workerGroup: str | None = Field(default=None)
    environmentCode: int | None = Field(default=None)
    timeout: int = Field(default=0)
    varPool: str | None = Field(default=None)
    nextWorkflowInstanceId: int = Field(default=0)
    dryRun: int = Field(default=0)
    restartTime: str | None = Field(default=None)

class WorkflowInstanceStateDesc(BaseEntityModel):
    time: str | None = Field(default=None)
    state: WorkflowExecutionStatus | None = Field(default=None)
    desc: str | None = Field(default=None)

__all__ = ["WorkflowInstance", "WorkflowInstanceStateDesc"]
