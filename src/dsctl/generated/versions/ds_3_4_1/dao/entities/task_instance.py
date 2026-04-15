from __future__ import annotations

from pydantic import Field
from ..._models import BaseEntityModel

from ...common.enums.flag import Flag
from ...common.enums.priority import Priority
from ...common.enums.task_execute_type import TaskExecuteType
from ...plugin.task_api.enums.task_execution_status import TaskExecutionStatus
from .task_definition import TaskDefinition
from .workflow_definition import WorkflowDefinition
from .workflow_instance import WorkflowInstance

class TaskInstance(BaseEntityModel):
    id: int | None = Field(default=None)
    name: str | None = Field(default=None)
    taskType: str | None = Field(default=None)
    workflowInstanceId: int = Field(default=0)
    workflowInstanceName: str | None = Field(default=None)
    projectCode: int | None = Field(default=None)
    taskCode: int = Field(default=0)
    taskDefinitionVersion: int = Field(default=0)
    processDefinitionName: str | None = Field(default=None)
    taskGroupPriority: int = Field(default=0)
    state: TaskExecutionStatus | None = Field(default=None)
    firstSubmitTime: str | None = Field(default=None)
    submitTime: str | None = Field(default=None)
    startTime: str | None = Field(default=None)
    endTime: str | None = Field(default=None)
    host: str | None = Field(default=None)
    executePath: str | None = Field(default=None)
    logPath: str | None = Field(default=None)
    retryTimes: int = Field(default=0)
    alertFlag: Flag | None = Field(default=None)
    workflowInstance: WorkflowInstance | None = Field(default=None)
    workflowDefinition: WorkflowDefinition | None = Field(default=None)
    taskDefine: TaskDefinition | None = Field(default=None)
    pid: int = Field(default=0)
    appLink: str | None = Field(default=None)
    flag: Flag | None = Field(default=None)
    duration: str | None = Field(default=None)
    maxRetryTimes: int = Field(default=0)
    retryInterval: int = Field(default=0)
    taskInstancePriority: Priority | None = Field(default=None)
    workflowInstancePriority: Priority | None = Field(default=None)
    workerGroup: str | None = Field(default=None)
    environmentCode: int | None = Field(default=None)
    environmentConfig: str | None = Field(default=None)
    executorId: int = Field(default=0)
    varPool: str | None = Field(default=None)
    executorName: str | None = Field(default=None)
    delayTime: int = Field(default=0)
    taskParams: str | None = Field(default=None)
    dryRun: int = Field(default=0)
    taskGroupId: int = Field(default=0)
    cpuQuota: int | None = Field(default=None)
    memoryMax: int | None = Field(default=None)
    taskExecuteType: TaskExecuteType | None = Field(default=None)

__all__ = ["TaskInstance"]
