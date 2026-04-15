from __future__ import annotations

from pydantic import Field
from ..._models import BaseEntityModel, JsonValue

from ...common.enums.flag import Flag
from ...common.enums.priority import Priority
from ...common.enums.task_execute_type import TaskExecuteType
from ...common.enums.timeout_flag import TimeoutFlag
from ...plugin.task_api.enums.task_timeout_strategy import TaskTimeoutStrategy
from ...plugin.task_api.model.property import Property

class TaskDefinition(BaseEntityModel):
    id: int | None = Field(default=None, description='id')
    code: int = Field(default=0, description='code')
    name: str | None = Field(default=None, description='name')
    version: int = Field(default=0, description='version')
    description: str | None = Field(default=None, description='description')
    projectCode: int = Field(default=0, description='project code')
    userId: int = Field(default=0, description='task user id')
    taskType: str | None = Field(default=None, description='task type')
    taskParams: JsonValue | None = Field(default=None, description='user defined parameters')
    taskParamList: list[Property] | None = Field(default=None, description='user defined parameter list')
    taskParamMap: dict[str, str] | None = Field(default=None, description='user defined parameter map')
    flag: Flag | None = Field(default=None, description='task is valid: yes/no // todo: remove the flag field')
    taskPriority: Priority | None = Field(default=None, description='task priority')
    userName: str | None = Field(default=None, description='user name')
    projectName: str | None = Field(default=None, description='project name')
    workerGroup: str | None = Field(default=None, description='worker group')
    environmentCode: int = Field(default=0, description='environment code')
    failRetryTimes: int = Field(default=0, description='fail retry times')
    failRetryInterval: int = Field(default=0, description='fail retry interval')
    timeoutFlag: TimeoutFlag | None = Field(default=None, description='timeout flag')
    timeoutNotifyStrategy: TaskTimeoutStrategy | None = Field(default=None, description='timeout notify strategy')
    timeout: int = Field(default=0, description='task warning time out. unit: minute')
    delayTime: int = Field(default=0, description='delay execution time.')
    resourceIds: str | None = Field(default=None, description='resource ids we do')
    createTime: str | None = Field(default=None, description='create time')
    updateTime: str | None = Field(default=None, description='update time')
    modifyBy: str | None = Field(default=None, description='modify user name')
    taskGroupId: int = Field(default=0, description='task group id')
    taskGroupPriority: int = Field(default=0, description='task group priority, todo: we should add this field to task instance when create task instance')
    cpuQuota: int | None = Field(default=None, description='cpu quota')
    memoryMax: int | None = Field(default=None, description='max memory')
    taskExecuteType: TaskExecuteType | None = Field(default=None, description='task execute type')

__all__ = ["TaskDefinition"]
