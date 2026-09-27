from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from typing import Generic, TypeVar
from pydantic import Field
from ....wire_runtime._models import BaseContractModel, BaseEntityModel, JsonValue

T = TypeVar("T")

from ..._schemas._enums.enum_c11d3edddb73e44c6b6bd8e6c14388d4bfbebf321256ace15cce422c39e3e2d4 import CommandType as CommandType

from ..._schemas._enums.enum_88d1c8623f27f59f427a832df8a372e59c1c1b21ef29e0d04cbccb86f26c3992 import ConditionType as ConditionType

from ..._schemas._enums.enum_4d4785692b8670b354c4d35d14faf3c271fe5144ba0b9d261ee02e66907631a1 import DataType as DataType

from ..._schemas._enums.enum_8d471a35722c2ff88418836345e29fb1aae45407f2106711c1d8a9028772810b import Direct as Direct

from ..._schemas._enums.enum_340913ac6c37a9d9d3e68b1d50565a70b364df06a173e4ed945588d246ecf607 import FailureStrategy as FailureStrategy

from ..._schemas._enums.enum_3f648bf0967bf9b023cb4b874fe7000847e14d34036ab1c73d2526b8435d0e3c import Flag as Flag

from ..._schemas._enums.enum_1bfd725ef403a1fdc215a0df82a4cf6f6f57b576f222c5a3efb4b33006134f72 import Priority as Priority

from ..._schemas._enums.enum_c7ef46fe4ab574cc54a5e52a08773a1bb7b934f4fe11274c94f950c6425066b1 import ProcessExecutionTypeEnum as ProcessExecutionTypeEnum

from ..._schemas._enums.enum_22d45c0dad770da81d8b43eb6910f221bc4177bf3bb277d5073b5d81d38dbeb2 import ReleaseState as ReleaseState

from ..._schemas._enums.enum_218e01f94e85b13f51221bb663dd1fce2e8cd502caee65aedb9b8fe9509eda6c import TaskDependType as TaskDependType

from ..._schemas._enums.enum_3a17a111adfcb3b03b5bb8b3bc2077e935962ed2aa46b0bfcd4a9d73503fb450 import TaskExecuteType as TaskExecuteType

from ..._schemas._enums.enum_3d9abde8e14b8445a445e9f5874c2ca80291bdbd2d99e22a1660f59c6622f8ce import TaskTimeoutStrategy as TaskTimeoutStrategy

from ..._schemas._enums.enum_144f476510d97dd7a4229cc12308cecdd8bbfc0751f2ad4b74754f663c1ecd92 import TimeoutFlag as TimeoutFlag

from ..._schemas._enums.enum_ed0180176a382ad27a1c0709aa04b1040af0b0f2b1ead67fe29fe7f849923c64 import WarningType as WarningType

from ..._schemas._enums.enum_018b010393bb1ccc2cc54631042c1b82fe5379e437dea889308446c48fa0e6ae import WorkflowExecutionStatus as WorkflowExecutionStatus

class DagData(BaseEntityModel):
    processDefinition: ProcessDefinition | None = Field(default=None)
    processTaskRelationList: list[ProcessTaskRelation] | None = Field(default=None)
    taskDefinitionList: list[TaskDefinition] | None = Field(default=None)

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] = Field(default_factory=list)
    total: int = Field(default=0)
    totalPage: int | None = Field(default=None)
    pageSize: int = Field(default=20)
    currentPage: int | None = Field(default=0)
    pageNo: int | None = Field(default=None)

class ProcessDefinition(BaseEntityModel):
    id: int | None = Field(default=None)
    code: int = Field(default=0)
    name: str | None = Field(default=None)
    version: int = Field(default=0)
    releaseState: ReleaseState | None = Field(default=None)
    projectCode: int = Field(default=0)
    description: str | None = Field(default=None)
    globalParams: str | None = Field(default=None)
    globalParamList: list[Property] | None = Field(default=None)
    globalParamMap: dict[str, str] | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)
    flag: Flag | None = Field(default=None)
    userId: int = Field(default=0)
    userName: str | None = Field(default=None)
    projectName: str | None = Field(default=None)
    locations: str | None = Field(default=None)
    scheduleReleaseState: ReleaseState | None = Field(default=None)
    timeout: int = Field(default=0)
    modifyBy: str | None = Field(default=None)
    warningGroupId: int | None = Field(default=None)
    executionType: ProcessExecutionTypeEnum | None = Field(default=None)

class ProcessInstance(BaseEntityModel):
    id: int | None = Field(default=None)
    processDefinitionCode: int | None = Field(default=None)
    processDefinitionVersion: int = Field(default=0)
    projectCode: int | None = Field(default=None)
    state: WorkflowExecutionStatus | None = Field(default=None)
    stateHistory: str | None = Field(default=None)
    stateDescList: list[ProcessInstanceStateDesc] | None = Field(default=None)
    recovery: Flag | None = Field(default=None)
    startTime: str | None = Field(default=None)
    endTime: str | None = Field(default=None)
    runTimes: int = Field(default=0)
    name: str | None = Field(default=None)
    host: str | None = Field(default=None)
    processDefinition: ProcessDefinition | None = Field(default=None)
    commandType: CommandType | None = Field(default=None)
    commandParam: str | None = Field(default=None)
    taskDependType: TaskDependType | None = Field(default=None)
    maxTryTimes: int = Field(default=0)
    failureStrategy: FailureStrategy | None = Field(default=None)
    warningType: WarningType | None = Field(default=None)
    warningGroupId: int | None = Field(default=None)
    scheduleTime: str | None = Field(default=None)
    commandStartTime: str | None = Field(default=None)
    globalParams: str | None = Field(default=None)
    dagData: DagData | None = Field(default=None)
    executorId: int = Field(default=0)
    executorName: str | None = Field(default=None)
    tenantCode: str | None = Field(default=None)
    queue: str | None = Field(default=None)
    isSubProcess: Flag | None = Field(default=None)
    locations: str | None = Field(default=None)
    historyCmd: str | None = Field(default=None)
    dependenceScheduleTimes: str | None = Field(default=None)
    duration: str | None = Field(default=None)
    processInstancePriority: Priority | None = Field(default=None)
    workerGroup: str | None = Field(default=None)
    environmentCode: int | None = Field(default=None)
    timeout: int = Field(default=0)
    varPool: str | None = Field(default=None)
    nextProcessInstanceId: int = Field(default=0)
    dryRun: int = Field(default=0)
    restartTime: str | None = Field(default=None)
    isBlocked: bool = Field(default=False, alias='blocked')
    testFlag: int = Field(default=0)

class ProcessInstanceStateDesc(BaseEntityModel):
    time: str | None = Field(default=None)
    state: WorkflowExecutionStatus | None = Field(default=None)
    desc: str | None = Field(default=None)

class ProcessTaskRelation(BaseEntityModel):
    id: int | None = Field(default=None)
    name: str | None = Field(default=None)
    processDefinitionVersion: int = Field(default=0)
    projectCode: int = Field(default=0)
    processDefinitionCode: int = Field(default=0)
    preTaskCode: int = Field(default=0)
    preTaskVersion: int = Field(default=0)
    postTaskCode: int = Field(default=0)
    postTaskVersion: int = Field(default=0)
    conditionType: ConditionType | None = Field(default=None)
    conditionParams: JsonValue | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)

class Property(BaseContractModel):
    prop: str | None = Field(default=None)
    direct: Direct | None = Field(default=None)
    type: DataType | None = Field(default=None)
    value: str | None = Field(default=None)

class TaskDefinition(BaseEntityModel):
    id: int | None = Field(default=None)
    code: int = Field(default=0)
    name: str | None = Field(default=None)
    version: int = Field(default=0)
    description: str | None = Field(default=None)
    projectCode: int = Field(default=0)
    userId: int = Field(default=0)
    taskType: str | None = Field(default=None)
    taskParams: JsonValue | None = Field(default=None)
    taskParamList: list[Property] | None = Field(default=None)
    taskParamMap: dict[str, str | None] | None = Field(default=None)
    flag: Flag | None = Field(default=None)
    isCache: Flag | None = Field(default=None)
    taskPriority: Priority | None = Field(default=None)
    userName: str | None = Field(default=None)
    projectName: str | None = Field(default=None)
    workerGroup: str | None = Field(default=None)
    environmentCode: int = Field(default=0)
    failRetryTimes: int = Field(default=0)
    failRetryInterval: int = Field(default=0)
    timeoutFlag: TimeoutFlag | None = Field(default=None)
    timeoutNotifyStrategy: TaskTimeoutStrategy | None = Field(default=None)
    timeout: int = Field(default=0)
    delayTime: int = Field(default=0)
    resourceIds: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)
    modifyBy: str | None = Field(default=None)
    taskGroupId: int = Field(default=0)
    taskGroupPriority: int = Field(default=0)
    cpuQuota: int | None = Field(default=None)
    memoryMax: int | None = Field(default=None)
    taskExecuteType: TaskExecuteType | None = Field(default=None)

class PageInfoProcessInstance(PageInfo[ProcessInstance]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.dao.entity.ProcessInstance>."""

__all__ = ["CommandType", "ConditionType", "DataType", "Direct", "FailureStrategy", "Flag", "Priority", "ProcessExecutionTypeEnum", "ReleaseState", "TaskDependType", "TaskExecuteType", "TaskTimeoutStrategy", "TimeoutFlag", "WarningType", "WorkflowExecutionStatus", "DagData", "PageInfo", "ProcessDefinition", "ProcessInstance", "ProcessInstanceStateDesc", "ProcessTaskRelation", "Property", "TaskDefinition", "PageInfoProcessInstance"]

DagData.model_rebuild(_types_namespace=globals())
PageInfo.model_rebuild(_types_namespace=globals())
ProcessDefinition.model_rebuild(_types_namespace=globals())
ProcessInstance.model_rebuild(_types_namespace=globals())
ProcessInstanceStateDesc.model_rebuild(_types_namespace=globals())
ProcessTaskRelation.model_rebuild(_types_namespace=globals())
Property.model_rebuild(_types_namespace=globals())
TaskDefinition.model_rebuild(_types_namespace=globals())
PageInfo[ProcessInstance].model_rebuild(_types_namespace=globals())
PageInfoProcessInstance.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:536ca2ad0f5e1f49043f1c4685192e2ebd6497564cd734980ad62c70a28d9597'

RESPONSE_TYPE = PageInfoProcessInstance
EXECUTABLE_SCHEMA_DIGEST = 'sha256:80d1b1d71e3d7f8da65e9f2ade8dae89e2776f8677bd5c2e30229e1c4e225377'
