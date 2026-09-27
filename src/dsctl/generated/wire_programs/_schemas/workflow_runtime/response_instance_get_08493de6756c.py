from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from pydantic import Field
from ....wire_runtime._models import BaseContractModel, BaseEntityModel, JsonValue

from ..._schemas._enums.enum_1d0388109afa88441a126d74b86dd2decf5558fc310e18a984a1514545248bfa import CommandType as CommandType

from ..._schemas._enums.enum_88d1c8623f27f59f427a832df8a372e59c1c1b21ef29e0d04cbccb86f26c3992 import ConditionType as ConditionType

from ..._schemas._enums.enum_4d4785692b8670b354c4d35d14faf3c271fe5144ba0b9d261ee02e66907631a1 import DataType as DataType

from ..._schemas._enums.enum_8d471a35722c2ff88418836345e29fb1aae45407f2106711c1d8a9028772810b import Direct as Direct

from ..._schemas._enums.enum_340913ac6c37a9d9d3e68b1d50565a70b364df06a173e4ed945588d246ecf607 import FailureStrategy as FailureStrategy

from ..._schemas._enums.enum_3f648bf0967bf9b023cb4b874fe7000847e14d34036ab1c73d2526b8435d0e3c import Flag as Flag

from ..._schemas._enums.enum_1bfd725ef403a1fdc215a0df82a4cf6f6f57b576f222c5a3efb4b33006134f72 import Priority as Priority

from ..._schemas._enums.enum_22d45c0dad770da81d8b43eb6910f221bc4177bf3bb277d5073b5d81d38dbeb2 import ReleaseState as ReleaseState

from ..._schemas._enums.enum_218e01f94e85b13f51221bb663dd1fce2e8cd502caee65aedb9b8fe9509eda6c import TaskDependType as TaskDependType

from ..._schemas._enums.enum_3a17a111adfcb3b03b5bb8b3bc2077e935962ed2aa46b0bfcd4a9d73503fb450 import TaskExecuteType as TaskExecuteType

from ..._schemas._enums.enum_3d9abde8e14b8445a445e9f5874c2ca80291bdbd2d99e22a1660f59c6622f8ce import TaskTimeoutStrategy as TaskTimeoutStrategy

from ..._schemas._enums.enum_144f476510d97dd7a4229cc12308cecdd8bbfc0751f2ad4b74754f663c1ecd92 import TimeoutFlag as TimeoutFlag

from ..._schemas._enums.enum_ed0180176a382ad27a1c0709aa04b1040af0b0f2b1ead67fe29fe7f849923c64 import WarningType as WarningType

from ..._schemas._enums.enum_65a98f45fef9d2238bc307a44e09cf80e2e00807dffbb6786119a150441997c8 import WorkflowExecutionStatus as WorkflowExecutionStatus

from ..._schemas._enums.enum_32fce7bcae4edc5d12b75ac8481cae7142bc5af424dbf5e4d1fe66f96a7dad38 import WorkflowExecutionTypeEnum as WorkflowExecutionTypeEnum

class DagData(BaseEntityModel):
    workflowDefinition: WorkflowDefinition | None = Field(default=None)
    workflowTaskRelationList: list[WorkflowTaskRelation] | None = Field(default=None)
    taskDefinitionList: list[TaskDefinition] | None = Field(default=None)

class Property(BaseContractModel):
    prop: str | None = Field(default=None)
    direct: Direct | None = Field(default=None)
    type: DataType | None = Field(default=None)
    value: str | None = Field(default=None)

class Schedule(BaseEntityModel):
    id: int | None = Field(default=None)
    workflowDefinitionCode: int = Field(default=0)
    workflowDefinitionName: str | None = Field(default=None)
    projectName: str | None = Field(default=None)
    definitionDescription: str | None = Field(default=None)
    startTime: str | None = Field(default=None)
    endTime: str | None = Field(default=None)
    timezoneId: str | None = Field(default=None)
    crontab: str | None = Field(default=None)
    failureStrategy: FailureStrategy | None = Field(default=None)
    warningType: WarningType | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)
    userId: int = Field(default=0)
    userName: str | None = Field(default=None)
    releaseState: ReleaseState | None = Field(default=None)
    warningGroupId: int = Field(default=0)
    workflowInstancePriority: Priority | None = Field(default=None)
    workerGroup: str | None = Field(default=None)
    tenantCode: str | None = Field(default=None)
    environmentCode: int | None = Field(default=None)
    environmentName: str | None = Field(default=None)

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

class WorkflowDefinition(BaseEntityModel):
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
    schedule: Schedule | None = Field(default=None)
    timeout: int = Field(default=0)
    modifyBy: str | None = Field(default=None)
    warningGroupId: int | None = Field(default=None)
    executionType: WorkflowExecutionTypeEnum | None = Field(default=None)

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
    globalParams: str | None = Field(default=None)
    dagData: DagData | None = Field(default=None)
    executorId: int = Field(default=0)
    executorName: str | None = Field(default=None)
    tenantCode: str | None = Field(default=None)
    queue: str | None = Field(default=None)
    isSubWorkflow: Flag | None = Field(default=None)
    locations: str | None = Field(default=None)
    historyCmd: str | None = Field(default=None)
    dependenceScheduleTimes: str | None = Field(default=None)
    duration: str | None = Field(default=None)
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

class WorkflowTaskRelation(BaseEntityModel):
    id: int | None = Field(default=None)
    name: str | None = Field(default=None)
    workflowDefinitionVersion: int = Field(default=0)
    projectCode: int = Field(default=0)
    workflowDefinitionCode: int = Field(default=0)
    preTaskCode: int = Field(default=0)
    preTaskVersion: int = Field(default=0)
    postTaskCode: int = Field(default=0)
    postTaskVersion: int = Field(default=0)
    conditionType: ConditionType | None = Field(default=None)
    conditionParams: JsonValue | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)

__all__ = ["CommandType", "ConditionType", "DataType", "Direct", "FailureStrategy", "Flag", "Priority", "ReleaseState", "TaskDependType", "TaskExecuteType", "TaskTimeoutStrategy", "TimeoutFlag", "WarningType", "WorkflowExecutionStatus", "WorkflowExecutionTypeEnum", "DagData", "Property", "Schedule", "TaskDefinition", "WorkflowDefinition", "WorkflowInstance", "WorkflowInstanceStateDesc", "WorkflowTaskRelation"]

DagData.model_rebuild(_types_namespace=globals())
Property.model_rebuild(_types_namespace=globals())
Schedule.model_rebuild(_types_namespace=globals())
TaskDefinition.model_rebuild(_types_namespace=globals())
WorkflowDefinition.model_rebuild(_types_namespace=globals())
WorkflowInstance.model_rebuild(_types_namespace=globals())
WorkflowInstanceStateDesc.model_rebuild(_types_namespace=globals())
WorkflowTaskRelation.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:dfd70cf90c4d14ad00efcd1eeb57a6a732ca12ece634cb2bdb7db01c89652d19'

RESPONSE_TYPE = WorkflowInstance
EXECUTABLE_SCHEMA_DIGEST = 'sha256:08493de6756c84d83a8a1dc79bdfb8796673e81043700094fc8f5387111abcaa'
