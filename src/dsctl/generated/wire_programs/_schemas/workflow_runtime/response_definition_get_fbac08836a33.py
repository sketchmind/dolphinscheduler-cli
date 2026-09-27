from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from pydantic import Field, StrictInt
from ....wire_runtime._models import BaseContractModel, BaseEntityModel, JsonValue

from ..._schemas._enums.enum_88d1c8623f27f59f427a832df8a372e59c1c1b21ef29e0d04cbccb86f26c3992 import ConditionType as ConditionType

from ..._schemas._enums.enum_20d207118e9e893582e7f5cc3b5726390f0180559edac951673652b3125d5d9f import DataType as DataType

from ..._schemas._enums.enum_8d471a35722c2ff88418836345e29fb1aae45407f2106711c1d8a9028772810b import Direct as Direct

from ..._schemas._enums.enum_3f648bf0967bf9b023cb4b874fe7000847e14d34036ab1c73d2526b8435d0e3c import Flag as Flag

from ..._schemas._enums.enum_1bfd725ef403a1fdc215a0df82a4cf6f6f57b576f222c5a3efb4b33006134f72 import Priority as Priority

from ..._schemas._enums.enum_c7ef46fe4ab574cc54a5e52a08773a1bb7b934f4fe11274c94f950c6425066b1 import ProcessExecutionTypeEnum as ProcessExecutionTypeEnum

from ..._schemas._enums.enum_22d45c0dad770da81d8b43eb6910f221bc4177bf3bb277d5073b5d81d38dbeb2 import ReleaseState as ReleaseState

from ..._schemas._enums.enum_3a17a111adfcb3b03b5bb8b3bc2077e935962ed2aa46b0bfcd4a9d73503fb450 import TaskExecuteType as TaskExecuteType

from ..._schemas._enums.enum_3d9abde8e14b8445a445e9f5874c2ca80291bdbd2d99e22a1660f59c6622f8ce import TaskTimeoutStrategy as TaskTimeoutStrategy

from ..._schemas._enums.enum_144f476510d97dd7a4229cc12308cecdd8bbfc0751f2ad4b74754f663c1ecd92 import TimeoutFlag as TimeoutFlag

class DagData(BaseEntityModel):
    processDefinition: ProcessDefinition | None = Field(default=None)
    processTaskRelationList: list[ProcessTaskRelation] | None = Field(default=None)
    taskDefinitionList: list[TaskDefinition] | None = Field(default=None)

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
    tenantId: int = Field(default=0)
    tenantCode: str | None = Field(default=None)
    modifyBy: str | None = Field(default=None)
    warningGroupId: int = Field(default=0)
    executionType: ProcessExecutionTypeEnum | None = Field(default=None)

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
    code: StrictInt = Field(default=0)
    name: str | None = Field(default=None)
    version: StrictInt = Field(default=0)
    description: str | None = Field(default=None)
    projectCode: StrictInt = Field(default=0)
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

__all__ = ["ConditionType", "DataType", "Direct", "Flag", "Priority", "ProcessExecutionTypeEnum", "ReleaseState", "TaskExecuteType", "TaskTimeoutStrategy", "TimeoutFlag", "DagData", "ProcessDefinition", "ProcessTaskRelation", "Property", "TaskDefinition"]

DagData.model_rebuild(_types_namespace=globals())
ProcessDefinition.model_rebuild(_types_namespace=globals())
ProcessTaskRelation.model_rebuild(_types_namespace=globals())
Property.model_rebuild(_types_namespace=globals())
TaskDefinition.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:14f716ca6d28130b17c0e0ab98f5084d5f218c37d731e6ea80b2b005cf0c729c'

RESPONSE_TYPE = DagData
EXECUTABLE_SCHEMA_DIGEST = 'sha256:fbac08836a33c64536b8de56c4ea79488546f1bcda2424c3fee0994dcfde87f1'
