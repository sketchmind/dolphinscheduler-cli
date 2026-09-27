from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from typing import Generic, TypeVar
from pydantic import Field, StrictInt
from ....wire_runtime._models import BaseContractModel, BaseEntityModel, JsonValue

T = TypeVar("T")

from ..._schemas._enums.enum_20d207118e9e893582e7f5cc3b5726390f0180559edac951673652b3125d5d9f import DataType as DataType

from ..._schemas._enums.enum_8d471a35722c2ff88418836345e29fb1aae45407f2106711c1d8a9028772810b import Direct as Direct

from ..._schemas._enums.enum_3f648bf0967bf9b023cb4b874fe7000847e14d34036ab1c73d2526b8435d0e3c import Flag as Flag

from ..._schemas._enums.enum_1bfd725ef403a1fdc215a0df82a4cf6f6f57b576f222c5a3efb4b33006134f72 import Priority as Priority

from ..._schemas._enums.enum_3a17a111adfcb3b03b5bb8b3bc2077e935962ed2aa46b0bfcd4a9d73503fb450 import TaskExecuteType as TaskExecuteType

from ..._schemas._enums.enum_3d9abde8e14b8445a445e9f5874c2ca80291bdbd2d99e22a1660f59c6622f8ce import TaskTimeoutStrategy as TaskTimeoutStrategy

from ..._schemas._enums.enum_144f476510d97dd7a4229cc12308cecdd8bbfc0751f2ad4b74754f663c1ecd92 import TimeoutFlag as TimeoutFlag

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] = Field(default_factory=list)
    total: StrictInt = Field(default=0)
    totalPage: StrictInt | None = Field(default=None)
    pageSize: StrictInt = Field(default=20)
    currentPage: StrictInt | None = Field(default=0)
    pageNo: StrictInt | None = Field(default=None)

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

class TaskDefinitionLog(TaskDefinition):
    operator: int = Field(default=0)
    operateTime: str | None = Field(default=None)

class PageInfoTaskDefinitionLog(PageInfo[TaskDefinitionLog]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.dao.entity.TaskDefinitionLog>."""

__all__ = ["DataType", "Direct", "Flag", "Priority", "TaskExecuteType", "TaskTimeoutStrategy", "TimeoutFlag", "PageInfo", "Property", "TaskDefinition", "TaskDefinitionLog", "PageInfoTaskDefinitionLog"]

PageInfo.model_rebuild(_types_namespace=globals())
Property.model_rebuild(_types_namespace=globals())
TaskDefinition.model_rebuild(_types_namespace=globals())
TaskDefinitionLog.model_rebuild(_types_namespace=globals())
PageInfo[TaskDefinitionLog].model_rebuild(_types_namespace=globals())
PageInfoTaskDefinitionLog.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:80658812be8859557ad23276b102544e3677dea56b0ed52f2e71ef31a54de142'

RESPONSE_TYPE = PageInfoTaskDefinitionLog
EXECUTABLE_SCHEMA_DIGEST = 'sha256:f2a61a3c44e6cda17fb60026903ee293e51c2edebd034a889109a4ba9daf689a'
