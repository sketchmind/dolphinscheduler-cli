from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from typing import Generic, TypeVar
from pydantic import Field
from ....wire_runtime._models import BaseContractModel, BaseEntityModel

T = TypeVar("T")

from ..._schemas._enums.enum_4d4785692b8670b354c4d35d14faf3c271fe5144ba0b9d261ee02e66907631a1 import DataType as DataType

from ..._schemas._enums.enum_8d471a35722c2ff88418836345e29fb1aae45407f2106711c1d8a9028772810b import Direct as Direct

from ..._schemas._enums.enum_340913ac6c37a9d9d3e68b1d50565a70b364df06a173e4ed945588d246ecf607 import FailureStrategy as FailureStrategy

from ..._schemas._enums.enum_3f648bf0967bf9b023cb4b874fe7000847e14d34036ab1c73d2526b8435d0e3c import Flag as Flag

from ..._schemas._enums.enum_1bfd725ef403a1fdc215a0df82a4cf6f6f57b576f222c5a3efb4b33006134f72 import Priority as Priority

from ..._schemas._enums.enum_c7ef46fe4ab574cc54a5e52a08773a1bb7b934f4fe11274c94f950c6425066b1 import ProcessExecutionTypeEnum as ProcessExecutionTypeEnum

from ..._schemas._enums.enum_22d45c0dad770da81d8b43eb6910f221bc4177bf3bb277d5073b5d81d38dbeb2 import ReleaseState as ReleaseState

from ..._schemas._enums.enum_878d4d11e37dab4c1fa1553fc63169e22641320fdd5253b5ae993e948f231c32 import WarningType as WarningType

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
    schedule: Schedule | None = Field(default=None)
    timeout: int = Field(default=0)
    modifyBy: str | None = Field(default=None)
    warningGroupId: int | None = Field(default=None)
    executionType: ProcessExecutionTypeEnum | None = Field(default=None)

class Property(BaseContractModel):
    prop: str | None = Field(default=None)
    direct: Direct | None = Field(default=None)
    type: DataType | None = Field(default=None)
    value: str | None = Field(default=None)

class Schedule(BaseEntityModel):
    id: int | None = Field(default=None)
    processDefinitionCode: int = Field(default=0)
    processDefinitionName: str | None = Field(default=None)
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
    processInstancePriority: Priority | None = Field(default=None)
    workerGroup: str | None = Field(default=None)
    tenantCode: str | None = Field(default=None)
    environmentCode: int | None = Field(default=None)
    environmentName: str | None = Field(default=None)

class PageInfoProcessDefinition(PageInfo[ProcessDefinition]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.dao.entity.ProcessDefinition>."""

__all__ = ["DataType", "Direct", "FailureStrategy", "Flag", "Priority", "ProcessExecutionTypeEnum", "ReleaseState", "WarningType", "PageInfo", "ProcessDefinition", "Property", "Schedule", "PageInfoProcessDefinition"]

PageInfo.model_rebuild(_types_namespace=globals())
ProcessDefinition.model_rebuild(_types_namespace=globals())
Property.model_rebuild(_types_namespace=globals())
Schedule.model_rebuild(_types_namespace=globals())
PageInfo[ProcessDefinition].model_rebuild(_types_namespace=globals())
PageInfoProcessDefinition.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:27357a8cc12d8ba6a3f00e9f400df6b0ae40fd95c0600c4f452c3fe1ea95d35e'

RESPONSE_TYPE = PageInfoProcessDefinition
EXECUTABLE_SCHEMA_DIGEST = 'sha256:62171f2b659ed14390c6e4de0174bd43092ec649e91b9127e6e88452172a6c4b'
