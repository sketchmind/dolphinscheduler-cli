from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from typing import Generic, TypeVar
from pydantic import Field
from ....wire_runtime._models import BaseContractModel, BaseViewModel

T = TypeVar("T")

from ..._schemas._enums.enum_340913ac6c37a9d9d3e68b1d50565a70b364df06a173e4ed945588d246ecf607 import FailureStrategy as FailureStrategy

from ..._schemas._enums.enum_1bfd725ef403a1fdc215a0df82a4cf6f6f57b576f222c5a3efb4b33006134f72 import Priority as Priority

from ..._schemas._enums.enum_22d45c0dad770da81d8b43eb6910f221bc4177bf3bb277d5073b5d81d38dbeb2 import ReleaseState as ReleaseState

from ..._schemas._enums.enum_254fd4c31bb26a513eb44c791479cf168b771e328b7f4a6c97f4b997d539852a import ScheduleMissedFirePolicy as ScheduleMissedFirePolicy

from ..._schemas._enums.enum_ed0180176a382ad27a1c0709aa04b1040af0b0f2b1ead67fe29fe7f849923c64 import WarningType as WarningType

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] = Field(default_factory=list)
    total: int = Field(default=0)
    totalPage: int | None = Field(default=None)
    pageSize: int = Field(default=20)
    currentPage: int | None = Field(default=0)
    pageNo: int | None = Field(default=None)

class ScheduleVO(BaseViewModel):
    id: int = Field(default=0)
    workflowDefinitionCode: int = Field(default=0)
    workflowDefinitionName: str | None = Field(default=None)
    projectName: str | None = Field(default=None)
    definitionDescription: str | None = Field(default=None)
    startTime: str | None = Field(default=None)
    endTime: str | None = Field(default=None)
    timezoneId: str | None = Field(default=None)
    crontab: str | None = Field(default=None)
    missedFirePolicy: ScheduleMissedFirePolicy | None = Field(default=None)
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

class PageInfoScheduleVO(PageInfo[ScheduleVO]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.api.vo.ScheduleVO>."""

__all__ = ["FailureStrategy", "Priority", "ReleaseState", "ScheduleMissedFirePolicy", "WarningType", "PageInfo", "ScheduleVO", "PageInfoScheduleVO"]

PageInfo.model_rebuild(_types_namespace=globals())
ScheduleVO.model_rebuild(_types_namespace=globals())
PageInfo[ScheduleVO].model_rebuild(_types_namespace=globals())
PageInfoScheduleVO.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:5b60ce9f06f6238812935351a835918b4c7e76335d9f11646049a67daaf2462b'

RESPONSE_TYPE = PageInfoScheduleVO
EXECUTABLE_SCHEMA_DIGEST = 'sha256:1f16e6df3f74aa5d0f5a780001077011a9d799ddbf99ef07542753d896e462a2'
