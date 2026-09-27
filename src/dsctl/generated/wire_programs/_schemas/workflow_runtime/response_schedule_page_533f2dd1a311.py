from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from typing import Generic, TypeVar
from pydantic import Field
from ....wire_runtime._models import BaseContractModel, BaseEntityModel

T = TypeVar("T")

from ..._schemas._enums.enum_340913ac6c37a9d9d3e68b1d50565a70b364df06a173e4ed945588d246ecf607 import FailureStrategy as FailureStrategy

from ..._schemas._enums.enum_1bfd725ef403a1fdc215a0df82a4cf6f6f57b576f222c5a3efb4b33006134f72 import Priority as Priority

from ..._schemas._enums.enum_22d45c0dad770da81d8b43eb6910f221bc4177bf3bb277d5073b5d81d38dbeb2 import ReleaseState as ReleaseState

from ..._schemas._enums.enum_ed0180176a382ad27a1c0709aa04b1040af0b0f2b1ead67fe29fe7f849923c64 import WarningType as WarningType

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] | None = Field(default=None)
    total: int = Field(default=0)
    totalPage: int | None = Field(default=None)
    currentPage: int | None = Field(default=0)

class Schedule(BaseEntityModel):
    id: int = Field(default=0)
    processDefinitionId: int = Field(default=0)
    processDefinitionName: str | None = Field(default=None)
    projectName: str | None = Field(default=None)
    definitionDescription: str | None = Field(default=None)
    startTime: str | None = Field(default=None)
    endTime: str | None = Field(default=None)
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

class PageInfoSchedule(PageInfo[Schedule]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.dao.entity.Schedule>."""

__all__ = ["FailureStrategy", "Priority", "ReleaseState", "WarningType", "PageInfo", "Schedule", "PageInfoSchedule"]

PageInfo.model_rebuild(_types_namespace=globals())
Schedule.model_rebuild(_types_namespace=globals())
PageInfo[Schedule].model_rebuild(_types_namespace=globals())
PageInfoSchedule.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:47dd7db52c703de356059491bb4a3a672095ee1d98fdec39ba6942eecb99a777'

RESPONSE_TYPE = PageInfoSchedule
EXECUTABLE_SCHEMA_DIGEST = 'sha256:533f2dd1a311f81db0684f61b1462cb0d3ef2eded4a5cc13c298ce93a7d7c6e8'
