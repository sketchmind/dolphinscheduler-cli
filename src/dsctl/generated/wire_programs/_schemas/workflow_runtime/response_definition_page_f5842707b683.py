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

from ..._schemas._enums.enum_22d45c0dad770da81d8b43eb6910f221bc4177bf3bb277d5073b5d81d38dbeb2 import ReleaseState as ReleaseState

from ..._schemas._enums.enum_254fd4c31bb26a513eb44c791479cf168b771e328b7f4a6c97f4b997d539852a import ScheduleMissedFirePolicy as ScheduleMissedFirePolicy

from ..._schemas._enums.enum_ed0180176a382ad27a1c0709aa04b1040af0b0f2b1ead67fe29fe7f849923c64 import WarningType as WarningType

from ..._schemas._enums.enum_32fce7bcae4edc5d12b75ac8481cae7142bc5af424dbf5e4d1fe66f96a7dad38 import WorkflowExecutionTypeEnum as WorkflowExecutionTypeEnum

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] = Field(default_factory=list)
    total: int = Field(default=0)
    totalPage: int | None = Field(default=None)
    pageSize: int = Field(default=20)
    currentPage: int | None = Field(default=0)
    pageNo: int | None = Field(default=None)

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

class PageInfoWorkflowDefinition(PageInfo[WorkflowDefinition]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.dao.entity.WorkflowDefinition>."""

__all__ = ["DataType", "Direct", "FailureStrategy", "Flag", "Priority", "ReleaseState", "ScheduleMissedFirePolicy", "WarningType", "WorkflowExecutionTypeEnum", "PageInfo", "Property", "Schedule", "WorkflowDefinition", "PageInfoWorkflowDefinition"]

PageInfo.model_rebuild(_types_namespace=globals())
Property.model_rebuild(_types_namespace=globals())
Schedule.model_rebuild(_types_namespace=globals())
WorkflowDefinition.model_rebuild(_types_namespace=globals())
PageInfo[WorkflowDefinition].model_rebuild(_types_namespace=globals())
PageInfoWorkflowDefinition.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:e1402f5c25acc7535708cdee25ea8b4286273e3cbeadc7cdfc774924812c4bc7'

RESPONSE_TYPE = PageInfoWorkflowDefinition
EXECUTABLE_SCHEMA_DIGEST = 'sha256:f5842707b6837aa0a5e37a577b3fbf5c4b18d2792faaf9e32c41a107c28858d7'
