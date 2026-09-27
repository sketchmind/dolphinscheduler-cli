from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from typing import Generic, TypeVar
from pydantic import Field
from ....wire_runtime._models import BaseContractModel, BaseEntityModel

T = TypeVar("T")

from ..._schemas._enums.enum_bc4cd4180cacc5f42d4176ec2d34db870eee919298a123ffedbc690854336877 import CommandType as CommandType

from ..._schemas._enums.enum_78e381705bdbe0474b22ab02487219cbbdb989107b1553b39dbdde5254627fbc import DataType as DataType

from ..._schemas._enums.enum_8d471a35722c2ff88418836345e29fb1aae45407f2106711c1d8a9028772810b import Direct as Direct

from ..._schemas._enums.enum_0fdf6309c5e0ea6bf4f8010f09afb5ea3d42bd51930597daf1a7473802f27eb7 import ExecutionStatus as ExecutionStatus

from ..._schemas._enums.enum_340913ac6c37a9d9d3e68b1d50565a70b364df06a173e4ed945588d246ecf607 import FailureStrategy as FailureStrategy

from ..._schemas._enums.enum_3f648bf0967bf9b023cb4b874fe7000847e14d34036ab1c73d2526b8435d0e3c import Flag as Flag

from ..._schemas._enums.enum_1bfd725ef403a1fdc215a0df82a4cf6f6f57b576f222c5a3efb4b33006134f72 import Priority as Priority

from ..._schemas._enums.enum_22d45c0dad770da81d8b43eb6910f221bc4177bf3bb277d5073b5d81d38dbeb2 import ReleaseState as ReleaseState

from ..._schemas._enums.enum_218e01f94e85b13f51221bb663dd1fce2e8cd502caee65aedb9b8fe9509eda6c import TaskDependType as TaskDependType

from ..._schemas._enums.enum_ed0180176a382ad27a1c0709aa04b1040af0b0f2b1ead67fe29fe7f849923c64 import WarningType as WarningType

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] | None = Field(default=None)
    total: int = Field(default=0)
    totalPage: int | None = Field(default=None)
    currentPage: int | None = Field(default=0)

class ProcessDefinition(BaseEntityModel):
    id: int = Field(default=0)
    name: str | None = Field(default=None)
    version: int = Field(default=0)
    releaseState: ReleaseState | None = Field(default=None)
    projectId: int = Field(default=0)
    processDefinitionJson: str | None = Field(default=None)
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
    connects: str | None = Field(default=None)
    receivers: str | None = Field(default=None)
    receiversCc: str | None = Field(default=None)
    scheduleReleaseState: ReleaseState | None = Field(default=None)
    timeout: int = Field(default=0)
    tenantId: int = Field(default=0)
    modifyBy: str | None = Field(default=None)
    resourceIds: str | None = Field(default=None)

class ProcessInstance(BaseEntityModel):
    id: int = Field(default=0)
    processDefinitionId: int = Field(default=0)
    state: ExecutionStatus | None = Field(default=None)
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
    processInstanceJson: str | None = Field(default=None)
    executorId: int = Field(default=0)
    executorName: str | None = Field(default=None)
    tenantCode: str | None = Field(default=None)
    queue: str | None = Field(default=None)
    isSubProcess: Flag | None = Field(default=None)
    locations: str | None = Field(default=None)
    connects: str | None = Field(default=None)
    historyCmd: str | None = Field(default=None)
    dependenceScheduleTimes: str | None = Field(default=None)
    duration: str | None = Field(default=None)
    processInstancePriority: Priority | None = Field(default=None)
    workerGroup: str | None = Field(default=None)
    timeout: int = Field(default=0)
    tenantId: int = Field(default=0)
    receivers: str | None = Field(default=None)
    receiversCc: str | None = Field(default=None)

class Property(BaseContractModel):
    prop: str | None = Field(default=None)
    direct: Direct | None = Field(default=None)
    type: DataType | None = Field(default=None)
    value: str | None = Field(default=None)

class PageInfoProcessInstance(PageInfo[ProcessInstance]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.dao.entity.ProcessInstance>."""

__all__ = ["CommandType", "DataType", "Direct", "ExecutionStatus", "FailureStrategy", "Flag", "Priority", "ReleaseState", "TaskDependType", "WarningType", "PageInfo", "ProcessDefinition", "ProcessInstance", "Property", "PageInfoProcessInstance"]

PageInfo.model_rebuild(_types_namespace=globals())
ProcessDefinition.model_rebuild(_types_namespace=globals())
ProcessInstance.model_rebuild(_types_namespace=globals())
Property.model_rebuild(_types_namespace=globals())
PageInfo[ProcessInstance].model_rebuild(_types_namespace=globals())
PageInfoProcessInstance.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:1d86e2344124b0ff1ea4c1a624803aabdc74abeec67a4eabd222bc446ccd5c06'

RESPONSE_TYPE = PageInfoProcessInstance
EXECUTABLE_SCHEMA_DIGEST = 'sha256:1dc8237be91032e1a535a539d13c83ca8a69f7dcbc6f232fcbeba102cfd0bb46'
