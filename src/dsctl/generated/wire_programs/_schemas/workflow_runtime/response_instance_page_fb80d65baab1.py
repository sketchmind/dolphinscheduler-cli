from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from typing import Generic, TypeVar
from pydantic import Field
from ....wire_runtime._models import BaseContractModel, BaseViewModel

T = TypeVar("T")

from ..._schemas._enums.enum_1d0388109afa88441a126d74b86dd2decf5558fc310e18a984a1514545248bfa import CommandType as CommandType

from ..._schemas._enums.enum_340913ac6c37a9d9d3e68b1d50565a70b364df06a173e4ed945588d246ecf607 import FailureStrategy as FailureStrategy

from ..._schemas._enums.enum_3f648bf0967bf9b023cb4b874fe7000847e14d34036ab1c73d2526b8435d0e3c import Flag as Flag

from ..._schemas._enums.enum_1bfd725ef403a1fdc215a0df82a4cf6f6f57b576f222c5a3efb4b33006134f72 import Priority as Priority

from ..._schemas._enums.enum_218e01f94e85b13f51221bb663dd1fce2e8cd502caee65aedb9b8fe9509eda6c import TaskDependType as TaskDependType

from ..._schemas._enums.enum_ed0180176a382ad27a1c0709aa04b1040af0b0f2b1ead67fe29fe7f849923c64 import WarningType as WarningType

from ..._schemas._enums.enum_e1515f2a772ca85e259dffd346f9bfa902dad2906cf77715ad525bbbfd143483 import WorkflowExecutionStatus as WorkflowExecutionStatus

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] = Field(default_factory=list)
    total: int = Field(default=0)
    totalPage: int | None = Field(default=None)
    pageSize: int = Field(default=20)
    currentPage: int | None = Field(default=0)
    pageNo: int | None = Field(default=None)

class WorkflowInstanceSummaryVO(BaseViewModel):
    id: int | None = Field(default=None)
    workflowDefinitionCode: int | None = Field(default=None)
    workflowDefinitionVersion: int = Field(default=0)
    projectCode: int | None = Field(default=None)
    state: WorkflowExecutionStatus | None = Field(default=None)
    recovery: Flag | None = Field(default=None)
    startTime: str | None = Field(default=None)
    endTime: str | None = Field(default=None)
    runTimes: int = Field(default=0)
    name: str | None = Field(default=None)
    host: str | None = Field(default=None)
    commandType: CommandType | None = Field(default=None)
    taskDependType: TaskDependType | None = Field(default=None)
    maxTryTimes: int = Field(default=0)
    failureStrategy: FailureStrategy | None = Field(default=None)
    warningType: WarningType | None = Field(default=None)
    warningGroupId: int | None = Field(default=None)
    scheduleTime: str | None = Field(default=None)
    commandStartTime: str | None = Field(default=None)
    isSubWorkflow: Flag | None = Field(default=None)
    executorId: int = Field(default=0)
    executorName: str | None = Field(default=None)
    workflowInstancePriority: Priority | None = Field(default=None)
    workerGroup: str | None = Field(default=None)
    environmentCode: int | None = Field(default=None)
    timeout: int = Field(default=0)
    tenantCode: str | None = Field(default=None)
    dryRun: int = Field(default=0)
    nextWorkflowInstanceId: int = Field(default=0)
    restartTime: str | None = Field(default=None)
    duration: str | None = Field(default=None)

class PageInfoWorkflowInstanceSummaryVO(PageInfo[WorkflowInstanceSummaryVO]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.api.vo.WorkflowInstanceSummaryVO>."""

__all__ = ["CommandType", "FailureStrategy", "Flag", "Priority", "TaskDependType", "WarningType", "WorkflowExecutionStatus", "PageInfo", "WorkflowInstanceSummaryVO", "PageInfoWorkflowInstanceSummaryVO"]

PageInfo.model_rebuild(_types_namespace=globals())
WorkflowInstanceSummaryVO.model_rebuild(_types_namespace=globals())
PageInfo[WorkflowInstanceSummaryVO].model_rebuild(_types_namespace=globals())
PageInfoWorkflowInstanceSummaryVO.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:c519881dc3c44878750b986aa3aeb964539eb007d0a968b3445439b03de6f1c7'

RESPONSE_TYPE = PageInfoWorkflowInstanceSummaryVO
EXECUTABLE_SCHEMA_DIGEST = 'sha256:fb80d65baab1178473b751fc3d4f34aca8875819b13c99d260e1cd346aae0f6e'
