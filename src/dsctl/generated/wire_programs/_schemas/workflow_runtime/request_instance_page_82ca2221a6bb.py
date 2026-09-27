from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_e1515f2a772ca85e259dffd346f9bfa902dad2906cf77715ad525bbbfd143483 import WorkflowExecutionStatus as WorkflowExecutionStatus

__all__ = ["WorkflowExecutionStatus"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class InstancePageParams(BaseParamsModel):
    projectCode: int
    workflowDefinitionCode: int | None = Field(default=0)
    searchVal: str | None = Field(default=None)
    executorName: str | None = Field(default=None)
    stateType: WorkflowExecutionStatus | None = Field(default=None)
    host: str | None = Field(default=None)
    startDate: str | None = Field(default=None)
    endDate: str | None = Field(default=None)
    otherParamsJson: str | None = Field(default=None)
    pageNo: int
    pageSize: int

SOURCE_CLOSURE_DIGEST = 'sha256:a4118e8b128902098923eb4a6ae3782dd32412fbc66ab956c32bf15b745653c1'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:82ca2221a6bb6bc01cde1547c4f78e349d796c5385a64a850e916b0822996db8'
