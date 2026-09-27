from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_018b010393bb1ccc2cc54631042c1b82fe5379e437dea889308446c48fa0e6ae import WorkflowExecutionStatus as WorkflowExecutionStatus

__all__ = ["WorkflowExecutionStatus"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class InstancePageParams(BaseParamsModel):
    projectCode: int
    processDefineCode: int | None = Field(default=0)
    searchVal: str | None = Field(default=None)
    executorName: str | None = Field(default=None)
    stateType: WorkflowExecutionStatus | None = Field(default=None)
    host: str | None = Field(default=None)
    startDate: str | None = Field(default=None)
    endDate: str | None = Field(default=None)
    otherParamsJson: str | None = Field(default=None)
    pageNo: int
    pageSize: int

SOURCE_CLOSURE_DIGEST = 'sha256:e9c78b24324989e53684c62a6a7007ff1b68f36dfb63d6d8ea94d2fcf3a23c42'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:19b304421bbb57610541b2ccf53f44706918946f7fb73b60af9591bc3170d79c'
