from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_aa87095abf892a674d9bc9d6e4fba7691e04e734501b487ccbaee365f84b47e3 import ExecutionStatus as ExecutionStatus

__all__ = ["ExecutionStatus"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class TaskInstancePageParams(BaseParamsModel):
    projectCode: int
    processInstanceId: int | None = Field(default=0)
    processInstanceName: str | None = Field(default=None)
    searchVal: str | None = Field(default=None)
    taskName: str | None = Field(default=None)
    executorName: str | None = Field(default=None)
    stateType: ExecutionStatus | None = Field(default=None)
    host: str | None = Field(default=None)
    startDate: str | None = Field(default=None)
    endDate: str | None = Field(default=None)
    pageNo: int
    pageSize: int

SOURCE_CLOSURE_DIGEST = 'sha256:2ca8b051b99ac014839e659bf943b4341805b15618a5f56be1ef06b9e1a80024'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:d7ef2ac8f2479cfa042fc228a55a4b73d6c3ffcb94a1f3f010921b7c1810e593'
