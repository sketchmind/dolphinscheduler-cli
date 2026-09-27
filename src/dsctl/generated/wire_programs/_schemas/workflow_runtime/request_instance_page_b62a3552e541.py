from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_0f7108f98b8a7144b2b01aaf20f364e7e549f49bcda73389d4255622bf4b9be3 import ExecutionStatus as ExecutionStatus

__all__ = ["ExecutionStatus"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class InstancePageParams(BaseParamsModel):
    projectCode: int
    processDefineCode: int | None = Field(default=0)
    searchVal: str | None = Field(default=None)
    executorName: str | None = Field(default=None)
    stateType: ExecutionStatus | None = Field(default=None)
    host: str | None = Field(default=None)
    startDate: str | None = Field(default=None)
    endDate: str | None = Field(default=None)
    pageNo: int
    pageSize: int

SOURCE_CLOSURE_DIGEST = 'sha256:0e177459f43068643066c99e86ceaba4396cf4e6a80b7b525dca21c384da859f'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:b62a3552e5412385634825abad0f3bb0c08fc271cd1cfc73161d3c4876ede2a6'
