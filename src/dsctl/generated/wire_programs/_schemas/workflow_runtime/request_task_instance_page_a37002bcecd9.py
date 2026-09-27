from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_0f7108f98b8a7144b2b01aaf20f364e7e549f49bcda73389d4255622bf4b9be3 import ExecutionStatus as ExecutionStatus

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

SOURCE_CLOSURE_DIGEST = 'sha256:fc88dc608f68e8d13608750e88db326f49f9c70b085ecad3f41351a62bdf054c'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:a37002bcecd94e8263b00c1438e38f0bdc320e3a0a2a2fc6892623797b678de3'
