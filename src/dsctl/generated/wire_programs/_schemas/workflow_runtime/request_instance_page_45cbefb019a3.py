from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_aa87095abf892a674d9bc9d6e4fba7691e04e734501b487ccbaee365f84b47e3 import ExecutionStatus as ExecutionStatus

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

SOURCE_CLOSURE_DIGEST = 'sha256:c29a57325ec9af8ebfc5bf82ae837debdfd541ae9123b9adf57ed06bb0f37d45'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:45cbefb019a309df80575b47ca3ad56e0a30cad891f3134dfee4ce4a1da67e52'
