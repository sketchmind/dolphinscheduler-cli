from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_d1f73e5a15e4d9713bee012bc479e894a208d338ad1d29974710e66dae315424 import WorkflowExecutionStatus as WorkflowExecutionStatus

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

SOURCE_CLOSURE_DIGEST = 'sha256:1ea76dd10191edd335c6c7be354c5895fd4fccc93613597a464eeb96637a9abc'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:7e29df01e196e62cdffd7cfc1ed5ff56e6a471bc5ac0ab20c457fad90d84417f'
