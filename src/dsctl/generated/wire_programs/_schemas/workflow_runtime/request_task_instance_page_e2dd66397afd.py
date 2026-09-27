from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_0fdf6309c5e0ea6bf4f8010f09afb5ea3d42bd51930597daf1a7473802f27eb7 import ExecutionStatus as ExecutionStatus

__all__ = ["ExecutionStatus"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class TaskInstancePageParams(BaseParamsModel):
    projectName: str
    processInstanceId: int | None = Field(default=0)
    searchVal: str | None = Field(default=None)
    taskName: str | None = Field(default=None)
    executorName: str | None = Field(default=None)
    stateType: ExecutionStatus | None = Field(default=None)
    host: str | None = Field(default=None)
    startDate: str | None = Field(default=None)
    endDate: str | None = Field(default=None)
    pageNo: int
    pageSize: int

SOURCE_CLOSURE_DIGEST = 'sha256:4c6c431f11d245be2c21b59a39af1171b53e91b5515c8be5569b0554d0110fe9'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:e2dd66397afd3c893b6f824c54255dfa8f1031ead0419bb6dbb3d193ae1c47bf'
