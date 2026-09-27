from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_0fdf6309c5e0ea6bf4f8010f09afb5ea3d42bd51930597daf1a7473802f27eb7 import ExecutionStatus as ExecutionStatus

__all__ = ["ExecutionStatus"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class InstancePageParams(BaseParamsModel):
    projectName: str
    processDefinitionId: int | None = Field(default=0)
    searchVal: str | None = Field(default=None)
    executorName: str | None = Field(default=None)
    stateType: ExecutionStatus | None = Field(default=None)
    host: str | None = Field(default=None)
    startDate: str | None = Field(default=None)
    endDate: str | None = Field(default=None)
    pageNo: int
    pageSize: int

SOURCE_CLOSURE_DIGEST = 'sha256:382446d8fab522ff7563123f0813d44d27332f814ace709dd99a6ca0d3f17cef'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:a11b574c930b7e6517949b91a47987996cf63bd018b6a23e8f52343e3a198f41'
