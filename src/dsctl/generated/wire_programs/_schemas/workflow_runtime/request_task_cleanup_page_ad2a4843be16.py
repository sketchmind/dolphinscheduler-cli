from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_3a17a111adfcb3b03b5bb8b3bc2077e935962ed2aa46b0bfcd4a9d73503fb450 import TaskExecuteType as TaskExecuteType

__all__ = ["TaskExecuteType"]

from typing import Annotated, Literal
from pydantic import Field, GetPydanticSchema
from ....wire_runtime.api.operations._base import BaseParamsModel

class TaskCleanupPageParams(BaseParamsModel):
    projectCode: int
    searchWorkflowName: str | None = Field(default=None)
    searchTaskName: str | None = Field(default=None)
    taskType: str | None = Field(default=None)
    taskExecuteType: Annotated[TaskExecuteType | None | Literal['BATCH'], GetPydanticSchema(lambda _type, handler: handler(TaskExecuteType | None))] = Field(default='BATCH')
    pageNo: int
    pageSize: int

SOURCE_CLOSURE_DIGEST = 'sha256:fe5f3c3029e1d92c86a982de84ac590e1bd2184de64036116bee3840d5117c86'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:ad2a4843be16b915c98539ba14d536471e559afb0196832f12af038137244d33'
