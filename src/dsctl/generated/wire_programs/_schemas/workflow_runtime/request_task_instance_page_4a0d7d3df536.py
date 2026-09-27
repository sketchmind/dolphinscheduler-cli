from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_3a17a111adfcb3b03b5bb8b3bc2077e935962ed2aa46b0bfcd4a9d73503fb450 import TaskExecuteType as TaskExecuteType

from ..._schemas._enums.enum_4419bb3e09efc98c376c88d16b6c9a7e7e5a778b38f62483b50f2a4d99de26ac import TaskExecutionStatus as TaskExecutionStatus

__all__ = ["TaskExecuteType", "TaskExecutionStatus"]

from typing import Annotated, Literal
from pydantic import Field, GetPydanticSchema
from ....wire_runtime.api.operations._base import BaseParamsModel

class TaskInstancePageParams(BaseParamsModel):
    projectCode: int
    processInstanceId: int | None = Field(default=0)
    processInstanceName: str | None = Field(default=None)
    processDefinitionName: str | None = Field(default=None)
    searchVal: str | None = Field(default=None)
    taskName: str | None = Field(default=None)
    executorName: str | None = Field(default=None)
    stateType: TaskExecutionStatus | None = Field(default=None)
    host: str | None = Field(default=None)
    startDate: str | None = Field(default=None)
    endDate: str | None = Field(default=None)
    taskExecuteType: Annotated[TaskExecuteType | None | Literal['BATCH'], GetPydanticSchema(lambda _type, handler: handler(TaskExecuteType | None))] = Field(default='BATCH')
    pageNo: int
    pageSize: int

SOURCE_CLOSURE_DIGEST = 'sha256:ddf27083f64f0c5c6e65a7eaa01b76d35a15db0fa4004eaf0515a52c4e83247f'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:4a0d7d3df536e797b87a9758efcec5df4f79c6ac588e6df37bed1ee7b7d46c80'
