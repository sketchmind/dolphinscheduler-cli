from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_3a17a111adfcb3b03b5bb8b3bc2077e935962ed2aa46b0bfcd4a9d73503fb450 import TaskExecuteType as TaskExecuteType

from ..._schemas._enums.enum_83bb8baac51b374757245101b46ab8f208f4801d63c3e4233da340112d54965a import TaskExecutionStatus as TaskExecutionStatus

__all__ = ["TaskExecuteType", "TaskExecutionStatus"]

from typing import Annotated, Literal
from pydantic import Field, GetPydanticSchema
from ....wire_runtime.api.operations._base import BaseParamsModel

class TaskInstancePageParams(BaseParamsModel):
    projectCode: int
    workflowInstanceId: int | None = Field(default=0)
    workflowInstanceName: str | None = Field(default=None)
    workflowDefinitionName: str | None = Field(default=None)
    searchVal: str | None = Field(default=None)
    taskName: str | None = Field(default=None)
    taskCode: int | None = Field(default=None)
    executorName: str | None = Field(default=None)
    stateType: TaskExecutionStatus | None = Field(default=None)
    host: str | None = Field(default=None)
    startDate: str | None = Field(default=None)
    endDate: str | None = Field(default=None)
    taskExecuteType: Annotated[TaskExecuteType | None | Literal['BATCH'], GetPydanticSchema(lambda _type, handler: handler(TaskExecuteType | None))] = Field(default='BATCH')
    pageNo: int
    pageSize: int

SOURCE_CLOSURE_DIGEST = 'sha256:0b0fbbdc5ecd8fe6768426b845d420b248f3edcf6bb2a6cf383e50032adf48f2'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:3bb184f46c7564b92fbe5c818ec21a23222a71bde4486a99ebf18ee1886148fd'
