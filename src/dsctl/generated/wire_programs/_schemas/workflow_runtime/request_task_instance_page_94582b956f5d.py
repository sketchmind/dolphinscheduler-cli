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

SOURCE_CLOSURE_DIGEST = 'sha256:75121a76f161650db71a6f42833a9124bc3b0d0f4b3bdbec1b5bca167496d503'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:94582b956f5da153ee286db6e6baed0b99572daa4bb1e6142e5a37970f58f5b4'
