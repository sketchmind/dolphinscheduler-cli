from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_340913ac6c37a9d9d3e68b1d50565a70b364df06a173e4ed945588d246ecf607 import FailureStrategy as FailureStrategy

from ..._schemas._enums.enum_1bfd725ef403a1fdc215a0df82a4cf6f6f57b576f222c5a3efb4b33006134f72 import Priority as Priority

from ..._schemas._enums.enum_ed0180176a382ad27a1c0709aa04b1040af0b0f2b1ead67fe29fe7f849923c64 import WarningType as WarningType

__all__ = ["FailureStrategy", "Priority", "WarningType"]

from typing import Annotated, Literal
from pydantic import Field, GetPydanticSchema
from ....wire_runtime.api.operations._base import BaseParamsModel

class ScheduleCreateParams(BaseParamsModel):
    projectCode: int
    processDefinitionCode: int
    schedule: str
    warningType: Annotated[WarningType | None | Literal['DEFAULT_WARNING_TYPE'], GetPydanticSchema(lambda _type, handler: handler(WarningType | None))] = Field(default='DEFAULT_WARNING_TYPE')
    warningGroupId: Annotated[int | None | Literal['DEFAULT_NOTIFY_GROUP_ID'], GetPydanticSchema(lambda _type, handler: handler(int | None))] = Field(default='DEFAULT_NOTIFY_GROUP_ID')
    failureStrategy: Annotated[FailureStrategy | None | Literal['DEFAULT_FAILURE_POLICY'], GetPydanticSchema(lambda _type, handler: handler(FailureStrategy | None))] = Field(default='DEFAULT_FAILURE_POLICY')
    workerGroup: str | None = Field(default='default')
    tenantCode: str | None = Field(default='default')
    environmentCode: int | None = Field(default=-1)
    processInstancePriority: Annotated[Priority | None | Literal['DEFAULT_PROCESS_INSTANCE_PRIORITY'], GetPydanticSchema(lambda _type, handler: handler(Priority | None))] = Field(default='DEFAULT_PROCESS_INSTANCE_PRIORITY')

SOURCE_CLOSURE_DIGEST = 'sha256:2117425901fa16d60f4abeed066796556492657c7b8a40bd27c7acb8635b0bbf'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:c7b9b1449f04b0a742e47b8df705d5ca3a70f6f65778236799b76870b1b416ee'
