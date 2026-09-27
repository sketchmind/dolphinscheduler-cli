from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_340913ac6c37a9d9d3e68b1d50565a70b364df06a173e4ed945588d246ecf607 import FailureStrategy as FailureStrategy

from ..._schemas._enums.enum_1bfd725ef403a1fdc215a0df82a4cf6f6f57b576f222c5a3efb4b33006134f72 import Priority as Priority

from ..._schemas._enums.enum_ed0180176a382ad27a1c0709aa04b1040af0b0f2b1ead67fe29fe7f849923c64 import WarningType as WarningType

__all__ = ["FailureStrategy", "Priority", "WarningType"]

from typing import Annotated, Literal
from pydantic import Field, GetPydanticSchema
from ....wire_runtime.api.operations._base import BaseParamsModel

class ScheduleUpdateParams(BaseParamsModel):
    projectCode: int
    id: int
    schedule: str
    warningType: Annotated[WarningType | None | Literal['DEFAULT_WARNING_TYPE'], GetPydanticSchema(lambda _type, handler: handler(WarningType | None))] = Field(default='DEFAULT_WARNING_TYPE')
    warningGroupId: int | None = Field(default=None)
    failureStrategy: Annotated[FailureStrategy | None | Literal['END'], GetPydanticSchema(lambda _type, handler: handler(FailureStrategy | None))] = Field(default='END')
    workerGroup: str | None = Field(default='default')
    environmentCode: int | None = Field(default=-1)
    processInstancePriority: Priority | None = Field(default=None)

SOURCE_CLOSURE_DIGEST = 'sha256:159e4ea5538d4da119b00cba8c84d01da1c008270f8ee1b6a5e3992e5879ff05'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:6e4f645cf03cf8d11409ce3ce6faa0f840b9e699ebb4072c0886d7c9f76dda6d'
