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
    projectName: str
    id: int
    schedule: str
    warningType: Annotated[WarningType | None | Literal['DEFAULT_WARNING_TYPE'], GetPydanticSchema(lambda _type, handler: handler(WarningType | None))] = Field(default='DEFAULT_WARNING_TYPE')
    warningGroupId: int | None = Field(default=None)
    failureStrategy: Annotated[FailureStrategy | None | Literal['END'], GetPydanticSchema(lambda _type, handler: handler(FailureStrategy | None))] = Field(default='END')
    receivers: str | None = Field(default=None)
    receiversCc: str | None = Field(default=None)
    workerGroup: str | None = Field(default='default')
    processInstancePriority: Priority | None = Field(default=None)

SOURCE_CLOSURE_DIGEST = 'sha256:7a98c59c8f778ff312612a6b7cf7aceba1910a5c9806cd5a5afad6fe935f1bea'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:1b8820c76d99ee1e7906f99c85a89da85a1dbe4b503d7453482247c7cd0b5b16'
