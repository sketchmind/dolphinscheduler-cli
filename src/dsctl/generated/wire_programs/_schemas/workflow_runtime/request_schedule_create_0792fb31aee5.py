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
    workflowDefinitionCode: int
    schedule: str
    warningType: Annotated[WarningType | None | Literal['DEFAULT_WARNING_TYPE'], GetPydanticSchema(lambda _type, handler: handler(WarningType | None))] = Field(default='DEFAULT_WARNING_TYPE')
    warningGroupId: Annotated[int | None | Literal['DEFAULT_NOTIFY_GROUP_ID'], GetPydanticSchema(lambda _type, handler: handler(int | None))] = Field(default='DEFAULT_NOTIFY_GROUP_ID')
    failureStrategy: Annotated[FailureStrategy | None | Literal['DEFAULT_FAILURE_POLICY'], GetPydanticSchema(lambda _type, handler: handler(FailureStrategy | None))] = Field(default='DEFAULT_FAILURE_POLICY')
    workerGroup: str | None = Field(default='default')
    tenantCode: str | None = Field(default='default')
    environmentCode: int | None = Field(default=-1)
    workflowInstancePriority: Annotated[Priority | None | Literal['DEFAULT_WORKFLOW_INSTANCE_PRIORITY'], GetPydanticSchema(lambda _type, handler: handler(Priority | None))] = Field(default='DEFAULT_WORKFLOW_INSTANCE_PRIORITY')

SOURCE_CLOSURE_DIGEST = 'sha256:bc5fe800919e8d09900a5ce1f2f0c9e2c4752ac8f63a75a28f4f9628867cd2ca'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:0792fb31aee58a4b188ba3c49932599b11422a673b55284ff6ca1c39f317a792'
