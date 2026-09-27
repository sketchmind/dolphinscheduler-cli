from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_c7ef46fe4ab574cc54a5e52a08773a1bb7b934f4fe11274c94f950c6425066b1 import ProcessExecutionTypeEnum as ProcessExecutionTypeEnum

__all__ = ["ProcessExecutionTypeEnum"]

from typing import Annotated, Literal
from pydantic import Field, GetPydanticSchema
from ....wire_runtime.api.operations._base import BaseParamsModel

class DefinitionCreateParams(BaseParamsModel):
    projectCode: int
    name: str
    description: str | None = Field(default=None)
    globalParams: str | None = Field(default='[]')
    locations: str | None = Field(default=None)
    timeout: int | None = Field(default=0)
    taskRelationJson: str
    taskDefinitionJson: str
    otherParamsJson: str | None = Field(default=None)
    executionType: Annotated[ProcessExecutionTypeEnum | None | Literal['PARALLEL'], GetPydanticSchema(lambda _type, handler: handler(ProcessExecutionTypeEnum | None))] = Field(default='PARALLEL')

SOURCE_CLOSURE_DIGEST = 'sha256:78f855cb5e16ad97ff505ebb129914ac76b53936987b3276412c8b24bebb7a48'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:c9e254de3ff82005c9345358757c9c5ae5ebf84b9e4b22d03499e1b59bc65166'
