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
    tenantCode: str
    taskRelationJson: str
    taskDefinitionJson: str
    otherParamsJson: str | None = Field(default=None)
    executionType: Annotated[ProcessExecutionTypeEnum | None | Literal['PARALLEL'], GetPydanticSchema(lambda _type, handler: handler(ProcessExecutionTypeEnum | None))] = Field(default='PARALLEL')

SOURCE_CLOSURE_DIGEST = 'sha256:d567250681af4ba30246cae464623853511aff8d66cef55166b637795112980d'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:04184767cbca4c1c4b2f070abcbeb3d549f3edb6a911308a269dd82cebb0bc59'
