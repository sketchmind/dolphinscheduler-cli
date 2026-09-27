from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_22d45c0dad770da81d8b43eb6910f221bc4177bf3bb277d5073b5d81d38dbeb2 import ReleaseState as ReleaseState

__all__ = ["ReleaseState"]

from typing import Annotated, Literal
from pydantic import Field, GetPydanticSchema
from ....wire_runtime.api.operations._base import BaseParamsModel

class DefinitionUpdateParams(BaseParamsModel):
    projectCode: int
    name: str
    code: int
    description: str | None = Field(default=None)
    globalParams: str | None = Field(default='[]')
    locations: str | None = Field(default=None)
    timeout: int | None = Field(default=0)
    tenantCode: str
    taskRelationJson: str
    taskDefinitionJson: str
    releaseState: Annotated[ReleaseState | None | Literal['OFFLINE'], GetPydanticSchema(lambda _type, handler: handler(ReleaseState | None))] = Field(default='OFFLINE')

SOURCE_CLOSURE_DIGEST = 'sha256:cdfdbfdc35b97fe2a1fa01c4ed81b061eedb3e8b2d541d47c2cbe5febce41074'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:a749cc17c1b9d3009ea48b4b6cac7e067041d41d2b5d5e523e44431ba74744d0'
