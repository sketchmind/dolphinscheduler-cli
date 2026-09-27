from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_c7ef46fe4ab574cc54a5e52a08773a1bb7b934f4fe11274c94f950c6425066b1 import ProcessExecutionTypeEnum as ProcessExecutionTypeEnum

from ..._schemas._enums.enum_22d45c0dad770da81d8b43eb6910f221bc4177bf3bb277d5073b5d81d38dbeb2 import ReleaseState as ReleaseState

__all__ = ["ProcessExecutionTypeEnum", "ReleaseState"]

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
    otherParamsJson: str | None = Field(default=None)
    executionType: Annotated[ProcessExecutionTypeEnum | None | Literal['PARALLEL'], GetPydanticSchema(lambda _type, handler: handler(ProcessExecutionTypeEnum | None))] = Field(default='PARALLEL')
    releaseState: Annotated[ReleaseState | None | Literal['OFFLINE'], GetPydanticSchema(lambda _type, handler: handler(ReleaseState | None))] = Field(default='OFFLINE')

SOURCE_CLOSURE_DIGEST = 'sha256:bb891f5b329b2d4059026befc3f1d38f8116d2ffa9c504eb9b515ee6806dae19'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:082ecccde898c1332b8fd9f39ddbc46dafa319b5d2341ef7cea46cdac1227a13'
