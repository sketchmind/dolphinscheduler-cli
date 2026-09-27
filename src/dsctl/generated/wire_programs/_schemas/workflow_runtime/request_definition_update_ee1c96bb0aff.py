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
    taskRelationJson: str
    taskDefinitionJson: str
    executionType: Annotated[ProcessExecutionTypeEnum | None | Literal['PARALLEL'], GetPydanticSchema(lambda _type, handler: handler(ProcessExecutionTypeEnum | None))] = Field(default='PARALLEL')
    releaseState: Annotated[ReleaseState | None | Literal['OFFLINE'], GetPydanticSchema(lambda _type, handler: handler(ReleaseState | None))] = Field(default='OFFLINE')

SOURCE_CLOSURE_DIGEST = 'sha256:69575c3fbb6e2667668c64966b03443205bef7a8bc05729a99e777d891892780'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:ee1c96bb0aff8ef077163327f433dbe729bd55deda5be2c00d81b26f919311ef'
