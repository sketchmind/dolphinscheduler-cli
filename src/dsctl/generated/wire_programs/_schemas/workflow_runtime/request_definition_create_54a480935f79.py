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
    executionType: Annotated[ProcessExecutionTypeEnum | None | Literal['PARALLEL'], GetPydanticSchema(lambda _type, handler: handler(ProcessExecutionTypeEnum | None))] = Field(default='PARALLEL')

SOURCE_CLOSURE_DIGEST = 'sha256:6c89fd1f6ee292ac0ac7cdf8d4a0decb40292e0b85f891452819292cafd8a80e'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:54a480935f79a853f77ee22cb6ed429130a03870d9f7cef70a23a7568fe1b8c8'
