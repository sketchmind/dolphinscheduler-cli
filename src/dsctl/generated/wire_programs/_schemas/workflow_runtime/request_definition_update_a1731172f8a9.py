from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_22d45c0dad770da81d8b43eb6910f221bc4177bf3bb277d5073b5d81d38dbeb2 import ReleaseState as ReleaseState

from ..._schemas._enums.enum_32fce7bcae4edc5d12b75ac8481cae7142bc5af424dbf5e4d1fe66f96a7dad38 import WorkflowExecutionTypeEnum as WorkflowExecutionTypeEnum

__all__ = ["ReleaseState", "WorkflowExecutionTypeEnum"]

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
    executionType: Annotated[WorkflowExecutionTypeEnum | None | Literal['PARALLEL'], GetPydanticSchema(lambda _type, handler: handler(WorkflowExecutionTypeEnum | None))] = Field(default='PARALLEL')
    releaseState: Annotated[ReleaseState | None | Literal['OFFLINE'], GetPydanticSchema(lambda _type, handler: handler(ReleaseState | None))] = Field(default='OFFLINE')

SOURCE_CLOSURE_DIGEST = 'sha256:7b699070cb2536ef71b785d03a6d0b05cb57a97a149646db0c7e4cc9c6a29b01'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:a1731172f8a9d4f8422871999028e818dfdf1bd168096ab122380bc9a68a5a31'
