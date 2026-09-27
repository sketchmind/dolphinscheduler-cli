from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_32fce7bcae4edc5d12b75ac8481cae7142bc5af424dbf5e4d1fe66f96a7dad38 import WorkflowExecutionTypeEnum as WorkflowExecutionTypeEnum

__all__ = ["WorkflowExecutionTypeEnum"]

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
    executionType: Annotated[WorkflowExecutionTypeEnum | None | Literal['PARALLEL'], GetPydanticSchema(lambda _type, handler: handler(WorkflowExecutionTypeEnum | None))] = Field(default='PARALLEL')

SOURCE_CLOSURE_DIGEST = 'sha256:a932fccd1ef6e0f91e54ca1abcb043d322984f3e548afbef9fb22225dbbc234e'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:c337c0865c5fc5f812a66dd63bae54dac7dd9a2d43ab5862ab2e92ec17a41c64'
