from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseContractModel

class EnvironmentDto(BaseContractModel):
    id: int = Field(default=0)
    code: int | None = Field(default=None)
    name: str | None = Field(default=None)
    config: str | None = Field(default=None)
    description: str | None = Field(default=None)
    workerGroups: list[str] | None = Field(default=None)
    operator: int | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)

__all__ = ["EnvironmentDto"]

EnvironmentDto.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = EnvironmentDto
EXECUTABLE_SCHEMA_DIGEST = 'sha256:90d23745c4cb6f8e00a443707fff0dbab800ec5ce7156abaeb701ed888458fae'
