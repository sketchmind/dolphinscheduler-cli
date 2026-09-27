from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseContractModel

class ClusterDto(BaseContractModel):
    id: int = Field(default=0)
    code: int | None = Field(default=None)
    name: str | None = Field(default=None)
    config: str | None = Field(default=None)
    description: str | None = Field(default=None)
    processDefinitions: list[str] | None = Field(default=None)
    operator: int | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)

__all__ = ["ClusterDto"]

ClusterDto.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = ClusterDto
EXECUTABLE_SCHEMA_DIGEST = 'sha256:6ced810d3e0babcab0910669a40b46548352ccff975f988b7fa16ea1ffe85bbd'
