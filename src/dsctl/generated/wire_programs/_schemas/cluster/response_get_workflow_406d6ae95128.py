from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseContractModel

class ClusterDto(BaseContractModel):
    id: int = Field(default=0)
    code: int | None = Field(default=None)
    name: str | None = Field(default=None)
    config: str | None = Field(default=None)
    description: str | None = Field(default=None)
    workflowDefinitions: list[str] | None = Field(default=None)
    operator: int | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)

__all__ = ["ClusterDto"]

ClusterDto.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = ClusterDto
EXECUTABLE_SCHEMA_DIGEST = 'sha256:406d6ae95128c125b52a8859ee201fe253e373aea847d9361f8d7d2a71a7cc3d'
