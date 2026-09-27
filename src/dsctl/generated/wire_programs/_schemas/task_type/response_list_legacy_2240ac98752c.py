from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseContractModel

class FavTaskDto(BaseContractModel):
    taskName: str | None = Field(default=None)
    isCollection: bool = Field(default=False, alias='collection')
    taskType: str | None = Field(default=None)

__all__ = ["FavTaskDto"]

FavTaskDto.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = list[FavTaskDto]
EXECUTABLE_SCHEMA_DIGEST = 'sha256:2240ac98752ccedb67d81a3d90d7d25318eabba0152bf57b2201d91eb920476f'
