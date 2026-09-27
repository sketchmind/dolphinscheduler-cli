from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseContractModel

class FavTaskDto(BaseContractModel):
    taskType: str | None = Field(default=None)
    isCollection: bool = Field(default=False, alias='collection')
    taskCategory: str | None = Field(default=None)

__all__ = ["FavTaskDto"]

FavTaskDto.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = list[FavTaskDto]
EXECUTABLE_SCHEMA_DIGEST = 'sha256:058396d96ccd98c8fc294faf42e48d727a23093905ad7a4abb9f52d58ebfd8af'
