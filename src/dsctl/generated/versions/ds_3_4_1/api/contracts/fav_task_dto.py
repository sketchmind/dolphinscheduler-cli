from __future__ import annotations

from pydantic import Field
from ..._models import BaseContractModel

class FavTaskDto(BaseContractModel):
    taskType: str | None = Field(default=None)
    isCollection: bool = Field(default=False, alias='collection')
    taskCategory: str | None = Field(default=None)

__all__ = ["FavTaskDto"]
