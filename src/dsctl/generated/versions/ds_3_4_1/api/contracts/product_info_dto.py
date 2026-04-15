from __future__ import annotations

from pydantic import Field
from ..._models import BaseContractModel

class ProductInfoDto(BaseContractModel):
    """ProductInfoDto"""
    version: str | None = Field(default=None)

__all__ = ["ProductInfoDto"]
