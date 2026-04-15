from __future__ import annotations

from pydantic import Field
from ..._models import BaseContractModel

class PageQueryDto(BaseContractModel):
    """Page query dto"""
    pageSize: int = Field(examples=[10])
    pageNo: int = Field(examples=[1])

__all__ = ["PageQueryDto"]
