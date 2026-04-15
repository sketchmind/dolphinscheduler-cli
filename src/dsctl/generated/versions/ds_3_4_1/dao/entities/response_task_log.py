from __future__ import annotations

from pydantic import Field
from ..._models import BaseEntityModel

class ResponseTaskLog(BaseEntityModel):
    """Log of the logger service response"""
    lineNum: int = Field(default=0)
    message: str | None = Field(default=None)

__all__ = ["ResponseTaskLog"]
