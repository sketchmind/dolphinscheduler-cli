from __future__ import annotations

from pydantic import Field
from ..._models import BaseContractModel

class AuditDto(BaseContractModel):
    userName: str | None = Field(default=None)
    modelType: str | None = Field(default=None)
    modelName: str | None = Field(default=None)
    operation: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    description: str | None = Field(default=None)
    detail: str | None = Field(default=None)
    latency: str | None = Field(default=None)

__all__ = ["AuditDto"]
