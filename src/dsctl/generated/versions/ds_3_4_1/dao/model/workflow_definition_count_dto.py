from __future__ import annotations

from pydantic import Field
from ..._models import BaseContractModel

class WorkflowDefinitionCountDto(BaseContractModel):
    userName: str | None = Field(default=None)
    userId: int | None = Field(default=None)
    count: int = Field(default=0)

__all__ = ["WorkflowDefinitionCountDto"]
