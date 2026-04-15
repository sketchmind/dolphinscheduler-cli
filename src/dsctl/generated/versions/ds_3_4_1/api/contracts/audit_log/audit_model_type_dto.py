from __future__ import annotations

from pydantic import Field
from ...._models import BaseContractModel

class AuditModelTypeDto(BaseContractModel):
    name: str | None = Field(default=None)
    child: list[AuditModelTypeDto] | None = Field(default=None)

__all__ = ["AuditModelTypeDto"]
