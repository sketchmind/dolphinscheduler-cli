from __future__ import annotations

from pydantic import Field
from ...._models import BaseContractModel

class AuditOperationTypeDto(BaseContractModel):
    name: str | None = Field(default=None)

__all__ = ["AuditOperationTypeDto"]
