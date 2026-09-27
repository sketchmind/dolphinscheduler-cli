from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseContractModel

class AuditModelTypeDto(BaseContractModel):
    name: str | None = Field(default=None)
    child: list[AuditModelTypeDto] | None = Field(default=None)

__all__ = ["AuditModelTypeDto"]

AuditModelTypeDto.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = list[AuditModelTypeDto]
EXECUTABLE_SCHEMA_DIGEST = 'sha256:52d7d1031ffe90c52b3970f9ae5fc0c9beeb260bcfa5ef2ada03f5bb8456d079'
