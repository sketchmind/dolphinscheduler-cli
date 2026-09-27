from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseContractModel

class AuditOperationTypeDto(BaseContractModel):
    name: str | None = Field(default=None)

__all__ = ["AuditOperationTypeDto"]

AuditOperationTypeDto.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = list[AuditOperationTypeDto]
EXECUTABLE_SCHEMA_DIGEST = 'sha256:c8414e3b761d68f60e55e1cbd2d680dd62d7c1373d6b8b1f6d257c6c2e328f3d'
