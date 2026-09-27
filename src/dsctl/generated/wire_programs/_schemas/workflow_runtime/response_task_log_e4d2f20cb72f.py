from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

class ResponseTaskLog(BaseEntityModel):
    lineNum: int = Field(default=0)
    message: str | None = Field(default=None)

__all__ = ["ResponseTaskLog"]

ResponseTaskLog.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = ResponseTaskLog
EXECUTABLE_SCHEMA_DIGEST = 'sha256:e4d2f20cb72f367098c7ecada2a0dd38ad619d6ce1c1af39916ad29c12826fd7'
