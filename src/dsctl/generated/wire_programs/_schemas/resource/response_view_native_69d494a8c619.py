from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseViewModel

class FetchFileContentResponse(BaseViewModel):
    content: str | None = Field(default=None)

__all__ = ["FetchFileContentResponse"]

FetchFileContentResponse.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = FetchFileContentResponse
EXECUTABLE_SCHEMA_DIGEST = 'sha256:69d494a8c61972b1c4c4304fa3d24530603b716876fdf43a0c94b2d91a72b80d'
