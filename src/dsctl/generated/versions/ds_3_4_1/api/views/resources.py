from __future__ import annotations

from pydantic import Field
from ..._models import BaseViewModel

class FetchFileContentResponse(BaseViewModel):
    content: str | None = Field(default=None)

__all__ = ["FetchFileContentResponse"]
