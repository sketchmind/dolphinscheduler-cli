from __future__ import annotations

from pydantic import Field
from ...._models import BaseContractModel

class DynamicTaskInfo(BaseContractModel):
    name: str | None = Field(default=None)
    hover: str | None = Field(default=None)
    icon: str | None = Field(default=None)
    json_field: str | None = Field(default=None, alias='json')

__all__ = ["DynamicTaskInfo"]
