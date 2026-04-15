from __future__ import annotations

from pydantic import Field
from ..._models import BaseEntityModel

class ProjectPreference(BaseEntityModel):
    id: int | None = Field(default=None)
    code: int = Field(default=0)
    projectCode: int = Field(default=0)
    preferences: str | None = Field(default=None)
    userId: int | None = Field(default=None)
    state: int = Field(default=0)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)

__all__ = ["ProjectPreference"]
