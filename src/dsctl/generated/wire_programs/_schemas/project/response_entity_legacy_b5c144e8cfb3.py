from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

class Project(BaseEntityModel):
    id: int = Field(default=0)
    userId: int = Field(default=0)
    userName: str | None = Field(default=None)
    name: str | None = Field(default=None)
    description: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)
    perm: int = Field(default=0)
    defCount: int = Field(default=0)
    instRunningCount: int = Field(default=0)

__all__ = ["Project"]

Project.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = Project
EXECUTABLE_SCHEMA_DIGEST = 'sha256:b5c144e8cfb330a07039c195a11d0bb93356c2c20dd6e6a63e756608b15921ca'
