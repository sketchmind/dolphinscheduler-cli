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

RESPONSE_TYPE = list[Project]
EXECUTABLE_SCHEMA_DIGEST = 'sha256:64d276b2d4f65a0230272c2cc24c4b45b1cd98b0a425464ca5730f09b9f31e02'
