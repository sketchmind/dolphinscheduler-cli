from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

class Project(BaseEntityModel):
    id: int | None = Field(default=None)
    userId: int | None = Field(default=None)
    userName: str | None = Field(default=None)
    code: int = Field(default=0)
    name: str | None = Field(default=None)
    description: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)
    perm: int = Field(default=0)
    defCount: int = Field(default=0)

__all__ = ["Project"]

Project.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = Project
EXECUTABLE_SCHEMA_DIGEST = 'sha256:7edcd8ce05f76dd48c01002e7be777fe459c63e940f706ed160e70b2641093f3'
