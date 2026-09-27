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
    instRunningCount: int = Field(default=0)

__all__ = ["Project"]

Project.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = Project
EXECUTABLE_SCHEMA_DIGEST = 'sha256:713f7a7757bd4bf012c02ce10fdc724e6e9050da85dca88554960b76d0c739e4'
