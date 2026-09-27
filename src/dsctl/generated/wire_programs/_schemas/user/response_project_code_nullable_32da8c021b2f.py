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

RESPONSE_TYPE = list[Project]
EXECUTABLE_SCHEMA_DIGEST = 'sha256:32da8c021b2fd55069365daade582b80da338112f53788946fa69346755d330c'
