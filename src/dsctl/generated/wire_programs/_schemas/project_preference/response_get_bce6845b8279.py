from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

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

ProjectPreference.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = ProjectPreference | None
EXECUTABLE_SCHEMA_DIGEST = 'sha256:bce6845b8279264b550d6ac65999e1df587a5c54e0ac6137760f8e0d630a1a53'
