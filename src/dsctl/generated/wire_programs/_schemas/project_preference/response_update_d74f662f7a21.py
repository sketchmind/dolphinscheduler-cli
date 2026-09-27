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

RESPONSE_TYPE = ProjectPreference
EXECUTABLE_SCHEMA_DIGEST = 'sha256:d74f662f7a2157936bb46f08bb40519f0bbf5bedc63386fe11a155dbc5560b00'
