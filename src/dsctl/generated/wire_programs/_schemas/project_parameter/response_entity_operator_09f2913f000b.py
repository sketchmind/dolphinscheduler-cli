from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

class ProjectParameter(BaseEntityModel):
    id: int | None = Field(default=None)
    userId: int | None = Field(default=None)
    operator: int | None = Field(default=None)
    code: int = Field(default=0)
    projectCode: int = Field(default=0)
    paramName: str | None = Field(default=None)
    paramValue: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)
    createUser: str | None = Field(default=None)
    modifyUser: str | None = Field(default=None)

__all__ = ["ProjectParameter"]

ProjectParameter.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = ProjectParameter
EXECUTABLE_SCHEMA_DIGEST = 'sha256:09f2913f000b3664d83a231de34ec38012f291b0077ca3c5b66644d7e8d57540'
