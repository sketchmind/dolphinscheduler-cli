from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

class ProjectParameter(BaseEntityModel):
    id: int | None = Field(default=None)
    userId: int | None = Field(default=None)
    code: int = Field(default=0)
    projectCode: int = Field(default=0)
    paramName: str | None = Field(default=None)
    paramValue: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)

__all__ = ["ProjectParameter"]

ProjectParameter.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = ProjectParameter
EXECUTABLE_SCHEMA_DIGEST = 'sha256:a83406af7c1706db4d9751ccdb3b24afb521838c86f09c3d1ef13f9279ee034f'
