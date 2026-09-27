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
    paramDataType: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)
    createUser: str | None = Field(default=None)
    modifyUser: str | None = Field(default=None)

__all__ = ["ProjectParameter"]

ProjectParameter.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = ProjectParameter
EXECUTABLE_SCHEMA_DIGEST = 'sha256:d2d596b1880a80a971be1bbf7904c88055b88eee2c13b7d7d76a2c20a380ba17'
