from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

class AccessToken(BaseEntityModel):
    id: int | None = Field(default=None)
    userId: int = Field(default=0)
    token: str | None = Field(default=None)
    expireTime: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)
    userName: str | None = Field(default=None)

__all__ = ["AccessToken"]

AccessToken.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = AccessToken
EXECUTABLE_SCHEMA_DIGEST = 'sha256:3b2b70a680f95615a209fa1abb98d674b93dc9208eca2863368db1b930924158'
