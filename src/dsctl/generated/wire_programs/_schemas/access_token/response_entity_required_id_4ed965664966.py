from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

class AccessToken(BaseEntityModel):
    id: int = Field(default=0)
    userId: int = Field(default=0)
    token: str | None = Field(default=None)
    expireTime: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)
    userName: str | None = Field(default=None)

__all__ = ["AccessToken"]

AccessToken.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = AccessToken
EXECUTABLE_SCHEMA_DIGEST = 'sha256:4ed9656649666b6da3601342f020845b98bed9a6028da4c1e4ff04a4f2d7d433'
