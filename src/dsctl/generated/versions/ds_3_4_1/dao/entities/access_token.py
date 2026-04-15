from __future__ import annotations

from pydantic import Field
from ..._models import BaseEntityModel

class AccessToken(BaseEntityModel):
    id: int | None = Field(default=None, description='primary key')
    userId: int = Field(default=0, description='user_id')
    token: str | None = Field(default=None, description='token')
    expireTime: str | None = Field(default=None, description='expire_time')
    createTime: str | None = Field(default=None, description='create_time')
    updateTime: str | None = Field(default=None, description='update_time')
    userName: str | None = Field(default=None)

__all__ = ["AccessToken"]
