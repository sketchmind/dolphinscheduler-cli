from __future__ import annotations

from pydantic import Field
from ..._models import BaseEntityModel

class AlertGroup(BaseEntityModel):
    id: int | None = Field(default=None, description='primary key')
    groupName: str | None = Field(default=None, description='group_name')
    alertInstanceIds: str | None = Field(default=None)
    description: str | None = Field(default=None, description='description')
    createTime: str | None = Field(default=None, description='create_time')
    updateTime: str | None = Field(default=None, description='update_time')
    createUserId: int = Field(default=0, description='create_user_id')

__all__ = ["AlertGroup"]
