from __future__ import annotations

from pydantic import Field
from ..._models import BaseEntityModel

class Tenant(BaseEntityModel):
    id: int | None = Field(default=None, description='id')
    tenantCode: str | None = Field(default=None, description='tenant code')
    description: str | None = Field(default=None, description='description')
    queueId: int = Field(default=0, description='queue id')
    queueName: str | None = Field(default=None, description='queue name')
    queue: str | None = Field(default=None, description='queue')
    createTime: str | None = Field(default=None, description='create time')
    updateTime: str | None = Field(default=None, description='update time')

__all__ = ["Tenant"]
