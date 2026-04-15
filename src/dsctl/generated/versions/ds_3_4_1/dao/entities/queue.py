from __future__ import annotations

from pydantic import Field
from ..._models import BaseEntityModel

class Queue(BaseEntityModel):
    id: int | None = Field(default=None, description='id')
    queueName: str | None = Field(default=None, description='queue name')
    queue: str | None = Field(default=None, description='yarn queue name')
    createTime: str | None = Field(default=None, description='create time')
    updateTime: str | None = Field(default=None, description='update time')

__all__ = ["Queue"]
