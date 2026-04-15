from __future__ import annotations

from pydantic import Field
from ..._models import BaseEntityModel

from ...common.enums.flag import Flag

class TaskGroup(BaseEntityModel):
    id: int | None = Field(default=None)
    name: str | None = Field(default=None)
    projectCode: int = Field(default=0)
    description: str | None = Field(default=None)
    groupSize: int = Field(default=0)
    useSize: int = Field(default=0)
    userId: int = Field(default=0)
    status: Flag | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)

__all__ = ["TaskGroup"]
