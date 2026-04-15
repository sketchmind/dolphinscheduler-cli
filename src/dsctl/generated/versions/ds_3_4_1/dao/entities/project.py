from __future__ import annotations

from pydantic import Field
from ..._models import BaseEntityModel

class Project(BaseEntityModel):
    id: int | None = Field(default=None, description='id')
    userId: int | None = Field(default=None, description='user id')
    userName: str | None = Field(default=None, description='user name')
    code: int = Field(default=0, description='project code')
    name: str | None = Field(default=None, description='project name')
    description: str | None = Field(default=None, description='project description')
    createTime: str | None = Field(default=None, description='create time')
    updateTime: str | None = Field(default=None, description='update time')
    perm: int = Field(default=0, description='permission')
    defCount: int = Field(default=0, description='process define count')

__all__ = ["Project"]
