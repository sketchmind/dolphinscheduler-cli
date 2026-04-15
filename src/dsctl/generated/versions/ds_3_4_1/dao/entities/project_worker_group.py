from __future__ import annotations

from pydantic import Field
from ..._models import BaseEntityModel

class ProjectWorkerGroup(BaseEntityModel):
    id: int | None = Field(default=None, description='id')
    projectCode: int | None = Field(default=None, description='project code')
    workerGroup: str | None = Field(default=None, description='worker group')
    createTime: str | None = Field(default=None, description='create time')
    updateTime: str | None = Field(default=None, description='update time')

__all__ = ["ProjectWorkerGroup"]
