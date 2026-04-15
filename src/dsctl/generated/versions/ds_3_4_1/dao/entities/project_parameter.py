from __future__ import annotations

from pydantic import Field
from ..._models import BaseEntityModel

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
