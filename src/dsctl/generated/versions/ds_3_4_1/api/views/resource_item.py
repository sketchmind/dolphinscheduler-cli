from __future__ import annotations

from pydantic import Field
from ..._models import BaseViewModel

from ...spi.enums.resource_type import ResourceType

class ResourceItemVO(BaseViewModel):
    alias: str | None = Field(default=None)
    userName: str | None = Field(default=None)
    fileName: str | None = Field(default=None)
    fullName: str | None = Field(default=None)
    isDirectory: bool = Field(default=False, alias='directory')
    type: ResourceType | None = Field(default=None)
    size: int = Field(default=0)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)

__all__ = ["ResourceItemVO"]
