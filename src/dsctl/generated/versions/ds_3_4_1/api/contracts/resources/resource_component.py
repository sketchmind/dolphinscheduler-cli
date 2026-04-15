from __future__ import annotations

from pydantic import Field
from ...._models import BaseContractModel

from ....spi.enums.resource_type import ResourceType

class ResourceComponent(BaseContractModel):
    """Resource component"""
    name: str | None = Field(default=None, description='name')
    currentDir: str | None = Field(default=None, description='current directory')
    fullName: str | None = Field(default=None, description='full name')
    description: str | None = Field(default=None, description='description')
    isDirctory: bool = Field(default=False, alias='dirctory', description='is directory')
    type: ResourceType | None = Field(default=None, description='resoruce type')
    children: list[ResourceComponent] = Field(default_factory=list, description='children')

__all__ = ["ResourceComponent"]
