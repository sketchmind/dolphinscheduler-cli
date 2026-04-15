from __future__ import annotations

from pydantic import Field
from ...._models import BaseContractModel

from .instance import Instance

class TreeViewDto(BaseContractModel):
    """TreeView"""
    name: str | None = Field(default=None, description='name')
    type: str | None = Field(default=None, description='type')
    code: int = Field(default=0, description='code')
    instances: list[Instance] = Field(default_factory=list, description='instances list')
    children: list[TreeViewDto] = Field(default_factory=list, description='children')

__all__ = ["TreeViewDto"]
