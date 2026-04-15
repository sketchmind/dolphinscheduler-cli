from __future__ import annotations

from pydantic import Field
from ..._models import BaseEntityModel

class DependentSimplifyDefinition(BaseEntityModel):
    """Dependent node simplify definition"""
    code: int | None = Field(default=None, description='definition code')
    name: str | None = Field(default=None, description='definition name')
    version: int | None = Field(default=None, description='definition version')

__all__ = ["DependentSimplifyDefinition"]
