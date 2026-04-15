from __future__ import annotations

from pydantic import Field
from ...._models import BaseContractModel

class ProjectUpdateRequest(BaseContractModel):
    """Project update request"""
    projectName: str = Field(examples=['pro123'])
    description: str | None = Field(default=None, examples=['this is a project'])

__all__ = ["ProjectUpdateRequest"]
