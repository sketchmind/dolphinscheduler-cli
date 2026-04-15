from __future__ import annotations

from pydantic import Field
from ...._models import BaseContractModel

class ProjectCreateRequest(BaseContractModel):
    """Project create request"""
    projectName: str = Field(examples=['pro123'])
    description: str | None = Field(default=None, examples=['this is a project'])

__all__ = ["ProjectCreateRequest"]
