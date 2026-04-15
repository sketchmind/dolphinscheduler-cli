from __future__ import annotations

from pydantic import Field
from ...._models import BaseContractModel

class TaskRelationCreateRequest(BaseContractModel):
    projectCode: int = Field(default=0, examples=[12345678])
    workflowCode: int = Field(examples=[87654321])
    preTaskCode: int = Field(examples=[12345])
    postTaskCode: int = Field(examples=[54321])

__all__ = ["TaskRelationCreateRequest"]
