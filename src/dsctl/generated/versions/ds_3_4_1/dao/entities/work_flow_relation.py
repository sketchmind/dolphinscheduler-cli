from __future__ import annotations

from pydantic import Field
from ..._models import BaseEntityModel

class WorkFlowRelation(BaseEntityModel):
    sourceWorkFlowCode: int = Field(default=0)
    targetWorkFlowCode: int = Field(default=0)

__all__ = ["WorkFlowRelation"]
