from __future__ import annotations

from pydantic import Field
from ..._models import BaseEntityModel

from .work_flow_relation import WorkFlowRelation
from .work_flow_relation_detail import WorkFlowRelationDetail

class WorkFlowLineage(BaseEntityModel):
    workFlowRelationList: list[WorkFlowRelation] | None = Field(default=None)
    workFlowRelationDetailList: list[WorkFlowRelationDetail] | None = Field(default=None)

__all__ = ["WorkFlowLineage"]
