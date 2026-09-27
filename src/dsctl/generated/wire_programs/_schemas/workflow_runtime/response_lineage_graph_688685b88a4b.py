from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

class WorkFlowLineage(BaseEntityModel):
    workFlowRelationList: list[WorkFlowRelation] | None = Field(default=None)
    workFlowRelationDetailList: list[WorkFlowRelationDetail] | None = Field(default=None)

class WorkFlowRelation(BaseEntityModel):
    sourceWorkFlowCode: int = Field(default=0)
    targetWorkFlowCode: int = Field(default=0)

class WorkFlowRelationDetail(BaseEntityModel):
    workFlowCode: int = Field(default=0)
    workFlowName: str | None = Field(default=None)
    workFlowPublishStatus: str | None = Field(default=None)
    scheduleStartTime: str | None = Field(default=None)
    scheduleEndTime: str | None = Field(default=None)
    crontab: str | None = Field(default=None)
    schedulePublishStatus: int = Field(default=0)
    sourceWorkFlowCode: str | None = Field(default=None)

__all__ = ["WorkFlowLineage", "WorkFlowRelation", "WorkFlowRelationDetail"]

WorkFlowLineage.model_rebuild(_types_namespace=globals())
WorkFlowRelation.model_rebuild(_types_namespace=globals())
WorkFlowRelationDetail.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = WorkFlowLineage
EXECUTABLE_SCHEMA_DIGEST = 'sha256:688685b88a4b0f4c13cfed196f91ca72763bcd46a61d7396d2446386d5a54ad0'
