from __future__ import annotations

from pydantic import Field
from ..._models import BaseEntityModel

class WorkFlowRelationDetail(BaseEntityModel):
    workFlowCode: int = Field(default=0)
    workFlowName: str | None = Field(default=None)
    workFlowPublishStatus: str | None = Field(default=None)
    scheduleStartTime: str | None = Field(default=None)
    scheduleEndTime: str | None = Field(default=None)
    crontab: str | None = Field(default=None)
    schedulePublishStatus: int = Field(default=0)
    sourceWorkFlowCode: str | None = Field(default=None)

__all__ = ["WorkFlowRelationDetail"]
