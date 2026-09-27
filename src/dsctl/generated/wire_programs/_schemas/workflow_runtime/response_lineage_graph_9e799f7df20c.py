from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseEntityModel, BaseViewModel

class WorkFlowLineage(BaseEntityModel):
    workFlowCode: int = Field(default=0)
    workFlowName: str | None = Field(default=None)
    workFlowPublishStatus: str | None = Field(default=None)
    scheduleStartTime: str | None = Field(default=None)
    scheduleEndTime: str | None = Field(default=None)
    crontab: str | None = Field(default=None)
    schedulePublishStatus: int = Field(default=0)
    sourceWorkFlowCode: str | None = Field(default=None)

class WorkFlowLineageServiceImplQueryWorkFlowLineageByCodeWorkFlowLists(BaseViewModel):
    """AST-inferred view from generated.view.WorkFlowLineageServiceImpl_queryWorkFlowLineageByCode_workFlowLists."""
    workFlowList: list[WorkFlowLineage] | None = Field(default=None)
    workFlowRelationList: list[WorkFlowRelation] | None = Field(default=None)

class WorkFlowRelation(BaseEntityModel):
    sourceWorkFlowCode: int = Field(default=0)
    targetWorkFlowCode: int = Field(default=0)

__all__ = ["WorkFlowLineage", "WorkFlowLineageServiceImplQueryWorkFlowLineageByCodeWorkFlowLists", "WorkFlowRelation"]

WorkFlowLineage.model_rebuild(_types_namespace=globals())
WorkFlowLineageServiceImplQueryWorkFlowLineageByCodeWorkFlowLists.model_rebuild(_types_namespace=globals())
WorkFlowRelation.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = WorkFlowLineageServiceImplQueryWorkFlowLineageByCodeWorkFlowLists
EXECUTABLE_SCHEMA_DIGEST = 'sha256:9e799f7df20c8d214e6b966342a7bcb5ea9d3de57cdff69734948b906f13846d'
