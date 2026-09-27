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

class WorkFlowLineageServiceImplQueryWorkFlowLineageWorkFlowLists(BaseViewModel):
    """AST-inferred view from generated.view.WorkFlowLineageServiceImpl_queryWorkFlowLineage_workFlowLists."""
    workFlowList: list[WorkFlowLineage] | None = Field(default=None)
    workFlowRelationList: list[WorkFlowRelation] | None = Field(default=None)

class WorkFlowRelation(BaseEntityModel):
    sourceWorkFlowCode: int = Field(default=0)
    targetWorkFlowCode: int = Field(default=0)

__all__ = ["WorkFlowLineage", "WorkFlowLineageServiceImplQueryWorkFlowLineageWorkFlowLists", "WorkFlowRelation"]

WorkFlowLineage.model_rebuild(_types_namespace=globals())
WorkFlowLineageServiceImplQueryWorkFlowLineageWorkFlowLists.model_rebuild(_types_namespace=globals())
WorkFlowRelation.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = WorkFlowLineageServiceImplQueryWorkFlowLineageWorkFlowLists
EXECUTABLE_SCHEMA_DIGEST = 'sha256:8c1f23f3ccf8de3194ac8131ea09f6ed4f6f74adc6ac0be57180fccd6e03ca92'
