from __future__ import annotations

from pydantic import Field
from ..._models import BaseViewModel

from ...dao.entities.dependent_lineage_task import DependentLineageTask
from ...dao.entities.work_flow_lineage import WorkFlowLineage

class WorkflowLineageByCodeResult(BaseViewModel):
    """AST-inferred view from generated.view.WorkflowLineageController_queryWorkFlowLineageByCode_result."""
    data: WorkFlowLineage | None = Field(default=None)

class WorkflowLineageDependentTasksResult(BaseViewModel):
    """AST-inferred view from generated.view.WorkflowLineageController_queryDependentTasks_result."""
    data: list[DependentLineageTask] | None = Field(default=None)

class WorkflowLineageResult(BaseViewModel):
    """AST-inferred view from generated.view.WorkflowLineageController_queryWorkFlowLineage_result."""
    data: WorkFlowLineage | None = Field(default=None)

__all__ = ["WorkflowLineageByCodeResult", "WorkflowLineageDependentTasksResult", "WorkflowLineageResult"]
