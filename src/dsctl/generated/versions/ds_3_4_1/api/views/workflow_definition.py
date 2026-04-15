from __future__ import annotations

from pydantic import Field
from ..._models import BaseViewModel

from ...plugin.task_api.model.property import Property

class WorkflowDefinitionLocalParamsEntry(BaseViewModel):
    """AST-inferred view from generated.view.WorkflowDefinitionServiceImpl_getLocalParams_localUserDefParamsValue."""
    localParamsList: list[Property] | None = Field(default=None)

class WorkflowDefinitionSimpleItem(BaseViewModel):
    """AST-inferred view from generated.view.WorkflowDefinitionServiceImpl_queryWorkflowDefinitionSimpleList_arrayNodeItem."""
    id: int | None = Field(default=None)
    code: int | None = Field(default=None)
    name: str | None = Field(default=None)
    projectCode: int | None = Field(default=None)

class WorkflowDefinitionVariablesView(BaseViewModel):
    """AST-inferred view from generated.view.WorkflowDefinitionServiceImpl_viewVariables_resultMap."""
    globalParams: list[Property] | None = Field(default=None)
    localParams: dict[str, WorkflowDefinitionLocalParamsEntry] | None = Field(default=None)

__all__ = ["WorkflowDefinitionLocalParamsEntry", "WorkflowDefinitionSimpleItem", "WorkflowDefinitionVariablesView"]
