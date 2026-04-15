from __future__ import annotations

from pydantic import Field
from ..._models import BaseViewModel

from ...dao.entities.task_instance_dependent_details import TaskInstanceDependentDetailsAbstractTaskInstanceContext
from ...plugin.task_api.model.property import Property

class WorkflowInstanceLocalParamsEntry(BaseViewModel):
    """AST-inferred view from generated.view.WorkflowInstanceServiceImpl_getLocalParams_localUserDefParamsValue."""
    taskType: str | None = Field(default=None)
    localParamsList: list[Property] | None = Field(default=None)

class WorkflowInstanceParentInstanceView(BaseViewModel):
    """AST-inferred view from generated.view.WorkflowInstanceServiceImpl_queryParentInstanceBySubId_dataMap."""
    parentWorkflowInstance: int | None = Field(default=None)

class WorkflowInstanceSubWorkflowInstanceView(BaseViewModel):
    """AST-inferred view from generated.view.WorkflowInstanceServiceImpl_querySubWorkflowInstanceByTaskId_dataMap."""
    subWorkflowInstanceId: int | None = Field(default=None)

class WorkflowInstanceTaskListView(BaseViewModel):
    """AST-inferred view from generated.view.WorkflowInstanceServiceImpl_queryTaskListByWorkflowInstanceId_resultMap."""
    workflowInstanceState: str | None = Field(default=None)
    taskList: list[TaskInstanceDependentDetailsAbstractTaskInstanceContext] | None = Field(default=None)

class WorkflowInstanceVariablesView(BaseViewModel):
    """AST-inferred view from generated.view.WorkflowInstanceServiceImpl_viewVariables_resultMap."""
    globalParams: list[Property] | None = Field(default=None)
    localParams: dict[str, WorkflowInstanceLocalParamsEntry] | None = Field(default=None)

__all__ = ["WorkflowInstanceLocalParamsEntry", "WorkflowInstanceParentInstanceView", "WorkflowInstanceSubWorkflowInstanceView", "WorkflowInstanceTaskListView", "WorkflowInstanceVariablesView"]
