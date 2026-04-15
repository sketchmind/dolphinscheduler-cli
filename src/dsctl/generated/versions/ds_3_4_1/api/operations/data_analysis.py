from __future__ import annotations

from ._base import BaseRequestsClient, BaseParamsModel

from pydantic import Field, TypeAdapter

from ..contracts.command_state_count import CommandStateCount
from ..contracts.page_info import PageInfoCommand
from ..contracts.page_info import PageInfoErrorCommand
from ..views.task_instance_count import TaskInstanceCountVO
from ..views.workflow_definition_count import WorkflowDefinitionCountVO
from ..views.workflow_instance_count import WorkflowInstanceCountVO

class CountDefinitionByUserParams(BaseParamsModel):
    """
    Count Definition By User
    
    Query parameters for DataAnalysisController.countDefinitionByUser.
    """
    projectCode: int | None = Field(default=None, examples=[100])

class ListPagingParams(BaseParamsModel):
    """
    List Pending Commands
    
    Query parameters for DataAnalysisController.listPaging.
    """
    projectCode: int | None = Field(default=None)
    pageNo: int = Field(examples=[1])
    pageSize: int = Field(examples=[20])

class ListErrorCommandParams(BaseParamsModel):
    """
    List Error Command
    
    Query parameters for DataAnalysisController.listErrorCommand.
    """
    projectCode: int | None = Field(default=None)
    pageNo: int = Field(examples=[1])
    pageSize: int = Field(examples=[20])

class GetTaskInstanceStateCountParams(BaseParamsModel):
    """
    Count Task State
    
    Query parameters for DataAnalysisController.getTaskInstanceStateCount.
    """
    startDate: str | None = Field(default=None)
    endDate: str | None = Field(default=None)
    projectCode: int | None = Field(default=None, examples=[100])

class GetWorkflowInstanceStateCountParams(BaseParamsModel):
    """
    Count Workflow Instance State
    
    Query parameters for DataAnalysisController.getWorkflowInstanceStateCount.
    """
    startDate: str | None = Field(default=None)
    endDate: str | None = Field(default=None)
    projectCode: int | None = Field(default=None, examples=[100])

class DataAnalysisOperations(BaseRequestsClient):
    def count_command_state(
        self
    ) -> list[CommandStateCount]:
        """
        Count Command State
        
        Statistical command status data
        
        DS operation: DataAnalysisController.countCommandState | GET /projects/analysis/command-state-count
        
        Returns:
            command state of user projects
        """
        payload = self._request("GET", "projects/analysis/command-state-count")
        return self._validate_payload(payload, TypeAdapter(list[CommandStateCount]))

    def count_definition_by_user(
        self,
        params: CountDefinitionByUserParams
    ) -> WorkflowDefinitionCountVO:
        """
        Count Definition By User
        
        DS operation: DataAnalysisController.countDefinitionByUser | GET /projects/analysis/define-user-count
        
        Args:
            params: Query parameters bag for this operation.
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "projects/analysis/define-user-count",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(WorkflowDefinitionCountVO))

    def list_paging(
        self,
        params: ListPagingParams
    ) -> PageInfoCommand:
        """
        List Pending Commands
        
        Command queue
        
        DS operation: DataAnalysisController.listPaging | GET /projects/analysis/listCommand
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            queue state count
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "projects/analysis/listCommand",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(PageInfoCommand))

    def list_error_command(
        self,
        params: ListErrorCommandParams
    ) -> PageInfoErrorCommand:
        """
        List Error Command
        
        Error command
        
        DS operation: DataAnalysisController.listErrorCommand | GET /projects/analysis/listErrorCommand
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            queue state count
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "projects/analysis/listErrorCommand",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(PageInfoErrorCommand))

    def count_queue_state(
        self
    ) -> None:
        """
        Count Queue State
        
        Queue count
        
        DS operation: DataAnalysisController.countQueueState | GET /projects/analysis/queue-count
        
        Returns:
            queue state count
        """
        self._request("GET", "projects/analysis/queue-count")
        return None

    def get_task_instance_state_count(
        self,
        params: GetTaskInstanceStateCountParams
    ) -> TaskInstanceCountVO:
        """
        Count Task State
        
        DS operation: DataAnalysisController.getTaskInstanceStateCount | GET /projects/analysis/task-state-count
        
        Args:
            params: Query parameters bag for this operation.
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "projects/analysis/task-state-count",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(TaskInstanceCountVO))

    def get_workflow_instance_state_count(
        self,
        params: GetWorkflowInstanceStateCountParams
    ) -> WorkflowInstanceCountVO:
        """
        Count Workflow Instance State
        
        DS operation: DataAnalysisController.getWorkflowInstanceStateCount | GET /projects/analysis/workflow-state-count
        
        Args:
            params: Query parameters bag for this operation.
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "projects/analysis/workflow-state-count",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(WorkflowInstanceCountVO))

__all__ = ["DataAnalysisOperations"]
