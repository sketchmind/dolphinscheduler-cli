from __future__ import annotations

from ._base import BaseRequestsClient, BaseParamsModel

from pydantic import Field, TypeAdapter

from ...common.enums.task_execute_type import TaskExecuteType
from ...plugin.task_api.enums.task_execution_status import TaskExecutionStatus
from ..contracts.page_info import PageInfoTaskInstance

class QueryTaskListPagingParams(BaseParamsModel):
    """
    Query Task List Paging
    
    Query parameters for TaskInstanceController.queryTaskListPaging.
    """
    workflowInstanceId: int | None = Field(default=None, description='workflow instance id', examples=[100])
    workflowInstanceName: str | None = Field(default=None)
    workflowDefinitionName: str | None = Field(default=None)
    searchVal: str | None = Field(default=None, description='search value')
    taskName: str | None = Field(default=None, description='task name')
    taskCode: int | None = Field(default=None)
    executorName: str | None = Field(default=None)
    stateType: TaskExecutionStatus | None = Field(default=None, description='state type')
    host: str | None = Field(default=None, description='host')
    startDate: str | None = Field(default=None, description='start time')
    endDate: str | None = Field(default=None, description='end time')
    taskExecuteType: TaskExecuteType | None = Field(default=None, description='task execute type', examples=['STREAM'])
    pageNo: int = Field(description='page number', examples=[1])
    pageSize: int = Field(description='page size', examples=[20])

class TaskInstanceOperations(BaseRequestsClient):
    def query_task_list_paging(
        self,
        project_code: int,
        params: QueryTaskListPagingParams
    ) -> PageInfoTaskInstance:
        """
        Query Task List Paging
        
        Query task list paging
        
        DS operation: TaskInstanceController.queryTaskListPaging | GET /projects/{projectCode}/task-instances
        
        Args:
            project_code: project code
            params: Query parameters bag for this operation.
        
        Returns:
            task list page
        """
        path = f"projects/{project_code}/task-instances"
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            path,
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(PageInfoTaskInstance))

    def force_task_success(
        self,
        project_code: int,
        id: int
    ) -> None:
        """
        Force Success
        
        Change one task instance's state from FAILURE to FORCED_SUCCESS
        
        DS operation: TaskInstanceController.forceTaskSuccess | POST /projects/{projectCode}/task-instances/{id}/force-success
        
        Args:
            project_code: project code
            id: task instance id
        
        Returns:
            the result code and msg
        """
        path = f"projects/{project_code}/task-instances/{id}/force-success"
        self._request("POST", path)
        return None

    def task_save_point(
        self,
        project_code: int,
        id: int
    ) -> None:
        """
        Savepoint
        
        Task savepoint, for stream task
        
        DS operation: TaskInstanceController.taskSavePoint | POST /projects/{projectCode}/task-instances/{id}/savepoint
        
        Args:
            project_code: project code
            id: task instance id
        
        Returns:
            the result code and msg
        """
        path = f"projects/{project_code}/task-instances/{id}/savepoint"
        self._request("POST", path)
        return None

    def stop_task(
        self,
        project_code: int,
        id: int
    ) -> None:
        """
        Stop
        
        Task stop, for stream task
        
        DS operation: TaskInstanceController.stopTask | POST /projects/{projectCode}/task-instances/{id}/stop
        
        Args:
            project_code: project code
            id: task instance id
        
        Returns:
            the result code and msg
        """
        path = f"projects/{project_code}/task-instances/{id}/stop"
        self._request("POST", path)
        return None

__all__ = ["TaskInstanceOperations"]
