from __future__ import annotations

from ._base import BaseRequestsClient, BaseParamsModel

from pydantic import Field, TypeAdapter

from ...dao.entities.task_group import TaskGroup
from ..contracts.page_info import PageInfoTaskGroup
from ..contracts.page_info import PageInfoTaskGroupQueue

class CloseTaskGroupParams(BaseParamsModel):
    """
    Close Task Group
    
    Form parameters for TaskGroupController.closeTaskGroup.
    """
    id: int | None = Field(default=None, description='id')

class CreateTaskGroupParams(BaseParamsModel):
    """
    Create
    
    Form parameters for TaskGroupController.createTaskGroup.
    """
    name: str = Field(description='project id')
    projectCode: int | None = Field(default=None)
    description: str = Field(description='description')
    groupSize: int = Field(description='group size')

class ForceStartParams(BaseParamsModel):
    """
    Force Start
    
    Form parameters for TaskGroupController.forceStart.
    """
    queueId: int

class QueryAllTaskGroupParams(BaseParamsModel):
    """
    List Paging
    
    Query parameters for TaskGroupController.queryAllTaskGroup.
    """
    name: str | None = Field(default=None)
    status: int | None = Field(default=None)
    pageNo: int = Field(description='page number', examples=[1])
    pageSize: int = Field(description='page size', examples=[20])

class ModifyPriorityParams(BaseParamsModel):
    """
    Modify Priority
    
    Form parameters for TaskGroupController.modifyPriority.
    """
    queueId: int = Field(description='task group queue id')
    priority: int

class QueryTaskGroupQueuesParams(BaseParamsModel):
    """
    Query Task Group Queues By Group Id
    
    Query parameters for TaskGroupController.queryTaskGroupQueues.
    """
    groupId: int | None = Field(default=None, description='ID for task group', examples=[1])
    taskInstanceName: str | None = Field(default=None, description='Task Name', examples=['taskName'])
    workflowInstanceName: str | None = Field(default=None, description='workflow instance name', examples=['workflowInstanceName'])
    status: int | None = Field(default=None, description='Task queue status', examples=[1])
    pageNo: int = Field(description='page number', examples=[1])
    pageSize: int = Field(description='page size', examples=[20])

class QueryTaskGroupByCodeParams(BaseParamsModel):
    """
    Query Task Group By Name
    
    Query parameters for TaskGroupController.queryTaskGroupByCode.
    """
    pageNo: int = Field(description='page number', examples=[1])
    projectCode: int | None = Field(default=None, description='project code')
    pageSize: int = Field(description='page size', examples=[20])

class QueryTaskGroupByStatusParams(BaseParamsModel):
    """
    Query Task Group By Status
    
    Query parameters for TaskGroupController.queryTaskGroupByStatus.
    """
    pageNo: int = Field(description='page number', examples=[1])
    status: int | None = Field(default=None, description='status')
    pageSize: int = Field(description='page size', examples=[20])

class StartTaskGroupParams(BaseParamsModel):
    """
    Start Task Group
    
    Form parameters for TaskGroupController.startTaskGroup.
    """
    id: int | None = Field(default=None, description='id')

class UpdateTaskGroupParams(BaseParamsModel):
    """
    Update
    
    Form parameters for TaskGroupController.updateTaskGroup.
    """
    id: int
    name: str = Field(description='project id')
    description: str = Field(description='description')
    groupSize: int = Field(description='group size')

class TaskGroupOperations(BaseRequestsClient):
    def close_task_group(
        self,
        form: CloseTaskGroupParams
    ) -> None:
        """
        Close Task Group
        
        Close a task group
        
        DS operation: TaskGroupController.closeTaskGroup | POST /task-group/close-task-group
        
        Args:
            form: Form parameters bag for this operation.
        
        Returns:
            result
        """
        data = self._model_mapping(form)
        self._request(
            "POST",
            "task-group/close-task-group",
        data=data,
        )
        return None

    def create_task_group(
        self,
        form: CreateTaskGroupParams
    ) -> TaskGroup:
        """
        Create
        
        Query task group list
        
        DS operation: TaskGroupController.createTaskGroup | POST /task-group/create
        
        Args:
            form: Form parameters bag for this operation.
        
        Returns:
            result and msg code
        """
        data = self._model_mapping(form)
        payload = self._request(
            "POST",
            "task-group/create",
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(TaskGroup))

    def force_start(
        self,
        form: ForceStartParams
    ) -> None:
        """
        Force Start
        
        Force start task without task group
        
        DS operation: TaskGroupController.forceStart | POST /task-group/forceStart
        
        Args:
            form: Form parameters bag for this operation.
        
        Returns:
            result
        """
        data = self._model_mapping(form)
        self._request(
            "POST",
            "task-group/forceStart",
        data=data,
        )
        return None

    def query_all_task_group(
        self,
        params: QueryAllTaskGroupParams
    ) -> PageInfoTaskGroup:
        """
        List Paging
        
        Query task group list paging
        
        DS operation: TaskGroupController.queryAllTaskGroup | GET /task-group/list-paging
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            queue list
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "task-group/list-paging",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(PageInfoTaskGroup))

    def modify_priority(
        self,
        form: ModifyPriorityParams
    ) -> None:
        """
        Modify Priority
        
        Force start task without task group
        
        DS operation: TaskGroupController.modifyPriority | POST /task-group/modifyPriority
        
        Args:
            form: Form parameters bag for this operation.
        
        Returns:
            result
        """
        data = self._model_mapping(form)
        self._request(
            "POST",
            "task-group/modifyPriority",
        data=data,
        )
        return None

    def query_task_group_queues(
        self,
        params: QueryTaskGroupQueuesParams
    ) -> PageInfoTaskGroupQueue:
        """
        Query Task Group Queues By Group Id
        
        Query task group queue list paging
        
        DS operation: TaskGroupController.queryTaskGroupQueues | GET /task-group/query-list-by-group-id
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            queue list
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "task-group/query-list-by-group-id",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(PageInfoTaskGroupQueue))

    def query_task_group_by_code(
        self,
        params: QueryTaskGroupByCodeParams
    ) -> PageInfoTaskGroup:
        """
        Query Task Group By Name
        
        Query task group list paging by project code
        
        DS operation: TaskGroupController.queryTaskGroupByCode | GET /task-group/query-list-by-projectCode
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            queue list
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "task-group/query-list-by-projectCode",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(PageInfoTaskGroup))

    def query_task_group_by_status(
        self,
        params: QueryTaskGroupByStatusParams
    ) -> PageInfoTaskGroup:
        """
        Query Task Group By Status
        
        Query task group list paging
        
        DS operation: TaskGroupController.queryTaskGroupByStatus | GET /task-group/query-list-by-status
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            queue list
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "task-group/query-list-by-status",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(PageInfoTaskGroup))

    def start_task_group(
        self,
        form: StartTaskGroupParams
    ) -> None:
        """
        Start Task Group
        
        Start a task group
        
        DS operation: TaskGroupController.startTaskGroup | POST /task-group/start-task-group
        
        Args:
            form: Form parameters bag for this operation.
        
        Returns:
            result
        """
        data = self._model_mapping(form)
        self._request(
            "POST",
            "task-group/start-task-group",
        data=data,
        )
        return None

    def update_task_group(
        self,
        form: UpdateTaskGroupParams
    ) -> TaskGroup:
        """
        Update
        
        Update task group list
        
        DS operation: TaskGroupController.updateTaskGroup | POST /task-group/update
        
        Args:
            form: Form parameters bag for this operation.
        
        Returns:
            result and msg code
        """
        data = self._model_mapping(form)
        payload = self._request(
            "POST",
            "task-group/update",
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(TaskGroup))

__all__ = ["TaskGroupOperations"]
