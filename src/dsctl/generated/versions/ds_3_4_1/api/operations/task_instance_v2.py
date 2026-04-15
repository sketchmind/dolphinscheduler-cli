from __future__ import annotations

from ._base import BaseRequestsClient

from pydantic import TypeAdapter

from ...dao.entities.task_instance import TaskInstance
from ..contracts.page_info import PageInfoTaskInstance
from ..contracts.task_instance.task_instance_query_request import TaskInstanceQueryRequest

class TaskInstanceV2Operations(BaseRequestsClient):
    def query_task_list_paging(
        self,
        project_code: int,
        request: TaskInstanceQueryRequest
    ) -> PageInfoTaskInstance:
        """
        Query Task List Paging
        
        Query task list paging
        
        DS operation: TaskInstanceV2Controller.queryTaskListPaging | GET /v2/projects/{projectCode}/task-instances
        
        Args:
            project_code: project code
            request: Request payload.
        
        Returns:
            task list page
        """
        path = f"v2/projects/{project_code}/task-instances"
        query_params = self._model_mapping(request)
        headers = {"Content-Type": "application/json"}
        payload = self._request(
            "GET",
            path,
        params=query_params,
        headers=headers,
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
        
        DS operation: TaskInstanceV2Controller.forceTaskSuccess | POST /v2/projects/{projectCode}/task-instances/{id}/force-success
        
        Args:
            project_code: project code
            id: task instance id
        
        Returns:
            the result code and msg
        """
        path = f"v2/projects/{project_code}/task-instances/{id}/force-success"
        headers = {"Content-Type": "application/json"}
        self._request(
            "POST",
            path,
        headers=headers,
        )
        return None

    def task_save_point(
        self,
        project_code: int,
        id: int
    ) -> None:
        """
        Savepoint
        
        Task savepoint, for stream task
        
        DS operation: TaskInstanceV2Controller.taskSavePoint | POST /v2/projects/{projectCode}/task-instances/{id}/savepoint
        
        Args:
            project_code: project code
            id: task instance id
        
        Returns:
            the result code and msg
        """
        path = f"v2/projects/{project_code}/task-instances/{id}/savepoint"
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
        
        DS operation: TaskInstanceV2Controller.stopTask | POST /v2/projects/{projectCode}/task-instances/{id}/stop
        
        Args:
            project_code: project code
            id: task instance id
        
        Returns:
            the result code and msg
        """
        path = f"v2/projects/{project_code}/task-instances/{id}/stop"
        self._request("POST", path)
        return None

    def query_task_instance_by_code(
        self,
        project_code: int,
        task_instance_id: int
    ) -> TaskInstance:
        """
        Query One Task Instance
        
        Query taskInstance by taskInstanceCode
        
        DS operation: TaskInstanceV2Controller.queryTaskInstanceByCode | POST /v2/projects/{projectCode}/task-instances/{taskInstanceId}
        
        Args:
            project_code: project code
            task_instance_id: taskInstance Id
        
        Returns:
            the result code and msg
        """
        path = f"v2/projects/{project_code}/task-instances/{task_instance_id}"
        headers = {"Content-Type": "application/json"}
        payload = self._request(
            "POST",
            path,
        headers=headers,
        )
        return self._validate_payload(payload, TypeAdapter(TaskInstance))

__all__ = ["TaskInstanceV2Operations"]
