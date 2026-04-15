from __future__ import annotations

from ._base import BaseRequestsClient

from pydantic import TypeAdapter

from ...dao.entities.task_definition import TaskDefinition
from ..contracts.page_info import PageInfoTaskDefinition
from ..contracts.task.task_filter_request import TaskFilterRequest

class TaskDefinitionV2Operations(BaseRequestsClient):
    def filter_task_definition(
        self,
        task_filter_request: TaskFilterRequest
    ) -> PageInfoTaskDefinition:
        """
        Get
        
        Get resource task definition according to query parameter
        
        DS operation: TaskDefinitionV2Controller.filterTaskDefinition | POST /v2/tasks/query
        
        Args:
            task_filter_request: Request body payload.
        
        Returns:
            PageResourceResponse from condition
        """
        payload = self._request(
            "POST",
            "v2/tasks/query",
        json=self._json_payload(task_filter_request),
        )
        return self._validate_payload(payload, TypeAdapter(PageInfoTaskDefinition))

    def get_task_definition(
        self,
        code: int
    ) -> TaskDefinition:
        """
        Get
        
        Get resource task definition by code
        
        DS operation: TaskDefinitionV2Controller.getTaskDefinition | GET /v2/tasks/{code}
        
        Args:
            code: task code of resource you want to update
        
        Returns:
            ResourceResponse object get from condition
        """
        path = f"v2/tasks/{code}"
        payload = self._request("GET", path)
        return self._validate_payload(payload, TypeAdapter(TaskDefinition))

__all__ = ["TaskDefinitionV2Operations"]
