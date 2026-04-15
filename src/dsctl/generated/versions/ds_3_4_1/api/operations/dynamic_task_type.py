from __future__ import annotations

from ._base import BaseRequestsClient

from pydantic import TypeAdapter

from ..contracts.task_type.dynamic_task_info import DynamicTaskInfo

class DynamicTaskTypeOperations(BaseRequestsClient):
    def list_dynamic_task_categories(
        self
    ) -> list[str]:
        """
        List Task Categories
        
        Get dynamic task category list
        
        DS operation: DynamicTaskTypeController.listDynamicTaskCategories | GET /dynamic/taskCategories
        
        Returns:
            dynamic task category list
        """
        payload = self._request("GET", "dynamic/taskCategories")
        return self._validate_payload(payload, TypeAdapter(list[str]))

    def list_dynamic_task_types(
        self,
        task_category: str
    ) -> list[DynamicTaskInfo]:
        """
        List Dynamic Task Types
        
        Get dynamic task category list
        
        DS operation: DynamicTaskTypeController.listDynamicTaskTypes | GET /dynamic/{taskCategory}/taskTypes
        
        Returns:
            dynamic task category list
        """
        path = f"dynamic/{task_category}/taskTypes"
        payload = self._request("GET", path)
        return self._validate_payload(payload, TypeAdapter(list[DynamicTaskInfo]))

__all__ = ["DynamicTaskTypeOperations"]
