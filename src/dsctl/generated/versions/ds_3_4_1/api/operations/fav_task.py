from __future__ import annotations

from ._base import BaseRequestsClient

from pydantic import TypeAdapter

from ..contracts.fav_task_dto import FavTaskDto

class FavTaskOperations(BaseRequestsClient):
    def list_task_type(
        self
    ) -> list[FavTaskDto]:
        """
        List Task Type
        
        Get task type list
        
        DS operation: FavTaskController.listTaskType | GET /favourite/taskTypes
        
        Returns:
            task type list
        """
        payload = self._request("GET", "favourite/taskTypes")
        return self._validate_payload(payload, TypeAdapter(list[FavTaskDto]))

    def delete_fav_task(
        self,
        task_type: str
    ) -> bool:
        """
        Delete Task Type
        
        Delete task fav
        
        DS operation: FavTaskController.deleteFavTask | DELETE /favourite/{taskType}
        """
        path = f"favourite/{task_type}"
        payload = self._request("DELETE", path)
        return self._validate_payload(payload, TypeAdapter(bool))

    def add_fav_task(
        self,
        task_type: str
    ) -> bool:
        """
        Add Task Type
        
        Add task fav
        
        DS operation: FavTaskController.addFavTask | POST /favourite/{taskType}
        """
        path = f"favourite/{task_type}"
        payload = self._request("POST", path)
        return self._validate_payload(payload, TypeAdapter(bool))

__all__ = ["FavTaskOperations"]
