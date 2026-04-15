from __future__ import annotations

from ._base import BaseRequestsClient

from pydantic import TypeAdapter

from ...dao.entities.workflow_task_relation import WorkflowTaskRelation
from ..contracts.task_relation.task_relation_create_request import TaskRelationCreateRequest
from ..contracts.task_relation.task_relation_update_upstream_request import TaskRelationUpdateUpstreamRequest

class WorkflowTaskRelationV2Operations(BaseRequestsClient):
    def create_task_relation(
        self,
        task_relation_create_request: TaskRelationCreateRequest
    ) -> WorkflowTaskRelation:
        """
        Create
        
        Create resource workflow task relation
        
        DS operation: WorkflowTaskRelationV2Controller.createTaskRelation | POST /v2/relations
        
        Args:
            task_relation_create_request: Request body payload.
        
        Returns:
            Result object created
        """
        payload = self._request(
            "POST",
            "v2/relations",
        json=self._json_payload(task_relation_create_request),
        )
        return self._validate_payload(payload, TypeAdapter(WorkflowTaskRelation))

    def delete_task_relation(
        self,
        code_pair: str
    ) -> None:
        """
        Delete
        
        Delete resource workflow task relation
        
        DS operation: WorkflowTaskRelationV2Controller.deleteTaskRelation | DELETE /v2/relations/{code-pair}
        
        Args:
            code_pair: code pair you want to delete the task relation, use `upstream,downstream` as example, will delete exists relation upstream -> downstream, throw error if not exists
        
        Returns:
            delete result code
        """
        path = f"v2/relations/{code_pair}"
        self._request("DELETE", path)
        return None

    def update_upstream_task_definition(
        self,
        code: int,
        task_relation_update_upstream_request: TaskRelationUpdateUpstreamRequest
    ) -> list[WorkflowTaskRelation]:
        """
        Update
        
        Update resource task relation by code, only update this code's upstreams
        
        DS operation: WorkflowTaskRelationV2Controller.updateUpstreamTaskDefinition | PUT /v2/relations/{code}
        
        Args:
            code: resource task code want to update its upstreams
            task_relation_update_upstream_request: Request body payload.
        
        Returns:
            ResourceResponse object updated
        """
        path = f"v2/relations/{code}"
        payload = self._request(
            "PUT",
            path,
        json=self._json_payload(task_relation_update_upstream_request),
        )
        return self._validate_payload(payload, TypeAdapter(list[WorkflowTaskRelation]))

__all__ = ["WorkflowTaskRelationV2Operations"]
