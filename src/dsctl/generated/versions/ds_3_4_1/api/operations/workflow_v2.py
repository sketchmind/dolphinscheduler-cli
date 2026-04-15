from __future__ import annotations

from ._base import BaseRequestsClient

from pydantic import TypeAdapter

from ...dao.entities.workflow_definition import WorkflowDefinition
from ..contracts.page_info import PageInfoWorkflowDefinition
from ..contracts.workflow.workflow_create_request import WorkflowCreateRequest
from ..contracts.workflow.workflow_filter_request import WorkflowFilterRequest
from ..contracts.workflow.workflow_update_request import WorkflowUpdateRequest

class WorkflowV2Operations(BaseRequestsClient):
    def create_workflow(
        self,
        workflow_create_request: WorkflowCreateRequest
    ) -> WorkflowDefinition:
        """
        Create
        
        Create resource workflow
        
        DS operation: WorkflowV2Controller.createWorkflow | POST /v2/workflows
        
        Args:
            workflow_create_request: Request body payload.
        
        Returns:
            ResourceResponse object created
        """
        payload = self._request(
            "POST",
            "v2/workflows",
        json=self._json_payload(workflow_create_request),
        )
        return self._validate_payload(payload, TypeAdapter(WorkflowDefinition))

    def filter_workflows(
        self,
        workflow_filter_request: WorkflowFilterRequest
    ) -> PageInfoWorkflowDefinition:
        """
        Get
        
        Get resource workflows according to query parameter
        
        DS operation: WorkflowV2Controller.filterWorkflows | POST /v2/workflows/query
        
        Args:
            workflow_filter_request: Request body payload.
        
        Returns:
            PageResourceResponse from condition
        """
        payload = self._request(
            "POST",
            "v2/workflows/query",
        json=self._json_payload(workflow_filter_request),
        )
        return self._validate_payload(payload, TypeAdapter(PageInfoWorkflowDefinition))

    def delete_workflow(
        self,
        code: int
    ) -> None:
        """
        Delete
        
        Delete workflow by code
        
        DS operation: WorkflowV2Controller.deleteWorkflow | DELETE /v2/workflows/{code}
        
        Args:
            code: workflow definition code
        
        Returns:
            Result result object delete
        """
        path = f"v2/workflows/{code}"
        self._request("DELETE", path)
        return None

    def get_workflow(
        self,
        code: int
    ) -> WorkflowDefinition:
        """
        Get
        
        Get resource workflow
        
        DS operation: WorkflowV2Controller.getWorkflow | GET /v2/workflows/{code}
        
        Args:
            code: workflow resource code you want to update
        
        Returns:
            ResourceResponse object get from condition
        """
        path = f"v2/workflows/{code}"
        payload = self._request("GET", path)
        return self._validate_payload(payload, TypeAdapter(WorkflowDefinition))

    def update_workflow(
        self,
        code: int,
        workflow_update_request: WorkflowUpdateRequest
    ) -> WorkflowDefinition:
        """
        Update
        
        Update resource workflow
        
        DS operation: WorkflowV2Controller.updateWorkflow | PUT /v2/workflows/{code}
        
        Args:
            code: workflow resource code you want to update
            workflow_update_request: Request body payload.
        
        Returns:
            ResourceResponse object updated
        """
        path = f"v2/workflows/{code}"
        payload = self._request(
            "PUT",
            path,
        json=self._json_payload(workflow_update_request),
        )
        return self._validate_payload(payload, TypeAdapter(WorkflowDefinition))

__all__ = ["WorkflowV2Operations"]
