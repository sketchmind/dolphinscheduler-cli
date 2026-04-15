from __future__ import annotations

from ._base import BaseRequestsClient

from pydantic import TypeAdapter

from ...dao.entities.workflow_instance import WorkflowInstance
from ..contracts.page_info import PageInfoWorkflowInstance
from ..contracts.workflow_instance.workflow_instance_query_request import WorkflowInstanceQueryRequest
from ..enums.execute_type import ExecuteType

class WorkflowInstanceV2Operations(BaseRequestsClient):
    def query_workflow_instance_list_paging(
        self,
        workflow_instance_query_request: WorkflowInstanceQueryRequest
    ) -> PageInfoWorkflowInstance:
        """
        Query Workflow Instance List Paging
        
        Query workflow instance list paging
        
        DS operation: WorkflowInstanceV2Controller.queryWorkflowInstanceListPaging | GET /v2/workflow-instances
        
        Args:
            workflow_instance_query_request: Request body payload.
        
        Returns:
            workflow instance list
        """
        payload = self._request(
            "GET",
            "v2/workflow-instances",
        json=self._json_payload(workflow_instance_query_request),
        )
        return self._validate_payload(payload, TypeAdapter(PageInfoWorkflowInstance))

    def delete_workflow_instance(
        self,
        workflow_instance_id: int
    ) -> None:
        """
        Delete
        
        Delete workflowInstance by id
        
        DS operation: WorkflowInstanceV2Controller.deleteWorkflowInstance | DELETE /v2/workflow-instances/{workflowInstanceId}
        
        Args:
            workflow_instance_id: workflow instance code
        
        Returns:
            Result result object delete
        """
        path = f"v2/workflow-instances/{workflow_instance_id}"
        self._request("DELETE", path)
        return None

    def query_workflow_instance_by_id(
        self,
        workflow_instance_id: int
    ) -> WorkflowInstance:
        """
        Query Workflow Instance By Id
        
        Query workflowInstance by id
        
        DS operation: WorkflowInstanceV2Controller.queryWorkflowInstanceById | GET /v2/workflow-instances/{workflowInstanceId}
        
        Args:
            workflow_instance_id: workflow instance id
        
        Returns:
            Result result object query
        """
        path = f"v2/workflow-instances/{workflow_instance_id}"
        payload = self._request("GET", path)
        return self._validate_payload(payload, TypeAdapter(WorkflowInstance))

    def execute(
        self,
        workflow_instance_id: int,
        execute_type: ExecuteType
    ) -> None:
        """
        Execute
        
        Do action to workflow instance: pause, stop, repeat, recover from pause, recover from stop
        
        DS operation: WorkflowInstanceV2Controller.execute | POST /v2/workflow-instances/{workflowInstanceId}/execute/{executeType}
        
        Args:
            workflow_instance_id: workflow instance id
            execute_type: execute type
        
        Returns:
            execute result code
        """
        path = f"v2/workflow-instances/{workflow_instance_id}/execute/{execute_type}"
        self._request("POST", path)
        return None

__all__ = ["WorkflowInstanceV2Operations"]
