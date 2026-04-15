from __future__ import annotations

from ._base import BaseRequestsClient, BaseParamsModel

from pydantic import Field, TypeAdapter

from ...dao.entities.task_definition_log import TaskDefinitionLog
from ...dao.entities.workflow_definition import WorkflowDefinition

class CreateWorkflowTaskRelationParams(BaseParamsModel):
    """
    Save
    
    Form parameters for WorkflowTaskRelationController.createWorkflowTaskRelation.
    """
    workflowDefinitionCode: int = Field(description='workflowDefinitionCode')
    preTaskCode: int = Field(description='preTaskCode')
    postTaskCode: int = Field(description='postTaskCode')

class DeleteTaskWorkflowRelationParams(BaseParamsModel):
    """
    Delete Relation
    
    Query parameters for WorkflowTaskRelationController.deleteTaskWorkflowRelation.
    """
    workflowDefinitionCode: int = Field(description='workflow definition code')

class DeleteDownstreamRelationParams(BaseParamsModel):
    """
    Delete Downstream Relation
    
    Query parameters for WorkflowTaskRelationController.deleteDownstreamRelation.
    """
    postTaskCodes: str = Field(description="the post task codes, sep ','", examples=['1,2'])

class DeleteUpstreamRelationParams(BaseParamsModel):
    """
    Delete Upstream Relation
    
    Query parameters for WorkflowTaskRelationController.deleteUpstreamRelation.
    """
    preTaskCodes: str = Field(description="the pre task codes, sep ','", examples=['1,2'])

class WorkflowTaskRelationOperations(BaseRequestsClient):
    def create_workflow_task_relation(
        self,
        project_code: int,
        form: CreateWorkflowTaskRelationParams
    ) -> None:
        """
        Save
        
        Create workflow task relation
        
        DS operation: WorkflowTaskRelationController.createWorkflowTaskRelation | POST /projects/{projectCode}/workflow-task-relation
        
        Args:
            project_code: project code
            form: Form parameters bag for this operation.
        
        Returns:
            create result code
        """
        path = f"projects/{project_code}/workflow-task-relation"
        data = self._model_mapping(form)
        self._request(
            "POST",
            path,
        data=data,
        )
        return None

    def delete_task_workflow_relation(
        self,
        project_code: int,
        task_code: int,
        params: DeleteTaskWorkflowRelationParams
    ) -> WorkflowDefinition:
        """
        Delete Relation
        
        Delete workflow task relation (delete task from workflow)
        
        DS operation: WorkflowTaskRelationController.deleteTaskWorkflowRelation | DELETE /projects/{projectCode}/workflow-task-relation/{taskCode}
        
        Args:
            project_code: project code
            task_code: the post task code
            params: Query parameters bag for this operation.
        
        Returns:
            delete result code
        """
        path = f"projects/{project_code}/workflow-task-relation/{task_code}"
        query_params = self._model_mapping(params)
        payload = self._request(
            "DELETE",
            path,
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(WorkflowDefinition))

    def delete_downstream_relation(
        self,
        project_code: int,
        task_code: int,
        params: DeleteDownstreamRelationParams
    ) -> WorkflowDefinition:
        """
        Delete Downstream Relation
        
        Delete task downstream relation
        
        DS operation: WorkflowTaskRelationController.deleteDownstreamRelation | DELETE /projects/{projectCode}/workflow-task-relation/{taskCode}/downstream
        
        Args:
            project_code: project code
            task_code: the pre task code
            params: Query parameters bag for this operation.
        
        Returns:
            delete result code
        """
        path = f"projects/{project_code}/workflow-task-relation/{task_code}/downstream"
        query_params = self._model_mapping(params)
        payload = self._request(
            "DELETE",
            path,
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(WorkflowDefinition))

    def query_downstream_relation(
        self,
        project_code: int,
        task_code: int
    ) -> list[TaskDefinitionLog]:
        """
        Query Downstream Relation
        
        Query task downstream relation
        
        DS operation: WorkflowTaskRelationController.queryDownstreamRelation | GET /projects/{projectCode}/workflow-task-relation/{taskCode}/downstream
        
        Args:
            project_code: project code
            task_code: pre task code
        
        Returns:
            workflow task relation list
        """
        path = f"projects/{project_code}/workflow-task-relation/{task_code}/downstream"
        payload = self._request("GET", path)
        return self._validate_payload(payload, TypeAdapter(list[TaskDefinitionLog]))

    def delete_upstream_relation(
        self,
        project_code: int,
        task_code: int,
        params: DeleteUpstreamRelationParams
    ) -> WorkflowDefinition:
        """
        Delete Upstream Relation
        
        Delete task upstream relation
        
        DS operation: WorkflowTaskRelationController.deleteUpstreamRelation | DELETE /projects/{projectCode}/workflow-task-relation/{taskCode}/upstream
        
        Args:
            project_code: project code
            task_code: the post task code
            params: Query parameters bag for this operation.
        
        Returns:
            delete result code
        """
        path = f"projects/{project_code}/workflow-task-relation/{task_code}/upstream"
        query_params = self._model_mapping(params)
        payload = self._request(
            "DELETE",
            path,
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(WorkflowDefinition))

    def query_upstream_relation(
        self,
        project_code: int,
        task_code: int
    ) -> list[TaskDefinitionLog]:
        """
        Query Upstream Relation
        
        Query task upstream relation
        
        DS operation: WorkflowTaskRelationController.queryUpstreamRelation | GET /projects/{projectCode}/workflow-task-relation/{taskCode}/upstream
        
        Args:
            project_code: project code
            task_code: current task code (post task code)
        
        Returns:
            workflow task relation list
        """
        path = f"projects/{project_code}/workflow-task-relation/{task_code}/upstream"
        payload = self._request("GET", path)
        return self._validate_payload(payload, TypeAdapter(list[TaskDefinitionLog]))

    def delete_edge(
        self,
        project_code: int,
        workflow_definition_code: int,
        pre_task_code: int,
        post_task_code: int
    ) -> WorkflowDefinition:
        """
        Delete Edge
        
        Delete edge
        
        DS operation: WorkflowTaskRelationController.deleteEdge | DELETE /projects/{projectCode}/workflow-task-relation/{workflowDefinitionCode}/{preTaskCode}/{postTaskCode}
        
        Args:
            project_code: project code
            workflow_definition_code: workflow definition code
            pre_task_code: pre task code
            post_task_code: post task code
        
        Returns:
            delete result code
        """
        path = f"projects/{project_code}/workflow-task-relation/{workflow_definition_code}/{pre_task_code}/{post_task_code}"
        payload = self._request("DELETE", path)
        return self._validate_payload(payload, TypeAdapter(WorkflowDefinition))

__all__ = ["WorkflowTaskRelationOperations"]
