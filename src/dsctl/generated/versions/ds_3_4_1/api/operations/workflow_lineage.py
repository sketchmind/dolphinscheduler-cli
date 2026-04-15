from __future__ import annotations

from ._base import BaseRequestsClient, BaseParamsModel

from pydantic import Field, TypeAdapter

from ...dao.entities.dependent_lineage_task import DependentLineageTask
from ...dao.entities.work_flow_lineage import WorkFlowLineage
from ...dao.entities.work_flow_relation_detail import WorkFlowRelationDetail

class QueryWorkFlowLineageByNameParams(BaseParamsModel):
    """
    Query Lineage By Work Flow Name
    
    Query parameters for WorkflowLineageController.queryWorkFlowLineageByName.
    """
    workflowDefinitionName: str | None = Field(default=None)

class QueryDependentTasksParams(BaseParamsModel):
    """
    Verify Task Can Delete
    
    Query parameters for WorkflowLineageController.queryDependentTasks.
    """
    workFlowCode: int
    taskCode: int | None = Field(default=None)

class VerifyTaskCanDeleteParams(BaseParamsModel):
    """
    Verify Task Can Delete
    
    Form parameters for WorkflowLineageController.verifyTaskCanDelete.
    """
    workflowDefinitionCode: int = Field(description='project code which taskCode belong')
    taskCode: int = Field(description='task definition code', examples=[123456789])

class WorkflowLineageOperations(BaseRequestsClient):
    def query_work_flow_lineage(
        self,
        project_code: int
    ) -> WorkFlowLineage:
        """
        Query Work Flow List
        
        DS operation: WorkflowLineageController.queryWorkFlowLineage | GET /projects/{projectCode}/lineages/list
        """
        path = f"projects/{project_code}/lineages/list"
        payload = self._request("GET", path)
        payload = self._project_single_data(payload)
        return self._validate_payload(payload, TypeAdapter(WorkFlowLineage))

    def query_work_flow_lineage_by_name(
        self,
        project_code: int,
        params: QueryWorkFlowLineageByNameParams
    ) -> list[WorkFlowRelationDetail]:
        """
        Query Lineage By Work Flow Name
        
        DS operation: WorkflowLineageController.queryWorkFlowLineageByName | GET /projects/{projectCode}/lineages/query-by-name
        
        Args:
            params: Query parameters bag for this operation.
        """
        path = f"projects/{project_code}/lineages/query-by-name"
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            path,
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(list[WorkFlowRelationDetail]))

    def query_dependent_tasks(
        self,
        project_code: int,
        params: QueryDependentTasksParams
    ) -> list[DependentLineageTask]:
        """
        Verify Task Can Delete
        
        Whether task can be deleted or not, avoiding task depend on other task of workflow definition delete by accident.
        
        DS operation: WorkflowLineageController.queryDependentTasks | GET /projects/{projectCode}/lineages/query-dependent-tasks
        
        Args:
            params: Query parameters bag for this operation.
        """
        path = f"projects/{project_code}/lineages/query-dependent-tasks"
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            path,
        params=query_params,
        )
        payload = self._project_single_data(payload)
        return self._validate_payload(payload, TypeAdapter(list[DependentLineageTask]))

    def verify_task_can_delete(
        self,
        project_code: int,
        form: VerifyTaskCanDeleteParams
    ) -> None:
        """
        Verify Task Can Delete
        
        Whether task can be deleted or not, avoiding task depend on other task of workflow definition delete by accident.
        
        DS operation: WorkflowLineageController.verifyTaskCanDelete | POST /projects/{projectCode}/lineages/tasks/verify-delete
        
        Args:
            project_code: project codes which taskCode belong
            form: Form parameters bag for this operation.
        
        Returns:
            Result of task can be deleted or not
        """
        path = f"projects/{project_code}/lineages/tasks/verify-delete"
        data = self._model_mapping(form)
        self._request(
            "POST",
            path,
        data=data,
        )
        return None

    def query_work_flow_lineage_by_code(
        self,
        project_code: int,
        work_flow_code: int
    ) -> WorkFlowLineage:
        """
        Query Lineage By Work Flow Code
        
        DS operation: WorkflowLineageController.queryWorkFlowLineageByCode | GET /projects/{projectCode}/lineages/{workFlowCode}
        """
        path = f"projects/{project_code}/lineages/{work_flow_code}"
        payload = self._request("GET", path)
        payload = self._project_single_data(payload)
        return self._validate_payload(payload, TypeAdapter(WorkFlowLineage))

__all__ = ["WorkflowLineageOperations"]
