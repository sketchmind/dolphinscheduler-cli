from __future__ import annotations

from ._base import BaseRequestsClient, BaseParamsModel

from pydantic import Field, TypeAdapter

from ...common.enums.release_state import ReleaseState
from ...common.enums.workflow_execution_type_enum import WorkflowExecutionTypeEnum
from ...dao.entities.dag_data import DagData
from ...dao.entities.dependent_simplify_definition import DependentSimplifyDefinition
from ...dao.entities.task_definition import TaskDefinition
from ...dao.entities.workflow_definition import WorkflowDefinition
from ..contracts.page_info import PageInfoWorkflowDefinition
from ..contracts.page_info import PageInfoWorkflowDefinitionLog
from ..contracts.treeview.tree_view_dto import TreeViewDto
from ..views.workflow_definition import WorkflowDefinitionSimpleItem
from ..views.workflow_definition import WorkflowDefinitionVariablesView

class QueryWorkflowDefinitionListPagingParams(BaseParamsModel):
    """
    Query List Paging
    
    Query parameters for WorkflowDefinitionController.queryWorkflowDefinitionListPaging.
    """
    searchVal: str | None = Field(default=None, description='search value')
    otherParamsJson: str | None = Field(default=None, description='otherParamsJson handle other params')
    userId: int | None = Field(default=None, description='user id', examples=[100])
    pageNo: int = Field(description='page number', examples=[1])
    pageSize: int = Field(description='page size', examples=[10])

class CreateWorkflowDefinitionParams(BaseParamsModel):
    """
    Create Workflow Definition
    
    Form parameters for WorkflowDefinitionController.createWorkflowDefinition.
    """
    name: str = Field(description='workflow definition name')
    description: str | None = Field(default=None, description='description')
    globalParams: str | None = Field(default=None, description='globalParams')
    locations: str | None = Field(default=None, description='locations for nodes')
    timeout: int | None = Field(default=None, description='timeout')
    taskRelationJson: str = Field(description='relation json for nodes')
    taskDefinitionJson: str = Field(description='taskDefinitionJson')
    otherParamsJson: str | None = Field(default=None, description='otherParamsJson handle other params')
    executionType: WorkflowExecutionTypeEnum | None = Field(default=None)

class CopyWorkflowDefinitionParams(BaseParamsModel):
    """
    Batch Copy By Codes
    
    Form parameters for WorkflowDefinitionController.copyWorkflowDefinition.
    """
    codes: str = Field(description='workflow definition codes', examples=['3,4'])
    targetProjectCode: int = Field(description='target project code', examples=[123])

class BatchDeleteWorkflowDefinitionByCodesParams(BaseParamsModel):
    """
    Batch Delete By Codes
    
    Form parameters for WorkflowDefinitionController.batchDeleteWorkflowDefinitionByCodes.
    """
    codes: str = Field(description='workflow definition code list')

class MoveWorkflowDefinitionParams(BaseParamsModel):
    """
    Batch Move By Codes
    
    Form parameters for WorkflowDefinitionController.moveWorkflowDefinition.
    """
    codes: str = Field(description='workflow definition codes', examples=['3,4'])
    targetProjectCode: int = Field(description='target project code', examples=[123])

class GetNodeListMapByDefinitionCodesParams(BaseParamsModel):
    """
    Get Task List By Definition Codes
    
    Query parameters for WorkflowDefinitionController.getNodeListMapByDefinitionCodes.
    """
    codes: str = Field(description='workflow definition codes', examples=['100,200,300'])

class QueryWorkflowDefinitionByNameParams(BaseParamsModel):
    """
    Query Workflow Definition By Name
    
    Query parameters for WorkflowDefinitionController.queryWorkflowDefinitionByName.
    """
    name: str = Field(description='workflow definition name')

class GetTaskListByWorkflowDefinitionCodeParams(BaseParamsModel):
    """
    Get Task List By Workflow Definition Code
    
    Query parameters for WorkflowDefinitionController.getTaskListByWorkflowDefinitionCode.
    """
    workflowDefinitionCode: int = Field(examples=[100])

class VerifyWorkflowDefinitionNameParams(BaseParamsModel):
    """
    Verify Name
    
    Query parameters for WorkflowDefinitionController.verifyWorkflowDefinitionName.
    """
    name: str = Field(description='name')
    workflowDefinitionCode: int | None = Field(default=None)

class UpdateWorkflowDefinitionParams(BaseParamsModel):
    """
    Update
    
    Form parameters for WorkflowDefinitionController.updateWorkflowDefinition.
    """
    name: str = Field(description='workflow definition name')
    description: str | None = Field(default=None, description='description')
    globalParams: str | None = Field(default=None, description='globalParams')
    locations: str | None = Field(default=None, description='locations for nodes')
    timeout: int | None = Field(default=None, description='timeout')
    taskRelationJson: str = Field(description='relation json for nodes')
    taskDefinitionJson: str = Field(description='taskDefinitionJson')
    executionType: WorkflowExecutionTypeEnum | None = Field(default=None)
    releaseState: ReleaseState | None = Field(default=None)

class ReleaseWorkflowDefinitionParams(BaseParamsModel):
    """
    Release
    
    Form parameters for WorkflowDefinitionController.releaseWorkflowDefinition.
    """
    releaseState: ReleaseState

class QueryWorkflowDefinitionVersionsParams(BaseParamsModel):
    """
    Query Versions
    
    Query parameters for WorkflowDefinitionController.queryWorkflowDefinitionVersions.
    """
    pageNo: int = Field(description='the workflow definition version list current page number', examples=[1])
    pageSize: int = Field(description='the workflow definition version list page size', examples=[10])

class ViewTreeParams(BaseParamsModel):
    """
    View Tree
    
    Query parameters for WorkflowDefinitionController.viewTree.
    """
    limit: int = Field(description='limit', examples=[100])

class WorkflowDefinitionOperations(BaseRequestsClient):
    def query_workflow_definition_list_paging(
        self,
        project_code: int,
        params: QueryWorkflowDefinitionListPagingParams
    ) -> PageInfoWorkflowDefinition:
        """
        Query List Paging
        
        Query workflow definition list paging
        
        DS operation: WorkflowDefinitionController.queryWorkflowDefinitionListPaging | GET /projects/{projectCode}/workflow-definition
        
        Args:
            project_code: project code
            params: Query parameters bag for this operation.
        
        Returns:
            workflow definition page
        """
        path = f"projects/{project_code}/workflow-definition"
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            path,
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(PageInfoWorkflowDefinition))

    def create_workflow_definition(
        self,
        project_code: int,
        form: CreateWorkflowDefinitionParams
    ) -> None:
        """
        Create Workflow Definition
        
        Create workflow definition
        
        DS operation: WorkflowDefinitionController.createWorkflowDefinition | POST /projects/{projectCode}/workflow-definition
        
        Args:
            project_code: project code
            form: Form parameters bag for this operation.
        
        Returns:
            create result code
        """
        path = f"projects/{project_code}/workflow-definition"
        data = self._model_mapping(form)
        self._request(
            "POST",
            path,
        data=data,
        )
        return None

    def query_all_workflow_definition_by_project_code(
        self,
        project_code: int
    ) -> list[DagData]:
        """
        Query All By Project Code
        
        QUERY_WORKFLOW_DEFINITION_All_BY_PROJECT_CODE_NOTES
        
        Query all workflow definition by project code
        
        DS operation: WorkflowDefinitionController.queryAllWorkflowDefinitionByProjectCode | GET /projects/{projectCode}/workflow-definition/all
        
        Args:
            project_code: project code
        
        Returns:
            workflow definition list
        """
        path = f"projects/{project_code}/workflow-definition/all"
        payload = self._request("GET", path)
        return self._validate_payload(payload, TypeAdapter(list[DagData]))

    def copy_workflow_definition(
        self,
        project_code: int,
        form: CopyWorkflowDefinitionParams
    ) -> WorkflowDefinition:
        """
        Batch Copy By Codes
        
        Copy workflow definition
        
        DS operation: WorkflowDefinitionController.copyWorkflowDefinition | POST /projects/{projectCode}/workflow-definition/batch-copy
        
        Args:
            project_code: project code
            form: Form parameters bag for this operation.
        
        Returns:
            copy result code
        """
        path = f"projects/{project_code}/workflow-definition/batch-copy"
        data = self._model_mapping(form)
        payload = self._request(
            "POST",
            path,
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(WorkflowDefinition))

    def batch_delete_workflow_definition_by_codes(
        self,
        project_code: int,
        form: BatchDeleteWorkflowDefinitionByCodesParams
    ) -> None:
        """
        Batch Delete By Codes
        
        Batch delete workflow definition by codes
        
        DS operation: WorkflowDefinitionController.batchDeleteWorkflowDefinitionByCodes | POST /projects/{projectCode}/workflow-definition/batch-delete
        
        Args:
            project_code: project code
            form: Form parameters bag for this operation.
        
        Returns:
            delete result code
        """
        path = f"projects/{project_code}/workflow-definition/batch-delete"
        data = self._model_mapping(form)
        self._request(
            "POST",
            path,
        data=data,
        )
        return None

    def move_workflow_definition(
        self,
        project_code: int,
        form: MoveWorkflowDefinitionParams
    ) -> WorkflowDefinition:
        """
        Batch Move By Codes
        
        Move workflow definition
        
        DS operation: WorkflowDefinitionController.moveWorkflowDefinition | POST /projects/{projectCode}/workflow-definition/batch-move
        
        Args:
            project_code: project code
            form: Form parameters bag for this operation.
        
        Returns:
            move result code
        """
        path = f"projects/{project_code}/workflow-definition/batch-move"
        data = self._model_mapping(form)
        payload = self._request(
            "POST",
            path,
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(WorkflowDefinition))

    def get_node_list_map_by_definition_codes(
        self,
        project_code: int,
        params: GetNodeListMapByDefinitionCodesParams
    ) -> dict[int, list[TaskDefinition]]:
        """
        Get Task List By Definition Codes
        
        Get tasks list map by workflow definition multiple code
        
        DS operation: WorkflowDefinitionController.getNodeListMapByDefinitionCodes | GET /projects/{projectCode}/workflow-definition/batch-query-tasks
        
        Args:
            project_code: project code
            params: Query parameters bag for this operation.
        
        Returns:
            node list data
        """
        path = f"projects/{project_code}/workflow-definition/batch-query-tasks"
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            path,
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(dict[int, list[TaskDefinition]]))

    def query_workflow_definition_list(
        self,
        project_code: int
    ) -> list[DagData]:
        """
        Query List
        
        Query workflow definition list
        
        DS operation: WorkflowDefinitionController.queryWorkflowDefinitionList | GET /projects/{projectCode}/workflow-definition/list
        
        Args:
            project_code: project code
        
        Returns:
            workflow definition list
        """
        path = f"projects/{project_code}/workflow-definition/list"
        payload = self._request("GET", path)
        return self._validate_payload(payload, TypeAdapter(list[DagData]))

    def query_workflow_definition_by_name(
        self,
        project_code: int,
        params: QueryWorkflowDefinitionByNameParams
    ) -> DagData:
        """
        Query Workflow Definition By Name
        
        Query detail of workflow definition by name
        
        DS operation: WorkflowDefinitionController.queryWorkflowDefinitionByName | GET /projects/{projectCode}/workflow-definition/query-by-name
        
        Args:
            project_code: project code
            params: Query parameters bag for this operation.
        
        Returns:
            workflow definition detail
        """
        path = f"projects/{project_code}/workflow-definition/query-by-name"
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            path,
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(DagData))

    def get_task_list_by_workflow_definition_code(
        self,
        project_code: int,
        params: GetTaskListByWorkflowDefinitionCodeParams
    ) -> list[DependentSimplifyDefinition]:
        """
        Get Task List By Workflow Definition Code
        
        Get task definition list by workflow definition code
        
        DS operation: WorkflowDefinitionController.getTaskListByWorkflowDefinitionCode | GET /projects/{projectCode}/workflow-definition/query-task-definition-list
        
        Args:
            project_code: project code
            params: Query parameters bag for this operation.
        
        Returns:
            workflow definition list data
        """
        path = f"projects/{project_code}/workflow-definition/query-task-definition-list"
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            path,
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(list[DependentSimplifyDefinition]))

    def get_workflow_list_by_project_code(
        self,
        project_code: int
    ) -> list[DependentSimplifyDefinition]:
        """
        Get Workflow List By Project Code
        
        Get workflow definition list map by project code
        
        DS operation: WorkflowDefinitionController.getWorkflowListByProjectCode | GET /projects/{projectCode}/workflow-definition/query-workflow-definition-list
        
        Args:
            project_code: project code
        
        Returns:
            workflow definition list data
        """
        path = f"projects/{project_code}/workflow-definition/query-workflow-definition-list"
        payload = self._request("GET", path)
        return self._validate_payload(payload, TypeAdapter(list[DependentSimplifyDefinition]))

    def query_workflow_definition_simple_list(
        self,
        project_code: int
    ) -> list[WorkflowDefinitionSimpleItem]:
        """
        Query Simple List
        
        Query workflow definition simple list
        
        DS operation: WorkflowDefinitionController.queryWorkflowDefinitionSimpleList | GET /projects/{projectCode}/workflow-definition/simple-list
        
        Args:
            project_code: project code
        
        Returns:
            workflow definition list
        """
        path = f"projects/{project_code}/workflow-definition/simple-list"
        payload = self._request("GET", path)
        return self._validate_payload(payload, TypeAdapter(list[WorkflowDefinitionSimpleItem]))

    def verify_workflow_definition_name(
        self,
        project_code: int,
        params: VerifyWorkflowDefinitionNameParams
    ) -> None:
        """
        Verify Name
        
        Verify workflow definition name unique
        
        DS operation: WorkflowDefinitionController.verifyWorkflowDefinitionName | GET /projects/{projectCode}/workflow-definition/verify-name
        
        Args:
            project_code: project code
            params: Query parameters bag for this operation.
        
        Returns:
            true if workflow definition name not exists, otherwise false
        """
        path = f"projects/{project_code}/workflow-definition/verify-name"
        query_params = self._model_mapping(params)
        self._request(
            "GET",
            path,
        params=query_params,
        )
        return None

    def delete_workflow_definition_by_code(
        self,
        project_code: int,
        code: int
    ) -> None:
        """
        Delete By Workflow Definition Code
        
        DS operation: WorkflowDefinitionController.deleteWorkflowDefinitionByCode | DELETE /projects/{projectCode}/workflow-definition/{code}
        """
        path = f"projects/{project_code}/workflow-definition/{code}"
        self._request("DELETE", path)
        return None

    def query_workflow_definition_by_code(
        self,
        project_code: int,
        code: int
    ) -> DagData:
        """
        Query Workflow Definition By Code
        
        Query detail of workflow definition by code
        
        DS operation: WorkflowDefinitionController.queryWorkflowDefinitionByCode | GET /projects/{projectCode}/workflow-definition/{code}
        
        Args:
            project_code: project code
            code: workflow definition code
        
        Returns:
            workflow definition detail
        """
        path = f"projects/{project_code}/workflow-definition/{code}"
        payload = self._request("GET", path)
        return self._validate_payload(payload, TypeAdapter(DagData))

    def update_workflow_definition(
        self,
        project_code: int,
        code: int,
        form: UpdateWorkflowDefinitionParams
    ) -> None:
        """
        Update
        
        Update workflow definition, with whole workflow definition object including task definition, task relation and location.
        
        DS operation: WorkflowDefinitionController.updateWorkflowDefinition | PUT /projects/{projectCode}/workflow-definition/{code}
        
        Args:
            project_code: project code
            code: workflow definition code
            form: Form parameters bag for this operation.
        
        Returns:
            update result code
        """
        path = f"projects/{project_code}/workflow-definition/{code}"
        data = self._model_mapping(form)
        self._request(
            "PUT",
            path,
        data=data,
        )
        return None

    def release_workflow_definition(
        self,
        project_code: int,
        code: int,
        form: ReleaseWorkflowDefinitionParams
    ) -> bool:
        """
        Release
        
        DS operation: WorkflowDefinitionController.releaseWorkflowDefinition | POST /projects/{projectCode}/workflow-definition/{code}/release
        
        Args:
            form: Form parameters bag for this operation.
        """
        path = f"projects/{project_code}/workflow-definition/{code}/release"
        data = self._model_mapping(form)
        payload = self._request(
            "POST",
            path,
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(bool))

    def get_node_list_by_definition_code(
        self,
        project_code: int,
        code: int
    ) -> list[TaskDefinition]:
        """
        Get Tasks By Definition Code
        
        Get tasks list by workflow definition code
        
        DS operation: WorkflowDefinitionController.getNodeListByDefinitionCode | GET /projects/{projectCode}/workflow-definition/{code}/tasks
        
        Args:
            project_code: project code
            code: workflow definition code
        
        Returns:
            task list
        """
        path = f"projects/{project_code}/workflow-definition/{code}/tasks"
        payload = self._request("GET", path)
        return self._validate_payload(payload, TypeAdapter(list[TaskDefinition]))

    def query_workflow_definition_versions(
        self,
        project_code: int,
        code: int,
        params: QueryWorkflowDefinitionVersionsParams
    ) -> PageInfoWorkflowDefinitionLog:
        """
        Query Versions
        
        Query workflow definition version paging list info
        
        DS operation: WorkflowDefinitionController.queryWorkflowDefinitionVersions | GET /projects/{projectCode}/workflow-definition/{code}/versions
        
        Args:
            project_code: project code
            code: the workflow definition code
            params: Query parameters bag for this operation.
        
        Returns:
            the workflow definition version list
        """
        path = f"projects/{project_code}/workflow-definition/{code}/versions"
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            path,
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(PageInfoWorkflowDefinitionLog))

    def delete_workflow_definition_version(
        self,
        project_code: int,
        code: int,
        version: int
    ) -> None:
        """
        Delete Version
        
        Delete the certain workflow definition version by version and workflow definition code
        
        DS operation: WorkflowDefinitionController.deleteWorkflowDefinitionVersion | DELETE /projects/{projectCode}/workflow-definition/{code}/versions/{version}
        
        Args:
            project_code: project code
            code: the workflow definition code
            version: the workflow definition version user want to delete
        
        Returns:
            delete version result code
        """
        path = f"projects/{project_code}/workflow-definition/{code}/versions/{version}"
        self._request("DELETE", path)
        return None

    def switch_workflow_definition_version(
        self,
        project_code: int,
        code: int,
        version: int
    ) -> None:
        """
        Switch Version
        
        Switch certain workflow definition version
        
        DS operation: WorkflowDefinitionController.switchWorkflowDefinitionVersion | GET /projects/{projectCode}/workflow-definition/{code}/versions/{version}
        
        Args:
            project_code: project code
            code: the workflow definition code
            version: the version user want to switch
        
        Returns:
            switch version result code
        """
        path = f"projects/{project_code}/workflow-definition/{code}/versions/{version}"
        self._request("GET", path)
        return None

    def view_tree(
        self,
        project_code: int,
        code: int,
        params: ViewTreeParams
    ) -> TreeViewDto:
        """
        View Tree
        
        Encapsulation tree view structure
        
        DS operation: WorkflowDefinitionController.viewTree | GET /projects/{projectCode}/workflow-definition/{code}/view-tree
        
        Args:
            project_code: project code
            code: workflow definition code
            params: Query parameters bag for this operation.
        
        Returns:
            tree view json data
        """
        path = f"projects/{project_code}/workflow-definition/{code}/view-tree"
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            path,
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(TreeViewDto))

    def view_variables(
        self,
        project_code: int,
        code: int
    ) -> WorkflowDefinitionVariablesView:
        """
        View Variables
        
        Query workflow definition global variables and local variables
        
        DS operation: WorkflowDefinitionController.viewVariables | GET /projects/{projectCode}/workflow-definition/{code}/view-variables
        
        Args:
            code: workflow definition code
        
        Returns:
            variables data
        """
        path = f"projects/{project_code}/workflow-definition/{code}/view-variables"
        payload = self._request("GET", path)
        return self._validate_payload(payload, TypeAdapter(WorkflowDefinitionVariablesView))

__all__ = ["WorkflowDefinitionOperations"]
