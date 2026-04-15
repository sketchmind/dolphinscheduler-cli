from __future__ import annotations

from ._base import BaseRequestsClient, BaseParamsModel

from pydantic import Field, TypeAdapter

from ...common.enums.release_state import ReleaseState
from ..contracts.page_info import PageInfoTaskDefinitionLog
from ..views.task_definition import TaskDefinitionVO

class GenTaskCodeListParams(BaseParamsModel):
    """
    Gen Task Code List
    
    Query parameters for TaskDefinitionController.genTaskCodeList.
    """
    genNum: int = Field(description='gen num', examples=[1])

class ReleaseTaskDefinitionParams(BaseParamsModel):
    """
    Release Task Definition
    
    Form parameters for TaskDefinitionController.releaseTaskDefinition.
    """
    releaseState: ReleaseState = Field(description='releaseState')

class QueryTaskDefinitionVersionsParams(BaseParamsModel):
    """
    Query Versions
    
    Query parameters for TaskDefinitionController.queryTaskDefinitionVersions.
    """
    pageNo: int = Field(description='the task definition version list current page number', examples=[1])
    pageSize: int = Field(description='the task definition version list page size', examples=[10])

class UpdateTaskWithUpstreamParams(BaseParamsModel):
    """
    Update With Upstream
    
    Form parameters for TaskDefinitionController.updateTaskWithUpstream.
    """
    taskDefinitionJsonObj: str = Field(description='task definition json object')
    upstreamCodes: str | None = Field(default=None, description='upstream task codes, sep comma')

class TaskDefinitionOperations(BaseRequestsClient):
    def gen_task_code_list(
        self,
        project_code: int,
        params: GenTaskCodeListParams
    ) -> list[int]:
        """
        Gen Task Code List
        
        Gen task code list
        
        DS operation: TaskDefinitionController.genTaskCodeList | GET /projects/{projectCode}/task-definition/gen-task-codes
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            task code list
        """
        path = f"projects/{project_code}/task-definition/gen-task-codes"
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            path,
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(list[int]))

    def query_task_definition_detail(
        self,
        project_code: int,
        code: int
    ) -> TaskDefinitionVO:
        """
        Query Task Definition By Code
        
        Query detail of task definition by code
        
        DS operation: TaskDefinitionController.queryTaskDefinitionDetail | GET /projects/{projectCode}/task-definition/{code}
        
        Args:
            project_code: project code
            code: the task definition code
        
        Returns:
            task definition detail
        """
        path = f"projects/{project_code}/task-definition/{code}"
        payload = self._request("GET", path)
        return self._validate_payload(payload, TypeAdapter(TaskDefinitionVO))

    def release_task_definition(
        self,
        project_code: int,
        code: int,
        form: ReleaseTaskDefinitionParams
    ) -> None:
        """
        Release Task Definition
        
        Release task definition
        
        DS operation: TaskDefinitionController.releaseTaskDefinition | POST /projects/{projectCode}/task-definition/{code}/release
        
        Args:
            project_code: project code
            code: task definition code
            form: Form parameters bag for this operation.
        
        Returns:
            update result code
        """
        path = f"projects/{project_code}/task-definition/{code}/release"
        data = self._model_mapping(form)
        self._request(
            "POST",
            path,
        data=data,
        )
        return None

    def query_task_definition_versions(
        self,
        project_code: int,
        code: int,
        params: QueryTaskDefinitionVersionsParams
    ) -> PageInfoTaskDefinitionLog:
        """
        Query Versions
        
        Query task definition version paging list info
        
        DS operation: TaskDefinitionController.queryTaskDefinitionVersions | GET /projects/{projectCode}/task-definition/{code}/versions
        
        Args:
            project_code: project code
            code: the task definition code
            params: Query parameters bag for this operation.
        
        Returns:
            the task definition version list
        """
        path = f"projects/{project_code}/task-definition/{code}/versions"
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            path,
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(PageInfoTaskDefinitionLog))

    def delete_task_definition_version(
        self,
        project_code: int,
        code: int,
        version: int
    ) -> None:
        """
        Delete Version
        
        Delete the certain task definition version by version and code
        
        DS operation: TaskDefinitionController.deleteTaskDefinitionVersion | DELETE /projects/{projectCode}/task-definition/{code}/versions/{version}
        
        Args:
            project_code: project code
            code: the task definition code
            version: the task definition version user want to delete
        
        Returns:
            delete version result code
        """
        path = f"projects/{project_code}/task-definition/{code}/versions/{version}"
        self._request("DELETE", path)
        return None

    def switch_task_definition_version(
        self,
        project_code: int,
        code: int,
        version: int
    ) -> None:
        """
        Switch Version
        
        Switch task definition version
        
        DS operation: TaskDefinitionController.switchTaskDefinitionVersion | GET /projects/{projectCode}/task-definition/{code}/versions/{version}
        
        Args:
            project_code: project code
            code: the task definition code
            version: the version user want to switch
        
        Returns:
            switch version result code
        """
        path = f"projects/{project_code}/task-definition/{code}/versions/{version}"
        self._request("GET", path)
        return None

    def update_task_with_upstream(
        self,
        project_code: int,
        code: int,
        form: UpdateTaskWithUpstreamParams
    ) -> int:
        """
        Update With Upstream
        
        Update task definition
        
        DS operation: TaskDefinitionController.updateTaskWithUpstream | PUT /projects/{projectCode}/task-definition/{code}/with-upstream
        
        Args:
            project_code: project code
            code: task definition code
            form: Form parameters bag for this operation.
        
        Returns:
            update result code
        """
        path = f"projects/{project_code}/task-definition/{code}/with-upstream"
        data = self._model_mapping(form)
        payload = self._request(
            "PUT",
            path,
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(int))

__all__ = ["TaskDefinitionOperations"]
