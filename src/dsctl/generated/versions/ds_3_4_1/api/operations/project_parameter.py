from __future__ import annotations

from ._base import BaseRequestsClient, BaseParamsModel

from pydantic import Field, TypeAdapter

from ...dao.entities.project_parameter import ProjectParameter
from ..contracts.page_info import PageInfoProjectParameter

class QueryProjectParameterListPagingParams(BaseParamsModel):
    """
    Query Project Parameter List Paging
    
    Query parameters for ProjectParameterController.queryProjectParameterListPaging.
    """
    searchVal: str | None = Field(default=None)
    projectParameterDataType: str | None = Field(default=None)
    pageNo: int = Field(examples=[1])
    pageSize: int = Field(examples=[10])

class CreateProjectParameterParams(BaseParamsModel):
    """
    Create Project Parameter
    
    Form parameters for ProjectParameterController.createProjectParameter.
    """
    projectParameterName: str
    projectParameterValue: str
    projectParameterDataType: str | None = Field(default=None)

class BatchDeleteProjectParametersByCodesParams(BaseParamsModel):
    """
    Batch Delete Project Parameters By Codes
    
    Form parameters for ProjectParameterController.batchDeleteProjectParametersByCodes.
    """
    codes: str

class DeleteProjectParametersByCodeParams(BaseParamsModel):
    """
    Delete Project Parameters By Code
    
    Form parameters for ProjectParameterController.deleteProjectParametersByCode.
    """
    code: int

class UpdateProjectParameterParams(BaseParamsModel):
    """
    Update Project Parameter
    
    Form parameters for ProjectParameterController.updateProjectParameter.
    """
    projectParameterName: str
    projectParameterValue: str
    projectParameterDataType: str

class ProjectParameterOperations(BaseRequestsClient):
    def query_project_parameter_list_paging(
        self,
        project_code: int,
        params: QueryProjectParameterListPagingParams
    ) -> PageInfoProjectParameter:
        """
        Query Project Parameter List Paging
        
        DS operation: ProjectParameterController.queryProjectParameterListPaging | GET /projects/{projectCode}/project-parameter
        
        Args:
            params: Query parameters bag for this operation.
        """
        path = f"projects/{project_code}/project-parameter"
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            path,
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(PageInfoProjectParameter))

    def create_project_parameter(
        self,
        project_code: int,
        form: CreateProjectParameterParams
    ) -> ProjectParameter:
        """
        Create Project Parameter
        
        DS operation: ProjectParameterController.createProjectParameter | POST /projects/{projectCode}/project-parameter
        
        Args:
            form: Form parameters bag for this operation.
        """
        path = f"projects/{project_code}/project-parameter"
        data = self._model_mapping(form)
        payload = self._request(
            "POST",
            path,
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(ProjectParameter))

    def batch_delete_project_parameters_by_codes(
        self,
        project_code: int,
        form: BatchDeleteProjectParametersByCodesParams
    ) -> None:
        """
        Batch Delete Project Parameters By Codes
        
        DS operation: ProjectParameterController.batchDeleteProjectParametersByCodes | POST /projects/{projectCode}/project-parameter/batch-delete
        
        Args:
            form: Form parameters bag for this operation.
        """
        path = f"projects/{project_code}/project-parameter/batch-delete"
        data = self._model_mapping(form)
        self._request(
            "POST",
            path,
        data=data,
        )
        return None

    def delete_project_parameters_by_code(
        self,
        project_code: int,
        form: DeleteProjectParametersByCodeParams
    ) -> None:
        """
        Delete Project Parameters By Code
        
        DS operation: ProjectParameterController.deleteProjectParametersByCode | POST /projects/{projectCode}/project-parameter/delete
        
        Args:
            form: Form parameters bag for this operation.
        """
        path = f"projects/{project_code}/project-parameter/delete"
        data = self._model_mapping(form)
        self._request(
            "POST",
            path,
        data=data,
        )
        return None

    def query_project_parameter_by_code(
        self,
        project_code: int,
        code: int
    ) -> ProjectParameter:
        """
        Query Project Parameter By Code
        
        DS operation: ProjectParameterController.queryProjectParameterByCode | GET /projects/{projectCode}/project-parameter/{code}
        """
        path = f"projects/{project_code}/project-parameter/{code}"
        payload = self._request("GET", path)
        return self._validate_payload(payload, TypeAdapter(ProjectParameter))

    def update_project_parameter(
        self,
        project_code: int,
        code: int,
        form: UpdateProjectParameterParams
    ) -> ProjectParameter:
        """
        Update Project Parameter
        
        DS operation: ProjectParameterController.updateProjectParameter | PUT /projects/{projectCode}/project-parameter/{code}
        
        Args:
            form: Form parameters bag for this operation.
        """
        path = f"projects/{project_code}/project-parameter/{code}"
        data = self._model_mapping(form)
        payload = self._request(
            "PUT",
            path,
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(ProjectParameter))

__all__ = ["ProjectParameterOperations"]
