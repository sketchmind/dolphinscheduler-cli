from __future__ import annotations

from ._base import BaseRequestsClient, BaseParamsModel

from pydantic import Field, TypeAdapter

from ...dao.entities.environment import Environment
from ..contracts.environment_dto import EnvironmentDto
from ..contracts.page_info import PageInfoEnvironmentDto

class CreateEnvironmentParams(BaseParamsModel):
    """
    Create Environment
    
    Form parameters for EnvironmentController.createEnvironment.
    """
    name: str = Field(description='environment name')
    config: str = Field(description='config')
    description: str | None = Field(default=None, description='description')
    workerGroups: str | None = Field(default=None)

class DeleteEnvironmentParams(BaseParamsModel):
    """
    Delete Environment By Code
    
    Form parameters for EnvironmentController.deleteEnvironment.
    """
    environmentCode: int = Field(description='environment code', examples=[100])

class QueryEnvironmentListPagingParams(BaseParamsModel):
    """
    Query Environment List Paging
    
    Query parameters for EnvironmentController.queryEnvironmentListPaging.
    """
    searchVal: str | None = Field(default=None, description='search value')
    pageSize: int = Field(description='page size', examples=[20])
    pageNo: int = Field(description='page number', examples=[1])

class QueryEnvironmentByCodeParams(BaseParamsModel):
    """
    Query Environment By Code
    
    Query parameters for EnvironmentController.queryEnvironmentByCode.
    """
    environmentCode: int = Field(description='environment code', examples=[100])

class UpdateEnvironmentParams(BaseParamsModel):
    """
    Update Environment
    
    Form parameters for EnvironmentController.updateEnvironment.
    """
    code: int = Field(description='environment code', examples=[100])
    name: str = Field(description='environment name')
    config: str = Field(description='environment config')
    description: str | None = Field(default=None, description='description')
    workerGroups: str | None = Field(default=None)

class VerifyEnvironmentParams(BaseParamsModel):
    """
    Verify Environment
    
    Form parameters for EnvironmentController.verifyEnvironment.
    """
    environmentName: str = Field(description='environment name')

class EnvironmentOperations(BaseRequestsClient):
    def create_environment(
        self,
        form: CreateEnvironmentParams
    ) -> int:
        """
        Create Environment
        
        Create environment
        
        DS operation: EnvironmentController.createEnvironment | POST /environment/create
        
        Args:
            form: Form parameters bag for this operation.
        
        Returns:
            returns an error if it exists
        """
        data = self._model_mapping(form)
        payload = self._request(
            "POST",
            "environment/create",
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(int))

    def delete_environment(
        self,
        form: DeleteEnvironmentParams
    ) -> None:
        """
        Delete Environment By Code
        
        Delete environment by code
        
        DS operation: EnvironmentController.deleteEnvironment | POST /environment/delete
        
        Args:
            form: Form parameters bag for this operation.
        
        Returns:
            delete result code
        """
        data = self._model_mapping(form)
        self._request(
            "POST",
            "environment/delete",
        data=data,
        )
        return None

    def query_environment_list_paging(
        self,
        params: QueryEnvironmentListPagingParams
    ) -> PageInfoEnvironmentDto:
        """
        Query Environment List Paging
        
        Query environment list paging
        
        DS operation: EnvironmentController.queryEnvironmentListPaging | GET /environment/list-paging
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            environment list which the login user have permission to see
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "environment/list-paging",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(PageInfoEnvironmentDto))

    def query_environment_by_code(
        self,
        params: QueryEnvironmentByCodeParams
    ) -> EnvironmentDto:
        """
        Query Environment By Code
        
        Query environment details by code
        
        DS operation: EnvironmentController.queryEnvironmentByCode | GET /environment/query-by-code
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            environment detail information
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "environment/query-by-code",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(EnvironmentDto))

    def query_all_environment_list(
        self
    ) -> list[EnvironmentDto]:
        """
        Query All Environment List
        
        Query all environment list
        
        DS operation: EnvironmentController.queryAllEnvironmentList | GET /environment/query-environment-list
        
        Returns:
            all environment list
        """
        payload = self._request("GET", "environment/query-environment-list")
        return self._validate_payload(payload, TypeAdapter(list[EnvironmentDto]))

    def update_environment(
        self,
        form: UpdateEnvironmentParams
    ) -> Environment:
        """
        Update Environment
        
        Update environment
        
        DS operation: EnvironmentController.updateEnvironment | POST /environment/update
        
        Args:
            form: Form parameters bag for this operation.
        
        Returns:
            update result code
        """
        data = self._model_mapping(form)
        payload = self._request(
            "POST",
            "environment/update",
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(Environment))

    def verify_environment(
        self,
        form: VerifyEnvironmentParams
    ) -> None:
        """
        Verify Environment
        
        Verify environment and environment name
        
        DS operation: EnvironmentController.verifyEnvironment | POST /environment/verify-environment
        
        Args:
            form: Form parameters bag for this operation.
        
        Returns:
            true if the environment name not exists, otherwise return false
        """
        data = self._model_mapping(form)
        self._request(
            "POST",
            "environment/verify-environment",
        data=data,
        )
        return None

__all__ = ["EnvironmentOperations"]
