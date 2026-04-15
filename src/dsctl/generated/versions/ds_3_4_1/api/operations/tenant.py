from __future__ import annotations

from ._base import BaseRequestsClient, BaseParamsModel

from pydantic import Field, TypeAdapter

from ...dao.entities.tenant import Tenant
from ..contracts.page_info import PageInfoTenant

class QueryTenantListPagingParams(BaseParamsModel):
    """
    Query Tenant List Paging
    
    Query parameters for TenantController.queryTenantListPaging.
    """
    searchVal: str | None = Field(default=None, description='search value')
    pageNo: int = Field(description='page number', examples=[1])
    pageSize: int = Field(description='page size', examples=[20])

class CreateTenantParams(BaseParamsModel):
    """
    Create Tenant
    
    Form parameters for TenantController.createTenant.
    """
    tenantCode: str = Field(description='tenant code')
    queueId: int = Field(description='queue id', examples=[100])
    description: str | None = Field(default=None, description='description')

class VerifyTenantCodeParams(BaseParamsModel):
    """
    Verify Tenant Code
    
    Query parameters for TenantController.verifyTenantCode.
    """
    tenantCode: str = Field(description='tenant code')

class UpdateTenantParams(BaseParamsModel):
    """
    Update Tenant
    
    Form parameters for TenantController.updateTenant.
    """
    tenantCode: str = Field(description='tenant code')
    queueId: int = Field(description='queue id', examples=[100])
    description: str | None = Field(default=None, description='description')

class TenantOperations(BaseRequestsClient):
    def query_tenant_list_paging(
        self,
        params: QueryTenantListPagingParams
    ) -> PageInfoTenant:
        """
        Query Tenant List Paging
        
        Query tenant list paging
        
        DS operation: TenantController.queryTenantListPaging | GET /tenants
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            tenant list page
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "tenants",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(PageInfoTenant))

    def create_tenant(
        self,
        form: CreateTenantParams
    ) -> Tenant:
        """
        Create Tenant
        
        Create tenant
        
        DS operation: TenantController.createTenant | POST /tenants
        
        Args:
            form: Form parameters bag for this operation.
        
        Returns:
            create result code
        """
        data = self._model_mapping(form)
        payload = self._request(
            "POST",
            "tenants",
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(Tenant))

    def query_tenant_list(
        self
    ) -> list[Tenant]:
        """
        Query Tenant List
        
        Tenant list
        
        DS operation: TenantController.queryTenantList | GET /tenants/list
        
        Returns:
            tenant list
        """
        payload = self._request("GET", "tenants/list")
        return self._validate_payload(payload, TypeAdapter(list[Tenant]))

    def verify_tenant_code(
        self,
        params: VerifyTenantCodeParams
    ) -> bool:
        """
        Verify Tenant Code
        
        Verify tenant code
        
        DS operation: TenantController.verifyTenantCode | GET /tenants/verify-code
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            true if tenant code can use, otherwise return false
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "tenants/verify-code",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(bool))

    def delete_tenant_by_id(
        self,
        id: int
    ) -> bool:
        """
        Delete Tenant By Id
        
        Delete tenant by id
        
        DS operation: TenantController.deleteTenantById | DELETE /tenants/{id}
        
        Args:
            id: tenant id
        
        Returns:
            delete result code
        """
        path = f"tenants/{id}"
        payload = self._request("DELETE", path)
        return self._validate_payload(payload, TypeAdapter(bool))

    def update_tenant(
        self,
        id: int,
        form: UpdateTenantParams
    ) -> bool:
        """
        Update Tenant
        
        Update tenant
        
        DS operation: TenantController.updateTenant | PUT /tenants/{id}
        
        Args:
            id: tenant id
            form: Form parameters bag for this operation.
        
        Returns:
            update result code
        """
        path = f"tenants/{id}"
        data = self._model_mapping(form)
        payload = self._request(
            "PUT",
            path,
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(bool))

__all__ = ["TenantOperations"]
