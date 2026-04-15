from __future__ import annotations

from ._base import BaseRequestsClient, BaseParamsModel

from pydantic import Field, TypeAdapter

from ...dao.entities.k8s_namespace import K8sNamespace
from ..contracts.page_info import PageInfoK8sNamespace

class QueryNamespaceListPagingParams(BaseParamsModel):
    """
    Query Namespace List Paging
    
    Query parameters for K8sNamespaceController.queryNamespaceListPaging.
    """
    searchVal: str | None = Field(default=None, description='search value')
    pageSize: int = Field(description='page size', examples=[10])
    pageNo: int = Field(description='page number', examples=[1])

class CreateNamespaceParams(BaseParamsModel):
    """
    Create K8s Namespace
    
    Form parameters for K8sNamespaceController.createNamespace.
    """
    namespace: str = Field(description='k8s namespace')
    clusterCode: int = Field(description='clusterCode')

class QueryAuthorizedNamespaceParams(BaseParamsModel):
    """
    Query Authorized Namespace
    
    Query parameters for K8sNamespaceController.queryAuthorizedNamespace.
    """
    userId: int = Field(description='user id', examples=[100])

class DelNamespaceByIdParams(BaseParamsModel):
    """
    Del Namespace By Id
    
    Form parameters for K8sNamespaceController.delNamespaceById.
    """
    id: int = Field(description='namespace id', examples=[100])

class QueryUnauthorizedNamespaceParams(BaseParamsModel):
    """
    Query Unauthorized Namespace
    
    Query parameters for K8sNamespaceController.queryUnauthorizedNamespace.
    """
    userId: int = Field(description='user id', examples=[100])

class VerifyNamespaceParams(BaseParamsModel):
    """
    Verify Namespace K8s
    
    Form parameters for K8sNamespaceController.verifyNamespace.
    """
    namespace: str = Field(description='namespace')
    clusterCode: int = Field(description='cluster code')

class K8sNamespaceOperations(BaseRequestsClient):
    def query_namespace_list_paging(
        self,
        params: QueryNamespaceListPagingParams
    ) -> PageInfoK8sNamespace:
        """
        Query Namespace List Paging
        
        Query namespace list paging
        
        DS operation: K8sNamespaceController.queryNamespaceListPaging | GET /k8s-namespace
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            namespace list which the login user have permission to see
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "k8s-namespace",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(PageInfoK8sNamespace))

    def create_namespace(
        self,
        form: CreateNamespaceParams
    ) -> K8sNamespace:
        """
        Create K8s Namespace
        
        Register namespace in db,need to create namespace in k8s first
        
        DS operation: K8sNamespaceController.createNamespace | POST /k8s-namespace
        
        Args:
            form: Form parameters bag for this operation.
        """
        data = self._model_mapping(form)
        payload = self._request(
            "POST",
            "k8s-namespace",
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(K8sNamespace))

    def query_authorized_namespace(
        self,
        params: QueryAuthorizedNamespaceParams
    ) -> list[K8sNamespace]:
        """
        Query Authorized Namespace
        
        Query unauthorized namespace
        
        DS operation: K8sNamespaceController.queryAuthorizedNamespace | GET /k8s-namespace/authed-namespace
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            namespaces which the user have permission to see
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "k8s-namespace/authed-namespace",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(list[K8sNamespace]))

    def query_available_namespace_list(
        self
    ) -> list[K8sNamespace]:
        """
        Query Available Namespace List
        
        Query namespace available
        
        DS operation: K8sNamespaceController.queryAvailableNamespaceList | GET /k8s-namespace/available-list
        
        Returns:
            namespace list
        """
        payload = self._request("GET", "k8s-namespace/available-list")
        return self._validate_payload(payload, TypeAdapter(list[K8sNamespace]))

    def del_namespace_by_id(
        self,
        form: DelNamespaceByIdParams
    ) -> None:
        """
        Del Namespace By Id
        
        Delete namespace by id
        
        DS operation: K8sNamespaceController.delNamespaceById | POST /k8s-namespace/delete
        
        Args:
            form: Form parameters bag for this operation.
        
        Returns:
            delete result code
        """
        data = self._model_mapping(form)
        self._request(
            "POST",
            "k8s-namespace/delete",
        data=data,
        )
        return None

    def query_unauthorized_namespace(
        self,
        params: QueryUnauthorizedNamespaceParams
    ) -> list[K8sNamespace]:
        """
        Query Unauthorized Namespace
        
        Query unauthorized namespace
        
        DS operation: K8sNamespaceController.queryUnauthorizedNamespace | GET /k8s-namespace/unauth-namespace
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            the namespaces which user have no permission to see
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "k8s-namespace/unauth-namespace",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(list[K8sNamespace]))

    def verify_namespace(
        self,
        form: VerifyNamespaceParams
    ) -> None:
        """
        Verify Namespace K8s
        
        Verify namespace and k8s,one k8s namespace is unique
        
        DS operation: K8sNamespaceController.verifyNamespace | POST /k8s-namespace/verify
        
        Args:
            form: Form parameters bag for this operation.
        
        Returns:
            true if the k8s and namespace not exists, otherwise return false
        """
        data = self._model_mapping(form)
        self._request(
            "POST",
            "k8s-namespace/verify",
        data=data,
        )
        return None

__all__ = ["K8sNamespaceOperations"]
