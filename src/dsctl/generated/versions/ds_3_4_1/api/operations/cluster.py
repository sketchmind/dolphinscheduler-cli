from __future__ import annotations

from ._base import BaseRequestsClient, BaseParamsModel

from pydantic import Field, TypeAdapter

from ...dao.entities.cluster import Cluster
from ..contracts.cluster_dto import ClusterDto
from ..contracts.page_info import PageInfoClusterDto

class CreateClusterParams(BaseParamsModel):
    """
    Create Cluster
    
    Form parameters for ClusterController.createCluster.
    """
    name: str = Field(description='cluster name')
    config: str = Field(description='config')
    description: str | None = Field(default=None, description='description')

class DeleteClusterParams(BaseParamsModel):
    """
    Delete Cluster By Code
    
    Form parameters for ClusterController.deleteCluster.
    """
    clusterCode: int = Field(description='cluster code', examples=[100])

class QueryClusterListPagingParams(BaseParamsModel):
    """
    Query Cluster List Paging
    
    Query parameters for ClusterController.queryClusterListPaging.
    """
    searchVal: str | None = Field(default=None, description='search value')
    pageSize: int = Field(description='page size', examples=[20])
    pageNo: int = Field(description='page number', examples=[1])

class QueryClusterByCodeParams(BaseParamsModel):
    """
    Query Cluster By Code
    
    Query parameters for ClusterController.queryClusterByCode.
    """
    clusterCode: int = Field(description='cluster code', examples=[100])

class UpdateClusterParams(BaseParamsModel):
    """
    Update Cluster
    
    Form parameters for ClusterController.updateCluster.
    """
    code: int = Field(description='cluster code', examples=[100])
    name: str = Field(description='cluster name')
    config: str = Field(description='cluster config')
    description: str | None = Field(default=None, description='description')

class VerifyClusterParams(BaseParamsModel):
    """
    Verify Cluster
    
    Form parameters for ClusterController.verifyCluster.
    """
    clusterName: str = Field(description='cluster name')

class ClusterOperations(BaseRequestsClient):
    def create_cluster(
        self,
        form: CreateClusterParams
    ) -> int:
        """
        Create Cluster
        
        Create cluster
        
        DS operation: ClusterController.createCluster | POST /cluster/create
        
        Args:
            form: Form parameters bag for this operation.
        
        Returns:
            returns an error if it exists
        """
        data = self._model_mapping(form)
        payload = self._request(
            "POST",
            "cluster/create",
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(int))

    def delete_cluster(
        self,
        form: DeleteClusterParams
    ) -> bool:
        """
        Delete Cluster By Code
        
        Delete cluster by code
        
        DS operation: ClusterController.deleteCluster | POST /cluster/delete
        
        Args:
            form: Form parameters bag for this operation.
        
        Returns:
            delete result code
        """
        data = self._model_mapping(form)
        payload = self._request(
            "POST",
            "cluster/delete",
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(bool))

    def query_cluster_list_paging(
        self,
        params: QueryClusterListPagingParams
    ) -> PageInfoClusterDto:
        """
        Query Cluster List Paging
        
        Query cluster list paging
        
        DS operation: ClusterController.queryClusterListPaging | GET /cluster/list-paging
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            cluster list which the login user have permission to see
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "cluster/list-paging",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(PageInfoClusterDto))

    def query_cluster_by_code(
        self,
        params: QueryClusterByCodeParams
    ) -> ClusterDto:
        """
        Query Cluster By Code
        
        Query cluster details by code
        
        DS operation: ClusterController.queryClusterByCode | GET /cluster/query-by-code
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            cluster detail information
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "cluster/query-by-code",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(ClusterDto))

    def query_all_cluster_list(
        self
    ) -> list[ClusterDto]:
        """
        Query All Cluster List
        
        Query all cluster list
        
        DS operation: ClusterController.queryAllClusterList | GET /cluster/query-cluster-list
        
        Returns:
            all cluster list
        """
        payload = self._request("GET", "cluster/query-cluster-list")
        return self._validate_payload(payload, TypeAdapter(list[ClusterDto]))

    def update_cluster(
        self,
        form: UpdateClusterParams
    ) -> Cluster:
        """
        Update Cluster
        
        Update cluster
        
        DS operation: ClusterController.updateCluster | POST /cluster/update
        
        Args:
            form: Form parameters bag for this operation.
        
        Returns:
            update result code
        """
        data = self._model_mapping(form)
        payload = self._request(
            "POST",
            "cluster/update",
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(Cluster))

    def verify_cluster(
        self,
        form: VerifyClusterParams
    ) -> bool:
        """
        Verify Cluster
        
        Verify cluster and cluster name
        
        DS operation: ClusterController.verifyCluster | POST /cluster/verify-cluster
        
        Args:
            form: Form parameters bag for this operation.
        
        Returns:
            true if the cluster name not exists, otherwise return false
        """
        data = self._model_mapping(form)
        payload = self._request(
            "POST",
            "cluster/verify-cluster",
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(bool))

__all__ = ["ClusterOperations"]
