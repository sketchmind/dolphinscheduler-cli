from __future__ import annotations

from ._base import BaseRequestsClient, BaseParamsModel

from pydantic import Field, TypeAdapter

from ...dao.entities.worker_group import WorkerGroup
from ..contracts.page_info import PageInfoWorkerGroupPageDetail

class QueryAllWorkerGroupsPagingParams(BaseParamsModel):
    """
    Query All Worker Groups Paging
    
    Query parameters for WorkerGroupController.queryAllWorkerGroupsPaging.
    """
    pageNo: int = Field(description='page number', examples=[1])
    pageSize: int = Field(description='page size', examples=[20])
    searchVal: str | None = Field(default=None, description='search value')

class SaveWorkerGroupParams(BaseParamsModel):
    """
    Save Worker Group
    
    Form parameters for WorkerGroupController.saveWorkerGroup.
    """
    id: int | None = Field(default=None, description='worker group id', examples=[10])
    name: str = Field(description='worker group name')
    addrList: str = Field(description='addr list')
    description: str | None = Field(default=None)

class WorkerGroupOperations(BaseRequestsClient):
    def query_all_worker_groups_paging(
        self,
        params: QueryAllWorkerGroupsPagingParams
    ) -> PageInfoWorkerGroupPageDetail:
        """
        Query All Worker Groups Paging
        
        Query worker groups paging
        
        DS operation: WorkerGroupController.queryAllWorkerGroupsPaging | GET /worker-groups
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            worker group list page
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "worker-groups",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(PageInfoWorkerGroupPageDetail))

    def save_worker_group(
        self,
        form: SaveWorkerGroupParams
    ) -> WorkerGroup:
        """
        Save Worker Group
        
        Create or update a worker group
        
        DS operation: WorkerGroupController.saveWorkerGroup | POST /worker-groups
        
        Args:
            form: Form parameters bag for this operation.
        
        Returns:
            create or update result code
        """
        data = self._model_mapping(form)
        payload = self._request(
            "POST",
            "worker-groups",
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(WorkerGroup))

    def query_all_worker_groups(
        self
    ) -> list[str]:
        """
        Query All Worker Groups
        
        Query all worker groups
        
        DS operation: WorkerGroupController.queryAllWorkerGroups | GET /worker-groups/all
        
        Returns:
            all worker group list
        """
        payload = self._request("GET", "worker-groups/all")
        return self._validate_payload(payload, TypeAdapter(list[str]))

    def query_worker_address_list(
        self
    ) -> list[str]:
        """
        Query Worker Address List
        
        Query worker address list
        
        DS operation: WorkerGroupController.queryWorkerAddressList | GET /worker-groups/worker-address-list
        
        Returns:
            all worker address list
        """
        payload = self._request("GET", "worker-groups/worker-address-list")
        return self._validate_payload(payload, TypeAdapter(list[str]))

    def delete_worker_group_by_id(
        self,
        id: int
    ) -> None:
        """
        Delete Worker Group By Id
        
        Delete worker group by id
        
        DS operation: WorkerGroupController.deleteWorkerGroupById | DELETE /worker-groups/{id}
        
        Args:
            id: group id
        
        Returns:
            delete result code
        """
        path = f"worker-groups/{id}"
        self._request("DELETE", path)
        return None

__all__ = ["WorkerGroupOperations"]
