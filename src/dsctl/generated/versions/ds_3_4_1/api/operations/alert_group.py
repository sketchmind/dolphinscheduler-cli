from __future__ import annotations

from ._base import BaseRequestsClient, BaseParamsModel

from pydantic import Field, TypeAdapter

from ...dao.entities.alert_group import AlertGroup
from ..contracts.page_info import PageInfoAlertGroup

class ListPagingParams(BaseParamsModel):
    """
    Query Alert Group List Paging
    
    Query parameters for AlertGroupController.listPaging.
    """
    searchVal: str | None = Field(default=None, description='search value')
    pageNo: int = Field(description='page number', examples=[1])
    pageSize: int = Field(description='page size', examples=[20])

class CreateAlertGroupParams(BaseParamsModel):
    """
    Create Alert Group
    
    Form parameters for AlertGroupController.createAlertGroup.
    """
    groupName: str = Field(description='group name')
    description: str | None = Field(default=None, description='description')
    alertInstanceIds: str = Field(description='alertInstanceIds')

class QueryAlertGroupByIdParams(BaseParamsModel):
    """
    Query Alert Group By Id
    
    Form parameters for AlertGroupController.queryAlertGroupById.
    """
    id: int = Field(description='alert group id', examples=[1])

class VerifyGroupNameParams(BaseParamsModel):
    """
    Verify Group Name
    
    Query parameters for AlertGroupController.verifyGroupName.
    """
    groupName: str = Field(description='group name')

class UpdateAlertGroupByIdParams(BaseParamsModel):
    """
    Update Alert Group
    
    Form parameters for AlertGroupController.updateAlertGroupById.
    """
    groupName: str = Field(description='group name')
    description: str | None = Field(default=None, description='description')
    alertInstanceIds: str

class AlertGroupOperations(BaseRequestsClient):
    def list_paging(
        self,
        params: ListPagingParams
    ) -> PageInfoAlertGroup:
        """
        Query Alert Group List Paging
        
        Paging query alarm group list
        
        DS operation: AlertGroupController.listPaging | GET /alert-groups
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            alert group list page
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "alert-groups",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(PageInfoAlertGroup))

    def create_alert_group(
        self,
        form: CreateAlertGroupParams
    ) -> AlertGroup:
        """
        Create Alert Group
        
        Create alert group
        
        DS operation: AlertGroupController.createAlertGroup | POST /alert-groups
        
        Args:
            form: Form parameters bag for this operation.
        
        Returns:
            create result code
        """
        data = self._model_mapping(form)
        payload = self._request(
            "POST",
            "alert-groups",
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(AlertGroup))

    def list(
        self
    ) -> list[AlertGroup]:
        """
        List Alert Group By Id
        
        Alert group list
        
        DS operation: AlertGroupController.list | GET /alert-groups/list
        
        Returns:
            alert group list
        """
        payload = self._request("GET", "alert-groups/list")
        return self._validate_payload(payload, TypeAdapter(list[AlertGroup]))

    def query_alert_group_by_id(
        self,
        form: QueryAlertGroupByIdParams
    ) -> AlertGroup:
        """
        Query Alert Group By Id
        
        Check alarm group detail by id
        
        DS operation: AlertGroupController.queryAlertGroupById | POST /alert-groups/query
        
        Args:
            form: Form parameters bag for this operation.
        
        Returns:
            one alert group
        """
        data = self._model_mapping(form)
        payload = self._request(
            "POST",
            "alert-groups/query",
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(AlertGroup))

    def verify_group_name(
        self,
        params: VerifyGroupNameParams
    ) -> None:
        """
        Verify Group Name
        
        Check alert group exist
        
        DS operation: AlertGroupController.verifyGroupName | GET /alert-groups/verify-name
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            check result code
        """
        query_params = self._model_mapping(params)
        self._request(
            "GET",
            "alert-groups/verify-name",
        params=query_params,
        )
        return None

    def delete_alert_group_by_id(
        self,
        id: int
    ) -> bool:
        """
        Del Alert Group By Id
        
        Delete alert group by id
        
        DS operation: AlertGroupController.deleteAlertGroupById | DELETE /alert-groups/{id}
        
        Args:
            id: alert group id
        
        Returns:
            delete result code
        """
        path = f"alert-groups/{id}"
        payload = self._request("DELETE", path)
        return self._validate_payload(payload, TypeAdapter(bool))

    def update_alert_group_by_id(
        self,
        id: int,
        form: UpdateAlertGroupByIdParams
    ) -> AlertGroup:
        """
        Update Alert Group
        
        UpdateWorkflowInstance alert group
        
        DS operation: AlertGroupController.updateAlertGroupById | PUT /alert-groups/{id}
        
        Args:
            id: alert group id
            form: Form parameters bag for this operation.
        
        Returns:
            update result code
        """
        path = f"alert-groups/{id}"
        data = self._model_mapping(form)
        payload = self._request(
            "PUT",
            path,
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(AlertGroup))

__all__ = ["AlertGroupOperations"]
