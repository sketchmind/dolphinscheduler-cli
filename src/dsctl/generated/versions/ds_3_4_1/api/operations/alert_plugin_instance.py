from __future__ import annotations

from ._base import BaseRequestsClient, BaseParamsModel

from pydantic import Field, TypeAdapter

from ...dao.entities.alert_plugin_instance import AlertPluginInstance
from ..contracts.page_info import PageInfoAlertPluginInstanceVO
from ..views.alert_plugin_instance import AlertPluginInstanceVO

class ListPagingParams(BaseParamsModel):
    """
    Query Alert Plugin Instance List Paging
    
    Query parameters for AlertPluginInstanceController.listPaging.
    """
    searchVal: str | None = Field(default=None, description='search value')
    pageNo: int = Field(description='page number', examples=[1])
    pageSize: int = Field(description='page size', examples=[20])

class CreateAlertPluginInstanceParams(BaseParamsModel):
    """
    Create Alert Plugin Instance
    
    Form parameters for AlertPluginInstanceController.createAlertPluginInstance.
    """
    pluginDefineId: int = Field(description='alert plugin define id', examples=[100])
    instanceName: str = Field(description='instance name', examples=['DING TALK'])
    pluginInstanceParams: str = Field(description='instance params', examples=['ALERT_PLUGIN_INSTANCE_PARAMS'])

class TestSendAlertPluginInstanceParams(BaseParamsModel):
    """
    Test Send Alert Plugin Instance
    
    Form parameters for AlertPluginInstanceController.testSendAlertPluginInstance.
    """
    pluginDefineId: int = Field(examples=[100])
    pluginInstanceParams: str = Field(examples=['ALERT_PLUGIN_INSTANCE_PARAMS'])

class VerifyGroupNameParams(BaseParamsModel):
    """
    Verify Alert Instance Name
    
    Query parameters for AlertPluginInstanceController.verifyGroupName.
    """
    alertInstanceName: str = Field(description='alert instance name')

class UpdateAlertPluginInstanceByIdParams(BaseParamsModel):
    """
    Update Alert Plugin Instance
    
    Form parameters for AlertPluginInstanceController.updateAlertPluginInstanceById.
    """
    instanceName: str = Field(description='instance name', examples=['DING TALK'])
    pluginInstanceParams: str = Field(description='instance params', examples=['ALERT_PLUGIN_INSTANCE_PARAMS'])

class AlertPluginInstanceOperations(BaseRequestsClient):
    def list_paging(
        self,
        params: ListPagingParams
    ) -> PageInfoAlertPluginInstanceVO:
        """
        Query Alert Plugin Instance List Paging
        
        Paging query alert plugin instance group list
        
        DS operation: AlertPluginInstanceController.listPaging | GET /alert-plugin-instances
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            alert plugin instance list page
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "alert-plugin-instances",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(PageInfoAlertPluginInstanceVO))

    def create_alert_plugin_instance(
        self,
        form: CreateAlertPluginInstanceParams
    ) -> AlertPluginInstance:
        """
        Create Alert Plugin Instance
        
        Create alert plugin instance
        
        DS operation: AlertPluginInstanceController.createAlertPluginInstance | POST /alert-plugin-instances
        
        Args:
            form: Form parameters bag for this operation.
        
        Returns:
            result
        """
        data = self._model_mapping(form)
        payload = self._request(
            "POST",
            "alert-plugin-instances",
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(AlertPluginInstance))

    def get_alert_plugin_instance_get_alert_plugin_instances_list(
        self
    ) -> list[AlertPluginInstanceVO] | None:
        """
        Query Alert Plugin Instance List
        
        GetAlertPluginInstance
        
        DS operation: AlertPluginInstanceController.getAlertPluginInstance | GET /alert-plugin-instances/list
        
        Returns:
            result
        """
        payload = self._request("GET", "alert-plugin-instances/list")
        return self._validate_payload(payload, TypeAdapter(list[AlertPluginInstanceVO] | None))

    def test_send_alert_plugin_instance(
        self,
        form: TestSendAlertPluginInstanceParams
    ) -> bool:
        """
        Test Send Alert Plugin Instance
        
        DS operation: AlertPluginInstanceController.testSendAlertPluginInstance | POST /alert-plugin-instances/test-send
        
        Args:
            form: Form parameters bag for this operation.
        """
        data = self._model_mapping(form)
        payload = self._request(
            "POST",
            "alert-plugin-instances/test-send",
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(bool))

    def verify_group_name(
        self,
        params: VerifyGroupNameParams
    ) -> None:
        """
        Verify Alert Instance Name
        
        Check alert group exist
        
        DS operation: AlertPluginInstanceController.verifyGroupName | GET /alert-plugin-instances/verify-name
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            check result code
        """
        query_params = self._model_mapping(params)
        self._request(
            "GET",
            "alert-plugin-instances/verify-name",
        params=query_params,
        )
        return None

    def delete_alert_plugin_instance(
        self,
        id: int
    ) -> bool:
        """
        Delete Alert Plugin Instance
        
        DeleteAlertPluginInstance
        
        DS operation: AlertPluginInstanceController.deleteAlertPluginInstance | DELETE /alert-plugin-instances/{id}
        
        Args:
            id: id
        
        Returns:
            result
        """
        path = f"alert-plugin-instances/{id}"
        payload = self._request("DELETE", path)
        return self._validate_payload(payload, TypeAdapter(bool))

    def get_alert_plugin_instance_get_alert_plugin_instances_id(
        self,
        id: int
    ) -> AlertPluginInstance:
        """
        Get Alert Plugin Instance
        
        GetAlertPluginInstance
        
        DS operation: AlertPluginInstanceController.getAlertPluginInstance | GET /alert-plugin-instances/{id}
        
        Args:
            id: alert plugin instance id
        
        Returns:
            result
        """
        path = f"alert-plugin-instances/{id}"
        payload = self._request("GET", path)
        return self._validate_payload(payload, TypeAdapter(AlertPluginInstance))

    def update_alert_plugin_instance_by_id(
        self,
        id: int,
        form: UpdateAlertPluginInstanceByIdParams
    ) -> AlertPluginInstance:
        """
        Update Alert Plugin Instance
        
        UpdateAlertPluginInstance
        
        DS operation: AlertPluginInstanceController.updateAlertPluginInstanceById | PUT /alert-plugin-instances/{id}
        
        Args:
            id: alert plugin instance id
            form: Form parameters bag for this operation.
        
        Returns:
            result
        """
        path = f"alert-plugin-instances/{id}"
        data = self._model_mapping(form)
        payload = self._request(
            "PUT",
            path,
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(AlertPluginInstance))

__all__ = ["AlertPluginInstanceOperations"]
