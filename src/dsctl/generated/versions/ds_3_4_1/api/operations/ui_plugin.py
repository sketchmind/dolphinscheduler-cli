from __future__ import annotations

from ._base import BaseRequestsClient, BaseParamsModel

from pydantic import Field, TypeAdapter

from ...common.enums.plugin_type import PluginType
from ...dao.entities.plugin_define import PluginDefine
from ..contracts.product_info_dto import ProductInfoDto

class QueryUiPluginsByTypeParams(BaseParamsModel):
    """
    Query Ui Plugins By Type
    
    Query parameters for UiPluginController.queryUiPluginsByType.
    """
    pluginType: PluginType = Field(description='pluginType')

class UiPluginOperations(BaseRequestsClient):
    def query_ui_plugins_by_type(
        self,
        params: QueryUiPluginsByTypeParams
    ) -> list[PluginDefine]:
        """
        Query Ui Plugins By Type
        
        DS operation: UiPluginController.queryUiPluginsByType | GET /ui-plugins/query-by-type
        
        Args:
            params: Query parameters bag for this operation.
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "ui-plugins/query-by-type",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(list[PluginDefine]))

    def query_product_info(
        self
    ) -> ProductInfoDto:
        """
        Query Product Info
        
        DS operation: UiPluginController.queryProductInfo | GET /ui-plugins/query-product-info
        """
        payload = self._request("GET", "ui-plugins/query-product-info")
        return self._validate_payload(payload, TypeAdapter(ProductInfoDto))

    def query_ui_plugin_detail_by_id(
        self,
        id: int
    ) -> PluginDefine:
        """
        Query Ui Plugin Detail By Id
        
        DS operation: UiPluginController.queryUiPluginDetailById | GET /ui-plugins/{id}
        """
        path = f"ui-plugins/{id}"
        payload = self._request("GET", path)
        return self._validate_payload(payload, TypeAdapter(PluginDefine))

__all__ = ["UiPluginOperations"]
