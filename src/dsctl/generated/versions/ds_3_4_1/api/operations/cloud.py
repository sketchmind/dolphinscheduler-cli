from __future__ import annotations

from ._base import BaseRequestsClient, BaseParamsModel

from pydantic import Field, TypeAdapter

class ListPipelineParams(BaseParamsModel):
    """
    List Pipeline
    
    Query parameters for CloudController.listPipeline.
    """
    factoryName: str
    resourceGroupName: str

class CloudOperations(BaseRequestsClient):
    def list_data_factory(
        self
    ) -> list[str]:
        """
        List Data Factory
        
        Get data factory list
        
        DS operation: CloudController.listDataFactory | GET /cloud/azure/datafactory/factories
        
        Returns:
            data factory name list
        """
        payload = self._request("GET", "cloud/azure/datafactory/factories")
        return self._validate_payload(payload, TypeAdapter(list[str]))

    def list_pipeline(
        self,
        params: ListPipelineParams
    ) -> list[str]:
        """
        List Pipeline
        
        Get resourceGroup list
        
        DS operation: CloudController.listPipeline | GET /cloud/azure/datafactory/pipelines
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            resourceGroup list
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "cloud/azure/datafactory/pipelines",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(list[str]))

    def list_resource_group(
        self
    ) -> list[str]:
        """
        List Resource Group
        
        Get resourceGroup list
        
        DS operation: CloudController.listResourceGroup | GET /cloud/azure/datafactory/resourceGroups
        
        Returns:
            resourceGroup list
        """
        payload = self._request("GET", "cloud/azure/datafactory/resourceGroups")
        return self._validate_payload(payload, TypeAdapter(list[str]))

__all__ = ["CloudOperations"]
