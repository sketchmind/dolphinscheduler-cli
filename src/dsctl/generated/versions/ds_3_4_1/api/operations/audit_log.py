from __future__ import annotations

from ._base import BaseRequestsClient, BaseParamsModel

from pydantic import Field, TypeAdapter

from ..contracts.audit_log.audit_model_type_dto import AuditModelTypeDto
from ..contracts.audit_log.audit_operation_type_dto import AuditOperationTypeDto
from ..contracts.page_info import PageInfoAuditDto

class QueryAuditLogListPagingParams(BaseParamsModel):
    """
    Query Audit Log List Paging
    
    Query parameters for AuditLogController.queryAuditLogListPaging.
    """
    pageNo: int = Field(description='page number', examples=[1])
    pageSize: int = Field(description='page size', examples=[20])
    modelTypes: str | None = Field(default=None, description='model types')
    operationTypes: str | None = Field(default=None, description='operation types')
    startDate: str | None = Field(default=None, description='start time')
    endDate: str | None = Field(default=None, description='end time')
    userName: str | None = Field(default=None, description='user name')
    modelName: str | None = Field(default=None, description='model name')

class AuditLogOperations(BaseRequestsClient):
    def query_audit_log_list_paging(
        self,
        params: QueryAuditLogListPagingParams
    ) -> PageInfoAuditDto:
        """
        Query Audit Log List Paging
        
        Query audit log list paging
        
        DS operation: AuditLogController.queryAuditLogListPaging | GET /projects/audit/audit-log-list
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            audit log content
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "projects/audit/audit-log-list",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(PageInfoAuditDto))

    def query_audit_model_type_list(
        self
    ) -> list[AuditModelTypeDto]:
        """
        Query Audit Model Type List
        
        Query audit log model type list
        
        DS operation: AuditLogController.queryAuditModelTypeList | GET /projects/audit/audit-log-model-type
        
        Returns:
            model type list
        """
        payload = self._request("GET", "projects/audit/audit-log-model-type")
        return self._validate_payload(payload, TypeAdapter(list[AuditModelTypeDto]))

    def query_audit_operation_type_list(
        self
    ) -> list[AuditOperationTypeDto]:
        """
        Query Audit Operation Type List
        
        Query audit log operation type list
        
        DS operation: AuditLogController.queryAuditOperationTypeList | GET /projects/audit/audit-log-operation-type
        
        Returns:
            object type list
        """
        payload = self._request("GET", "projects/audit/audit-log-operation-type")
        return self._validate_payload(payload, TypeAdapter(list[AuditOperationTypeDto]))

__all__ = ["AuditLogOperations"]
