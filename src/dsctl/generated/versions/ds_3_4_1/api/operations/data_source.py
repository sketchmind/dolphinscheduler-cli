from __future__ import annotations

from ._base import BaseRequestsClient, BaseParamsModel

from pydantic import Field, TypeAdapter

from ...dao.entities.data_source import DataSource
from ...plugin.datasource_api.datasource.base_data_source_param_dto import BaseDataSourceParamDTO
from ...spi.enums.db_type import DbType
from ...spi.params.base.params_options import ParamsOptions
from ..contracts.page_info import PageInfoDataSource

class QueryDataSourceListPagingParams(BaseParamsModel):
    """
    Query Data Source List Paging
    
    Query parameters for DataSourceController.queryDataSourceListPaging.
    """
    searchVal: str | None = Field(default=None, description='search value')
    pageNo: int = Field(description='page number', examples=[1])
    pageSize: int = Field(description='page size', examples=[20])

class AuthedDatasourceParams(BaseParamsModel):
    """
    Authed Datasource
    
    Query parameters for DataSourceController.authedDatasource.
    """
    userId: int = Field(description='user id', examples=[100])

class GetDatabasesParams(BaseParamsModel):
    """
    Databases
    
    Query parameters for DataSourceController.getDatabases.
    """
    datasourceId: int = Field(examples=[1])

class QueryDataSourceListParams(BaseParamsModel):
    """
    Query Data Source List
    
    Query parameters for DataSourceController.queryDataSourceList.
    """
    type: DbType = Field(description='data source type')

class GetTableColumnsParams(BaseParamsModel):
    """
    Table Columns
    
    Query parameters for DataSourceController.getTableColumns.
    """
    datasourceId: int = Field(examples=[1])
    tableName: str = Field(examples=['test'])
    database: str = Field(examples=['test'])

class GetTablesParams(BaseParamsModel):
    """
    Tables
    
    Query parameters for DataSourceController.getTables.
    """
    datasourceId: int = Field(examples=[1])
    database: str = Field(examples=['test'])

class UnAuthDatasourceParams(BaseParamsModel):
    """
    Unauthorized Datasource
    
    Query parameters for DataSourceController.unAuthDatasource.
    """
    userId: int = Field(description='user id', examples=[100])

class VerifyDataSourceNameParams(BaseParamsModel):
    """
    Verify Data Source Name
    
    Query parameters for DataSourceController.verifyDataSourceName.
    """
    name: str = Field(description='data source name')

class DataSourceOperations(BaseRequestsClient):
    def query_data_source_list_paging(
        self,
        params: QueryDataSourceListPagingParams
    ) -> PageInfoDataSource:
        """
        Query Data Source List Paging
        
        Query datasource with paging
        
        DS operation: DataSourceController.queryDataSourceListPaging | GET /datasources
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            data source list page
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "datasources",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(PageInfoDataSource))

    def create_data_source(
        self,
        json_str: str
    ) -> DataSource:
        """
        Create Data Source
        
        Create data source
        
        DS operation: DataSourceController.createDataSource | POST /datasources
        
        Args:
            json_str: Request body payload.
        
        Returns:
            create result code
        """
        headers = {"Content-Type": "application/json"}
        payload = self._request(
            "POST",
            "datasources",
        content=json_str,
        headers=headers,
        )
        return self._validate_payload(payload, TypeAdapter(DataSource))

    def authed_datasource(
        self,
        params: AuthedDatasourceParams
    ) -> list[DataSource]:
        """
        Authed Datasource
        
        Authorized datasource
        
        DS operation: DataSourceController.authedDatasource | GET /datasources/authed-datasource
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            authorized result code
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "datasources/authed-datasource",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(list[DataSource]))

    def connect_data_source(
        self,
        json_str: str
    ) -> bool:
        """
        Connect Data Source
        
        Connect datasource
        
        DS operation: DataSourceController.connectDataSource | POST /datasources/connect
        
        Args:
            json_str: Request body payload.
        
        Returns:
            connect result code
        """
        headers = {"Content-Type": "application/json"}
        payload = self._request(
            "POST",
            "datasources/connect",
        content=json_str,
        headers=headers,
        )
        return self._validate_payload(payload, TypeAdapter(bool))

    def get_databases(
        self,
        params: GetDatabasesParams
    ) -> list[ParamsOptions]:
        """
        Databases
        
        DS operation: DataSourceController.getDatabases | GET /datasources/databases
        
        Args:
            params: Query parameters bag for this operation.
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "datasources/databases",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(list[ParamsOptions]))

    def get_kerberos_startup_state(
        self
    ) -> bool:
        """
        Get Kerberos Startup State
        
        Get user info
        
        DS operation: DataSourceController.getKerberosStartupState | GET /datasources/kerberos-startup-state
        
        Returns:
            user info data
        """
        payload = self._request("GET", "datasources/kerberos-startup-state")
        return self._validate_payload(payload, TypeAdapter(bool))

    def query_data_source_list(
        self,
        params: QueryDataSourceListParams
    ) -> list[DataSource]:
        """
        Query Data Source List
        
        Query datasource by type
        
        DS operation: DataSourceController.queryDataSourceList | GET /datasources/list
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            data source list page
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "datasources/list",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(list[DataSource]))

    def get_table_columns(
        self,
        params: GetTableColumnsParams
    ) -> list[ParamsOptions]:
        """
        Table Columns
        
        DS operation: DataSourceController.getTableColumns | GET /datasources/tableColumns
        
        Args:
            params: Query parameters bag for this operation.
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "datasources/tableColumns",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(list[ParamsOptions]))

    def get_tables(
        self,
        params: GetTablesParams
    ) -> list[ParamsOptions]:
        """
        Tables
        
        DS operation: DataSourceController.getTables | GET /datasources/tables
        
        Args:
            params: Query parameters bag for this operation.
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "datasources/tables",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(list[ParamsOptions]))

    def un_auth_datasource(
        self,
        params: UnAuthDatasourceParams
    ) -> list[DataSource]:
        """
        Unauthorized Datasource
        
        Unauthorized datasource
        
        DS operation: DataSourceController.unAuthDatasource | GET /datasources/unauth-datasource
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            unauthorized data source result code
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "datasources/unauth-datasource",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(list[DataSource]))

    def verify_data_source_name(
        self,
        params: VerifyDataSourceNameParams
    ) -> bool:
        """
        Verify Data Source Name
        
        Verify datasource name
        
        DS operation: DataSourceController.verifyDataSourceName | GET /datasources/verify-name
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            true if data source name not exists, otherwise return false
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "datasources/verify-name",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(bool))

    def delete_data_source(
        self,
        id: int
    ) -> bool:
        """
        Delete Data Source
        
        Delete datasource by id
        
        DS operation: DataSourceController.deleteDataSource | DELETE /datasources/{id}
        
        Args:
            id: datasource id
        
        Returns:
            delete result
        """
        path = f"datasources/{id}"
        payload = self._request("DELETE", path)
        return self._validate_payload(payload, TypeAdapter(bool))

    def query_data_source(
        self,
        id: int
    ) -> BaseDataSourceParamDTO:
        """
        Query Data Source
        
        Query data source detail
        
        DS operation: DataSourceController.queryDataSource | GET /datasources/{id}
        
        Args:
            id: datasource id
        
        Returns:
            data source detail
        """
        path = f"datasources/{id}"
        payload = self._request("GET", path)
        return self._validate_payload(payload, TypeAdapter(BaseDataSourceParamDTO))

    def update_data_source(
        self,
        id: int,
        json_str: str
    ) -> DataSource:
        """
        Update Data Source
        
        UpdateWorkflowInstance data source
        
        DS operation: DataSourceController.updateDataSource | PUT /datasources/{id}
        
        Args:
            id: datasource id
            json_str: Request body payload.
        
        Returns:
            update result code
        """
        path = f"datasources/{id}"
        headers = {"Content-Type": "application/json"}
        payload = self._request(
            "PUT",
            path,
        content=json_str,
        headers=headers,
        )
        return self._validate_payload(payload, TypeAdapter(DataSource))

    def connection_test(
        self,
        id: int
    ) -> bool:
        """
        Connection Test
        
        Connection test
        
        DS operation: DataSourceController.connectionTest | GET /datasources/{id}/connect-test
        
        Args:
            id: data source id
        
        Returns:
            connect result code
        """
        path = f"datasources/{id}/connect-test"
        payload = self._request("GET", path)
        return self._validate_payload(payload, TypeAdapter(bool))

__all__ = ["DataSourceOperations"]
