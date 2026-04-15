from __future__ import annotations

from ._base import BaseRequestsClient, BaseParamsModel

from pydantic import Field, TypeAdapter

from ...dao.entities.response_task_log import ResponseTaskLog

class QueryLogGetLogDetailParams(BaseParamsModel):
    """
    Query Log
    
    Query parameters for LoggerController.queryLog__get_log_detail.
    """
    taskInstanceId: int = Field(description='task instance id', examples=[100])
    skipLineNum: int = Field(description='skip number', examples=[100])
    limit: int = Field(description='limit', examples=[100])

class DownloadTaskLogGetLogDownloadLogParams(BaseParamsModel):
    """
    Download Task Log
    
    Query parameters for LoggerController.downloadTaskLog__get_log_download_log.
    """
    taskInstanceId: int = Field(description='task instance id', examples=[100])

class QueryLogGetLogProjectCodeDetailParams(BaseParamsModel):
    """
    Query Log In Specified Project
    
    Query parameters for LoggerController.queryLog__get_log_projectCode_detail.
    """
    taskInstanceId: int = Field(description='task instance id', examples=[100])
    skipLineNum: int = Field(description='skip number', examples=[100])
    limit: int = Field(description='limit', examples=[100])

class DownloadTaskLogGetLogProjectCodeDownloadLogParams(BaseParamsModel):
    """
    Download Task Log In Specified Project
    
    Query parameters for LoggerController.downloadTaskLog__get_log_projectCode_download_log.
    """
    taskInstanceId: int = Field(description='task instance id', examples=[100])

class LoggerOperations(BaseRequestsClient):
    def query_log_get_log_detail(
        self,
        params: QueryLogGetLogDetailParams
    ) -> ResponseTaskLog:
        """
        Query Log
        
        Query task log
        
        DS operation: LoggerController.queryLog | GET /log/detail
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            task log content
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "log/detail",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(ResponseTaskLog))

    def download_task_log_get_log_download_log(
        self,
        params: DownloadTaskLogGetLogDownloadLogParams
    ) -> bytes:
        """
        Download Task Log
        
        Download log file
        
        DS operation: LoggerController.downloadTaskLog | GET /log/download-log
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            log file content
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "log/download-log",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(bytes))

    def query_log_get_log_project_code_detail(
        self,
        project_code: int,
        params: QueryLogGetLogProjectCodeDetailParams
    ) -> str:
        """
        Query Log In Specified Project
        
        Query task log in specified project
        
        DS operation: LoggerController.queryLog | GET /log/{projectCode}/detail
        
        Args:
            project_code: project code
            params: Query parameters bag for this operation.
        
        Returns:
            task log content
        """
        path = f"log/{project_code}/detail"
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            path,
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(str))

    def download_task_log_get_log_project_code_download_log(
        self,
        project_code: int,
        params: DownloadTaskLogGetLogProjectCodeDownloadLogParams
    ) -> bytes:
        """
        Download Task Log In Specified Project
        
        Download log file
        
        DS operation: LoggerController.downloadTaskLog | GET /log/{projectCode}/download-log
        
        Args:
            project_code: project code
            params: Query parameters bag for this operation.
        
        Returns:
            log file content
        """
        path = f"log/{project_code}/download-log"
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            path,
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(bytes))

__all__ = ["LoggerOperations"]
