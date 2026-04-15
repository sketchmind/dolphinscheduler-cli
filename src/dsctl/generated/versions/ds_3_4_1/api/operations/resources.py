from __future__ import annotations

from ._base import BaseRequestsClient, BaseParamsModel, UploadFileLike

from pydantic import Field, TypeAdapter

from ...spi.enums.resource_type import ResourceType
from ..contracts.page_info import PageInfoResourceItemVO
from ..contracts.resources.resource_component import ResourceComponent
from ..views.resources import FetchFileContentResponse

class DeleteResourceParams(BaseParamsModel):
    """
    Delete Resource
    
    Query parameters for ResourcesController.deleteResource.
    """
    fullName: str = Field(examples=['file:////tmp/dolphinscheduler/storage/default/resources/demo.sql'])

class PagingResourceItemParams(BaseParamsModel):
    """
    Paging Resource Item Request
    
    Query parameters for ResourcesController.pagingResourceItemRequest.
    """
    fullName: str = Field(examples=['bucket_name/tenant_name/type/ds'])
    type: ResourceType
    searchVal: str | None = Field(default=None)
    pageNo: int = Field(examples=[1])
    pageSize: int = Field(examples=[20])

class CreateFileParams(BaseParamsModel):
    """
    Upload File
    
    Form parameters for ResourcesController.createFile.
    """
    type: ResourceType
    name: str
    file: UploadFileLike
    currentDir: str

class UpdateResourceParams(BaseParamsModel):
    """
    Update Resource
    
    Form parameters for ResourcesController.updateResource.
    """
    fullName: str
    name: str
    file: UploadFileLike | None = Field(default=None)

class QueryResourceBaseDirParams(BaseParamsModel):
    """
    Query Resource Base Dir
    
    Query parameters for ResourcesController.queryResourceBaseDir.
    """
    type: ResourceType

class CreateDirectoryParams(BaseParamsModel):
    """
    Create Directory
    
    Form parameters for ResourcesController.createDirectory.
    """
    type: ResourceType
    name: str
    currentDir: str

class DownloadResourceParams(BaseParamsModel):
    """
    Download Resource
    
    Query parameters for ResourcesController.downloadResource.
    """
    fullName: str = Field(examples=['test/'])

class QueryResourceListParams(BaseParamsModel):
    """
    Query Resource List
    
    Query parameters for ResourcesController.queryResourceList.
    """
    type: ResourceType

class CreateFileFromContentParams(BaseParamsModel):
    """
    Create File From Content
    
    Form parameters for ResourcesController.createFileFromContent.
    """
    type: ResourceType
    fileName: str
    suffix: str
    content: str
    currentDir: str

class QueryResourceFileListParams(BaseParamsModel):
    """
    Query Resource File List
    
    Query parameters for ResourcesController.queryResourceFileList.
    """
    type: ResourceType

class UpdateFileContentParams(BaseParamsModel):
    """
    Update File Content
    
    Form parameters for ResourcesController.updateFileContent.
    """
    fullName: str
    content: str

class ViewResourceParams(BaseParamsModel):
    """
    View Resource
    
    Query parameters for ResourcesController.viewResource.
    """
    fullName: str = Field(examples=['tenant/1.png'])
    skipLineNum: int = Field(examples=[100])
    limit: int = Field(examples=[100])

class ResourcesOperations(BaseRequestsClient):
    def delete_resource(
        self,
        params: DeleteResourceParams
    ) -> None:
        """
        Delete Resource
        
        DS operation: ResourcesController.deleteResource | DELETE /resources
        
        Args:
            params: Query parameters bag for this operation.
        """
        query_params = self._model_mapping(params)
        self._request(
            "DELETE",
            "resources",
        params=query_params,
        )
        return None

    def paging_resource_item_request(
        self,
        params: PagingResourceItemParams
    ) -> PageInfoResourceItemVO:
        """
        Paging Resource Item Request
        
        DS operation: ResourcesController.pagingResourceItemRequest | GET /resources
        
        Args:
            params: Query parameters bag for this operation.
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "resources",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(PageInfoResourceItemVO))

    def create_file(
        self,
        form: CreateFileParams
    ) -> None:
        """
        Upload File
        
        DS operation: ResourcesController.createFile | POST /resources
        
        Args:
            form: Form parameters bag for this operation.
        """
        data = self._model_mapping(form)
        self._request(
            "POST",
            "resources",
        data=data,
        )
        return None

    def update_resource(
        self,
        form: UpdateResourceParams
    ) -> None:
        """
        Update Resource
        
        DS operation: ResourcesController.updateResource | PUT /resources
        
        Args:
            form: Form parameters bag for this operation.
        """
        data = self._model_mapping(form)
        self._request(
            "PUT",
            "resources",
        data=data,
        )
        return None

    def query_resource_base_dir(
        self,
        params: QueryResourceBaseDirParams
    ) -> str:
        """
        Query Resource Base Dir
        
        DS operation: ResourcesController.queryResourceBaseDir | GET /resources/base-dir
        
        Args:
            params: Query parameters bag for this operation.
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "resources/base-dir",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(str))

    def create_directory(
        self,
        form: CreateDirectoryParams
    ) -> None:
        """
        Create Directory
        
        DS operation: ResourcesController.createDirectory | POST /resources/directory
        
        Args:
            form: Form parameters bag for this operation.
        """
        data = self._model_mapping(form)
        self._request(
            "POST",
            "resources/directory",
        data=data,
        )
        return None

    def download_resource(
        self,
        params: DownloadResourceParams
    ) -> None:
        """
        Download Resource
        
        DS operation: ResourcesController.downloadResource | GET /resources/download
        
        Args:
            params: Query parameters bag for this operation.
        """
        query_params = self._model_mapping(params)
        self._request(
            "GET",
            "resources/download",
        params=query_params,
        )
        return None

    def query_resource_list(
        self,
        params: QueryResourceListParams
    ) -> ResourceComponent:
        """
        Query Resource List
        
        DS operation: ResourcesController.queryResourceList | GET /resources/list
        
        Args:
            params: Query parameters bag for this operation.
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "resources/list",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(ResourceComponent))

    def create_file_from_content(
        self,
        form: CreateFileFromContentParams
    ) -> None:
        """
        Create File From Content
        
        DS operation: ResourcesController.createFileFromContent | POST /resources/online-create
        
        Args:
            form: Form parameters bag for this operation.
        """
        data = self._model_mapping(form)
        self._request(
            "POST",
            "resources/online-create",
        data=data,
        )
        return None

    def query_resource_file_list(
        self,
        params: QueryResourceFileListParams
    ) -> ResourceComponent:
        """
        Query Resource File List
        
        DS operation: ResourcesController.queryResourceFileList | GET /resources/query-by-type
        
        Args:
            params: Query parameters bag for this operation.
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "resources/query-by-type",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(ResourceComponent))

    def update_file_content(
        self,
        form: UpdateFileContentParams
    ) -> None:
        """
        Update File Content
        
        DS operation: ResourcesController.updateFileContent | PUT /resources/update-content
        
        Args:
            form: Form parameters bag for this operation.
        """
        data = self._model_mapping(form)
        self._request(
            "PUT",
            "resources/update-content",
        data=data,
        )
        return None

    def view_resource(
        self,
        params: ViewResourceParams
    ) -> FetchFileContentResponse:
        """
        View Resource
        
        DS operation: ResourcesController.viewResource | GET /resources/view
        
        Args:
            params: Query parameters bag for this operation.
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "resources/view",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(FetchFileContentResponse))

__all__ = ["ResourcesOperations"]
