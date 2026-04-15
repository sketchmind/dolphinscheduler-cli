from __future__ import annotations

from ._base import BaseRequestsClient, BaseParamsModel

from pydantic import Field, TypeAdapter

from ...dao.entities.queue import Queue
from ..contracts.page_info import PageInfoQueue

class QueryQueueListPagingParams(BaseParamsModel):
    """
    Query Queue List Paging
    
    Query parameters for QueueController.queryQueueListPaging.
    """
    pageNo: int = Field(description='page number', examples=[1])
    searchVal: str | None = Field(default=None, description='search value')
    pageSize: int = Field(description='page size', examples=[20])

class CreateQueueParams(BaseParamsModel):
    """
    Create Queue
    
    Form parameters for QueueController.createQueue.
    """
    queue: str = Field(description='queue')
    queueName: str = Field(description='queue name')

class VerifyQueueParams(BaseParamsModel):
    """
    Verify Queue
    
    Form parameters for QueueController.verifyQueue.
    """
    queue: str = Field(description='queue')
    queueName: str = Field(description='queue name')

class UpdateQueueParams(BaseParamsModel):
    """
    Update Queue
    
    Form parameters for QueueController.updateQueue.
    """
    queue: str = Field(description='queue')
    queueName: str = Field(description='queue name')

class QueueOperations(BaseRequestsClient):
    def query_queue_list_paging(
        self,
        params: QueryQueueListPagingParams
    ) -> PageInfoQueue:
        """
        Query Queue List Paging
        
        Query queue list paging
        
        DS operation: QueueController.queryQueueListPaging | GET /queues
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            queue list
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "queues",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(PageInfoQueue))

    def create_queue(
        self,
        form: CreateQueueParams
    ) -> Queue:
        """
        Create Queue
        
        Create queue
        
        DS operation: QueueController.createQueue | POST /queues
        
        Args:
            form: Form parameters bag for this operation.
        
        Returns:
            create result
        """
        data = self._model_mapping(form)
        payload = self._request(
            "POST",
            "queues",
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(Queue))

    def query_list(
        self
    ) -> list[Queue]:
        """
        Query List
        
        Query queue list
        
        DS operation: QueueController.queryList | GET /queues/list
        
        Returns:
            queue list
        """
        payload = self._request("GET", "queues/list")
        return self._validate_payload(payload, TypeAdapter(list[Queue]))

    def verify_queue(
        self,
        form: VerifyQueueParams
    ) -> bool:
        """
        Verify Queue
        
        Verify queue and queue name
        
        DS operation: QueueController.verifyQueue | POST /queues/verify
        
        Args:
            form: Form parameters bag for this operation.
        
        Returns:
            true if the queue name not exists, otherwise return false
        """
        data = self._model_mapping(form)
        payload = self._request(
            "POST",
            "queues/verify",
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(bool))

    def delete_queue_by_id(
        self,
        id: int
    ) -> bool:
        """
        Delete Queue By Id
        
        Delete queue by id
        
        DS operation: QueueController.deleteQueueById | DELETE /queues/{id}
        
        Args:
            id: queue id
        
        Returns:
            update result code
        """
        path = f"queues/{id}"
        payload = self._request("DELETE", path)
        return self._validate_payload(payload, TypeAdapter(bool))

    def update_queue(
        self,
        id: int,
        form: UpdateQueueParams
    ) -> Queue:
        """
        Update Queue
        
        Update queue
        
        DS operation: QueueController.updateQueue | PUT /queues/{id}
        
        Args:
            id: queue id
            form: Form parameters bag for this operation.
        
        Returns:
            update result code
        """
        path = f"queues/{id}"
        data = self._model_mapping(form)
        payload = self._request(
            "PUT",
            path,
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(Queue))

__all__ = ["QueueOperations"]
