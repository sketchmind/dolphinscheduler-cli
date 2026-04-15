from __future__ import annotations

from ._base import BaseRequestsClient

from pydantic import TypeAdapter

from ...dao.entities.queue import Queue
from ..contracts.page_info import PageInfoQueue
from ..contracts.queue.queue_create_request import QueueCreateRequest
from ..contracts.queue.queue_query_request import QueueQueryRequest
from ..contracts.queue.queue_update_request import QueueUpdateRequest
from ..contracts.queue.queue_verify_request import QueueVerifyRequest

class QueueV2Operations(BaseRequestsClient):
    def query_queue_list_paging(
        self,
        request: QueueQueryRequest
    ) -> PageInfoQueue:
        """
        Query Queue List Paging
        
        Query queue list paging
        
        DS operation: QueueV2Controller.queryQueueListPaging | GET /v2/queues
        
        Args:
            request: Request payload.
        
        Returns:
            queue list
        """
        query_params = self._model_mapping(request)
        payload = self._request(
            "GET",
            "v2/queues",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(PageInfoQueue))

    def create_queue(
        self,
        queue_create_request: QueueCreateRequest
    ) -> Queue:
        """
        Create Queue
        
        Create queue
        
        DS operation: QueueV2Controller.createQueue | POST /v2/queues
        
        Args:
            queue_create_request: Request body payload.
        
        Returns:
            create result
        """
        payload = self._request(
            "POST",
            "v2/queues",
        json=self._json_payload(queue_create_request),
        )
        return self._validate_payload(payload, TypeAdapter(Queue))

    def query_list(
        self
    ) -> list[Queue]:
        """
        Query List
        
        Query queue list
        
        DS operation: QueueV2Controller.queryList | GET /v2/queues/list
        
        Returns:
            queue list
        """
        payload = self._request("GET", "v2/queues/list")
        return self._validate_payload(payload, TypeAdapter(list[Queue]))

    def verify_queue(
        self,
        queue_verify_request: QueueVerifyRequest
    ) -> bool:
        """
        Verify Queue
        
        Verify queue and queue name
        
        DS operation: QueueV2Controller.verifyQueue | POST /v2/queues/verify
        
        Args:
            queue_verify_request: Request body payload.
        
        Returns:
            true if the queue name not exists, otherwise return false
        """
        payload = self._request(
            "POST",
            "v2/queues/verify",
        json=self._json_payload(queue_verify_request),
        )
        return self._validate_payload(payload, TypeAdapter(bool))

    def update_queue(
        self,
        id: int,
        queue_update_request: QueueUpdateRequest
    ) -> Queue:
        """
        Update Queue
        
        Update queue
        
        DS operation: QueueV2Controller.updateQueue | PUT /v2/queues/{id}
        
        Args:
            id: queue id
            queue_update_request: Request body payload.
        
        Returns:
            update result code
        """
        path = f"v2/queues/{id}"
        payload = self._request(
            "PUT",
            path,
        json=self._json_payload(queue_update_request),
        )
        return self._validate_payload(payload, TypeAdapter(Queue))

__all__ = ["QueueV2Operations"]
