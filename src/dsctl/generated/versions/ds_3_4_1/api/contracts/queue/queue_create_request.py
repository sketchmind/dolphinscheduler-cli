from __future__ import annotations

from pydantic import Field
from ...._models import BaseContractModel

class QueueCreateRequest(BaseContractModel):
    """Queue create request"""
    queue: str = Field(examples=['queue11'])
    queueName: str = Field(examples=['test_queue11'])

__all__ = ["QueueCreateRequest"]
