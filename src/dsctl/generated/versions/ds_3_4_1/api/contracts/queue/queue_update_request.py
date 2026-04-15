from __future__ import annotations

from pydantic import Field
from ...._models import BaseContractModel

class QueueUpdateRequest(BaseContractModel):
    """Queue update request"""
    queue: str = Field(examples=['queue11'])
    queueName: str = Field(examples=['test_queue11'])

__all__ = ["QueueUpdateRequest"]
