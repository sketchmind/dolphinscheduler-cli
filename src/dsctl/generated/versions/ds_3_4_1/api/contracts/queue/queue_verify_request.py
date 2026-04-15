from __future__ import annotations

from pydantic import Field
from ...._models import BaseContractModel

class QueueVerifyRequest(BaseContractModel):
    """Queue verify request"""
    queue: str = Field(examples=['queue11'])
    queueName: str = Field(examples=['queue11'])

__all__ = ["QueueVerifyRequest"]
