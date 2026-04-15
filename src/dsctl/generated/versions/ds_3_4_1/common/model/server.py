from __future__ import annotations

from pydantic import Field
from ..._models import BaseContractModel

class Server(BaseContractModel):
    id: int = Field(default=0)
    host: str | None = Field(default=None)
    port: int = Field(default=0)
    serverDirectory: str | None = Field(default=None)
    heartBeatInfo: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    lastHeartbeatTime: str | None = Field(default=None)

__all__ = ["Server"]
