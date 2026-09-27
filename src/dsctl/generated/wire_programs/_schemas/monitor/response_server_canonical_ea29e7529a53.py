from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseContractModel

class Server(BaseContractModel):
    id: int = Field(default=0)
    host: str | None = Field(default=None)
    port: int = Field(default=0)
    serverDirectory: str | None = Field(default=None)
    heartBeatInfo: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    lastHeartbeatTime: str | None = Field(default=None)

__all__ = ["Server"]

Server.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = list[Server]
EXECUTABLE_SCHEMA_DIGEST = 'sha256:ea29e7529a53f96fa711a4073b596a2189a2e40b21d2b3c2a90d1e7964b24219'
