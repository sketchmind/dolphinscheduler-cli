from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseContractModel

class Server(BaseContractModel):
    id: int = Field(default=0)
    host: str | None = Field(default=None)
    port: int = Field(default=0)
    zkDirectory: str | None = Field(default=None)
    resInfo: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    lastHeartbeatTime: str | None = Field(default=None)

__all__ = ["Server"]

Server.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = list[Server]
EXECUTABLE_SCHEMA_DIGEST = 'sha256:9ff0a22d034d0466db3cb8badb5a90c73e8acb02134c3e3695ef98254e71d7e4'
