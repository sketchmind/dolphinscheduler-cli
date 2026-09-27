from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseContractModel

class WorkerServerModel(BaseContractModel):
    id: int = Field(default=0)
    host: str | None = Field(default=None)
    port: int = Field(default=0)
    zkDirectories: list[str] | None = Field(default=None)
    resInfo: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    lastHeartbeatTime: str | None = Field(default=None)

__all__ = ["WorkerServerModel"]

WorkerServerModel.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = list[WorkerServerModel]
EXECUTABLE_SCHEMA_DIGEST = 'sha256:5dfba947ecd8ea9777c39f8fff73f3a2300dafc3ad4858847508dbe469f5cd07'
