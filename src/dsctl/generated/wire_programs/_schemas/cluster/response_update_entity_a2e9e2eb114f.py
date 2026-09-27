from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

class Cluster(BaseEntityModel):
    id: int | None = Field(default=None)
    code: int | None = Field(default=None)
    name: str | None = Field(default=None)
    config: str | None = Field(default=None)
    description: str | None = Field(default=None)
    operator: int | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)

__all__ = ["Cluster"]

Cluster.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = Cluster
EXECUTABLE_SCHEMA_DIGEST = 'sha256:a2e9e2eb114f87350cb10cc99d7958e2439c0fb82856f2ed102d385fad55f405'
