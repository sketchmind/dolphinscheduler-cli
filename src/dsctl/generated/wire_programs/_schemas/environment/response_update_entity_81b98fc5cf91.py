from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

class Environment(BaseEntityModel):
    id: int | None = Field(default=None)
    code: int | None = Field(default=None)
    name: str | None = Field(default=None)
    config: str | None = Field(default=None)
    description: str | None = Field(default=None)
    operator: int | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)

__all__ = ["Environment"]

Environment.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = Environment
EXECUTABLE_SCHEMA_DIGEST = 'sha256:81b98fc5cf9155163ff882605086b419a73a616b538b7e27c3e20ff261ec05e9'
