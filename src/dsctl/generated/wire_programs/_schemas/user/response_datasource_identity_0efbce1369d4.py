from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseViewModel

class DataSourceSimpleInfoVO(BaseViewModel):
    id: int | None = Field(default=None)
    name: str | None = Field(default=None)

__all__ = ["DataSourceSimpleInfoVO"]

DataSourceSimpleInfoVO.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = list[DataSourceSimpleInfoVO]
EXECUTABLE_SCHEMA_DIGEST = 'sha256:0efbce1369d482fd936d2ccd76580a06ecf462c9a508209e6c17d05bfbee5536'
