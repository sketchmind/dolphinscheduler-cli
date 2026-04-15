from __future__ import annotations

from pydantic import Field
from ...._models import BaseContractModel

from ..enums.data_type import DataType
from ..enums.direct import Direct

class Property(BaseContractModel):
    prop: str | None = Field(default=None, description='key')
    direct: Direct | None = Field(default=None, description='input/output')
    type: DataType | None = Field(default=None, description='data type')
    value: str | None = Field(default=None, description='value')

__all__ = ["Property"]
