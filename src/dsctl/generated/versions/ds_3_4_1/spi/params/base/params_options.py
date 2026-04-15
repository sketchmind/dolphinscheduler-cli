from __future__ import annotations

from pydantic import Field
from ...._models import BaseContractModel

class ParamsOptions(BaseContractModel):
    """The options field in form-create`s json rule Set radio, select, checkbox and other component option options"""
    label: str | None = Field(default=None)
    value: object | None = Field(default=None)
    disabled: bool = Field(default=False, description='is can be select')

__all__ = ["ParamsOptions"]
