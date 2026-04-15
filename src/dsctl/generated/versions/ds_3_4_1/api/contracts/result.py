from __future__ import annotations

from typing import Generic, TypeVar
from pydantic import Field
from ..._models import BaseContractModel

T = TypeVar("T")

class Result(BaseContractModel, Generic[T]):
    """Result"""
    code: int | None = Field(default=None, description='status')
    msg: str | None = Field(default=None, description='message')
    data: T | None = Field(default=None, description='data')

__all__ = ["Result"]
