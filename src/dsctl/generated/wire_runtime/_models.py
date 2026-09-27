from __future__ import annotations

from typing import TypeAlias

from pydantic import BaseModel, ConfigDict, JsonValue

JsonObject: TypeAlias = dict[str, JsonValue]

class BaseContractModel(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
        extra="ignore",
    )

class BaseViewModel(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
        extra="ignore",
    )

class BaseEntityModel(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
        extra="ignore",
    )

__all__ = [
    "BaseContractModel",
    "BaseEntityModel",
    "BaseViewModel",
    "JsonObject",
    "JsonValue",
]
