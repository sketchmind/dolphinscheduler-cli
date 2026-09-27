"""Canonical datasource references accepted by reviewed task authoring."""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, TypeAlias

from pydantic import BeforeValidator, Field
from pydantic_core import PydanticCustomError

if TYPE_CHECKING:
    from dsctl.models.common import YamlValue

_DATASOURCE_REFERENCE_TYPE_ERROR = "datasource_reference_type"


def _validate_datasource_reference(value: YamlValue) -> int | str:
    """Keep YAML scalar type meaningful: integers are ids and strings are names."""
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        message = "datasource must be one positive integer id or nonblank exact name"
        raise PydanticCustomError(_DATASOURCE_REFERENCE_TYPE_ERROR, message)
    if isinstance(value, int):
        if value <= 0:
            message = "datasource id must be a positive integer"
            raise ValueError(message)
        return value
    if not value.strip():
        message = "datasource name must not be blank"
        raise ValueError(message)
    return value


_PositiveDatasourceId: TypeAlias = Annotated[int, Field(strict=True, ge=1)]
_ExactDatasourceName: TypeAlias = Annotated[
    str,
    Field(strict=True, pattern=r"\S"),
]

DatasourceReference: TypeAlias = Annotated[
    _PositiveDatasourceId | _ExactDatasourceName,
    BeforeValidator(_validate_datasource_reference),
]


__all__ = ["DatasourceReference"]
