"""Atomic executable-schema entries for generated wire registries."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeAlias

if TYPE_CHECKING:
    from pydantic import BaseModel

OpaqueSchemaAnnotation: TypeAlias = object

@dataclass(frozen=True)
class CompiledRequestSchema:
    """One generated request model and its compiler-owned identity."""

    model: type[BaseModel]
    digest: str

@dataclass(frozen=True)
class CompiledResponseSchema:
    """One generated response type and its compiler-owned identity."""

    annotation: OpaqueSchemaAnnotation
    digest: str

__all__ = [
    "CompiledRequestSchema",
    "CompiledResponseSchema",
]
