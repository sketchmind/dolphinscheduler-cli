from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseViewModel

class ResourcesServiceImplReadResourceMap(BaseViewModel):
    """AST-inferred view from generated.view.ResourcesServiceImpl_readResource_map."""
    alias: str | None = Field(default=None)
    content: str | None = Field(default=None)

__all__ = ["ResourcesServiceImplReadResourceMap"]

ResourcesServiceImplReadResourceMap.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = ResourcesServiceImplReadResourceMap
EXECUTABLE_SCHEMA_DIGEST = 'sha256:4193f50000439c44ee1040523335a2148b748fc59db8983e017216cff4e18bef'
