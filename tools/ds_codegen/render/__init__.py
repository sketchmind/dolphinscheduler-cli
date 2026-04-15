from __future__ import annotations

from ds_codegen.render.adapter import write_upstream_adapter_package
from ds_codegen.render.registry import write_python_registry
from ds_codegen.render.requests_client import write_requests_client
from ds_codegen.render.requests_example import write_requests_example

__all__ = [
    "write_python_registry",
    "write_requests_client",
    "write_requests_example",
    "write_upstream_adapter_package",
]
