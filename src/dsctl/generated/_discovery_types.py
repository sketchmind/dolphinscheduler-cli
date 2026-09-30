"""Generated API-document discovery facts; do not edit by hand."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DocumentProbe:
    """Public API metadata; matching a document never proves a release."""
    source: str
    path: str
    exact_versions: tuple[str, ...]
    document_group: str | None


@dataclass(frozen=True)
class ApiParameterContract:
    name: str
    location: str
    schema_type: str | None
    required: bool
    document_required: tuple[bool, ...]
    enum_values: tuple[str, ...]
    default_value: str | None
    item_schema_type: str | None
    document_schema_types: tuple[str, ...]


@dataclass(frozen=True)
class ApiOperationContract:
    operation_id: str
    document_group: str | None
    method: str
    path: str
    parameters: tuple[ApiParameterContract, ...]
    ignored_document_parameters: tuple[str, ...]


@dataclass(frozen=True)
class ApiContractProfile:
    document_paths: tuple[str, ...]
    operations: tuple[str, ...]

