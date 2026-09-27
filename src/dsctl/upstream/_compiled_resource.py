"""Exact resource programs with distinct JSON, upload, and binary transports."""

from dataclasses import replace
from typing import Literal

from dsctl.upstream.compiled_domain import (
    MUTATION_ONCE_REQUIRED,
    READ_RETRY_OPTIONAL,
    CompiledDomainPrograms,
)

ResourcePrimitive = Literal[
    "page",
    "lookup",
    "base_dir",
    "create",
    "mkdir",
    "delete",
    "upload",
    "download",
    "view_native",
]

RESOURCE_PROGRAMS = CompiledDomainPrograms[ResourcePrimitive](
    name="resource",
    schema_constant="COMPILED_RESOURCE_SCHEMA_VERSION",
    schema_version=1,
    expectations={
        "page": READ_RETRY_OPTIONAL,
        "lookup": replace(
            READ_RETRY_OPTIONAL,
            absent_versions=frozenset(
                {
                    "3.2.0",
                    "3.2.1",
                    "3.2.2",
                    "3.3.1",
                    "3.3.2",
                    "3.4.0",
                    "3.4.1",
                    "3.4.2",
                    "3.4.3",
                }
            ),
        ),
        "base_dir": replace(
            READ_RETRY_OPTIONAL,
            absent_versions=frozenset(
                {
                    "1.3.9",
                    "2.0.0",
                    "2.0.1",
                    "2.0.2",
                    "2.0.3",
                    "2.0.4",
                    "2.0.5",
                    "2.0.6",
                    "2.0.7",
                    "2.0.8",
                    "2.0.9",
                    "3.0.0",
                    "3.0.1",
                    "3.0.2",
                    "3.0.3",
                    "3.0.4",
                    "3.0.5",
                    "3.0.6",
                    "3.1.0",
                    "3.1.1",
                    "3.1.2",
                    "3.1.3",
                    "3.1.4",
                    "3.1.5",
                    "3.1.6",
                    "3.1.7",
                    "3.1.8",
                    "3.1.9",
                }
            ),
        ),
        "create": MUTATION_ONCE_REQUIRED,
        "mkdir": MUTATION_ONCE_REQUIRED,
        "delete": MUTATION_ONCE_REQUIRED,
        "upload": replace(MUTATION_ONCE_REQUIRED, file_fields=("file",)),
        "download": replace(READ_RETRY_OPTIONAL, response_transport="binary"),
        "view_native": READ_RETRY_OPTIONAL,
    },
)
