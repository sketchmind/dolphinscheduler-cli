"""Exact project programs shared by lifecycle and definition reads."""

from dataclasses import replace
from typing import Literal

from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.upstream.compiled_domain import (
    MUTATION_ONCE_REQUIRED,
    READ_RETRY_OPTIONAL,
    CompiledDomainPrograms,
)

ProjectPrimitive = Literal[
    "page", "get", "create", "created_and_authed", "update", "delete", "delete_legacy"
]

PROJECT_PROGRAMS = CompiledDomainPrograms[ProjectPrimitive](
    name="project",
    schema_constant="COMPILED_PROJECT_SCHEMA_VERSION",
    schema_version=1,
    expectations={
        "page": READ_RETRY_OPTIONAL,
        "get": READ_RETRY_OPTIONAL,
        "create": MUTATION_ONCE_REQUIRED,
        "created_and_authed": replace(
            READ_RETRY_OPTIONAL,
            absent_versions=frozenset(TARGET_DS_VERSIONS) - {"2.0.0", "2.0.1"},
        ),
        "update": MUTATION_ONCE_REQUIRED,
        "delete": replace(MUTATION_ONCE_REQUIRED, absent_versions=frozenset({"1.3.9"})),
        "delete_legacy": replace(
            # 1.3.9 ProjectController uses GET for a database deletion.
            MUTATION_ONCE_REQUIRED,
            envelope=READ_RETRY_OPTIONAL.envelope,
            absent_versions=frozenset(TARGET_DS_VERSIONS) - {"1.3.9"},
        ),
    },
)
