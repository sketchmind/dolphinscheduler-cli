from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

ProcedureMethodSyntax = Literal["bare-name", "jdbc-positional", "jdbc-named"]


@dataclass(frozen=True, slots=True)
class ProcedureAuthoringSurface:
    """Exact procedure-call syntax and derived runtime fields."""

    method_syntax: ProcedureMethodSyntax
    runtime_only_fields: tuple[str, ...]


_PROCEDURE_BARE_NAME = ProcedureAuthoringSurface(
    method_syntax="bare-name",
    runtime_only_fields=(),
)
_PROCEDURE_JDBC_POSITIONAL = ProcedureAuthoringSurface(
    method_syntax="jdbc-positional",
    runtime_only_fields=(),
)
_PROCEDURE_JDBC_NAMED = ProcedureAuthoringSurface(
    method_syntax="jdbc-named",
    runtime_only_fields=(),
)
_PROCEDURE_JDBC_NAMED_WITH_RUNTIME_OUTPUT = ProcedureAuthoringSurface(
    method_syntax="jdbc-named",
    runtime_only_fields=("outProperty",),
)
_PROCEDURE_RUNTIME_OUTPUT_VERSIONS = frozenset(
    {
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
        "3.2.0",
        "3.2.1",
        "3.2.2",
        "3.3.1",
        "3.3.2",
        "3.4.0",
    }
)


def _procedure_surface(version: str) -> ProcedureAuthoringSurface:
    if version == "1.3.9":
        return _PROCEDURE_BARE_NAME
    if version in {"2.0.0", "2.0.1"}:
        return _PROCEDURE_JDBC_POSITIONAL
    if version in _PROCEDURE_RUNTIME_OUTPUT_VERSIONS:
        return _PROCEDURE_JDBC_NAMED_WITH_RUNTIME_OUTPUT
    if version in {"3.4.1", "3.4.2", "3.4.3"}:
        return _PROCEDURE_JDBC_NAMED
    message = f"No exact PROCEDURE authoring surface for DolphinScheduler {version}"
    raise ValueError(message)
