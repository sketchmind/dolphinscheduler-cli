"""Pure reference traversal for Java-shaped type expressions in contract IR."""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

CANONICAL_BUILTIN_REFERENCE_ALIASES = {
    "com.fasterxml.jackson.databind.node.ArrayNode": "ArrayNode",
    "com.fasterxml.jackson.databind.node.ObjectNode": "ObjectNode",
    "jakarta.servlet.ServletRequest": "ServletRequest",
    "jakarta.servlet.ServletResponse": "ServletResponse",
    "jakarta.servlet.http.HttpServletRequest": "HttpServletRequest",
    "jakarta.servlet.http.HttpServletResponse": "HttpServletResponse",
    "jakarta.servlet.http.HttpSession": "HttpSession",
    "java.lang.Boolean": "Boolean",
    "java.lang.Byte": "Byte",
    "java.lang.Character": "Character",
    "java.lang.Class": "Class",
    "java.lang.Double": "Double",
    "java.lang.Float": "Float",
    "java.lang.Integer": "Integer",
    "java.lang.Long": "Long",
    "java.lang.Object": "Object",
    "java.lang.Short": "Short",
    "java.lang.String": "String",
    "java.lang.Void": "Void",
    "java.math.BigDecimal": "BigDecimal",
    "java.math.BigInteger": "BigInteger",
    "java.time.Instant": "Instant",
    "java.time.LocalDate": "LocalDate",
    "java.time.LocalDateTime": "LocalDateTime",
    "java.time.LocalTime": "LocalTime",
    "java.time.OffsetDateTime": "OffsetDateTime",
    "java.time.OffsetTime": "OffsetTime",
    "java.time.ZonedDateTime": "ZonedDateTime",
    "java.util.ArrayList": "ArrayList",
    "java.util.Collection": "Collection",
    "java.util.Date": "Date",
    "java.util.HashMap": "HashMap",
    "java.util.LinkedHashMap": "LinkedHashMap",
    "java.util.LinkedList": "LinkedList",
    "java.util.List": "List",
    "java.util.Map": "Map",
    "java.util.Optional": "Optional",
    "java.util.Set": "Set",
    "java.util.stream.Stream": "Stream",
    "javax.servlet.ServletRequest": "ServletRequest",
    "javax.servlet.ServletResponse": "ServletResponse",
    "javax.servlet.http.HttpServletRequest": "HttpServletRequest",
    "javax.servlet.http.HttpServletResponse": "HttpServletResponse",
    "javax.servlet.http.HttpSession": "HttpSession",
    "org.springframework.http.ResponseEntity": "ResponseEntity",
    "org.springframework.web.multipart.MultipartFile": "MultipartFile",
}

BUILTIN_REFERENCE_TYPES = frozenset(
    {
        *CANONICAL_BUILTIN_REFERENCE_ALIASES.values(),
        "Any",
        "BigDecimal",
        "BigInteger",
        "Boolean",
        "Byte",
        "Character",
        "Class",
        "Collection",
        "Date",
        "Double",
        "Float",
        "HashMap",
        "HttpServletRequest",
        "HttpServletResponse",
        "HttpSession",
        "Integer",
        "JsonObject",
        "JsonValue",
        "List",
        "LinkedHashMap",
        "LinkedList",
        "LocalDate",
        "LocalDateTime",
        "LocalTime",
        "Long",
        "Map",
        "MultipartFile",
        "Object",
        "Optional",
        "ResponseEntity",
        "ServletRequest",
        "ServletResponse",
        "Set",
        "Short",
        "Stream",
        "String",
        "T",
        "Void",
        "boolean",
        "byte",
        "double",
        "float",
        "int",
        "long",
        "short",
        "void",
    }
)
_REFERENCE_NAME = re.compile(r"[A-Za-z_$][A-Za-z0-9_$.]*\Z")


class UnsupportedContractTypeExpressionError(ValueError):
    """Raised when normalized IR uses a type grammar this compiler cannot prove."""


@dataclass(frozen=True)
class ContractTypeExpression:
    """One validated, canonical type expression from contract IR."""

    name: str
    arguments: tuple[ContractTypeExpression, ...] = ()
    is_array: bool = False

    def render(self) -> str:
        """Return the stable string form used by snapshots and generated packages."""

        rendered = self.name
        if self.arguments:
            rendered_arguments = ", ".join(
                argument.render() for argument in self.arguments
            )
            rendered = f"{rendered}<{rendered_arguments}>"
        return f"{rendered}[]" if self.is_array else rendered


def build_contract_type_candidate_catalog(
    types: Iterable[tuple[str, str]],
) -> dict[str, frozenset[str]]:
    """Index every exact type identity by qualified and source-visible aliases."""

    candidates_by_name: dict[str, set[str]] = defaultdict(set)
    for name, import_path in types:
        aliases = {
            name,
            name.rsplit(".", 1)[-1],
            import_path,
            import_path.rsplit(".", 1)[-1],
        }
        for alias in aliases:
            candidates_by_name[alias].add(import_path)
    return {
        name: frozenset(import_paths)
        for name, import_paths in candidates_by_name.items()
    }


def collect_type_reference_names(java_type: str) -> set[str]:
    """Return non-builtin identities named by one normalized IR type string."""

    expression = parse_contract_type_expression(java_type)
    return {
        node.name
        for node in _iter_type_expression_nodes(expression)
        if node.name not in BUILTIN_REFERENCE_TYPES
    }


def parse_contract_type_expression(java_type: str) -> ContractTypeExpression:
    """Parse and canonicalize the narrow type grammar persisted in contract IR."""

    return _canonicalize_type_expression(_parse_type_expression(java_type))


def canonicalize_builtin_type_expression(java_type: str) -> str:
    """Collapse proven platform FQNs to the compiler's canonical type grammar."""

    return parse_contract_type_expression(java_type).render()


def generic_base_type(java_type: str) -> str:
    """Return the outer type name from one normalized Java type expression."""

    if "<" not in java_type or not java_type.endswith(">"):
        return java_type
    return java_type.split("<", 1)[0]


def generic_inner_types(java_type: str) -> list[str]:
    """Return the top-level arguments from one normalized generic type."""

    if "<" not in java_type or not java_type.endswith(">"):
        return []
    return _split_top_level_generic_types(java_type.split("<", 1)[1][:-1])


def is_fully_qualified_reference_name(reference_name: str) -> bool:
    """Return whether a normalized reference carries a package identity."""

    head, separator, _ = reference_name.partition(".")
    return bool(separator and head[:1].islower())


def replace_type_reference_names(
    java_type: str,
    resolver: Callable[[str], str | None],
) -> str:
    """Replace normalized reference tokens without changing container grammar."""

    replacements = {
        reference_name: resolved
        for reference_name in collect_type_reference_names(java_type)
        if (resolved := resolver(reference_name)) is not None
        and resolved != reference_name
    }
    replaced = java_type
    for reference_name, resolved in sorted(
        replacements.items(),
        key=lambda item: len(item[0]),
        reverse=True,
    ):
        replaced = re.sub(
            rf"(?<![A-Za-z0-9_$.]){re.escape(reference_name)}"
            r"(?![A-Za-z0-9_$.])",
            resolved,
            replaced,
        )
    return replaced


def substitute_type_parameters(
    java_type: str,
    substitutions: dict[str, str],
) -> str:
    """Apply type-variable substitutions while preserving container structure."""

    if java_type in substitutions:
        return substitutions[java_type]
    if java_type.endswith("[]"):
        return substitute_type_parameters(java_type[:-2], substitutions) + "[]"
    generic_args = generic_inner_types(java_type)
    if not generic_args:
        return substitutions.get(java_type, java_type)
    rendered_args = ", ".join(
        substitute_type_parameters(generic_arg, substitutions)
        for generic_arg in generic_args
    )
    return f"{generic_base_type(java_type)}<{rendered_args}>"


def _parse_type_expression(java_type: str) -> ContractTypeExpression:
    if (
        not java_type
        or java_type != java_type.strip()
        or "?" in java_type
        or ">." in java_type
    ):
        _raise_unsupported(java_type)
    scalar = java_type.removesuffix("[]")
    is_array = scalar != java_type
    generic_start = scalar.find("<")
    if generic_start == -1:
        if not _REFERENCE_NAME.fullmatch(scalar):
            _raise_unsupported(java_type)
        return ContractTypeExpression(scalar, is_array=is_array)
    if not scalar.endswith(">"):
        _raise_unsupported(java_type)
    base = scalar[:generic_start]
    if not _REFERENCE_NAME.fullmatch(base):
        _raise_unsupported(java_type)
    arguments = tuple(
        _parse_type_expression(argument)
        for argument in _split_top_level_generic_types(scalar[generic_start + 1 : -1])
    )
    return ContractTypeExpression(base, arguments, is_array)


def _canonicalize_type_expression(
    expression: ContractTypeExpression,
) -> ContractTypeExpression:
    return ContractTypeExpression(
        CANONICAL_BUILTIN_REFERENCE_ALIASES.get(expression.name, expression.name),
        tuple(_canonicalize_type_expression(item) for item in expression.arguments),
        expression.is_array,
    )


def _iter_type_expression_nodes(
    expression: ContractTypeExpression,
) -> Iterable[ContractTypeExpression]:
    yield expression
    for argument in expression.arguments:
        yield from _iter_type_expression_nodes(argument)


def _raise_unsupported(java_type: str) -> None:
    message = f"unsupported contract type expression: {java_type!r}"
    raise UnsupportedContractTypeExpressionError(message)


def _split_top_level_generic_types(value: str) -> list[str]:
    parts: list[str] = []
    start = 0
    depth = 0
    for index, char in enumerate(value):
        if char == "<":
            depth += 1
        elif char == ">":
            depth -= 1
            if depth < 0:
                _raise_unsupported(value)
        elif char == "," and depth == 0:
            part = value[start:index].strip()
            if not part:
                _raise_unsupported(value)
            parts.append(part)
            start = index + 1
    if depth != 0:
        _raise_unsupported(value)
    tail = value[start:].strip()
    if not tail:
        _raise_unsupported(value)
    parts.append(tail)
    return parts


__all__ = [
    "BUILTIN_REFERENCE_TYPES",
    "CANONICAL_BUILTIN_REFERENCE_ALIASES",
    "ContractTypeExpression",
    "UnsupportedContractTypeExpressionError",
    "build_contract_type_candidate_catalog",
    "canonicalize_builtin_type_expression",
    "collect_type_reference_names",
    "generic_base_type",
    "generic_inner_types",
    "is_fully_qualified_reference_name",
    "parse_contract_type_expression",
    "replace_type_reference_names",
    "substitute_type_parameters",
]
