"""Share closed enum implementations without merging exact schema ownership."""

from __future__ import annotations

import ast
import hashlib
import symtable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ds_codegen.compiled_wire_artifacts import CompiledWireModule

if TYPE_CHECKING:
    from collections.abc import Iterable

_ENUM_BASES = frozenset({"Enum", "IntEnum", "StrEnum"})
_BUILTINS = frozenset(
    {
        "bool",
        "bytes",
        "classmethod",
        "float",
        "int",
        "object",
        "property",
        "staticmethod",
        "str",
        "TypeError",
        "ValueError",
    }
)
_HEADER = (
    "from __future__ import annotations\n\nfrom enum import Enum, IntEnum, StrEnum\n\n"
)


@dataclass(frozen=True)
class PooledSchemaSource:
    source: str
    modules: tuple[CompiledWireModule, ...]


def pool_schema_enums(
    source: str, *, module_parts: tuple[str, ...]
) -> PooledSchemaSource:
    """Replace only self-contained standard enums with content-addressed imports.

    The complete canonical class syntax supplies identity, including its name,
    base, ordered members and method bodies. A source owner remains responsible
    for its own schema and digest; sharing grants no cross-version support claim.
    """
    if module_parts[:1] != ("wire_programs",):
        message = "schema enum pooling requires a compiled wire-program module"
        raise ValueError(message)
    tree = ast.parse(source)
    if not any(
        isinstance(statement, ast.ImportFrom)
        and statement.module == "__future__"
        and any(alias.name == "annotations" for alias in statement.names)
        for statement in tree.body
    ):
        # The pool uses postponed annotations just like generated closures.
        # Do not alter annotation evaluation for a different source dialect.
        return PooledSchemaSource(source, ())
    module_scope = symtable.symtable(source, "<schema>", "exec")
    bound_globals = {
        symbol.get_name()
        for symbol in module_scope.get_symbols()
        if symbol.is_assigned() or symbol.is_imported()
    }
    assigned_globals = {
        symbol.get_name()
        for symbol in module_scope.get_symbols()
        if symbol.is_assigned()
    }
    enum_imports = {
        alias.asname or alias.name
        for statement in tree.body
        if isinstance(statement, ast.ImportFrom)
        and statement.level == 0
        and statement.module == "enum"
        for alias in statement.names
        if alias.name in _ENUM_BASES and alias.asname in {None, alias.name}
    }
    for statement in tree.body:
        if isinstance(statement, ast.Import):
            enum_imports.difference_update(
                alias.asname or alias.name.split(".")[0] for alias in statement.names
            )
        elif isinstance(statement, ast.ImportFrom) and (
            statement.module != "enum" or statement.level
        ):
            enum_imports.difference_update(
                alias.asname or alias.name for alias in statement.names
            )
    enum_imports.difference_update(assigned_globals)
    lines = source.splitlines(keepends=True)
    replacements: list[tuple[int, int, str]] = []
    modules: list[CompiledWireModule] = []
    for node in tree.body:
        if not isinstance(node, ast.ClassDef) or not _is_closed_enum(
            node, enum_imports=enum_imports, bound_globals=bound_globals
        ):
            continue
        # Unparse normalizes formatting while retaining the complete class AST's
        # semantics. In particular, aliases and member declaration order remain.
        content = _HEADER + ast.unparse(node) + f"\n\n__all__ = [{node.name!r}]\n"
        digest = hashlib.sha256(
            b"dsctl-schema-enum-v1\0" + content.encode()
        ).hexdigest()
        name = f"_schemas._enums.enum_{digest}"
        modules.append(CompiledWireModule(name=name, content=content, kind="support"))
        # Ascend from the root schema's package to wire_programs, then descend
        # into its implementation pool. Explicit aliases preserve static exports.
        relative = "." * (len(module_parts) - 1)
        replacement = f"from {relative}{name} import {node.name} as {node.name}\n"
        if node.end_lineno is None:
            message = f"compiled enum {node.name} has no source extent"
            raise ValueError(message)
        replacements.append((node.lineno - 1, node.end_lineno, replacement))
    for start, end, replacement in reversed(replacements):
        lines[start:end] = [replacement]
    return PooledSchemaSource("".join(lines), unique_schema_modules(modules))


def unique_schema_modules(
    modules: Iterable[CompiledWireModule],
) -> tuple[CompiledWireModule, ...]:
    """Merge identical physical modules and reject any content-address collision."""
    selected: dict[str, CompiledWireModule] = {}
    for module in modules:
        previous = selected.setdefault(module.name, module)
        if previous != module:
            message = f"compiled schema module content collision: {module.name}"
            raise ValueError(message)
    return tuple(selected.values())


def _is_closed_enum(
    node: ast.ClassDef, *, enum_imports: set[str], bound_globals: set[str]
) -> bool:
    if (
        len(node.bases) != 1
        or not isinstance(node.bases[0], ast.Name)
        or node.bases[0].id not in enum_imports
        or node.decorator_list
        or node.keywords
    ):
        return False
    allowed = _BUILTINS | enum_imports | {node.name}
    if (_BUILTINS & bound_globals) or any(
        isinstance(child, (ast.Import, ast.ImportFrom))
        or (
            isinstance(child, ast.Attribute)
            and child.attr in {"__module__", "__qualname__"}
        )
        for child in ast.walk(node)
    ):
        return False
    scope = symtable.symtable(_HEADER + ast.unparse(node), "<enum>", "exec")
    if _referenced_globals(scope) - allowed:
        return False
    # Future annotations do not appear in symbol tables. Check them separately,
    # including the renderer's quoted self return annotations.
    for child in ast.walk(node):
        annotation = (
            child.annotation
            if isinstance(child, (ast.AnnAssign, ast.arg))
            else child.returns
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef))
            else None
        )
        if annotation is None:
            continue
        if isinstance(annotation, ast.Constant) and isinstance(annotation.value, str):
            try:
                annotation = ast.parse(annotation.value, mode="eval").body
            except SyntaxError:
                return False
        if any(
            isinstance(name, ast.Name) and name.id not in allowed
            for name in ast.walk(annotation)
        ):
            return False
    return True


def _referenced_globals(scope: symtable.SymbolTable) -> set[str]:
    names = {
        symbol.get_name()
        for symbol in scope.get_symbols()
        if symbol.is_global() and symbol.is_referenced()
    }
    for child in scope.get_children():
        names.update(_referenced_globals(child))
    return names
