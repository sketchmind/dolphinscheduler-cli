"""Java source parsing and import-resolution helpers shared by codegen stages."""

from __future__ import annotations

import re
from functools import cache
from pathlib import Path
from typing import TypeAlias

import javalang

_METHOD_REFERENCE_PATTERN = re.compile(
    r"\b[A-Za-z_$][A-Za-z0-9_$.<>\[\], ?]*::[A-Za-z_$][A-Za-z0-9_$.]*\b"
)

BUILTIN_REFERENCE_TYPES = {
    "BigDecimal",
    "BigInteger",
    "Boolean",
    "Byte",
    "Character",
    "Class",
    "Date",
    "Double",
    "Float",
    "Integer",
    "List",
    "LocalDate",
    "LocalDateTime",
    "LocalTime",
    "Long",
    "Map",
    "Object",
    "ResponseEntity",
    "Set",
    "Short",
    "String",
    "T",
    "Void",
}
LoadedTypeDeclaration: TypeAlias = tuple[
    javalang.tree.CompilationUnit,
    javalang.tree.TypeDeclaration,
    dict[str, str],
    str | None,
]
JavaParseCache: TypeAlias = dict[str, LoadedTypeDeclaration | None]


def parse_java_compilation_unit(source: str) -> javalang.tree.CompilationUnit:
    try:
        return javalang.parse.parse(source)
    except javalang.parser.JavaSyntaxError:
        # javalang cannot parse Java method references, but replacing them with
        # null is sufficient for the structural extraction we do here.
        if "::" not in source:
            raise
        normalized_source = _METHOD_REFERENCE_PATTERN.sub("null", source)
        return javalang.parse.parse(normalized_source)


def try_parse_java_compilation_unit(
    source: str,
) -> javalang.tree.CompilationUnit | None:
    try:
        return parse_java_compilation_unit(source)
    except (
        AttributeError,
        IndexError,
        TypeError,
        javalang.parser.JavaSyntaxError,
        javalang.tokenizer.LexerError,
    ):
        return None


def build_import_map(
    compilation_unit: javalang.tree.CompilationUnit,
) -> dict[str, str]:
    import_map: dict[str, str] = {}
    for java_import in compilation_unit.imports:
        if java_import.wildcard:
            continue
        if java_import.static:
            import_map[f"@static:{java_import.path.rsplit('.', 1)[-1]}"] = (
                java_import.path
            )
            continue
        import_map[java_import.path.rsplit(".", 1)[-1]] = java_import.path
    return import_map


@cache
def load_primary_type_declaration_from_path(
    source_path: Path,
) -> LoadedTypeDeclaration | None:
    source = source_path.read_text()
    compilation_unit = parse_java_compilation_unit(source)
    if not compilation_unit.types:
        return None
    return (
        compilation_unit,
        compilation_unit.types[0],
        build_import_map(compilation_unit),
        compilation_unit.package.name if compilation_unit.package else None,
    )


@cache
def resolve_import_path(repo_root: Path, import_path: str) -> Path | None:
    exact_match = _resolve_exact_import_path(repo_root, import_path)
    if exact_match is not None:
        return exact_match
    return _resolve_import_path_by_simple_name(
        repo_root,
        import_path.rsplit(".", 1)[-1],
    )


@cache
def _resolve_exact_import_path(repo_root: Path, import_path: str) -> Path | None:
    import_suffix = Path(*import_path.split(".")).with_suffix(".java")
    search_root = repo_root / "references/dolphinscheduler"
    for candidate in search_root.rglob(import_suffix.name):
        try:
            candidate.relative_to(search_root)
        except ValueError:
            continue
        if tuple(candidate.parts[-len(import_suffix.parts) :]) == import_suffix.parts:
            return candidate
    return None


@cache
def _resolve_import_path_by_simple_name(
    repo_root: Path,
    simple_name: str,
) -> Path | None:
    search_root = repo_root / "references/dolphinscheduler"
    matches = sorted(search_root.rglob(f"{simple_name}.java"))
    if len(matches) != 1:
        return None
    return matches[0]


def load_type_declaration(
    repo_root: Path,
    import_path: str,
    parse_cache: JavaParseCache,
) -> LoadedTypeDeclaration | None:
    if import_path in parse_cache:
        return parse_cache[import_path]

    resolved = resolve_import_path_with_nested(repo_root, import_path)
    if resolved is None:
        parse_cache[import_path] = None
        return None
    declaration_file, nested_names = resolved
    source = declaration_file.read_text()
    compilation_unit = parse_java_compilation_unit(source)
    type_declaration = compilation_unit.types[0]
    nested_declaration = find_nested_type_declaration(type_declaration, nested_names)
    if nested_declaration is None:
        parse_cache[import_path] = None
        return None

    parse_cache[import_path] = (
        compilation_unit,
        nested_declaration,
        build_import_map(compilation_unit),
        compilation_unit.package.name if compilation_unit.package else None,
    )
    return parse_cache[import_path]


def resolve_import_path_with_nested(
    repo_root: Path,
    import_path: str,
) -> tuple[Path, list[str]] | None:
    parts = import_path.split(".")
    for split_index in range(len(parts), 0, -1):
        candidate_import_path = ".".join(parts[:split_index])
        candidate_file = resolve_import_path(repo_root, candidate_import_path)
        if candidate_file is None:
            continue
        return candidate_file, parts[split_index:]
    return None


def _resolve_exact_import_path_with_nested(
    repo_root: Path,
    import_path: str,
) -> tuple[Path, list[str]] | None:
    parts = import_path.split(".")
    for split_index in range(len(parts), 0, -1):
        candidate_import_path = ".".join(parts[:split_index])
        candidate_file = _resolve_exact_import_path(repo_root, candidate_import_path)
        if candidate_file is None:
            continue
        return candidate_file, parts[split_index:]
    return None


def find_nested_type_declaration(
    type_declaration: javalang.tree.TypeDeclaration,
    nested_names: list[str],
) -> javalang.tree.TypeDeclaration | None:
    current_declaration = type_declaration
    for nested_name in nested_names:
        next_declaration: javalang.tree.TypeDeclaration | None = None
        body = getattr(current_declaration, "body", None) or []
        for body_declaration in body:
            if not isinstance(
                body_declaration,
                (
                    javalang.tree.ClassDeclaration,
                    javalang.tree.EnumDeclaration,
                    javalang.tree.InterfaceDeclaration,
                ),
            ):
                continue
            if body_declaration.name == nested_name:
                next_declaration = body_declaration
                break
        if next_declaration is None:
            return None
        current_declaration = next_declaration
    return current_declaration


def logical_type_name(import_path: str) -> str:
    parts = import_path.split(".")
    class_parts = [part for part in parts if part and part[0].isupper()]
    return ".".join(class_parts) if class_parts else parts[-1]


def resolve_referenced_import_path(
    repo_root: Path,
    type_name: str,
    import_map: dict[str, str],
    package_name: str | None,
    owner_import_path: str | None = None,
    nested_type_names: set[str] | None = None,
) -> str | None:
    if type_name in BUILTIN_REFERENCE_TYPES:
        return None
    if (
        nested_type_names
        and owner_import_path is not None
        and type_name in nested_type_names
    ):
        return f"{owner_import_path}.{type_name}"
    if type_name in import_map:
        return import_map[type_name]
    if "." in type_name:
        outer_name, _, nested_suffix = type_name.partition(".")
        outer_import_path = import_map.get(outer_name)
        if outer_import_path is not None:
            return f"{outer_import_path}.{nested_suffix}"
    if package_name is not None:
        package_import_path = f"{package_name}.{type_name}"
        if (
            _resolve_exact_import_path_with_nested(repo_root, package_import_path)
            is not None
        ):
            return package_import_path
    return resolve_global_import_path(repo_root, type_name)


@cache
def resolve_global_import_path(
    repo_root: Path,
    type_name: str,
) -> str | None:
    return dict(_build_global_type_index(repo_root)).get(type_name)


@cache
def _build_global_type_index(repo_root: Path) -> tuple[tuple[str, str], ...]:
    type_paths_by_name: dict[str, set[str]] = {}
    references_root = repo_root / "references/dolphinscheduler"
    for java_path in sorted(references_root.rglob("*.java")):
        compilation_unit = try_parse_java_compilation_unit(java_path.read_text())
        if compilation_unit is None:
            continue
        if not compilation_unit.types or compilation_unit.package is None:
            continue
        top_level_type = compilation_unit.types[0]
        top_level_import_path = f"{compilation_unit.package.name}.{top_level_type.name}"
        _index_type_declaration(
            type_paths_by_name,
            top_level_type,
            top_level_import_path,
        )
    type_index = {
        logical_name: next(iter(import_paths))
        for logical_name, import_paths in type_paths_by_name.items()
        if len(import_paths) == 1
    }
    return tuple(sorted(type_index.items()))


def _index_type_declaration(
    type_paths_by_name: dict[str, set[str]],
    type_declaration: javalang.tree.TypeDeclaration,
    import_path: str,
) -> None:
    resolved_name = logical_type_name(import_path)
    type_paths_by_name.setdefault(resolved_name, set()).add(import_path)
    for body_declaration in getattr(type_declaration, "body", None) or []:
        if not isinstance(
            body_declaration,
            (
                javalang.tree.ClassDeclaration,
                javalang.tree.EnumDeclaration,
                javalang.tree.InterfaceDeclaration,
            ),
        ):
            continue
        _index_type_declaration(
            type_paths_by_name,
            body_declaration,
            f"{import_path}.{body_declaration.name}",
        )
