"""Java source parsing and import-resolution helpers shared by codegen stages."""

from __future__ import annotations

import re
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from functools import cache
from types import MappingProxyType
from typing import TYPE_CHECKING, TypeAlias

import javalang

from ds_codegen.contract_type_refs import (
    BUILTIN_REFERENCE_TYPES,
    collect_type_reference_names,
    is_fully_qualified_reference_name,
    replace_type_reference_names,
)

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping
    from pathlib import Path

_METHOD_REFERENCE_PATTERN = re.compile(
    r"\b[A-Za-z_$][A-Za-z0-9_$.<>\[\], ?]*::[A-Za-z_$][A-Za-z0-9_$.]*\b"
)

LoadedTypeDeclaration: TypeAlias = tuple[
    javalang.tree.CompilationUnit,
    javalang.tree.TypeDeclaration,
    dict[str, str],
    str | None,
]
JavaParseCache: TypeAlias = dict[str, LoadedTypeDeclaration | None]
_COMPILATION_UNITS: ContextVar[dict[str, javalang.tree.CompilationUnit] | None] = (
    ContextVar("java_compilation_units", default=None)
)


@dataclass(frozen=True)
class JavaTypeLocation:
    """Exact source file and declaration chain for one Java type identity."""

    source_path: Path
    declaration_names: tuple[str, ...]


@dataclass(frozen=True)
class SourceResolutionScope:
    """Complete Java lookup scope for one source-declared use-site."""

    import_map: dict[str, str]
    package_name: str | None
    owner_import_path: str | None = None
    nested_type_names: frozenset[str] = frozenset()


class AmbiguousJavaTypeReferenceError(ValueError):
    """Raised when Java scope rules expose multiple equally valid types."""


@contextmanager
def java_source_cache_scope() -> Iterator[None]:
    """Share read-only syntax trees during one extraction, then release them."""
    token = _COMPILATION_UNITS.set({})
    clear_java_source_caches()
    try:
        yield
    finally:
        clear_java_source_caches()
        _COMPILATION_UNITS.reset(token)


def clear_java_source_caches() -> None:
    """Release source-root-scoped caches after one isolated extraction."""
    load_primary_type_declaration_from_path.cache_clear()
    resolve_import_path.cache_clear()
    _resolve_exact_type_location.cache_clear()
    _exact_declared_type_exists.cache_clear()
    _java_source_paths.cache_clear()
    _java_source_paths_by_filename.cache_clear()
    _java_source_paths_by_package.cache_clear()
    _java_source_paths_in_package.cache_clear()
    _declared_type_locations_from_path.cache_clear()
    compilation_units = _COMPILATION_UNITS.get()
    if compilation_units is not None:
        compilation_units.clear()


def parse_java_compilation_unit(source: str) -> javalang.tree.CompilationUnit:
    compilation_units = _COMPILATION_UNITS.get()
    if compilation_units is None:
        return _parse_java_compilation_unit(source)
    if source not in compilation_units:
        compilation_units[source] = _parse_java_compilation_unit(source)
    return compilation_units[source]


def _parse_java_compilation_unit(source: str) -> javalang.tree.CompilationUnit:
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
            if not java_import.static:
                import_map[f"@wildcard:{java_import.path}"] = java_import.path
            continue
        if java_import.static:
            import_map[f"@static:{java_import.path.rsplit('.', 1)[-1]}"] = (
                java_import.path
            )
            continue
        import_map[java_import.path.rsplit(".", 1)[-1]] = java_import.path
    if compilation_unit.package is not None:
        package_name = compilation_unit.package.name
        for type_declaration in compilation_unit.types:
            import_path = f"{package_name}.{type_declaration.name}"
            import_map[f"@same_unit:{type_declaration.name}"] = import_path
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
    location = _resolve_exact_type_location(repo_root, import_path)
    return location.source_path if location is not None else None


@cache
def _resolve_exact_type_location(
    repo_root: Path,
    import_path: str,
) -> JavaTypeLocation | None:
    candidate_paths: set[Path] = set()
    parts = import_path.split(".")
    paths_by_filename = _java_source_paths_by_filename(repo_root)
    for declaration_index in range(len(parts) - 1, -1, -1):
        filename = f"{parts[declaration_index]}.java"
        expected_suffix = "/".join((*parts[:declaration_index], filename))
        candidate_paths.update(
            path
            for path in paths_by_filename.get(filename, ())
            if path.as_posix().endswith(f"/{expected_suffix}")
        )

    class_index = next(
        (index for index, part in enumerate(parts) if part and part[0].isupper()),
        len(parts) - 1,
    )
    package_name = ".".join(parts[:class_index])
    if package_name:
        candidate_paths.update(_java_source_paths_in_package(repo_root, package_name))

    locations = tuple(
        location
        for source_path in sorted(candidate_paths)
        for declared_import_path, location in _declared_type_locations_from_path(
            source_path
        )
        if declared_import_path == import_path
    )
    if len(locations) > 1:
        message = (
            f"Java type {import_path!r} has multiple exact source declarations: "
            f"{[str(location.source_path) for location in locations]!r}"
        )
        raise AmbiguousJavaTypeReferenceError(message)
    if locations:
        return locations[0]
    return None


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
    declaration_file, declaration_names = resolved
    source = declaration_file.read_text()
    compilation_unit = parse_java_compilation_unit(source)
    type_declaration = next(
        (
            declaration
            for declaration in compilation_unit.types
            if declaration.name == declaration_names[0]
        ),
        None,
    )
    nested_declaration = (
        find_nested_type_declaration(type_declaration, declaration_names[1:])
        if type_declaration is not None
        else None
    )
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
    location = _resolve_exact_type_location(repo_root, import_path)
    if location is None:
        return None
    return location.source_path, list(location.declaration_names)


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
    source_scope: SourceResolutionScope,
) -> str | None:
    if type_name in BUILTIN_REFERENCE_TYPES:
        return None
    lexical_import_path = _resolve_lexical_type_reference(
        repo_root,
        type_name,
        owner_import_path=source_scope.owner_import_path,
        nested_type_names=set(source_scope.nested_type_names),
    )
    if lexical_import_path is not None:
        return lexical_import_path
    return _resolve_non_inherited_type_reference(
        repo_root,
        type_name,
        import_map=dict(source_scope.import_map),
        package_name=source_scope.package_name,
        owner_import_path=source_scope.owner_import_path,
    )


def qualify_java_type_references(
    repo_root: Path,
    java_type: str,
    source_scope: SourceResolutionScope,
) -> str:
    """Persist exact Java identities while the declaring scope is available."""

    # Validate the complete normalized expression before making replacements.
    collect_type_reference_names(java_type)
    return replace_type_reference_names(
        java_type,
        lambda reference_name: resolve_referenced_import_path(
            repo_root,
            reference_name,
            source_scope,
        ),
    )


def _resolve_non_inherited_type_reference(
    repo_root: Path,
    type_name: str,
    *,
    import_map: dict[str, str],
    package_name: str | None,
    owner_import_path: str | None,
) -> str | None:
    explicit_import = import_map.get(type_name)
    if explicit_import is not None:
        return explicit_import
    if "." in type_name:
        if is_fully_qualified_reference_name(type_name):
            # A fully-qualified spelling in source is already proof of identity,
            # including dependencies that are intentionally not materialized in
            # the DolphinScheduler source tree.
            return type_name
        if _exact_declared_type_exists(repo_root, type_name):
            return type_name
        outer_name, _, nested_suffix = type_name.partition(".")
        outer_import_path = import_map.get(outer_name)
        if outer_import_path is None:
            outer_import_path = import_map.get(f"@same_unit:{outer_name}")
        if outer_import_path is not None:
            nested_import_path = f"{outer_import_path}.{nested_suffix}"
            if _exact_declared_type_exists(repo_root, nested_import_path):
                return nested_import_path
    same_unit_import = import_map.get(f"@same_unit:{type_name}")
    if same_unit_import is not None:
        return same_unit_import
    if package_name is not None:
        package_import_path = f"{package_name}.{type_name}"
        if _exact_declared_type_exists(repo_root, package_import_path):
            return package_import_path
    wildcard_candidates = {
        f"{wildcard_package}.{type_name}"
        for key, wildcard_package in import_map.items()
        if key.startswith("@wildcard:")
        and _exact_declared_type_exists(
            repo_root,
            f"{wildcard_package}.{type_name}",
        )
    }
    if len(wildcard_candidates) > 1:
        message = (
            f"Java reference {type_name!r} has ambiguous on-demand imports in "
            f"{owner_import_path or package_name or '<unknown>'}: "
            f"{sorted(wildcard_candidates)!r}"
        )
        raise AmbiguousJavaTypeReferenceError(message)
    return next(iter(wildcard_candidates), None)


def _resolve_lexical_type_reference(
    repo_root: Path,
    type_name: str,
    *,
    owner_import_path: str | None,
    nested_type_names: set[str] | None,
) -> str | None:
    if owner_import_path is None:
        return None
    if nested_type_names and type_name in nested_type_names:
        direct_candidate = f"{owner_import_path}.{type_name}"
        if _exact_declared_type_exists(repo_root, direct_candidate):
            return direct_candidate
    owner_location = _resolve_exact_type_location(repo_root, owner_import_path)
    if owner_location is None:
        return None
    owner_parts = owner_import_path.split(".")
    top_level_owner_size = len(owner_parts) - len(owner_location.declaration_names) + 1
    for prefix_size in range(len(owner_parts), top_level_owner_size - 1, -1):
        lexical_owner = ".".join(owner_parts[:prefix_size])
        declared_candidate = f"{lexical_owner}.{type_name}"
        if _exact_declared_type_exists(repo_root, declared_candidate):
            return declared_candidate
        inherited_candidate = _resolve_inherited_member_type(
            repo_root,
            owner_import_path=lexical_owner,
            type_name=type_name,
            active_owner_import_paths=frozenset(),
        )
        if inherited_candidate is not None:
            return inherited_candidate
    return None


def _resolve_inherited_member_type(
    repo_root: Path,
    *,
    owner_import_path: str,
    type_name: str,
    active_owner_import_paths: frozenset[str],
) -> str | None:
    if owner_import_path in active_owner_import_paths:
        return None
    loaded_owner = load_type_declaration(repo_root, owner_import_path, {})
    if loaded_owner is None:
        return None
    _, owner_declaration, owner_import_map, owner_package_name = loaded_owner
    inherited_candidates: set[str] = set()
    nested_active_owners = active_owner_import_paths | {owner_import_path}
    for super_type in _direct_super_types(owner_declaration):
        super_type_name = _render_reference_type_name(super_type)
        super_import_path = _resolve_non_inherited_type_reference(
            repo_root,
            super_type_name,
            import_map=owner_import_map,
            package_name=owner_package_name,
            owner_import_path=owner_import_path,
        )
        if super_import_path is None:
            continue
        direct_candidate = f"{super_import_path}.{type_name}"
        if _exact_declared_type_exists(repo_root, direct_candidate):
            inherited_candidates.add(direct_candidate)
            continue
        transitive_candidate = _resolve_inherited_member_type(
            repo_root,
            owner_import_path=super_import_path,
            type_name=type_name,
            active_owner_import_paths=nested_active_owners,
        )
        if transitive_candidate is not None:
            inherited_candidates.add(transitive_candidate)
    if len(inherited_candidates) > 1:
        message = (
            f"Java reference {type_name!r} is inherited ambiguously by "
            f"{owner_import_path}: {sorted(inherited_candidates)!r}"
        )
        raise AmbiguousJavaTypeReferenceError(message)
    return next(iter(inherited_candidates), None)


def _direct_super_types(
    declaration: javalang.tree.TypeDeclaration,
) -> tuple[javalang.tree.ReferenceType, ...]:
    super_types: list[javalang.tree.ReferenceType] = []
    extends = getattr(declaration, "extends", None)
    if isinstance(extends, javalang.tree.ReferenceType):
        super_types.append(extends)
    elif isinstance(extends, list):
        super_types.extend(
            item for item in extends if isinstance(item, javalang.tree.ReferenceType)
        )
    super_types.extend(
        item
        for item in (getattr(declaration, "implements", None) or [])
        if isinstance(item, javalang.tree.ReferenceType)
    )
    return tuple(super_types)


def _render_reference_type_name(type_node: javalang.tree.ReferenceType) -> str:
    name = str(type_node.name)
    if type_node.sub_type is not None:
        return f"{name}.{_render_reference_type_name(type_node.sub_type)}"
    return name


@cache
def _exact_declared_type_exists(repo_root: Path, import_path: str) -> bool:
    return _resolve_exact_type_location(repo_root, import_path) is not None


@cache
def _java_source_paths(repo_root: Path) -> tuple[Path, ...]:
    references_root = repo_root / "references/dolphinscheduler"
    return tuple(sorted(references_root.rglob("*.java")))


@cache
def _java_source_paths_by_filename(
    repo_root: Path,
) -> Mapping[str, tuple[Path, ...]]:
    paths_by_filename: dict[str, list[Path]] = {}
    for source_path in _java_source_paths(repo_root):
        paths_by_filename.setdefault(source_path.name, []).append(source_path)
    return MappingProxyType(
        {filename: tuple(paths) for filename, paths in paths_by_filename.items()}
    )


@cache
def _java_source_paths_by_package(repo_root: Path) -> Mapping[str, tuple[Path, ...]]:
    paths_by_package: dict[str, list[Path]] = {}
    for source_path in _java_source_paths(repo_root):
        parts = source_path.parent.parts
        for start in range(len(parts)):
            package_suffix = "/".join(parts[start:])
            paths_by_package.setdefault(package_suffix, []).append(source_path)
    return MappingProxyType(
        {package: tuple(paths) for package, paths in paths_by_package.items()}
    )


@cache
def _java_source_paths_in_package(
    repo_root: Path,
    package_name: str,
) -> tuple[Path, ...]:
    return _java_source_paths_by_package(repo_root).get(
        package_name.replace(".", "/"), ()
    )


@cache
def _declared_type_locations_from_path(
    source_path: Path,
) -> tuple[tuple[str, JavaTypeLocation], ...]:
    compilation_unit = try_parse_java_compilation_unit(source_path.read_text())
    if (
        compilation_unit is None
        or not compilation_unit.types
        or compilation_unit.package is None
    ):
        return ()
    locations_by_import_path: dict[str, set[JavaTypeLocation]] = {}
    for top_level_type in compilation_unit.types:
        top_level_import_path = f"{compilation_unit.package.name}.{top_level_type.name}"
        _index_type_location(
            locations_by_import_path,
            source_path=source_path,
            type_declaration=top_level_type,
            import_path=top_level_import_path,
            declaration_names=(str(top_level_type.name),),
        )
    return tuple(
        (import_path, location)
        for import_path, locations in sorted(locations_by_import_path.items())
        for location in sorted(
            locations,
            key=lambda item: (str(item.source_path), item.declaration_names),
        )
    )


def _index_type_location(
    locations_by_import_path: dict[str, set[JavaTypeLocation]],
    *,
    source_path: Path,
    type_declaration: javalang.tree.TypeDeclaration,
    import_path: str,
    declaration_names: tuple[str, ...],
) -> None:
    locations_by_import_path.setdefault(import_path, set()).add(
        JavaTypeLocation(
            source_path=source_path,
            declaration_names=declaration_names,
        )
    )
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
        nested_name = str(body_declaration.name)
        _index_type_location(
            locations_by_import_path,
            source_path=source_path,
            type_declaration=body_declaration,
            import_path=f"{import_path}.{nested_name}",
            declaration_names=(*declaration_names, nested_name),
        )
