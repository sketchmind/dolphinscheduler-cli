from __future__ import annotations

import argparse
import ast
import tarfile
import unicodedata
import zipfile
from collections import Counter
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import TypeAlias

from runtime_bundle_manifest import (
    DEFAULT_RUNTIME_BUNDLE_MANIFEST,
    RuntimeBundleSpec,
    load_runtime_bundle_specs,
)

ArchiveNames: TypeAlias = tuple[str, ...]
RequiredPathPredicate: TypeAlias = Callable[["_ArchiveInventory"], bool]
NamePredicate: TypeAlias = tuple[str, RequiredPathPredicate]

COMMON_FORBIDDEN_ROOT_SEGMENTS = frozenset(
    {
        ".git",
        ".import_linter_cache",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        ".venv",
        "__pycache__",
        "build",
        "config",
        "dist",
        "references",
        "skills",
    }
)
WHEEL_FORBIDDEN_ROOT_SEGMENTS = COMMON_FORBIDDEN_ROOT_SEGMENTS | frozenset(
    {
        "docs",
        "tests",
        "tools",
    }
)
COMMON_FORBIDDEN_ANY_SEGMENTS = frozenset(
    {"__pycache__", ".agents", ".claude", ".codex", "localdevdocs"}
) | frozenset(
    segment for segment in COMMON_FORBIDDEN_ROOT_SEGMENTS if segment.startswith(".")
)
COMMON_FORBIDDEN_BASENAMES = frozenset(
    {
        ".dsctl-context.yaml",
        ".ds_store",
        "agents.md",
        "claude.md",
        "context",
    }
)
ROOT = Path(__file__).resolve().parents[1]
_REVIEWED_TEST_INPUT_PATHS = (
    "tests/fixtures/error_translation/pre_package_workflow_inventory.json",
    "tests/fixtures/task_authoring/current_contract_fingerprints.json",
    "tests/fixtures/task_authoring/parameter_examples.json",
    "tests/fixtures/command_contract/pre_catalog_fingerprints.json",
    "tests/fixtures/version_discovery/legacy-swagger.json",
    "tests/fixtures/version_discovery/README.md",
    "tests/compatibility/corpus/v0.3.0-ds3.4.1-sql-inline.json",
)
_RUNTIME_BUNDLE_CORE_MODULES = (
    "__init__.py",
    "_artifact.py",
    "_manifest.py",
)
_GENERATED_RUNTIME_ROOT_MODULES = (
    "__init__.py",
    "_discovery_evidence.py",
    "_discovery_operations.py",
    "_discovery_parameters.py",
    "_discovery_profiles.py",
    "_discovery_types.py",
    "conformance_bundles.py",
    "datasource_profiles.py",
    "runtime_instance_profiles.py",
    "task_definition_cleanup_profiles.py",
    "task_definition_profiles.py",
    "task_profiles.py",
    "version_profiles.py",
    "version_discovery.py",
    "workflow_profiles.py",
)
_WIRE_RUNTIME_MODULES = (
    "wire_runtime/__init__.py",
    "wire_runtime/_compiled_schema.py",
    "wire_runtime/_manifest.py",
    "wire_runtime/_models.py",
    "wire_runtime/api/__init__.py",
    "wire_runtime/api/operations/__init__.py",
    "wire_runtime/api/operations/_base.py",
)


@dataclass(frozen=True)
class CheckResult:
    path: Path
    errors: tuple[str, ...]

    @property
    def ok(self) -> bool:
        return not self.errors


@dataclass(frozen=True)
class _ArchiveMember:
    name: str
    parts: tuple[str, ...]
    canonical_name: str
    canonical_path: str
    portable_parts: tuple[str, ...]
    is_directory: bool
    is_absolute: bool

    @classmethod
    def from_name(cls, name: str) -> _ArchiveMember:
        path = PurePosixPath(name)
        canonical_path = path.as_posix()
        is_directory = name.endswith("/")
        canonical_name = canonical_path
        if is_directory and canonical_name != ".":
            canonical_name = f"{canonical_name}/"
        return cls(
            name=name,
            parts=path.parts,
            canonical_name=canonical_name,
            canonical_path=canonical_path.rstrip("/"),
            portable_parts=tuple(
                unicodedata.normalize("NFC", part).rstrip(" .").casefold()
                for part in path.parts
            ),
            is_directory=is_directory,
            is_absolute=path.is_absolute(),
        )


@dataclass(frozen=True)
class _ArchiveInventory:
    """One immutable, once-parsed view of an archive member list."""

    members: tuple[_ArchiveMember, ...]
    exact_names: frozenset[str]
    file_paths: frozenset[str]
    directory_paths: frozenset[str]

    @classmethod
    def from_names(cls, names: Iterable[str]) -> _ArchiveInventory:
        members = tuple(_ArchiveMember.from_name(name) for name in sorted(names))
        file_paths = frozenset(
            member.canonical_name for member in members if not member.is_directory
        )
        directory_paths = {
            PurePosixPath(*member.parts[:index]).as_posix()
            for member in members
            for index in range(1, len(member.parts))
        }
        directory_paths.update(
            member.canonical_path for member in members if member.is_directory
        )
        return cls(
            members=members,
            exact_names=frozenset(member.name for member in members),
            file_paths=file_paths,
            directory_paths=frozenset(directory_paths),
        )

    def strip_first_component(self) -> _ArchiveInventory:
        stripped_names: list[str] = []
        for member in self.members:
            if len(member.parts) <= 1:
                stripped_names.append("")
                continue
            relative_path = "/".join(member.parts[1:])
            if member.is_directory:
                relative_path = f"{relative_path}/"
            stripped_names.append(relative_path)
        return self.from_names(stripped_names)


@dataclass(frozen=True)
class _PackageExpectation:
    """Repository-derived allowlist and required-path snapshot for one check."""

    bundle_specs: tuple[RuntimeBundleSpec, ...]
    runtime_modules_by_package: Mapping[str, tuple[str, ...]]
    generated_artifact_modules: frozenset[str]
    wire_program_modules: frozenset[str]


def check_distribution(
    path: Path,
    *,
    runtime_bundle_manifest: Path = DEFAULT_RUNTIME_BUNDLE_MANIFEST,
) -> CheckResult:
    suffixes = path.suffixes
    if path.suffix == ".whl":
        bundle_specs = load_runtime_bundle_specs(ROOT, runtime_bundle_manifest)
        return _check_wheel(
            path,
            expectation=_build_package_expectation(bundle_specs),
        )
    if suffixes[-2:] == [".tar", ".gz"]:
        bundle_specs = load_runtime_bundle_specs(ROOT, runtime_bundle_manifest)
        return _check_sdist(
            path,
            expectation=_build_package_expectation(bundle_specs),
        )
    return CheckResult(path=path, errors=(f"unsupported distribution type: {path}",))


def _build_package_expectation(
    bundle_specs: tuple[RuntimeBundleSpec, ...],
) -> _PackageExpectation:
    wire_program_modules = _tracked_wire_program_modules()
    return _PackageExpectation(
        bundle_specs=bundle_specs,
        runtime_modules_by_package={
            spec.package_slug: _tracked_runtime_bundle_modules(spec)
            for spec in bundle_specs
        },
        generated_artifact_modules=frozenset(
            _tracked_generated_artifact_modules(
                wire_program_modules=wire_program_modules,
            )
        ),
        wire_program_modules=frozenset(wire_program_modules),
    )


def _check_wheel(
    path: Path,
    *,
    expectation: _PackageExpectation,
) -> CheckResult:
    return _check_wheel_inventory(
        path,
        inventory=_ArchiveInventory.from_names(_wheel_names(path)),
        expectation=expectation,
    )


def _check_wheel_inventory(
    path: Path,
    *,
    inventory: _ArchiveInventory,
    expectation: _PackageExpectation,
) -> CheckResult:
    errors = [
        *_duplicate_path_errors(inventory),
        *_file_directory_collision_errors(inventory),
        *_validate_archive_paths(inventory),
        *_portable_path_collision_errors(inventory),
        *_protected_path_spelling_errors(
            inventory,
            protected_root="dsctl/generated",
        ),
        *_wheel_data_relocation_errors(inventory),
        *_forbidden_path_errors(
            inventory,
            forbidden_root_segments=WHEEL_FORBIDDEN_ROOT_SEGMENTS,
        ),
        *_undeclared_generated_module_errors(
            inventory,
            generated_root="dsctl/generated",
        ),
        *_undeclared_runtime_bundle_errors(
            inventory,
            declared_packages=frozenset(
                expectation.runtime_modules_by_package,
            ),
            versions_root="dsctl/generated/versions",
        ),
        *_undeclared_runtime_bundle_module_errors(
            inventory,
            expected_by_package=expectation.runtime_modules_by_package,
            versions_root="dsctl/generated/versions",
        ),
        *_undeclared_wire_runtime_errors(
            inventory,
            runtime_root="dsctl/generated/wire_runtime",
        ),
        *_undeclared_wire_program_errors(
            inventory,
            expected_modules=expectation.wire_program_modules,
            programs_root="dsctl/generated/wire_programs",
        ),
        *_missing_required_path_errors(
            inventory,
            required_paths=_wheel_required_paths(expectation),
        ),
    ]
    return CheckResult(path=path, errors=tuple(errors))


def _check_sdist(
    path: Path,
    *,
    expectation: _PackageExpectation,
) -> CheckResult:
    inventory, member_type_errors = _read_sdist(path)
    return _check_sdist_inventory(
        path,
        inventory=inventory,
        member_type_errors=member_type_errors,
        expectation=expectation,
    )


def _check_sdist_inventory(
    path: Path,
    *,
    inventory: _ArchiveInventory,
    expectation: _PackageExpectation,
    member_type_errors: tuple[str, ...] = (),
) -> CheckResult:
    stripped_inventory = inventory.strip_first_component()
    errors = [
        *member_type_errors,
        *_duplicate_path_errors(inventory),
        *_file_directory_collision_errors(inventory),
        *_validate_archive_paths(inventory),
        *_portable_path_collision_errors(inventory),
        *_sdist_root_errors(inventory),
        *_duplicate_path_errors(_without_empty_names(stripped_inventory)),
        *_protected_path_spelling_errors(
            stripped_inventory,
            protected_root="src/dsctl/generated",
        ),
        *_forbidden_path_errors(
            stripped_inventory,
            forbidden_root_segments=COMMON_FORBIDDEN_ROOT_SEGMENTS,
        ),
        *_undeclared_generated_module_errors(
            stripped_inventory,
            generated_root="src/dsctl/generated",
        ),
        *_undeclared_runtime_bundle_errors(
            stripped_inventory,
            declared_packages=frozenset(
                expectation.runtime_modules_by_package,
            ),
            versions_root="src/dsctl/generated/versions",
        ),
        *_undeclared_runtime_bundle_module_errors(
            stripped_inventory,
            expected_by_package=expectation.runtime_modules_by_package,
            versions_root="src/dsctl/generated/versions",
        ),
        *_undeclared_wire_runtime_errors(
            stripped_inventory,
            runtime_root="src/dsctl/generated/wire_runtime",
        ),
        *_undeclared_wire_program_errors(
            stripped_inventory,
            expected_modules=expectation.wire_program_modules,
            programs_root="src/dsctl/generated/wire_programs",
        ),
        *_missing_required_path_errors(
            stripped_inventory,
            required_paths=_sdist_required_paths(expectation),
        ),
    ]
    return CheckResult(path=path, errors=tuple(errors))


def _wheel_names(path: Path) -> ArchiveNames:
    with zipfile.ZipFile(path) as archive:
        return tuple(archive.namelist())


def _read_sdist(path: Path) -> tuple[_ArchiveInventory, tuple[str, ...]]:
    names: list[str] = []
    errors: list[str] = []
    with tarfile.open(path, mode="r:gz") as archive:
        for member in archive.getmembers():
            name = member.name
            if member.isdir():
                if not name.endswith("/"):
                    name = f"{name}/"
            elif not member.isfile():
                errors.append(f"non-regular sdist member is not allowed: {member.name}")
            elif name.endswith("/"):
                errors.append(
                    f"regular sdist member uses a directory path: {member.name}"
                )
            names.append(name)
    return _ArchiveInventory.from_names(names), tuple(errors)


def _validate_archive_paths(inventory: _ArchiveInventory) -> list[str]:
    errors: list[str] = []
    for member in inventory.members:
        name = member.name
        if "\\" in name:
            errors.append(f"backslash archive path is not allowed: {name}")
        if PureWindowsPath(name).drive:
            errors.append(f"Windows-rooted archive path is not allowed: {name}")
        if member.is_absolute:
            errors.append(f"absolute archive path is not allowed: {name}")
        if ".." in member.parts:
            errors.append(f"path traversal archive path is not allowed: {name}")
        if name != member.canonical_name:
            errors.append(f"non-canonical archive path is not allowed: {name}")
    return errors


def _portable_path_collision_errors(inventory: _ArchiveInventory) -> list[str]:
    names_by_portable_path: dict[tuple[str, ...], set[str]] = {}
    for member in inventory.members:
        names_by_portable_path.setdefault(member.portable_parts, set()).add(
            member.canonical_path
        )
    return [
        "portable archive path collision is not allowed: "
        + " <-> ".join(sorted(colliding_names))
        for _portable_path, colliding_names in sorted(names_by_portable_path.items())
        if len(colliding_names) > 1
    ]


def _protected_path_spelling_errors(
    inventory: _ArchiveInventory,
    *,
    protected_root: str,
) -> list[str]:
    protected_parts = PurePosixPath(protected_root).parts
    portable_protected_parts = tuple(part.casefold() for part in protected_parts)
    return [
        f"protected archive path spelling is not allowed: {member.name}"
        for member in inventory.members
        if (
            member.portable_parts[: len(portable_protected_parts)]
            == portable_protected_parts
            and member.parts[: len(protected_parts)] != protected_parts
        )
    ]


def _wheel_data_relocation_errors(inventory: _ArchiveInventory) -> list[str]:
    return [
        f"wheel data relocation path is not allowed: {member.name}"
        for member in inventory.members
        if member.parts and member.parts[0].casefold().endswith(".data")
    ]


def _duplicate_path_errors(inventory: _ArchiveInventory) -> list[str]:
    return [
        f"duplicate archive path is not allowed: {name}"
        for name, count in sorted(
            Counter(member.canonical_path for member in inventory.members).items()
        )
        if count > 1
    ]


def _file_directory_collision_errors(inventory: _ArchiveInventory) -> list[str]:
    return [
        f"archive file conflicts with directory path: {name}"
        for name in sorted(inventory.file_paths & inventory.directory_paths)
    ]


def _sdist_root_errors(inventory: _ArchiveInventory) -> list[str]:
    roots = {member.parts[0] for member in inventory.members if member.parts}
    errors: list[str] = []
    if len(roots) != 1:
        errors.append(
            "sdist archive must use exactly one top-level root: "
            f"{', '.join(sorted(roots)) or '<none>'}"
        )
    errors.extend(
        f"sdist file is outside the top-level root: {member.name}"
        for member in inventory.members
        if len(member.parts) == 1 and not member.is_directory
    )
    return errors


def _forbidden_path_errors(
    inventory: _ArchiveInventory,
    *,
    forbidden_root_segments: frozenset[str],
) -> list[str]:
    errors: list[str] = []
    for member in inventory.members:
        parts = member.parts
        if any(part in COMMON_FORBIDDEN_ANY_SEGMENTS for part in member.portable_parts):
            errors.append(f"forbidden path in distribution: {member.name}")
            continue
        if parts and parts[0] in forbidden_root_segments:
            errors.append(f"forbidden path in distribution: {member.name}")
            continue
        if parts and (
            parts[-1].casefold() in COMMON_FORBIDDEN_BASENAMES
            or _is_private_env_filename(parts[-1])
        ):
            errors.append(f"forbidden file in distribution: {member.name}")
    return errors


def _is_private_env_filename(filename: str) -> bool:
    name = filename.casefold()
    if name.endswith(".example"):
        return False
    return (
        name in {".env", ".envrc"}
        or name.startswith((".env.", ".envrc."))
        or name.endswith(".env")
    )


def _missing_required_path_errors(
    inventory: _ArchiveInventory,
    *,
    required_paths: tuple[NamePredicate, ...],
) -> list[str]:
    errors: list[str] = []
    for label, predicate in required_paths:
        if not predicate(inventory):
            errors.append(f"missing required package path: {label}")
    return errors


def _undeclared_generated_module_errors(
    inventory: _ArchiveInventory,
    *,
    generated_root: str,
) -> list[str]:
    allowed_root_modules = set(_GENERATED_RUNTIME_ROOT_MODULES)
    delegated_subtrees = frozenset({"versions", "wire_programs", "wire_runtime"})
    root_parts = PurePosixPath(generated_root).parts
    errors: list[str] = []
    for member in inventory.members:
        parts = member.parts
        if (
            (member.is_directory and "wire_families" not in parts)
            or len(parts) <= len(root_parts)
            or parts[: len(root_parts)] != root_parts
        ):
            continue
        relative_parts = parts[len(root_parts) :]
        if relative_parts[0] in delegated_subtrees:
            if len(relative_parts) > 1:
                continue
            errors.append(f"undeclared generated module: {member.name}")
            continue
        if len(relative_parts) == 1 and relative_parts[0] in allowed_root_modules:
            continue
        errors.append(f"undeclared generated module: {member.name}")
    return errors


def _undeclared_runtime_bundle_errors(
    inventory: _ArchiveInventory,
    *,
    declared_packages: frozenset[str],
    versions_root: str,
) -> list[str]:
    root_parts = PurePosixPath(versions_root).parts
    package_index = len(root_parts)
    archive_packages = {
        member.parts[package_index]
        for member in inventory.members
        if len(member.parts) > package_index + 1
        and member.parts[:package_index] == root_parts
        and not member.is_directory
    }
    return [
        f"undeclared runtime bundle package: {versions_root}/{package_slug}"
        for package_slug in sorted(archive_packages - declared_packages)
    ]


def _undeclared_runtime_bundle_module_errors(
    inventory: _ArchiveInventory,
    *,
    expected_by_package: Mapping[str, tuple[str, ...]],
    versions_root: str,
) -> list[str]:
    root_parts = PurePosixPath(versions_root).parts
    expected_sets = {
        package_slug: frozenset(modules)
        for package_slug, modules in expected_by_package.items()
    }
    errors: list[str] = []
    for member in inventory.members:
        parts = member.parts
        if (
            member.is_directory
            or len(parts) <= len(root_parts)
            or parts[: len(root_parts)] != root_parts
        ):
            continue
        relative_parts = parts[len(root_parts) :]
        if len(relative_parts) == 1:
            if relative_parts[0] != "__init__.py":
                errors.append(f"undeclared runtime bundle module: {member.name}")
            continue
        package_slug = parts[len(root_parts)]
        expected = expected_sets.get(package_slug)
        if expected is None:
            continue
        relative_path = "/".join(parts[len(root_parts) + 1 :])
        if relative_path not in expected:
            errors.append(f"undeclared runtime bundle module: {member.name}")
    return errors


def _undeclared_wire_runtime_errors(
    inventory: _ArchiveInventory,
    *,
    runtime_root: str,
) -> list[str]:
    allowed_relative_paths = {
        PurePosixPath(module).relative_to("wire_runtime").as_posix()
        for module in _WIRE_RUNTIME_MODULES
    }
    root_parts = PurePosixPath(runtime_root).parts
    archive_paths = {
        "/".join(member.parts[len(root_parts) :])
        for member in inventory.members
        if len(member.parts) > len(root_parts)
        and member.parts[: len(root_parts)] == root_parts
        and not member.is_directory
    }
    return [
        f"undeclared wire runtime module: {runtime_root}/{relative_path}"
        for relative_path in sorted(archive_paths - allowed_relative_paths)
    ]


def _undeclared_wire_program_errors(
    inventory: _ArchiveInventory,
    *,
    expected_modules: frozenset[str],
    programs_root: str,
) -> list[str]:
    root_parts = PurePosixPath(programs_root).parts
    archive_paths = {
        "/".join(member.parts[len(root_parts) :])
        for member in inventory.members
        if len(member.parts) > len(root_parts)
        and member.parts[: len(root_parts)] == root_parts
        and not member.is_directory
    }
    return [
        f"undeclared wire program module: {programs_root}/{relative_path}"
        for relative_path in sorted(archive_paths - expected_modules)
    ]


def _wheel_required_paths(
    expectation: _PackageExpectation,
) -> tuple[NamePredicate, ...]:
    return (
        ("dsctl/__init__.py", _contains("dsctl/__init__.py")),
        ("dsctl/app.py", _contains("dsctl/app.py")),
        *_tracked_generated_root_required_paths(package_root="dsctl"),
        *_generated_artifact_required_paths(
            expectation.generated_artifact_modules,
            package_root="dsctl",
        ),
        *_expected_runtime_bundle_required_paths(
            expectation,
            versions_root="dsctl/generated/versions",
        ),
        ("dsctl/upstream/registry.py", _contains("dsctl/upstream/registry.py")),
        ("dsctl/py.typed", _contains("dsctl/py.typed")),
        (".dist-info/METADATA", _endswith(".dist-info/METADATA")),
        (".dist-info/entry_points.txt", _endswith(".dist-info/entry_points.txt")),
        (".dist-info/licenses/LICENSE", _endswith(".dist-info/licenses/LICENSE")),
    )


def _sdist_required_paths(
    expectation: _PackageExpectation,
) -> tuple[NamePredicate, ...]:
    return (
        ("pyproject.toml", _contains("pyproject.toml")),
        ("MANIFEST.in", _contains("MANIFEST.in")),
        ("README.md", _contains("README.md")),
        ("LICENSE", _contains("LICENSE")),
        *((name, _contains(name)) for name in _REVIEWED_TEST_INPUT_PATHS),
        ("src/dsctl/__init__.py", _contains("src/dsctl/__init__.py")),
        *_tracked_generated_root_required_paths(package_root="src/dsctl"),
        *_generated_artifact_required_paths(
            expectation.generated_artifact_modules,
            package_root="src/dsctl",
        ),
        *_expected_runtime_bundle_required_paths(
            expectation,
            versions_root="src/dsctl/generated/versions",
        ),
        (
            "src/dsctl/upstream/registry.py",
            _contains("src/dsctl/upstream/registry.py"),
        ),
        ("docs/development/release.md", _contains("docs/development/release.md")),
        ("docs/development/tooling.md", _contains("docs/development/tooling.md")),
        (
            "docs/development/live-evidence/external-shell/3.4.2/*.json",
            _under_with_suffix(
                "docs/development/live-evidence/external-shell/3.4.2/",
                ".json",
            ),
        ),
        (
            "tools/check_package_contents.py",
            _contains(
                "tools/check_package_contents.py",
            ),
        ),
        (
            "tools/analyze_ds_conformance_bundles.py",
            _contains("tools/analyze_ds_conformance_bundles.py"),
        ),
        (
            "tools/generate_ds_runtime_bundles.py",
            _contains("tools/generate_ds_runtime_bundles.py"),
        ),
        (
            "tools/ds_codegen/conformance_bundles.json",
            _contains("tools/ds_codegen/conformance_bundles.json"),
        ),
        (
            "tools/ds_codegen/conformance_bundles.py",
            _contains("tools/ds_codegen/conformance_bundles.py"),
        ),
        (
            "tools/ds_codegen/runtime_bundles.json",
            _contains("tools/ds_codegen/runtime_bundles.json"),
        ),
        (
            "tools/ds_codegen/runtime_artifacts.py",
            _contains("tools/ds_codegen/runtime_artifacts.py"),
        ),
        (
            "tools/prepare_ds_runtime_sources.py",
            _contains("tools/prepare_ds_runtime_sources.py"),
        ),
        (
            "tools/runtime_bundle_manifest.py",
            _contains("tools/runtime_bundle_manifest.py"),
        ),
        (
            "tests/packaging/test_pyproject_metadata.py",
            _contains(
                "tests/packaging/test_pyproject_metadata.py",
            ),
        ),
    )


def _contains(path: str) -> RequiredPathPredicate:
    def predicate(inventory: _ArchiveInventory) -> bool:
        return path in inventory.exact_names

    return predicate


def _expected_runtime_bundle_required_paths(
    expectation: _PackageExpectation,
    *,
    versions_root: str,
) -> tuple[NamePredicate, ...]:
    return tuple(
        _required_exact_path(f"{versions_root}/{spec.package_slug}/{relative_path}")
        for spec in expectation.bundle_specs
        for relative_path in expectation.runtime_modules_by_package[spec.package_slug]
    )


def _tracked_generated_root_required_paths(
    *,
    package_root: str,
) -> tuple[NamePredicate, ...]:
    return tuple(
        _required_exact_path(f"{package_root}/generated/{module_name}")
        for module_name in _GENERATED_RUNTIME_ROOT_MODULES
    )


def _generated_artifact_required_paths(
    modules: Iterable[str],
    *,
    package_root: str,
) -> tuple[NamePredicate, ...]:
    return tuple(
        _required_exact_path(
            (Path(package_root) / "generated" / relative_path).as_posix()
        )
        for relative_path in sorted(modules)
    )


def _tracked_generated_artifact_modules(
    *,
    wire_program_modules: tuple[str, ...] | None = None,
) -> tuple[str, ...]:
    resolved_wire_program_modules = (
        wire_program_modules
        if wire_program_modules is not None
        else _tracked_wire_program_modules()
    )
    relative_paths = {
        *_WIRE_RUNTIME_MODULES,
        *(f"wire_programs/{module}" for module in resolved_wire_program_modules),
    }
    return tuple(sorted(relative_paths))


def _tracked_wire_program_modules() -> tuple[str, ...]:
    manifest_path = (
        ROOT / "src" / "dsctl" / "generated" / "wire_programs" / ("_manifest.py")
    )
    tree = ast.parse(
        manifest_path.read_text(encoding="utf-8"),
        filename=str(manifest_path),
    )
    modules = _manifest_string_tuple(tree, "MODULES", source=manifest_path)
    domain_modules = _manifest_string_tuple(
        tree,
        "DOMAIN_MODULES",
        source=manifest_path,
    )
    support_modules = _manifest_string_tuple(
        tree,
        "SUPPORT_MODULES",
        source=manifest_path,
        allow_empty=True,
    )
    expected_modules = tuple(sorted(("__init__.py", *domain_modules, *support_modules)))
    if not domain_modules or modules != expected_modules:
        message = (
            "compiled wire manifest domain/support module inventory is inconsistent"
        )
        raise ValueError(message)
    _validate_manifest_module_inventory(modules, source=manifest_path)
    return tuple(sorted(("_manifest.py", *modules)))


def _manifest_string_tuple(
    tree: ast.Module,
    name: str,
    *,
    source: Path,
    allow_empty: bool = False,
) -> tuple[str, ...]:
    assignments = [
        node.value
        for node in tree.body
        if isinstance(node, (ast.Assign, ast.AnnAssign))
        and (
            any(
                isinstance(target, ast.Name) and target.id == name
                for target in node.targets
            )
            if isinstance(node, ast.Assign)
            else isinstance(node.target, ast.Name) and node.target.id == name
        )
    ]
    if len(assignments) != 1 or not isinstance(assignments[0], ast.Tuple):
        message = f"compiled wire manifest {name} must be one literal tuple: {source}"
        raise ValueError(message)
    values = assignments[0].elts
    if not allow_empty and not values:
        message = f"compiled wire manifest {name} must be nonempty: {source}"
        raise ValueError(message)
    result: list[str] = []
    for value in values:
        if not isinstance(value, ast.Constant) or not isinstance(value.value, str):
            message = f"compiled wire manifest {name} must contain strings: {source}"
            raise TypeError(message)
        result.append(value.value)
    return tuple(result)


def _validate_manifest_module_inventory(
    modules: tuple[str, ...],
    *,
    source: Path,
) -> None:
    if len(modules) != len(set(modules)) or modules != tuple(sorted(modules)):
        message = f"compiled wire manifest MODULES must be sorted and unique: {source}"
        raise ValueError(message)
    portable_paths: set[tuple[str, ...]] = set()
    for module in modules:
        path = PurePosixPath(module)
        portable_path = tuple(
            unicodedata.normalize("NFC", part).rstrip(" .").casefold()
            for part in path.parts
        )
        if (
            "\\" in module
            or PureWindowsPath(module).drive
            or path.is_absolute()
            or ".." in path.parts
            or path.as_posix() != module
            or path.suffix != ".py"
            or portable_path in portable_paths
        ):
            message = f"compiled wire manifest contains an unsafe module: {module!r}"
            raise ValueError(message)
        portable_paths.add(portable_path)


def _tracked_runtime_bundle_modules(spec: RuntimeBundleSpec) -> tuple[str, ...]:
    package_root = ROOT / "src" / "dsctl" / "generated" / "versions" / spec.package_slug
    if not package_root.is_dir():
        return _RUNTIME_BUNDLE_CORE_MODULES
    modules = tuple(
        path.relative_to(package_root).as_posix()
        for path in sorted(package_root.rglob("*.py"))
        if "__pycache__" not in path.parts
    )
    return modules or _RUNTIME_BUNDLE_CORE_MODULES


def _required_exact_path(path: str) -> NamePredicate:
    return path, _contains(path)


def _endswith(suffix: str) -> RequiredPathPredicate:
    def predicate(inventory: _ArchiveInventory) -> bool:
        return any(member.name.endswith(suffix) for member in inventory.members)

    return predicate


def _under_with_suffix(prefix: str, suffix: str) -> RequiredPathPredicate:
    def predicate(inventory: _ArchiveInventory) -> bool:
        return any(
            member.name.startswith(prefix) and member.name.endswith(suffix)
            for member in inventory.members
        )

    return predicate


def _without_empty_names(inventory: _ArchiveInventory) -> _ArchiveInventory:
    return _ArchiveInventory.from_names(
        member.name for member in inventory.members if member.name
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--runtime-bundle-manifest",
        type=Path,
        default=DEFAULT_RUNTIME_BUNDLE_MANIFEST,
    )
    parser.add_argument("distributions", nargs="+", type=Path)
    args = parser.parse_args(argv)

    results = [
        check_distribution(
            path,
            runtime_bundle_manifest=args.runtime_bundle_manifest,
        )
        for path in args.distributions
    ]
    for result in results:
        if result.ok:
            print(f"package content check passed: {result.path}")
            continue
        print(f"package content check failed: {result.path}")
        for error in result.errors:
            print(f"  - {error}")

    return 0 if all(result.ok for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
