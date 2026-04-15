from __future__ import annotations

import argparse
import tarfile
import zipfile
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import TypeAlias

ArchiveNames: TypeAlias = tuple[str, ...]
RequiredPathPredicate: TypeAlias = Callable[[ArchiveNames], bool]
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
    }
)
WHEEL_FORBIDDEN_ROOT_SEGMENTS = COMMON_FORBIDDEN_ROOT_SEGMENTS | frozenset(
    {
        "docs",
        "tests",
        "tools",
    }
)
COMMON_FORBIDDEN_ANY_SEGMENTS = frozenset({"__pycache__"})
COMMON_FORBIDDEN_BASENAMES = frozenset(
    {
        ".dsctl-context.yaml",
        ".DS_Store",
        "context",
    }
)


@dataclass(frozen=True)
class CheckResult:
    path: Path
    errors: tuple[str, ...]

    @property
    def ok(self) -> bool:
        return not self.errors


def check_distribution(path: Path) -> CheckResult:
    suffixes = path.suffixes
    if path.suffix == ".whl":
        return _check_wheel(path)
    if suffixes[-2:] == [".tar", ".gz"]:
        return _check_sdist(path)
    return CheckResult(path=path, errors=(f"unsupported distribution type: {path}",))


def _check_wheel(path: Path) -> CheckResult:
    names = _wheel_names(path)
    errors = [
        *_validate_archive_paths(names),
        *_forbidden_path_errors(
            names,
            forbidden_root_segments=WHEEL_FORBIDDEN_ROOT_SEGMENTS,
        ),
        *_missing_required_path_errors(names, required_paths=_wheel_required_paths()),
    ]
    return CheckResult(path=path, errors=tuple(errors))


def _check_sdist(path: Path) -> CheckResult:
    names = _sdist_names(path)
    stripped_names = _strip_sdist_root(names)
    errors = [
        *_validate_archive_paths(names),
        *_forbidden_path_errors(
            stripped_names,
            forbidden_root_segments=COMMON_FORBIDDEN_ROOT_SEGMENTS,
        ),
        *_missing_required_path_errors(
            stripped_names,
            required_paths=_sdist_required_paths(),
        ),
    ]
    return CheckResult(path=path, errors=tuple(errors))


def _wheel_names(path: Path) -> ArchiveNames:
    with zipfile.ZipFile(path) as archive:
        return tuple(sorted(archive.namelist()))


def _sdist_names(path: Path) -> ArchiveNames:
    with tarfile.open(path, mode="r:gz") as archive:
        return tuple(sorted(archive.getnames()))


def _validate_archive_paths(names: ArchiveNames) -> list[str]:
    errors: list[str] = []
    for name in names:
        pure_path = PurePosixPath(name)
        if pure_path.is_absolute():
            errors.append(f"absolute archive path is not allowed: {name}")
        if ".." in pure_path.parts:
            errors.append(f"path traversal archive path is not allowed: {name}")
    return errors


def _forbidden_path_errors(
    names: ArchiveNames,
    *,
    forbidden_root_segments: frozenset[str],
) -> list[str]:
    errors: list[str] = []
    for name in names:
        parts = PurePosixPath(name).parts
        if any(part in COMMON_FORBIDDEN_ANY_SEGMENTS for part in parts):
            errors.append(f"forbidden path in distribution: {name}")
            continue
        if parts and parts[0] in forbidden_root_segments:
            errors.append(f"forbidden path in distribution: {name}")
            continue
        if parts and parts[-1] in COMMON_FORBIDDEN_BASENAMES:
            errors.append(f"forbidden file in distribution: {name}")
    return errors


def _missing_required_path_errors(
    names: ArchiveNames,
    *,
    required_paths: tuple[NamePredicate, ...],
) -> list[str]:
    errors: list[str] = []
    for label, predicate in required_paths:
        if not predicate(names):
            errors.append(f"missing required package path: {label}")
    return errors


def _wheel_required_paths() -> tuple[NamePredicate, ...]:
    return (
        ("dsctl/__init__.py", _contains("dsctl/__init__.py")),
        ("dsctl/app.py", _contains("dsctl/app.py")),
        (
            "dsctl/generated/versions/ds_3_4_1/__init__.py",
            _contains(
                "dsctl/generated/versions/ds_3_4_1/__init__.py",
            ),
        ),
        ("dsctl/py.typed", _contains("dsctl/py.typed")),
        (".dist-info/METADATA", _endswith(".dist-info/METADATA")),
        (".dist-info/entry_points.txt", _endswith(".dist-info/entry_points.txt")),
        (".dist-info/licenses/LICENSE", _endswith(".dist-info/licenses/LICENSE")),
    )


def _sdist_required_paths() -> tuple[NamePredicate, ...]:
    return (
        ("pyproject.toml", _contains("pyproject.toml")),
        ("MANIFEST.in", _contains("MANIFEST.in")),
        ("README.md", _contains("README.md")),
        ("LICENSE", _contains("LICENSE")),
        ("src/dsctl/__init__.py", _contains("src/dsctl/__init__.py")),
        ("docs/development/release.md", _contains("docs/development/release.md")),
        ("docs/development/tooling.md", _contains("docs/development/tooling.md")),
        (
            "tools/check_package_contents.py",
            _contains(
                "tools/check_package_contents.py",
            ),
        ),
        (
            "tests/packaging/test_pyproject_metadata.py",
            _contains(
                "tests/packaging/test_pyproject_metadata.py",
            ),
        ),
    )


def _contains(path: str) -> RequiredPathPredicate:
    def predicate(names: ArchiveNames) -> bool:
        return path in names

    return predicate


def _endswith(suffix: str) -> RequiredPathPredicate:
    def predicate(names: ArchiveNames) -> bool:
        return any(name.endswith(suffix) for name in names)

    return predicate


def _strip_sdist_root(names: ArchiveNames) -> ArchiveNames:
    stripped: list[str] = []
    for name in names:
        parts = PurePosixPath(name).parts
        if len(parts) <= 1:
            stripped.append("")
            continue
        stripped.append(PurePosixPath(*parts[1:]).as_posix())
    return tuple(stripped)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("distributions", nargs="+", type=Path)
    args = parser.parse_args(argv)

    results = [check_distribution(path) for path in args.distributions]
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
