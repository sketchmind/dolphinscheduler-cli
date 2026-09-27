"""Load the complete runtime bundle plan without code-generation dependencies."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast

RuntimeBundleSelection = Literal["full", "runtime-slice"]
DEFAULT_RUNTIME_BUNDLE_MANIFEST = (
    Path(__file__).with_name("ds_codegen") / "runtime_bundles.json"
)
RUNTIME_BUNDLE_SELECTIONS = frozenset({"full", "runtime-slice"})
_VERSION_LABEL = re.compile(r"\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?\Z")


@dataclass(frozen=True)
class RuntimeBundleSpec:
    """One exact upstream input and the generated surface selected from it."""

    version: str
    source_root: Path
    selection: RuntimeBundleSelection

    @property
    def package_slug(self) -> str:
        """Return the generated Python package directory for this version."""
        return runtime_bundle_package_slug(self.version)


def load_runtime_bundle_specs(
    repo_root: Path,
    manifest_path: Path | None = None,
) -> tuple[RuntimeBundleSpec, ...]:
    """Load the tracked, repository-relative complete runtime bundle plan."""
    path = manifest_path or DEFAULT_RUNTIME_BUNDLE_MANIFEST
    raw_bundles = _load_raw_bundles(path)
    specs = [
        _parse_runtime_bundle_spec(repo_root, index=index, raw_bundle=raw_bundle)
        for index, raw_bundle in enumerate(raw_bundles)
    ]

    versions = [item.version for item in specs]
    duplicates = sorted(
        {version for version in versions if versions.count(version) > 1},
        key=natural_version_key,
    )
    if duplicates:
        message = f"duplicate runtime bundle versions: {', '.join(duplicates)}"
        raise ValueError(message)
    return tuple(sorted(specs, key=lambda item: natural_version_key(item.version)))


def runtime_bundle_versions(manifest_path: Path | None = None) -> tuple[str, ...]:
    """Return the complete packaging selection without reading upstream sources."""
    return tuple(
        spec.version for spec in load_runtime_bundle_specs(Path(), manifest_path)
    )


def _load_raw_bundles(path: Path) -> list[object]:
    loaded: object = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        message = "runtime bundle manifest must contain a JSON object"
        raise TypeError(message)
    if loaded.get("schema_version") != 1:
        message = "runtime bundle manifest schema_version must be 1"
        raise ValueError(message)
    raw_bundles = loaded.get("bundles")
    if not isinstance(raw_bundles, list) or not raw_bundles:
        message = "runtime bundle manifest requires at least one bundle"
        raise ValueError(message)
    return list(raw_bundles)


def _parse_runtime_bundle_spec(
    repo_root: Path,
    *,
    index: int,
    raw_bundle: object,
) -> RuntimeBundleSpec:
    if not isinstance(raw_bundle, dict):
        message = f"runtime bundle {index} must be a JSON object"
        raise TypeError(message)
    version = raw_bundle.get("version")
    if not isinstance(version, str) or _VERSION_LABEL.fullmatch(version) is None:
        message = f"runtime bundle {index} version must be an exact DS version"
        raise ValueError(message)
    raw_source_root = raw_bundle.get("source_root")
    if not isinstance(raw_source_root, str) or not raw_source_root:
        message = f"runtime bundle {version} source_root must be a path"
        raise ValueError(message)
    relative_source_root = Path(raw_source_root)
    if relative_source_root.is_absolute() or ".." in relative_source_root.parts:
        message = f"runtime bundle {version} source_root must be repository-relative"
        raise ValueError(message)
    selection = raw_bundle.get("selection")
    if selection not in RUNTIME_BUNDLE_SELECTIONS:
        message = (
            f"runtime bundle {version} selection must be 'full' or 'runtime-slice'"
        )
        raise ValueError(message)
    return RuntimeBundleSpec(
        version=version,
        source_root=repo_root / relative_source_root,
        selection=cast("RuntimeBundleSelection", selection),
    )


def runtime_bundle_package_slug(version: str) -> str:
    """Map one validated exact DS version to its generated package name."""
    if _VERSION_LABEL.fullmatch(version) is None:
        message = f"runtime bundle version must be exact: {version!r}"
        raise ValueError(message)
    return f"ds_{version.replace('.', '_')}"


def natural_version_key(version: str) -> tuple[tuple[int, int | str], ...]:
    """Return a stable natural ordering key for exact runtime versions."""
    parts = re.findall(r"\d+|\D+", version)
    return tuple(
        (0, int(part)) if part.isdigit() else (1, part.casefold()) for part in parts
    )


__all__ = [
    "DEFAULT_RUNTIME_BUNDLE_MANIFEST",
    "RUNTIME_BUNDLE_SELECTIONS",
    "RuntimeBundleSelection",
    "RuntimeBundleSpec",
    "load_runtime_bundle_specs",
    "natural_version_key",
    "runtime_bundle_package_slug",
    "runtime_bundle_versions",
]
