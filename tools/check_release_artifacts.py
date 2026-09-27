from __future__ import annotations

import argparse
import ast
import base64
import configparser
import csv
import hashlib
import io
import json
import re
import sys
import tarfile
import tomllib
import zipfile
from collections import Counter
from dataclasses import dataclass
from email import policy
from email.parser import BytesParser
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Never

from packaging.markers import Marker
from packaging.requirements import InvalidRequirement, Requirement
from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.utils import canonicalize_name
from packaging.version import InvalidVersion, Version

from check_conformance_bundle_evidence import (
    ConformanceBundleCorpus,
    check_conformance_bundle_evidence_corpus,
)
from check_package_contents import (
    COMMON_FORBIDDEN_ANY_SEGMENTS,
    COMMON_FORBIDDEN_BASENAMES,
)
from check_release_version import check_release_version
from live_gate.exact_profile_policy import CURRENT_RELEASE_EXACT_GATE_POLICY
from live_gate.exact_profile_promotion_evidence import (
    check_exact_profile_promotion_evidence,
)
from live_gate.exact_profile_read_corpus import check_exact_profile_read_evidence_corpus

if TYPE_CHECKING:
    from collections.abc import Mapping
    from email.message import Message

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_EVIDENCE_DIR = CURRENT_RELEASE_EXACT_GATE_POLICY.evidence_directory(ROOT)
_CONFORMANCE_EVIDENCE_RELATIVE = (
    Path("docs") / "development" / "live-evidence" / "conformance-bundles"
)
_EXACT_READ_EVIDENCE_RELATIVE = (
    Path("docs") / "development" / "live-evidence" / "exact-read"
)
_ROOT_SOURCE_FILES = (
    "pyproject.toml",
    "MANIFEST.in",
    "README.md",
    "LICENSE",
    "CHANGELOG.md",
    "SECURITY.md",
    "CONTRIBUTING.md",
)
_EGG_INFO = "src/dolphinscheduler_cli.egg-info"
_GENERATED_SDIST_FILES = frozenset(
    {
        "PKG-INFO",
        "setup.cfg",
        f"{_EGG_INFO}/PKG-INFO",
        f"{_EGG_INFO}/SOURCES.txt",
        f"{_EGG_INFO}/dependency_links.txt",
        f"{_EGG_INFO}/entry_points.txt",
        f"{_EGG_INFO}/requires.txt",
        f"{_EGG_INFO}/top_level.txt",
    }
)
_SOURCES_GENERATED_FILES = _GENERATED_SDIST_FILES - {"PKG-INFO", "setup.cfg"}
_SETUP_CFG = b"[egg_info]\ntag_build = \ntag_date = 0\n\n"
_CONTRACT_FIELDS = {
    "DS_VERSION": "ds_version",
    "SELECTION": "selection",
    "SEMANTIC_OPERATIONS": "semantic_operations",
    "SOURCE_TAG": "source_tag",
    "SOURCE_COMMIT": "source_commit",
    "SOURCE_TREE": "source_tree",
    "SOURCE_CONTRACT_DIGEST": "source_contract_digest",
    "RENDERED_CONTRACT_DIGEST": "rendered_contract_digest",
    "OPERATION_COUNT": "operation_count",
}
_CORE_SINGLE_HEADERS = frozenset(
    {
        "Metadata-Version",
        "Name",
        "Version",
        "Summary",
        "Description",
        "Description-Content-Type",
        "Keywords",
        "Home-page",
        "Download-URL",
        "Author",
        "Author-email",
        "Maintainer",
        "Maintainer-email",
        "License",
        "License-Expression",
        "Requires-Python",
    }
)
_CORE_REPEATED_HEADERS = frozenset(
    {
        "Dynamic",
        "Platform",
        "Supported-Platform",
        "License-File",
        "Classifier",
        "Requires-Dist",
        "Requires-External",
        "Project-URL",
        "Provides-Extra",
        "Provides-Dist",
        "Obsoletes-Dist",
        "Import-Name",
        "Import-Namespace",
        "Requires",
        "Provides",
        "Obsoletes",
    }
)
_CORE_HEADERS_BY_LOWER = {
    name.lower(): name for name in _CORE_SINGLE_HEADERS | _CORE_REPEATED_HEADERS
}
_EXPECTED_METADATA_VERSION = "2.4"
_SPDX_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9.+:-]*")
_MAX_SPDX_TOKENS = 256
_MAX_SPDX_NESTING = 32
RequirementIdentity = tuple[
    str,
    tuple[str, ...],
    str,
    str | None,
    str | None,
]


@dataclass(frozen=True)
class ReleaseArtifacts:
    """One immutable release distribution set and its governed live evidence."""

    version: str
    wheel: Path
    sdist: Path
    receipt: Path
    conformance_corpus: ConformanceBundleCorpus
    sha256_by_filename: dict[str, str]


@dataclass(frozen=True)
class WheelArtifact:
    """One canonical wheel validated against the current source checkout."""

    version: str
    wheel: Path
    sha256: str
    contract: dict[str, object]


@dataclass(frozen=True)
class _ProjectMetadata:
    name: str
    version: str
    summary: str | None
    requires_python: str
    requirements: tuple[RequirementIdentity, ...]
    extras: tuple[str, ...]
    dynamic: tuple[str, ...]
    entry_points: dict[str, dict[str, str]]
    readme: bytes
    readme_content_type: str
    author: str | None
    author_email: str | None
    maintainer: str | None
    maintainer_email: str | None
    license_expression: tuple[str, ...]
    license_files: tuple[str, ...]
    keywords: tuple[str, ...]
    classifiers: tuple[str, ...]
    project_urls: dict[str, str]
    license: bytes


@dataclass(frozen=True)
class _WheelMetadata:
    metadata: bytes
    entry_points: bytes
    top_level: bytes
    contract: dict[str, object]


class _CaseSensitiveConfigParser(configparser.ConfigParser):
    def optionxform(self, optionstr: str) -> str:
        """Preserve case-sensitive entry-point names."""
        return optionstr


def check_wheel_artifact(root: Path, wheel: Path) -> WheelArtifact:
    """Validate one canonical wheel before it is used by a live gate."""
    project = _load_project_metadata(root)
    checked, _ = _validate_canonical_wheel(
        root,
        wheel,
        project=project,
        version=project.version,
    )
    return checked


def _validate_canonical_wheel(
    root: Path,
    wheel: Path,
    *,
    project: _ProjectMetadata,
    version: str,
) -> tuple[WheelArtifact, _WheelMetadata]:
    if not wheel.is_file():
        message = f"release artifact does not exist: {wheel}"
        raise FileNotFoundError(message)
    expected_name = f"dolphinscheduler_cli-{version}-py3-none-any.whl"
    if wheel.name != expected_name:
        message = (
            f"canonical wheel filename must be {expected_name!r}, got {wheel.name!r}"
        )
        raise ValueError(message)
    metadata = _validate_wheel(
        wheel,
        source_root=root / "src" / "dsctl",
        project=project,
        version=version,
    )
    return (
        WheelArtifact(
            version=version,
            wheel=wheel,
            sha256=_sha256(wheel),
            contract=metadata.contract,
        ),
        metadata,
    )


def check_release_artifacts(
    root: Path,
    artifacts: list[Path],
    *,
    tag: str,
    evidence_dir: Path,
    conformance_evidence_root: Path | None = None,
    index_payload: object | None = None,
) -> ReleaseArtifacts:
    """Validate canonical artifacts against source, live evidence, and an index."""
    version = check_release_version(root, tag=tag)
    wheel_name = f"dolphinscheduler_cli-{version}-py3-none-any.whl"
    sdist_name = f"dolphinscheduler_cli-{version}.tar.gz"
    paths_by_name = _unique_paths_by_name(artifacts)
    expected_names = {wheel_name, sdist_name}
    if set(paths_by_name) != expected_names:
        message = (
            "release artifacts must be exactly "
            f"{sorted(expected_names)!r}, got {sorted(paths_by_name)!r}"
        )
        raise ValueError(message)

    sha256_by_filename = {name: _sha256(path) for name, path in paths_by_name.items()}
    project = _load_project_metadata(root)
    _, wheel_metadata = _validate_canonical_wheel(
        root,
        paths_by_name[wheel_name],
        project=project,
        version=version,
    )
    _validate_sdist(
        paths_by_name[sdist_name],
        root=root,
        project=project,
        version=version,
        wheel=wheel_metadata,
    )
    conformance_corpus = check_conformance_bundle_evidence_corpus(
        conformance_evidence_root or root / _CONFORMANCE_EVIDENCE_RELATIVE,
        expected_wheel_filename=wheel_name,
        expected_wheel_sha256="sha256:" + sha256_by_filename[wheel_name],
        source_root=root,
    )
    check_exact_profile_read_evidence_corpus(
        root / _EXACT_READ_EVIDENCE_RELATIVE,
        expected_wheel_filename=wheel_name,
        expected_wheel_sha256="sha256:" + sha256_by_filename[wheel_name],
        source_root=root,
    )
    promotion = check_exact_profile_promotion_evidence(
        evidence_dir,
        source_root=root,
        expected_wheel_filename=wheel_name,
        expected_wheel_sha256="sha256:" + sha256_by_filename[wheel_name],
        ds_version=CURRENT_RELEASE_EXACT_GATE_POLICY.ds_version,
    )
    if promotion.receipt is None:
        message = (
            "current release requires one promoted exact "
            f"{CURRENT_RELEASE_EXACT_GATE_POLICY.ds_version} receipt"
        )
        raise ValueError(message)
    if index_payload is not None:
        _validate_index_payload(
            index_payload,
            version=version,
            sha256_by_filename=sha256_by_filename,
        )
    return ReleaseArtifacts(
        version=version,
        wheel=paths_by_name[wheel_name],
        sdist=paths_by_name[sdist_name],
        receipt=promotion.receipt,
        conformance_corpus=conformance_corpus,
        sha256_by_filename=sha256_by_filename,
    )


def _unique_paths_by_name(artifacts: list[Path]) -> dict[str, Path]:
    paths_by_name: dict[str, Path] = {}
    for path in artifacts:
        if not path.is_file():
            message = f"release artifact does not exist: {path}"
            raise FileNotFoundError(message)
        if path.name in paths_by_name:
            message = f"duplicate release artifact filename: {path.name}"
            raise ValueError(message)
        paths_by_name[path.name] = path
    return paths_by_name


def _load_project_metadata(root: Path) -> _ProjectMetadata:
    payload: object = tomllib.loads(
        (root / "pyproject.toml").read_text(encoding="utf-8")
    )
    pyproject = _mapping(payload, label="pyproject")
    project = _mapping(pyproject.get("project"), label="pyproject project")
    name = _text(project.get("name"), label="project name")
    version = _text(project.get("version"), label="project version")
    summary = _optional_text(project.get("description"), label="project description")
    requires_python = _text(
        project.get("requires-python"),
        label="project requires-python",
    )
    requirements = [
        _requirement_identity(value)
        for value in _string_list(
            project.get("dependencies", []),
            label="project dependencies",
        )
    ]
    optional = _mapping(
        project.get("optional-dependencies", {}),
        label="project optional-dependencies",
    )
    for extra, values in optional.items():
        requirements.extend(
            _requirement_identity(value, extra=extra)
            for value in _string_list(
                values,
                label=f"project optional dependency {extra}",
            )
        )
    extras = tuple(sorted(canonicalize_name(extra) for extra in optional))
    declared_dynamic = _string_list(
        project.get("dynamic", []),
        label="project dynamic metadata",
    )
    if declared_dynamic:
        message = "release project metadata must be fully static"
        raise ValueError(message)
    dynamic = ("license-file",) if "license-files" in project else ()

    entry_points: dict[str, dict[str, str]] = {}
    _add_entry_point_group(entry_points, "console_scripts", project.get("scripts", {}))
    _add_entry_point_group(
        entry_points,
        "gui_scripts",
        project.get("gui-scripts", {}),
    )
    raw_groups = _mapping(
        project.get("entry-points", {}),
        label="project entry-points",
    )
    for group, values in raw_groups.items():
        _add_entry_point_group(entry_points, group, values)

    readme_value = project.get("readme")
    if not isinstance(readme_value, str):
        message = "project readme must be a file path for release validation"
        raise TypeError(message)
    readme_path = root / readme_value
    if not readme_path.is_file():
        message = f"project readme does not exist: {readme_path}"
        raise FileNotFoundError(message)
    readme_content_type = _readme_content_type(readme_path)
    author, author_email = _project_contacts(
        project.get("authors", []),
        label="project authors",
    )
    maintainer, maintainer_email = _project_contacts(
        project.get("maintainers", []),
        label="project maintainers",
    )
    license_expression = _license_expression(project.get("license"))
    license_files = _matched_license_files(
        root,
        _string_list(
            project.get("license-files", []),
            label="project license-files",
        ),
    )
    keywords = tuple(
        _string_list(project.get("keywords", []), label="project keywords")
    )
    classifiers = tuple(
        _string_list(project.get("classifiers", []), label="project classifiers")
    )
    project_urls = {
        label: _text(value, label=f"project URL {label}")
        for label, value in _mapping(
            project.get("urls", {}),
            label="project urls",
        ).items()
    }
    return _ProjectMetadata(
        name=name,
        version=version,
        summary=summary,
        requires_python=requires_python,
        requirements=tuple(requirements),
        extras=extras,
        dynamic=dynamic,
        entry_points=entry_points,
        readme=readme_path.read_bytes(),
        readme_content_type=readme_content_type,
        author=author,
        author_email=author_email,
        maintainer=maintainer,
        maintainer_email=maintainer_email,
        license_expression=license_expression,
        license_files=license_files,
        keywords=keywords,
        classifiers=classifiers,
        project_urls=project_urls,
        license=(root / "LICENSE").read_bytes(),
    )


def _optional_text(value: object, *, label: str) -> str | None:
    if value is None:
        return None
    return _text(value, label=label)


def _readme_content_type(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".md":
        return "text/markdown"
    if suffix == ".rst":
        return "text/x-rst"
    message = f"project readme suffix has no standard content type: {path.name!r}"
    raise ValueError(message)


def _project_contacts(
    value: object,
    *,
    label: str,
) -> tuple[str | None, str | None]:
    if not isinstance(value, list):
        message = f"{label} must be an array"
        raise TypeError(message)
    names: list[str] = []
    addresses: list[str] = []
    for position, raw_contact in enumerate(value):
        contact = _mapping(raw_contact, label=f"{label} entry {position}")
        name = _optional_text(contact.get("name"), label=f"{label} name {position}")
        email = _optional_text(
            contact.get("email"),
            label=f"{label} email {position}",
        )
        if name is None and email is None:
            message = f"{label} entry {position} must contain a name or email"
            raise ValueError(message)
        if email is None:
            if name is not None:
                names.append(name)
            continue
        addresses.append(email if name is None else f"{name} <{email}>")
    return (", ".join(names) or None, ", ".join(addresses) or None)


def _license_expression(value: object) -> tuple[str, ...]:
    expression = _text(value, label="project license expression")
    return _spdx_token_sequence(expression)


def _spdx_token_sequence(expression: str) -> tuple[str, ...]:
    tokens: list[str] = []
    position = 0
    nesting = 0
    while position < len(expression):
        if expression[position].isspace():
            position += 1
            continue
        if expression[position] in "()":
            if expression[position] == "(":
                nesting += 1
                if nesting > _MAX_SPDX_NESTING:
                    _raise_spdx_limit("nesting", _MAX_SPDX_NESTING)
            elif nesting > 0:
                nesting -= 1
            _append_spdx_token(tokens, expression[position])
            position += 1
            continue
        match = _SPDX_IDENTIFIER.match(expression, position)
        if match is None:
            _raise_invalid_spdx(expression)
        value = match.group()
        normalized = value.casefold()
        _append_spdx_token(
            tokens,
            normalized.upper()
            if normalized in {"and", "or", "with"}
            else f"id:{normalized}",
        )
        position = match.end()
    if not tokens or _parse_spdx_or(tokens, 0, expression) != len(tokens):
        _raise_invalid_spdx(expression)
    return tuple(tokens)


def _append_spdx_token(tokens: list[str], value: str) -> None:
    if len(tokens) >= _MAX_SPDX_TOKENS:
        _raise_spdx_limit("token count", _MAX_SPDX_TOKENS)
    tokens.append(value)


def _parse_spdx_or(tokens: list[str], index: int, expression: str) -> int:
    index = _parse_spdx_and(tokens, index, expression)
    while index < len(tokens) and tokens[index] == "OR":
        index = _parse_spdx_and(tokens, index + 1, expression)
    return index


def _parse_spdx_and(tokens: list[str], index: int, expression: str) -> int:
    index = _parse_spdx_with(tokens, index, expression)
    while index < len(tokens) and tokens[index] == "AND":
        index = _parse_spdx_with(tokens, index + 1, expression)
    return index


def _parse_spdx_with(tokens: list[str], index: int, expression: str) -> int:
    start = index
    index = _parse_spdx_primary(tokens, index, expression)
    if index >= len(tokens) or tokens[index] != "WITH":
        return index
    if not tokens[start].startswith("id:"):
        _raise_invalid_spdx(expression)
    index += 1
    if index >= len(tokens) or not tokens[index].startswith("id:"):
        _raise_invalid_spdx(expression)
    return index + 1


def _parse_spdx_primary(tokens: list[str], index: int, expression: str) -> int:
    if index >= len(tokens):
        _raise_invalid_spdx(expression)
    current = tokens[index]
    if current.startswith("id:"):
        return index + 1
    if current != "(":
        _raise_invalid_spdx(expression)
    index = _parse_spdx_or(tokens, index + 1, expression)
    if index >= len(tokens) or tokens[index] != ")":
        _raise_invalid_spdx(expression)
    return index + 1


def _raise_invalid_spdx(expression: str) -> Never:
    message = f"invalid SPDX license expression: {expression!r}"
    raise ValueError(message)


def _raise_spdx_limit(label: str, limit: int) -> Never:
    message = f"invalid SPDX license expression: {label} exceeds limit {limit}"
    raise ValueError(message)


def _matched_license_files(root: Path, patterns: list[str]) -> tuple[str, ...]:
    matched: set[str] = set()
    for pattern in patterns:
        path = PurePosixPath(pattern)
        if path.is_absolute() or ".." in path.parts:
            message = f"project license-files pattern is unsafe: {pattern!r}"
            raise ValueError(message)
        for candidate in root.glob(pattern):
            if candidate.is_file():
                matched.add(candidate.relative_to(root).as_posix())
    return tuple(sorted(matched))


def _add_entry_point_group(
    groups: dict[str, dict[str, str]],
    group: str,
    value: object,
) -> None:
    entries = _mapping(value, label=f"project entry-point group {group}")
    if not entries:
        return
    if group in groups:
        message = f"duplicate project entry-point group: {group}"
        raise ValueError(message)
    parsed: dict[str, str] = {}
    for name, target in entries.items():
        parsed[name] = _text(target, label=f"project entry point {group}.{name}")
    groups[group] = parsed


def _requirement_identity(
    value: str,
    *,
    extra: str | None = None,
) -> RequirementIdentity:
    try:
        requirement = Requirement(value)
        if extra is not None:
            marker = f'extra == "{extra}"'
            if requirement.marker is not None:
                marker = f"({requirement.marker}) and {marker}"
            requirement.marker = Marker(marker)
    except (InvalidRequirement, ValueError) as error:
        message = f"invalid project requirement: {value!r}"
        raise ValueError(message) from error
    return (
        canonicalize_name(requirement.name),
        tuple(sorted(requirement.extras)),
        str(requirement.specifier),
        requirement.url,
        None if requirement.marker is None else str(requirement.marker),
    )


def _validate_wheel(
    wheel: Path,
    *,
    source_root: Path,
    project: _ProjectMetadata,
    version: str,
) -> _WheelMetadata:
    source_payload = _runtime_source_payload(source_root)
    expected_dist_info = f"dolphinscheduler_cli-{version}.dist-info"
    expected_members = {
        "metadata": f"{expected_dist_info}/METADATA",
        "wheel": f"{expected_dist_info}/WHEEL",
        "entry_points": f"{expected_dist_info}/entry_points.txt",
        "top_level": f"{expected_dist_info}/top_level.txt",
        "license": f"{expected_dist_info}/licenses/LICENSE",
        "record": f"{expected_dist_info}/RECORD",
    }
    manifest_name = CURRENT_RELEASE_EXACT_GATE_POLICY.wheel_manifest_path
    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
        if len(names) != len(set(names)):
            message = "release wheel contains duplicate archive members"
            raise ValueError(message)
        expected_files = {
            *(f"dsctl/{name}" for name in source_payload),
            *expected_members.values(),
        }
        actual_files = {name for name in names if not name.endswith("/")}
        allowed_directories = {
            f"{parent}/" for name in expected_files for parent in _parent_paths(name)
        }
        actual_directories = {name for name in names if name.endswith("/")}
        if (
            actual_files != expected_files
            or not actual_directories <= allowed_directories
        ):
            missing = sorted(expected_files - actual_files)
            extra = sorted(
                (actual_files - expected_files)
                | (actual_directories - allowed_directories)
            )
            message = (
                "release wheel member set differs from the fail-closed package: "
                f"missing={missing!r}, extra={extra!r}"
            )
            raise ValueError(message)
        wheel_payload = {
            name.removeprefix("dsctl/"): _read_wheel_member(archive, name)
            for name in names
            if name.startswith("dsctl/") and not name.endswith("/")
        }
        missing = sorted(
            name
            for name in [*expected_members.values(), manifest_name]
            if name not in names
        )
        if missing:
            message = f"release wheel lacks required metadata files: {missing!r}"
            raise ValueError(message)
        metadata = _read_wheel_member(archive, expected_members["metadata"])
        wheel_descriptor = _read_wheel_member(archive, expected_members["wheel"])
        entry_points = _read_wheel_member(archive, expected_members["entry_points"])
        top_level = _read_wheel_member(archive, expected_members["top_level"])
        license_text = _read_wheel_member(archive, expected_members["license"])
        record = _read_wheel_member(archive, expected_members["record"])
        contract = _contract_from_manifest(_read_wheel_member(archive, manifest_name))

    _compare_runtime_payload(source_payload, wheel_payload)
    _validate_core_metadata(metadata, project=project, version=version)
    _validate_wheel_descriptor(wheel_descriptor)
    _validate_entry_points(entry_points, expected=project.entry_points)
    expected_top_level = f"{source_root.name}\n".encode()
    if top_level != expected_top_level:
        message = "release wheel top_level.txt differs from the source package"
        raise ValueError(message)
    if license_text != project.license:
        message = "release wheel bundled license differs from the source license"
        raise ValueError(message)
    _validate_wheel_record(
        record,
        payload={
            **{f"dsctl/{name}": content for name, content in wheel_payload.items()},
            expected_members["metadata"]: metadata,
            expected_members["wheel"]: wheel_descriptor,
            expected_members["entry_points"]: entry_points,
            expected_members["top_level"]: top_level,
            expected_members["license"]: license_text,
        },
        record_name=expected_members["record"],
    )
    return _WheelMetadata(
        metadata=metadata,
        entry_points=entry_points,
        top_level=top_level,
        contract=contract,
    )


def _read_wheel_member(archive: zipfile.ZipFile, name: str) -> bytes:
    try:
        return archive.read(name)
    except (NotImplementedError, RuntimeError) as error:
        message = f"release wheel member cannot be read: {name}: {error}"
        raise ValueError(message) from error


def _parent_paths(name: str) -> tuple[str, ...]:
    path = PurePosixPath(name)
    return tuple(
        PurePosixPath(*path.parts[:length]).as_posix()
        for length in range(1, len(path.parts))
    )


def _runtime_source_payload(source_root: Path) -> dict[str, bytes]:
    payload = {
        path.relative_to(source_root).as_posix(): path.read_bytes()
        for path in source_root.rglob("*")
        if _is_controlled_file(path)
    }
    if not payload:
        message = f"runtime source package is empty: {source_root}"
        raise ValueError(message)
    return payload


def _compare_runtime_payload(
    source_payload: Mapping[str, bytes],
    wheel_payload: Mapping[str, bytes],
) -> None:
    if source_payload.keys() != wheel_payload.keys():
        missing = sorted(source_payload.keys() - wheel_payload.keys())
        extra = sorted(wheel_payload.keys() - source_payload.keys())
        message = (
            "release wheel runtime file set differs from source: "
            f"missing={missing!r}, extra={extra!r}"
        )
        raise ValueError(message)
    changed = sorted(
        name
        for name, content in source_payload.items()
        if wheel_payload[name] != content
    )
    if changed:
        message = f"release wheel runtime content differs from source: {changed!r}"
        raise ValueError(message)


def _validate_core_metadata(
    raw: bytes,
    *,
    project: _ProjectMetadata,
    version: str,
) -> None:
    metadata = _parse_core_metadata(raw)
    name = _required_metadata_value(metadata, "Name")
    if canonicalize_name(name) != canonicalize_name(project.name):
        error = "release wheel Name metadata differs from pyproject"
        raise ValueError(error)
    try:
        wheel_version = Version(_required_metadata_value(metadata, "Version"))
        source_version = Version(version)
    except InvalidVersion as error:
        text = "release wheel or source version is invalid"
        raise ValueError(text) from error
    if wheel_version != source_version or project.version != version:
        message_text = "release wheel Version metadata differs from pyproject/tag"
        raise ValueError(message_text)
    try:
        wheel_python = SpecifierSet(
            _required_metadata_value(metadata, "Requires-Python")
        )
        source_python = SpecifierSet(project.requires_python)
    except InvalidSpecifier as error:
        text = "release wheel or source Requires-Python is invalid"
        raise ValueError(text) from error
    if wheel_python != source_python:
        message_text = "release wheel Requires-Python metadata differs from pyproject"
        raise ValueError(message_text)

    metadata_requirements = tuple(
        _requirement_identity(value)
        for value in _metadata_values(metadata, "Requires-Dist")
    )
    if Counter(metadata_requirements) != Counter(project.requirements):
        message_text = "release wheel Requires-Dist metadata differs from pyproject"
        raise ValueError(message_text)

    _validate_project_metadata_fields(metadata, project=project)
    # compat32's text payload replaces non-ASCII UTF-8 bytes. The decoded
    # payload preserves the original distribution bytes for this comparison.
    description = metadata.get_payload(decode=True)
    if not isinstance(description, bytes):
        message_text = "release wheel metadata description must be bytes"
        raise TypeError(message_text)
    if description != project.readme:
        message_text = "release wheel description metadata differs from project readme"
        raise ValueError(message_text)


def _parse_core_metadata(raw: bytes) -> Message:
    metadata = BytesParser(policy=policy.compat32).parsebytes(raw)
    if metadata.defects:
        message = f"release wheel METADATA has parser defects: {metadata.defects!r}"
        raise ValueError(message)
    unknown_headers = sorted(
        {name for name in metadata if name.lower() not in _CORE_HEADERS_BY_LOWER}
    )
    if unknown_headers:
        message = f"release wheel METADATA has unknown fields: {unknown_headers!r}"
        raise ValueError(message)
    metadata_version = _required_metadata_value(metadata, "Metadata-Version")
    for field in ("Import-Name", "Import-Namespace"):
        if _metadata_values(metadata, field):
            message = f"release wheel {field} is not declared by pyproject"
            raise ValueError(message)
    if metadata_version != _EXPECTED_METADATA_VERSION:
        message = (
            "release wheel Metadata-Version differs from the current build: "
            f"expected {_EXPECTED_METADATA_VERSION!r}, got {metadata_version!r}"
        )
        raise ValueError(message)
    return metadata


def _validate_project_metadata_fields(
    metadata: Message,
    *,
    project: _ProjectMetadata,
) -> None:
    raw_license_expression = _optional_metadata_value(
        metadata,
        "License-Expression",
    )
    singular_fields = (
        ("Summary", _optional_metadata_value(metadata, "Summary"), project.summary),
        ("Description", _optional_metadata_value(metadata, "Description"), None),
        ("Author", _optional_metadata_value(metadata, "Author"), project.author),
        (
            "Author-email",
            _optional_metadata_value(metadata, "Author-email"),
            project.author_email,
        ),
        (
            "Maintainer",
            _optional_metadata_value(metadata, "Maintainer"),
            project.maintainer,
        ),
        (
            "Maintainer-email",
            _optional_metadata_value(metadata, "Maintainer-email"),
            project.maintainer_email,
        ),
        (
            "License-Expression",
            None
            if raw_license_expression is None
            else _spdx_token_sequence(raw_license_expression),
            project.license_expression,
        ),
        ("License", _optional_metadata_value(metadata, "License"), None),
        (
            "Description-Content-Type",
            _optional_metadata_value(metadata, "Description-Content-Type"),
            project.readme_content_type,
        ),
        ("Project-URL", _metadata_project_urls(metadata), project.project_urls),
        ("Home-page", _optional_metadata_value(metadata, "Home-page"), None),
        ("Download-URL", _optional_metadata_value(metadata, "Download-URL"), None),
    )
    for field, actual, expected in singular_fields:
        if actual != expected:
            message = f"release wheel {field} metadata differs from pyproject"
            raise ValueError(message)

    repeated_fields = (
        (
            "License-File",
            _metadata_values(metadata, "License-File"),
            project.license_files,
        ),
        ("Keywords", _metadata_keywords(metadata), project.keywords),
        ("Classifier", _metadata_values(metadata, "Classifier"), project.classifiers),
        (
            "Provides-Extra",
            tuple(
                canonicalize_name(value)
                for value in _metadata_values(metadata, "Provides-Extra")
            ),
            project.extras,
        ),
        (
            "Dynamic",
            tuple(value.lower() for value in _metadata_values(metadata, "Dynamic")),
            project.dynamic,
        ),
        ("Platform", _metadata_values(metadata, "Platform"), ()),
        ("Supported-Platform", _metadata_values(metadata, "Supported-Platform"), ()),
        ("Requires-External", _metadata_values(metadata, "Requires-External"), ()),
        ("Provides-Dist", _metadata_values(metadata, "Provides-Dist"), ()),
        ("Obsoletes-Dist", _metadata_values(metadata, "Obsoletes-Dist"), ()),
        ("Import-Name", _metadata_values(metadata, "Import-Name"), ()),
        ("Import-Namespace", _metadata_values(metadata, "Import-Namespace"), ()),
        ("Requires", _metadata_values(metadata, "Requires"), ()),
        ("Provides", _metadata_values(metadata, "Provides"), ()),
        ("Obsoletes", _metadata_values(metadata, "Obsoletes"), ()),
    )
    for repeated_field, repeated_actual, repeated_expected in repeated_fields:
        if Counter(repeated_actual) != Counter(repeated_expected):
            message = f"release wheel {repeated_field} metadata differs from pyproject"
            raise ValueError(message)


def _required_metadata_value(metadata: Message, field: str) -> str:
    value = _optional_metadata_value(metadata, field)
    if value is None:
        message = f"release wheel METADATA lacks required field {field}"
        raise ValueError(message)
    return value


def _optional_metadata_value(metadata: Message, field: str) -> str | None:
    values = metadata.get_all(field, [])
    if len(values) > 1:
        message = f"release wheel METADATA repeats singular field {field}"
        raise ValueError(message)
    if not values:
        return None
    return _text(values[0], label=f"wheel metadata {field}")


def _metadata_values(metadata: Message, field: str) -> tuple[str, ...]:
    return tuple(
        _text(value, label=f"wheel metadata {field}")
        for value in metadata.get_all(field, [])
    )


def _metadata_keywords(metadata: Message) -> tuple[str, ...]:
    value = _optional_metadata_value(metadata, "Keywords")
    if value is None:
        return ()
    keywords = tuple(part.strip() for part in value.split(","))
    if any(not keyword for keyword in keywords):
        message = "release wheel Keywords metadata contains an empty value"
        raise ValueError(message)
    return keywords


def _metadata_project_urls(metadata: Message) -> dict[str, str]:
    urls: dict[str, str] = {}
    for value in _metadata_values(metadata, "Project-URL"):
        label, separator, raw_url = value.partition(",")
        if not separator:
            message = f"release wheel Project-URL metadata is invalid: {value!r}"
            raise ValueError(message)
        label = _text(label, label="wheel metadata Project-URL label")
        url = _text(raw_url, label=f"wheel metadata Project-URL {label}")
        if label in urls:
            message = f"release wheel Project-URL repeats label {label!r}"
            raise ValueError(message)
        urls[label] = url
    return urls


def _header(message: object, name: str) -> str:
    if not hasattr(message, "get"):
        error = "release wheel METADATA is not an email-style document"
        raise TypeError(error)
    value = message.get(name)
    return _text(value, label=f"wheel metadata {name}")


def _validate_wheel_descriptor(raw: bytes) -> None:
    descriptor = BytesParser(policy=policy.default).parsebytes(raw)
    if _header(descriptor, "Wheel-Version") != "1.0":
        message = "release wheel has an unsupported Wheel-Version"
        raise ValueError(message)
    if _header(descriptor, "Root-Is-Purelib").lower() != "true":
        message = "release wheel must install as pure Python"
        raise ValueError(message)
    if descriptor.get_all("Tag", []) != ["py3-none-any"]:
        message = "release wheel compatibility tag differs from its filename"
        raise ValueError(message)


def _validate_wheel_record(
    raw: bytes,
    *,
    payload: Mapping[str, bytes],
    record_name: str,
) -> None:
    try:
        rows = list(csv.reader(io.StringIO(raw.decode("utf-8"), newline="")))
    except (UnicodeDecodeError, csv.Error) as error:
        message = "release wheel RECORD is invalid"
        raise ValueError(message) from error
    if not all(len(row) == 3 for row in rows):
        message = "release wheel RECORD rows must have three fields"
        raise ValueError(message)
    by_name = {row[0]: row[1:] for row in rows}
    expected_names = {*payload, record_name}
    if len(by_name) != len(rows) or set(by_name) != expected_names:
        message = "release wheel RECORD file set differs from archive members"
        raise ValueError(message)
    if by_name[record_name] != ["", ""]:
        message = "release wheel RECORD must leave its own hash and size empty"
        raise ValueError(message)
    for name, content in payload.items():
        digest = base64.urlsafe_b64encode(hashlib.sha256(content).digest())
        expected_digest = f"sha256={digest.rstrip(b'=').decode()}"
        if by_name[name] != [expected_digest, str(len(content))]:
            message = f"release wheel RECORD digest or size differs for {name}"
            raise ValueError(message)


def _validate_entry_points(
    raw: bytes,
    *,
    expected: Mapping[str, Mapping[str, str]],
) -> None:
    parser = _CaseSensitiveConfigParser(interpolation=None)
    try:
        parser.read_string(raw.decode())
    except (UnicodeDecodeError, configparser.Error) as error:
        message = "release wheel entry_points.txt is invalid"
        raise ValueError(message) from error
    actual = {section: dict(parser.items(section)) for section in parser.sections()}
    if actual != expected:
        message = "release wheel console scripts/entry points differ from pyproject"
        raise ValueError(message)


def _contract_from_manifest(raw: bytes) -> dict[str, object]:
    try:
        tree = ast.parse(raw.decode("utf-8"))
    except (SyntaxError, UnicodeDecodeError) as error:
        message = (
            "release wheel exact "
            f"{CURRENT_RELEASE_EXACT_GATE_POLICY.ds_version} manifest is invalid"
        )
        raise ValueError(message) from error
    values: dict[str, object] = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if not isinstance(target, ast.Name) or target.id not in _CONTRACT_FIELDS:
            continue
        try:
            values[_CONTRACT_FIELDS[target.id]] = ast.literal_eval(node.value)
        except (ValueError, TypeError) as error:
            message = f"release wheel contract field {target.id} is not literal"
            raise ValueError(message) from error
    if set(values) != set(_CONTRACT_FIELDS.values()):
        missing = sorted(set(_CONTRACT_FIELDS.values()) - values.keys())
        message = f"release wheel contract manifest lacks fields: {missing!r}"
        raise ValueError(message)
    semantic = values["semantic_operations"]
    if not isinstance(semantic, tuple) or not all(
        isinstance(item, str) for item in semantic
    ):
        message = "release wheel contract semantic operations must be a tuple"
        raise ValueError(message)
    values["semantic_operations"] = list(semantic)
    return values


def _validate_sdist(
    sdist: Path,
    *,
    root: Path,
    project: _ProjectMetadata,
    version: str,
    wheel: _WheelMetadata,
) -> None:
    expected_controlled = _controlled_source_payload(root)
    actual = _read_sdist_payload(sdist, version=version)
    expected_names = set(expected_controlled) | _GENERATED_SDIST_FILES
    if set(actual) != expected_names:
        missing = sorted(expected_names - actual.keys())
        extra = sorted(actual.keys() - expected_names)
        message = (
            "release sdist file set differs from controlled source: "
            f"missing={missing!r}, extra={extra!r}"
        )
        raise ValueError(message)
    changed = sorted(
        name for name, content in expected_controlled.items() if actual[name] != content
    )
    if changed:
        message = f"release sdist controlled content differs from source: {changed!r}"
        raise ValueError(message)

    if (
        actual["PKG-INFO"] != wheel.metadata
        or actual[f"{_EGG_INFO}/PKG-INFO"] != wheel.metadata
    ):
        message = "release sdist package metadata differs from the wheel"
        raise ValueError(message)
    if actual[f"{_EGG_INFO}/entry_points.txt"] != wheel.entry_points:
        message = "release sdist entry points differ from the wheel"
        raise ValueError(message)
    if actual[f"{_EGG_INFO}/top_level.txt"] != wheel.top_level:
        message = "release sdist top-level package differs from the wheel"
        raise ValueError(message)
    if actual["setup.cfg"] != _SETUP_CFG:
        message = "release sdist contains unexpected generated setup.cfg"
        raise ValueError(message)
    if actual[f"{_EGG_INFO}/dependency_links.txt"] != b"\n":
        message = "release sdist contains unexpected dependency links"
        raise ValueError(message)
    sdist_requirements = _requirements_from_egg_info(
        actual[f"{_EGG_INFO}/requires.txt"]
    )
    if Counter(sdist_requirements) != Counter(project.requirements):
        message = "release sdist dependency metadata differs from pyproject"
        raise ValueError(message)
    _validate_sources_manifest(
        actual[f"{_EGG_INFO}/SOURCES.txt"],
        expected=set(expected_controlled) | _SOURCES_GENERATED_FILES,
    )


def _requirements_from_egg_info(raw: bytes) -> tuple[RequirementIdentity, ...]:
    try:
        lines = raw.decode("utf-8").splitlines()
    except UnicodeDecodeError as error:
        message = "release sdist requires.txt is not UTF-8"
        raise ValueError(message) from error
    requirements: list[RequirementIdentity] = []
    extra: str | None = None
    marker: str | None = None
    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("[") and line.endswith("]"):
            section = line[1:-1]
            extra, separator, marker_value = section.partition(":")
            extra = extra or None
            marker = marker_value if separator else None
            continue
        requirement = line if marker is None else f"{line}; {marker}"
        requirements.append(_requirement_identity(requirement, extra=extra))
    return tuple(requirements)


def _controlled_source_payload(root: Path) -> dict[str, bytes]:
    payload: dict[str, bytes] = {}
    for relative in _ROOT_SOURCE_FILES:
        path = root / relative
        if not path.is_file():
            message = f"controlled release source is missing: {relative}"
            raise FileNotFoundError(message)
        payload[relative] = path.read_bytes()
    _add_controlled_files(payload, root, "src/dsctl", suffixes=None)
    _add_controlled_files(payload, root, "docs", suffixes={".md"})
    _add_controlled_files(
        payload,
        root,
        "docs/development/live-evidence",
        suffixes={".json"},
    )
    _add_controlled_files(payload, root, "tests", suffixes={".py"})
    _add_controlled_files(payload, root, "tests/fixtures", suffixes={".json"})
    _add_controlled_files(
        payload, root, "tests/compatibility/corpus", suffixes={".json"}
    )
    test_readme = "tests/fixtures/version_discovery/README.md"
    readme_path = root / test_readme
    if not readme_path.is_file():
        message = f"controlled release source is missing: {test_readme}"
        raise FileNotFoundError(message)
    payload[test_readme] = readme_path.read_bytes()
    _add_controlled_files(payload, root, "tools", suffixes={".py", ".json", ".txt"})
    return payload


def _add_controlled_files(
    payload: dict[str, bytes],
    root: Path,
    relative_root: str,
    *,
    suffixes: set[str] | None,
) -> None:
    directory = root / relative_root
    if not directory.is_dir():
        message = f"controlled release source directory is missing: {relative_root}"
        raise FileNotFoundError(message)
    for path in directory.rglob("*"):
        if not _is_controlled_file(path):
            continue
        if suffixes is not None and path.suffix not in suffixes:
            continue
        payload[path.relative_to(root).as_posix()] = path.read_bytes()


def _is_controlled_file(path: Path) -> bool:
    basename = path.name.casefold()
    return (
        path.is_file()
        and not COMMON_FORBIDDEN_ANY_SEGMENTS.intersection(
            part.casefold() for part in path.parts
        )
        and basename not in COMMON_FORBIDDEN_BASENAMES
        and not basename.startswith((".env.", ".envrc."))
        and path.suffix not in {".pyc", ".pyo"}
    )


def _read_sdist_payload(sdist: Path, *, version: str) -> dict[str, bytes]:
    prefix = f"dolphinscheduler_cli-{version}"
    payload: dict[str, bytes] = {}
    with tarfile.open(sdist, mode="r:gz") as archive:
        for member in archive.getmembers():
            path = PurePosixPath(member.name)
            if path.is_absolute() or ".." in path.parts:
                message = f"release sdist has unsafe member path: {member.name!r}"
                raise ValueError(message)
            if not path.parts or path.parts[0] != prefix:
                message = f"release sdist member is outside {prefix!r}: {member.name!r}"
                raise ValueError(message)
            if member.isdir():
                continue
            if not member.isfile() or len(path.parts) < 2:
                message = f"release sdist member is not a regular file: {member.name!r}"
                raise ValueError(message)
            relative = PurePosixPath(*path.parts[1:]).as_posix()
            if relative in payload:
                message = f"release sdist contains duplicate member: {relative}"
                raise ValueError(message)
            stream = archive.extractfile(member)
            if stream is None:
                message = f"release sdist member cannot be read: {relative}"
                raise ValueError(message)
            payload[relative] = stream.read()
    return payload


def _validate_sources_manifest(raw: bytes, *, expected: set[str]) -> None:
    try:
        values = raw.decode("utf-8").splitlines()
    except UnicodeDecodeError as error:
        message = "release sdist SOURCES.txt is not UTF-8"
        raise ValueError(message) from error
    if len(values) != len(set(values)) or set(values) != expected:
        message = "release sdist SOURCES.txt differs from its controlled file set"
        raise ValueError(message)


def _validate_index_payload(
    payload: object,
    *,
    version: str,
    sha256_by_filename: dict[str, str],
) -> None:
    index = _mapping(payload, label="package index response")
    info = _mapping(index.get("info"), label="package index info")
    if info.get("version") != version:
        message = "package index version does not match release version"
        raise ValueError(message)
    urls = index.get("urls")
    if not isinstance(urls, list):
        message = "package index response has no urls list"
        raise TypeError(message)
    digests_by_filename: dict[str, str] = {}
    for position, raw_file in enumerate(urls):
        file = _mapping(raw_file, label=f"package index file {position}")
        filename = file.get("filename")
        if not isinstance(filename, str) or not filename:
            message = f"package index file {position} has no filename"
            raise ValueError(message)
        if filename in digests_by_filename:
            message = f"package index contains duplicate filename: {filename}"
            raise ValueError(message)
        expected_package_type = "bdist_wheel" if filename.endswith(".whl") else "sdist"
        if file.get("packagetype") != expected_package_type:
            message = f"package index file {filename} has unexpected package type"
            raise ValueError(message)
        digests = _mapping(
            file.get("digests"),
            label=f"package index {filename} digests",
        )
        sha256 = digests.get("sha256")
        if not isinstance(sha256, str):
            message = f"package index file {filename} has no SHA-256 digest"
            raise TypeError(message)
        if file.get("yanked") is not False:
            message = f"package index file {filename} is yanked or lacks yanked=false"
            raise ValueError(message)
        digests_by_filename[filename] = sha256
    if digests_by_filename != sha256_by_filename:
        message = (
            "package index artifacts do not match the canonical release bytes: "
            f"expected {sha256_by_filename!r}, got {digests_by_filename!r}"
        )
        raise ValueError(message)


def _mapping(value: object, *, label: str) -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        message = f"{label} must be an object with string keys"
        raise TypeError(message)
    return value


def _text(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        message = f"{label} must be non-empty text"
        raise TypeError(message)
    return value.strip()


def _string_list(value: object, *, label: str) -> list[str]:
    if not isinstance(value, list) or not all(
        isinstance(item, str) and item for item in value
    ):
        message = f"{label} must be a string array"
        raise TypeError(message)
    return value


def _sha256(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _load_index_payload(path: Path | None) -> object | None:
    if path is None:
        return None
    payload: object = json.loads(path.read_text(encoding="utf-8"))
    return payload


def _wheel_only_artifact(
    artifacts: list[Path],
    *,
    index_json: Path | None,
    evidence_dir: Path | None,
    conformance_evidence_root: Path | None,
) -> Path:
    if len(artifacts) != 1:
        message = "wheel-only preflight requires exactly one artifact"
        raise ValueError(message)
    if index_json is not None:
        message = "wheel-only preflight does not accept --index-json"
        raise ValueError(message)
    if evidence_dir is not None:
        message = "wheel-only preflight does not accept --evidence-dir"
        raise ValueError(message)
    if conformance_evidence_root is not None:
        message = "wheel-only preflight does not accept --conformance-evidence-root"
        raise ValueError(message)
    return artifacts[0]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Preflight one canonical wheel or verify one build-once release "
            "artifact set against exact-profile live evidence and optional "
            "package-index metadata."
        )
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--tag")
    mode.add_argument("--wheel-only", action="store_true")
    parser.add_argument("--index-json", type=Path)
    parser.add_argument("--evidence-dir", type=Path)
    parser.add_argument("--conformance-evidence-root", type=Path)
    parser.add_argument("--root", type=Path, default=ROOT, help=argparse.SUPPRESS)
    parser.add_argument("artifacts", nargs="+", type=Path)
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        if args.wheel_only:
            artifact = _wheel_only_artifact(
                args.artifacts,
                index_json=args.index_json,
                evidence_dir=args.evidence_dir,
                conformance_evidence_root=args.conformance_evidence_root,
            )
            wheel = check_wheel_artifact(args.root, artifact)
            print(
                "wheel artifact preflight passed: "
                f"{wheel.version}; {wheel.wheel.name}=sha256:{wheel.sha256}"
            )
            return 0
        checked = check_release_artifacts(
            args.root,
            args.artifacts,
            tag=args.tag,
            evidence_dir=args.evidence_dir or DEFAULT_EVIDENCE_DIR,
            conformance_evidence_root=args.conformance_evidence_root,
            index_payload=_load_index_payload(args.index_json),
        )
    except (
        OSError,
        TypeError,
        ValueError,
        json.JSONDecodeError,
        tarfile.TarError,
        zipfile.BadZipFile,
        zipfile.LargeZipFile,
    ) as error:
        print(f"release artifact check failed: {error}", file=sys.stderr)
        return 1
    digests = ", ".join(
        f"{name}=sha256:{digest}"
        for name, digest in sorted(checked.sha256_by_filename.items())
    )
    print(f"release artifact check passed: {checked.version}; {digests}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
