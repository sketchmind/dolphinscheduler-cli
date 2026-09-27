"""Shared contract inputs for multi-version code-generation tools."""

from __future__ import annotations

import hashlib
import json
import re
import shutil
import subprocess
from dataclasses import dataclass
from functools import cache
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as package_version
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from ds_codegen.extract import build_contract_snapshot
from ds_codegen.ir import (
    ContractSnapshot,
    DtoFieldSpec,
    DtoSpec,
    EnumFieldSpec,
    EnumSpec,
    EnumValueSpec,
    ModelSpec,
    OperationSpec,
    ParameterSpec,
)
from ds_codegen.snapshot_resolution import SnapshotTypeResolver
from ds_codegen.source import codegen_repo_root_for_ds_source

if TYPE_CHECKING:
    from collections.abc import Mapping

ContractInputKind = Literal["snapshot", "ds-source"]
_DS_VERSION_LABEL = re.compile(r"\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?\Z")
PROVENANCE_SCHEMA_VERSION = 1
JsonObject = dict[str, Any]
_IGNORED_TREE_PARTS = frozenset({".git", "build", "dist", "node_modules", "target"})
_EXTRACTOR_ROOT = Path(__file__).resolve().parent
_EXTRACTOR_CORE_INPUTS = (
    _EXTRACTOR_ROOT / "contract_inputs.py",
    _EXTRACTOR_ROOT / "contract_type_refs.py",
    _EXTRACTOR_ROOT / "contract_visibility.py",
    _EXTRACTOR_ROOT / "ir.py",
    _EXTRACTOR_ROOT / "java_literals.py",
    _EXTRACTOR_ROOT / "java_source.py",
    _EXTRACTOR_ROOT / "snapshot_resolution.py",
    _EXTRACTOR_ROOT / "source.py",
)


@dataclass(frozen=True)
class ContractInput:
    """One labeled source of a DolphinScheduler contract snapshot."""

    label: str
    path: Path
    kind: ContractInputKind


@dataclass(frozen=True)
class LoadedContract:
    """One loaded contract plus auditable input provenance."""

    label: str
    snapshot: ContractSnapshot
    provenance: JsonObject


@dataclass(frozen=True)
class ExactGitSourceIdentity:
    """Lightweight identity of one clean exact-tag source worktree."""

    tag: str
    commit: str
    tree: str


class ExactProvenanceError(ValueError):
    """Raised when an input cannot support an exact-version claim."""

    def __init__(self, message: str, *, provenance: JsonObject) -> None:
        super().__init__(message)
        self.provenance = provenance


def natural_version_key(version: str) -> tuple[tuple[int, int | str], ...]:
    """Return a natural ordering key for version-like labels."""
    parts = re.findall(r"\d+|\D+", version)
    return tuple(
        (0, int(part)) if part.isdigit() else (1, part.casefold()) for part in parts
    )


def parse_contract_inputs(
    *,
    snapshot_values: list[str],
    source_values: list[str],
    minimum_count: int,
    require_version_labels: bool,
) -> list[ContractInput]:
    """Parse and validate shared ``LABEL=PATH`` contract inputs."""
    inputs = [
        ContractInput(label, path, "snapshot")
        for label, path in (
            _parse_labeled_path(value, require_version=require_version_labels)
            for value in snapshot_values
        )
    ]
    inputs.extend(
        ContractInput(label, path, "ds-source")
        for label, path in (
            _parse_labeled_path(value, require_version=require_version_labels)
            for value in source_values
        )
    )
    if len(inputs) < minimum_count:
        if minimum_count == 1:
            message = "at least one contract input is required"
        elif minimum_count == 2:
            message = "at least two contract inputs are required"
        else:
            message = f"at least {minimum_count} contract inputs are required"
        raise ValueError(message)
    labels = [item.label for item in inputs]
    duplicates = sorted(
        {label for label in labels if labels.count(label) > 1},
        key=natural_version_key,
    )
    if duplicates:
        message = f"duplicate input labels: {', '.join(duplicates)}"
        raise ValueError(message)
    return inputs


def contract_snapshot_digest(snapshot: ContractSnapshot) -> str:
    """Return a canonical digest of one extracted wire contract."""
    return canonical_json_digest(snapshot.to_json_dict())


def canonical_json_digest(payload: object) -> str:
    """Return a stable SHA-256 digest for one JSON-compatible payload."""
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


@cache
def contract_extractor_fingerprint() -> str:
    """Identify the code and parser version that materialize contract snapshots."""
    paths = (
        *_EXTRACTOR_CORE_INPUTS,
        *sorted((_EXTRACTOR_ROOT / "extract").glob("*.py")),
    )
    inputs = [
        {
            "path": path.relative_to(_EXTRACTOR_ROOT).as_posix(),
            "digest": f"sha256:{hashlib.sha256(path.read_bytes()).hexdigest()}",
        }
        for path in paths
    ]
    try:
        javalang_version = package_version("javalang")
    except PackageNotFoundError:  # pragma: no cover - extractor cannot run either
        javalang_version = "missing"
    return canonical_json_digest(
        {
            "schema_version": 1,
            "inputs": inputs,
            "javalang_version": javalang_version,
        }
    )


def build_source_provenance(
    source_root: Path,
    *,
    label: str,
    snapshot: ContractSnapshot,
) -> JsonObject:
    """Describe the exact source state used to extract one contract."""
    return build_source_contract_provenance(
        source_root,
        label=label,
        ds_version=snapshot.ds_version,
        contract_digest=contract_snapshot_digest(snapshot),
    )


def build_source_contract_provenance(
    source_root: Path,
    *,
    label: str,
    ds_version: str,
    contract_digest: str,
) -> JsonObject:
    """Describe source provenance for any generated DS contract surface."""
    resolved_root = source_root.resolve()
    git = _git_metadata(resolved_root, label=label)
    reason: str | None
    if git is None:
        content_digest = _directory_tree_digest(resolved_root)
        origin: JsonObject = {
            "kind": "ds-source",
            "content_digest": content_digest,
        }
        exact = False
        reason = "source is not a verifiable Git worktree"
    else:
        content_digest = (
            f"git-tree:{git['tree']}"
            if git["dirty"] is False
            else _directory_tree_digest(resolved_root)
        )
        origin = {
            "kind": "ds-source",
            "content_digest": content_digest,
            "git": git,
        }
        exact = (
            git["dirty"] is False and git.get("tag") == label and ds_version == label
        )
        reason = (
            None
            if exact
            else "source is not a clean exact Git tag matching the input label"
        )
    provenance: JsonObject = {
        "schema_version": PROVENANCE_SCHEMA_VERSION,
        "exact": exact,
        "input_kind": "ds-source",
        "content_digest": content_digest,
        "contract_digest": contract_digest,
        "extractor_fingerprint": contract_extractor_fingerprint(),
        "origin": origin,
    }
    if reason is not None:
        provenance["exact_reason"] = reason
    return provenance


def require_exact_provenance(
    provenance: Mapping[str, object],
    *,
    label: str,
) -> None:
    """Reject provenance that cannot substantiate one exact-version label."""
    if _is_exact_provenance(provenance, label=label):
        return
    details = dict(provenance)
    reason = provenance.get("exact_reason")
    suffix = f": {reason}" if isinstance(reason, str) and reason else ""
    message = f"contract input {label!r} is not a clean exact Git tag{suffix}"
    raise ExactProvenanceError(message, provenance=details)


def require_exact_git_source_identity(
    source_root: Path,
    *,
    label: str,
) -> ExactGitSourceIdentity:
    """Read and validate exact source identity without extracting a contract."""
    resolved_root = source_root.resolve()
    if not resolved_root.is_dir():
        message = (
            f"contract input {label!r} source root does not exist or is not a "
            f"directory: {resolved_root}"
        )
        raise ValueError(message)
    git = _git_metadata(resolved_root, label=label)
    if git is None:
        message = (
            f"contract input {label!r} at {resolved_root} is not a verifiable "
            "Git worktree"
        )
        raise ValueError(message)
    if (
        git.get("dirty") is not False
        or git.get("tag") != label
        or git.get("ref") != f"refs/tags/{label}"
    ):
        message = (
            f"contract input {label!r} at {resolved_root} is not a clean exact "
            "Git tag matching the input label"
        )
        raise ValueError(message)
    commit = git.get("commit")
    tree = git.get("tree")
    if (
        not isinstance(commit, str)
        or not _is_git_object_id(commit)
        or not isinstance(tree, str)
        or not _is_git_object_id(tree)
    ):
        message = (
            f"contract input {label!r} at {resolved_root} has an invalid Git "
            "commit or tree identity"
        )
        raise ValueError(message)
    return ExactGitSourceIdentity(
        tag=label,
        commit=commit,
        tree=tree,
    )


def write_contract_snapshot_document(
    snapshot: ContractSnapshot,
    output_path: Path,
    *,
    provenance: Mapping[str, object] | None = None,
) -> None:
    """Persist one cached contract and its optional source provenance."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(
            snapshot_json_document(snapshot, provenance=provenance),
            indent=2,
            ensure_ascii=True,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def load_contract_input(
    item: ContractInput,
    *,
    require_exact: bool,
) -> LoadedContract:
    """Load one source or cached snapshot and preserve its provenance."""
    if item.kind == "ds-source":
        snapshot = build_contract_snapshot_from_source(item.path)
        provenance = build_source_provenance(
            item.path,
            label=item.label,
            snapshot=snapshot,
        )
    else:
        snapshot, provenance = _load_cached_snapshot(item.path)
    if require_exact:
        require_exact_provenance(provenance, label=item.label)
    return LoadedContract(
        label=item.label,
        snapshot=snapshot,
        provenance=provenance,
    )


def build_contract_snapshot_from_source(source_root: Path) -> ContractSnapshot:
    """Extract one contract from a checked-out DolphinScheduler source tree."""
    with codegen_repo_root_for_ds_source(source_root) as repo_root:
        return build_contract_snapshot(repo_root)


def load_contract_snapshot(path: Path) -> ContractSnapshot:
    """Load a cached snapshot without making an exact-version claim."""
    payload = _read_snapshot_payload(path)
    return snapshot_from_json(payload)


def snapshot_from_json(payload: JsonObject) -> ContractSnapshot:
    """Deserialize one contract snapshot while ignoring additive metadata."""
    if "reference_resolutions" in payload:
        message = (
            "snapshot contains the retired reference_resolutions side table; "
            "refresh it from exact source"
        )
        raise ValueError(message)
    operations = [
        OperationSpec(
            **{
                **_require_mapping(item, label="operation"),
                "parameters": [
                    ParameterSpec(**_require_mapping(parameter, label="parameter"))
                    for parameter in _require_list(
                        _require_mapping(item, label="operation").get("parameters"),
                        label="operation.parameters",
                    )
                ],
            }
        )
        for item in _require_list(payload.get("operations"), label="operations")
    ]
    enums = [
        EnumSpec(
            **{
                **_require_mapping(item, label="enum"),
                "fields": [
                    EnumFieldSpec(**_require_mapping(field, label="enum field"))
                    for field in _require_list(
                        _require_mapping(item, label="enum").get("fields"),
                        label="enum.fields",
                    )
                ],
                "values": [
                    EnumValueSpec(**_require_mapping(value, label="enum value"))
                    for value in _require_list(
                        _require_mapping(item, label="enum").get("values"),
                        label="enum.values",
                    )
                ],
            }
        )
        for item in _require_list(payload.get("enums"), label="enums")
    ]
    dtos = [
        DtoSpec(
            **{
                **_require_mapping(item, label="dto"),
                "fields": [
                    DtoFieldSpec(**_require_mapping(field, label="dto field"))
                    for field in _require_list(
                        _require_mapping(item, label="dto").get("fields"),
                        label="dto.fields",
                    )
                ],
            }
        )
        for item in _require_list(payload.get("dtos"), label="dtos")
    ]
    models = [
        ModelSpec(
            **{
                **_require_mapping(item, label="model"),
                "fields": [
                    DtoFieldSpec(**_require_mapping(field, label="model field"))
                    for field in _require_list(
                        _require_mapping(item, label="model").get("fields"),
                        label="model.fields",
                    )
                ],
            }
        )
        for item in _require_list(payload.get("models"), label="models")
    ]
    snapshot = ContractSnapshot(
        ds_version=str(payload["ds_version"]),
        operation_count=int(payload["operation_count"]),
        enum_count=int(payload["enum_count"]),
        dto_count=int(payload["dto_count"]),
        model_count=int(payload["model_count"]),
        operations=operations,
        enums=enums,
        dtos=dtos,
        models=models,
    )
    SnapshotTypeResolver.compile(snapshot)
    return snapshot


def snapshot_json_document(
    snapshot: ContractSnapshot,
    *,
    provenance: Mapping[str, object] | None = None,
) -> JsonObject:
    """Return the persisted snapshot document with optional provenance."""
    payload = snapshot.to_json_dict()
    if provenance is not None:
        payload["provenance"] = dict(provenance)
    return payload


def _load_cached_snapshot(path: Path) -> tuple[ContractSnapshot, JsonObject]:
    payload = _read_snapshot_payload(path)
    snapshot = snapshot_from_json(payload)
    digest = contract_snapshot_digest(snapshot)
    provenance = cached_contract_provenance(
        payload.get("provenance"),
        contract_digest=digest,
        path=path,
    )
    return snapshot, provenance


def cached_contract_provenance(
    embedded: object,
    *,
    contract_digest: str,
    path: Path,
) -> JsonObject:
    """Normalize and verify provenance embedded in any cached contract."""
    if not isinstance(embedded, dict):
        return {
            "schema_version": PROVENANCE_SCHEMA_VERSION,
            "exact": False,
            "exact_reason": "cached snapshot has no embedded provenance",
            "input_kind": "snapshot",
            "content_digest": contract_digest,
            "contract_digest": contract_digest,
            "origin": {"kind": "unknown"},
        }
    recorded_digest = embedded.get("contract_digest")
    if recorded_digest != contract_digest:
        message = f"cached snapshot contract digest does not match provenance: {path}"
        raise ValueError(message)
    origin = embedded.get("origin")
    if not isinstance(origin, dict):
        message = f"cached snapshot provenance origin must be an object: {path}"
        raise TypeError(message)
    provenance: JsonObject = {
        "schema_version": PROVENANCE_SCHEMA_VERSION,
        "exact": embedded.get("exact") is True,
        "input_kind": "snapshot",
        "content_digest": contract_digest,
        "contract_digest": contract_digest,
        "origin": origin,
    }
    extractor_fingerprint = embedded.get("extractor_fingerprint")
    if extractor_fingerprint is not None:
        if not _is_digest(extractor_fingerprint, prefix="sha256:"):
            message = f"cached snapshot extractor fingerprint is invalid: {path}"
            raise ValueError(message)
        provenance["extractor_fingerprint"] = extractor_fingerprint
    exact_reason = embedded.get("exact_reason")
    if isinstance(exact_reason, str):
        provenance["exact_reason"] = exact_reason
    return provenance


def _is_exact_provenance(
    provenance: Mapping[str, object],
    *,
    label: str,
) -> bool:
    if provenance.get("schema_version") != PROVENANCE_SCHEMA_VERSION:
        return False
    if provenance.get("exact") is not True:
        return False
    contract_digest = provenance.get("contract_digest")
    if not _is_digest(contract_digest, prefix="sha256:"):
        return False
    origin = provenance.get("origin")
    if not isinstance(origin, dict):
        return False
    input_kind = provenance.get("input_kind")
    input_digest = provenance.get("content_digest")
    if input_kind not in {"snapshot", "ds-source"}:
        return False
    if not isinstance(input_digest, str):
        return False
    git = origin.get("git")
    if origin.get("kind") != "ds-source" or not isinstance(git, dict):
        return False
    commit = git.get("commit")
    tree = git.get("tree")
    if not _is_git_object_id(commit) or not _is_git_object_id(tree):
        return False
    if git.get("dirty") is not False:
        return False
    if git.get("tag") != label or git.get("ref") != f"refs/tags/{label}":
        return False
    source_digest = f"git-tree:{tree}"
    if origin.get("content_digest") != source_digest:
        return False
    if input_kind == "ds-source":
        return input_digest == source_digest
    return input_digest == contract_digest


def _is_digest(value: object, *, prefix: str) -> bool:
    if not isinstance(value, str) or not value.startswith(prefix):
        return False
    digest = value.removeprefix(prefix)
    return len(digest) == 64 and all(char in "0123456789abcdef" for char in digest)


def _is_git_object_id(value: object) -> bool:
    if not isinstance(value, str) or len(value) not in {40, 64}:
        return False
    return all(char in "0123456789abcdef" for char in value)


def _read_snapshot_payload(path: Path) -> JsonObject:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        message = f"Snapshot file must contain a JSON object: {path}"
        raise TypeError(message)
    return payload


def _git_metadata(source_root: Path, *, label: str) -> JsonObject | None:
    git = shutil.which("git")
    if git is None:
        return None
    top_level = _git_output(git, source_root, "rev-parse", "--show-toplevel")
    if top_level is None or Path(top_level).resolve() != source_root:
        return None
    commit = _git_output(git, source_root, "rev-parse", "HEAD^{commit}")
    tree = _git_output(git, source_root, "rev-parse", "HEAD^{tree}")
    if commit is None or tree is None:
        return None
    tags_output = _git_output(git, source_root, "tag", "--points-at", "HEAD") or ""
    tags = {tag.strip() for tag in tags_output.splitlines() if tag.strip()}
    exact_tag = label if label in tags else None
    branch = _git_output(git, source_root, "symbolic-ref", "-q", "HEAD")
    status = _git_output(
        git,
        source_root,
        "status",
        "--porcelain",
        "--untracked-files=normal",
    )
    metadata: JsonObject = {
        "commit": commit,
        "tree": tree,
        "ref": f"refs/tags/{exact_tag}" if exact_tag is not None else branch,
        "dirty": bool(status),
    }
    if exact_tag is not None:
        metadata["tag"] = exact_tag
    return metadata


def _git_output(git: str, source_root: Path, *args: str) -> str | None:
    completed = subprocess.run(  # noqa: S603
        [git, *args],
        cwd=source_root,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        return None
    return completed.stdout.strip()


def _directory_tree_digest(source_root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(source_root.rglob("*")):
        relative = path.relative_to(source_root)
        if any(part in _IGNORED_TREE_PARTS for part in relative.parts):
            continue
        if not path.is_file():
            continue
        encoded_path = relative.as_posix().encode()
        content = path.read_bytes()
        digest.update(len(encoded_path).to_bytes(8, "big"))
        digest.update(encoded_path)
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return f"sha256:{digest.hexdigest()}"


def _require_mapping(value: object, *, label: str) -> JsonObject:
    if not isinstance(value, dict):
        message = f"{label} must be a JSON object"
        raise TypeError(message)
    return value


def _require_list(value: object, *, label: str) -> list[Any]:
    if not isinstance(value, list):
        message = f"{label} must be a JSON array"
        raise TypeError(message)
    return value


def _parse_labeled_path(
    value: str,
    *,
    require_version: bool,
) -> tuple[str, Path]:
    label, separator, raw_path = value.partition("=")
    if not separator or not label.strip() or not raw_path.strip():
        message = f"invalid input {value!r}; expected LABEL=PATH"
        raise ValueError(message)
    normalized_label = label.strip()
    if require_version and _DS_VERSION_LABEL.fullmatch(normalized_label) is None:
        message = f"invalid DS version label {normalized_label!r}"
        raise ValueError(message)
    return normalized_label, Path(raw_path).expanduser()


__all__ = [
    "ContractInput",
    "ContractInputKind",
    "ExactProvenanceError",
    "LoadedContract",
    "build_contract_snapshot_from_source",
    "build_source_provenance",
    "contract_extractor_fingerprint",
    "contract_snapshot_digest",
    "load_contract_input",
    "load_contract_snapshot",
    "natural_version_key",
    "parse_contract_inputs",
    "require_exact_provenance",
    "snapshot_from_json",
    "snapshot_json_document",
    "write_contract_snapshot_document",
]
