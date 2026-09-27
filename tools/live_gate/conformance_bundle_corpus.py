"""Fail-closed validation for the exact-version conformance receipt corpus."""

from __future__ import annotations

import argparse
import json
import os
import re
import stat
import sys
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Never

from live_gate.conformance_bundle_evidence import (
    ConformanceBundleAssessment,
    ConformanceBundleAssessmentBundle,
    ConformanceBundleEvidenceValidator,
)

if TYPE_CHECKING:
    from collections.abc import Mapping


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_EVIDENCE_ROOT = (
    ROOT / "docs" / "development" / "live-evidence" / "conformance-bundles"
)
_EXACT_DS_VERSIONS = (
    "1.3.9",
    "2.0.0",
    "2.0.1",
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
    "3.4.1",
    "3.4.2",
    "3.4.3",
)
_RECEIPT_FILENAME = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})-(?P<wheel_sha12>[0-9a-f]{12})\.json$"
)


@dataclass(frozen=True)
class ConformanceBundleCorpusReceipt:
    """Auditable identity of one accepted exact-version receipt."""

    ds_version: str
    bundle: str
    path: Path
    receipt_digest: str
    authoring_mode: str


@dataclass(frozen=True)
class ConformanceBundleCorpus:
    """Identity of one complete same-wheel conformance corpus."""

    evidence_root: Path
    versions: tuple[str, ...]
    wheel_filename: str
    wheel_sha256: str
    bundle_counts: Mapping[str, int]
    receipts: tuple[ConformanceBundleCorpusReceipt, ...]


@dataclass(frozen=True)
class _OpenDirectory:
    path: Path
    name: str | None
    descriptor: int
    device: int
    inode: int


@dataclass(frozen=True)
class _OpenReceipt:
    path: Path
    name: str
    descriptor: int
    device: int
    inode: int


def check_conformance_bundle_evidence_corpus(
    evidence_root: Path,
    *,
    expected_wheel_filename: str | None = None,
    expected_wheel_sha256: str | None = None,
    source_root: Path = ROOT,
) -> ConformanceBundleCorpus:
    """Validate exactly one highest-ready bundle receipt for every exact version."""
    validator = ConformanceBundleEvidenceValidator.load(source_root=source_root)
    assessment = validator.assessment
    versions = _governed_exact_versions(assessment.versions)
    expected_bundles = _highest_ready_bundles(
        assessment,
        versions=versions,
    )
    receipts: list[ConformanceBundleCorpusReceipt] = []
    shared_wheel: tuple[str, str] | None = None
    bundle_counts: dict[str, int] = {}
    root_directory = _open_corpus_root(evidence_root)
    try:
        _validate_corpus_entries(
            evidence_root,
            # Descriptor-relative enumeration is the security boundary.
            entries=os.listdir(root_directory.descriptor),  # noqa: PTH208
            versions=versions,
        )
        for version in versions:
            version_directory = _open_version_directory(
                root_directory,
                version=version,
            )
            try:
                receipt_file = _single_receipt(
                    version_directory,
                    version=version,
                )
                try:
                    receipt_path = receipt_file.path
                    receipt = _load_receipt(receipt_file)
                    expected_bundle = expected_bundles[version]
                    wheel_filename = (
                        expected_wheel_filename
                        if shared_wheel is None
                        else shared_wheel[0]
                    )
                    wheel_sha256 = (
                        expected_wheel_sha256
                        if shared_wheel is None
                        else shared_wheel[1]
                    )
                    try:
                        summary = validator.validate(
                            receipt,
                            expected_ds_version=version,
                            expected_bundle=expected_bundle,
                            expected_wheel_filename=wheel_filename,
                            expected_wheel_sha256=wheel_sha256,
                        )
                    except (TypeError, ValueError) as error:
                        message = (
                            f"{receipt_path}: invalid conformance-bundle receipt: "
                            f"{error}"
                        )
                        raise ValueError(message) from error
                    recorded_at_utc_date = _receipt_recorded_at_utc_date(
                        receipt,
                        path=receipt_path,
                    )
                    validate_receipt_filename(
                        receipt_path,
                        wheel_sha256=summary.wheel_sha256,
                        recorded_at_utc_date=recorded_at_utc_date,
                    )
                    _ensure_receipt_entry_unchanged(
                        receipt_file,
                        parent_descriptor=version_directory.descriptor,
                        version=version,
                    )
                finally:
                    os.close(receipt_file.descriptor)
                _ensure_version_directory_unchanged(
                    version_directory,
                    parent_descriptor=root_directory.descriptor,
                    version=version,
                )
            finally:
                os.close(version_directory.descriptor)
            runner = (summary.wheel_filename, summary.wheel_sha256)
            if shared_wheel is None:
                shared_wheel = runner
            elif runner != shared_wheel:  # pragma: no cover
                message = f"{receipt_path}: runner identity differs from corpus wheel"
                raise ValueError(message)
            bundle_counts[summary.bundle] = bundle_counts.get(summary.bundle, 0) + 1
            receipts.append(
                ConformanceBundleCorpusReceipt(
                    ds_version=summary.ds_version,
                    bundle=summary.bundle,
                    path=receipt_path,
                    receipt_digest=summary.receipt_digest,
                    authoring_mode=summary.authoring_mode,
                )
            )
        _ensure_corpus_root_unchanged(root_directory)
    finally:
        os.close(root_directory.descriptor)

    if shared_wheel is None:  # pragma: no cover - the exact matrix is non-empty
        message = "conformance-bundle corpus has no receipts"
        raise ValueError(message)
    return ConformanceBundleCorpus(
        evidence_root=evidence_root,
        versions=versions,
        wheel_filename=shared_wheel[0],
        wheel_sha256=shared_wheel[1],
        bundle_counts=bundle_counts,
        receipts=tuple(receipts),
    )


def _governed_exact_versions(versions: tuple[str, ...]) -> tuple[str, ...]:
    if versions != _EXACT_DS_VERSIONS:
        message = (
            "validated target versions differ from the governed "
            f"{len(_EXACT_DS_VERSIONS)}-version "
            f"conformance matrix; expected {_EXACT_DS_VERSIONS!r}, got {versions!r}"
        )
        raise ValueError(message)
    return versions


def _highest_ready_bundles(
    assessment: ConformanceBundleAssessment,
    *,
    versions: tuple[str, ...],
) -> dict[str, str]:
    if assessment.versions != versions:
        message = (
            "validated conformance assessment versions differ from the governed "
            f"matrix; expected {versions!r}, got {assessment.versions!r}"
        )
        raise ValueError(message)
    ancestor_closure = _bundle_ancestor_closure(assessment.bundles)

    expected: dict[str, str] = {}
    for version in versions:
        ready = {
            bundle.name
            for bundle in assessment.bundles
            if bundle.coordinates.get(version) == "ready"
        }
        maxima = sorted(
            candidate
            for candidate in ready
            if not any(
                candidate in ancestor_closure[other]
                for other in ready
                if other != candidate
            )
        )
        if not maxima:
            message = f"DS {version} has no ready conformance bundle"
            raise ValueError(message)
        if len(maxima) != 1:
            message = (
                f"DS {version} has multiple incomparable highest-ready "
                f"conformance bundles: {', '.join(maxima)}"
            )
            raise ValueError(message)
        expected[version] = maxima[0]
    return expected


def _bundle_ancestor_closure(
    assessment_bundles: tuple[ConformanceBundleAssessmentBundle, ...],
) -> dict[str, frozenset[str]]:
    bundles = {bundle.name: bundle for bundle in assessment_bundles}
    if not bundles or len(bundles) != len(assessment_bundles):
        message = "validated conformance assessment bundle names are not unique"
        raise ValueError(message)
    closure: dict[str, frozenset[str]] = {}
    visiting: set[str] = set()
    for bundle_name in bundles:
        _resolve_bundle_ancestors(
            bundle_name,
            bundles=bundles,
            closure=closure,
            visiting=visiting,
        )
    return closure


def _resolve_bundle_ancestors(
    bundle_name: str,
    *,
    bundles: Mapping[str, ConformanceBundleAssessmentBundle],
    closure: dict[str, frozenset[str]],
    visiting: set[str],
) -> frozenset[str]:
    if bundle_name in closure:
        return closure[bundle_name]
    if bundle_name in visiting:
        message = (
            "validated conformance assessment inheritance cycle includes "
            f"{bundle_name!r}"
        )
        raise ValueError(message)
    visiting.add(bundle_name)
    inherited: set[str] = set()
    for parent_name in bundles[bundle_name].extends:
        if parent_name not in bundles:
            message = (
                f"validated conformance bundle {bundle_name!r} has unknown "
                f"parent {parent_name!r}"
            )
            raise ValueError(message)
        inherited.add(parent_name)
        inherited.update(
            _resolve_bundle_ancestors(
                parent_name,
                bundles=bundles,
                closure=closure,
                visiting=visiting,
            )
        )
    visiting.remove(bundle_name)
    resolved = frozenset(inherited)
    closure[bundle_name] = resolved
    return resolved


def _open_corpus_root(root: Path) -> _OpenDirectory:
    flags = _directory_open_flags(label=f"conformance-bundle evidence root {root}")
    try:
        descriptor = os.open(root, flags)
    except FileNotFoundError as error:
        message = (
            f"conformance-bundle evidence root does not exist: {root}; "
            "track one current highest-bundle receipt for every exact version"
        )
        raise FileNotFoundError(message) from error
    except OSError as error:
        _raise_corpus_root_open_error(root, error=error)
    try:
        opened = os.fstat(descriptor)
        _validate_directory_metadata(
            opened,
            path=root,
            label="conformance-bundle evidence root",
        )
        return _OpenDirectory(
            path=root,
            name=None,
            descriptor=descriptor,
            device=opened.st_dev,
            inode=opened.st_ino,
        )
    except BaseException:
        os.close(descriptor)
        raise


def _validate_corpus_entries(
    root: Path,
    *,
    entries: list[str],
    versions: tuple[str, ...],
) -> None:
    observed = set(entries)
    expected = set(versions)
    missing = sorted(expected - observed)
    unexpected = sorted(observed - expected)
    if missing or unexpected:
        details: list[str] = []
        if missing:
            details.append("missing version directories: " + ", ".join(missing))
        if unexpected:
            details.append("unexpected root entries: " + ", ".join(unexpected))
        message = f"{root}: conformance corpus is incomplete; {'; '.join(details)}"
        raise ValueError(message)


def _open_version_directory(
    root: _OpenDirectory,
    *,
    version: str,
) -> _OpenDirectory:
    path = root.path / version
    flags = _directory_open_flags(label=f"DS {version} receipt directory {path}")
    try:
        descriptor = os.open(
            version,
            flags,
            dir_fd=root.descriptor,
        )
    except OSError as error:
        _raise_version_directory_open_error(
            root,
            version=version,
            error=error,
        )
    try:
        opened = os.fstat(descriptor)
        _validate_directory_metadata(
            opened,
            path=path,
            label=f"DS {version} receipt directory",
        )
        return _OpenDirectory(
            path=path,
            name=version,
            descriptor=descriptor,
            device=opened.st_dev,
            inode=opened.st_ino,
        )
    except BaseException:
        os.close(descriptor)
        raise


def _single_receipt(
    version_root: _OpenDirectory,
    *,
    version: str,
) -> _OpenReceipt:
    # Descriptor-relative enumeration is the security boundary.
    entries = sorted(os.listdir(version_root.descriptor))  # noqa: PTH208
    if len(entries) != 1:
        names = ", ".join(entries) or "none"
        message = (
            f"{version_root.path}: DS {version} requires exactly one highest-bundle "
            f"receipt; found {len(entries)} ({names})"
        )
        raise ValueError(message)
    name = entries[0]
    path = version_root.path / name
    try:
        listed = os.stat(
            name,
            dir_fd=version_root.descriptor,
            follow_symlinks=False,
        )
    except OSError as error:
        message = f"DS {version} receipt changed during validation: {path}"
        raise ValueError(message) from error
    if not stat.S_ISREG(listed.st_mode):
        message = f"DS {version} receipt must be a non-symlink regular file: {path}"
        raise ValueError(message)
    _validate_receipt_permissions(listed.st_mode, version=version, path=path)
    no_follow = _no_follow_flag(label=f"DS {version} receipt {path}")
    try:
        descriptor = os.open(
            name,
            os.O_RDONLY | no_follow,
            dir_fd=version_root.descriptor,
        )
    except OSError as error:
        _raise_receipt_open_error(
            version_root,
            name=name,
            version=version,
            error=error,
        )
    try:
        opened = os.fstat(descriptor)
        _validate_opened_receipt(
            opened,
            listed=listed,
            version=version,
            path=path,
        )
        return _OpenReceipt(
            path=path,
            name=name,
            descriptor=descriptor,
            device=opened.st_dev,
            inode=opened.st_ino,
        )
    except BaseException:
        os.close(descriptor)
        raise


def _validate_opened_receipt(
    opened: os.stat_result,
    *,
    listed: os.stat_result,
    version: str,
    path: Path,
) -> None:
    if (
        not stat.S_ISREG(opened.st_mode)
        or opened.st_dev != listed.st_dev
        or opened.st_ino != listed.st_ino
    ):
        message = (
            f"DS {version} receipt changed or could not be opened securely: {path}"
        )
        raise ValueError(message)
    _validate_receipt_permissions(opened.st_mode, version=version, path=path)


def _validate_receipt_permissions(mode: int, *, version: str, path: Path) -> None:
    permissions = stat.S_IMODE(mode)
    forbidden_permissions = stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH
    forbidden_permissions |= stat.S_IWGRP | stat.S_IWOTH
    if permissions & forbidden_permissions:
        message = (
            f"DS {version} receipt has unsafe mode {permissions:#o}: {path}; "
            "execute and group/world-write bits are forbidden"
        )
        raise ValueError(message)


def _load_receipt(receipt: _OpenReceipt) -> dict[str, object]:
    path = receipt.path
    try:
        chunks: list[bytes] = []
        while chunk := os.read(receipt.descriptor, 64 * 1024):
            chunks.append(chunk)
        source = b"".join(chunks).decode("utf-8")
    except UnicodeDecodeError as error:
        message = f"{path}: receipt is not valid UTF-8"
        raise ValueError(message) from error
    try:
        payload: object = json.loads(
            source,
            object_pairs_hook=_unique_json_object,
            parse_constant=_reject_nonstandard_json_constant,
        )
    except json.JSONDecodeError as error:
        message = f"{path}: receipt is not valid JSON: {error.msg}"
        raise ValueError(message) from error
    if not isinstance(payload, dict) or not all(
        isinstance(key, str) for key in payload
    ):
        message = f"{path}: receipt must be an object with string keys"
        raise TypeError(message)
    return payload


def _directory_open_flags(*, label: str) -> int:
    directory = getattr(os, "O_DIRECTORY", None)
    if directory is None:
        message = f"{label} cannot be securely opened on this platform"
        raise ValueError(message)
    if not isinstance(directory, int):  # pragma: no cover - invalid stdlib contract
        message = "os.O_DIRECTORY must be an integer flag"
        raise TypeError(message)
    return os.O_RDONLY | directory | _no_follow_flag(label=label)


def _no_follow_flag(*, label: str) -> int:
    no_follow = getattr(os, "O_NOFOLLOW", None)
    if no_follow is None:
        message = f"{label} cannot be securely opened on this platform"
        raise ValueError(message)
    if not isinstance(no_follow, int):  # pragma: no cover - invalid stdlib contract
        message = "os.O_NOFOLLOW must be an integer flag"
        raise TypeError(message)
    return no_follow


def _validate_directory_metadata(
    opened: os.stat_result,
    *,
    path: Path,
    label: str,
) -> None:
    if not stat.S_ISDIR(opened.st_mode):
        message = f"{path}: {label} is not a directory"
        raise NotADirectoryError(message)
    get_effective_user_id = getattr(os, "geteuid", None)
    if get_effective_user_id is None:
        message = f"{label} ownership cannot be verified on this platform: {path}"
        raise ValueError(message)
    expected_owner = get_effective_user_id()
    if opened.st_uid != expected_owner:
        message = (
            f"{label} must be owned by effective user {expected_owner}: {path}; "
            f"got uid {opened.st_uid}"
        )
        raise ValueError(message)
    permissions = stat.S_IMODE(opened.st_mode)
    if permissions & (stat.S_IWGRP | stat.S_IWOTH):
        message = (
            f"{label} has unsafe mode {permissions:#o}: {path}; "
            "group/world-write bits are forbidden"
        )
        raise ValueError(message)


def _raise_corpus_root_open_error(root: Path, *, error: OSError) -> Never:
    try:
        current = root.lstat()
    except FileNotFoundError:
        message = (
            f"conformance-bundle evidence root does not exist: {root}; "
            "track one current highest-bundle receipt for every exact version"
        )
        raise FileNotFoundError(message) from error
    if stat.S_ISLNK(current.st_mode):
        message = f"conformance-bundle evidence root must not be a symlink: {root}"
        raise ValueError(message) from error
    if not stat.S_ISDIR(current.st_mode):
        message = f"conformance-bundle evidence root is not a directory: {root}"
        raise NotADirectoryError(message) from error
    message = f"conformance-bundle evidence root could not be opened securely: {root}"
    raise ValueError(message) from error


def _raise_version_directory_open_error(
    root: _OpenDirectory,
    *,
    version: str,
    error: OSError,
) -> Never:
    path = root.path / version
    try:
        current = os.stat(
            version,
            dir_fd=root.descriptor,
            follow_symlinks=False,
        )
    except FileNotFoundError:
        message = f"DS {version} receipt directory changed during validation: {path}"
        raise ValueError(message) from error
    if stat.S_ISLNK(current.st_mode):
        message = f"receipt directory for DS {version} must not be a symlink: {path}"
        raise ValueError(message) from error
    if not stat.S_ISDIR(current.st_mode):
        message = f"{path}: expected a receipt directory for DS {version}"
        raise NotADirectoryError(message) from error
    message = f"DS {version} receipt directory could not be opened securely: {path}"
    raise ValueError(message) from error


def _raise_receipt_open_error(
    version_root: _OpenDirectory,
    *,
    name: str,
    version: str,
    error: OSError,
) -> Never:
    path = version_root.path / name
    message = f"DS {version} receipt changed or could not be opened securely: {path}"
    raise ValueError(message) from error


def _ensure_corpus_root_unchanged(root: _OpenDirectory) -> None:
    try:
        opened = os.fstat(root.descriptor)
        current = root.path.lstat()
    except OSError as error:
        message = (
            f"conformance-bundle evidence root changed during validation: {root.path}"
        )
        raise ValueError(message) from error
    if (
        not stat.S_ISDIR(opened.st_mode)
        or not stat.S_ISDIR(current.st_mode)
        or opened.st_dev != root.device
        or opened.st_ino != root.inode
        or current.st_dev != root.device
        or current.st_ino != root.inode
    ):
        message = (
            f"conformance-bundle evidence root changed during validation: {root.path}"
        )
        raise ValueError(message)
    _validate_directory_metadata(
        opened,
        path=root.path,
        label="conformance-bundle evidence root",
    )
    _validate_directory_metadata(
        current,
        path=root.path,
        label="conformance-bundle evidence root",
    )


def _ensure_version_directory_unchanged(
    version_root: _OpenDirectory,
    *,
    parent_descriptor: int,
    version: str,
) -> None:
    if version_root.name is None:  # pragma: no cover - construction invariant
        message = "version directory handle lacks an entry name"
        raise ValueError(message)
    try:
        opened = os.fstat(version_root.descriptor)
        current = os.stat(
            version_root.name,
            dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
    except OSError as error:
        message = (
            f"DS {version} receipt directory changed during validation: "
            f"{version_root.path}"
        )
        raise ValueError(message) from error
    if (
        not stat.S_ISDIR(opened.st_mode)
        or not stat.S_ISDIR(current.st_mode)
        or opened.st_dev != version_root.device
        or opened.st_ino != version_root.inode
        or current.st_dev != version_root.device
        or current.st_ino != version_root.inode
    ):
        message = (
            f"DS {version} receipt directory changed during validation: "
            f"{version_root.path}"
        )
        raise ValueError(message)
    label = f"DS {version} receipt directory"
    _validate_directory_metadata(opened, path=version_root.path, label=label)
    _validate_directory_metadata(current, path=version_root.path, label=label)


def _ensure_receipt_entry_unchanged(
    receipt: _OpenReceipt,
    *,
    parent_descriptor: int,
    version: str,
) -> None:
    try:
        opened = os.fstat(receipt.descriptor)
        current = os.stat(
            receipt.name,
            dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
    except OSError as error:
        message = f"DS {version} receipt changed during validation: {receipt.path}"
        raise ValueError(message) from error
    if (
        not stat.S_ISREG(opened.st_mode)
        or not stat.S_ISREG(current.st_mode)
        or opened.st_dev != receipt.device
        or opened.st_ino != receipt.inode
        or current.st_dev != receipt.device
        or current.st_ino != receipt.inode
    ):
        message = f"DS {version} receipt changed during validation: {receipt.path}"
        raise ValueError(message)
    _validate_receipt_permissions(opened.st_mode, version=version, path=receipt.path)
    _validate_receipt_permissions(current.st_mode, version=version, path=receipt.path)


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, nested in pairs:
        if key in value:
            message = f"duplicate JSON object key {key!r}"
            raise ValueError(message)
        value[key] = nested
    return value


def _reject_nonstandard_json_constant(value: str) -> object:
    message = f"non-standard JSON constant {value!r} is forbidden"
    raise ValueError(message)


def _receipt_recorded_at_utc_date(
    receipt: Mapping[str, object],
    *,
    path: Path,
) -> date:
    recorded_at = receipt.get("recorded_at")
    if not isinstance(recorded_at, str):
        message = f"{path}: receipt recorded_at must be a timezone-aware timestamp"
        raise TypeError(message)
    try:
        timestamp = datetime.fromisoformat(recorded_at)
    except ValueError as error:
        message = f"{path}: receipt recorded_at must be an ISO-8601 timestamp"
        raise ValueError(message) from error
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        message = f"{path}: receipt recorded_at must include a UTC offset"
        raise ValueError(message)
    return timestamp.astimezone(UTC).date()


def validate_receipt_filename(
    path: Path,
    *,
    wheel_sha256: str,
    recorded_at_utc_date: date,
) -> None:
    """Require the governed date-and-wheel-digest receipt filename."""
    match = _RECEIPT_FILENAME.fullmatch(path.name)
    if match is None:
        message = (
            f"{path}: receipt filename must match "
            "YYYY-MM-DD-<first-12-wheel-sha256>.json"
        )
        raise ValueError(message)
    try:
        filename_date = date.fromisoformat(match["date"])
    except ValueError as error:
        message = f"{path}: receipt filename date is not a calendar date"
        raise ValueError(message) from error
    if filename_date != recorded_at_utc_date:
        message = (
            f"{path}: filename date {match['date']!r} must equal receipt "
            f"recorded_at UTC date {recorded_at_utc_date.isoformat()!r}"
        )
        raise ValueError(message)
    expected_suffix = wheel_sha256.removeprefix("sha256:")[:12]
    if match["wheel_sha12"] != expected_suffix:
        message = (
            f"{path}: filename digest suffix must be {expected_suffix!r} "
            "from runner.wheel_sha256"
        )
        raise ValueError(message)


def conformance_bundle_corpus_summary(
    corpus: ConformanceBundleCorpus,
) -> dict[str, object]:
    """Project a validated corpus into a deterministic audit summary."""
    return {
        "schema_version": 1,
        "kind": "dsctl-conformance-bundle-corpus-summary",
        "status": "passed",
        "evidence_root": str(corpus.evidence_root),
        "receipt_count": len(corpus.receipts),
        "versions": list(corpus.versions),
        "bundles": dict(corpus.bundle_counts),
        "wheel": {
            "filename": corpus.wheel_filename,
            "sha256": corpus.wheel_sha256,
        },
        "receipts": [
            {
                "ds_version": receipt.ds_version,
                "bundle": receipt.bundle,
                "path": str(receipt.path.relative_to(corpus.evidence_root)),
                "receipt_digest": receipt.receipt_digest,
                "authoring_mode": receipt.authoring_mode,
            }
            for receipt in corpus.receipts
        ],
    }


def run_conformance_bundle_evidence_cli(
    argv: list[str] | None = None,
    *,
    default_evidence_root: Path = DEFAULT_EVIDENCE_ROOT,
) -> int:
    """Run the conformance-corpus validator and emit one JSON audit summary."""
    parser = argparse.ArgumentParser(
        description=(
            f"Validate the complete {len(_EXACT_DS_VERSIONS)}-version "
            "same-wheel conformance-bundle "
            "receipt corpus."
        )
    )
    parser.add_argument(
        "--evidence-root",
        type=Path,
        default=default_evidence_root,
        help=(
            "receipt corpus root (defaults to the tracked conformance-bundle "
            "evidence tree)"
        ),
    )
    parser.add_argument(
        "--expected-wheel-filename",
        help="require every receipt to name this exact wheel basename",
    )
    parser.add_argument(
        "--expected-wheel-sha256",
        help="require every receipt to bind this full sha256:<64-hex> identity",
    )
    parser.add_argument(
        "--source-root",
        type=Path,
        default=ROOT,
        help=argparse.SUPPRESS,
    )
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        checked = check_conformance_bundle_evidence_corpus(
            args.evidence_root.expanduser().absolute(),
            expected_wheel_filename=args.expected_wheel_filename,
            expected_wheel_sha256=args.expected_wheel_sha256,
            source_root=args.source_root.expanduser().absolute(),
        )
    except (OSError, TypeError, ValueError) as error:
        print(f"conformance-bundle evidence check failed: {error}", file=sys.stderr)
        return 1
    print(
        json.dumps(
            conformance_bundle_corpus_summary(checked),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
    )
    return 0


__all__ = [
    "DEFAULT_EVIDENCE_ROOT",
    "ConformanceBundleCorpus",
    "ConformanceBundleCorpusReceipt",
    "check_conformance_bundle_evidence_corpus",
    "conformance_bundle_corpus_summary",
    "run_conformance_bundle_evidence_cli",
    "validate_receipt_filename",
]
