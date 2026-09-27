"""Strict public provenance contract for conformance API images."""

from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING

from dsctl.generated.version_profiles import VERSION_PROFILES
from live_gate.conformance_image_contract import (
    MANAGED_API_VERSIONS,
    MANAGED_LOCK_PRESENCE,
    MANAGED_PROVENANCE_LABELS,
    expected_api_image_repository,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

_SHA256 = re.compile(r"sha256:[0-9a-f]{64}\Z")
_SHA512 = re.compile(r"sha512:[0-9a-f]{128}\Z")
_GIT_OBJECT = re.compile(r"[0-9a-f]{40}\Z")


def validate_image_reference(value: object, *, ds_version: str) -> str:
    """Validate an exact tag with an optional matching published digest suffix."""
    text = _text(value, label="Image reference")
    tagged_ref, at, published_digest = text.partition("@")
    if at and "@" in published_digest:
        message = "Image reference has an invalid format"
        raise ValueError(message)
    repository, separator, tag = tagged_ref.rpartition(":")
    if (
        separator != ":"
        or tag != ds_version
        or repository != expected_api_image_repository(ds_version)
    ):
        message = f"Image reference must identify exact DS {ds_version}"
        raise ValueError(message)
    if at and _SHA256.fullmatch(published_digest) is None:
        message = "Image reference contains an invalid published digest"
        raise ValueError(message)
    return text


def validate_image_provenance(
    value: object,
    *,
    image_ref: str,
    image_id: str,
    ds_version: str,
    label: str = "Image provenance",
) -> dict[str, object]:
    """Validate and normalize one registry or managed image proof."""
    provenance = _mapping(value, label=label)
    kind = _text(provenance.get("kind"), label=f"{label} kind")
    if kind == "registry-digest/v1":
        return _validate_registry(provenance, image_ref=image_ref, label=label)
    if kind == "managed-image-lock/v1":
        return _validate_managed(
            provenance,
            image_ref=image_ref,
            image_id=image_id,
            ds_version=ds_version,
            label=label,
        )
    message = f"{label} kind is not recognized"
    raise ValueError(message)


def _validate_registry(
    provenance: Mapping[str, object],
    *,
    image_ref: str,
    label: str,
) -> dict[str, object]:
    _exact_keys(
        provenance,
        {"kind", "repo_digests", "selected_repo_digest"},
        label=label,
    )
    repo_digests = provenance.get("repo_digests")
    if not isinstance(repo_digests, list) or not repo_digests:
        message = f"{label} RepoDigests must be a non-empty array"
        raise TypeError(message)
    validated = tuple(
        _repo_digest(item, label=f"{label} RepoDigest") for item in repo_digests
    )
    if len(validated) != len(set(validated)):
        message = f"{label} RepoDigests must be unique"
        raise ValueError(message)
    selected = _repo_digest(
        provenance.get("selected_repo_digest"),
        label=f"{label} selected RepoDigest",
    )
    if validated.count(selected) != 1:
        message = f"Selected {label} RepoDigest must occur exactly once"
        raise ValueError(message)
    repository, published_digest = _image_reference_parts(image_ref)
    matching = tuple(item for item in validated if item.partition("@")[0] == repository)
    if published_digest is None:
        if matching != (selected,):
            message = f"{label} does not bind one selected service RepoDigest"
            raise ValueError(message)
    elif selected != f"{repository}@{published_digest}":
        message = "Image reference published digest differs from selected RepoDigest"
        raise ValueError(message)
    return {
        "kind": "registry-digest/v1",
        "repo_digests": list(validated),
        "selected_repo_digest": selected,
    }


def _validate_managed(
    provenance: Mapping[str, object],
    *,
    image_ref: str,
    image_id: str,
    ds_version: str,
    label: str,
) -> dict[str, object]:
    _exact_keys(provenance, {"actual", "kind", "lock", "management"}, label=label)
    if ds_version not in MANAGED_API_VERSIONS:
        message = (
            "A managed unified image lock is not trusted for this API version; "
            "registry digest evidence is required"
        )
        raise ValueError(message)
    management = _mapping(
        provenance.get("management"),
        label=f"{label} management revision",
    )
    _exact_keys(
        management,
        {"commit", "lock_file_sha256", "worktree_clean"},
        label=f"{label} management revision",
    )
    if management.get("worktree_clean") is not True:
        message = "Managed image management worktree must be clean"
        raise ValueError(message)
    _digest(management.get("commit"), _GIT_OBJECT, label="management commit")
    _digest(
        management.get("lock_file_sha256"),
        _SHA256,
        label="lock file digest",
    )
    lock = _managed_lock(
        provenance.get("lock"),
        image_ref=image_ref,
        image_id=image_id,
        ds_version=ds_version,
    )
    actual = _managed_actual(
        provenance.get("actual"),
        lock=lock,
        ds_version=ds_version,
    )
    return {
        "kind": "managed-image-lock/v1",
        "management": dict(management),
        "lock": lock,
        "actual": actual,
    }


def _managed_lock(
    value: object,
    *,
    image_ref: str,
    image_id: str,
    ds_version: str,
) -> dict[str, object]:
    lock = _mapping(value, label="Managed image lock row")
    _exact_keys(
        lock,
        {
            "archive_basename",
            "archive_sha256",
            "base_manifest",
            "binary_sha512",
            "component",
            "image_id",
            "image_ref",
            "published_manifest",
            "schema_sha256",
            "source_commit",
            "source_sha512",
            "version",
        },
        label="Managed image lock row",
    )
    if (
        lock.get("component") != "unified"
        or lock.get("version") != ds_version
        or lock.get("image_ref") != image_ref
        or lock.get("image_id") != image_id
    ):
        message = "Managed image lock row does not bind the exact unified API image"
        raise ValueError(message)
    validate_image_reference(lock.get("image_ref"), ds_version=ds_version)
    _digest(lock.get("image_id"), _SHA256, label="lock image ID")
    published = _optional_repo_digest(
        lock.get("published_manifest"),
        label="published manifest",
    )
    repository, published_digest = _image_reference_parts(image_ref)
    if published is not None and published.partition("@")[0] != repository:
        message = "Managed lock published manifest uses the wrong API repository"
        raise ValueError(message)
    if published_digest is not None and published != (
        f"{repository}@{published_digest}"
    ):
        message = "Image reference published digest differs from managed lock"
        raise ValueError(message)
    _optional_repo_digest(lock.get("base_manifest"), label="base manifest")
    _optional_digest(lock.get("source_commit"), _GIT_OBJECT, label="source commit")
    _optional_digest(lock.get("source_sha512"), _SHA512, label="source SHA-512")
    _optional_digest(
        lock.get("binary_sha512"),
        _SHA512,
        label="binary SHA-512",
    )
    _optional_digest(lock.get("schema_sha256"), _SHA256, label="schema SHA-256")
    _enforce_managed_lock_presence(lock, ds_version=ds_version)
    if lock.get("source_commit") != VERSION_PROFILES[ds_version]["source"]["commit"]:
        message = "Managed image source commit differs from the exact source contract"
        raise ValueError(message)
    _digest(lock.get("archive_sha256"), _SHA256, label="archive SHA-256")
    basename = _text(lock.get("archive_basename"), label="archive basename")
    if (
        Path(basename).name != basename
        or not basename.endswith(".tar")
        or "://" in basename
        or "\\" in basename
    ):
        message = "Managed lock archive basename must be a safe tar basename"
        raise ValueError(message)
    return dict(lock)


def _managed_actual(
    value: object,
    *,
    lock: Mapping[str, object],
    ds_version: str,
) -> dict[str, object]:
    actual = _mapping(value, label="Managed image actual observation")
    _exact_keys(
        actual,
        {"archive_sha256", "archive_verified", "labels", "repo_digests"},
        label="Managed image actual observation",
    )
    repo_digests = actual.get("repo_digests")
    if not isinstance(repo_digests, list):
        message = "Managed image actual RepoDigests must be an array"
        raise TypeError(message)
    validated = tuple(
        _repo_digest(item, label="Managed image actual RepoDigest")
        for item in repo_digests
    )
    if len(validated) != len(set(validated)):
        message = "Managed image actual RepoDigests must be unique"
        raise ValueError(message)
    observed_archive = actual.get("archive_sha256")
    if observed_archive is not None:
        _digest(observed_archive, _SHA256, label="observed archive SHA-256")
        if observed_archive != lock.get("archive_sha256"):
            message = "Managed image observed archive digest differs from the lock"
            raise ValueError(message)
    archive_verified = actual.get("archive_verified")
    if not isinstance(archive_verified, bool):
        message = "Managed image archive_verified must be boolean"
        raise TypeError(message)
    if archive_verified != (observed_archive is not None):
        message = "Managed image archive verification claim lacks its observed digest"
        raise ValueError(message)
    if not validated and observed_archive is None:
        message = "Managed image without RepoDigests requires verified archive evidence"
        raise ValueError(message)
    published = lock.get("published_manifest")
    if validated and (not isinstance(published, str) or published not in validated):
        message = (
            "Managed image RepoDigests do not contain the locked published manifest"
        )
        raise ValueError(message)
    labels = _managed_labels(
        actual.get("labels"),
        lock=lock,
        ds_version=ds_version,
    )
    return {
        "repo_digests": list(validated),
        "labels": labels,
        "archive_verified": archive_verified,
        "archive_sha256": observed_archive,
    }


def _managed_labels(
    value: object,
    *,
    lock: Mapping[str, object],
    ds_version: str,
) -> dict[str, object]:
    labels = _mapping(value, label="Managed image labels")
    if not set(labels).issubset(MANAGED_PROVENANCE_LABELS):
        message = "Managed image labels contain unsupported fields"
        raise ValueError(message)
    for key, label_value in labels.items():
        _text(label_value, label=f"Managed image label {key}")
    if ds_version == "1.3.9" and labels:
        message = "DS 1.3.9 managed API image must not claim matrix provenance labels"
        raise ValueError(message)
    if ds_version != "1.3.9":
        expected: dict[str, object] = {
            "org.apache.dolphinscheduler.matrix.source-tag": ds_version,
            "org.apache.dolphinscheduler.matrix.source-commit": lock["source_commit"],
        }
        optional = {
            "org.apache.dolphinscheduler.matrix.source-sha512": lock["source_sha512"],
            "org.apache.dolphinscheduler.matrix.binary-sha512": lock["binary_sha512"],
            "org.apache.dolphinscheduler.matrix.base-digest": lock["base_manifest"],
        }
        expected.update(
            {key: item for key, item in optional.items() if item is not None}
        )
        normalized = {
            key: item.removeprefix("sha512:")
            if key.endswith("sha512") and isinstance(item, str)
            else item
            for key, item in expected.items()
        }
        if labels != normalized:
            message = "Managed image provenance labels differ from the lock row"
            raise ValueError(message)
    return dict(labels)


def _enforce_managed_lock_presence(
    lock: Mapping[str, object],
    *,
    ds_version: str,
) -> None:
    policy = MANAGED_LOCK_PRESENCE[ds_version]
    for field, required in policy.items():
        present = lock.get(field) is not None
        if present == required:
            continue
        expectation = "non-null" if required else "null"
        message = (
            f"DS {ds_version} managed lock presence policy requires "
            f"{field} to be {expectation}"
        )
        raise ValueError(message)


def _image_reference_parts(image_ref: str) -> tuple[str, str | None]:
    tagged_ref, at, published_digest = image_ref.partition("@")
    repository, separator, _tag = tagged_ref.rpartition(":")
    if separator != ":" or not _valid_repository(repository):
        message = "Image reference has an invalid format"
        raise ValueError(message)
    return repository, published_digest if at else None


def _repo_digest(value: object, *, label: str) -> str:
    text = _text(value, label=label)
    repository, separator, digest = text.partition("@")
    if (
        separator != "@"
        or not _valid_repository(repository)
        or _SHA256.fullmatch(digest) is None
    ):
        message = f"{label} has an invalid format"
        raise ValueError(message)
    return text


def _valid_repository(repository: str) -> bool:
    if (
        not repository
        or repository.startswith("/")
        or repository.endswith("/")
        or "//" in repository
        or "://" in repository
        or "@" in repository
        or "\\" in repository
        or any(character.isspace() for character in repository)
    ):
        return False
    return all(part not in {"", ".", ".."} for part in repository.split("/"))


def _mapping(value: object, *, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        message = f"{label} must be an object"
        raise TypeError(message)
    return value


def _exact_keys(value: Mapping[str, object], expected: set[str], *, label: str) -> None:
    if set(value) != expected:
        message = f"{label} has invalid fields"
        raise ValueError(message)


def _text(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        message = f"{label} must be non-empty text"
        raise TypeError(message)
    return value


def _digest(value: object, pattern: re.Pattern[str], *, label: str) -> str:
    text = _text(value, label=label)
    if pattern.fullmatch(text) is None:
        message = f"{label} has an invalid format"
        raise ValueError(message)
    return text


def _optional_digest(
    value: object,
    pattern: re.Pattern[str],
    *,
    label: str,
) -> str | None:
    if value is None:
        return None
    return _digest(value, pattern, label=label)


def _optional_repo_digest(value: object, *, label: str) -> str | None:
    if value is None:
        return None
    return _repo_digest(value, label=label)


__all__ = [
    "expected_api_image_repository",
    "validate_image_provenance",
    "validate_image_reference",
]
