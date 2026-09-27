"""Validate receipt cluster identity and registry or managed-image provenance."""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

from live_gate.conformance_evidence.values import (
    _GIT_OBJECT,
    _SHA256,
    _exact_keys,
    _git_object,
    _sha256,
    _text_sequence,
)
from live_gate.evidence_values import require_constant_equal as _constant
from live_gate.evidence_values import required_hmac_sha256 as _hmac_sha256
from live_gate.evidence_values import required_mapping_view as _mapping
from live_gate.evidence_values import required_text as _text
from live_gate.evidence_values import required_utc_timestamp as _timestamp

if TYPE_CHECKING:
    from collections.abc import Mapping

    from live_gate.conformance_evidence.types import (
        _ImageContract,
    )

_SHA512 = re.compile(r"sha512:[0-9a-f]{128}\Z")


_IMAGE_SOURCE = "node-local-inspection"


_MAX_OBSERVATION_AGE = timedelta(minutes=15)


_MAX_CLOCK_SKEW = timedelta(minutes=2)


def _validate_receipt_image_source(value: object, *, source_commit: str) -> None:
    """Bind a structurally validated image proof to the verified contract source."""
    provenance = _mapping(value, label="image provenance")
    if provenance.get("kind") == "managed-image-lock/v1":
        lock = _mapping(provenance.get("lock"), label="managed image lock")
        if lock.get("source_commit") != source_commit:
            message = (
                "managed image source commit differs from the exact source contract"
            )
            raise ValueError(message)


def _validate_cluster(
    value: object,
    *,
    recorded_at: datetime,
    image_contract: _ImageContract,
) -> str:
    cluster = _mapping(value, label="dolphinscheduler")
    _exact_keys(
        cluster,
        {
            "release",
            "image_ref",
            "image_id",
            "image_source",
            "image_provenance",
            "image_observed_at",
            "api_target_hmac_sha256",
            "principal_hmac_sha256",
            "persona",
        },
        label="dolphinscheduler",
    )
    release = _text(cluster.get("release"), label="DS release")
    _validate_image_reference(
        _text(cluster.get("image_ref"), label="image_ref"),
        release=release,
        image_contract=image_contract,
    )
    image_id = _sha256(cluster.get("image_id"), label="cluster image ID")
    _validate_receipt_image_provenance(
        cluster.get("image_provenance"),
        image_ref=_text(cluster.get("image_ref"), label="image_ref"),
        image_id=image_id,
        release=release,
        image_contract=image_contract,
    )
    _constant(
        cluster.get("image_source"),
        _IMAGE_SOURCE,
        label="image_source",
    )
    observed_at = _timestamp(
        cluster.get("image_observed_at"),
        label="image_observed_at",
    )
    if observed_at > recorded_at + _MAX_CLOCK_SKEW:
        message = "image observation is implausibly later than the gate"
        raise ValueError(message)
    if recorded_at - observed_at > _MAX_OBSERVATION_AGE:
        message = "image observation is too stale for conformance evidence"
        raise ValueError(message)
    _hmac_sha256(
        cluster.get("api_target_hmac_sha256"),
        label="api_target_hmac_sha256",
    )
    _hmac_sha256(
        cluster.get("principal_hmac_sha256"),
        label="principal_hmac_sha256",
    )
    _constant(cluster.get("persona"), "etl-developer", label="persona")
    return release


def _validate_image_reference(
    value: str,
    *,
    release: str,
    image_contract: _ImageContract,
) -> None:
    tagged_ref, separator, published_digest = value.partition("@")
    repository, tag_separator, tag = tagged_ref.rpartition(":")
    expected_repository = image_contract.repositories.get(release)
    if expected_repository is None:
        message = f"no conformance API image repository is declared for DS {release}"
        raise ValueError(message)
    if not tag_separator or repository != expected_repository or tag != release:
        message = f"cluster image reference must identify DolphinScheduler {release}"
        raise ValueError(message)
    if separator and _SHA256.fullmatch(published_digest) is None:
        message = "cluster image reference contains an invalid published digest"
        raise ValueError(message)


def _validate_receipt_image_provenance(
    value: object,
    *,
    image_ref: str,
    image_id: str,
    release: str,
    image_contract: _ImageContract,
) -> None:
    provenance = _mapping(value, label="image_provenance")
    kind = _text(provenance.get("kind"), label="image_provenance kind")
    if kind == "registry-digest/v1":
        _validate_receipt_registry_provenance(provenance, image_ref=image_ref)
        return
    if kind == "managed-image-lock/v1":
        _validate_receipt_managed_provenance(
            provenance,
            image_ref=image_ref,
            image_id=image_id,
            release=release,
            image_contract=image_contract,
        )
        return
    message = "image_provenance kind is not recognized"
    raise ValueError(message)


def _validate_receipt_registry_provenance(
    provenance: Mapping[str, object],
    *,
    image_ref: str,
) -> None:
    _exact_keys(
        provenance,
        {"kind", "repo_digests", "selected_repo_digest"},
        label="registry image_provenance",
    )
    repo_digests = tuple(
        _text_sequence(
            provenance.get("repo_digests"),
            label="registry image_provenance repo_digests",
            allow_empty=False,
        )
    )
    validated = tuple(
        _validate_receipt_repo_digest(item, label="registry RepoDigest")
        for item in repo_digests
    )
    if len(validated) != len(set(validated)):
        message = "registry image_provenance RepoDigests must be unique"
        raise ValueError(message)
    selected = _validate_receipt_repo_digest(
        provenance.get("selected_repo_digest"),
        label="selected registry RepoDigest",
    )
    if validated.count(selected) != 1:
        message = "selected registry RepoDigest must occur exactly once"
        raise ValueError(message)
    tagged_ref, at, digest = image_ref.partition("@")
    repository = tagged_ref.rpartition(":")[0]
    matching = tuple(item for item in validated if item.partition("@")[0] == repository)
    if not at:
        if matching != (selected,):
            message = "registry proof does not bind one selected API RepoDigest"
            raise ValueError(message)
    elif selected != f"{repository}@{digest}":
        message = "image reference published digest differs from selected RepoDigest"
        raise ValueError(message)


def _validate_receipt_managed_provenance(
    provenance: Mapping[str, object],
    *,
    image_ref: str,
    image_id: str,
    release: str,
    image_contract: _ImageContract,
) -> None:
    _exact_keys(
        provenance,
        {"actual", "kind", "lock", "management"},
        label="managed image_provenance",
    )
    if release not in image_contract.managed_versions:
        message = "managed unified image provenance is not trusted for this API version"
        raise ValueError(message)
    management = _mapping(
        provenance.get("management"),
        label="managed image management revision",
    )
    _exact_keys(
        management,
        {"commit", "lock_file_sha256", "worktree_clean"},
        label="managed image management revision",
    )
    _constant(
        management.get("worktree_clean"),
        True,
        label="managed image management worktree must be clean",
    )
    _git_object(management.get("commit"), label="managed image management commit")
    _sha256(
        management.get("lock_file_sha256"),
        label="managed image lock file digest",
    )
    lock = _validate_receipt_managed_lock(
        provenance.get("lock"),
        image_ref=image_ref,
        image_id=image_id,
        release=release,
        image_contract=image_contract,
    )
    _validate_receipt_managed_actual(
        provenance.get("actual"),
        lock=lock,
        release=release,
        managed_labels=image_contract.managed_labels,
    )


def _validate_receipt_managed_lock(
    value: object,
    *,
    image_ref: str,
    image_id: str,
    release: str,
    image_contract: _ImageContract,
) -> Mapping[str, object]:
    lock = _mapping(value, label="managed image lock row")
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
        label="managed image lock row",
    )
    for field, expected in (
        ("component", "unified"),
        ("version", release),
        ("image_ref", image_ref),
        ("image_id", image_id),
    ):
        _constant(lock.get(field), expected, label=f"managed lock {field}")
    _validate_image_reference(
        _text(lock.get("image_ref"), label="managed lock image_ref"),
        release=release,
        image_contract=image_contract,
    )
    _sha256(lock.get("image_id"), label="managed lock image ID")
    published = _optional_receipt_repo_digest(
        lock.get("published_manifest"),
        label="managed lock published manifest",
    )
    tagged_ref, at, published_digest = image_ref.partition("@")
    repository = tagged_ref.rpartition(":")[0]
    if published is not None and published.partition("@")[0] != repository:
        message = "managed lock published manifest uses the wrong API repository"
        raise ValueError(message)
    if at and published != f"{repository}@{published_digest}":
        message = "image reference published digest differs from managed lock"
        raise ValueError(message)
    _optional_receipt_repo_digest(lock.get("base_manifest"), label="base manifest")
    _optional_receipt_digest(lock.get("source_commit"), _GIT_OBJECT, "source commit")
    _optional_receipt_digest(lock.get("source_sha512"), _SHA512, "source SHA-512")
    _optional_receipt_digest(
        lock.get("binary_sha512"),
        _SHA512,
        "binary SHA-512",
    )
    _optional_receipt_digest(lock.get("schema_sha256"), _SHA256, "schema SHA-256")
    _enforce_receipt_managed_lock_presence(
        lock,
        release=release,
        policy=image_contract.managed_lock_presence[release],
    )
    _sha256(lock.get("archive_sha256"), label="managed lock archive SHA-256")
    basename = _text(lock.get("archive_basename"), label="managed archive basename")
    if (
        Path(basename).name != basename
        or not basename.endswith(".tar")
        or "://" in basename
        or "\\" in basename
    ):
        message = "managed archive basename must be a safe tar basename"
        raise ValueError(message)
    return lock


def _validate_receipt_managed_actual(
    value: object,
    *,
    lock: Mapping[str, object],
    release: str,
    managed_labels: frozenset[str],
) -> None:
    actual = _mapping(value, label="managed image actual observation")
    _exact_keys(
        actual,
        {"archive_sha256", "archive_verified", "labels", "repo_digests"},
        label="managed image actual observation",
    )
    repo_digests = tuple(
        _text_sequence(
            actual.get("repo_digests"),
            label="managed actual repo_digests",
            allow_empty=True,
        )
    )
    validated = tuple(
        _validate_receipt_repo_digest(item, label="managed actual RepoDigest")
        for item in repo_digests
    )
    if len(validated) != len(set(validated)):
        message = "managed actual RepoDigests must be unique"
        raise ValueError(message)
    observed_archive = actual.get("archive_sha256")
    if observed_archive is not None:
        _sha256(observed_archive, label="managed observed archive SHA-256")
        if observed_archive != lock.get("archive_sha256"):
            message = "managed observed archive digest differs from the lock"
            raise ValueError(message)
    verified = actual.get("archive_verified")
    if not isinstance(verified, bool) or verified != (observed_archive is not None):
        message = "managed archive verification claim lacks its observed digest"
        raise ValueError(message)
    if not validated and observed_archive is None:
        message = "managed image without RepoDigests requires verified archive evidence"
        raise ValueError(message)
    published = lock.get("published_manifest")
    if validated and (not isinstance(published, str) or published not in validated):
        message = "managed RepoDigests do not contain the locked published manifest"
        raise ValueError(message)
    _validate_receipt_managed_labels(
        actual.get("labels"),
        lock=lock,
        release=release,
        managed_labels=managed_labels,
    )


def _validate_receipt_managed_labels(
    value: object,
    *,
    lock: Mapping[str, object],
    release: str,
    managed_labels: frozenset[str],
) -> None:
    labels = _mapping(value, label="managed image labels")
    if not set(labels).issubset(managed_labels):
        message = "managed image labels contain unsupported fields"
        raise ValueError(message)
    for key, label_value in labels.items():
        _text(label_value, label=f"managed image label {key}")
    if release == "1.3.9" and labels:
        message = "DS 1.3.9 managed API image must not claim matrix provenance labels"
        raise ValueError(message)
    if release != "1.3.9":
        expected: dict[str, object] = {
            "org.apache.dolphinscheduler.matrix.source-tag": release,
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
            message = "managed image provenance labels differ from the lock row"
            raise ValueError(message)


def _enforce_receipt_managed_lock_presence(
    lock: Mapping[str, object],
    *,
    release: str,
    policy: Mapping[str, bool],
) -> None:
    for field, required in policy.items():
        present = lock.get(field) is not None
        if present == required:
            continue
        expectation = "non-null" if required else "null"
        message = (
            f"DS {release} managed lock presence policy requires "
            f"{field} to be {expectation}"
        )
        raise ValueError(message)


def _validate_receipt_repo_digest(value: object, *, label: str) -> str:
    text = _text(value, label=label)
    repository, separator, digest = text.partition("@")
    if (
        separator != "@"
        or not repository
        or repository.startswith("/")
        or repository.endswith("/")
        or "//" in repository
        or "://" in repository
        or "@" in repository
        or "\\" in repository
        or any(character.isspace() for character in repository)
        or any(part in {"", ".", ".."} for part in repository.split("/"))
        or _SHA256.fullmatch(digest) is None
    ):
        message = f"{label} has an invalid format"
        raise ValueError(message)
    return text


def _optional_receipt_repo_digest(value: object, *, label: str) -> str | None:
    if value is None:
        return None
    return _validate_receipt_repo_digest(value, label=label)


def _optional_receipt_digest(
    value: object,
    pattern: re.Pattern[str],
    label: str,
) -> str | None:
    if value is None:
        return None
    text = _text(value, label=label)
    if pattern.fullmatch(text) is None:
        message = f"{label} has an invalid format"
        raise ValueError(message)
    return text
