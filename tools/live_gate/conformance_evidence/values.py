"""Canonical digests and fail-closed scalar checks shared by evidence validators."""

from __future__ import annotations

import hashlib
import json
import re
from typing import TYPE_CHECKING

from live_gate.evidence_values import require_constant_equal as _constant
from live_gate.evidence_values import required_mapping_view as _mapping
from live_gate.evidence_values import required_text as _text

if TYPE_CHECKING:
    from collections.abc import Mapping


_SHA256 = re.compile(r"sha256:[0-9a-f]{64}\Z")


_GIT_OBJECT = re.compile(r"[0-9a-f]{40}\Z")


_FINGERPRINT_KEYS = frozenset(
    {"consumed_projection", "effective_wire", "preservation", "source"}
)


_SUPPORT_LEVELS = frozenset({"experimental", "legacy_core", "full"})


_EXECUTION_MODES = frozenset(
    {
        "local",
        "diagnostic_recipe",
        "legacy_adapter",
        "generated_adapter",
        "wire_program",
    }
)


_VERIFICATIONS = frozenset({"static", "contract_tested", "live_smoke", "live_full"})


_SENSITIVE_KEYS = frozenset(
    {
        "api_token",
        "api_url",
        "authorization",
        "credential",
        "env_file",
        "password",
        "project_code",
        "project_id",
        "project_name",
        "raw_argv",
        "schedule_id",
        "secret",
        "stderr",
        "stdout",
        "task_code",
        "task_id",
        "task_name",
        "token",
        "url",
        "value",
        "workflow_code",
        "workflow_id",
        "workflow_name",
    }
)


def canonical_conformance_bundle_receipt_digest(
    value: Mapping[str, object],
) -> str:
    """Return the canonical digest of a receipt excluding its digest field."""
    payload = {key: nested for key, nested in value.items() if key != "receipt_digest"}
    raw = json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(raw).hexdigest()}"


def _validate_source(
    value: object,
    *,
    ds_version: str,
    label: str,
) -> tuple[str, str, str]:
    source = _mapping(value, label=label)
    _exact_keys(source, {"tag", "commit", "tree"}, label=label)
    _constant(source.get("tag"), ds_version, label=f"{label} tag")
    commit = _git_object(source.get("commit"), label=f"{label} commit")
    tree = _git_object(source.get("tree"), label=f"{label} tree")
    return ds_version, commit, tree


def _validate_flat_contract_source(
    contract: Mapping[str, object],
    *,
    ds_version: str,
) -> tuple[str, str, str]:
    _constant(
        contract.get("source_tag"),
        ds_version,
        label="contract source_tag",
    )
    commit = _git_object(
        contract.get("source_commit"),
        label="contract source_commit",
    )
    tree = _git_object(contract.get("source_tree"), label="contract source_tree")
    return ds_version, commit, tree


def _validate_fingerprints(value: object, *, label: str) -> None:
    fingerprints = _mapping(value, label=label)
    _exact_keys(fingerprints, _FINGERPRINT_KEYS, label=label)
    for field in sorted(_FINGERPRINT_KEYS):
        _sha256(fingerprints.get(field), label=f"{label} {field}")


def _bundle_digest(name: str, *, required_actions: tuple[str, ...]) -> str:
    return _canonical_digest(
        {
            "claim": "static-action-closure-only",
            "name": name,
            "required_actions": sorted(required_actions),
        }
    )


def _canonical_digest(value: object) -> str:
    raw = json.dumps(
        value,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(raw).hexdigest()}"


def _reject_sensitive_keys(value: object) -> None:
    if isinstance(value, dict):
        for key, nested in value.items():
            if str(key).lower() in _SENSITIVE_KEYS:
                message = f"evidence contains sensitive field {key!r}"
                raise ValueError(message)
            _reject_sensitive_keys(nested)
    elif isinstance(value, list):
        for nested in value:
            _reject_sensitive_keys(nested)


def _exact_keys(
    value: Mapping[str, object],
    expected: frozenset[str] | set[str],
    *,
    label: str,
) -> None:
    if set(value) != set(expected):
        message = (
            f"{label} keys differ: expected {sorted(expected)}, got {sorted(value)}"
        )
        raise ValueError(message)


def _text_sequence(
    value: object,
    *,
    label: str,
    allow_empty: bool = False,
) -> list[str]:
    if not isinstance(value, list) or not all(
        isinstance(item, str) and item for item in value
    ):
        message = f"{label} must be a string list"
        raise TypeError(message)
    if not value and not allow_empty:
        message = f"{label} must not be empty"
        raise ValueError(message)
    if len(value) != len(set(value)):
        message = f"{label} must not contain duplicates"
        raise ValueError(message)
    return value


def _sha256(value: object, *, label: str) -> str:
    text = _text(value, label=label)
    if _SHA256.fullmatch(text) is None:
        message = f"{label} must be a SHA-256 identity"
        raise ValueError(message)
    return text


def _git_object(value: object, *, label: str) -> str:
    text = _text(value, label=label)
    if _GIT_OBJECT.fullmatch(text) is None:
        message = f"{label} must be a full Git object ID"
        raise ValueError(message)
    return text
