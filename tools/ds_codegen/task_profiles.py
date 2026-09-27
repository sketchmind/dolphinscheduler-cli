"""Compile exact task-plugin inventories into publishable task profiles."""

from __future__ import annotations

import copy
import hashlib
import json
import re
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any, cast

from ds_codegen.contract_inputs import require_exact_provenance
from ds_codegen.profile_ledger import exact_versions, select_exact_versions

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

TASK_PROFILE_SCHEMA_VERSION = 1
TASK_PROFILE_FACTS_KIND = "dolphinscheduler-task-profile-facts"
TASK_PROFILE_REVIEWS_KIND = "dolphinscheduler-task-profile-reviews"
TASK_PROFILE_DATA_KIND = "dolphinscheduler-task-profiles"
DEFAULT_TASK_PROFILE_FACTS = Path(__file__).with_name("task_profile_facts.json")
DEFAULT_TASK_PROFILE_REVIEWS = Path(__file__).with_name("task_profile_reviews.json")
GENERATED_TASK_PROFILE_PATH = Path("src/dsctl/generated/task_profiles.py")

_INVENTORY_KIND = "dolphinscheduler-task-plugin-contract-inventory"
_REGISTRATION_KINDS = frozenset({"legacy_switch", "logic_switch", "spi_factory"})
_SHA256 = re.compile(r"sha256:[0-9a-f]{64}\Z")
_GIT_TREE = re.compile(r"[0-9a-f]{40}\Z")
_CONTROL_TASKS_3_2 = frozenset(
    {
        "BLOCKING",
        "CONDITIONS",
        "DEPENDENT",
        "DYNAMIC",
        "SUB_PROCESS",
        "SWITCH",
    }
)


def load_task_profile_document(path: Path) -> dict[str, Any]:
    """Load one JSON task-profile input as a detached mutable mapping."""
    loaded: object = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        message = f"task-profile document must contain a JSON object: {path}"
        raise TypeError(message)
    return copy.deepcopy(loaded)


def project_task_profile_facts(
    inventory: Mapping[str, object],
) -> dict[str, Any]:
    """Project exact upstream task facts without making authoring claims."""
    raw_inventory = copy.deepcopy(dict(inventory))
    _validate_inventory_header(raw_inventory)
    raw_targets = _list(raw_inventory.get("targets"), label="inventory.targets")
    versions = tuple(
        _text(_mapping(target, label="inventory target").get("label"), label="label")
        for target in raw_targets
    )
    exact_versions(versions, label="inventory targets")

    profiles: dict[str, object] = {}
    for raw_target in raw_targets:
        target = _mapping(raw_target, label="inventory target")
        version = _text(target.get("label"), label="inventory target.label")
        profiles[version] = _project_target(version, target)

    return {
        "schema_version": TASK_PROFILE_SCHEMA_VERSION,
        "kind": TASK_PROFILE_FACTS_KIND,
        "source_inventory_digest": _digest(raw_inventory),
        "target_versions": list(versions),
        "profiles": profiles,
    }


def compile_task_profile_data(
    facts: Mapping[str, object],
    reviews: Mapping[str, object],
    *,
    source_roots: Mapping[str, Path] | None = None,
    versions: Iterable[str] | None = None,
) -> dict[str, Any]:
    """Join mechanical facts to explicit typed-authoring review decisions."""
    fact_document = copy.deepcopy(dict(facts))
    review_document = copy.deepcopy(dict(reviews))
    fact_profiles = _validate_fact_document(fact_document)
    review_profiles = _validate_review_document(review_document)
    selected = select_exact_versions(
        review_profiles, versions, label="task-profile reviews"
    )
    select_exact_versions(fact_profiles, selected, label="task-profile facts")

    profiles: dict[str, object] = {}
    for version in selected:
        fact_profile = _mapping(
            fact_profiles[version],
            label=f"task-profile facts {version}",
        )
        task_types = _mapping(
            fact_profile.get("task_types"),
            label=f"task-profile facts {version}.task_types",
        )
        review_profile = _mapping(
            review_profiles[version],
            label=f"task-profile reviews {version}",
        )
        _validate_review_source_tree(
            version=version,
            fact_profile=fact_profile,
            review_profile=review_profile,
        )
        typed_reviews = _validated_typed_reviews(
            version=version,
            task_types=task_types,
            profile=review_profile,
            source_root=(None if source_roots is None else source_roots.get(version)),
        )
        typed_exclusions = _validated_typed_exclusions(
            version=version,
            task_types=task_types,
            profile=review_profile,
            typed_reviews=typed_reviews,
            source_root=(None if source_roots is None else source_roots.get(version)),
        )
        compiled_profile = {
            "contract_fingerprint": fact_profile["contract_fingerprint"],
            "provenance": copy.deepcopy(fact_profile["provenance"]),
            "task_types": copy.deepcopy(task_types),
            "typed_authoring_reviews": typed_reviews,
        }
        if typed_exclusions:
            compiled_profile["typed_authoring_exclusions"] = typed_exclusions
        profiles[version] = compiled_profile

    return {
        "schema_version": TASK_PROFILE_SCHEMA_VERSION,
        "kind": TASK_PROFILE_DATA_KIND,
        "source_inventory_digest": fact_document["source_inventory_digest"],
        "target_versions": list(selected),
        "profiles": profiles,
    }


def render_task_profile_data(data: Mapping[str, object]) -> str:
    """Render searchable exact decisions with named fields and hex fingerprints."""
    normalized = _validated_compiled_data(data)
    payload = json.dumps(normalized, ensure_ascii=True, indent=2)
    return "\n".join(
        (
            "# ruff: noqa: E501",
            "from __future__ import annotations",
            "",
            "import json as _json",
            "",
            "# Generated by tools/generate_ds_task_profiles.py; do not edit.",
            "# Review ids resolve in tools/ds_codegen/task_profile_reviews.json.",
            "_TASK_PROFILE_JSON = r'''",
            payload,
            "'''",
            "TASK_PROFILE_DATA = _json.loads(_TASK_PROFILE_JSON)",
            "TASK_PROFILE_SCHEMA_VERSION = TASK_PROFILE_DATA['schema_version']",
            "TARGET_DS_VERSIONS = tuple(TASK_PROFILE_DATA['target_versions'])",
            "TASK_PROFILES = TASK_PROFILE_DATA['profiles']",
            "",
        )
    )


def write_task_profile_facts(
    output_path: Path,
    inventory: Mapping[str, object],
) -> dict[str, Any]:
    """Write the deterministic source-controlled projection of an inventory."""
    facts = project_task_profile_facts(inventory)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(facts, ensure_ascii=True, indent=2) + "\n",
        encoding="utf-8",
    )
    return facts


def write_task_profile_data(
    output_path: Path,
    *,
    facts: Mapping[str, object],
    reviews: Mapping[str, object],
    versions: Iterable[str] | None = None,
) -> dict[str, Any]:
    """Write the deterministic publishable Python task-profile module."""
    data = compile_task_profile_data(facts, reviews, versions=versions)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_task_profile_data(data), encoding="utf-8")
    return data


def _validate_inventory_header(inventory: Mapping[str, object]) -> None:
    if inventory.get("schema_version") != TASK_PROFILE_SCHEMA_VERSION:
        message = "task-plugin inventory schema_version must be 1"
        raise ValueError(message)
    if inventory.get("kind") != _INVENTORY_KIND:
        message = "invalid task-plugin inventory kind"
        raise ValueError(message)
    if (
        inventory.get("complete") is not True
        or inventory.get("source_complete") is not True
    ):
        message = "task-plugin inventory must be complete before profile projection"
        raise ValueError(message)
    diagnostics = _list(inventory.get("diagnostics"), label="inventory.diagnostics")
    if diagnostics:
        message = "complete task-plugin inventory must not contain diagnostics"
        raise ValueError(message)


def _project_target(version: str, target: Mapping[str, object]) -> dict[str, Any]:
    if target.get("ds_version") != version:
        message = f"task-plugin target {version} ds_version must match its label"
        raise ValueError(message)
    if target.get("source_complete") is not True:
        message = f"task-plugin target {version} source discovery is incomplete"
        raise ValueError(message)
    contract_fingerprint = _fingerprint(
        target.get("contract_fingerprint"),
        label=f"task-plugin target {version}.contract_fingerprint",
    )
    provenance = _mapping(
        target.get("provenance"),
        label=f"task-plugin target {version}.provenance",
    )
    require_exact_provenance(provenance, label=version)
    raw_task_types = _list(
        target.get("task_types"),
        label=f"task-plugin target {version}.task_types",
    )
    task_types: dict[str, object] = {}
    for raw_task_type in raw_task_types:
        task_type = _project_task_type(version, raw_task_type)
        key = task_type.pop("key")
        if key in task_types:
            message = f"task-plugin target {version} repeats task type {key}"
            raise ValueError(message)
        task_types[key] = task_type
    counts = _mapping(
        target.get("counts"),
        label=f"task-plugin target {version}.counts",
    )
    if counts.get("task_types") != len(task_types):
        message = f"task-plugin target {version} task type count is inconsistent"
        raise ValueError(message)
    if version in {"3.2.0", "3.2.1", "3.2.2"}:
        missing_controls = sorted(_CONTROL_TASKS_3_2 - task_types.keys())
        if missing_controls:
            missing = ", ".join(missing_controls)
            message = (
                f"task-plugin target {version} control task inventory is "
                f"incomplete: {missing}"
            )
            raise ValueError(message)
    return {
        "contract_fingerprint": contract_fingerprint,
        "provenance": copy.deepcopy(provenance),
        "task_types": dict(sorted(task_types.items())),
    }


def _project_task_type(version: str, raw_task_type: object) -> dict[str, str]:
    task_type = _mapping(
        raw_task_type,
        label=f"task-plugin target {version} task type",
    )
    key = _text(task_type.get("key"), label=f"task-plugin target {version} key")
    if key != key.strip().upper():
        message = f"task-plugin target {version} task type must be canonical: {key!r}"
        raise ValueError(message)
    parameter_model_import = _text(
        task_type.get("parameter_model_import"),
        label=f"task-plugin target {version} {key}.parameter_model_import",
    )
    semantic_fingerprint = _fingerprint(
        task_type.get("semantic_fingerprint"),
        label=f"task-plugin target {version} {key}.semantic_fingerprint",
    )
    registration_kind = _text(
        task_type.get("registration_kind"),
        label=f"task-plugin target {version} {key}.registration_kind",
    )
    if registration_kind not in _REGISTRATION_KINDS:
        message = (
            f"task-plugin target {version} {key} has unknown registration kind "
            f"{registration_kind!r}"
        )
        raise ValueError(message)
    return {
        "key": key,
        "parameter_model_import": parameter_model_import,
        "semantic_fingerprint": semantic_fingerprint,
        "registration_kind": registration_kind,
    }


def _validate_fact_document(
    facts: Mapping[str, object],
) -> dict[str, object]:
    if facts.get("schema_version") != TASK_PROFILE_SCHEMA_VERSION:
        message = "task-profile facts schema_version must be 1"
        raise ValueError(message)
    if facts.get("kind") != TASK_PROFILE_FACTS_KIND:
        message = "invalid task-profile facts kind"
        raise ValueError(message)
    _fingerprint(
        facts.get("source_inventory_digest"),
        label="task-profile facts.source_inventory_digest",
    )
    versions = tuple(
        _text(value, label="task-profile facts target version")
        for value in _list(
            facts.get("target_versions"),
            label="task-profile facts.target_versions",
        )
    )
    exact_versions(versions, label="task-profile facts target_versions")
    profiles = _mapping(facts.get("profiles"), label="task-profile facts.profiles")
    _require_target_versions(
        tuple(profiles), expected=versions, label="task-profile facts profiles"
    )
    for version in versions:
        _validate_fact_profile(version, profiles[version])
    return profiles


def _validate_fact_profile(version: str, raw_profile: object) -> None:
    profile = _mapping(raw_profile, label=f"task-profile facts {version}")
    _fingerprint(
        profile.get("contract_fingerprint"),
        label=f"task-profile facts {version}.contract_fingerprint",
    )
    provenance = _mapping(
        profile.get("provenance"),
        label=f"task-profile facts {version}.provenance",
    )
    require_exact_provenance(provenance, label=version)
    task_types = _mapping(
        profile.get("task_types"),
        label=f"task-profile facts {version}.task_types",
    )
    for task_type, raw_fact in task_types.items():
        fact = _mapping(raw_fact, label=f"task-profile facts {version}.{task_type}")
        _text(
            fact.get("parameter_model_import"),
            label=f"task-profile facts {version}.{task_type}.parameter_model_import",
        )
        _fingerprint(
            fact.get("semantic_fingerprint"),
            label=f"task-profile facts {version}.{task_type}.semantic_fingerprint",
        )
        registration_kind = _text(
            fact.get("registration_kind"),
            label=f"task-profile facts {version}.{task_type}.registration_kind",
        )
        if registration_kind not in _REGISTRATION_KINDS:
            message = (
                f"task-profile facts {version}.{task_type} has unknown "
                f"registration kind {registration_kind!r}"
            )
            raise ValueError(message)
    if version in {"3.2.0", "3.2.1", "3.2.2"}:
        missing_controls = sorted(_CONTROL_TASKS_3_2 - task_types.keys())
        if missing_controls:
            missing = ", ".join(missing_controls)
            message = (
                f"task-profile facts {version} control task inventory is "
                f"incomplete: {missing}"
            )
            raise ValueError(message)


def _validate_review_document(
    reviews: Mapping[str, object],
) -> dict[str, object]:
    if reviews.get("schema_version") != TASK_PROFILE_SCHEMA_VERSION:
        message = "task-profile reviews schema_version must be 1"
        raise ValueError(message)
    if reviews.get("kind") != TASK_PROFILE_REVIEWS_KIND:
        message = "invalid task-profile reviews kind"
        raise ValueError(message)
    profiles = _mapping(
        reviews.get("profiles"),
        label="task-profile reviews.profiles",
    )
    exact_versions(profiles, label="task-profile reviews profiles")
    return profiles


def _validate_review_source_tree(
    *,
    version: str,
    fact_profile: Mapping[str, object],
    review_profile: Mapping[str, object],
) -> None:
    expected = review_profile.get("source_tree")
    if not isinstance(expected, str) or _GIT_TREE.fullmatch(expected) is None:
        message = f"task-profile reviews {version}.source_tree must be a Git tree"
        raise ValueError(message)
    provenance = _mapping(
        fact_profile.get("provenance"),
        label=f"task-profile facts {version}.provenance",
    )
    origin = _mapping(
        provenance.get("origin"),
        label=f"task-profile facts {version}.provenance.origin",
    )
    git = _mapping(
        origin.get("git"),
        label=f"task-profile facts {version}.provenance.origin.git",
    )
    actual = git.get("tree")
    if expected != actual:
        message = (
            f"task-profile review {version} source tree is stale: "
            f"expected {expected}, found {actual}"
        )
        raise ValueError(message)


def _validated_typed_reviews(
    *,
    version: str,
    task_types: Mapping[str, object],
    profile: Mapping[str, object],
    source_root: Path | None,
) -> dict[str, object]:
    raw_reviews = _mapping(
        profile.get("typed_authoring"),
        label=f"task-profile reviews {version}.typed_authoring",
    )
    typed_reviews: dict[str, object] = {}
    for source_task_type, raw_review in raw_reviews.items():
        if (
            not isinstance(source_task_type, str)
            or source_task_type != source_task_type.strip().upper()
        ):
            message = f"task-profile review {version} task type must be canonical"
            raise ValueError(message)
        fact = task_types.get(source_task_type)
        if fact is None:
            message = (
                f"task-profile review {version}.{source_task_type} has no exact "
                "upstream task fact"
            )
            raise ValueError(message)
        review = _mapping(
            raw_review,
            label=f"task-profile review {version}.{source_task_type}",
        )
        _validate_review_evidence_paths(
            label=f"task-profile review {version}.{source_task_type}",
            review=review,
            source_root=source_root,
        )
        cli_task_type = review.get("cli_task_type", source_task_type)
        if (
            not isinstance(cli_task_type, str)
            or cli_task_type != cli_task_type.strip().upper()
        ):
            message = (
                f"task-profile review {version}.{source_task_type}.cli_task_type "
                "must be canonical"
            )
            raise ValueError(message)
        if cli_task_type in typed_reviews:
            message = (
                f"task-profile review {version} repeats CLI task type {cli_task_type}"
            )
            raise ValueError(message)
        expected = _fingerprint(
            review.get("semantic_fingerprint"),
            label=(
                f"task-profile review {version}.{source_task_type}.semantic_fingerprint"
            ),
        )
        actual = _mapping(
            fact,
            label=f"task-profile facts {version}.{source_task_type}",
        ).get("semantic_fingerprint")
        if expected != actual:
            message = (
                f"task-profile review {version}.{source_task_type} fingerprint is "
                "stale: "
                f"expected {expected}, found {actual}"
            )
            raise ValueError(message)
        _text(
            review.get("evidence"),
            label=f"task-profile review {version}.{source_task_type}.evidence",
        )
        compiled_review = {
            "semantic_fingerprint": expected,
            "review": _text(
                review.get("review"),
                label=f"task-profile review {version}.{source_task_type}.review",
            ),
            "cli_model": _text(
                review.get("cli_model"),
                label=f"task-profile review {version}.{source_task_type}.cli_model",
            ),
        }
        if source_task_type != cli_task_type:
            compiled_review["source_task_type"] = source_task_type
        typed_reviews[cli_task_type] = compiled_review
    return dict(sorted(typed_reviews.items()))


def _validated_typed_exclusions(
    *,
    version: str,
    task_types: Mapping[str, object],
    profile: Mapping[str, object],
    typed_reviews: Mapping[str, object],
    source_root: Path | None,
) -> dict[str, object]:
    """Validate exact task facts intentionally excluded from typed authoring."""
    raw_exclusions = _mapping(
        profile.get("typed_authoring_exclusions", {}),
        label=f"task-profile reviews {version}.typed_authoring_exclusions",
    )
    reviewed_source_types = {
        _mapping(
            raw_review,
            label=f"compiled task-profile review {version}.{cli_task_type}",
        ).get("source_task_type", cli_task_type)
        for cli_task_type, raw_review in typed_reviews.items()
    }
    exclusions: dict[str, object] = {}
    for task_type, raw_exclusion in raw_exclusions.items():
        if not isinstance(task_type, str) or task_type != task_type.strip().upper():
            message = f"task-profile exclusion {version} task type must be canonical"
            raise ValueError(message)
        if task_type in reviewed_source_types:
            message = (
                f"task-profile exclusion {version}.{task_type} overlaps a positive "
                "typed-authoring review"
            )
            raise ValueError(message)
        fact = task_types.get(task_type)
        if fact is None:
            message = (
                f"task-profile exclusion {version}.{task_type} has no exact "
                "upstream task fact"
            )
            raise ValueError(message)
        exclusion = _mapping(
            raw_exclusion,
            label=f"task-profile exclusion {version}.{task_type}",
        )
        _validate_review_evidence_paths(
            label=f"task-profile exclusion {version}.{task_type}",
            review=exclusion,
            source_root=source_root,
        )
        expected = _fingerprint(
            exclusion.get("semantic_fingerprint"),
            label=(
                f"task-profile exclusion {version}.{task_type}.semantic_fingerprint"
            ),
        )
        actual = _mapping(
            fact,
            label=f"task-profile facts {version}.{task_type}",
        ).get("semantic_fingerprint")
        if expected != actual:
            message = (
                f"task-profile exclusion {version}.{task_type} fingerprint is "
                f"stale: expected {expected}, found {actual}"
            )
            raise ValueError(message)
        _text(
            exclusion.get("evidence"),
            label=f"task-profile exclusion {version}.{task_type}.evidence",
        )
        exclusions[task_type] = {
            "semantic_fingerprint": expected,
            "reason": _text(
                exclusion.get("reason"),
                label=f"task-profile exclusion {version}.{task_type}.reason",
            ),
        }
    return dict(sorted(exclusions.items()))


def _validate_review_evidence_paths(
    *,
    label: str,
    review: Mapping[str, object],
    source_root: Path | None,
) -> None:
    raw_evidence_paths = review.get("evidence_paths")
    if not isinstance(raw_evidence_paths, dict) or not raw_evidence_paths:
        message = f"{label} requires structured evidence paths"
        raise ValueError(message)
    for plane, raw_paths in raw_evidence_paths.items():
        if not isinstance(plane, str) or not plane.strip():
            message = f"{label} evidence plane names must be nonempty strings"
            raise ValueError(message)
        paths = _list(raw_paths, label=f"{label}.evidence_paths.{plane}")
        if not paths:
            message = f"{label} evidence plane {plane!r} requires source paths"
            raise ValueError(message)
        seen: set[str] = set()
        for raw_path in paths:
            if not isinstance(raw_path, str) or not raw_path.strip():
                message = f"{label} evidence plane {plane!r} requires source paths"
                raise ValueError(message)
            parsed = PurePosixPath(raw_path)
            if (
                parsed.is_absolute()
                or parsed == PurePosixPath()
                or ".." in parsed.parts
                or "\\" in raw_path
                or parsed.as_posix() != raw_path
            ):
                message = (
                    f"{label} evidence path must be normalized and "
                    f"repository-relative: {raw_path}"
                )
                raise ValueError(message)
            if raw_path in seen:
                message = f"{label} repeats evidence path {raw_path!r}"
                raise ValueError(message)
            seen.add(raw_path)
            if source_root is not None and not (source_root / raw_path).is_file():
                message = f"{label} evidence path does not exist: {raw_path}"
                raise ValueError(message)


def _validated_compiled_data(data: Mapping[str, object]) -> dict[str, Any]:
    normalized = copy.deepcopy(dict(data))
    if normalized.get("schema_version") != TASK_PROFILE_SCHEMA_VERSION:
        message = "compiled task-profile schema_version must be 1"
        raise ValueError(message)
    if normalized.get("kind") != TASK_PROFILE_DATA_KIND:
        message = "invalid compiled task-profile kind"
        raise ValueError(message)
    _fingerprint(
        normalized.get("source_inventory_digest"),
        label="compiled task-profile source_inventory_digest",
    )
    versions = tuple(
        _text(value, label="compiled task-profile target version")
        for value in _list(
            normalized.get("target_versions"),
            label="compiled task-profile target_versions",
        )
    )
    exact_versions(versions, label="compiled task-profile target_versions")
    profiles = _mapping(
        normalized.get("profiles"),
        label="compiled task-profile profiles",
    )
    _require_target_versions(
        tuple(profiles), expected=versions, label="compiled task-profile profiles"
    )
    for version, profile in profiles.items():
        _validate_fact_profile(version, profile)
        _validate_compiled_claims(version, profile)
        for task_type, fact in profile["task_types"].items():
            unsupported = sorted(
                set(fact)
                - {
                    "parameter_model_import",
                    "semantic_fingerprint",
                    "registration_kind",
                }
            )
            if unsupported:
                message = (
                    f"compiled task-profile {version}.{task_type} has unsupported "
                    f"task-fact fields: {', '.join(unsupported)}"
                )
                raise ValueError(message)
    return normalized


def _validate_compiled_claims(version: str, profile: Mapping[str, object]) -> None:
    for group, text_fields in (
        ("typed_authoring_reviews", ("review", "cli_model")),
        ("typed_authoring_exclusions", ("reason",)),
    ):
        claims = _mapping(profile.get(group, {}), label=f"{version}.{group}")
        for task_type, raw_claim in claims.items():
            label = f"compiled task-profile {version}.{group}.{task_type}"
            claim = _mapping(raw_claim, label=label)
            _fingerprint(
                claim.get("semantic_fingerprint"), label=f"{label}.semantic_fingerprint"
            )
            for field in text_fields:
                _text(claim.get(field), label=f"{label}.{field}")
            if "source_task_type" in claim and not isinstance(
                claim["source_task_type"], str
            ):
                message = f"{label}.source_task_type must be text"
                raise TypeError(message)


def _require_target_versions(
    versions: tuple[str, ...], *, expected: tuple[str, ...], label: str
) -> None:
    if versions == expected:
        return
    missing = sorted(set(expected) - set(versions))
    extra = sorted(set(versions) - set(expected))
    details: list[str] = []
    if missing:
        details.append(f"missing: {', '.join(missing)}")
    if extra:
        details.append(f"unexpected: {', '.join(extra)}")
    if not missing and not extra:
        details.append("versions are out of order")
    message = f"{label} must match declared target_versions ({'; '.join(details)})"
    raise ValueError(message)


def _digest(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _fingerprint(value: object, *, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        message = f"{label} must be a sha256 fingerprint"
        raise ValueError(message)
    return value


def _text(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        message = f"{label} must be a non-empty string"
        raise ValueError(message)
    return value


def _mapping(value: object, *, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        message = f"{label} must be a JSON object"
        raise TypeError(message)
    return cast("dict[str, Any]", value)


def _list(value: object, *, label: str) -> list[Any]:
    if not isinstance(value, list):
        message = f"{label} must be a JSON array"
        raise TypeError(message)
    return value


__all__ = [
    "DEFAULT_TASK_PROFILE_FACTS",
    "DEFAULT_TASK_PROFILE_REVIEWS",
    "GENERATED_TASK_PROFILE_PATH",
    "TASK_PROFILE_DATA_KIND",
    "TASK_PROFILE_FACTS_KIND",
    "TASK_PROFILE_REVIEWS_KIND",
    "TASK_PROFILE_SCHEMA_VERSION",
    "compile_task_profile_data",
    "load_task_profile_document",
    "project_task_profile_facts",
    "render_task_profile_data",
    "write_task_profile_data",
    "write_task_profile_facts",
]
