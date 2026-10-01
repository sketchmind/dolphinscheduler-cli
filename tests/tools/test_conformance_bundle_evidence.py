from __future__ import annotations

import ast
import copy
import hashlib
import importlib
import json
import re
import shutil
import sys
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest
from tests.tools.conformance_bundle_testkit import EvidenceCase

from ds_codegen.compiled_literals import read_compiled_literals
from dsctl import __version__
from dsctl.generated import task_definition_cleanup_profiles as cleanup_profiles
from dsctl.generated import task_definition_profiles as task_profiles

# Independently captured HEADs of the managed read-only exact source checkouts.
_MANAGED_SOURCE_COMMITS = {
    "1.3.9": "174c78c4a90a53fdfe7131e9b065edaa38b7936f",
    "2.0.0": "9bd693b7efd8a20075685968ebe6a5941c22fd28",
    "2.0.4": "0ff01e3c68da6a174836216f87c05f707233f7f9",
    "2.0.7": "bf5a0f228b7858c8a3e682a0caa225e3569db15e",
    "2.0.8": "2a22b960580c907f67128a294df2b10b4380312a",
    "2.0.9": "23302054559410093573fb169336bafeb02db97f",
    "3.0.2": "d2ba3d4cfb3ab9f252340aa603ea036baa52f88c",
    "3.1.2": "f1aefae5e25daa5beef08accd8fbd26c32fb6b47",
}

if TYPE_CHECKING:
    from types import ModuleType
    from typing import Protocol, TypedDict

    from live_gate.conformance_evidence.types import _ImageContract

    class _ExpectedIdentity(TypedDict, total=False):
        expected_ds_version: str
        expected_bundle: str
        expected_wheel_filename: str
        expected_wheel_sha256: str

    class _EvidenceSummary(Protocol):
        schema_version: int
        ds_version: str
        bundle: str
        wheel_filename: str
        wheel_sha256: str
        receipt_digest: str
        required_actions: tuple[str, ...]
        authoring_mode: str

    class _EvidenceValidator(Protocol):
        def validate(
            self,
            receipt: object,
            *,
            expected_ds_version: str | None = None,
            expected_bundle: str | None = None,
            expected_wheel_filename: str | None = None,
            expected_wheel_sha256: str | None = None,
        ) -> _EvidenceSummary: ...


_FULL_TASK_DEFINITION_CLEANUP_VERSIONS = (
    cleanup_profiles.FULL_CORE_TASK_DEFINITION_CLEANUP_VERSIONS
)
_FULL_TASK_DEFINITION_RECONCILIATION_VERSIONS = (
    cleanup_profiles.FULL_CORE_TASK_DEFINITION_RECONCILIATION_VERSIONS
)
_TASK_DEFINITION_CLEANUP_ACTION = (
    cleanup_profiles.TASK_DEFINITION_CLEANUP_SEMANTIC_OPERATION
)
_TASK_DEFINITION_PROVE_ACTION = (
    _TASK_DEFINITION_CLEANUP_ACTION.removesuffix(".cleanup") + ".prove"
)
_DEPENDENCY_UPDATE_UPSTREAM_LIMITED_VERSIONS = tuple(
    ds_version
    for ds_version, profile in task_profiles.TASK_DEFINITION_PROFILES.items()
    if (
        profile["update_executable"] is True
        or (profile["executable"] is True and profile["whole_workflow_update"] is True)
    )
    and profile["dependency_update"] is False
)
_TASK_RECONCILIATION_STRATEGIES = {
    ds_version: cleanup_profiles.TASK_DEFINITION_CLEANUP_PROFILES[ds_version][
        "strategy"
    ]
    for ds_version in _FULL_TASK_DEFINITION_RECONCILIATION_VERSIONS
}
_TASK_PRE_DELETE_RELEASE_VERSIONS = frozenset(
    ds_version
    for ds_version, profile in cleanup_profiles.TASK_DEFINITION_CLEANUP_PROFILES.items()
    if ds_version in _FULL_TASK_DEFINITION_CLEANUP_VERSIONS
    and profile.get("pre_delete_release") == "offline"
)


_LEGACY_ACTIONS = (
    "doctor",
    "project.create",
    "project.delete",
    "project.get",
    "project.list",
    "project.update",
    "schedule.list",
    "workflow.get",
    "workflow.list",
)
_FULL_ACTIONS = (
    "doctor",
    "project.create",
    "project.delete",
    "project.get",
    "project.list",
    "project.update",
    "schedule.list",
    "task.get",
    "task.list",
    "task.update",
    "workflow.create",
    "workflow.delete",
    "workflow.describe",
    "workflow.digest",
    "workflow.edit",
    "workflow.export",
    "workflow.get",
    "workflow.list",
)
_LEGACY_OUTCOMES = (
    "bound-current-user",
    "capability-preflight-complete",
    "external-fixture-cross-checked",
    "gate-owned-project-round-trip",
    "negative-errors-translated",
)
_FULL_OUTCOMES = (
    *_LEGACY_OUTCOMES[:-1],
    "gate-owned-workflow-round-trip",
    "workflow-dag-cross-checked",
    "task-update-round-trip",
    "workflow-edit-round-trip",
    "full-cleanup-zero",
    _LEGACY_OUTCOMES[-1],
)
_TARGET_VERSIONS = (
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


def test_schema_one_accepts_a_digest_bound_legacy_bundle_receipt() -> None:
    evidence = _load_module()
    receipt = _receipt()

    summary = evidence.validate_conformance_bundle_evidence(
        receipt,
        expected_ds_version="3.2.2",
        expected_bundle="legacy_core/v1",
        expected_wheel_filename=f"dolphinscheduler_cli-{__version__}-py3-none-any.whl",
        expected_wheel_sha256="sha256:" + "a" * 64,
    )

    assert summary == evidence.ConformanceBundleEvidenceSummary(
        schema_version=1,
        ds_version="3.2.2",
        bundle="legacy_core/v1",
        wheel_filename=f"dolphinscheduler_cli-{__version__}-py3-none-any.whl",
        wheel_sha256="sha256:" + "a" * 64,
        receipt_digest=receipt["receipt_digest"],
        required_actions=_LEGACY_ACTIONS,
        authoring_mode="typed",
    )


@pytest.mark.parametrize(
    "target",
    [
        "profile",
        "assessment",
        "manifest",
        "image_contract",
        "task_cleanup",
        "task_update",
    ],
)
def test_source_root_generated_truth_is_not_executed(
    tmp_path: Path,
    target: str,
) -> None:
    source_root = _copy_current_source_truth(tmp_path)
    marker = tmp_path / f"generated-{target}-executed"
    generated_path = _generated_truth_path(source_root, target=target)
    generated_path.write_text(
        generated_path.read_text(encoding="utf-8")
        + f"\n__import__('pathlib').Path({str(marker)!r}).write_text('executed')\n",
        encoding="utf-8",
    )

    summary = _case(source_root=source_root).validate()

    assert summary.ds_version == "3.2.2"
    assert not marker.exists()


@pytest.mark.parametrize(
    "target",
    [
        "profile",
        "assessment",
        "manifest",
        "image_contract",
        "task_cleanup",
        "task_update",
    ],
)
def test_source_root_malicious_literal_call_fails_without_execution(
    tmp_path: Path,
    target: str,
) -> None:
    source_root = _copy_current_source_truth(tmp_path)
    marker = tmp_path / f"malicious-{target}-executed"
    generated_path = _generated_truth_path(source_root, target=target)
    assignment = {
        "profile": "_PROFILE_JSON",
        "assessment": "_CONFORMANCE_BUNDLE_JSON",
        "manifest": "DS_VERSION",
        "image_contract": "_CONFORMANCE_IMAGE_CONTRACT_JSON",
        "task_cleanup": "_TASK_DEFINITION_CLEANUP_PROFILE_JSON",
        "task_update": "_TASK_DEFINITION_PROFILE_JSON",
    }[target]
    generated_path.write_text(
        f"{assignment} = "
        f"__import__('pathlib').Path({str(marker)!r}).write_text('executed')\n",
        encoding="utf-8",
    )

    _case(source_root=source_root).assert_rejected("literal assignment")

    assert not marker.exists()


def test_source_root_image_contract_is_authoritative(tmp_path: Path) -> None:
    source_root = _copy_current_source_truth(tmp_path)
    contract_path = _generated_truth_path(source_root, target="image_contract")
    contract = _load_embedded_json(
        contract_path,
        name="_CONFORMANCE_IMAGE_CONTRACT_JSON",
    )
    repositories = contract["api_image_repositories"]
    assert isinstance(repositories, dict)
    repositories["3.2.2"] = "mirror/dolphinscheduler-api"
    _write_embedded_json(
        contract_path,
        name="_CONFORMANCE_IMAGE_CONTRACT_JSON",
        payload=contract,
    )

    _case(source_root=source_root).assert_rejected("image reference")


def test_source_root_task_update_dependency_policy_is_authoritative(
    tmp_path: Path,
) -> None:
    source_root = _copy_current_source_truth(tmp_path)
    profile_path = _generated_truth_path(source_root, target="task_update")
    data = _load_embedded_json(
        profile_path,
        name="_TASK_DEFINITION_PROFILE_JSON",
    )
    profile_specs = data["profile_specs"]
    profile_pool = data["profile_pool"]
    assert isinstance(profile_specs, dict)
    assert isinstance(profile_pool, list)
    profile_index = profile_specs["3.2.0"]
    assert isinstance(profile_index, int)
    profile = profile_pool[profile_index]
    assert isinstance(profile, dict)
    profile["dependency_update"] = True
    _write_embedded_json(
        profile_path,
        name="_TASK_DEFINITION_PROFILE_JSON",
        payload=data,
    )

    _case(
        bundle_name="full_core/v1",
        ds_version="3.2.0",
        source_root=source_root,
    ).assert_rejected("dependency")


def test_source_root_task_cleanup_applicability_is_authoritative(
    tmp_path: Path,
) -> None:
    source_root = _copy_current_source_truth(tmp_path)
    contract_path = _generated_truth_path(source_root, target="task_cleanup")
    contract = _load_embedded_json(
        contract_path,
        name="_TASK_DEFINITION_CLEANUP_PROFILE_JSON",
    )
    full_core_versions = contract["full_core_versions"]
    assert isinstance(full_core_versions, list)
    full_core_versions.remove("3.0.6")
    _write_embedded_json(
        contract_path,
        name="_TASK_DEFINITION_CLEANUP_PROFILE_JSON",
        payload=contract,
    )

    _case(
        bundle_name="full_core/v1",
        ds_version="3.0.6",
        source_root=source_root,
    ).assert_rejected("remote_mutations")


@pytest.mark.parametrize("drift", [None, "missing", "unexpected"])
def test_task_cleanup_applicability_requires_verified_runtime_ownership(
    drift: str | None,
) -> None:
    _load_module()
    current_truth = importlib.import_module(
        "live_gate.conformance_evidence.current_truth"
    )
    current = current_truth._load_current_truth(Path(__file__).resolve().parents[2])
    operation = "release-gate.task-definition.cleanup"
    assert operation not in current.contracts["3.1.0"]["semantic_operations"]
    assert operation in current.runtime_operations["3.1.0"]
    operations = dict(current.runtime_operations)
    if drift == "missing":
        operations["3.1.0"] = operations["3.1.0"] - {operation}
    elif drift == "unexpected":
        operations["3.2.0"] = operations["3.2.0"] | {operation}
    if drift is None:
        contract = current_truth._validate_current_task_cleanup_contract(
            cleanup_profiles.TASK_DEFINITION_CLEANUP_PROFILE_DATA,
            versions=current.versions,
            runtime_operations=operations,
        )
        assert contract.semantic_operation == operation
    else:
        with pytest.raises(ValueError, match="cleanup root and runtime ownership"):
            current_truth._validate_current_task_cleanup_contract(
                cleanup_profiles.TASK_DEFINITION_CLEANUP_PROFILE_DATA,
                versions=current.versions,
                runtime_operations=operations,
            )


def test_private_cleanup_release_and_full_core_membership_are_independent() -> None:
    _load_module()
    current_truth = importlib.import_module(
        "live_gate.conformance_evidence.current_truth"
    )
    current = current_truth._load_current_truth(Path(__file__).resolve().parents[2])
    cleanup = current_truth._validate_current_task_cleanup_contract(
        cleanup_profiles.TASK_DEFINITION_CLEANUP_PROFILE_DATA,
        versions=current.versions,
        runtime_operations=current.runtime_operations,
    )
    assert "2.0.1" in cleanup.pre_delete_release_versions
    assert {"2.0.0", "2.0.1"} <= cleanup.full_core_versions
    assert "2.0.0" not in cleanup.pre_delete_release_versions
    assert {"2.0.0", "2.0.1"} <= cleanup.cross_process_recovery_versions

    drifted = copy.deepcopy(cleanup_profiles.TASK_DEFINITION_CLEANUP_PROFILE_DATA)
    drifted["profiles"]["3.1.9"]["pre_delete_release"] = "offline"
    with pytest.raises(ValueError, match="strategy structure drifted"):
        current_truth._validate_current_task_cleanup_contract(
            drifted,
            versions=current.versions,
            runtime_operations=current.runtime_operations,
        )


def test_source_root_task_cleanup_recipe_is_authoritative(tmp_path: Path) -> None:
    source_root = _copy_current_source_truth(tmp_path)
    contract_path = _generated_truth_path(source_root, target="task_cleanup")
    contract = _load_embedded_json(
        contract_path,
        name="_TASK_DEFINITION_CLEANUP_PROFILE_JSON",
    )
    profiles = contract["profiles"]
    assert isinstance(profiles, dict)
    profile = profiles["2.0.9"]
    assert isinstance(profile, dict)
    profile["page_model"] = "candidate.generated.TaskPageRow"
    profile["row_fields"] = [
        "candidateCode",
        "candidateName",
        "candidateVersion",
    ]
    _write_embedded_json(
        contract_path,
        name="_TASK_DEFINITION_CLEANUP_PROFILE_JSON",
        payload=contract,
    )

    summary = _case(source_root=source_root).validate()

    assert summary.ds_version == "3.2.2"


def test_source_root_proof_only_recipe_fields_are_authoritative(tmp_path: Path) -> None:
    source_root = _copy_current_source_truth(tmp_path)
    contract_path = _generated_truth_path(source_root, target="task_cleanup")
    contract = _load_embedded_json(
        contract_path,
        name="_TASK_DEFINITION_CLEANUP_PROFILE_JSON",
    )
    profiles = contract["profiles"]
    assert isinstance(profiles, dict)
    profile = profiles["3.1.9"]
    assert isinstance(profile, dict)
    profile["workflow_binding_fields"] = [
        "candidateWorkflowCode",
        "candidateWorkflowVersion",
        "candidateWorkflowName",
        "candidateWorkflowState",
    ]
    profile["history_model"] = "candidate.generated.TaskHistory"
    _write_embedded_json(
        contract_path,
        name="_TASK_DEFINITION_CLEANUP_PROFILE_JSON",
        payload=contract,
    )

    summary = _case(source_root=source_root).validate()

    assert summary.ds_version == "3.2.2"


def test_source_root_rejects_malformed_proof_only_recipe_structure(
    tmp_path: Path,
) -> None:
    source_root = _copy_current_source_truth(tmp_path)
    contract_path = _generated_truth_path(source_root, target="task_cleanup")
    contract = _load_embedded_json(
        contract_path,
        name="_TASK_DEFINITION_CLEANUP_PROFILE_JSON",
    )
    profiles = contract["profiles"]
    assert isinstance(profiles, dict)
    profile = profiles["3.1.9"]
    assert isinstance(profile, dict)
    profile["workflow_binding_fields"] = ["code", "version", "name"]
    _write_embedded_json(
        contract_path,
        name="_TASK_DEFINITION_CLEANUP_PROFILE_JSON",
        payload=contract,
    )

    _case(source_root=source_root).assert_rejected("strategy structure drifted")


def test_source_root_rejects_a_second_profile_payload_assignment(
    tmp_path: Path,
) -> None:
    source_root = _copy_current_source_truth(tmp_path)
    profile_path = source_root / "src" / "dsctl" / "generated" / "version_profiles.py"
    profile_path.write_text(
        profile_path.read_text(encoding="utf-8") + "\n_PROFILE_JSON: str = '{}'\n",
        encoding="utf-8",
    )

    _case(source_root=source_root).assert_rejected(r"_PROFILE_JSON.*literal assignment")


def test_source_root_rejects_a_second_assessment_payload_assignment(
    tmp_path: Path,
) -> None:
    source_root = _copy_current_source_truth(tmp_path)
    assessment_path = _generated_truth_path(source_root, target="assessment")
    assessment_path.write_text(
        assessment_path.read_text(encoding="utf-8")
        + "\n_CONFORMANCE_BUNDLE_JSON = '{}'\n",
        encoding="utf-8",
    )

    _case(source_root=source_root).assert_rejected(
        r"_CONFORMANCE_BUNDLE_JSON.*more than once"
    )


def test_source_root_rejects_a_second_manifest_field_assignment(
    tmp_path: Path,
) -> None:
    source_root = _copy_current_source_truth(tmp_path)
    manifest_path = _manifest_path(source_root, ds_version="3.2.2")
    manifest_path.write_text(
        manifest_path.read_text(encoding="utf-8") + "\nDS_VERSION: str = '3.2.2'\n",
        encoding="utf-8",
    )

    _case(source_root=source_root).assert_rejected(r"DS_VERSION.*literal assignment")


def test_source_root_pyproject_version_is_authoritative(tmp_path: Path) -> None:
    source_root = _copy_current_source_truth(tmp_path)
    case = _case(source_root=source_root)
    runner = case.mapping("runner")
    cli_version = runner["cli_version"]
    assert isinstance(cli_version, str)
    pyproject = source_root / "pyproject.toml"
    source = pyproject.read_text(encoding="utf-8")
    version_assignment = f'version = "{cli_version}"'
    assert source.count(version_assignment) == 1
    pyproject.write_text(
        source.replace(version_assignment, 'version = "9.9.9"'),
        encoding="utf-8",
    )

    case.assert_rejected("runner cli_version")


def test_source_root_profile_facts_are_authoritative(tmp_path: Path) -> None:
    source_root = _copy_current_source_truth(tmp_path)
    profile_path = _generated_truth_path(source_root, target="profile")
    data = _load_embedded_json(profile_path, name="_PROFILE_JSON")
    metadata = data["profile_metadata"]
    assert isinstance(metadata, dict)
    profile = metadata["3.2.2"]
    assert isinstance(profile, dict)
    profile["family"] = "candidate-stale-family"
    _write_embedded_json(profile_path, name="_PROFILE_JSON", payload=data)

    _case(source_root=source_root).assert_rejected("profile family")


def test_source_root_manifest_facts_are_authoritative(tmp_path: Path) -> None:
    source_root = _copy_current_source_truth(tmp_path)
    case = _case(source_root=source_root)
    manifest_path = _manifest_path(source_root, ds_version="3.2.2")
    source = manifest_path.read_text(encoding="utf-8")
    current_digest = case.mapping("contract")
    digest = current_digest["source_contract_digest"]
    assert isinstance(digest, str)
    assert source.count(digest) == 1
    manifest_path.write_text(
        source.replace(digest, "sha256:" + "f" * 64),
        encoding="utf-8",
    )

    case.assert_rejected("source identity")


def test_source_root_requires_current_bundle_manifest_schema(tmp_path: Path) -> None:
    source_root = _copy_current_source_truth(tmp_path)
    manifest_path = _manifest_path(source_root, ds_version="3.2.2")
    source = manifest_path.read_text(encoding="utf-8")
    current = "BUNDLE_MANIFEST_SCHEMA_VERSION = 2"
    assert source.count(current) == 1
    manifest_path.write_text(
        source.replace(current, "BUNDLE_MANIFEST_SCHEMA_VERSION = 1"),
        encoding="utf-8",
    )

    _case(source_root=source_root).assert_rejected("generated bundle manifest schema")


def test_public_canonical_digest_excludes_only_the_receipt_digest_field() -> None:
    evidence = _load_module()

    digest = evidence.canonical_conformance_bundle_receipt_digest(
        {"a": 1, "receipt_digest": "ignored"}
    )

    assert digest == (
        "sha256:015abd7f5cc57a2dd94b7590f04ad8084273905ee33ec5cebeae62276a97f862"
    )


def test_public_current_assessment_exposes_exact_validated_bundle_coordinates() -> None:
    evidence = _load_module()

    assessment = evidence.load_conformance_bundle_assessment()

    assert assessment.versions == _TARGET_VERSIONS
    assert tuple((bundle.name, bundle.extends) for bundle in assessment.bundles) == (
        ("legacy_core/v1", ()),
        ("full_core/v1", ("legacy_core/v1",)),
    )
    legacy, full = assessment.bundles
    assert tuple(legacy.coordinates.items()) == tuple(
        (version, "ready") for version in _TARGET_VERSIONS
    )
    assert tuple(full.coordinates.values()) == tuple(
        "ready" for _version in _TARGET_VERSIONS
    )


def test_public_current_assessment_never_executes_generated_source(
    tmp_path: Path,
) -> None:
    evidence = _load_module()
    source_root = _copy_current_source_truth(tmp_path)
    marker = tmp_path / "assessment-loader-executed"
    assessment_path = _generated_truth_path(source_root, target="assessment")
    assessment_path.write_text(
        assessment_path.read_text(encoding="utf-8")
        + f"\n__import__('pathlib').Path({str(marker)!r}).write_text('executed')\n",
        encoding="utf-8",
    )

    assessment = evidence.load_conformance_bundle_assessment(
        source_root=source_root,
    )

    assert assessment.versions == _TARGET_VERSIONS
    assert not marker.exists()


def test_public_current_assessment_coordinates_are_read_only() -> None:
    evidence = _load_module()
    assessment = evidence.load_conformance_bundle_assessment()
    coordinates = cast("dict[str, str]", assessment.bundles[0].coordinates)

    with pytest.raises(TypeError):
        coordinates["1.3.9"] = "blocked"


def test_prepared_validator_pins_truth_while_one_shot_reloads_it(
    tmp_path: Path,
) -> None:
    evidence = _load_module()
    source_root = _copy_current_source_truth(tmp_path)
    receipt = _receipt()
    validator = evidence.ConformanceBundleEvidenceValidator.load(
        source_root=source_root,
    )

    before = validator.validate(receipt)
    pyproject_path = source_root / "pyproject.toml"
    pyproject = pyproject_path.read_text(encoding="utf-8")
    next_version = "9.9.9"
    assert next_version != __version__
    version_assignment = f'version = "{__version__}"'
    assert pyproject.count(version_assignment) == 1
    pyproject_path.write_text(
        pyproject.replace(version_assignment, f'version = "{next_version}"', 1),
        encoding="utf-8",
    )

    after = validator.validate(receipt)
    assert after == before
    with pytest.raises(
        ValueError, match=rf"runner cli_version must equal '{re.escape(next_version)}'"
    ):
        evidence.validate_conformance_bundle_evidence(
            receipt,
            source_root=source_root,
        )


def test_schema_one_rejects_unknown_top_level_fields() -> None:
    case = _case()
    case.receipt["promotion"] = "full"
    case.assert_rejected("evidence keys differ", rehash=True)


@pytest.mark.parametrize(
    "location",
    [
        "runner",
        "profile",
        "contract",
        "conformance_bundle",
        "scenario",
        "fixture",
        "effects",
        "cleanup",
        "evidence_scope",
        "operation_trace",
    ],
)
def test_schema_one_rejects_unknown_nested_fields(location: str) -> None:
    case = _case()
    nested = (
        case.mapping(location, 0)
        if location == "operation_trace"
        else case.mapping(location)
    )
    nested["unexpected"] = "value"
    case.assert_rejected(r"keys differ|fields differ", rehash=True)


@pytest.mark.parametrize(
    ("field", "invalid"),
    [
        ("schema_version", 2),
        ("sanitization_schema_version", 2),
        ("gate", "legacy-read"),
        ("status", "failed"),
        ("secrets_recorded", True),
    ],
)
def test_schema_one_rejects_nonpassing_or_different_contract_constants(
    field: str,
    invalid: object,
) -> None:
    case = _case()
    case.receipt[field] = invalid
    case.assert_rejected(field, rehash=True)


def test_receipt_digest_binds_every_other_schema_field() -> None:
    case = _case()
    changed_version = "9.9.9"
    assert changed_version != case.mapping("runner")["cli_version"]
    case.mapping("runner")["cli_version"] = changed_version
    case.assert_rejected("receipt_digest")


@pytest.mark.parametrize(
    "sensitive",
    [
        "Bearer super-secret",
        "DS_API_TOKEN=super-secret",
        "password=super-secret",
        "https://dolphinscheduler.example.test/api",
    ],
)
def test_receipt_rejects_sensitive_or_target_text(sensitive: str) -> None:
    case = _case()
    case.mapping("fixture")["provisioner"] = sensitive
    case.assert_rejected(r"sensitive text|URLs", rehash=True)


def test_receipt_rejects_sensitive_keys_even_when_the_value_is_redacted() -> None:
    case = _case()
    case.mapping("fixture")["project_name"] = "REDACTED"
    case.assert_rejected("sensitive field", rehash=True)


def test_validation_is_independent_of_a_particular_wheel_filename() -> None:
    case = _case()
    runner = case.mapping("runner")
    runner["wheel_filename"] = "candidate-renamed.whl"
    case.refresh_receipt(case.receipt)
    summary = case.validate(
        expected_wheel_sha256="sha256:" + "a" * 64,
    )

    assert summary.wheel_filename == "candidate-renamed.whl"


@pytest.mark.parametrize(
    ("expected", "message"),
    [
        ({"expected_ds_version": "3.4.2"}, "DS version"),
        ({"expected_bundle": "full_core/v1"}, "bundle"),
        ({"expected_wheel_filename": "different.whl"}, "wheel filename"),
        ({"expected_wheel_sha256": "sha256:" + "f" * 64}, "wheel sha256"),
    ],
)
def test_optional_expected_identities_fail_closed(
    expected: _ExpectedIdentity,
    message: str,
) -> None:
    _case().assert_rejected(message, **expected)


@pytest.mark.parametrize(
    ("field", "invalid", "message"),
    [
        ("artifact", "source-tree", "runner artifact"),
        ("cli_version", "", "runner cli_version"),
        ("wheel_filename", "../candidate.whl", "wheel_filename"),
        ("wheel_sha256", "sha256:abc", "wheel_sha256"),
    ],
)
def test_runner_identity_is_strict(
    field: str,
    invalid: object,
    message: str,
) -> None:
    case = _case()
    case.mapping("runner")[field] = invalid
    case.assert_rejected(message, error=(TypeError, ValueError), rehash=True)


def test_runner_cli_version_is_bound_to_the_current_project_version() -> None:
    case = _case()
    case.mapping("runner")["cli_version"] = "9.9.9"
    case.assert_rejected("runner cli_version", rehash=True)


@pytest.mark.parametrize(
    ("field", "invalid", "message"),
    [
        ("image_ref", "dsmatrix-local/dolphinscheduler:3.4.2", "image reference"),
        ("image_id", "sha256:abc", "image ID"),
        ("api_target_hmac_sha256", "sha256:" + "c" * 64, "HMAC-SHA256"),
        ("principal_hmac_sha256", "hmac-sha256:abc", "HMAC-SHA256"),
        ("persona", "admin", "persona"),
    ],
)
def test_cluster_identity_is_exact(
    field: str,
    invalid: object,
    message: str,
) -> None:
    case = _case()
    case.mapping("dolphinscheduler")[field] = invalid
    case.assert_rejected(message, error=(TypeError, ValueError), rehash=True)


@pytest.mark.parametrize(
    "attack",
    [
        "ssh://operator@internal-host/image",
        "file:///private/tmp/image-inspection.json",
        "jdbc:postgresql://db.internal/runtime",
        "/private/tmp/image-inspection.json",
        "../private/image-inspection.json",
        "operator@internal-host",
        "operator:credential@internal-host",
        "token-deadbeef",
    ],
)
def test_cluster_image_source_rejects_high_entropy_or_target_shaped_text(
    attack: str,
) -> None:
    case = _case()
    case.mapping("dolphinscheduler")["image_source"] = attack
    case.assert_rejected("image_source", rehash=True)


def test_receipt_accepts_pinned_digest_with_multiple_repository_digests() -> None:
    case = _case(ds_version="2.0.5")
    cluster = case.mapping("dolphinscheduler")
    provenance = case.mapping("dolphinscheduler", "image_provenance")
    repository = "apache/dolphinscheduler"
    selected_digest = (
        "sha256:e1303513da517f594132a03d772dc29465a83d0e5fbc0297f97c1752c2e16a09"
    )
    selected = f"{repository}@{selected_digest}"
    cluster["image_ref"] = f"{repository}:2.0.5@{selected_digest}"
    provenance["repo_digests"] = [
        repository
        + "@sha256:6191c39acc35dc0afb8cfffdbea375e6ab2f4ee94244e40c3bef39973a1a51eb",
        selected,
    ]
    provenance["selected_repo_digest"] = selected
    case.refresh_receipt(case.receipt)

    assert case.validate().ds_version == "2.0.5"


def test_receipt_rejects_tag_only_multiple_repository_digests() -> None:
    case = _case(ds_version="2.0.5")
    provenance = case.mapping("dolphinscheduler", "image_provenance")
    provenance["repo_digests"] = [
        "apache/dolphinscheduler@sha256:" + "f" * 64,
        provenance["selected_repo_digest"],
    ]
    case.assert_rejected("one selected API RepoDigest", rehash=True)


@pytest.mark.parametrize("failure", ["wrong-pin", "wrong-repository"])
def test_receipt_rejects_pinned_digest_not_equal_to_selected(failure: str) -> None:
    case = _case(ds_version="2.0.5")
    cluster = case.mapping("dolphinscheduler")
    provenance = case.mapping("dolphinscheduler", "image_provenance")
    repository = "apache/dolphinscheduler"
    cluster["image_ref"] = repository + ":2.0.5@sha256:" + "f" * 64
    provenance["repo_digests"] = [
        repository + "@sha256:" + "f" * 64,
        provenance["selected_repo_digest"],
    ]
    if failure == "wrong-repository":
        selected = "mirror.example/dolphinscheduler@sha256:" + "e" * 64
        repo_digests = cast("list[object]", provenance["repo_digests"])
        provenance["repo_digests"] = [repo_digests[0], selected]
        provenance["selected_repo_digest"] = selected
    case.assert_rejected("published digest differs from selected", rehash=True)


@pytest.mark.parametrize(
    "ds_version",
    ["1.3.9", "2.0.0", "2.0.4", "2.0.7", "2.0.8", "2.0.9", "3.0.2", "3.1.2"],
)
def test_validator_accepts_managed_unified_image_provenance(
    ds_version: str,
) -> None:
    case = _case(ds_version=ds_version)
    cluster = case.mapping("dolphinscheduler")
    cluster["image_provenance"] = _managed_provenance(ds_version=ds_version)
    case.refresh_receipt(case.receipt)
    summary = case.validate()

    assert summary.ds_version == ds_version


@pytest.mark.parametrize(
    "ds_version",
    ["1.3.9", "2.0.0", "2.0.4", "2.0.7", "2.0.8", "2.0.9", "3.0.2", "3.1.2"],
)
def test_independent_image_provenance_paths_accept_exact_managed_evidence(
    ds_version: str,
) -> None:
    shared = importlib.import_module("live_gate.conformance_image_provenance")
    verifier = importlib.import_module("live_gate.conformance_evidence.provenance")
    provenance = _managed_provenance(ds_version=ds_version)
    lock = cast("dict[str, object]", provenance["lock"])
    shared.validate_image_provenance(
        provenance,
        image_ref=cast("str", lock["image_ref"]),
        image_id=cast("str", lock["image_id"]),
        ds_version=ds_version,
    )
    verifier._validate_receipt_managed_provenance(
        provenance,
        image_ref=cast("str", lock["image_ref"]),
        image_id=cast("str", lock["image_id"]),
        release=ds_version,
        image_contract=_receipt_image_contract(),
    )


@pytest.mark.parametrize("ds_version", tuple(_MANAGED_SOURCE_COMMITS))
def test_validators_reject_matching_forged_managed_source_commit(
    ds_version: str,
) -> None:
    shared = importlib.import_module("live_gate.conformance_image_provenance")
    verifier = importlib.import_module("live_gate.conformance_evidence.provenance")
    provenance = _managed_provenance(ds_version=ds_version)
    lock = cast("dict[str, object]", provenance["lock"])
    actual = cast("dict[str, object]", provenance["actual"])
    lock["source_commit"] = "f" * 40
    actual["labels"] = _managed_labels_from_lock(ds_version=ds_version, lock=lock)

    with pytest.raises(ValueError, match="source commit differs"):
        shared.validate_image_provenance(
            provenance,
            image_ref=cast("str", lock["image_ref"]),
            image_id=cast("str", lock["image_id"]),
            ds_version=ds_version,
        )
    # Structural/label verification succeeds; the independent source binding rejects.
    verifier._validate_receipt_managed_provenance(
        provenance,
        image_ref=cast("str", lock["image_ref"]),
        image_id=cast("str", lock["image_id"]),
        release=ds_version,
        image_contract=_receipt_image_contract(),
    )
    with pytest.raises(ValueError, match="source commit differs"):
        verifier._validate_receipt_image_source(
            provenance, source_commit=_MANAGED_SOURCE_COMMITS[ds_version]
        )


@pytest.mark.parametrize("ds_version", tuple(_MANAGED_SOURCE_COMMITS))
def test_full_receipt_rejects_matching_forged_managed_source_after_rehash(
    ds_version: str,
) -> None:
    case = _case(ds_version=ds_version)
    cluster = case.mapping("dolphinscheduler")
    provenance = _managed_provenance(ds_version=ds_version)
    lock = cast("dict[str, object]", provenance["lock"])
    actual = cast("dict[str, object]", provenance["actual"])
    lock["source_commit"] = "f" * 40
    actual["labels"] = _managed_labels_from_lock(ds_version=ds_version, lock=lock)
    cluster["image_provenance"] = provenance
    case.assert_rejected("source commit differs", rehash=True)


@pytest.mark.parametrize(
    "ds_version", ["2.0.4", "2.0.7", "2.0.8", "2.0.9", "3.0.2", "3.1.2"]
)
@pytest.mark.parametrize(
    "mutation",
    [
        "version",
        "source-commit",
        "source-sha512",
        "binary-sha512",
        "base",
        "missing",
        "extra",
    ],
)
def test_validators_independently_reject_managed_release_label_drift(
    ds_version: str,
    mutation: str,
) -> None:
    shared = importlib.import_module("live_gate.conformance_image_provenance")
    case = _case(ds_version=ds_version)
    cluster = case.mapping("dolphinscheduler")
    provenance = _managed_provenance(ds_version=ds_version)
    actual = cast("dict[str, object]", provenance["actual"])
    labels = cast("dict[str, object]", actual["labels"])
    prefix = "org.apache.dolphinscheduler.matrix."
    if mutation == "version":
        labels[prefix + "source-tag"] = "1.3.9"
    elif mutation == "source-commit":
        labels[prefix + "source-commit"] = "9" * 40
    elif mutation in {"source-sha512", "binary-sha512"}:
        labels[prefix + mutation] = "9" * 128
    elif mutation == "base":
        labels[prefix + "base-digest"] = "mirror.example/base@sha256:" + "9" * 64
    elif mutation == "missing":
        labels.pop(prefix + "source-commit")
    else:
        labels[prefix + "unreviewed"] = "extra"

    with pytest.raises(ValueError, match="labels"):
        shared.validate_image_provenance(
            provenance,
            image_ref=cast("str", cluster["image_ref"]),
            image_id=cast("str", cluster["image_id"]),
            ds_version=ds_version,
        )
    verifier = importlib.import_module("live_gate.conformance_evidence.provenance")
    with pytest.raises(ValueError, match="labels"):
        verifier._validate_receipt_managed_provenance(
            provenance,
            image_ref=cast("str", cluster["image_ref"]),
            image_id=cast("str", cluster["image_id"]),
            release=ds_version,
            image_contract=_receipt_image_contract(),
        )


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("missing-archive-digest", "archive verification"),
        ("wrong-archive-digest", "observed archive digest"),
        ("dirty-management", "worktree must be clean"),
        ("wrong-lock-digest", "lock file digest"),
        ("wrong-published-repository", "wrong API repository"),
        ("injected-139-label", "1.3.9.*must not claim"),
        ("path-shaped-base-manifest", "invalid format"),
        ("windows-archive-path", "safe tar basename"),
        ("mismatched-tag-digest", "published digest differs"),
    ],
)
def test_validator_independently_rejects_forged_managed_image_provenance(
    mutation: str,
    message: str,
) -> None:
    shared = importlib.import_module("live_gate.conformance_image_provenance")
    case = _case(ds_version="1.3.9")
    cluster = case.mapping("dolphinscheduler")
    provenance = _managed_provenance(ds_version="1.3.9")
    actual = cast("dict[str, object]", provenance["actual"])
    management = cast("dict[str, object]", provenance["management"])
    lock = cast("dict[str, object]", provenance["lock"])
    if mutation == "missing-archive-digest":
        actual["archive_sha256"] = None
    elif mutation == "wrong-archive-digest":
        actual["archive_sha256"] = "sha256:" + "8" * 64
    elif mutation == "dirty-management":
        management["worktree_clean"] = False
    elif mutation == "wrong-lock-digest":
        management["lock_file_sha256"] = "not-a-digest"
    elif mutation == "wrong-published-repository":
        lock["published_manifest"] = "evil/repository@sha256:" + "e" * 64
        actual["repo_digests"] = ["evil/repository@sha256:" + "e" * 64]
    elif mutation == "path-shaped-base-manifest":
        lock["base_manifest"] = "../private/target@sha256:" + "e" * 64
    elif mutation == "windows-archive-path":
        lock["archive_basename"] = r"C:\private\archive.tar"
    elif mutation == "mismatched-tag-digest":
        pinned_ref = cast("str", cluster["image_ref"]) + "@sha256:" + "a" * 64
        cluster["image_ref"] = pinned_ref
        lock["image_ref"] = pinned_ref
    else:
        actual["labels"] = {"org.apache.dolphinscheduler.matrix.source-tag": "1.3.9"}
    with pytest.raises((TypeError, ValueError), match=message):
        shared.validate_image_provenance(
            provenance,
            image_ref=cast("str", cluster["image_ref"]),
            image_id=cast("str", cluster["image_id"]),
            ds_version="1.3.9",
        )
    cluster["image_provenance"] = provenance
    case.assert_rejected(message, error=(TypeError, ValueError), rehash=True)


@pytest.mark.parametrize(
    ("ds_version", "message"),
    [
        ("2.0.0", r"2.0.0.*binary.*null"),
        ("2.0.7", r"2.0.7.*binary.*non-null"),
        ("2.0.8", r"2.0.8.*binary.*non-null"),
        ("2.0.9", r"2.0.9.*binary.*non-null"),
    ],
)
def test_validators_reject_version_specific_managed_binary_lock_drift(
    ds_version: str,
    message: str,
) -> None:
    shared = importlib.import_module("live_gate.conformance_image_provenance")
    case = _case(ds_version=ds_version)
    cluster = case.mapping("dolphinscheduler")
    provenance = _managed_provenance(ds_version=ds_version)
    actual = cast("dict[str, object]", provenance["actual"])
    labels = cast("dict[str, object]", actual["labels"])
    lock = cast("dict[str, object]", provenance["lock"])
    binary_label = "org.apache.dolphinscheduler.matrix.binary-sha512"
    if ds_version == "2.0.0":
        lock["binary_sha512"] = "sha512:" + "5" * 128
        labels[binary_label] = "5" * 128
    else:
        lock["binary_sha512"] = None
        labels.pop(binary_label)

    with pytest.raises(ValueError, match=message):
        shared.validate_image_provenance(
            provenance,
            image_ref=cast("str", cluster["image_ref"]),
            image_id=cast("str", cluster["image_id"]),
            ds_version=ds_version,
        )
    cluster["image_provenance"] = provenance
    case.assert_rejected(message, rehash=True)


_MANAGED_LOCK_PRESENCE = {
    "1.3.9": {
        "base_manifest": False,
        "binary_sha512": False,
        "published_manifest": True,
        "schema_sha256": False,
        "source_commit": True,
        "source_sha512": False,
    },
    "2.0.0": {
        "base_manifest": True,
        "binary_sha512": False,
        "published_manifest": False,
        "schema_sha256": True,
        "source_commit": True,
        "source_sha512": True,
    },
    "2.0.7": {
        "base_manifest": True,
        "binary_sha512": True,
        "published_manifest": False,
        "schema_sha256": False,
        "source_commit": True,
        "source_sha512": True,
    },
    "2.0.8": {
        "base_manifest": True,
        "binary_sha512": True,
        "published_manifest": False,
        "schema_sha256": False,
        "source_commit": True,
        "source_sha512": True,
    },
    "2.0.9": {
        "base_manifest": True,
        "binary_sha512": True,
        "published_manifest": False,
        "schema_sha256": False,
        "source_commit": True,
        "source_sha512": True,
    },
    "3.0.2": {
        "base_manifest": True,
        "binary_sha512": True,
        "published_manifest": False,
        "schema_sha256": False,
        "source_commit": True,
        "source_sha512": True,
    },
    "3.1.2": {
        "base_manifest": True,
        "binary_sha512": True,
        "published_manifest": False,
        "schema_sha256": False,
        "source_commit": True,
        "source_sha512": True,
    },
}


@pytest.mark.parametrize(
    ("ds_version", "field"),
    [
        (ds_version, field)
        for ds_version, presence in _MANAGED_LOCK_PRESENCE.items()
        for field in presence
    ],
)
def test_validators_reject_every_managed_lock_presence_flip(
    ds_version: str,
    field: str,
) -> None:
    shared = importlib.import_module("live_gate.conformance_image_provenance")
    case = _case(ds_version=ds_version)
    cluster = case.mapping("dolphinscheduler")
    provenance = _managed_provenance(ds_version=ds_version)
    actual = cast("dict[str, object]", provenance["actual"])
    lock = cast("dict[str, object]", provenance["lock"])
    replacement = {
        "base_manifest": "mirror.example/base@sha256:" + "6" * 64,
        "binary_sha512": "sha512:" + "5" * 128,
        "published_manifest": (
            _api_image_ref(ds_version).rpartition(":")[0] + "@sha256:" + "e" * 64
        ),
        "schema_sha256": "sha256:" + "9" * 64,
        "source_commit": "3" * 40,
        "source_sha512": "sha512:" + "4" * 128,
    }[field]
    lock[field] = None if _MANAGED_LOCK_PRESENCE[ds_version][field] else replacement
    published = lock["published_manifest"]
    actual["repo_digests"] = [published] if isinstance(published, str) else []
    actual["labels"] = _managed_labels_from_lock(ds_version=ds_version, lock=lock)

    with pytest.raises(ValueError, match="presence policy"):
        shared.validate_image_provenance(
            provenance,
            image_ref=cast("str", cluster["image_ref"]),
            image_id=cast("str", cluster["image_id"]),
            ds_version=ds_version,
        )
    cluster["image_provenance"] = provenance
    case.assert_rejected("presence policy", rehash=True)


@pytest.mark.parametrize(
    "image_ref",
    [
        "private.internal/team/dolphinscheduler-api:3.2.2",
        "user@host/dolphinscheduler-api:3.2.2",
        "https://apache/dolphinscheduler-api:3.2.2",
    ],
)
def test_validator_rejects_non_allowlisted_api_image_repository(
    image_ref: str,
) -> None:
    case = _case()
    case.mapping("dolphinscheduler")["image_ref"] = image_ref
    case.assert_rejected(r"image reference|URLs", rehash=True)


def test_cluster_image_observation_must_be_fresh_and_timezone_aware() -> None:
    case = _case()
    case.mapping("dolphinscheduler")["image_observed_at"] = "2026-08-10T11:30:00+00:00"
    case.assert_rejected("too stale", rehash=True)


@pytest.mark.parametrize(
    ("field", "invalid", "message"),
    [
        ("selected_ds_version", "3.4.2", "profile selected_ds_version"),
        ("contract_version", "3.4.2", "profile contract_version"),
        ("support_level", "stable", "support_level"),
        ("tested", True, "support and tested"),
    ],
)
def test_profile_is_bound_to_the_observed_exact_release(
    field: str,
    invalid: object,
    message: str,
) -> None:
    case = _case()
    case.mapping("profile")[field] = invalid
    case.assert_rejected(message, error=(TypeError, ValueError), rehash=True)


def test_profile_family_is_bound_to_the_current_generated_profile() -> None:
    case = _case()
    case.mapping("profile")["family"] = "fabricated-family"
    case.assert_rejected("profile family", rehash=True)


def test_contract_and_profile_must_share_one_exact_source_identity() -> None:
    case = _case()
    case.mapping("contract")["source_tree"] = "0" * 40
    case.assert_rejected(r"profile/contract source|contract source", rehash=True)


def test_profile_and_contract_source_are_bound_to_the_current_generated_profile() -> (
    None
):
    case = _case()
    contract = case.mapping("contract")
    source = case.mapping("profile", "source")
    source["commit"] = "0" * 40
    source["tree"] = "1" * 40
    contract["source_commit"] = source["commit"]
    contract["source_tree"] = source["tree"]
    case.assert_rejected("profile source", rehash=True)


def test_profile_fingerprints_are_bound_to_the_current_generated_profile() -> None:
    case = _case()
    fingerprints = case.mapping("profile", "fingerprints")
    for field in fingerprints:
        fingerprints[field] = "sha256:" + "f" * 64
    case.assert_rejected("profile fingerprints", rehash=True)


def test_bundle_digest_binds_the_exact_named_action_closure() -> None:
    case = _case()
    required_actions = case.sequence("conformance_bundle", "required_actions")
    direct_actions = case.sequence("conformance_bundle", "direct_actions")
    recipes = case.sequence("conformance_bundle", "action_recipes")
    required_actions.remove("workflow.list")
    direct_actions.remove("workflow.list")
    recipes.pop()
    case.assert_rejected("required_actions", rehash=True)


def test_schema_one_accepts_full_core_with_digest_bound_inheritance() -> None:
    summary = _case(
        bundle_name="full_core/v1",
        authoring_mode="opaque",
    ).validate(
        expected_ds_version="3.2.2",
        expected_bundle="full_core/v1",
    )

    assert summary.required_actions == _FULL_ACTIONS
    assert summary.authoring_mode == "opaque"


def test_full_fixed_scenario_rejects_typed_authoring_even_when_rehashed() -> None:
    _case(
        bundle_name="full_core/v1",
        authoring_mode="typed",
    ).assert_rejected("authoring_mode")


def test_schema_one_accepts_the_stable_full_manifest_profile_without_slice_roots() -> (
    None
):
    summary = _case(
        bundle_name="full_core/v1",
        authoring_mode="opaque",
        ds_version="3.4.1",
    ).validate()

    assert summary.ds_version == "3.4.1"
    assert summary.bundle == "full_core/v1"


def test_current_full_bundle_has_no_static_blocked_coordinate() -> None:
    evidence = _load_module()
    assessment = evidence.load_conformance_bundle_assessment()
    full = next(
        bundle for bundle in assessment.bundles if bundle.name == "full_core/v1"
    )

    assert set(full.coordinates.values()) == {"ready"}


def test_recipe_semantic_operations_must_be_present_in_the_contract(
    tmp_path: Path,
) -> None:
    source_root = _copy_current_source_truth(tmp_path)
    case = _case(source_root=source_root)
    artifact = source_root / "src/dsctl/generated/wire_programs/workflow_runtime.py"
    profiles = cast("dict[str, object]", _candidate_literal(artifact, "PROFILES"))
    profile = cast("dict[str, object]", profiles["3.2.2"])
    programs = cast("dict[str, object]", profile["programs"])
    programs.pop("definition_page")
    profile["profile_digest"] = _candidate_digest(
        {
            "schema_version": 2,
            **{key: value for key, value in profile.items() if key != "profile_digest"},
        }
    )
    _replace_candidate_literal(artifact, "PROFILES", profiles)
    _rehash_candidate_compiled_artifact(source_root)
    case.assert_rejected("primitive inventory")


@pytest.mark.parametrize(
    ("drift", "message"),
    [
        ("missing_program", "primitive inventory"),
        ("source", "source identity"),
        ("program_digest", "program digest"),
        ("profile_digest", "profile digest"),
        ("artifact_digest", "artifact content digest"),
        ("wrong_primitive", "source primitive"),
    ],
)
def test_current_compiled_owner_is_bound_to_real_candidate_programs(
    tmp_path: Path, drift: str, message: str
) -> None:
    source_root = _copy_current_source_truth(tmp_path)
    user_path = source_root / "src/dsctl/generated/wire_programs/user.py"
    profiles = cast("dict[str, object]", _candidate_literal(user_path, "PROFILES"))
    profile = cast("dict[str, object]", profiles["3.2.2"])
    programs = cast("dict[str, object]", profile["programs"])
    current = cast("dict[str, object]", programs["current"])
    if drift == "missing_program":
        programs.pop("current")
    elif drift == "source":
        cast("dict[str, object]", profile["source"])["commit"] = "0" * 40
    elif drift == "program_digest":
        current["program_digest"] = "sha256:" + "0" * 64
    elif drift == "wrong_primitive":
        current["source_operation"] = "UsersController.listAll"
        codecs = cast("dict[str, object]", _candidate_literal(user_path, "CODECS"))
        current["program_digest"] = _candidate_digest(
            {
                "schema_version": 4,
                "codec_record": codecs[cast("str", current["codec"])],
                **{
                    key: value
                    for key, value in current.items()
                    if key != "program_digest"
                },
            }
        )
    if drift == "profile_digest":
        profile["profile_digest"] = "sha256:" + "0" * 64
    else:
        profile["profile_digest"] = _candidate_digest(
            {
                "schema_version": 2,
                **{
                    key: value
                    for key, value in profile.items()
                    if key != "profile_digest"
                },
            }
        )
    _replace_candidate_literal(user_path, "PROFILES", profiles)
    if drift != "artifact_digest":
        _rehash_candidate_compiled_artifact(source_root)
    _case(source_root=source_root).assert_rejected(message)


def test_current_compiled_candidate_is_read_without_executing_or_install_fallback(
    tmp_path: Path,
) -> None:
    source_root = _copy_current_source_truth(tmp_path)
    user_path = source_root / "src/dsctl/generated/wire_programs/user.py"
    marker = tmp_path / "must-not-execute"
    user_path.write_text(
        user_path.read_text(encoding="utf-8")
        + f"\n__import__('pathlib').Path({str(marker)!r}).write_text('executed')\n",
        encoding="utf-8",
    )
    _rehash_candidate_compiled_artifact(source_root)
    assert _case(source_root=source_root).validate().ds_version == "3.2.2"
    assert not marker.exists()
    user_path.unlink()
    _case(source_root=source_root).assert_rejected("artifact inventory")


@pytest.mark.parametrize(
    ("field", "tampered"),
    [
        ("execution_mode", "legacy_adapter"),
        ("verification", "live_smoke"),
    ],
)
def test_action_recipe_capability_is_bound_to_the_current_generated_profile(
    field: str,
    tampered: str,
) -> None:
    case = _case()
    recipe = _receipt_action_recipe(case.receipt, action="project.create")
    recipe[field] = tampered
    case.assert_rejected(f"recipe {field}", rehash=True)


def test_action_recipe_semantic_operation_is_bound_to_the_generated_decision() -> None:
    case = _case()
    recipe = _receipt_action_recipe(case.receipt, action="project.create")
    recipe["semantic_operation"] = "project.page"
    case.assert_rejected("recipe semantic_operation", rehash=True)


def test_action_recipe_fingerprints_are_bound_to_the_generated_decision() -> None:
    case = _case()
    recipe = _receipt_action_recipe(case.receipt, action="project.create")
    fingerprints = recipe["fingerprints"]
    assert isinstance(fingerprints, dict)
    for field in fingerprints:
        fingerprints[field] = "sha256:" + "f" * 64
    case.assert_rejected(r"recipe project\.create fingerprints", rehash=True)


def test_current_generated_required_action_decision_must_remain_accepted(
    tmp_path: Path,
) -> None:
    source_root = _copy_current_source_truth(tmp_path)
    _set_generated_build_status(
        source_root,
        ds_version="3.2.2",
        semantic_operation="project.create",
        status="blocked",
    )

    _case(source_root=source_root).assert_rejected("build_status")


def test_contract_semantic_roots_are_bound_to_the_current_generated_manifest() -> None:
    case = _case()
    semantic_operations = case.sequence("contract", "semantic_operations")
    assert semantic_operations == []
    semantic_operations.append("resource.page")
    case.assert_rejected("semantic_operations", rehash=True)


@pytest.mark.parametrize(
    ("field", "tampered"),
    [
        ("source_contract_digest", "sha256:" + "a" * 64),
        ("rendered_contract_digest", "sha256:" + "b" * 64),
        ("operation_count", 999),
    ],
)
def test_contract_identity_is_bound_to_the_current_generated_manifest(
    field: str,
    tampered: object,
) -> None:
    case = _case()
    case.mapping("contract")[field] = tampered
    case.assert_rejected(field, rehash=True)


def test_current_bundle_manifest_schema_two_receipt_is_valid() -> None:
    case = _case()
    contract = case.mapping("contract")
    assert contract["bundle_manifest_schema_version"] == 2
    case.validate()


def test_historical_bundle_manifest_schema_is_valid_but_stale() -> None:
    case = _case()
    case.mapping("contract")["bundle_manifest_schema_version"] = 1
    case.assert_rejected(
        "contract bundle_manifest_schema_version must equal 2",
        rehash=True,
    )


def test_unknown_bundle_manifest_schema_is_rejected_before_binding_check() -> None:
    case = _case()
    case.mapping("contract")["bundle_manifest_schema_version"] = 3
    case.assert_rejected(
        "contract bundle_manifest_schema_version must be 1 or 2",
        rehash=True,
    )


def test_validator_follows_the_current_generated_bundle_closure(
    tmp_path: Path,
) -> None:
    source_root = _copy_current_source_truth(tmp_path)
    generated = importlib.import_module("dsctl.generated.conformance_bundles")
    assessment = copy.deepcopy(generated.CONFORMANCE_BUNDLE_DATA)
    bundles = {row["name"]: row for row in assessment["bundles"]}
    legacy = bundles["legacy_core/v1"]
    full = bundles["full_core/v1"]
    legacy["required_actions"].remove("workflow.list")
    legacy["direct_actions"].remove("workflow.list")
    legacy["bundle_digest"] = _bundle_digest(
        "legacy_core/v1",
        legacy["required_actions"],
    )
    full["direct_actions"].append("workflow.list")
    full["direct_actions"].sort()
    _refresh_assessment_digests(assessment)
    _write_embedded_json(
        source_root / "src" / "dsctl" / "generated" / "conformance_bundles.py",
        name="_CONFORMANCE_BUNDLE_JSON",
        payload=assessment,
    )
    case = _case(source_root=source_root)
    _bind_receipt_to_assessment(case.receipt, assessment)
    summary = case.validate()

    assert summary.required_actions == _LEGACY_ACTIONS[:-1]


def test_malformed_current_generated_assessment_fails_closed(
    tmp_path: Path,
) -> None:
    source_root = _copy_current_source_truth(tmp_path)
    generated = importlib.import_module("dsctl.generated.conformance_bundles")
    assessment = copy.deepcopy(generated.CONFORMANCE_BUNDLE_DATA)
    assessment["bundles"][0]["required_actions"].remove("workflow.list")
    _write_embedded_json(
        source_root / "src" / "dsctl" / "generated" / "conformance_bundles.py",
        name="_CONFORMANCE_BUNDLE_JSON",
        payload=assessment,
    )

    _case(source_root=source_root).assert_rejected("current conformance assessment")


@pytest.mark.parametrize(
    "attack",
    [
        "bundle-order",
        "bundle-duplicate",
        "action-order",
        "action-duplicate",
        "version-duplicate",
    ],
)
def test_coherently_resigned_noncanonical_assessment_structure_fails_closed(
    tmp_path: Path,
    attack: str,
) -> None:
    source_root = _copy_current_source_truth(tmp_path)
    generated = importlib.import_module("dsctl.generated.conformance_bundles")
    assessment = copy.deepcopy(generated.CONFORMANCE_BUNDLE_DATA)
    bundles = assessment["bundles"]
    assert isinstance(bundles, list)
    full = next(
        row
        for row in bundles
        if isinstance(row, dict) and row.get("name") == "full_core/v1"
    )
    if attack == "bundle-order":
        bundles.reverse()
    elif attack == "bundle-duplicate":
        full["name"] = "legacy_core/v1"
        full["bundle_digest"] = _bundle_digest(
            "legacy_core/v1",
            full["required_actions"],
        )
    elif attack == "action-order":
        full["required_actions"][0], full["required_actions"][1] = (
            full["required_actions"][1],
            full["required_actions"][0],
        )
        full["direct_actions"].reverse()
        full["bundle_digest"] = _bundle_digest(
            "full_core/v1",
            full["required_actions"],
        )
    elif attack == "action-duplicate":
        full["required_actions"].insert(1, full["required_actions"][0])
        full["bundle_digest"] = _bundle_digest(
            "full_core/v1",
            full["required_actions"],
        )
    else:
        full["versions"][1] = copy.deepcopy(full["versions"][0])
    _refresh_assessment_digests(assessment)
    _write_embedded_json(
        source_root / "src" / "dsctl" / "generated" / "conformance_bundles.py",
        name="_CONFORMANCE_BUNDLE_JSON",
        payload=assessment,
    )

    _case(source_root=source_root).assert_rejected("current conformance assessment")


@pytest.mark.parametrize(
    "attack",
    [
        "unexpected-blocker",
    ],
)
def test_coherently_resigned_noncanonical_assessment_blocker_fails_closed(
    tmp_path: Path,
    attack: str,
) -> None:
    source_root = _copy_current_source_truth(tmp_path)
    generated = importlib.import_module("dsctl.generated.conformance_bundles")
    assessment = copy.deepcopy(generated.CONFORMANCE_BUNDLE_DATA)
    bundles = assessment["bundles"]
    assert isinstance(bundles, list)
    full = next(
        row
        for row in bundles
        if isinstance(row, dict) and row.get("name") == "full_core/v1"
    )
    coordinate = next(
        row
        for row in full["versions"]
        if isinstance(row, dict) and row.get("version") == "1.3.9"
    )
    blockers = coordinate["blockers"]
    assert isinstance(blockers, list)
    assert attack == "unexpected-blocker"
    blockers.append({"action": "project.create"})
    _refresh_assessment_digests(assessment)
    _write_embedded_json(
        source_root / "src" / "dsctl" / "generated" / "conformance_bundles.py",
        name="_CONFORMANCE_BUNDLE_JSON",
        payload=assessment,
    )

    _case(source_root=source_root).assert_rejected("current conformance assessment")


def test_scenario_digest_binds_bundle_mode_and_required_outcomes() -> None:
    case = _case()
    case.mapping("scenario")["digest"] = "sha256:" + "0" * 64
    case.assert_rejected("scenario digest", rehash=True)


def test_coherently_rehashed_scenario_cannot_change_named_bundle_obligations() -> None:
    case = _case()
    scenario = case.mapping("scenario")
    required = case.sequence("scenario", "required_outcomes")
    observed = case.sequence("scenario", "observed_outcomes")
    conflict = case.mapping("operation_trace", -2)
    required[-1] = "profile-promoted"
    observed[-1] = "profile-promoted"
    conflict["outcomes"] = ["profile-promoted"]
    _refresh_scenario_digest(scenario)
    case.assert_rejected("required_outcomes", rehash=True)


def test_passing_scenario_requires_every_declared_outcome_to_be_observed() -> None:
    case = _case()
    observed = case.sequence("scenario", "observed_outcomes")
    observed.remove("negative-errors-translated")
    case.assert_rejected("observed_outcomes", rehash=True)


@pytest.mark.parametrize(
    ("field", "invalid"),
    [
        ("claim", "profile-promotion"),
        ("observed_evidence", "live_full"),
        ("facet_claims", ["SHELL"]),
        ("promotion_claimed", True),
        ("support_level_changes", True),
        ("tested_changes", True),
    ],
)
def test_evidence_scope_cannot_claim_facets_or_profile_promotion(
    field: str,
    invalid: object,
) -> None:
    case = _case()
    case.mapping("evidence_scope")[field] = invalid
    case.assert_rejected(field, rehash=True)


def test_authoring_mode_does_not_imply_a_facet_and_is_bound_once() -> None:
    case = _case(authoring_mode="typed")
    case.mapping("evidence_scope")["authoring_mode"] = "opaque"
    case.assert_rejected("authoring_mode", rehash=True)


def test_fixture_hmac_proves_the_external_fixture_was_unchanged() -> None:
    case = _case()
    case.mapping("fixture")["after_state_hmac_sha256"] = "hmac-sha256:" + "3" * 64
    case.assert_rejected("before/after state HMAC", rehash=True)


@pytest.mark.parametrize(
    "attack",
    [
        "ssh://operator@internal-host/fixture",
        "file:///private/tmp/fixture.json",
        "jdbc:postgresql://db.internal/fixture",
        "/private/tmp/fixture.json",
        "../private/fixture.json",
        "operator@internal-host",
        "operator:credential@internal-host",
        "token-deadbeef",
    ],
)
def test_fixture_provisioner_rejects_high_entropy_or_target_shaped_text(
    attack: str,
) -> None:
    case = _case()
    case.mapping("fixture")["provisioner"] = attack
    case.assert_rejected("fixture provisioner", rehash=True)


@pytest.mark.parametrize(
    ("field", "invalid"),
    [
        ("remote_mutations", 0),
        ("gate_owned_only", False),
        ("external_fixture_mutated", True),
    ],
)
def test_passing_effects_are_bounded_to_gate_owned_resources(
    field: str,
    invalid: object,
) -> None:
    case = _case()
    case.mapping("effects")[field] = invalid
    case.assert_rejected(field, rehash=True)


@pytest.mark.parametrize(
    ("bundle_name", "ds_version", "expected", "tampered"),
    [
        ("legacy_core/v1", "3.2.2", 3, 7),
        ("full_core/v1", "3.4.2", 7, 9),
        *(
            (
                "full_core/v1",
                ds_version,
                11 if ds_version in _TASK_PRE_DELETE_RELEASE_VERSIONS else 9,
                7,
            )
            for ds_version in sorted(_FULL_TASK_DEFINITION_CLEANUP_VERSIONS)
        ),
    ],
)
def test_effect_count_is_exact_for_each_fixed_bundle_scenario(
    bundle_name: str,
    ds_version: str,
    expected: int,
    tampered: int,
) -> None:
    case = _case(bundle_name=bundle_name, ds_version=ds_version)
    effects = case.mapping("effects")
    assert effects["remote_mutations"] == expected
    effects["remote_mutations"] = tampered
    case.assert_rejected("remote_mutations", rehash=True)


def test_full_trace_predicates_accept_the_fixed_workflow_task_lifecycle() -> None:
    summary = _case(bundle_name="full_core/v1", ds_version="3.4.2").validate()

    assert summary.bundle == "full_core/v1"


@pytest.mark.parametrize(
    "ds_version",
    sorted(_FULL_TASK_DEFINITION_CLEANUP_VERSIONS),
)
def test_affected_full_trace_accepts_exact_private_task_cleanup_lifecycle(
    ds_version: str,
) -> None:
    case = _case(bundle_name="full_core/v1", ds_version=ds_version)
    trace = case.sequence("operation_trace")
    effects = case.mapping("effects")

    actions = [entry["action"] for entry in trace if isinstance(entry, dict)]
    assert actions.count(_TASK_DEFINITION_PROVE_ACTION) == 1
    assert actions.count(_TASK_DEFINITION_CLEANUP_ACTION) == 1
    assert effects["remote_mutations"] == (
        11 if ds_version in _TASK_PRE_DELETE_RELEASE_VERSIONS else 9
    )

    summary = case.validate()

    assert summary.ds_version == ds_version
    assert summary.bundle == "full_core/v1"
    assert summary.required_actions == _FULL_ACTIONS


@pytest.mark.parametrize(
    "ds_version",
    tuple(
        version
        for version, strategy in _TASK_RECONCILIATION_STRATEGIES.items()
        if strategy == "workflow-cascade-proof-only"
    ),
)
def test_proof_only_full_trace_accepts_zero_mutation_private_lifecycle(
    ds_version: str,
) -> None:
    case = _case(bundle_name="full_core/v1", ds_version=ds_version)
    trace = case.sequence("operation_trace")
    effects = case.mapping("effects")

    actions = [entry["action"] for entry in trace if isinstance(entry, dict)]
    assert actions.count(_TASK_DEFINITION_PROVE_ACTION) == 1
    assert actions.count(_TASK_DEFINITION_CLEANUP_ACTION) == 1
    assert effects["remote_mutations"] == 7

    summary = case.validate()

    assert summary.ds_version == ds_version
    assert summary.bundle == "full_core/v1"


@pytest.mark.parametrize(
    "attack",
    [
        "delete-proof",
        "delete-cleanup",
        "delete-both",
        "forge-proof",
        "forge-cleanup",
    ],
)
def test_proof_only_full_trace_rejects_rehashed_private_lifecycle_attacks(
    attack: str,
) -> None:
    case = _case(bundle_name="full_core/v1", ds_version="3.1.9")
    trace = case.sequence("operation_trace")

    private_entries = {
        entry["action"]: entry
        for entry in trace
        if isinstance(entry, dict)
        and entry.get("action")
        in {_TASK_DEFINITION_PROVE_ACTION, _TASK_DEFINITION_CLEANUP_ACTION}
    }
    if attack in {"delete-proof", "delete-both"}:
        trace.remove(private_entries[_TASK_DEFINITION_PROVE_ACTION])
    if attack in {"delete-cleanup", "delete-both"}:
        trace.remove(private_entries[_TASK_DEFINITION_CLEANUP_ACTION])
    if attack == "forge-proof":
        private_entries[_TASK_DEFINITION_PROVE_ACTION]["assertions"] = [
            "project-wide-task-inventory-proven",
            "exact-two-owned-tasks",
            "zero-remote-mutations",
        ]
    if attack == "forge-cleanup":
        private_entries[_TASK_DEFINITION_CLEANUP_ACTION]["assertions"] = [
            "post-cascade-zero-reconciliation",
            "observed-zero-tasks",
            "deleted-two-tasks",
            "remaining-zero-tasks",
            "remote-mutations-two",
        ]
    _renumber_operation_trace(trace)
    case.assert_rejected(rehash=True)


@pytest.mark.parametrize(
    ("bundle_name", "ds_version"),
    [
        ("legacy_core/v1", "2.0.9"),
        ("full_core/v1", "3.4.2"),
    ],
)
@pytest.mark.parametrize(
    "action",
    [
        _TASK_DEFINITION_PROVE_ACTION,
        _TASK_DEFINITION_CLEANUP_ACTION,
    ],
)
def test_auxiliary_task_cleanup_actions_are_rejected_outside_affected_full_receipts(
    bundle_name: str,
    ds_version: str,
    action: str,
) -> None:
    case = _case(bundle_name=bundle_name, ds_version=ds_version)
    trace = case.sequence("operation_trace")
    trace.append(
        {
            "sequence": len(trace) + 1,
            "action": action,
            "argv_shape": action.replace(".", " "),
            "exit_code": 0,
            "ok": True,
            "assertions": ["fabricated-auxiliary-claim"],
            "outcomes": [],
        }
    )
    case.assert_rejected("outside the named bundle", rehash=True)


@pytest.mark.parametrize(
    ("action", "replacement", "expected_error"),
    [
        (
            _TASK_DEFINITION_PROVE_ACTION,
            ["exact-two-owned-tasks", "zero-remote-mutations"],
            "task-definition proof",
        ),
        (
            _TASK_DEFINITION_CLEANUP_ACTION,
            ["exact-two-owned-tasks-deleted", "remote-mutations-two"],
            "task-definition cleanup",
        ),
    ],
)
def test_affected_full_trace_requires_exact_auxiliary_assertions(
    action: str,
    replacement: list[str],
    expected_error: str,
) -> None:
    case = _case(bundle_name="full_core/v1", ds_version="3.0.6")
    trace = case.sequence("operation_trace")
    entry = next(
        row for row in trace if isinstance(row, dict) and row.get("action") == action
    )
    entry["assertions"] = replacement
    case.assert_rejected(expected_error, rehash=True)


@pytest.mark.parametrize(
    ("action", "anchor_action", "anchor_offset", "expected_error"),
    [
        (
            _TASK_DEFINITION_PROVE_ACTION,
            "workflow.delete",
            1,
            "task-definition proof",
        ),
        (
            _TASK_DEFINITION_CLEANUP_ACTION,
            "workflow.list",
            0,
            "workflow absence",
        ),
    ],
)
def test_affected_full_trace_rejects_auxiliary_cleanup_out_of_order(
    action: str,
    anchor_action: str,
    anchor_offset: int,
    expected_error: str,
) -> None:
    case = _case(bundle_name="full_core/v1", ds_version="3.1.0")
    trace = case.sequence("operation_trace")
    entry_index = next(
        index
        for index, row in enumerate(trace)
        if isinstance(row, dict) and row.get("action") == action
    )
    entry = trace.pop(entry_index)
    anchor_index = next(
        index
        for index, row in enumerate(trace)
        if isinstance(row, dict)
        and row.get("action") == anchor_action
        and (
            anchor_action != "workflow.list"
            or row.get("assertions") == ["workflow-leftovers-zero"]
        )
    )
    trace.insert(anchor_index + anchor_offset, entry)
    _renumber_operation_trace(trace)
    case.assert_rejected(expected_error, rehash=True)


def test_affected_full_trace_rejects_workflow_scoped_task_absence_substitution() -> (
    None
):
    case = _case(bundle_name="full_core/v1", ds_version="2.0.9")
    trace = case.sequence("operation_trace")
    cleanup = next(
        row
        for row in trace
        if isinstance(row, dict)
        and row.get("action") == _TASK_DEFINITION_CLEANUP_ACTION
    )
    cleanup.update(
        {
            "action": "task.list",
            "argv_shape": "task list --project PROJECT --workflow WORKFLOW",
            "exit_code": 1,
            "ok": False,
            "error_type": "not_found",
            "assertions": [
                "stable-not-found",
                "task-leftovers-zero-with-workflow-absent",
            ],
        }
    )
    case.assert_rejected("task-definition cleanup", rehash=True)


@pytest.mark.parametrize(
    "attack",
    [
        "workflow-duplicate-labels",
        "missing-dag-digest-stage",
        "task-apply-labels",
        "workflow-edit-apply-labels",
        "workflow-roundtrip-outcomes",
        "task-cleanup-stage",
        "full-cleanup-outcome",
    ],
)
def test_full_trace_rejects_labels_relocated_onto_the_wrong_stage(
    attack: str,
) -> None:
    case = _case(bundle_name="full_core/v1", ds_version="3.4.2")
    trace = case.sequence("operation_trace")
    if attack == "workflow-duplicate-labels":
        entry = next(
            item
            for item in trace
            if isinstance(item, dict)
            and item.get("action") == "workflow.create"
            and item.get("ok") is False
        )
        entry["action"] = "project.create"
        entry["argv_shape"] = "project create --name PROJECT_NAME"
    elif attack == "missing-dag-digest-stage":
        entry = _entry_with_assertion(trace, "digest-topology-match-describe")
        entry["assertions"] = ["workflow.digest-observed"]
    elif attack == "task-apply-labels":
        entry = _entry_with_assertion(trace, "command-updated-exactly")
        labels = entry["assertions"]
        entry["assertions"] = ["task.update-observed"]
        target = _entry_with_assertion(trace, "two-task-dag-matched")
        assert isinstance(labels, list)
        target_assertions = target["assertions"]
        assert isinstance(target_assertions, list)
        target_assertions.extend(labels)
    elif attack == "workflow-edit-apply-labels":
        entry = _entry_with_assertion(trace, "description-updated-exactly")
        labels = entry["assertions"]
        entry["assertions"] = ["workflow.edit-observed"]
        target = _entry_with_assertion(trace, "gate-owned-workflow-matched")
        assert isinstance(labels, list)
        target_assertions = target["assertions"]
        assert isinstance(target_assertions, list)
        target_assertions.extend(labels)
    elif attack == "workflow-roundtrip-outcomes":
        entry = _entry_with_assertion(trace, "workflow-leftovers-zero")
        outcomes = entry["outcomes"]
        entry["outcomes"] = []
        target = _entry_with_assertion(trace, "gate-owned-workflow-deleted")
        assert isinstance(outcomes, list)
        target["outcomes"] = outcomes
    elif attack == "task-cleanup-stage":
        entry = _entry_with_assertion(
            trace,
            "task-leftovers-zero-with-workflow-absent",
        )
        entry["assertions"] = ["stable-not-found"]
    else:
        entry = _entry_with_assertion(trace, "deleted-project-absent")
        target = next(
            item
            for item in trace
            if isinstance(item, dict)
            and item.get("outcomes")
            == ["full-cleanup-zero", "negative-errors-translated"]
        )
        outcomes = target["outcomes"]
        target["outcomes"] = []
        assert isinstance(outcomes, list)
        entry["outcomes"] = outcomes
    case.assert_rejected(r"full.*trace|trace.*full", rehash=True)


def test_full_trace_rejects_project_roundtrip_outcome_relocated_to_create() -> None:
    case = _case(bundle_name="full_core/v1", ds_version="3.4.2")
    trace = case.sequence("operation_trace")
    create = _entry_with_assertion(trace, "gate-owned-project-created")
    readback = _entry_with_assertion(trace, "updated-description-read-back")
    assert readback["outcomes"] == ["gate-owned-project-round-trip"]
    create["outcomes"] = readback["outcomes"]
    readback["outcomes"] = []
    case.assert_rejected(r"full.*project|project.*stage", rehash=True)


def test_full_trace_rejects_an_eighth_successful_apply_mutation() -> None:
    case = _case(bundle_name="full_core/v1", ds_version="3.4.2")
    trace = case.sequence("operation_trace")
    trace.append(
        {
            "sequence": len(trace) + 1,
            "action": "task.update",
            "argv_shape": (
                "task update TASK --project PROJECT --workflow WORKFLOW --set COMMAND"
            ),
            "exit_code": 0,
            "ok": True,
            "assertions": [
                "command-updated-exactly",
                "task-version-advanced",
                "non-owned-task-state-preserved",
            ],
            "outcomes": [],
        }
    )
    case.assert_rejected(r"seven|mutation stages", rehash=True)


@pytest.mark.parametrize(
    ("action", "argv_shape", "assertions"),
    [
        (
            "workflow.delete",
            "workflow delete WORKFLOW --project PROJECT --force",
            ["mutation-may-have-applied"],
        ),
        (
            "workflow.get",
            "workflow get WORKFLOW --project PROJECT",
            ["unexpected-transport-failure"],
        ),
    ],
)
def test_full_trace_rejects_interleaved_unexpected_failed_io_even_when_rehashed(
    action: str,
    argv_shape: str,
    assertions: list[str],
) -> None:
    case = _case(bundle_name="full_core/v1", ds_version="3.4.2")
    trace = case.sequence("operation_trace")
    delete_index = next(
        index
        for index, entry in enumerate(trace)
        if isinstance(entry, dict)
        and entry.get("action") == "workflow.delete"
        and entry.get("ok") is True
    )
    trace.insert(
        delete_index,
        {
            "sequence": 0,
            "action": action,
            "argv_shape": argv_shape,
            "exit_code": 1,
            "ok": False,
            "error_type": "api_transport_error",
            "assertions": assertions,
            "outcomes": [],
        },
    )
    for sequence, entry in enumerate(trace, start=1):
        assert isinstance(entry, dict)
        entry["sequence"] = sequence
    case.assert_rejected(r"unexpected failed|failed.*full trace", rehash=True)


@pytest.mark.parametrize(
    ("entry_index", "expected_error"),
    [(0, "version preflight"), (1, "capability preflight")],
)
def test_preflight_assertions_reject_extra_claims_even_when_rehashed(
    entry_index: int,
    expected_error: str,
) -> None:
    case = _case(bundle_name="full_core/v1", ds_version="3.4.2")
    assertions = case.sequence("operation_trace", entry_index, "assertions")
    assertions.append("fabricated-preflight-claim")
    case.assert_rejected(expected_error, rehash=True)


@pytest.mark.parametrize(
    "ds_version",
    _DEPENDENCY_UPDATE_UPSTREAM_LIMITED_VERSIONS,
)
def test_full_trace_accepts_generated_pre_io_dependency_rejection(
    ds_version: str,
) -> None:
    summary = _case(bundle_name="full_core/v1", ds_version=ds_version).validate()

    assert summary.ds_version == ds_version


@pytest.mark.parametrize(
    "ds_version",
    _DEPENDENCY_UPDATE_UPSTREAM_LIMITED_VERSIONS,
)
def test_full_trace_requires_generated_pre_io_dependency_rejection(
    ds_version: str,
) -> None:
    case = _case(bundle_name="full_core/v1", ds_version=ds_version)
    trace = case.sequence("operation_trace")
    dependency = _entry_with_assertion(
        trace,
        "dependency-update-upstream-limited",
    )
    dependency["assertions"] = ["pre-io-no-mutation"]
    case.assert_rejected(r"dependency", rehash=True)


@pytest.mark.parametrize(
    "resource",
    ["gate_owned_projects", "gate_owned_workflows", "gate_owned_tasks"],
)
def test_passing_cleanup_requires_zero_owned_resource_leftovers(
    resource: str,
) -> None:
    case = _case()
    case.mapping("cleanup", resource)["leftovers"] = 1
    case.assert_rejected(f"{resource}.*leftovers", rehash=True)


def test_trace_must_cover_every_required_action_successfully() -> None:
    case = _case()
    trace = case.sequence("operation_trace")
    workflow_list = next(
        entry
        for entry in trace
        if isinstance(entry, dict)
        and entry.get("action") == "workflow.list"
        and entry.get("ok") is True
    )
    assert isinstance(workflow_list, dict)
    workflow_list["ok"] = False
    workflow_list["exit_code"] = 1
    workflow_list["error_type"] = "not_found"
    case.assert_rejected(r"successful required actions.*workflow\.list", rehash=True)


def test_trace_accepts_version_and_per_action_capability_preflights() -> None:
    summary = _case().validate()

    assert summary.bundle == "legacy_core/v1"


@pytest.mark.parametrize("remove_index", [0, 1])
def test_trace_requires_version_and_every_action_capability_preflight(
    remove_index: int,
) -> None:
    case = _case()
    trace = case.sequence("operation_trace")
    trace.pop(remove_index)
    for sequence, entry in enumerate(trace, start=1):
        assert isinstance(entry, dict)
        entry["sequence"] = sequence
    case.assert_rejected("preflight", rehash=True)


def test_trace_sequence_is_contiguous_and_argv_shapes_have_no_angle_placeholders() -> (
    None
):
    case = _case()
    case.mapping("operation_trace", 0)["argv_shape"] = "project get <project>"
    case.assert_rejected(r"argv_shape.*angle brackets", rehash=True)


def test_trace_argv_shapes_cannot_record_concrete_resource_values() -> None:
    case = _case()
    trace = case.sequence("operation_trace")
    project_get = next(
        entry
        for entry in trace
        if isinstance(entry, dict)
        and entry.get("action") == "project.get"
        and entry.get("ok") is True
    )
    assert isinstance(project_get, dict)
    project_get["argv_shape"] = "project get finance-prod"
    case.assert_rejected(r"argv_shape.*metavariables", rehash=True)


def test_failed_trace_entries_require_stable_error_typing() -> None:
    case = _case()
    case.mapping("operation_trace", -2).pop("error_type")
    case.assert_rejected(r"failed trace.*error_type", rehash=True)


def test_trace_must_observe_every_digest_bound_scenario_outcome() -> None:
    case = _case()
    case.mapping("operation_trace", -2)["outcomes"] = []
    case.assert_rejected("trace outcomes", rehash=True)


@pytest.mark.parametrize(
    ("offset", "field", "invalid"),
    [
        (-2, "error_type", "api_result_error"),
        (-2, "assertions", ["stable-conflict"]),
        (-1, "error_type", "api_result_error"),
    ],
)
def test_trace_requires_stable_conflict_and_not_found_negative_outcomes(
    offset: int,
    field: str,
    invalid: object,
) -> None:
    case = _case()
    case.mapping("operation_trace", offset)[field] = invalid
    case.assert_rejected("required stable negative outcomes", rehash=True)


def _load_module() -> ModuleType:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module("live_gate.conformance_bundle_evidence")


@cache
def _default_evidence_validator() -> _EvidenceValidator:
    evidence = _load_module()
    return cast(
        "_EvidenceValidator",
        evidence.ConformanceBundleEvidenceValidator.load(),
    )


def _validate_conformance_bundle_evidence(
    receipt: object,
    *,
    expected_ds_version: str | None = None,
    expected_bundle: str | None = None,
    expected_wheel_filename: str | None = None,
    expected_wheel_sha256: str | None = None,
    source_root: Path | None = None,
) -> _EvidenceSummary:
    if source_root is not None:
        evidence = _load_module()
        return cast(
            "_EvidenceSummary",
            evidence.validate_conformance_bundle_evidence(
                receipt,
                expected_ds_version=expected_ds_version,
                expected_bundle=expected_bundle,
                expected_wheel_filename=expected_wheel_filename,
                expected_wheel_sha256=expected_wheel_sha256,
                source_root=source_root,
            ),
        )
    return _default_evidence_validator().validate(
        receipt,
        expected_ds_version=expected_ds_version,
        expected_bundle=expected_bundle,
        expected_wheel_filename=expected_wheel_filename,
        expected_wheel_sha256=expected_wheel_sha256,
    )


def _case(
    *,
    bundle_name: str = "legacy_core/v1",
    authoring_mode: str | None = None,
    ds_version: str = "3.2.2",
    source_root: Path | None = None,
) -> EvidenceCase[_EvidenceSummary]:
    return EvidenceCase(
        receipt=_receipt(
            bundle_name=bundle_name,
            authoring_mode=authoring_mode,
            ds_version=ds_version,
        ),
        validator=_validate_conformance_bundle_evidence,
        refresh_receipt=_refresh_receipt_digest,
        source_root=source_root,
    )


def _copy_current_source_truth(tmp_path: Path) -> Path:
    project_root = Path(__file__).resolve().parents[2]
    source_root = tmp_path / "candidate-source"
    source_root.mkdir()
    shutil.copy2(project_root / "pyproject.toml", source_root / "pyproject.toml")
    generated_source = project_root / "src" / "dsctl" / "generated"
    generated_target = source_root / "src" / "dsctl" / "generated"
    generated_target.mkdir(parents=True)
    for filename in (
        "conformance_bundles.py",
        "task_definition_cleanup_profiles.py",
        "task_definition_profiles.py",
        "version_profiles.py",
    ):
        shutil.copy2(generated_source / filename, generated_target / filename)
    for manifest in (generated_source / "versions").glob("ds_*/_manifest.py"):
        target = generated_target / "versions" / manifest.parent.name / manifest.name
        target.parent.mkdir(parents=True)
        shutil.copy2(manifest, target)
    for name in ("wire_programs", "wire_runtime"):
        shutil.copytree(
            generated_source / name,
            generated_target / name,
            ignore=shutil.ignore_patterns("__pycache__"),
        )
    image_contract = Path("tools/live_gate/conformance_image_contract.py")
    image_contract_target = source_root / image_contract
    image_contract_target.parent.mkdir(parents=True)
    shutil.copy2(project_root / image_contract, image_contract_target)
    return source_root


def _candidate_literal(path: Path, name: str) -> object:
    if name == "CODECS":
        return read_compiled_literals(path, {name})[name]
    for statement in ast.parse(path.read_text(encoding="utf-8")).body:
        if isinstance(statement, ast.Assign) and len(statement.targets) == 1:
            target = statement.targets[0]
            if isinstance(target, ast.Name) and target.id == name:
                return ast.literal_eval(statement.value)
    message = f"candidate lacks {name}"
    raise AssertionError(message)


def _replace_candidate_literal(path: Path, name: str, value: object) -> None:
    source = path.read_text(encoding="utf-8")
    lines = source.splitlines(keepends=True)
    for statement in ast.parse(source).body:
        if isinstance(statement, ast.Assign) and len(statement.targets) == 1:
            target = statement.targets[0]
            if isinstance(target, ast.Name) and target.id == name:
                assert statement.end_lineno is not None
                lines[statement.lineno - 1 : statement.end_lineno] = [
                    f"{name} = {value!r}\n"
                ]
                path.write_text("".join(lines), encoding="utf-8")
                return
    message = f"candidate lacks {name}"
    raise AssertionError(message)


def _candidate_digest(value: dict[str, object]) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode()
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _rehash_candidate_compiled_artifact(source_root: Path) -> None:
    root = source_root / "src/dsctl/generated/wire_programs"
    manifest = root / "_manifest.py"
    modules = _candidate_literal(manifest, "MODULES")
    assert isinstance(modules, tuple)
    digest = hashlib.sha256(b"dsctl-compiled-wire-artifact-v5\0")
    for name in modules:
        assert isinstance(name, str)
        digest.update(name.encode())
        digest.update(b"\0")
        digest.update((root / name).read_bytes())
        digest.update(b"\0")
    _replace_candidate_literal(
        manifest, "CONTENT_DIGEST", "sha256:" + digest.hexdigest()
    )


def _manifest_path(source_root: Path, *, ds_version: str) -> Path:
    return (
        source_root
        / "src"
        / "dsctl"
        / "generated"
        / "versions"
        / f"ds_{ds_version.replace('.', '_')}"
        / "_manifest.py"
    )


def _generated_truth_path(source_root: Path, *, target: str) -> Path:
    generated_root = source_root / "src" / "dsctl" / "generated"
    if target == "profile":
        return generated_root / "version_profiles.py"
    if target == "assessment":
        return generated_root / "conformance_bundles.py"
    if target == "manifest":
        return _manifest_path(source_root, ds_version="3.2.2")
    if target == "image_contract":
        return source_root / "tools" / "live_gate" / "conformance_image_contract.py"
    if target == "task_cleanup":
        return generated_root / "task_definition_cleanup_profiles.py"
    if target == "task_update":
        return generated_root / "task_definition_profiles.py"
    message = f"unknown generated truth target {target!r}"
    raise AssertionError(message)


def _load_embedded_json(path: Path, *, name: str) -> dict[str, object]:
    assignments = [
        statement
        for statement in ast.parse(
            path.read_text(encoding="utf-8"),
            filename=str(path),
        ).body
        if isinstance(statement, ast.Assign)
        and len(statement.targets) == 1
        and isinstance(statement.targets[0], ast.Name)
        and statement.targets[0].id == name
    ]
    assert len(assignments) == 1
    raw = ast.literal_eval(assignments[0].value)
    assert isinstance(raw, str)
    payload = json.loads(raw)
    assert isinstance(payload, dict)
    return payload


def _write_embedded_json(
    path: Path,
    *,
    name: str,
    payload: dict[str, object],
) -> None:
    path.write_text(
        f"{name} = {json.dumps(payload, sort_keys=True)!r}\n",
        encoding="utf-8",
    )


def _set_generated_build_status(
    source_root: Path,
    *,
    ds_version: str,
    semantic_operation: str,
    status: str,
) -> None:
    path = source_root / "src" / "dsctl" / "generated" / "version_profiles.py"
    data = _load_embedded_json(path, name="_PROFILE_JSON")
    profile_bindings = data["profile_bindings"]
    records = data["build_records"]
    assert isinstance(profile_bindings, dict)
    assert isinstance(records, dict)
    bindings = profile_bindings[ds_version]["build_decisions"]
    record = copy.deepcopy(records[bindings[semantic_operation]])
    assert isinstance(record, dict)
    record["build_status"] = status
    record["status_reason"] = "test-current-source-binding"
    name = f"test:{semantic_operation}@{ds_version}"
    records[name] = record
    bindings[semantic_operation] = name
    _write_embedded_json(path, name="_PROFILE_JSON", payload=data)


def _full_scenario_trace(  # noqa: C901
    *,
    ds_version: str,
    scenario_outcomes: list[str],
) -> list[dict[str, object]]:
    trace: list[dict[str, object]] = []
    reconciliation_strategy = _TASK_RECONCILIATION_STRATEGIES.get(ds_version)

    def success(
        action: str,
        argv_shape: str,
        assertions: list[str],
        *,
        outcomes: list[str] | None = None,
    ) -> None:
        trace.append(
            {
                "sequence": len(trace) + 1,
                "action": action,
                "argv_shape": argv_shape,
                "exit_code": 0,
                "ok": True,
                "assertions": assertions,
                "outcomes": [] if outcomes is None else outcomes,
            }
        )

    def failure(
        action: str,
        argv_shape: str,
        error_type: str,
        assertions: list[str],
    ) -> None:
        trace.append(
            {
                "sequence": len(trace) + 1,
                "action": action,
                "argv_shape": argv_shape,
                "exit_code": 1,
                "ok": False,
                "error_type": error_type,
                "assertions": assertions,
                "outcomes": [],
            }
        )

    def dag_readback(*, describe_assertions: list[str] | None = None) -> None:
        describe = [
            "two-task-dag-matched",
            "relation-endpoints-coherent",
            "workflow-offline",
        ]
        if describe_assertions is not None:
            describe.extend(describe_assertions)
        success(
            "workflow.describe",
            "workflow describe WORKFLOW --project PROJECT",
            describe,
        )
        success(
            "workflow.digest",
            "workflow digest WORKFLOW --project PROJECT",
            ["digest-counts-match-describe", "digest-topology-match-describe"],
        )
        success(
            "workflow.export",
            "workflow export WORKFLOW --project PROJECT",
            ["raw-yaml-dag-matches-describe", "raw-body-not-recorded"],
        )
        success(
            "task.list",
            "task list --project PROJECT --workflow WORKFLOW",
            [
                "two-task-list-matched-dag",
                (
                    "native-task-ids-coherent"
                    if ds_version == "1.3.9"
                    else "task-versions-coherent"
                ),
            ],
        )
        for _task in range(2):
            selector_kinds = ("name",) if ds_version == "1.3.9" else ("name", "native")
            for selector_kind in selector_kinds:
                success(
                    "task.get",
                    "task get TASK --project PROJECT --workflow WORKFLOW",
                    [
                        "task-get-matched-list-and-dag",
                        f"selector-{selector_kind}-matched",
                    ],
                )

    success(
        "doctor",
        "doctor",
        ["api-ready", "current-user-bound", "principal-hmac-matched"],
        outcomes=scenario_outcomes[:2],
    )
    success(
        "project.list",
        "project list --search PROJECT_NAME --page-no PAGE_NO --page-size PAGE_SIZE",
        ["bounded-exact-name-search"],
    )
    success(
        "project.get",
        "project get PROJECT",
        ["external-project-matched"],
    )
    success(
        "project.get",
        "project get PROJECT",
        ["external-project-native-identity-matched"],
    )
    success(
        "workflow.list",
        (
            "workflow list --project PROJECT --search WORKFLOW_NAME "
            "--page-no PAGE_NO --page-size PAGE_SIZE"
        ),
        ["external-workflow-native-identity-matched"],
    )
    for _selector in ("name", "native"):
        success(
            "workflow.get",
            "workflow get WORKFLOW --project PROJECT",
            ["external-workflow-and-schedule-matched"],
        )
    success(
        "schedule.list",
        (
            "schedule list --project PROJECT --workflow WORKFLOW "
            "--page-no PAGE_NO --page-size PAGE_SIZE"
        ),
        ["external-schedule-matched"],
        outcomes=[scenario_outcomes[2]],
    )
    success(
        "project.create",
        "project create --name PROJECT_NAME --description DESCRIPTION",
        ["gate-owned-project-created", "native-identity-captured"],
    )
    failure(
        "project.create",
        "project create --name PROJECT_NAME --description DESCRIPTION",
        "conflict",
        ["stable-conflict", "actionable-suggestion"],
    )
    success(
        "project.list",
        "project list --search PROJECT_NAME --page-no PAGE_NO --page-size PAGE_SIZE",
        ["bounded-exact-name-search"],
    )
    success(
        "project.get",
        "project get PROJECT",
        ["gate-owned-project-matched", "native-identity-matched"],
    )
    success(
        "project.get",
        "project get PROJECT",
        ["gate-owned-project-matched", "native-identity-matched"],
    )
    success(
        "project.update",
        "project update PROJECT --description DESCRIPTION",
        ["description-marker-updated"],
    )
    success(
        "project.get",
        "project get PROJECT",
        ["updated-description-read-back"],
        outcomes=[scenario_outcomes[3]],
    )
    success(
        "workflow.list",
        (
            "workflow list --project PROJECT --search WORKFLOW_NAME "
            "--page-no PAGE_NO --page-size PAGE_SIZE"
        ),
        ["pre-create-workflow-absent"],
    )
    success(
        "workflow.create",
        "workflow create --file WORKFLOW_FILE --project PROJECT --dry-run",
        ["installed-exact-create-plan", "dry-run-no-request-sent"],
    )
    success(
        "workflow.list",
        (
            "workflow list --project PROJECT --search WORKFLOW_NAME "
            "--page-no PAGE_NO --page-size PAGE_SIZE"
        ),
        ["create-dry-run-left-workflow-absent"],
    )
    success(
        "workflow.create",
        "workflow create --file WORKFLOW_FILE --project PROJECT",
        [
            "gate-owned-workflow-created",
            "native-workflow-code-captured",
            "workflow-offline",
        ],
    )
    success(
        "workflow.list",
        (
            "workflow list --project PROJECT --search WORKFLOW_NAME "
            "--page-no PAGE_NO --page-size PAGE_SIZE"
        ),
        ["gate-owned-workflow-listed", "native-workflow-code-matched"],
    )
    for _selector in ("name", "native"):
        success(
            "workflow.get",
            "workflow get WORKFLOW --project PROJECT",
            [
                "gate-owned-workflow-matched",
                "native-workflow-code-matched",
                "workflow-offline",
            ],
        )
    dag_readback()
    failure(
        "workflow.create",
        "workflow create --file WORKFLOW_FILE --project PROJECT",
        "conflict",
        ["stable-conflict"],
    )
    dag_readback(
        describe_assertions=["duplicate-workflow-state-unchanged"],
    )
    failure(
        "workflow.get",
        "workflow get MISSING_WORKFLOW --project PROJECT",
        "not_found",
        ["stable-not-found", "missing-workflow-nonmutating"],
    )
    if ds_version in _DEPENDENCY_UPDATE_UPSTREAM_LIMITED_VERSIONS:
        failure(
            "task.update",
            (
                "task update TASK --project PROJECT --workflow WORKFLOW "
                "--set DEPENDS_ON --dry-run"
            ),
            "unsupported_feature",
            ["dependency-update-upstream-limited", "pre-io-no-mutation"],
        )
        dag_readback()
    success(
        "task.update",
        (
            "task update TASK --project PROJECT --workflow WORKFLOW "
            "--set COMMAND --dry-run"
        ),
        ["installed-exact-task-update-plan", "dry-run-no-request-sent"],
    )
    dag_readback()
    success(
        "task.update",
        "task update TASK --project PROJECT --workflow WORKFLOW --set COMMAND",
        [
            "command-updated-exactly",
            (
                "native-task-id-preserved"
                if ds_version == "1.3.9"
                else "task-version-advanced"
            ),
            "non-owned-task-state-preserved",
        ],
    )
    dag_readback()
    success(
        "workflow.edit",
        ("workflow edit WORKFLOW --project PROJECT --patch PATCH_FILE --dry-run"),
        [
            "installed-exact-workflow-edit-plan",
            "description-only-diff",
            "dry-run-no-request-sent",
        ],
    )
    dag_readback()
    success(
        "workflow.edit",
        "workflow edit WORKFLOW --project PROJECT --patch PATCH_FILE",
        ["description-updated-exactly"],
    )
    dag_readback()
    if reconciliation_strategy == "direct-delete":
        success(
            _TASK_DEFINITION_PROVE_ACTION,
            "release-gate task-definition prove",
            [
                "project-wide-task-inventory-proven",
                "exact-two-owned-tasks",
                "zero-remote-mutations",
            ],
        )
    elif reconciliation_strategy == "workflow-cascade-proof-only":
        success(
            _TASK_DEFINITION_PROVE_ACTION,
            "release-gate task-definition prove",
            [
                "workflow-bound-task-inventory-proven",
                "batch-and-stream-inventories-proven",
                "task-history-lineage-proven",
                "exact-two-owned-tasks",
                "zero-remote-mutations",
            ],
        )
    success(
        "workflow.delete",
        "workflow delete WORKFLOW --project PROJECT --force",
        ["gate-owned-workflow-deleted"],
    )
    success(
        "workflow.list",
        (
            "workflow list --project PROJECT --search WORKFLOW_NAME "
            "--page-no PAGE_NO --page-size PAGE_SIZE"
        ),
        ["workflow-leftovers-zero"],
        outcomes=scenario_outcomes[4:8],
    )
    if reconciliation_strategy == "direct-delete":
        if ds_version in _TASK_PRE_DELETE_RELEASE_VERSIONS:
            cleanup_assertions = [
                "exact-two-online-tasks-released",
                "exact-two-owned-tasks-deleted",
                "fresh-zero-reconciliation",
                "remote-mutations-four",
            ]
        else:
            cleanup_assertions = [
                "exact-two-owned-tasks-deleted",
                "fresh-zero-reconciliation",
                "remote-mutations-two",
            ]
        success(
            _TASK_DEFINITION_CLEANUP_ACTION,
            "release-gate task-definition cleanup",
            cleanup_assertions,
        )
    elif reconciliation_strategy == "workflow-cascade-proof-only":
        success(
            _TASK_DEFINITION_CLEANUP_ACTION,
            "release-gate task-definition cleanup",
            [
                "post-cascade-zero-reconciliation",
                "observed-zero-tasks",
                "deleted-zero-tasks",
                "remaining-zero-tasks",
                "zero-remote-mutations",
            ],
        )
        failure(
            "task.list",
            "task list --project PROJECT --workflow WORKFLOW",
            "not_found",
            ["stable-not-found", "task-leftovers-zero-with-workflow-absent"],
        )
    else:
        failure(
            "task.list",
            "task list --project PROJECT --workflow WORKFLOW",
            "not_found",
            ["stable-not-found", "task-leftovers-zero-with-workflow-absent"],
        )
    success(
        "project.delete",
        "project delete PROJECT --force",
        ["gate-owned-project-deleted-after-workflow"],
    )
    failure(
        "project.get",
        "project get PROJECT",
        "not_found",
        ["stable-not-found", "deleted-project-absent"],
    )
    success(
        "project.list",
        "project list --search PROJECT_NAME --page-no PAGE_NO --page-size PAGE_SIZE",
        ["bounded-exact-name-search"],
        outcomes=scenario_outcomes[8:],
    )
    success(
        "project.list",
        "project list --search PROJECT_NAME --page-no PAGE_NO --page-size PAGE_SIZE",
        ["bounded-exact-name-search"],
    )
    success(
        "project.get",
        "project get PROJECT",
        ["external-project-matched"],
    )
    success(
        "project.get",
        "project get PROJECT",
        ["external-project-native-identity-matched"],
    )
    success(
        "workflow.list",
        (
            "workflow list --project PROJECT --search WORKFLOW_NAME "
            "--page-no PAGE_NO --page-size PAGE_SIZE"
        ),
        ["external-workflow-native-identity-matched"],
    )
    for _selector in ("name", "native"):
        success(
            "workflow.get",
            "workflow get WORKFLOW --project PROJECT",
            ["external-workflow-and-schedule-matched"],
        )
    success(
        "schedule.list",
        (
            "schedule list --project PROJECT --workflow WORKFLOW "
            "--page-no PAGE_NO --page-size PAGE_SIZE"
        ),
        ["external-schedule-matched"],
    )
    return trace


def _receipt(
    *,
    bundle_name: str = "legacy_core/v1",
    authoring_mode: str | None = None,
    ds_version: str = "3.2.2",
) -> dict[str, object]:
    if authoring_mode is None:
        authoring_mode = "opaque" if bundle_name == "full_core/v1" else "typed"
    generated = importlib.import_module("dsctl.generated.conformance_bundles")
    assessment = generated.CONFORMANCE_BUNDLE_DATA
    profile_module = importlib.import_module("dsctl.generated.version_profiles")
    dsctl_module = importlib.import_module("dsctl")
    generated_profile = profile_module.PROFILE_DATA["profiles"][ds_version]
    manifest = importlib.import_module(
        "dsctl.generated.versions.ds_" + ds_version.replace(".", "_") + "._manifest"
    )
    generated_source = generated_profile["source"]
    profile_fingerprints = copy.deepcopy(generated_profile["fingerprints"])
    generated_decisions = {
        decision["stable_action"]: decision
        for decision in generated_profile["build_decisions"].values()
    }
    assessed_bundles = {row["name"]: row for row in assessment["bundles"]}
    assessed_bundle = assessed_bundles[bundle_name]
    coordinate = next(
        row for row in assessed_bundle["versions"] if row["version"] == ds_version
    )
    required_actions = list(assessed_bundle["required_actions"])
    extends = list(assessed_bundle["extends"])
    inheritance = []
    for parent_name in extends:
        parent = assessed_bundles[parent_name]
        inheritance.append(
            {
                "name": parent_name,
                "bundle_digest": parent["bundle_digest"],
                "required_actions": list(parent["required_actions"]),
            }
        )
    scenario_outcomes = list(
        _FULL_OUTCOMES if bundle_name == "full_core/v1" else _LEGACY_OUTCOMES
    )
    scenario_definition = {
        "schema_version": 1,
        "id": f"{bundle_name.removesuffix('/v1').replace('_', '-')}-installed-wheel/v1",
        "bundle": bundle_name,
        "authoring_mode": authoring_mode,
        "required_outcomes": scenario_outcomes,
    }
    image_ref = _api_image_ref(ds_version)
    image_repository = image_ref.rpartition(":")[0]
    if bundle_name == "full_core/v1":
        trace = _full_scenario_trace(
            ds_version=ds_version,
            scenario_outcomes=scenario_outcomes,
        )
    else:
        trace = [
            {
                "sequence": sequence,
                "action": action,
                "argv_shape": action.replace(".", " "),
                "exit_code": 0,
                "ok": True,
                "assertions": [f"{action}-observed"],
                "outcomes": [],
            }
            for sequence, action in enumerate(required_actions, start=1)
        ]
        trace[0]["outcomes"] = list(scenario_outcomes[:-1])
        trace.extend(
            [
                {
                    "sequence": len(trace) + 1,
                    "action": "project.create",
                    "argv_shape": "project create --name PROJECT_NAME",
                    "exit_code": 1,
                    "ok": False,
                    "error_type": "conflict",
                    "assertions": ["stable-conflict", "actionable-suggestion"],
                    "outcomes": ["negative-errors-translated"],
                },
                {
                    "sequence": len(trace) + 2,
                    "action": "project.get",
                    "argv_shape": "project get PROJECT",
                    "exit_code": 1,
                    "ok": False,
                    "error_type": "not_found",
                    "assertions": ["stable-not-found"],
                    "outcomes": [],
                },
            ]
        )
    receipt: dict[str, object] = {
        "schema_version": 1,
        "sanitization_schema_version": 1,
        "gate": "exact-conformance-bundle",
        "status": "passed",
        "recorded_at": "2026-08-10T12:00:00+00:00",
        "runner": {
            "artifact": "installed-wheel-console-script",
            "cli_version": dsctl_module.__version__,
            "wheel_filename": f"dolphinscheduler_cli-{__version__}-py3-none-any.whl",
            "wheel_sha256": "sha256:" + "a" * 64,
        },
        "dolphinscheduler": {
            "release": ds_version,
            "image_ref": image_ref,
            "image_id": "sha256:" + "b" * 64,
            "image_source": "node-local-inspection",
            "image_provenance": {
                "kind": "registry-digest/v1",
                "repo_digests": [image_repository + "@sha256:" + "e" * 64],
                "selected_repo_digest": image_repository + "@sha256:" + "e" * 64,
            },
            "image_observed_at": "2026-08-10T11:59:00+00:00",
            "api_target_hmac_sha256": "hmac-sha256:" + "c" * 64,
            "principal_hmac_sha256": "hmac-sha256:" + "d" * 64,
            "persona": "etl-developer",
        },
        "profile": {
            "ds": ds_version,
            "selected_ds_version": ds_version,
            "contract_version": ds_version,
            "family": generated_profile["family"],
            "support_level": coordinate["support_level"],
            "tested": coordinate["tested"],
            "source": {
                "tag": ds_version,
                "commit": generated_source["commit"],
                "tree": generated_source["tree"],
            },
            "fingerprints": profile_fingerprints,
        },
        "contract": {
            "bundle_manifest_schema_version": manifest.BUNDLE_MANIFEST_SCHEMA_VERSION,
            "ds_version": manifest.DS_VERSION,
            "selection": manifest.SELECTION,
            "semantic_operations": list(manifest.SEMANTIC_OPERATIONS),
            "source_tag": manifest.SOURCE_TAG,
            "source_commit": manifest.SOURCE_COMMIT,
            "source_tree": manifest.SOURCE_TREE,
            "source_contract_digest": manifest.SOURCE_CONTRACT_DIGEST,
            "rendered_contract_digest": manifest.RENDERED_CONTRACT_DIGEST,
            "operation_count": manifest.OPERATION_COUNT,
        },
        "conformance_bundle": {
            "catalog_schema_version": assessment["schema_version"],
            "assessment_schema_version": assessment["schema_version"],
            "catalog_digest": assessment["catalog_digest"],
            "assessment_digest": assessment["assessment_digest"],
            "name": bundle_name,
            "bundle_digest": assessed_bundle["bundle_digest"],
            "coordinate_status": coordinate["status"],
            "extends": extends,
            "inheritance": inheritance,
            "direct_actions": list(assessed_bundle["direct_actions"]),
            "required_actions": list(required_actions),
            "action_recipes": [
                {
                    "action": action,
                    "semantic_operation": generated_decisions[action][
                        "semantic_operation"
                    ],
                    "availability": generated_profile["actions"][action][
                        "availability"
                    ],
                    "execution_mode": generated_profile["actions"][action][
                        "execution_mode"
                    ],
                    "verification": generated_profile["actions"][action][
                        "verification"
                    ],
                    "build_status": generated_decisions[action]["build_status"],
                    "fingerprints": copy.deepcopy(
                        generated_decisions[action]["fingerprints"]
                    ),
                }
                for action in required_actions
            ],
        },
        "scenario": {
            **scenario_definition,
            "digest": _digest(scenario_definition),
            "observed_outcomes": list(scenario_outcomes),
        },
        "fixture": {
            "manifest_sha256": "sha256:" + "9" * 64,
            "provisioner": "dsmatrix-conformance-fixture/v1",
            "identity_hmac_sha256": "hmac-sha256:" + "1" * 64,
            "before_state_hmac_sha256": "hmac-sha256:" + "2" * 64,
            "after_state_hmac_sha256": "hmac-sha256:" + "2" * 64,
            "scheduled_workflow": True,
        },
        "operation_trace": trace,
        "effects": {
            "remote_mutations": (
                11
                if bundle_name == "full_core/v1"
                and ds_version in _TASK_PRE_DELETE_RELEASE_VERSIONS
                else 9
                if bundle_name == "full_core/v1"
                and ds_version in _FULL_TASK_DEFINITION_CLEANUP_VERSIONS
                else 7
                if bundle_name == "full_core/v1"
                else 3
            ),
            "gate_owned_only": True,
            "external_fixture_mutated": False,
        },
        "cleanup": {
            "gate_owned_projects": {"confirmed": True, "leftovers": 0},
            "gate_owned_workflows": {"confirmed": True, "leftovers": 0},
            "gate_owned_tasks": {"confirmed": True, "leftovers": 0},
            "external_fixture": {
                "scope": "externally-managed-read-only",
                "mutated_by_gate": False,
                "state_hmac_matched": True,
            },
        },
        "evidence_scope": {
            "claim": "named-bundle-live-scenario",
            "observed_evidence": "live_smoke",
            "authoring_mode": authoring_mode,
            "facet_claims": [],
            "promotion_claimed": False,
            "support_level_changes": False,
            "tested_changes": False,
        },
        "secrets_recorded": False,
    }
    _add_preflight_trace(receipt)
    _refresh_receipt_digest(receipt)
    return receipt


def _api_image_ref(ds_version: str) -> str:
    if ds_version == "1.3.9":
        repository = "apache/dolphinscheduler"
    elif ds_version in {"2.0.0", "2.0.4", "2.0.7", "2.0.8", "2.0.9"}:
        repository = "dsmatrix-local/dolphinscheduler"
    elif ds_version in {"3.0.2", "3.1.2"}:
        repository = "dsmatrix-local/dolphinscheduler-api"
    elif ds_version in {
        "2.0.1",
        "2.0.2",
        "2.0.3",
        "2.0.4",
        "2.0.5",
        "2.0.6",
        "2.0.7",
        "2.0.8",
    }:
        repository = "apache/dolphinscheduler"
    else:
        repository = "apache/dolphinscheduler-api"
    return f"{repository}:{ds_version}"


def _receipt_image_contract() -> _ImageContract:
    truth = importlib.import_module("live_gate.conformance_evidence.current_truth")
    contract_path = (
        Path(__file__).resolve().parents[2]
        / "tools/live_gate/conformance_image_contract.py"
    )
    literal = read_compiled_literals(
        contract_path, {"_CONFORMANCE_IMAGE_CONTRACT_JSON"}
    )["_CONFORMANCE_IMAGE_CONTRACT_JSON"]
    contract_data = json.loads(cast("str", literal))
    return cast(
        "_ImageContract",
        truth._validate_current_image_contract(
            contract_data, versions=tuple(contract_data["api_image_repositories"])
        ),
    )


def _managed_provenance(*, ds_version: str) -> dict[str, object]:
    source_commit = _MANAGED_SOURCE_COMMITS.get(ds_version, "3" * 40)
    source_sha512 = "sha512:" + "4" * 128 if ds_version != "1.3.9" else None
    binary_sha512 = (
        "sha512:" + "5" * 128
        if ds_version in {"2.0.4", "2.0.7", "2.0.8", "2.0.9", "3.0.2", "3.1.2"}
        else None
    )
    base_manifest = (
        "mirror.example/base@sha256:" + "6" * 64 if ds_version != "1.3.9" else None
    )
    repository = _api_image_ref(ds_version).rpartition(":")[0]
    published = repository + "@sha256:" + "e" * 64 if ds_version == "1.3.9" else None
    lock: dict[str, object] = {
        "archive_basename": f"unified-{ds_version}.tar",
        "archive_sha256": "sha256:" + "7" * 64,
        "base_manifest": base_manifest,
        "binary_sha512": binary_sha512,
        "component": "unified",
        "image_id": "sha256:" + "b" * 64,
        "image_ref": _api_image_ref(ds_version),
        "published_manifest": published,
        "schema_sha256": "sha256:" + "9" * 64 if ds_version == "2.0.0" else None,
        "source_commit": source_commit,
        "source_sha512": source_sha512,
        "version": ds_version,
    }
    return {
        "actual": {
            "archive_sha256": "sha256:" + "7" * 64,
            "archive_verified": True,
            "labels": _managed_labels_from_lock(ds_version=ds_version, lock=lock),
            "repo_digests": [published] if published is not None else [],
        },
        "kind": "managed-image-lock/v1",
        "lock": lock,
        "management": {
            "commit": "1" * 40,
            "lock_file_sha256": "sha256:" + "2" * 64,
            "worktree_clean": True,
        },
    }


def _managed_labels_from_lock(
    *,
    ds_version: str,
    lock: dict[str, object],
) -> dict[str, object]:
    if ds_version == "1.3.9":
        return {}
    labels: dict[str, object] = {
        "org.apache.dolphinscheduler.matrix.source-tag": ds_version,
    }
    label_fields = {
        "org.apache.dolphinscheduler.matrix.base-digest": "base_manifest",
        "org.apache.dolphinscheduler.matrix.binary-sha512": "binary_sha512",
        "org.apache.dolphinscheduler.matrix.source-commit": "source_commit",
        "org.apache.dolphinscheduler.matrix.source-sha512": "source_sha512",
    }
    for label, field in label_fields.items():
        value = lock[field]
        if isinstance(value, str):
            labels[label] = value.removeprefix("sha512:")
    return labels


def _receipt_action_recipe(
    receipt: dict[str, object],
    *,
    action: str,
) -> dict[str, object]:
    bundle = receipt["conformance_bundle"]
    assert isinstance(bundle, dict)
    recipes = bundle["action_recipes"]
    assert isinstance(recipes, list)
    return next(
        row for row in recipes if isinstance(row, dict) and row.get("action") == action
    )


def _entry_with_assertion(
    trace: list[object],
    assertion: str,
) -> dict[str, object]:
    return next(
        entry
        for entry in trace
        if isinstance(entry, dict)
        and isinstance(entry.get("assertions"), list)
        and assertion in entry["assertions"]
    )


def _renumber_operation_trace(trace: list[object]) -> None:
    for sequence, entry in enumerate(trace, start=1):
        assert isinstance(entry, dict)
        entry["sequence"] = sequence


def _digest(value: object) -> str:
    canonical = json.dumps(
        value,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return f"sha256:{hashlib.sha256(canonical).hexdigest()}"


def _refresh_receipt_digest(receipt: dict[str, object]) -> None:
    receipt["receipt_digest"] = _digest(
        {key: value for key, value in receipt.items() if key != "receipt_digest"}
    )


def _refresh_scenario_digest(scenario: dict[str, object]) -> None:
    scenario["digest"] = _digest(
        {
            key: value
            for key, value in scenario.items()
            if key not in {"digest", "observed_outcomes"}
        }
    )


def _bundle_digest(name: str, required_actions: list[str]) -> str:
    return _digest(
        {
            "claim": "static-action-closure-only",
            "name": name,
            "required_actions": sorted(required_actions),
        }
    )


def _refresh_assessment_digests(assessment: dict[str, object]) -> None:
    bundles = assessment["bundles"]
    assert isinstance(bundles, list)
    normalized = []
    for raw_bundle in bundles:
        assert isinstance(raw_bundle, dict)
        normalized.append(
            {
                "name": raw_bundle["name"],
                "extends": sorted(raw_bundle["extends"]),
                "required_actions": sorted(raw_bundle["required_actions"]),
            }
        )
    assessment["catalog_digest"] = _digest(
        {
            "schema_version": 1,
            "kind": "dsctl-conformance-bundle-catalog",
            "claim": "static-action-closure-only",
            "bundles": sorted(normalized, key=lambda item: item["name"]),
        }
    )
    assessment["assessment_digest"] = _digest(
        {key: value for key, value in assessment.items() if key != "assessment_digest"}
    )


def _bind_receipt_to_assessment(
    receipt: dict[str, object],
    assessment: dict[str, object],
) -> None:
    raw_bundles = assessment["bundles"]
    assert isinstance(raw_bundles, list)
    bundles = {row["name"]: row for row in raw_bundles if isinstance(row, dict)}
    assessed = bundles["legacy_core/v1"]
    receipt_bundle = receipt["conformance_bundle"]
    trace = receipt["operation_trace"]
    assert isinstance(receipt_bundle, dict)
    assert isinstance(trace, list)
    for field in (
        "catalog_digest",
        "assessment_digest",
    ):
        receipt_bundle[field] = assessment[field]
    for field in (
        "name",
        "bundle_digest",
        "extends",
        "direct_actions",
        "required_actions",
    ):
        receipt_bundle[field] = copy.deepcopy(assessed[field])
    required = assessed["required_actions"]
    assert isinstance(required, list)
    recipes = receipt_bundle["action_recipes"]
    assert isinstance(recipes, list)
    receipt_bundle["action_recipes"] = [
        recipe
        for recipe in recipes
        if isinstance(recipe, dict) and recipe["action"] in required
    ]
    trace[:] = [
        entry
        for entry in trace
        if not (
            isinstance(entry, dict)
            and entry["action"] == "workflow.list"
            and entry["ok"] is True
        )
    ]
    for sequence, entry in enumerate(trace, start=1):
        assert isinstance(entry, dict)
        entry["sequence"] = sequence
    _refresh_receipt_digest(receipt)


def _add_preflight_trace(receipt: dict[str, object]) -> None:
    bundle = receipt["conformance_bundle"]
    trace = receipt["operation_trace"]
    assert isinstance(bundle, dict)
    assert isinstance(trace, list)
    required_actions = bundle["required_actions"]
    assert isinstance(required_actions, list)
    preflight: list[dict[str, object]] = [
        {
            "sequence": 1,
            "action": "version",
            "argv_shape": "version",
            "exit_code": 0,
            "ok": True,
            "assertions": ["selected-contract-and-family-matched"],
            "outcomes": [],
        }
    ]
    preflight.extend(
        {
            "sequence": 1,
            "action": "capabilities",
            "subject_action": action,
            "argv_shape": "capabilities --action ACTION",
            "exit_code": 0,
            "ok": True,
            "assertions": ["supported"],
            "outcomes": [],
        }
        for action in required_actions
    )
    trace[:0] = preflight
    for sequence, entry in enumerate(trace, start=1):
        assert isinstance(entry, dict)
        entry["sequence"] = sequence
    _refresh_receipt_digest(receipt)
