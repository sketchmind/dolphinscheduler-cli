"""Hermetic state-machine tests for the live conformance scenario."""

from __future__ import annotations

import copy
import importlib
import json
import stat
import zipfile
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import pytest
import yaml
from tests.live.conformance_bundle_gate import (
    BundleAssessment,
    ClusterIdentity,
    ConformanceBundleGateConfig,
    ConformanceScenarioCleanupError,
    ExternalFixture,
    InstalledBundleAttestation,
    NativeFixtureIdentity,
    TaskDefinitionCleanupDoNotRetryError,
    TaskDefinitionCleanupInvocation,
    _workflow_non_owned_digest,
    execute_conformance_bundle_scenario,
    identity_hmac,
    load_conformance_bundle_gate_config,
    recover_existing_full_conformance_state,
    write_conformance_bundle_candidate,
)
from tests.live.support import DsctlCommandResult
from tests.tools.conformance_bundle_testkit import ScenarioCase

from dsctl import __version__
from dsctl.generated import task_definition_cleanup_profiles as cleanup_profiles
from dsctl.generated import task_definition_profiles as task_profiles
from dsctl.generated.conformance_bundles import CONFORMANCE_BUNDLE_DATA
from dsctl.generated.version_profiles import VERSION_PROFILES

_FULL_TASK_DEFINITION_CLEANUP_VERSIONS = (
    cleanup_profiles.FULL_CORE_TASK_DEFINITION_CLEANUP_VERSIONS
)
_FULL_TASK_DEFINITION_RECONCILIATION_VERSIONS = (
    cleanup_profiles.FULL_CORE_TASK_DEFINITION_RECONCILIATION_VERSIONS
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
_PROOF_ONLY_TASK_RECONCILIATION_VERSIONS = tuple(
    ds_version
    for ds_version in _FULL_TASK_DEFINITION_RECONCILIATION_VERSIONS
    if cleanup_profiles.TASK_DEFINITION_CLEANUP_PROFILES[ds_version]["strategy"]
    == "workflow-cascade-proof-only"
)
_TASK_PRE_DELETE_RELEASE_VERSIONS = frozenset(
    ds_version
    for ds_version, profile in cleanup_profiles.TASK_DEFINITION_CLEANUP_PROFILES.items()
    if ds_version in _FULL_TASK_DEFINITION_CLEANUP_VERSIONS
    and profile.get("pre_delete_release") == "offline"
)
_TASK_DEFINITION_CLEANUP_ACTION = (
    cleanup_profiles.TASK_DEFINITION_CLEANUP_SEMANTIC_OPERATION
)
_TASK_DEFINITION_PROVE_ACTION = (
    _TASK_DEFINITION_CLEANUP_ACTION.removesuffix(".cleanup") + ".prove"
)

if TYPE_CHECKING:
    from collections.abc import Mapping


_FULL_READY_VERSIONS = tuple(
    coordinate["version"]
    for bundle in CONFORMANCE_BUNDLE_DATA["bundles"]
    if bundle["name"] == "full_core/v1"
    for coordinate in bundle["versions"]
    if coordinate["status"] == "ready"
)
_FULL_EXECUTABLE_VERSIONS = _FULL_READY_VERSIONS


@pytest.mark.parametrize(
    ("versions", "expected"),
    [
        (("1.3.9",), (False, True, False, False)),
        (("2.0.0", "2.0.1", "2.0.2"), (False, True, True, False)),
        (("2.0.3",), (False, True, False, False)),
        (
            (
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
            ),
            (True, False, False, True),
        ),
        (
            ("3.2.1", "3.2.2", "3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2"),
            (True, False, True, False),
        ),
    ],
)
def test_task_update_policies_match_independently_reviewed_source_epochs(
    versions: tuple[str, ...],
    expected: tuple[bool, bool, bool, bool],
) -> None:
    fields = (
        "update_executable",
        "whole_workflow_update",
        "dependency_update",
        "requires_unique_workflow_binding",
    )
    gate = importlib.import_module("tests.live.conformance_bundle_gate")
    limited = gate._validated_dependency_update_upstream_limited_versions()
    for version in versions:
        profile = task_profiles.TASK_DEFINITION_PROFILES[version]
        assert tuple(profile[field] for field in fields) == expected
        assert (version in limited) == (version != "1.3.9" and expected[2] is False)


def _scenario(
    config: ConformanceBundleGateConfig,
    remote: _FakeDsctl,
    task_cleanup: _FakeTaskCleanup | None = None,
) -> ScenarioCase:
    return ScenarioCase(
        config=config,
        invoke=remote,
        execute=execute_conformance_bundle_scenario,
        invoke_raw=(remote.invoke_raw if isinstance(remote, _FullFakeDsctl) else None),
        invoke_task_cleanup=task_cleanup,
    )


def test_legacy_bundle_runs_the_owned_project_lifecycle_and_restores_fixture(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    remote = _FakeDsctl(config)

    result = _scenario(config, remote).run()

    assert remote.mutations == ["create", "update", "delete"]
    assert result.effects.remote_mutations == 3
    assert result.cleanup.gate_owned_projects == 0
    assert result.cleanup.gate_owned_workflows == 0
    assert result.cleanup.gate_owned_tasks == 0
    assert result.fixture_before_hmac == result.fixture_after_hmac
    assert {entry.action for entry in result.operation_trace if entry.ok} >= set(
        config.bundle.required_actions
    )
    negative_types = {
        entry.error_type for entry in result.operation_trace if not entry.ok
    }
    assert negative_types == {"conflict", "not_found"}


def test_schedule_release_state_is_independent_from_workflow_release_state(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    assert config.fixture.workflow_release_state == "ONLINE"
    remote = _FakeDsctl(config, schedule_release_state="OFFLINE")

    result = _scenario(config, remote).run()

    assert result.fixture_before_hmac == result.fixture_after_hmac
    assert remote.mutations == ["create", "update", "delete"]


def test_schedule_list_must_match_the_hydrated_schedule_release_state(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    remote = _FakeDsctl(
        config,
        schedule_release_state="OFFLINE",
        schedule_list_release_state="ONLINE",
    )

    _scenario(config, remote).assert_run_rejected(
        AssertionError,
        "schedule release state",
    )

    assert remote.mutations == []


def test_workflow_name_and_native_reads_must_agree_on_schedule_release_state(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    remote = _FakeDsctl(
        config,
        schedule_release_state="OFFLINE",
        native_schedule_release_state="ONLINE",
    )

    _scenario(config, remote).assert_run_rejected(
        AssertionError,
        "schedule release state",
    )

    assert remote.mutations == []


def test_scenario_failure_cleans_up_the_proven_gate_owned_project(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    remote = _FakeDsctl(config, fail_at="update")

    _scenario(config, remote).assert_run_rejected(
        AssertionError,
        "gate-owned project update",
    )

    assert remote.mutations == ["create", "delete"]
    assert remote.owned_project is None


@pytest.mark.parametrize(
    "drift_at",
    ["create-result", "project-list", "get-name", "get-native"],
)
def test_pre_update_project_observations_require_the_exact_created_marker(
    tmp_path: Path,
    drift_at: str,
) -> None:
    config = _config(tmp_path)
    remote = _FakeDsctl(config, fail_at=f"early-marker-{drift_at}")

    _scenario(config, remote).assert_run_rejected(AssertionError, "description")

    assert remote.owned_project is None


def test_ambiguous_create_reconciles_once_and_never_deletes_foreign_state(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    remote = _FakeDsctl(config, fail_at="ambiguous_create_foreign")

    _scenario(config, remote).assert_run_rejected(
        AssertionError,
        "gate-owned project create",
    )

    create_calls = [argv for argv in remote.calls if argv[:2] == ["project", "create"]]
    delete_calls = [argv for argv in remote.calls if argv[:2] == ["project", "delete"]]
    assert len(create_calls) == 1
    assert delete_calls == []
    assert remote.owned_project is not None
    assert remote.owned_project["description"] == "foreign owner"


def test_ambiguous_create_reconciles_owned_state_before_cleanup(tmp_path: Path) -> None:
    config = _config(tmp_path)
    remote = _FakeDsctl(config, fail_at="ambiguous_create_owned")

    _scenario(config, remote).assert_run_rejected(
        AssertionError,
        "gate-owned project create",
    )

    create_indexes = [
        index
        for index, argv in enumerate(remote.calls)
        if argv[:2] == ["project", "create"]
    ]
    delete_indexes = [
        index
        for index, argv in enumerate(remote.calls)
        if argv[:2] == ["project", "delete"]
    ]
    assert len(create_indexes) == 1
    assert len(delete_indexes) == 1
    assert any(
        remote.calls[index][:2] == ["project", "list"]
        for index in range(create_indexes[0] + 1, delete_indexes[0])
    )
    assert remote.owned_project is None


def test_ambiguous_create_fails_closed_when_reconciliation_is_not_bounded(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    remote = _FakeDsctl(config, fail_at="ambiguous_create_owned_paginated")

    _scenario(config, remote).assert_run_rejected(ConformanceScenarioCleanupError)

    assert not any(argv[:2] == ["project", "delete"] for argv in remote.calls)
    assert remote.owned_project is not None


def test_definite_create_failure_does_not_trigger_mutation_reconciliation(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    remote = _FakeDsctl(config, fail_at="definite_create_permission")

    _scenario(config, remote).assert_run_rejected(
        AssertionError,
        "gate-owned project create",
    )

    create_index = next(
        index
        for index, argv in enumerate(remote.calls)
        if argv[:2] == ["project", "create"]
    )
    assert not any(
        argv[:2] == ["project", "list"] for argv in remote.calls[create_index + 1 :]
    )


def test_config_loader_binds_private_runtime_inputs_to_installed_bundle(
    tmp_path: Path,
) -> None:
    expected = _config(tmp_path)
    environment = _write_runtime_inputs(tmp_path, expected)

    loaded = load_conformance_bundle_gate_config(environment)

    assert loaded.ds_version == expected.ds_version
    assert loaded.bundle.required_actions == expected.bundle.required_actions
    assert loaded.installation.action_recipes == expected.installation.action_recipes
    assert loaded.fixture.project_identity.kind == "code"
    assert loaded.executable.parent == loaded.python.parent


def test_config_loader_accepts_the_341_runtime_slice(tmp_path: Path) -> None:
    expected = _config(
        tmp_path,
        ds_version="3.4.1",
        bundle_name="full_core/v1",
    )
    environment = _write_runtime_inputs(tmp_path, expected)

    loaded = load_conformance_bundle_gate_config(environment)

    assert loaded.ds_version == "3.4.1"
    assert loaded.installation.contract["selection"] == "runtime-slice"
    assert loaded.installation.contract["semantic_operations"] == []
    assert len(loaded.bundle.required_actions) == 18
    assert loaded.installation.action_recipes == expected.installation.action_recipes


def test_config_loader_binds_installed_contract_to_candidate_wheel(
    tmp_path: Path,
) -> None:
    expected = _config(tmp_path)
    environment = _write_runtime_inputs(tmp_path, expected)
    attestation_path = Path(environment["DS_LIVE_CONFORMANCE_INSTALLED_ATTESTATION"])
    attestation = json.loads(attestation_path.read_text(encoding="utf-8"))
    attestation["manifest"]["rendered_contract_digest"] = "sha256:" + "a" * 64
    attestation_path.write_text(json.dumps(attestation), encoding="utf-8")

    with pytest.raises(ValueError, match="contract differs from candidate wheel"):
        load_conformance_bundle_gate_config(environment)


def test_config_loader_rejects_historical_schema_for_installed_contract(
    tmp_path: Path,
) -> None:
    expected = _config(tmp_path)
    environment = _write_runtime_inputs(tmp_path, expected)
    attestation_path = Path(environment["DS_LIVE_CONFORMANCE_INSTALLED_ATTESTATION"])
    attestation = json.loads(attestation_path.read_text(encoding="utf-8"))
    attestation["manifest"]["bundle_manifest_schema_version"] = 1
    attestation_path.write_text(json.dumps(attestation), encoding="utf-8")

    with pytest.raises(
        ValueError,
        match="Installed contract identity differs from the exact profile",
    ):
        load_conformance_bundle_gate_config(environment)


def test_recovery_run_id_loader_reads_one_private_exact_identity(
    tmp_path: Path,
) -> None:
    gate_module = importlib.import_module("tests.live.conformance_bundle_gate")
    recovery = tmp_path / "recovery.json"
    recovery.write_text(
        json.dumps({"schema_version": 1, "run_id": "0123456789abcdef"}),
        encoding="utf-8",
    )
    recovery.chmod(0o600)

    run_id = gate_module.load_conformance_recovery_run_id(
        {"DS_LIVE_CONFORMANCE_RECOVERY_RUN_ID_FILE": str(recovery)}
    )

    assert run_id == "0123456789abcdef"
    assert gate_module.load_conformance_recovery_run_id({}) is None


def test_fixed_live_entry_uses_persisted_run_id_before_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entry = importlib.import_module("tests.live.test_exact_conformance_bundle")
    runtime = _config(tmp_path, ds_version="2.0.0", bundle_name="full_core/v1")
    identity = tmp_path / "run-id.json"
    identity.write_text(
        json.dumps({"schema_version": 1, "run_id": "0123456789abcdef"}),
        encoding="utf-8",
    )
    identity.chmod(0o600)
    monkeypatch.setenv("DS_LIVE_CONFORMANCE_RUN_ID_FILE", str(identity))
    monkeypatch.delenv("DS_LIVE_CONFORMANCE_RECOVERY_RUN_ID_FILE", raising=False)
    observed: list[str] = []

    def fake_execute(*args: object, **kwargs: object) -> object:
        run_id = kwargs["run_id"]
        assert isinstance(run_id, str)
        observed.append(run_id)
        return object()

    monkeypatch.setattr(entry, "execute_conformance_bundle_scenario", fake_execute)
    monkeypatch.setattr(
        entry, "write_conformance_bundle_candidate", lambda *a, **kw: None
    )
    entry.test_exact_conformance_bundle_installed_wheel_gate(tmp_path, runtime)
    assert observed == ["0123456789abcdef"]


def test_fixed_live_entry_dispatches_recovery_without_writing_a_candidate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entry = importlib.import_module("tests.live.test_exact_conformance_bundle")
    runtime = _config(tmp_path, ds_version="2.0.9", bundle_name="full_core/v1")
    recovery = tmp_path / "recovery.json"
    recovery.write_text(
        json.dumps({"schema_version": 1, "run_id": "0123456789abcdef"}),
        encoding="utf-8",
    )
    recovery.chmod(0o600)
    monkeypatch.setenv(
        "DS_LIVE_CONFORMANCE_RECOVERY_RUN_ID_FILE",
        str(recovery),
    )
    recovered: list[tuple[ConformanceBundleGateConfig, str]] = []

    def fake_recover(
        config: ConformanceBundleGateConfig,
        **kwargs: object,
    ) -> None:
        run_id = kwargs.get("run_id")
        assert isinstance(run_id, str)
        recovered.append((config, run_id))

    def unexpected_normal_path(*args: object, **kwargs: object) -> object:
        message = "recovery entered the normal evidence-producing path"
        raise AssertionError(message)

    monkeypatch.setattr(entry, "recover_existing_full_conformance_state", fake_recover)
    monkeypatch.setattr(
        entry, "execute_conformance_bundle_scenario", unexpected_normal_path
    )
    monkeypatch.setattr(
        entry, "write_conformance_bundle_candidate", unexpected_normal_path
    )

    entry.test_exact_conformance_bundle_installed_wheel_gate(tmp_path, runtime)

    assert recovered == [(runtime, "0123456789abcdef")]
    assert not runtime.evidence_path.exists()


def test_config_loader_rejects_installed_required_action_drift(tmp_path: Path) -> None:
    expected = _config(tmp_path)
    environment = _write_runtime_inputs(tmp_path, expected)
    attestation_path = Path(environment["DS_LIVE_CONFORMANCE_INSTALLED_ATTESTATION"])
    payload = json.loads(attestation_path.read_text(encoding="utf-8"))
    payload["required_actions"] = payload["required_actions"][:-1]
    attestation_path.write_text(json.dumps(payload), encoding="utf-8")
    attestation_path.chmod(0o600)

    with pytest.raises(ValueError, match="required actions differ"):
        load_conformance_bundle_gate_config(environment)


@pytest.mark.parametrize(
    ("environment_name", "field", "value", "expected_message"),
    [
        (
            "DS_LIVE_CONFORMANCE_CLUSTER_MANIFEST",
            "image_source",
            "ssh://matrix-host/image-inspect",
            "image_source",
        ),
        (
            "DS_LIVE_CONFORMANCE_FIXTURE_MANIFEST",
            "provisioner",
            "file:///tmp/fixture.json",
            "provisioner",
        ),
    ],
    ids=["cluster-image-source", "fixture-provisioner"],
)
def test_config_loader_rejects_noncanonical_provenance_values(
    tmp_path: Path,
    environment_name: str,
    field: str,
    value: str,
    expected_message: str,
) -> None:
    expected = _config(tmp_path)
    environment = _write_runtime_inputs(tmp_path, expected)
    manifest_path = Path(environment[environment_name])
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload[field] = value
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    manifest_path.chmod(0o600)

    with pytest.raises(ValueError, match=expected_message):
        load_conformance_bundle_gate_config(environment)


def test_config_loader_rejects_non_private_runner_snapshot(tmp_path: Path) -> None:
    expected = _config(tmp_path)
    environment = _write_runtime_inputs(tmp_path, expected)
    fixture_path = Path(environment["DS_LIVE_CONFORMANCE_FIXTURE_MANIFEST"])
    fixture_path.chmod(0o644)

    with pytest.raises(PermissionError, match="owner-private"):
        load_conformance_bundle_gate_config(environment)


def test_139_uses_id_as_the_native_project_identity(tmp_path: Path) -> None:
    config = _config(tmp_path, ds_version="1.3.9")
    remote = _FakeDsctl(config)

    _scenario(config, remote).run()

    selectors = [
        argv[2]
        for argv in remote.calls
        if argv[:2] in (["project", "update"], ["project", "delete"])
    ]
    assert selectors == ["901", "901"]
    assert remote.identity_key == "id"


@pytest.mark.parametrize("ds_version", ["1.3.9", "2.0.0", "2.0.9"])
def test_old_release_doctor_warning_uses_installed_current_user_fallback(
    tmp_path: Path,
    ds_version: str,
) -> None:
    config = _config(tmp_path, ds_version=ds_version)
    remote = _FakeDsctl(config, fail_at="doctor_warning")

    result = _scenario(config, remote).run()

    doctor = next(entry for entry in result.operation_trace if entry.action == "doctor")
    assert doctor.ok is True


def test_doctor_warning_requires_installed_current_user_recipe(tmp_path: Path) -> None:
    config = _config(tmp_path)
    recipes = tuple(
        {
            **recipe,
            "semantic_operation": "monitor.health",
        }
        if recipe["action"] == "doctor"
        else recipe
        for recipe in config.installation.action_recipes
    )
    config = replace(
        config,
        installation=replace(config.installation, action_recipes=recipes),
    )
    remote = _FakeDsctl(config, fail_at="doctor_warning")

    _scenario(config, remote).assert_run_rejected(
        AssertionError,
        "doctor did not prove",
    )


def test_ambiguous_delete_reconciles_before_one_owned_cleanup_delete(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    remote = _FakeDsctl(config, fail_at="ambiguous_delete_retained")

    _scenario(config, remote).assert_run_rejected(
        AssertionError,
        "gate-owned project delete",
    )

    delete_indexes = [
        index
        for index, argv in enumerate(remote.calls)
        if argv[:2] == ["project", "delete"]
    ]
    assert len(delete_indexes) == 2
    assert any(
        remote.calls[index][:2] == ["project", "list"]
        for index in range(delete_indexes[0] + 1, delete_indexes[1])
    )
    assert remote.owned_project is None


def test_cleanup_failure_returns_no_result_and_never_publishes(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    remote = _FakeDsctl(config, fail_at="update_cleanup_failure")

    _scenario(config, remote).assert_run_rejected(ConformanceScenarioCleanupError)

    assert remote.owned_project is not None
    assert not config.evidence_path.exists()


@pytest.mark.parametrize(
    ("ds_version", "expected_mutations"),
    [
        ("2.0.0", 9),
        ("2.0.1", 11),
        ("2.0.9", 9),
        ("3.0.0", 9),
        ("3.0.6", 9),
        ("3.1.0", 9),
    ],
)
def test_affected_full_versions_use_private_task_cleanup_and_count_both_deletes(
    tmp_path: Path,
    ds_version: str,
    expected_mutations: int,
) -> None:
    config = _config(tmp_path, ds_version=ds_version, bundle_name="full_core/v1")
    remote = _FullFakeDsctl(config)
    task_cleanup = _FakeTaskCleanup(remote)

    result = _scenario(config, remote, task_cleanup).run()

    assert remote.owned_project is None
    assert remote.owned_workflow is None
    assert remote.owned_tasks == {}
    assert result.effects.remote_mutations == expected_mutations
    assert ("task-definition-offline" in remote.mutations) is (ds_version == "2.0.1")
    assert [
        operation for operation, _project, _workflow, _run_id in task_cleanup.calls
    ] == [
        "prove",
        "cleanup",
    ]
    auxiliary = [
        entry.action
        for entry in result.operation_trace
        if entry.action.startswith("release-gate.task-definition")
    ]
    assert auxiliary == [
        _TASK_DEFINITION_PROVE_ACTION,
        _TASK_DEFINITION_CLEANUP_ACTION,
    ]


def test_full_scenario_accepts_endpoint_local_task_row_ids(tmp_path: Path) -> None:
    config = _config(tmp_path, ds_version="3.4.1", bundle_name="full_core/v1")
    remote = _FullFakeDsctl(config, endpoint_local_task_ids=True)

    result = _scenario(config, remote).run()

    assert result.cleanup.gate_owned_projects == 0
    assert result.cleanup.gate_owned_workflows == 0
    assert result.cleanup.gate_owned_tasks == 0
    assert remote.owned_project is None
    assert remote.owned_workflow is None


def test_full_scenario_accepts_endpoint_local_task_log_create_time(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, ds_version="3.4.1", bundle_name="full_core/v1")
    remote = _FullFakeDsctl(config, endpoint_local_task_create_times=True)

    result = _scenario(config, remote).run()

    assert result.cleanup.gate_owned_projects == 0
    assert result.cleanup.gate_owned_workflows == 0
    assert result.cleanup.gate_owned_tasks == 0
    assert remote.owned_project is None
    assert remote.owned_workflow is None


def test_full_scenario_still_rejects_task_detail_state_drift(tmp_path: Path) -> None:
    config = _config(tmp_path, ds_version="3.4.1", bundle_name="full_core/v1")
    remote = _FullFakeDsctl(config, fail_at="task-get-non-owned-drift")

    _scenario(config, remote).assert_run_rejected(
        AssertionError,
        "task get differs",
    )

    assert remote.owned_project is None
    assert remote.owned_workflow is None


def test_full_scenario_failure_cleans_workflow_before_project(tmp_path: Path) -> None:
    config = _config(tmp_path, ds_version="3.4.2", bundle_name="full_core/v1")
    remote = _FullFakeDsctl(config, fail_at="workflow-digest")

    _scenario(config, remote).assert_run_rejected(AssertionError, "workflow digest")

    assert remote.owned_workflow is None
    assert remote.owned_tasks == {}
    assert remote.owned_project is None
    assert remote.mutations[-2:] == ["workflow-delete", "delete"]


@pytest.mark.parametrize(
    ("fail_at", "expected_error"),
    [
        ("duplicate-workflow-state-drift", "duplicate workflow conflict changed"),
        ("workflow-edit-non-owned-drift", "outside its owned description"),
        ("lossy-workflow-export", r"exported task.*public task"),
        ("duplicate-export-task", r"export.*canonical structure"),
        ("extra-export-metadata", r"export.*canonical structure"),
        ("duplicate-task-list-row", r"task list.*exactly two unique"),
        ("duplicate-digest-task-row", r"digest.*exactly two unique"),
        ("digest-extra-top-level", r"workflow digest"),
        ("digest-workflow-extra-id", r"workflow digest"),
        ("digest-workflow-extra-global-params", r"workflow digest"),
        ("digest-workflow-extra-time", r"workflow digest"),
        ("digest-workflow-extra-user", r"workflow digest"),
        ("digest-workflow-wrong-version", r"workflow digest"),
        ("digest-workflow-wrong-project", r"workflow digest"),
        ("digest-workflow-wrong-release", r"workflow digest"),
        ("digest-workflow-wrong-schedule", r"workflow digest"),
        ("digest-global-param-names-mismatch", r"workflow digest"),
        ("duplicate-describe-task-row", r"describe.*exact two unique"),
        ("task-update-dry-run-extra-change", r"did not isolate the command field"),
        ("task-update-dry-run-wrong-before", r"did not isolate the command field"),
        ("task-update-dry-run-wrong-after", r"did not isolate the command field"),
        ("workflow-edit-dry-run-extra-change", r"was not description-only"),
        ("workflow-edit-dry-run-wrong-before", r"was not description-only"),
        ("workflow-edit-dry-run-wrong-after", r"was not description-only"),
    ],
)
def test_full_readback_rejects_each_explicit_fault_and_cleans_owned_state(
    tmp_path: Path,
    fail_at: str,
    expected_error: str,
) -> None:
    config = _config(tmp_path, ds_version="3.4.2", bundle_name="full_core/v1")
    remote = _FullFakeDsctl(config, fail_at=fail_at)

    _scenario(config, remote).assert_run_rejected(AssertionError, expected_error)

    assert remote.owned_workflow is None
    assert remote.owned_project is None


def test_full_ambiguous_workflow_create_reconciles_once_then_cleans(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, ds_version="3.4.2", bundle_name="full_core/v1")
    remote = _FullFakeDsctl(config, fail_at="ambiguous-workflow-create-applied")

    _scenario(config, remote).assert_run_rejected(AssertionError, "workflow create")

    create_calls = [
        argv
        for argv in remote.calls
        if argv[:2] == ["workflow", "create"] and "--dry-run" not in argv
    ]
    assert len(create_calls) == 1
    assert remote.owned_workflow is None
    assert remote.owned_project is None


@pytest.mark.parametrize("ds_version", ["1.3.9", "3.4.2"])
def test_full_cleanup_refuses_ambiguous_create_with_foreign_workflow_marker(
    tmp_path: Path,
    ds_version: str,
) -> None:
    config = _config(tmp_path, ds_version=ds_version, bundle_name="full_core/v1")
    remote = _FullFakeDsctl(
        config,
        fail_at="ambiguous-workflow-create-foreign-marker",
    )

    _scenario(config, remote).assert_run_rejected(ConformanceScenarioCleanupError)

    assert remote.owned_workflow is not None
    assert remote.owned_project is not None
    assert not any(argv[:2] == ["workflow", "delete"] for argv in remote.calls)
    assert not any(argv[:2] == ["project", "delete"] for argv in remote.calls)


def test_full_cleanup_refuses_disagreeing_fresh_workflow_selectors(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, ds_version="3.4.2", bundle_name="full_core/v1")
    remote = _FullFakeDsctl(
        config,
        fail_at="workflow-digest-cleanup-selector-mismatch",
    )

    _scenario(config, remote).assert_run_rejected(ConformanceScenarioCleanupError)

    assert remote.owned_workflow is not None
    assert remote.owned_project is not None
    assert not any(argv[:2] == ["workflow", "delete"] for argv in remote.calls)
    assert not any(argv[:2] == ["project", "delete"] for argv in remote.calls)


def test_full_scenario_rejects_raw_relation_versions_without_public_names(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, ds_version="3.4.2", bundle_name="full_core/v1")
    remote = _FullFakeDsctl(config, fail_at="raw-relation-shape")

    _scenario(config, remote).assert_run_rejected(ConformanceScenarioCleanupError)

    assert remote.owned_workflow is not None
    assert remote.owned_project is not None
    assert not any(argv[:2] == ["workflow", "delete"] for argv in remote.calls)


def test_full_cleanup_ambiguous_delete_retained_is_not_retried_or_published(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, ds_version="3.4.2", bundle_name="full_core/v1")
    remote = _FullFakeDsctl(
        config,
        fail_at="workflow-digest-cleanup-delete-retained",
    )

    _scenario(config, remote).assert_run_rejected(ConformanceScenarioCleanupError)

    delete_calls = [argv for argv in remote.calls if argv[:2] == ["workflow", "delete"]]
    assert len(delete_calls) == 1
    assert remote.owned_workflow is not None
    assert remote.owned_project is not None
    assert not config.evidence_path.exists()


@pytest.mark.parametrize("ds_version", ["2.0.0", "2.0.1", "2.0.9"])
def test_full_recovery_removes_proven_workflow_then_two_task_definitions(
    tmp_path: Path,
    ds_version: str,
) -> None:
    config = _config(tmp_path, ds_version=ds_version, bundle_name="full_core/v1")
    remote = _FullFakeDsctl(
        config,
        fail_at="workflow-digest-cleanup-delete-retained",
    )
    run_id = "0123456789abcdef"
    task_cleanup = _FakeTaskCleanup(remote)

    _scenario(config, remote, task_cleanup).assert_run_rejected(
        ConformanceScenarioCleanupError
    )
    remote.fail_at = None
    remote.calls.clear()
    task_cleanup.calls.clear()

    recover_existing_full_conformance_state(
        config,
        invoke=remote,
        invoke_task_cleanup=task_cleanup,
        run_id=run_id,
    )

    assert remote.owned_workflow is None
    assert remote.owned_tasks == {}
    assert remote.owned_project is None
    assert [
        operation for operation, _project, _workflow, _run in task_cleanup.calls
    ] == [
        "prove",
        "cleanup",
    ]
    workflows = [
        workflow for _operation, _project, workflow, _run in task_cleanup.calls
    ]
    assert workflows == [1001, 1001]
    assert ("task-definition-offline" in remote.mutations) is (ds_version == "2.0.1")


def test_full_recovery_requires_one_exact_preexisting_owned_residue(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, ds_version="2.0.9", bundle_name="full_core/v1")
    remote = _FullFakeDsctl(config)

    with pytest.raises(AssertionError, match="no exact gate-owned project"):
        recover_existing_full_conformance_state(
            config,
            invoke=remote,
            invoke_task_cleanup=_FakeTaskCleanup(remote),
            run_id="0123456789abcdef",
        )

    assert not any(
        argv[:2] in (["workflow", "delete"], ["project", "delete"])
        for argv in remote.calls
    )


@pytest.mark.parametrize("ds_version", ["2.0.0", "2.0.1", "2.0.9"])
def test_full_recovery_accepts_project_only_residue_with_zero_tasks(
    tmp_path: Path,
    ds_version: str,
) -> None:
    config = _config(tmp_path, ds_version=ds_version, bundle_name="full_core/v1")
    remote = _FullFakeDsctl(config)
    run_id = "0123456789abcdef"
    version_slug = config.ds_version.replace(".", "-")
    created = remote(
        [
            "project",
            "create",
            "--name",
            f"dsctl-conformance-{version_slug}-{run_id}",
            "--description",
            f"dsctl-conformance-owner:{run_id};phase=created",
        ]
    )
    assert created.payload["ok"] is True
    remote.calls.clear()
    task_cleanup = _FakeTaskCleanup(remote)

    recover_existing_full_conformance_state(
        config,
        invoke=remote,
        invoke_task_cleanup=task_cleanup,
        run_id=run_id,
    )

    assert remote.owned_project is None
    assert [
        operation for operation, _project, _workflow, _run in task_cleanup.calls
    ] == ["cleanup"]
    workflows = [
        workflow for _operation, _project, workflow, _run in task_cleanup.calls
    ]
    assert workflows == [None]
    assert not any(argv[:2] == ["workflow", "delete"] for argv in remote.calls)
    assert any(argv[:2] == ["project", "delete"] for argv in remote.calls)


@pytest.mark.parametrize("ds_version", ["2.0.0", "2.0.1", "2.0.9"])
def test_full_recovery_cleans_one_task_after_workflow_delete_crash(
    tmp_path: Path,
    ds_version: str,
) -> None:
    config = _config(tmp_path, ds_version=ds_version, bundle_name="full_core/v1")
    remote = _FullFakeDsctl(
        config,
        fail_at="workflow-digest-cleanup-delete-retained",
    )
    run_id = "0123456789abcdef"
    task_cleanup = _FakeTaskCleanup(remote)
    _scenario(config, remote, task_cleanup).assert_run_rejected(
        ConformanceScenarioCleanupError
    )
    remote.fail_at = None
    remote.owned_workflow = None
    remote.owned_relations = []
    remote.owned_tasks.pop("extract")
    remote.calls.clear()
    task_cleanup.calls.clear()

    recover_existing_full_conformance_state(
        config,
        invoke=remote,
        invoke_task_cleanup=task_cleanup,
        run_id=run_id,
    )

    assert remote.owned_tasks == {}
    assert remote.owned_project is None
    assert [
        operation for operation, _project, _workflow, _run in task_cleanup.calls
    ] == ["cleanup"]


@pytest.mark.parametrize("ds_version", ["2.0.1", "2.0.2", "2.0.3"])
def test_recovery_accepts_task_already_offline_from_prior_attempt(
    tmp_path: Path,
    ds_version: str,
) -> None:
    config = _config(tmp_path, ds_version=ds_version, bundle_name="full_core/v1")
    remote = _FullFakeDsctl(
        config,
        fail_at="workflow-digest-cleanup-delete-retained",
    )
    run_id = "0123456789abcdef"
    _scenario(config, remote, _FakeTaskCleanup(remote)).assert_run_rejected(
        ConformanceScenarioCleanupError
    )
    remote.fail_at = None
    remote.owned_workflow = None
    remote.owned_relations = []
    remote.owned_tasks.pop("extract")
    remote.calls.clear()
    remote.mutations.clear()
    cleanup_calls: list[str] = []

    def resume_offline_task(
        operation: Literal["prove", "cleanup"],
        project_code: int,
        workflow_code: int | None,
        callback_run_id: str,
    ) -> TaskDefinitionCleanupInvocation:
        assert operation == "cleanup"
        assert remote.owned_project is not None
        assert project_code == remote.owned_project["code"]
        assert workflow_code is None
        assert callback_run_id == run_id
        cleanup_calls.append(operation)
        observed = len(remote.owned_tasks)
        remote.owned_tasks = {}
        remote.mutations.extend(["task-definition-delete"] * observed)
        return TaskDefinitionCleanupInvocation(
            operation="cleanup",
            ds_version=ds_version,
            observed=observed,
            released=0,
            deleted=observed,
            remaining=0,
            remote_mutations=observed,
        )

    recover_existing_full_conformance_state(
        config,
        invoke=remote,
        invoke_task_cleanup=resume_offline_task,
        run_id=run_id,
    )

    assert cleanup_calls == ["cleanup"]
    assert remote.owned_tasks == {}
    assert remote.owned_project is None
    assert "task-definition-offline" not in remote.mutations


def test_private_cleanup_retained_ambiguity_is_never_retried(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, ds_version="2.0.9", bundle_name="full_core/v1")
    remote = _FullFakeDsctl(config)
    task_cleanup = _FakeTaskCleanup(remote, fail_at="ambiguous-retained")

    _scenario(config, remote, task_cleanup).assert_run_rejected(
        ConformanceScenarioCleanupError
    )

    assert [
        operation for operation, _project, _workflow, _run in task_cleanup.calls
    ] == [
        "prove",
        "cleanup",
    ]
    assert remote.owned_workflow is None
    assert len(remote.owned_tasks) == 2
    assert remote.owned_project is not None
    assert not any(argv[:2] == ["project", "delete"] for argv in remote.calls)
    assert not config.evidence_path.exists()


def test_full_recovery_refuses_non_generated_recovery_profile_without_invoking_dsctl(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, ds_version="3.4.2", bundle_name="full_core/v1")
    remote = _FullFakeDsctl(config)

    with pytest.raises(
        ValueError,
        match=(
            r"only supported.*2\.0\.0, 2\.0\.1, 2\.0\.2, 2\.0\.3, 2\.0\.4, 2\.0\.5, "
            r"2\.0\.6, 2\.0\.7, 2\.0\.8, 2\.0\.9, 3\.1\.3, 3\.1\.4, "
            r"3\.1\.5, 3\.1\.6, 3\.1\.7, 3\.1\.8, 3\.1\.9$"
        ),
    ):
        recover_existing_full_conformance_state(
            config,
            invoke=remote,
            run_id="0123456789abcdef",
        )

    assert remote.calls == []


def test_full_recovery_refuses_project_with_foreign_sibling_workflow(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, ds_version="2.0.9", bundle_name="full_core/v1")
    remote = _FullFakeDsctl(
        config,
        fail_at="workflow-digest-cleanup-delete-retained",
    )
    run_id = "0123456789abcdef"
    task_cleanup = _FakeTaskCleanup(remote)

    _scenario(config, remote, task_cleanup).assert_run_rejected(
        ConformanceScenarioCleanupError
    )
    remote.fail_at = None
    remote.foreign_project_workflow = True
    remote.calls.clear()

    with pytest.raises(AssertionError, match="could not isolate one owned workflow"):
        recover_existing_full_conformance_state(
            config,
            invoke=remote,
            invoke_task_cleanup=task_cleanup,
            run_id=run_id,
        )

    assert remote.owned_workflow is not None
    assert remote.owned_project is not None
    assert not any(
        argv[:2] in (["workflow", "delete"], ["project", "delete"])
        for argv in remote.calls
    )


def test_full_recovery_refuses_completed_workflow_instance_before_delete(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, ds_version="2.0.9", bundle_name="full_core/v1")
    remote = _FullFakeDsctl(
        config,
        fail_at="workflow-digest-cleanup-delete-retained",
    )
    run_id = "0123456789abcdef"
    task_cleanup = _FakeTaskCleanup(remote)

    _scenario(config, remote, task_cleanup).assert_run_rejected(
        ConformanceScenarioCleanupError
    )
    remote.fail_at = None
    remote.finished_workflow_instance = True
    remote.calls.clear()

    with pytest.raises(AssertionError, match="workflow instances"):
        recover_existing_full_conformance_state(
            config,
            invoke=remote,
            invoke_task_cleanup=task_cleanup,
            run_id=run_id,
        )

    assert remote.owned_workflow is not None
    assert remote.owned_project is not None
    assert not any(
        argv[:2] in (["workflow", "delete"], ["project", "delete"])
        for argv in remote.calls
    )


def test_full_recovery_uses_project_wide_instance_inventory_before_delete(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, ds_version="2.0.9", bundle_name="full_core/v1")
    remote = _FullFakeDsctl(
        config,
        fail_at="workflow-digest-cleanup-delete-retained",
    )
    run_id = "0123456789abcdef"
    task_cleanup = _FakeTaskCleanup(remote)

    _scenario(config, remote, task_cleanup).assert_run_rejected(
        ConformanceScenarioCleanupError
    )
    remote.fail_at = None
    remote.foreign_project_instance = True
    remote.calls.clear()

    with pytest.raises(AssertionError, match="workflow instances"):
        recover_existing_full_conformance_state(
            config,
            invoke=remote,
            invoke_task_cleanup=task_cleanup,
            run_id=run_id,
        )

    inventory_calls = [
        argv for argv in remote.calls if argv[:2] == ["workflow-instance", "list"]
    ]
    assert len(inventory_calls) == 1
    assert "--all" in inventory_calls[0]
    assert "--workflow" not in inventory_calls[0]
    assert remote.owned_workflow is not None
    assert remote.owned_project is not None
    assert not any(
        argv[:2] in (["workflow", "delete"], ["project", "delete"])
        for argv in remote.calls
    )


@pytest.mark.parametrize(
    "inventory_failure",
    [
        "workflow-instance-list-error",
        "workflow-instance-list-incomplete",
        "workflow-instance-list-malformed",
    ],
)
def test_full_recovery_fails_closed_when_instance_inventory_is_unprovable(
    tmp_path: Path,
    inventory_failure: str,
) -> None:
    config = _config(tmp_path, ds_version="2.0.9", bundle_name="full_core/v1")
    remote = _FullFakeDsctl(
        config,
        fail_at="workflow-digest-cleanup-delete-retained",
    )
    run_id = "0123456789abcdef"
    task_cleanup = _FakeTaskCleanup(remote)

    _scenario(config, remote, task_cleanup).assert_run_rejected(
        ConformanceScenarioCleanupError
    )
    remote.fail_at = inventory_failure
    remote.calls.clear()

    with pytest.raises(AssertionError):
        recover_existing_full_conformance_state(
            config,
            invoke=remote,
            invoke_task_cleanup=task_cleanup,
            run_id=run_id,
        )

    assert remote.owned_workflow is not None
    assert remote.owned_project is not None
    assert not any(
        argv[:2] in (["workflow", "delete"], ["project", "delete"])
        for argv in remote.calls
    )


def test_fixed_live_entry_provides_json_and_raw_installed_process_invokers(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    entry = importlib.import_module("tests.live.test_exact_conformance_bundle")
    config = _config(tmp_path, ds_version="2.0.9", bundle_name="full_core/v1")
    calls: list[tuple[str, tuple[str, ...]]] = []
    scenario_result = object()

    def fake_json(*_args: object, **_kwargs: object) -> object:
        calls.append(("json", ("version",)))
        return object()

    def fake_raw(*_args: object, **_kwargs: object) -> object:
        calls.append(("raw", ("workflow", "export")))
        return object()

    def fake_task_cleanup(*_args: object, **_kwargs: object) -> object:
        calls.append(("cleanup", ("prove",)))
        return object()

    def fake_execute(
        runtime: ConformanceBundleGateConfig,
        *,
        invoke: Any,
        invoke_raw: Any,
        invoke_task_cleanup: Any,
        run_id: str,
    ) -> object:
        assert runtime is config
        assert run_id == "0123456789abcdef"
        invoke(["version"])
        invoke_raw(["workflow", "export"])
        invoke_task_cleanup("prove", 1234, 5678, run_id)
        return scenario_result

    published: list[tuple[object, object]] = []
    monkeypatch.setattr(entry, "run_dsctl", fake_json)
    monkeypatch.setattr(entry, "run_dsctl_raw", fake_raw, raising=False)
    monkeypatch.setattr(
        entry,
        "run_task_definition_cleanup",
        fake_task_cleanup,
        raising=False,
    )
    monkeypatch.setattr(entry, "execute_conformance_bundle_scenario", fake_execute)
    monkeypatch.setattr(
        entry,
        "write_conformance_bundle_candidate",
        lambda runtime, *, result: published.append((runtime, result)),
    )
    monkeypatch.setattr(entry.secrets, "token_hex", lambda _size: "0123456789abcdef")

    entry.test_exact_conformance_bundle_installed_wheel_gate(tmp_path, config)

    assert calls == [
        ("json", ("version",)),
        ("raw", ("workflow", "export")),
        ("cleanup", ("prove",)),
    ]
    assert published == [(config, scenario_result)]


def test_full_blocked_coordinates_fail_before_json_or_raw_process_invocation(
    tmp_path: Path,
) -> None:
    config = _config(
        tmp_path,
        ds_version="2.0.0",
        bundle_name="full_core/v1",
    )
    config = replace(
        config,
        bundle=replace(config.bundle, coordinate_status="blocked"),
    )
    calls: list[tuple[str, tuple[str, ...]]] = []

    def invoked(kind: str) -> Any:
        def record(argv: list[str]) -> DsctlCommandResult:
            calls.append((kind, tuple(argv)))
            message = "blocked full coordinate invoked dsctl"
            raise AssertionError(message)

        return record

    with pytest.raises(ValueError, match="not static-ready"):
        execute_conformance_bundle_scenario(
            config,
            invoke=invoked("json"),
            invoke_raw=invoked("raw"),
            run_id="0123456789abcdef",
        )

    assert calls == []


def test_139_full_scenario_uses_native_task_ids_and_exact_name_gets(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, ds_version="1.3.9", bundle_name="full_core/v1")
    remote = _FullFakeDsctl(config)

    result = _scenario(config, remote).run()

    task_lists = [
        entry
        for entry in result.operation_trace
        if entry.action == "task.list"
        and "two-task-list-matched-dag" in entry.assertions
    ]
    assert task_lists
    assert all("native-task-ids-coherent" in entry.assertions for entry in task_lists)
    task_gets = [
        entry for entry in result.operation_trace if entry.action == "task.get"
    ]
    assert task_gets
    assert all("selector-name-matched" in entry.assertions for entry in task_gets)
    get_selectors = [argv[2] for argv in remote.calls if argv[:2] == ["task", "get"]]
    assert set(get_selectors) == {"extract", "load"}
    assert all(not selector.isdecimal() for selector in get_selectors)
    assert remote.owned_workflow is None
    assert remote.owned_tasks == {}
    assert remote.owned_project is None
    assert remote.mutations == [
        "create",
        "update",
        "workflow-create",
        "task-update",
        "workflow-edit",
        "workflow-delete",
        "delete",
    ]


def test_139_full_scenario_rejects_modern_fields_in_task_list(tmp_path: Path) -> None:
    config = _config(tmp_path, ds_version="1.3.9", bundle_name="full_core/v1")
    remote = _FullFakeDsctl(config, fail_at="legacy-task-list-modern-fields")

    _scenario(config, remote).assert_run_rejected(
        AssertionError,
        "task list row projection",
    )

    assert remote.owned_workflow is None
    assert remote.owned_tasks == {}
    assert remote.owned_project is None


def test_139_full_scenario_rejects_workflow_edit_version_increment(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, ds_version="1.3.9", bundle_name="full_core/v1")
    remote = _FullFakeDsctl(
        config,
        fail_at="legacy-workflow-edit-version-increment",
    )

    _scenario(config, remote).assert_run_rejected(
        AssertionError,
        "outside its owned description",
    )

    assert remote.owned_workflow is None
    assert remote.owned_tasks == {}
    assert remote.owned_project is None


@pytest.mark.parametrize(
    ("fail_at", "expected_error"),
    [
        (
            "legacy-export-missing-global-params",
            "workflow export differs from its canonical structure",
        ),
        (
            "legacy-export-missing-flag",
            "exported task differs from its canonical public task projection",
        ),
    ],
)
def test_139_full_scenario_rejects_missing_canonical_export_defaults(
    tmp_path: Path,
    fail_at: str,
    expected_error: str,
) -> None:
    config = _config(tmp_path, ds_version="1.3.9", bundle_name="full_core/v1")
    remote = _FullFakeDsctl(config, fail_at=fail_at)

    _scenario(config, remote).assert_run_rejected(AssertionError, expected_error)

    assert remote.owned_workflow is None
    assert remote.owned_tasks == {}
    assert remote.owned_project is None


@pytest.mark.parametrize(
    ("ds_version", "expected_error"),
    [
        ("1.3.9", "workflow export differs from its canonical structure"),
        ("3.4.2", "task update changed workflow identity or topology"),
    ],
)
def test_full_scenario_rejects_nonempty_global_param_task_update_drift(
    tmp_path: Path,
    ds_version: str,
    expected_error: str,
) -> None:
    config = _config(tmp_path, ds_version=ds_version, bundle_name="full_core/v1")
    remote = _FullFakeDsctl(config, fail_at="task-update-global-params-drift")

    _scenario(config, remote).assert_run_rejected(
        AssertionError,
        expected_error,
    )

    assert remote.owned_workflow is None
    assert remote.owned_tasks == {}
    assert remote.owned_project is None


def test_139_workflow_digest_normalizes_only_exact_empty_global_params() -> None:
    created = {"name": "owned", "globalParams": None, "globalParamMap": None}
    updated = {"name": "owned", "globalParams": "[]", "globalParamMap": {}}

    assert _workflow_non_owned_digest(created, ds_version="1.3.9") == (
        _workflow_non_owned_digest(updated, ds_version="1.3.9")
    )
    assert _workflow_non_owned_digest(created, ds_version="3.4.2") != (
        _workflow_non_owned_digest(updated, ds_version="3.4.2")
    )
    drifts: tuple[Mapping[str, object], ...] = (
        {"name": "owned"},
        {"name": "owned", "globalParams": None},
        {"name": "owned", "globalParamMap": None},
        {"name": "owned", "globalParams": "[]", "globalParamMap": None},
        {"name": "owned", "globalParams": None, "globalParamMap": {}},
        {
            "name": "owned",
            "globalParams": '[{"prop":"foreign"}]',
            "globalParamMap": {"foreign": "state"},
        },
    )
    for drift in drifts:
        assert _workflow_non_owned_digest(created, ds_version="1.3.9") != (
            _workflow_non_owned_digest(drift, ds_version="1.3.9")
        )


def test_modern_full_scenario_rejects_zero_workflow_version(tmp_path: Path) -> None:
    config = _config(tmp_path, ds_version="3.4.2", bundle_name="full_core/v1")
    remote = _FullFakeDsctl(config, fail_at="zero-workflow-version")

    _scenario(config, remote).assert_run_rejected(ConformanceScenarioCleanupError)

    assert remote.owned_workflow is not None
    assert remote.owned_project is not None
    assert not any(argv[:2] == ["workflow", "delete"] for argv in remote.calls)
    assert not any(argv[:2] == ["project", "delete"] for argv in remote.calls)


def test_passing_scenario_writes_one_private_publicly_validated_candidate(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    config.wheel.write_bytes(b"candidate wheel")
    result = _scenario(config, _FakeDsctl(config)).run()

    receipt = write_conformance_bundle_candidate(config, result=result)

    assert receipt["status"] == "passed"
    assert _mapping(receipt["dolphinscheduler"])["image_provenance"] == (
        config.cluster.image_provenance
    )
    assert receipt["evidence_scope"] == {
        "claim": "named-bundle-live-scenario",
        "observed_evidence": "live_smoke",
        "authoring_mode": "typed",
        "facet_claims": [],
        "promotion_claimed": False,
        "support_level_changes": False,
        "tested_changes": False,
    }
    assert stat.S_IMODE(config.evidence_path.stat().st_mode) == 0o600
    with pytest.raises(FileExistsError):
        write_conformance_bundle_candidate(config, result=result)


@pytest.mark.parametrize("ds_version", ["3.0.2", "3.1.2"])
def test_managed_modern_scenario_uses_exact_local_api_repository(
    tmp_path: Path,
    ds_version: str,
) -> None:
    config = _config(tmp_path, ds_version=ds_version)
    expected_repository = "dsmatrix-local/dolphinscheduler-api"

    assert config.cluster.image_ref == f"{expected_repository}:{ds_version}"
    provenance = config.cluster.image_provenance
    assert provenance["repo_digests"] == [expected_repository + "@sha256:" + "e" * 64]
    assert provenance["selected_repo_digest"] == (
        expected_repository + "@sha256:" + "e" * 64
    )


def test_candidate_validation_failure_creates_no_file(tmp_path: Path) -> None:
    config = _config(tmp_path)
    config.wheel.write_bytes(b"candidate wheel")
    result = _scenario(config, _FakeDsctl(config)).run()

    def reject_candidate(_receipt: object, **_expected: object) -> None:
        message = "synthetic evidence rejection"
        raise ValueError(message)

    with pytest.raises(ValueError, match="synthetic evidence rejection"):
        write_conformance_bundle_candidate(
            config,
            result=result,
            validator=reject_candidate,
        )

    assert not config.evidence_path.exists()


def test_full_209_refuses_missing_private_cleanup_before_any_remote_call(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, ds_version="2.0.9", bundle_name="full_core/v1")
    config.wheel.write_bytes(b"candidate wheel")
    remote = _FullFakeDsctl(config)

    _scenario(config, remote).assert_run_rejected(
        ValueError,
        "private task-definition cleanup runner",
    )

    assert not config.evidence_path.exists()
    assert remote.calls == []
    assert remote.owned_project is None
    assert remote.owned_workflow is None
    assert remote.owned_tasks == {}


@pytest.mark.parametrize(
    "ds_version",
    _DEPENDENCY_UPDATE_UPSTREAM_LIMITED_VERSIONS,
)
def test_full_scenario_records_generated_dependency_update_pre_io_rejection(
    tmp_path: Path,
    ds_version: str,
) -> None:
    config = _config(tmp_path, ds_version=ds_version, bundle_name="full_core/v1")
    remote = _FullFakeDsctl(config)
    task_cleanup = (
        _FakeTaskCleanup(remote)
        if ds_version in _FULL_TASK_DEFINITION_RECONCILIATION_VERSIONS
        else None
    )

    result = _scenario(config, remote, task_cleanup).run()

    dependency_rejections = [
        entry
        for entry in result.operation_trace
        if "dependency-update-upstream-limited" in entry.assertions
    ]
    assert len(dependency_rejections) == 1
    rejection = dependency_rejections[0]
    assert rejection.action == "task.update"
    assert rejection.error_type == "unsupported_feature"
    assert rejection.assertions == (
        "dependency-update-upstream-limited",
        "pre-io-no-mutation",
    )


@pytest.mark.parametrize(
    "ds_version",
    _PROOF_ONLY_TASK_RECONCILIATION_VERSIONS,
)
def test_full_scenario_records_proof_only_task_reconciliation_lifecycle(
    tmp_path: Path,
    ds_version: str,
) -> None:
    config = _config(tmp_path, ds_version=ds_version, bundle_name="full_core/v1")
    remote = _FullFakeDsctl(config)
    task_cleanup = _FakeTaskCleanup(remote)

    result = _scenario(config, remote, task_cleanup).run()

    proof = next(
        entry
        for entry in result.operation_trace
        if entry.action == _TASK_DEFINITION_PROVE_ACTION
    )
    cleanup = next(
        entry
        for entry in result.operation_trace
        if entry.action == _TASK_DEFINITION_CLEANUP_ACTION
    )
    assert proof.assertions == (
        "workflow-bound-task-inventory-proven",
        "batch-and-stream-inventories-proven",
        "task-history-lineage-proven",
        "exact-two-owned-tasks",
        "zero-remote-mutations",
    )
    assert cleanup.assertions == (
        "post-cascade-zero-reconciliation",
        "observed-zero-tasks",
        "deleted-zero-tasks",
        "remaining-zero-tasks",
        "zero-remote-mutations",
    )
    assert result.effects.remote_mutations == 7
    assert "task-definition-delete" not in remote.mutations


@pytest.mark.parametrize("ds_version", _FULL_EXECUTABLE_VERSIONS)
def test_full_receipt_validates_for_every_executable_exact_version(
    tmp_path: Path,
    ds_version: str,
) -> None:
    config = _config(
        tmp_path,
        ds_version=ds_version,
        bundle_name="full_core/v1",
    )
    config.wheel.write_bytes(b"candidate wheel")
    remote = _FullFakeDsctl(config)
    task_cleanup = (
        _FakeTaskCleanup(remote)
        if ds_version in _FULL_TASK_DEFINITION_RECONCILIATION_VERSIONS
        else None
    )

    result = _scenario(config, remote, task_cleanup).run()
    receipt = write_conformance_bundle_candidate(config, result=result)

    assert receipt["status"] == "passed"
    affected = ds_version in _FULL_TASK_DEFINITION_CLEANUP_VERSIONS
    expected_task_cleanup_mutations = (
        4 if ds_version in _TASK_PRE_DELETE_RELEASE_VERSIONS else 2
    )
    assert result.effects.remote_mutations == (
        7 + expected_task_cleanup_mutations if affected else 7
    )
    expected_mutations = [
        "create",
        "update",
        "workflow-create",
        "task-update",
        "workflow-edit",
        "workflow-delete",
    ]
    if affected:
        if ds_version in _TASK_PRE_DELETE_RELEASE_VERSIONS:
            expected_mutations.extend(
                ["task-definition-offline", "task-definition-offline"]
            )
        expected_mutations.extend(["task-definition-delete", "task-definition-delete"])
    expected_mutations.append("delete")
    assert remote.mutations == expected_mutations
    successful_actions = {entry.action for entry in result.operation_trace if entry.ok}
    assert successful_actions >= set(config.bundle.required_actions)


@pytest.mark.parametrize("ds_version", tuple(VERSION_PROFILES))
def test_legacy_receipt_validates_for_every_static_ready_exact_version(
    tmp_path: Path,
    ds_version: str,
) -> None:
    config = _config(tmp_path, ds_version=ds_version)
    config.wheel.write_bytes(b"candidate wheel")
    result = _scenario(config, _FakeDsctl(config)).run()

    receipt = write_conformance_bundle_candidate(config, result=result)

    assert receipt["status"] == "passed"
    cluster = receipt["dolphinscheduler"]
    assert isinstance(cluster, dict)
    assert cluster["release"] == ds_version


class _FakeDsctl:
    def __init__(
        self,
        config: ConformanceBundleGateConfig,
        *,
        fail_at: str | None = None,
        schedule_release_state: str = "ONLINE",
        schedule_list_release_state: str | None = None,
        native_schedule_release_state: str | None = None,
    ) -> None:
        self.config = config
        self.fail_at = fail_at
        self.identity_key = "id" if config.ds_version == "1.3.9" else "code"
        self.external_project = {
            self.identity_key: 41,
            "name": config.fixture.project_name,
            "description": "external project",
        }
        self.external_schedule: dict[str, object] = {
            "id": config.fixture.schedule_id,
            "releaseState": schedule_release_state,
        }
        self.external_workflow = {
            self.identity_key: 77,
            "name": config.fixture.workflow_name,
            "releaseState": config.fixture.workflow_release_state,
            "schedule": self.external_schedule,
            "scheduleId": config.fixture.schedule_id,
        }
        self.native_schedule_release_state = native_schedule_release_state
        self.schedule = {
            "id": config.fixture.schedule_id,
            "releaseState": schedule_list_release_state or schedule_release_state,
            "workflowDefinitionCode": config.fixture.workflow_identity.value,
            "workflowDefinitionName": config.fixture.workflow_name,
        }
        self.owned_project: dict[str, object] | None = None
        self.mutations: list[str] = []
        self.calls: list[list[str]] = []

    def __call__(self, argv: list[str]) -> DsctlCommandResult:  # noqa: C901
        self.calls.append(list(argv))
        action = tuple(argv[:2])
        if argv == ["version"]:
            profile = self.config.installation.profile
            return self._ok(
                argv,
                "version",
                {
                    "cli": self.config.installation.distribution_version,
                    "ds": self.config.ds_version,
                    "selected_ds_version": self.config.ds_version,
                    "contract_version": self.config.ds_version,
                    "family": profile["family"],
                    "support_level": profile["support_level"],
                },
            )
        if action == ("capabilities", "--action"):
            subject = argv[2]
            recipe = next(
                item
                for item in self.config.installation.action_recipes
                if item["action"] == subject
            )
            return self._ok(
                argv,
                "capabilities",
                {
                    "capability": {
                        "action": subject,
                        "availability": "supported",
                        "verification": recipe["verification"],
                    }
                },
            )
        if argv == ["doctor"]:
            api_check: dict[str, object] = {
                "name": "api",
                "status": "ok",
                "details": {},
            }
            if self.fail_at == "doctor_warning":
                api_check = {
                    "name": "api",
                    "status": "warning",
                    "details": {"verification_fallback": "current_user"},
                }
            return self._ok(
                argv,
                "doctor",
                {
                    "checks": [
                        api_check,
                        {
                            "name": "current_user",
                            "status": "ok",
                            "details": {
                                "userName": "etl-user",
                                "userType": "GENERAL_USER",
                            },
                        },
                    ]
                },
            )
        if action == ("project", "create"):
            name = _option(argv, "--name")
            description = _option(argv, "--description")
            if self.fail_at == "definite_create_permission":
                return self._error(
                    argv,
                    "project.create",
                    "permission_denied",
                    details={"mutation_applied": False},
                )
            if self.fail_at in {
                "ambiguous_create_owned",
                "ambiguous_create_owned_paginated",
            }:
                self.owned_project = {
                    self.identity_key: 901,
                    "name": name,
                    "description": description,
                }
                self.mutations.append("ambiguous-create-owned")
                return self._error(
                    argv,
                    "project.create",
                    "api_transport_error",
                    details={"mutation_may_have_applied": True},
                )
            if self.fail_at == "ambiguous_create_foreign":
                self.owned_project = {
                    self.identity_key: 901,
                    "name": name,
                    "description": "foreign owner",
                }
                self.mutations.append("ambiguous-create-foreign")
                return self._error(
                    argv,
                    "project.create",
                    "api_transport_error",
                    details={"mutation_may_have_applied": True},
                )
            if self.owned_project is not None:
                return self._error(
                    argv,
                    "project.create",
                    "conflict",
                    suggestion="Retry with a unique --name.",
                )
            self.owned_project = {
                self.identity_key: 901,
                "name": name,
                "description": description,
            }
            self.mutations.append("create")
            created = dict(self.owned_project)
            if self.fail_at == "early-marker-create-result":
                created["description"] = _updated_marker(description)
                self.fail_at = None
            return self._ok(argv, "project.create", created)
        if action == ("project", "list"):
            search = _option(argv, "--search")
            rows = [
                row
                for row in (self.external_project, self.owned_project)
                if row is not None and row.get("name") == search
            ]
            if (
                self.fail_at == "early-marker-project-list"
                and self.owned_project is not None
                and search == self.owned_project["name"]
            ):
                rows = [
                    {
                        **row,
                        "description": _updated_marker(str(row["description"])),
                    }
                    for row in rows
                ]
                self.fail_at = None
            page = _page(rows)
            if (
                self.fail_at == "ambiguous_create_owned_paginated"
                and self.owned_project is not None
                and search == self.owned_project["name"]
            ):
                page["total"] = 21
                page["totalPage"] = 2
            return self._ok(argv, "project.list", page)
        if action == ("project", "get"):
            selector = argv[2]
            row = self._project(selector)
            if row is None:
                return self._error(argv, "project.get", "not_found")
            data = dict(row)
            drift = (
                self.fail_at == "early-marker-get-name" and not selector.isdigit()
            ) or (self.fail_at == "early-marker-get-native" and selector.isdigit())
            if row is self.owned_project and drift:
                data["description"] = _updated_marker(str(row["description"]))
                self.fail_at = None
            return self._ok(argv, "project.get", data)
        if action == ("project", "update"):
            if self.fail_at in {"update", "update_cleanup_failure"}:
                return self._error(argv, "project.update", "api_result_error")
            row = self._project(argv[2])
            if row is None or row is self.external_project:
                return self._error(argv, "project.update", "not_found")
            row["description"] = _option(argv, "--description")
            self.mutations.append("update")
            return self._ok(argv, "project.update", dict(row))
        if action == ("project", "delete"):
            row = self._project(argv[2])
            if row is None or row is self.external_project:
                return self._error(argv, "project.delete", "not_found")
            if self.fail_at == "update_cleanup_failure":
                return self._error(
                    argv,
                    "project.delete",
                    "api_transport_error",
                    details={"mutation_may_have_applied": True},
                )
            if self.fail_at == "ambiguous_delete_retained":
                self.fail_at = None
                return self._error(
                    argv,
                    "project.delete",
                    "api_transport_error",
                    details={"mutation_may_have_applied": True},
                )
            deleted = dict(row)
            self.owned_project = None
            self.mutations.append("delete")
            return self._ok(
                argv,
                "project.delete",
                {"deleted": True, "project": deleted},
            )
        if action == ("workflow", "list"):
            return self._ok(argv, "workflow.list", _page([self.external_workflow]))
        if action == ("workflow", "get"):
            workflow = dict(self.external_workflow)
            schedule = dict(self.external_schedule)
            if argv[2].isdigit() and self.native_schedule_release_state is not None:
                schedule["releaseState"] = self.native_schedule_release_state
            workflow["schedule"] = schedule
            return self._ok(argv, "workflow.get", workflow)
        if action == ("schedule", "list"):
            return self._ok(argv, "schedule.list", _page([self.schedule]))
        message = f"unexpected fake dsctl invocation: {argv!r}"
        raise AssertionError(message)

    def _project(self, selector: str) -> dict[str, object] | None:
        for row in (self.external_project, self.owned_project):
            if row is not None and selector in {
                str(row[self.identity_key]),
                str(row["name"]),
            }:
                return row
        return None

    @staticmethod
    def _ok(
        argv: list[str],
        action: str,
        data: object,
        *,
        extra: Mapping[str, object] | None = None,
    ) -> DsctlCommandResult:
        payload: dict[str, object] = {"ok": True, "action": action, "data": data}
        if extra is not None:
            payload.update(extra)
        return DsctlCommandResult(
            argv=tuple(argv),
            exit_code=0,
            stdout=json.dumps(payload),
            stderr="",
            payload=payload,
        )

    @staticmethod
    def _error(
        argv: list[str],
        action: str,
        error_type: str,
        *,
        suggestion: str | None = None,
        details: Mapping[str, object] | None = None,
    ) -> DsctlCommandResult:
        error: dict[str, object] = {
            "type": error_type,
            "message": f"synthetic {error_type}",
        }
        if suggestion is not None:
            error["suggestion"] = suggestion
        if details is not None:
            error["details"] = dict(details)
        payload: dict[str, object] = {
            "ok": False,
            "action": action,
            "error": error,
        }
        return DsctlCommandResult(
            argv=tuple(argv),
            exit_code=1,
            stdout="",
            stderr=json.dumps(payload),
            payload=payload,
        )


class _FakeTaskCleanup:
    def __init__(
        self,
        remote: _FullFakeDsctl,
        *,
        fail_at: str | None = None,
    ) -> None:
        self._remote = remote
        self._fail_at = fail_at
        self.calls: list[tuple[str, int, int | None, str]] = []

    def __call__(
        self,
        operation: str,
        project_code: int,
        workflow_code: int | None,
        run_id: str,
    ) -> TaskDefinitionCleanupInvocation:
        project = self._remote.owned_project
        if project is None or project.get("code") != project_code:
            message = "private task cleanup received a drifting project identity"
            raise AssertionError(message)
        self.calls.append((operation, project_code, workflow_code, run_id))
        observed = len(self._remote.owned_tasks)
        if operation == "prove":
            return TaskDefinitionCleanupInvocation(
                operation="prove",
                ds_version=self._remote.config.ds_version,
                observed=observed,
                released=0,
                deleted=0,
                remaining=observed,
                remote_mutations=0,
            )
        if operation != "cleanup":
            message = "unknown private task cleanup operation"
            raise AssertionError(message)
        if self._fail_at == "ambiguous-retained":
            self._fail_at = None
            message = "private delete remained after fresh reconciliation"
            raise TaskDefinitionCleanupDoNotRetryError(message)
        released = (
            observed
            if self._remote.config.ds_version in _TASK_PRE_DELETE_RELEASE_VERSIONS
            else 0
        )
        self._remote.owned_tasks = {}
        self._remote.mutations.extend(["task-definition-offline"] * released)
        self._remote.mutations.extend(["task-definition-delete"] * observed)
        return TaskDefinitionCleanupInvocation(
            operation="cleanup",
            ds_version=self._remote.config.ds_version,
            observed=observed,
            released=released,
            deleted=observed,
            remaining=0,
            remote_mutations=released + observed,
        )


class _FullFakeDsctl(_FakeDsctl):
    def __init__(
        self,
        config: ConformanceBundleGateConfig,
        *,
        fail_at: str | None = None,
        endpoint_local_task_ids: bool = False,
        endpoint_local_task_create_times: bool = False,
    ) -> None:
        super().__init__(config, fail_at=fail_at)
        self.owned_workflow: dict[str, object] | None = None
        self.owned_tasks: dict[str, dict[str, object]] = {}
        self.owned_relations: list[dict[str, object]] = []
        self.cleanup_selector_mismatch = False
        self.endpoint_local_task_ids = endpoint_local_task_ids
        self.endpoint_local_task_create_times = endpoint_local_task_create_times
        self.foreign_project_workflow = False
        self.finished_workflow_instance = False
        self.foreign_project_instance = False

    def __call__(self, argv: list[str]) -> DsctlCommandResult:  # noqa: C901
        action = tuple(argv[:2])
        if action == ("project", "delete") and self.owned_workflow is not None:
            self.calls.append(list(argv))
            return self._error(
                argv,
                "project.delete",
                "invalid_state",
                details={"mutation_applied": False},
            )
        if argv and argv[0] in {"task", "workflow", "workflow-instance"}:
            self.calls.append(list(argv))
        if action == ("workflow", "create"):
            return self._workflow_create(argv)
        if action == ("workflow", "list") and self._uses_owned_project(argv):
            return self._workflow_list(argv)
        if action == ("workflow", "get") and self._uses_owned_project(argv):
            return self._workflow_get(argv)
        if action == ("workflow", "describe"):
            return self._workflow_describe(argv)
        if action == ("workflow", "digest"):
            return self._workflow_digest_result(argv)
        if action == ("workflow", "export"):
            return self.invoke_raw(argv)
        if action == ("workflow", "edit"):
            return self._workflow_edit(argv)
        if action == ("workflow", "delete"):
            return self._workflow_delete(argv)
        if action == ("workflow-instance", "list"):
            return self._workflow_instance_list(argv)
        if action == ("task", "list"):
            return self._task_list(argv)
        if action == ("task", "get"):
            return self._task_get(argv)
        if action == ("task", "update"):
            return self._task_update(argv)
        return super().__call__(argv)

    def invoke_raw(self, argv: list[str]) -> DsctlCommandResult:
        if tuple(argv[:2]) != ("workflow", "export"):
            return self(argv)
        self.calls.append(list(argv))
        workflow = self._selected_workflow(argv[2])
        if workflow is None:
            return self._error(argv, "workflow.export", "not_found")
        document = self._export_document()
        return DsctlCommandResult(
            argv=tuple(argv),
            exit_code=0,
            stdout=yaml.safe_dump(document, sort_keys=False),
            stderr="",
            payload={},
        )

    def _workflow_create(self, argv: list[str]) -> DsctlCommandResult:
        assert self.owned_project is not None
        assert _option(argv, "--project") == self.owned_project["name"]
        document = self._load_yaml(_option(argv, "--file"))
        if "--dry-run" in argv:
            assert _option(argv, "--columns") == "*"
            preview_tasks, preview_relations = self._compiled_graph(
                document,
                task_codes=(9001, 9002),
            )
            workflow = _mapping(document["workflow"])
            return self._ok(
                argv,
                "workflow.create",
                {
                    "dry_run": True,
                    "requests": [
                        {
                            "method": "POST",
                            "path": "/installed-exact-plan/workflow-definition",
                            "form": {
                                "name": workflow["name"],
                                "description": workflow["description"],
                                "taskDefinitionJson": json.dumps(preview_tasks),
                                "taskRelationJson": json.dumps(preview_relations),
                            },
                        }
                    ],
                },
                extra={
                    "warnings": [
                        {
                            "code": "dry_run_no_mutation_sent",
                            "message": (
                                "dry run: no mutation was sent; lookup and "
                                "verification reads may occur"
                            ),
                            "mutation_sent": False,
                        }
                    ]
                },
            )
        existing_workflow = self.owned_workflow
        if existing_workflow is not None:
            if self.fail_at == "duplicate-workflow-state-drift":
                existing_workflow["version"] = (
                    _fake_int(existing_workflow["version"]) + 1
                )
                self.fail_at = None
            return self._error(argv, "workflow.create", "conflict")
        self._install_workflow(document)
        self.mutations.append("workflow-create")
        owned_workflow = self.owned_workflow
        if owned_workflow is None:
            message = "fake workflow create did not install its workflow"
            raise AssertionError(message)
        if self.fail_at in {
            "ambiguous-workflow-create-applied",
            "ambiguous-workflow-create-foreign-marker",
        }:
            if self.fail_at == "ambiguous-workflow-create-foreign-marker":
                owned_workflow["description"] = "foreign workflow"
            self.fail_at = None
            return self._error(
                argv,
                "workflow.create",
                "api_transport_error",
                details={"mutation_may_have_applied": True},
            )
        return self._ok(
            argv,
            "workflow.create",
            self._workflow_public(owned_workflow),
        )

    def _workflow_list(self, argv: list[str]) -> DsctlCommandResult:
        search = _option(argv, "--search") if "--search" in argv else None
        rows: list[dict[str, object]] = []
        candidates: list[dict[str, object]] = []
        if self.owned_workflow is not None:
            candidates.append(self.owned_workflow)
        if self.foreign_project_workflow:
            candidates.append(
                {
                    "code": 8008,
                    "name": "foreign-sibling-workflow",
                    "version": 1,
                    "releaseState": "OFFLINE",
                }
            )
        for workflow in candidates:
            if search is not None and workflow["name"] != search:
                continue
            identity_key = "id" if self.config.ds_version == "1.3.9" else "code"
            rows.append(
                {
                    identity_key: workflow["code"],
                    "name": workflow["name"],
                    "version": workflow["version"],
                    "releaseState": workflow["releaseState"],
                    "scheduleReleaseState": None,
                    "scheduleId": None,
                }
            )
        return self._ok(argv, "workflow.list", _page(rows))

    def _workflow_get(self, argv: list[str]) -> DsctlCommandResult:
        workflow = self._selected_workflow(argv[2])
        if workflow is None:
            return self._error(argv, "workflow.get", "not_found")
        projected = self._workflow_public(workflow)
        if self.cleanup_selector_mismatch and argv[2].isdigit():
            projected["description"] = "foreign workflow"
        return self._ok(argv, "workflow.get", projected)

    def _workflow_instance_list(self, argv: list[str]) -> DsctlCommandResult:
        assert self.owned_project is not None
        assert _option(argv, "--project") == self.owned_project["name"]
        assert _option(argv, "--page-no") == "1"
        assert _option(argv, "--page-size") == "20"
        assert "--all" in argv
        if "--workflow" in argv:
            assert self.owned_workflow is not None
            assert _option(argv, "--workflow") == self.owned_workflow["name"]
        if self.fail_at == "workflow-instance-list-error":
            return self._error(
                argv,
                "workflow-instance.list",
                "api_result_error",
            )
        rows: list[dict[str, object]] = []
        if self.finished_workflow_instance:
            assert self.owned_workflow is not None
            rows.append(
                {
                    "id": 3001,
                    "workflowDefinitionCode": self.owned_workflow["code"],
                    "projectCode": self._owned_project_native(),
                    "name": f"{self.owned_workflow['name']}-1",
                    "state": "SUCCESS",
                }
            )
        if self.foreign_project_instance and "--workflow" not in argv:
            rows.append(
                {
                    "id": 3002,
                    "workflowDefinitionCode": 8008,
                    "projectCode": self._owned_project_native(),
                    "name": "foreign-workflow-instance-1",
                    "state": "SUCCESS",
                }
            )
        page = _page(rows)
        if rows:
            page["pageSize"] = len(rows)
        if self.fail_at == "workflow-instance-list-incomplete":
            page["total"] = 21
            page["totalPage"] = 2
        elif self.fail_at == "workflow-instance-list-malformed":
            page["pageSize"] = "20"
        return self._ok(argv, "workflow-instance.list", page)

    def _workflow_describe(self, argv: list[str]) -> DsctlCommandResult:
        workflow = self._selected_workflow(argv[2])
        if workflow is None:
            return self._error(argv, "workflow.describe", "not_found")
        task_names = {
            _fake_int(task["code"]): str(task["name"])
            for task in self.owned_tasks.values()
        }
        if self.config.ds_version == "1.3.9":
            tasks = [
                {
                    "id": task["legacyId"],
                    "name": task["name"],
                    "type": task["taskType"],
                    "taskParams": copy.deepcopy(task["taskParams"]),
                    "dependsOn": [
                        task_names[_fake_int(relation["preTaskCode"])]
                        for relation in self.owned_relations
                        if _fake_int(relation["preTaskCode"]) != 0
                        and _fake_int(relation["postTaskCode"])
                        == _fake_int(task["code"])
                    ],
                }
                for task in sorted(
                    self.owned_tasks.values(),
                    key=lambda item: _fake_int(item["code"]),
                )
            ]
            by_code = {
                _fake_int(task["code"]): task for task in self.owned_tasks.values()
            }
            relations = [
                {
                    "preTaskId": by_code[_fake_int(relation["preTaskCode"])][
                        "legacyId"
                    ],
                    "preTaskName": task_names[_fake_int(relation["preTaskCode"])],
                    "postTaskId": by_code[_fake_int(relation["postTaskCode"])][
                        "legacyId"
                    ],
                    "postTaskName": task_names[_fake_int(relation["postTaskCode"])],
                }
                for relation in self.owned_relations
                if _fake_int(relation["preTaskCode"]) != 0
            ]
            return self._ok(
                argv,
                "workflow.describe",
                {
                    "workflow": self._workflow_public(workflow),
                    "tasks": tasks,
                    "relations": relations,
                },
            )
        relations = (
            copy.deepcopy(self.owned_relations)
            if self.fail_at == "raw-relation-shape"
            else [
                {
                    "preTaskCode": relation["preTaskCode"],
                    "preTaskName": task_names.get(_fake_int(relation["preTaskCode"])),
                    "postTaskCode": relation["postTaskCode"],
                    "postTaskName": task_names.get(_fake_int(relation["postTaskCode"])),
                }
                for relation in self.owned_relations
            ]
        )
        tasks = [
            self._task_detail(task)
            for task in sorted(
                self.owned_tasks.values(),
                key=lambda item: _fake_int(item["code"]),
            )
        ]
        if self.fail_at == "duplicate-describe-task-row":
            tasks.append(copy.deepcopy(tasks[0]))
            self.fail_at = None
        return self._ok(
            argv,
            "workflow.describe",
            {
                "workflow": self._workflow_public(workflow),
                "tasks": tasks,
                "relations": relations,
            },
        )

    def _workflow_digest_result(self, argv: list[str]) -> DsctlCommandResult:
        workflow = self._selected_workflow(argv[2])
        if workflow is None:
            return self._error(argv, "workflow.digest", "not_found")
        if self.fail_at in {
            "workflow-digest",
            "workflow-digest-cleanup-delete-retained",
            "workflow-digest-cleanup-selector-mismatch",
        }:
            if self.fail_at == "workflow-digest":
                self.fail_at = None
            elif self.fail_at == "workflow-digest-cleanup-selector-mismatch":
                self.cleanup_selector_mismatch = True
                self.fail_at = None
            return self._error(argv, "workflow.digest", "api_result_error")
        code_to_task = {
            _fake_int(task["code"]): task for task in self.owned_tasks.values()
        }
        explicit_edges = [
            relation
            for relation in self.owned_relations
            if _fake_int(relation["preTaskCode"]) != 0
        ]
        digest_tasks: list[dict[str, object]] = []
        for task in sorted(
            self.owned_tasks.values(),
            key=lambda item: _fake_int(item["code"]),
        ):
            code = _fake_int(task["code"])
            upstream = [
                self._digest_task_ref(code_to_task[_fake_int(relation["preTaskCode"])])
                for relation in explicit_edges
                if _fake_int(relation["postTaskCode"]) == code
            ]
            downstream = [
                self._digest_task_ref(code_to_task[_fake_int(relation["postTaskCode"])])
                for relation in explicit_edges
                if _fake_int(relation["preTaskCode"]) == code
            ]
            digest_tasks.append(
                {
                    **self._digest_task_ref(task),
                    "taskType": task["taskType"],
                    "upstreamTasks": upstream,
                    "downstreamTasks": downstream,
                    "isRoot": not upstream,
                    "isLeaf": not downstream,
                }
            )
        roots = [task for task in digest_tasks if task["isRoot"] is True]
        leaves = [task for task in digest_tasks if task["isLeaf"] is True]
        reported_task_count = len(digest_tasks)
        if self.fail_at == "duplicate-digest-task-row":
            digest_tasks.append(copy.deepcopy(leaves[0]))
        raw_global_param_map = workflow["globalParamMap"]
        global_param_names = (
            []
            if raw_global_param_map is None
            else sorted(_mapping(raw_global_param_map))
        )
        workflow_identity_key = "id" if self.config.ds_version == "1.3.9" else "code"
        project_identity_key = (
            "projectId" if self.config.ds_version == "1.3.9" else "projectCode"
        )
        digest_workflow = {
            workflow_identity_key: workflow["code"],
            "name": workflow["name"],
            "version": workflow["version"],
            project_identity_key: workflow["projectCode"],
            "projectName": workflow["projectName"],
            "description": workflow["description"],
            "releaseState": workflow["releaseState"],
            "scheduleReleaseState": workflow["scheduleReleaseState"],
            "timeout": workflow["timeout"],
            "schedule": copy.deepcopy(workflow["schedule"]),
        }
        if self.config.ds_version != "1.3.9":
            digest_workflow["executionType"] = workflow["executionType"]
        digest_payload: dict[str, object] = {
            "workflow": digest_workflow,
            "taskCount": reported_task_count,
            "relationCount": len(explicit_edges),
            "taskTypeCounts": {"SHELL": 2},
            "globalParamNames": global_param_names,
            "rootTasks": [self._digest_task_ref(task) for task in roots],
            "leafTasks": [self._digest_task_ref(task) for task in leaves],
            "isolatedTasks": [],
            "tasks": digest_tasks,
        }
        self._apply_digest_attack(
            digest_payload,
            digest_workflow=digest_workflow,
            described_workflow=workflow,
        )
        return self._ok(
            argv,
            "workflow.digest",
            digest_payload,
        )

    def _apply_digest_attack(
        self,
        digest_payload: dict[str, object],
        *,
        digest_workflow: dict[str, object],
        described_workflow: Mapping[str, object],
    ) -> None:
        workflow_extras = {
            "digest-workflow-extra-id": ("id", 1001),
            "digest-workflow-extra-global-params": ("globalParams", "[]"),
            "digest-workflow-extra-time": ("updateTime", "2026-08-10T00:00:00Z"),
            "digest-workflow-extra-user": ("userName", "etl-user"),
        }
        workflow_replacements: dict[str, tuple[str, object]] = {
            "digest-workflow-wrong-version": (
                "version",
                _fake_int(described_workflow["version"]) + 1,
            ),
            "digest-workflow-wrong-project": (
                "projectCode",
                _fake_int(described_workflow["projectCode"]) + 1,
            ),
            "digest-workflow-wrong-release": ("releaseState", "ONLINE"),
            "digest-workflow-wrong-schedule": ("schedule", {"id": 777}),
        }
        if self.fail_at == "digest-extra-top-level":
            digest_payload["id"] = 1001
        if self.fail_at in workflow_extras:
            key, value = workflow_extras[self.fail_at]
            digest_workflow[key] = value
        if self.fail_at in workflow_replacements:
            key, value = workflow_replacements[self.fail_at]
            digest_workflow[key] = value
        if self.fail_at == "digest-global-param-names-mismatch":
            digest_payload["globalParamNames"] = ["ghost"]
        if isinstance(self.fail_at, str) and self.fail_at.startswith("digest-"):
            self.fail_at = None

    def _workflow_edit(self, argv: list[str]) -> DsctlCommandResult:
        workflow = self._selected_workflow(argv[2])
        if workflow is None:
            return self._error(argv, "workflow.edit", "not_found")
        patch = self._load_yaml(_option(argv, "--patch"))
        patch_data = _mapping(patch["patch"])
        workflow_patch = _mapping(patch_data["workflow"])
        updated_description = str(_mapping(workflow_patch["set"])["description"])
        if "--dry-run" in argv:
            assert _option(argv, "--columns") == "*"
            workflow_changes: list[dict[str, object]] = [
                {
                    "field": "description",
                    "before": workflow["description"],
                    "after": updated_description,
                }
            ]
            task_changes: list[dict[str, object]] = []
            if self.fail_at == "workflow-edit-dry-run-extra-change":
                task_changes.append(
                    {
                        "task": "load",
                        "changes": [
                            {
                                "field": "description",
                                "before": "original",
                                "after": "changed",
                            }
                        ],
                    }
                )
                self.fail_at = None
            elif self.fail_at == "workflow-edit-dry-run-wrong-before":
                workflow_changes[0]["before"] = "wrong current description"
                self.fail_at = None
            elif self.fail_at == "workflow-edit-dry-run-wrong-after":
                workflow_changes[0]["after"] = "wrong desired description"
                self.fail_at = None
            return self._ok(
                argv,
                "workflow.edit",
                {
                    "dry_run": True,
                    "requests": [
                        {
                            "method": (
                                "POST" if self.config.ds_version == "1.3.9" else "PUT"
                            ),
                            "path": "/installed-exact-plan/workflow-definition/1001",
                            "form": {"description": updated_description},
                        }
                    ],
                    "diff": {
                        "workflow_changes": workflow_changes,
                        "added_tasks": [],
                        "task_changes": task_changes,
                        "renamed_tasks": [],
                        "deleted_tasks": [],
                        "added_edges": [],
                        "removed_edges": [],
                        "dag_valid": True,
                    },
                    "no_change": False,
                    "workflow_state_constraints": [],
                    "schedule_impacts": [],
                },
                extra={
                    "warnings": [
                        {
                            "code": "dry_run_no_mutation_sent",
                            "message": (
                                "dry run: no mutation was sent; lookup and "
                                "verification reads may occur"
                            ),
                            "mutation_sent": False,
                        }
                    ]
                },
            )
        workflow["description"] = updated_description
        if self.config.ds_version != "1.3.9":
            workflow["version"] = _fake_int(workflow["version"]) + 1
        elif self.fail_at == "legacy-workflow-edit-version-increment":
            workflow["version"] = _fake_int(workflow["version"]) + 1
            self.fail_at = None
        if self.fail_at == "workflow-edit-non-owned-drift":
            workflow.update(
                {
                    "timeout": 30,
                    "executionType": "SERIAL_WAIT",
                    "globalParams": '[{"prop":"foreign"}]',
                    "globalParamMap": {"foreign": "state"},
                    "userName": "foreign-owner",
                }
            )
            self.fail_at = None
        self.mutations.append("workflow-edit")
        return self._ok(argv, "workflow.edit", self._workflow_public(workflow))

    def _workflow_delete(self, argv: list[str]) -> DsctlCommandResult:
        workflow = self._selected_workflow(argv[2])
        if workflow is None:
            return self._error(argv, "workflow.delete", "not_found")
        if self.fail_at == "workflow-digest-cleanup-delete-retained":
            self.fail_at = None
            return self._error(
                argv,
                "workflow.delete",
                "api_transport_error",
                details={"mutation_may_have_applied": True},
            )
        deleted = copy.deepcopy(workflow)
        self.owned_workflow = None
        if self.config.ds_version not in _FULL_TASK_DEFINITION_CLEANUP_VERSIONS:
            self.owned_tasks = {}
        self.owned_relations = []
        self.mutations.append("workflow-delete")
        return self._ok(
            argv,
            "workflow.delete",
            {"deleted": True, "workflow": deleted},
        )

    def _task_list(self, argv: list[str]) -> DsctlCommandResult:
        if self._selected_workflow(_option(argv, "--workflow")) is None:
            return self._error(argv, "task.list", "not_found")
        search = _option(argv, "--search") if "--search" in argv else None
        rows = [
            self._task_ref(task)
            for task in sorted(
                self.owned_tasks.values(),
                key=lambda item: _fake_int(item["code"]),
            )
            if search is None or search.lower() in str(task["name"]).lower()
        ]
        if self.fail_at == "duplicate-task-list-row" and rows:
            rows.append(copy.deepcopy(rows[0]))
            self.fail_at = None
        elif self.fail_at == "legacy-task-list-modern-fields" and rows:
            rows[0].update({"code": 2001, "version": 1})
            self.fail_at = None
        return self._ok(argv, "task.list", rows)

    def _task_get(self, argv: list[str]) -> DsctlCommandResult:
        task = self._selected_task(argv[2])
        if task is None:
            return self._error(argv, "task.get", "not_found")
        projected = self._task_detail(task)
        if self.endpoint_local_task_ids:
            projected["id"] = _fake_int(projected["id"]) + 10_000
        if (
            self.endpoint_local_task_create_times
            and projected["name"] == "load"
            and _fake_int(projected["version"]) > 1
        ):
            projected["createTime"] = "2026-08-10T00:00:00+00:00"
        if self.fail_at == "task-get-non-owned-drift":
            projected["workerGroup"] = "foreign-worker-group"
            self.fail_at = None
        return self._ok(argv, "task.get", projected)

    def _task_update(self, argv: list[str]) -> DsctlCommandResult:
        task = self._selected_task(argv[2])
        if task is None:
            return self._error(argv, "task.update", "not_found")
        assignment = _option(argv, "--set")
        if assignment.startswith("depends_on="):
            assert (
                self.config.ds_version in _DEPENDENCY_UPDATE_UPSTREAM_LIMITED_VERSIONS
            )
            return self._error(
                argv,
                "task.update",
                "unsupported_feature",
                suggestion=(
                    "Upgrade the server before changing workflow dependencies."
                    if self.config.ds_version == "2.0.3"
                    else "Use workflow edit for dependency changes."
                ),
                details={
                    "unsupported_fields": ["depends_on"],
                    "dependency_update": False,
                    "mutation_applied": False,
                },
            )
        replacement = assignment.removeprefix("command=")
        if "--dry-run" in argv:
            assert _option(argv, "--columns") == "*"
            compiled = copy.deepcopy(task)
            compiled_params = _mapping(compiled["taskParams"])
            previous = str(compiled_params["rawScript"])
            compiled_params["rawScript"] = replacement
            changes: list[dict[str, object]] = [
                {
                    "field": "command",
                    "before": previous,
                    "after": replacement,
                }
            ]
            if self.fail_at == "task-update-dry-run-extra-change":
                changes.append(
                    {
                        "field": "description",
                        "before": "original",
                        "after": "changed",
                    }
                )
                self.fail_at = None
            elif self.fail_at == "task-update-dry-run-wrong-before":
                changes[0]["before"] = "wrong current command"
                self.fail_at = None
            elif self.fail_at == "task-update-dry-run-wrong-after":
                changes[0]["after"] = "wrong replacement command"
                self.fail_at = None
            return self._ok(
                argv,
                "task.update",
                {
                    "dry_run": True,
                    "requests": [
                        {
                            "method": (
                                "POST" if self.config.ds_version == "1.3.9" else "PUT"
                            ),
                            "path": f"/installed-exact-plan/task/{task['code']}",
                            "form": {
                                "taskDefinitionJsonObj": json.dumps(compiled),
                            },
                        }
                    ],
                    "changes": changes,
                    "no_change": False,
                },
                extra={
                    "warnings": [
                        {
                            "code": "dry_run_no_mutation_sent",
                            "message": (
                                "dry run: no mutation was sent; lookup and "
                                "verification reads may occur"
                            ),
                            "mutation_sent": False,
                        }
                    ]
                },
            )
        task_params = _mapping(task["taskParams"])
        task_params["rawScript"] = replacement
        task["version"] = _fake_int(task["version"]) + 1
        self._refresh_relation_versions()
        self._apply_task_update_workflow_effects()
        cleanup_profile = cleanup_profiles.TASK_DEFINITION_CLEANUP_PROFILES.get(
            self.config.ds_version
        )
        if (
            cleanup_profile is not None
            and cleanup_profile.get("strategy") == "workflow-cascade-proof-only"
        ):
            workflow = self.owned_workflow
            assert workflow is not None
            workflow["version"] = _fake_int(workflow["version"]) + 1
        self.mutations.append("task-update")
        return self._ok(argv, "task.update", self._task_detail(task))

    def _install_workflow(self, document: Mapping[str, object]) -> None:
        assert self.owned_project is not None
        workflow = _mapping(document["workflow"])
        tasks, relations = self._compiled_graph(document, task_codes=(2001, 2002))
        workflow_version = 0 if self.config.ds_version == "1.3.9" else 1
        if self.fail_at == "zero-workflow-version":
            workflow_version = 0
            self.fail_at = None
        self.owned_workflow = {
            "id": 1001,
            "code": 1001,
            "name": workflow["name"],
            "version": workflow_version,
            "description": workflow["description"],
            "projectCode": self._owned_project_native(),
            "projectName": self.owned_project["name"],
            "globalParams": None if self.config.ds_version == "1.3.9" else "[]",
            "globalParamMap": None if self.config.ds_version == "1.3.9" else {},
            "createTime": "2026-08-10T00:00:00+00:00",
            "updateTime": "2026-08-10T00:00:00+00:00",
            "userId": 9,
            "userName": "etl-user",
            "timeout": 0,
            "releaseState": workflow["release_state"],
            "scheduleReleaseState": None,
            "executionType": "PARALLEL",
            "schedule": None,
        }
        self.owned_tasks = {str(task["name"]): task for task in tasks}
        self.owned_relations = relations

    def _apply_task_update_workflow_effects(self) -> None:
        workflow = self.owned_workflow
        assert workflow is not None
        if self.config.ds_version == "1.3.9":
            workflow["globalParams"] = "[]"
            workflow["globalParamMap"] = {}
        if self.fail_at == "task-update-global-params-drift":
            workflow["globalParams"] = (
                '[{"prop":"foreign","direct":"IN","type":"VARCHAR","value":"state"}]'
            )
            workflow["globalParamMap"] = {"foreign": "state"}
            self.fail_at = None

    def _compiled_graph(
        self,
        document: Mapping[str, object],
        *,
        task_codes: tuple[int, int],
    ) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
        assert self.owned_project is not None
        raw_tasks = document["tasks"]
        assert isinstance(raw_tasks, list)
        assert len(raw_tasks) == 2
        tasks: list[dict[str, object]] = []
        for raw_task, code in zip(raw_tasks, task_codes, strict=True):
            task_spec = _mapping(raw_task)
            tasks.append(
                {
                    "id": code + 10_000,
                    **(
                        {"legacyId": f"tasks-{code}"}
                        if self.config.ds_version == "1.3.9"
                        else {}
                    ),
                    "code": code,
                    "name": task_spec["name"],
                    "version": 1,
                    "description": task_spec["description"],
                    "projectCode": self._owned_project_native(),
                    "taskType": task_spec["type"],
                    "taskParams": copy.deepcopy(task_spec["task_params"]),
                    "flag": "YES",
                    "taskPriority": task_spec["priority"],
                    "workerGroup": task_spec["worker_group"],
                    "environmentCode": -1,
                    "failRetryTimes": 0,
                    "failRetryInterval": 0,
                    "timeoutFlag": "CLOSE",
                    "timeout": 0,
                    "delayTime": 0,
                    "createTime": "2026-08-10T00:00:01+00:00",
                }
            )
        by_name = {str(task["name"]): task for task in tasks}
        relations: list[dict[str, object]] = []
        for raw_task in raw_tasks:
            task_spec = _mapping(raw_task)
            post = by_name[str(task_spec["name"])]
            dependencies = task_spec.get("depends_on", [])
            assert isinstance(dependencies, list)
            if not dependencies:
                relations.append(self._relation(None, post))
            relations.extend(
                self._relation(by_name[str(dependency)], post)
                for dependency in dependencies
            )
        return tasks, relations

    def _export_document(self) -> dict[str, object]:
        assert self.owned_workflow is not None
        code_to_name = {
            _fake_int(task["code"]): str(task["name"])
            for task in self.owned_tasks.values()
        }
        tasks = []
        for task in sorted(
            self.owned_tasks.values(),
            key=lambda item: _fake_int(item["code"]),
        ):
            upstream_names = [
                code_to_name[_fake_int(relation["preTaskCode"])]
                for relation in self.owned_relations
                if _fake_int(relation["postTaskCode"]) == _fake_int(task["code"])
                and _fake_int(relation["preTaskCode"]) != 0
            ]
            task_document = {
                "name": task["name"],
                "description": task["description"],
                "type": task["taskType"],
                "task_params": copy.deepcopy(task["taskParams"]),
                "worker_group": task["workerGroup"],
                "priority": task["taskPriority"],
                "retry": {"times": 0, "interval": 0},
                "timeout": 0,
                "delay": 0,
            }
            if self.config.ds_version == "1.3.9":
                task_document.pop("delay")
                task_document.pop("timeout")
                task_document["flag"] = task["flag"]
            if upstream_names:
                task_document["depends_on"] = upstream_names
            elif self.config.ds_version == "1.3.9":
                task_document["depends_on"] = []
            tasks.append(task_document)
        if self.fail_at == "lossy-workflow-export":
            tasks = [
                {
                    "name": task["name"],
                    "type": task["type"],
                    "task_params": {
                        "rawScript": _mapping(task["task_params"])["rawScript"]
                    },
                    **(
                        {"depends_on": task["depends_on"]}
                        if "depends_on" in task
                        else {}
                    ),
                }
                for task in tasks
            ]
        document: dict[str, object] = {
            "workflow": {
                "name": self.owned_workflow["name"],
                "project": self.owned_workflow["projectName"],
                "description": self.owned_workflow["description"],
                "timeout": self.owned_workflow["timeout"],
                "execution_type": self.owned_workflow["executionType"],
                "release_state": self.owned_workflow["releaseState"],
            },
            "tasks": tasks,
        }
        if self.config.ds_version == "1.3.9":
            legacy_workflow = _mapping(document["workflow"])
            legacy_workflow.pop("execution_type")
            legacy_workflow["global_params"] = []
        global_params = self.owned_workflow["globalParamMap"]
        if isinstance(global_params, dict) and global_params:
            _mapping(document["workflow"])["global_params"] = copy.deepcopy(
                global_params
            )
        self._apply_legacy_export_default_faults(document, tasks=tasks)
        if self.fail_at == "duplicate-export-task":
            tasks.append(copy.deepcopy(tasks[0]))
        elif self.fail_at == "extra-export-metadata":
            _mapping(document["workflow"])["foreign"] = "metadata"
            document["schedule"] = {"cron": "0 0 * * *"}
        return document

    def _apply_legacy_export_default_faults(
        self,
        document: dict[str, object],
        *,
        tasks: list[dict[str, object]],
    ) -> None:
        if self.fail_at == "legacy-export-missing-global-params":
            _mapping(document["workflow"]).pop("global_params", None)
        elif self.fail_at == "legacy-export-missing-flag":
            for task in tasks:
                task.pop("flag", None)

    def _refresh_relation_versions(self) -> None:
        versions = {
            _fake_int(task["code"]): _fake_int(task["version"])
            for task in self.owned_tasks.values()
        }
        for relation in self.owned_relations:
            pre_code = _fake_int(relation["preTaskCode"])
            post_code = _fake_int(relation["postTaskCode"])
            relation["preTaskVersion"] = 0 if pre_code == 0 else versions[pre_code]
            relation["postTaskVersion"] = versions[post_code]

    def _selected_workflow(self, selector: str) -> dict[str, object] | None:
        if self.owned_workflow is None:
            return None
        if selector in {
            str(self.owned_workflow["code"]),
            str(self.owned_workflow["name"]),
        }:
            return self.owned_workflow
        return None

    def _workflow_public(
        self,
        workflow: Mapping[str, object],
    ) -> dict[str, object]:
        projected = copy.deepcopy(dict(workflow))
        if self.config.ds_version != "1.3.9":
            projected.pop("legacyId", None)
            return projected
        projected["id"] = projected.pop("code")
        projected["projectId"] = projected.pop("projectCode")
        projected.pop("executionType", None)
        return projected

    def _owned_project_native(self) -> int:
        assert self.owned_project is not None
        return _fake_int(self.owned_project[self.identity_key])

    def _selected_task(self, selector: str) -> dict[str, object] | None:
        if self.config.ds_version == "1.3.9":
            return next(
                (
                    task
                    for task in self.owned_tasks.values()
                    if selector == str(task["name"])
                ),
                None,
            )
        return next(
            (
                task
                for task in self.owned_tasks.values()
                if selector in {str(task["code"]), str(task["name"])}
            ),
            None,
        )

    def _task_detail(self, task: Mapping[str, object]) -> dict[str, object]:
        projected = copy.deepcopy(dict(task))
        projected.pop("legacyId", None)
        if self.config.ds_version != "1.3.9":
            return projected
        projected["id"] = task["legacyId"]
        projected.pop("code", None)
        projected.pop("version", None)
        projected.pop("projectCode", None)
        projected.pop("createTime", None)
        return projected

    def _uses_owned_project(self, argv: list[str]) -> bool:
        return (
            self.owned_project is not None
            and "--project" in argv
            and _option(argv, "--project") == self.owned_project["name"]
        )

    @staticmethod
    def _relation(
        pre_task: Mapping[str, object] | None,
        post_task: Mapping[str, object],
    ) -> dict[str, object]:
        return {
            "name": "",
            "preTaskCode": 0 if pre_task is None else pre_task["code"],
            "preTaskVersion": 0 if pre_task is None else pre_task["version"],
            "postTaskCode": post_task["code"],
            "postTaskVersion": post_task["version"],
            "conditionType": 0,
            "conditionParams": {},
        }

    def _task_ref(self, task: Mapping[str, object]) -> dict[str, object]:
        if self.config.ds_version == "1.3.9":
            return {
                "id": task.get("legacyId", task.get("id")),
                "name": task["name"],
            }
        return {
            "code": task["code"],
            "name": task["name"],
            "version": task["version"],
        }

    def _digest_task_ref(self, task: Mapping[str, object]) -> dict[str, object]:
        if self.config.ds_version == "1.3.9":
            return {
                "id": task.get("legacyId", task.get("id")),
                "name": task["name"],
            }
        return {
            "code": task["code"],
            "name": task["name"],
        }

    @staticmethod
    def _load_yaml(path: str) -> dict[str, object]:
        return _mapping(yaml.safe_load(Path(path).read_text(encoding="utf-8")))


def _fake_int(value: object) -> int:
    assert isinstance(value, int)
    assert not isinstance(value, bool)
    return value


def _config(
    tmp_path: Path,
    *,
    ds_version: str = "3.2.2",
    bundle_name: str = "legacy_core/v1",
) -> ConformanceBundleGateConfig:
    assessed = next(
        item
        for item in CONFORMANCE_BUNDLE_DATA["bundles"]
        if item["name"] == bundle_name
    )
    bundles_by_name = {
        item["name"]: item for item in CONFORMANCE_BUNDLE_DATA["bundles"]
    }
    coordinate = next(
        item for item in assessed["versions"] if item["version"] == ds_version
    )
    profile = VERSION_PROFILES[ds_version]
    recipes: list[dict[str, object]] = []
    for action in assessed["required_actions"]:
        decisions = [
            decision
            for decision in profile["build_decisions"].values()
            if decision["stable_action"] == action
        ]
        assert len(decisions) == 1
        decision = decisions[0]
        action_fact = profile["actions"][action]
        recipes.append(
            {
                "action": action,
                "semantic_operation": decision["semantic_operation"],
                "availability": action_fact["availability"],
                "execution_mode": action_fact["execution_mode"],
                "verification": action_fact["verification"],
                "build_status": decision["build_status"],
                "fingerprints": decision["fingerprints"],
            }
        )
    manifest_module = importlib.import_module(
        "dsctl.generated.versions.ds_" + ds_version.replace(".", "_") + "._manifest"
    )
    contract = {
        "bundle_manifest_schema_version": (
            manifest_module.BUNDLE_MANIFEST_SCHEMA_VERSION
        ),
        "ds_version": manifest_module.DS_VERSION,
        "selection": manifest_module.SELECTION,
        "semantic_operations": list(manifest_module.SEMANTIC_OPERATIONS),
        "source_tag": manifest_module.SOURCE_TAG,
        "source_commit": manifest_module.SOURCE_COMMIT,
        "source_tree": manifest_module.SOURCE_TREE,
        "source_contract_digest": manifest_module.SOURCE_CONTRACT_DIGEST,
        "rendered_contract_digest": manifest_module.RENDERED_CONTRACT_DIGEST,
        "operation_count": manifest_module.OPERATION_COUNT,
    }
    attestation_key = b"k" * 32
    image_ref = _api_image_ref(ds_version)
    image_repo = image_ref.rpartition(":")[0]
    return ConformanceBundleGateConfig(
        ds_version=ds_version,
        bundle_name=bundle_name,
        env_file=tmp_path / "profile.env",
        executable=tmp_path / "venv" / "bin" / "dsctl",
        python=tmp_path / "venv" / "bin" / "python",
        wheel=tmp_path / "candidate.whl",
        evidence_path=tmp_path / "candidate.json",
        attestation_key=attestation_key,
        cluster=ClusterIdentity(
            ds_version=ds_version,
            image_ref=image_ref,
            image_id="sha256:" + "a" * 64,
            image_source="node-local-inspection",
            image_provenance={
                "kind": "registry-digest/v1",
                "repo_digests": [image_repo + "@sha256:" + "e" * 64],
                "selected_repo_digest": image_repo + "@sha256:" + "e" * 64,
            },
            image_observed_at=datetime.now(tz=UTC).isoformat(),
            api_target_hmac_sha256="hmac-sha256:" + "b" * 64,
            principal_hmac_sha256=identity_hmac("etl-user", key=attestation_key),
            persona="etl-developer",
        ),
        fixture=ExternalFixture(
            project_name="external-project",
            project_identity=NativeFixtureIdentity(
                kind="id" if ds_version == "1.3.9" else "code",
                value=41,
            ),
            workflow_name="external-workflow",
            workflow_identity=NativeFixtureIdentity(
                kind="id" if ds_version == "1.3.9" else "code",
                value=77,
            ),
            schedule_id=88,
            workflow_release_state="ONLINE",
            provisioner="dsmatrix-conformance-fixture/v1",
            manifest_sha256="c" * 64,
            identity_hmac_sha256="hmac-sha256:" + "d" * 64,
        ),
        bundle=BundleAssessment(
            schema_version=CONFORMANCE_BUNDLE_DATA["schema_version"],
            catalog_digest=CONFORMANCE_BUNDLE_DATA["catalog_digest"],
            assessment_digest=CONFORMANCE_BUNDLE_DATA["assessment_digest"],
            name=bundle_name,
            bundle_digest=assessed["bundle_digest"],
            coordinate_status=coordinate["status"],
            support_level=coordinate["support_level"],
            tested=coordinate["tested"],
            extends=tuple(assessed["extends"]),
            inheritance=tuple(
                {
                    "name": parent_name,
                    "bundle_digest": bundles_by_name[parent_name]["bundle_digest"],
                    "required_actions": list(
                        bundles_by_name[parent_name]["required_actions"]
                    ),
                }
                for parent_name in assessed["extends"]
            ),
            direct_actions=tuple(assessed["direct_actions"]),
            required_actions=tuple(assessed["required_actions"]),
        ),
        installation=InstalledBundleAttestation(
            distribution_version=__version__,
            profile={
                key: profile[key]
                for key in (
                    "server_version",
                    "contract_version",
                    "family",
                    "support_level",
                    "tested",
                    "source",
                    "fingerprints",
                )
            },
            action_recipes=tuple(recipes),
            contract=contract,
        ),
    )


def _write_runtime_inputs(
    tmp_path: Path,
    config: ConformanceBundleGateConfig,
) -> dict[str, str]:
    api_url = "http://127.0.0.1:12345/dolphinscheduler"
    config.env_file.write_text(
        f"DS_API_URL={api_url}\nDS_API_TOKEN=synthetic-token\n"
        f"DS_VERSION={config.ds_version}\n",
        encoding="utf-8",
    )
    config.executable.parent.mkdir(parents=True)
    config.executable.write_text("synthetic dsctl", encoding="utf-8")
    config.python.write_text("synthetic python", encoding="utf-8")
    generated = Path(__file__).resolve().parents[2] / "src/dsctl/generated"
    with zipfile.ZipFile(
        config.wheel, "w", compression=zipfile.ZIP_DEFLATED
    ) as archive:
        for artifact in (
            *(generated / "versions").glob("ds_*/_manifest.py"),
            *(generated / "wire_programs").rglob("*.py"),
            *(generated / "wire_runtime").rglob("*.py"),
        ):
            archive.write(
                artifact,
                "dsctl/generated/" + artifact.relative_to(generated).as_posix(),
            )
    key_file = tmp_path / "attestation.key"
    key_file.write_bytes(config.attestation_key)
    cluster_manifest = tmp_path / "cluster.json"
    cluster_manifest.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "ds_version": config.ds_version,
                "image_ref": config.cluster.image_ref,
                "image_id": config.cluster.image_id,
                "image_source": config.cluster.image_source,
                "image_provenance": config.cluster.image_provenance,
                "image_observed_at": config.cluster.image_observed_at,
                "api_target_hmac_sha256": identity_hmac(
                    api_url,
                    key=config.attestation_key,
                ),
                "principal_hmac_sha256": config.cluster.principal_hmac_sha256,
                "persona": config.cluster.persona,
            }
        ),
        encoding="utf-8",
    )
    fixture_manifest = tmp_path / "fixture.json"
    fixture_identity = {
        "project": {
            "name": config.fixture.project_name,
            "identity": {
                "kind": config.fixture.project_identity.kind,
                "value": config.fixture.project_identity.value,
            },
        },
        "workflow": {
            "name": config.fixture.workflow_name,
            "identity": {
                "kind": config.fixture.workflow_identity.kind,
                "value": config.fixture.workflow_identity.value,
            },
            "scheduled": True,
            "schedule_id": config.fixture.schedule_id,
            "release_state": config.fixture.workflow_release_state,
        },
    }
    fixture_manifest.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "ds_version": config.ds_version,
                "image_ref": config.cluster.image_ref,
                "image_id": config.cluster.image_id,
                **fixture_identity,
                "provisioner": config.fixture.provisioner,
            }
        ),
        encoding="utf-8",
    )
    installed_attestation = tmp_path / "installed-attestation.json"
    installed_attestation.write_text(
        json.dumps(
            {
                "distribution_version": config.installation.distribution_version,
                "ds_version": config.ds_version,
                "bundle": config.bundle_name,
                "bundle_digest": config.bundle.bundle_digest,
                "catalog_digest": config.bundle.catalog_digest,
                "assessment_digest": config.bundle.assessment_digest,
                "required_actions": list(config.bundle.required_actions),
                "profile": config.installation.profile,
                "actions": list(config.installation.action_recipes),
                "manifest": config.installation.contract,
            }
        ),
        encoding="utf-8",
    )
    private_files = (
        config.env_file,
        config.executable,
        config.python,
        config.wheel,
        key_file,
        cluster_manifest,
        fixture_manifest,
        installed_attestation,
    )
    for path in private_files:
        path.chmod(0o600)
    return {
        "DS_LIVE_CONFORMANCE_VERSION": config.ds_version,
        "DS_LIVE_CONFORMANCE_BUNDLE": config.bundle_name,
        "DS_LIVE_CONFORMANCE_ENV_FILE": str(config.env_file),
        "DS_LIVE_CONFORMANCE_ATTESTATION_KEY_FILE": str(key_file),
        "DS_LIVE_CONFORMANCE_DSCTL": str(config.executable),
        "DS_LIVE_CONFORMANCE_PYTHON": str(config.python),
        "DS_LIVE_CONFORMANCE_WHEEL": str(config.wheel),
        "DS_LIVE_CONFORMANCE_CLUSTER_MANIFEST": str(cluster_manifest),
        "DS_LIVE_CONFORMANCE_FIXTURE_MANIFEST": str(fixture_manifest),
        "DS_LIVE_CONFORMANCE_INSTALLED_ATTESTATION": str(installed_attestation),
        "DS_LIVE_CONFORMANCE_EVIDENCE": str(config.evidence_path),
    }


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
    }:
        repository = "apache/dolphinscheduler"
    else:
        repository = "apache/dolphinscheduler-api"
    return f"{repository}:{ds_version}"


def _option(argv: list[str], name: str) -> str:
    return argv[argv.index(name) + 1]


def _mapping(value: object) -> dict[str, object]:
    assert isinstance(value, dict)
    return value


def _updated_marker(value: str) -> str:
    assert value.endswith(";phase=created")
    return value.removesuffix("created") + "updated"


def _page(rows: list[dict[str, object]]) -> dict[str, Any]:
    return {
        "totalList": [dict(row) for row in rows],
        "total": len(rows),
        "totalPage": 1 if rows else 0,
        "pageSize": 20,
        "currentPage": 1,
        "pageNo": 1,
    }
