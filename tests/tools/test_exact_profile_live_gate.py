from __future__ import annotations

import json
import subprocess
import sys
import zipfile
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path

import pytest
from tests.live.exact_profile_gate import (
    INSTALLED_EXACT_PROFILE_PROBE_CODE,
    CleanupAttestation,
    ExactProfileGateConfig,
    ExternalTaskCleanupAttestation,
    InstalledExactProfileAttestation,
    OperationTraceEntry,
    canonical_gate_bundle_digest,
    identity_hmac,
    inspect_installed_exact_profile,
    load_exact_profile_gate_config,
    load_exact_profile_manifest,
    validate_exact_profile_evidence_payload,
    write_exact_profile_evidence,
)
from tests.live.support import DsctlCommandResult
from tests.live.test_exact_profile import (
    _cleanup_project_after_failure,
    _doctor_checks_ready,
    _is_confirmed_create_rejection,
    _verify_task_definition_round_trip,
    _wait_for_doctor,
)

from dsctl.generated.version_profiles import VERSION_PROFILES
from dsctl.generated.versions.ds_3_4_2 import _manifest
from dsctl.upstream import Availability, Verification, get_version_support
from exact_342_evidence import EXACT_342_GATE_RECIPES
from exact_342_evidence import validate_exact_342_evidence_payload as validate_schema

_BASE_LIVE_SMOKE_ACTIONS = (
    "doctor",
    "project.create",
    "project.delete",
    "project.get",
    "project.list",
    "project.update",
    "schedule.list",
    "workflow.describe",
    "workflow.digest",
    "workflow.export",
    "workflow.get",
    "workflow.list",
)
_TASK_DEFINITION_GATE_ACTIONS = ("task.list", "task.get", "task.update")
_LIVE_SMOKE_ACTIONS = (*_BASE_LIVE_SMOKE_ACTIONS, *_TASK_DEFINITION_GATE_ACTIONS)

_IMAGE_TAG = "apache/dolphinscheduler-api:3.4.2"
_IMAGE_DIGEST = "apache/dolphinscheduler-api@sha256:" + "a" * 64
_SOURCE_DIGEST = _manifest.SOURCE_CONTRACT_DIGEST
_RENDERED_DIGEST = "sha256:" + "c" * 64
_ATTESTATION_KEY = b"k" * 32
_TEST_SEMANTIC_OPERATIONS: tuple[str, ...] = tuple(_manifest.SEMANTIC_OPERATIONS)
_PROFILE_342 = VERSION_PROFILES["3.4.2"]
_PROFILE_SOURCE_342 = _PROFILE_342["source"]
_HISTORICAL_SCHEMA_3_RECEIPT = (
    Path(__file__).resolve().parents[2]
    / "docs"
    / "development"
    / "live-evidence"
    / "external-shell"
    / "3.4.2"
    / "2026-08-04-48c6477c09bb.json"
)


def test_exact_gate_actions_bind_fixed_generated_recipes() -> None:
    assert EXACT_342_GATE_RECIPES == (
        ("doctor", "identity.current"),
        ("project.create", "project.create"),
        ("project.delete", "project.delete"),
        ("project.get", "project.get"),
        ("project.list", "project.page"),
        ("project.update", "project.update"),
        ("schedule.list", "schedule.page"),
        ("workflow.describe", "workflow.describe"),
        ("workflow.digest", "workflow.digest"),
        ("workflow.export", "workflow.export"),
        ("workflow.get", "workflow.get"),
        ("workflow.list", "workflow.page"),
        ("task.list", "task.list"),
        ("task.get", "task.get"),
        ("task.update", "task.update"),
    )
    for action, semantic_operation in EXACT_342_GATE_RECIPES:
        decision = _PROFILE_342["build_decisions"][semantic_operation]
        assert decision["stable_action"] == action
        assert decision["build_status"] == "accepted"


def test_exact_gate_actions_require_current_mutating_live_smoke_claims() -> None:
    support = get_version_support("3.4.2")
    live_smoke_actions = {
        action
        for action, capability in support.catalog.entries.items()
        if capability.availability is Availability.SUPPORTED
        and capability.verification is Verification.LIVE_SMOKE
    }
    supported_actions = {
        action
        for action, capability in support.catalog.entries.items()
        if capability.availability is Availability.SUPPORTED
    }

    assert set(_LIVE_SMOKE_ACTIONS) <= supported_actions
    assert live_smoke_actions == set(_LIVE_SMOKE_ACTIONS)


def test_exact_gate_waits_for_read_only_doctor_readiness() -> None:
    results = iter(
        (
            _doctor_result(api_status="error", current_user_status="error"),
            _doctor_result(api_status="ok", current_user_status="ok"),
        )
    )
    calls = 0

    def invoke(argv: list[str]) -> DsctlCommandResult:
        nonlocal calls
        calls += 1
        assert argv == ["doctor"]
        return next(results)

    result, attempts = _wait_for_doctor(
        invoke,
        max_attempts=3,
        interval_seconds=0,
    )

    assert _doctor_checks_ready(result)
    assert attempts == 2
    assert calls == 2


def test_exact_gate_bounds_doctor_readiness_attempts() -> None:
    result = _doctor_result(api_status="error", current_user_status="error")
    calls = 0

    def invoke(argv: list[str]) -> DsctlCommandResult:
        nonlocal calls
        calls += 1
        assert argv == ["doctor"]
        return result

    observed, attempts = _wait_for_doctor(
        invoke,
        max_attempts=3,
        interval_seconds=0,
    )

    assert observed is result
    assert attempts == 3
    assert calls == 3


def test_load_exact_profile_gate_config_cross_checks_manifests(tmp_path: Path) -> None:
    environment, paths = _gate_environment(tmp_path)

    config = load_exact_profile_gate_config(environment)

    assert config.env_file == paths["env_file"]
    assert config.executable == paths["executable"]
    assert config.python == paths["python"]
    assert config.wheel == paths["wheel"]
    assert config.cluster.image_tag == _IMAGE_TAG
    assert config.cluster.image_digest == _IMAGE_DIGEST
    assert config.cluster.persona == "etl-developer"
    assert config.cluster.api_target_hmac_sha256 == identity_hmac(
        "https://secret-host.example/dolphinscheduler",
        key=_ATTESTATION_KEY,
    )
    assert config.fixture.project_code == 7
    assert config.fixture.workflow_code == 101
    assert config.fixture.schedule_id == 23
    assert config.fixture.workflow_release_state == "OFFLINE"
    assert config.fixture.task_name == "fixture-shell"
    assert config.fixture.task_code == 202
    assert config.fixture.task_type == "SHELL"


def test_load_exact_profile_gate_config_requires_inspection_bound_projection_v2(
    tmp_path: Path,
) -> None:
    environment, paths = _gate_environment(tmp_path)
    fixture = json.loads(paths["fixture_manifest"].read_text(encoding="utf-8"))
    fixture["provisioner"] = "dsmatrix-exact-read-state-projection/v1"
    paths["fixture_manifest"].write_text(json.dumps(fixture), encoding="utf-8")

    with pytest.raises(ValueError, match="state-projection/v2"):
        load_exact_profile_gate_config(environment)


def test_load_exact_profile_gate_config_reports_all_missing_inputs() -> None:
    with pytest.raises(ValueError) as captured:
        load_exact_profile_gate_config({})

    message = str(captured.value)
    assert "DS_LIVE_EXACT_ENV_FILE" in message
    assert "DS_LIVE_EXACT_CLUSTER_MANIFEST" in message
    assert "DS_LIVE_EXACT_FIXTURE_MANIFEST" in message
    assert "DS_LIVE_EXACT_ATTESTATION_KEY_FILE" in message


def test_load_exact_profile_gate_config_requires_immutable_matching_image(
    tmp_path: Path,
) -> None:
    environment, paths = _gate_environment(tmp_path)
    paths["cluster_manifest"].write_text(
        json.dumps(
            {
                "schema_version": 2,
                "ds_version": "3.4.2",
                "image_tag": "apache/dolphinscheduler-api:3.4.1",
                "image_digest": _IMAGE_DIGEST,
                "image_source": "swarm service plus local image inspect",
                "persona": "admin-bootstrap",
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"3\.4\.2"):
        load_exact_profile_gate_config(environment)


def test_load_exact_profile_gate_config_requires_etl_developer_persona(
    tmp_path: Path,
) -> None:
    environment, paths = _gate_environment(tmp_path)
    cluster = json.loads(paths["cluster_manifest"].read_text(encoding="utf-8"))
    cluster["persona"] = "admin-bootstrap"
    paths["cluster_manifest"].write_text(json.dumps(cluster), encoding="utf-8")

    with pytest.raises(ValueError, match="etl-developer"):
        load_exact_profile_gate_config(environment)


def test_load_exact_profile_gate_config_rejects_stale_image_observation(
    tmp_path: Path,
) -> None:
    environment, paths = _gate_environment(tmp_path)
    cluster = json.loads(paths["cluster_manifest"].read_text(encoding="utf-8"))
    cluster["image_observed_at"] = datetime(2020, 1, 1, tzinfo=UTC).isoformat()
    paths["cluster_manifest"].write_text(json.dumps(cluster), encoding="utf-8")

    with pytest.raises(ValueError, match="too stale"):
        load_exact_profile_gate_config(environment)


@pytest.mark.parametrize(
    ("field_path", "value", "message"),
    [
        (("workflow", "release_state"), "ONLINE", "OFFLINE"),
        (("workflow", "editable_task", "type"), "SQL", "SHELL"),
    ],
)
def test_load_exact_profile_gate_config_requires_editable_offline_shell_task(
    tmp_path: Path,
    field_path: tuple[str, ...],
    value: str,
    message: str,
) -> None:
    environment, paths = _gate_environment(tmp_path)
    fixture = json.loads(paths["fixture_manifest"].read_text(encoding="utf-8"))
    target = fixture
    for field in field_path[:-1]:
        target = target[field]
    target[field_path[-1]] = value
    paths["fixture_manifest"].write_text(json.dumps(fixture), encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        load_exact_profile_gate_config(environment)


def test_load_exact_profile_manifest_reads_contract_from_wheel(tmp_path: Path) -> None:
    wheel = tmp_path / "runtime.whl"
    _write_manifest_wheel(wheel)

    manifest = load_exact_profile_manifest(wheel, ds_version="3.4.2")

    assert manifest == {
        "bundle_manifest_schema_version": 2,
        "ds_version": "3.4.2",
        "selection": "runtime-slice",
        "semantic_operations": list(_TEST_SEMANTIC_OPERATIONS),
        "source_tag": _PROFILE_SOURCE_342["tag"],
        "source_commit": _PROFILE_SOURCE_342["commit"],
        "source_tree": _PROFILE_SOURCE_342["tree"],
        "source_contract_digest": _SOURCE_DIGEST,
        "rendered_contract_digest": _RENDERED_DIGEST,
        "operation_count": len(_TEST_SEMANTIC_OPERATIONS),
    }


def test_load_exact_profile_manifest_rejects_historical_schema_in_current_wheel(
    tmp_path: Path,
) -> None:
    wheel = tmp_path / "runtime.whl"
    _write_manifest_wheel(wheel, bundle_manifest_schema_version=1)

    with pytest.raises(ValueError, match="wheel manifest schema is unsupported"):
        load_exact_profile_manifest(wheel, ds_version="3.4.2")


def test_inspect_installed_exact_profile_attests_semantic_profile(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    environment, _ = _gate_environment(tmp_path)
    config = load_exact_profile_gate_config(environment)
    installed_module = (
        config.python.parent.parent
        / "lib"
        / "python3.12"
        / "site-packages"
        / "dsctl"
        / "__init__.py"
    )

    def fake_run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        del args, kwargs
        return subprocess.CompletedProcess(
            [str(config.python)],
            0,
            stdout=json.dumps(_installed_probe_payload(installed_module)),
            stderr="",
        )

    monkeypatch.setattr("tests.live.exact_profile_gate.subprocess.run", fake_run)

    attestation = inspect_installed_exact_profile(config)

    assert attestation.server_version == "3.4.2"
    assert attestation.contract_version == "3.4.2"
    assert attestation.profile_fingerprints == _PROFILE_342["fingerprints"]
    assert attestation.action_verifications == {
        action: "live_smoke" for action, _operation in EXACT_342_GATE_RECIPES
    }
    assert (
        tuple(
            (recipe["action"], recipe["semantic_operation"])
            for recipe in attestation.gate_recipes
        )
        == EXACT_342_GATE_RECIPES
    )
    assert attestation.manifest == _test_manifest_data()

    # A valid checkout must not compensate for missing ownership in the wheel.
    _write_manifest_wheel(config.wheel, include_compiled=False)
    with pytest.raises(FileNotFoundError, match="wire_programs"):
        inspect_installed_exact_profile(config)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("installed-manifest", "(?i)installed manifest"),
        ("profile-source", "profile source"),
        ("non-live-smoke", "not all live_smoke"),
    ],
)
def test_inspect_installed_exact_profile_rejects_unbound_profile_facts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
    message: str,
) -> None:
    environment, _ = _gate_environment(tmp_path)
    config = load_exact_profile_gate_config(environment)
    installed_module = (
        config.python.parent.parent
        / "lib"
        / "python3.12"
        / "site-packages"
        / "dsctl"
        / "__init__.py"
    )
    payload = _installed_probe_payload(installed_module)
    if mutation == "installed-manifest":
        target = payload["manifest"]
        assert isinstance(target, dict)
        target["rendered_contract_digest"] = "sha256:" + "d" * 64
    elif mutation == "profile-source":
        target = payload["profile_source"]
        assert isinstance(target, dict)
        target["tree"] = "0" * 40
    else:
        profile_target = payload["action_verifications"]
        assert isinstance(profile_target, dict)
        profile_target["task.update"] = "contract_tested"

    monkeypatch.setattr(
        "tests.live.exact_profile_gate.subprocess.run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [str(config.python)], 0, stdout=json.dumps(payload), stderr=""
        ),
    )

    with pytest.raises(AssertionError, match=message):
        inspect_installed_exact_profile(config)


def test_installed_342_probe_uses_generated_profile_and_manifest() -> None:
    import_blocker = """
import sys
class _BlockUpstream:
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "dsctl.upstream" or fullname.startswith("dsctl.upstream."):
            raise AssertionError(f"forbidden upstream import: {fullname}")
        return None
sys.meta_path.insert(0, _BlockUpstream())
"""
    completed = subprocess.run(  # noqa: S603 - fixed interpreter and probe source
        [
            sys.executable,
            "-I",
            "-c",
            import_blocker + INSTALLED_EXACT_PROFILE_PROBE_CODE,
            json.dumps(EXACT_342_GATE_RECIPES),
            "3.4.2",
        ],
        capture_output=True,
        check=False,
        text=True,
        timeout=30.0,
    )

    assert completed.returncode == 0, completed.stderr
    payload = json.loads(completed.stdout)
    assert payload["server_version"] == "3.4.2"
    assert not any("adapter" in key for key in payload)
    assert "catalog_action_verifications" not in payload
    assert payload["profile_fingerprints"] == _PROFILE_342["fingerprints"]
    assert tuple(payload["action_verifications"]) == tuple(
        action for action, _operation in EXACT_342_GATE_RECIPES
    )
    assert (
        tuple(
            (recipe["action"], recipe["semantic_operation"])
            for recipe in payload["gate_recipes"]
        )
        == EXACT_342_GATE_RECIPES
    )
    assert payload["manifest"]["bundle_manifest_schema_version"] == 2


def test_write_exact_profile_evidence_is_secret_free_and_machine_readable(
    tmp_path: Path,
) -> None:
    environment, _ = _gate_environment(tmp_path)
    config = load_exact_profile_gate_config(environment)

    write_exact_profile_evidence(
        config,
        version_data=_version_data(),
        installation=_installation(),
        operation_trace=_complete_trace(),
        cleanup=_cleanup_attestation(),
        recorded_at=datetime.now(tz=UTC),
    )

    raw = config.evidence_path.read_text(encoding="utf-8")
    payload = json.loads(raw)
    assert payload["status"] == "passed"
    assert payload["schema_version"] == 7
    assert payload["runner"]["wheel_sha256"].startswith("sha256:")
    assert payload["contract"]["source_commit"] == _PROFILE_SOURCE_342["commit"]
    assert payload["contract"]["bundle_manifest_schema_version"] == 2
    assert payload["profile"]["fingerprints"] == _PROFILE_342["fingerprints"]
    assert set(payload["profile"]) == {
        "contract_version",
        "ds",
        "family",
        "fingerprints",
        "selected_ds_version",
        "support_level",
        "tested",
    }
    gate_bundle = payload["gate_bundle"]
    assert gate_bundle["actions"] == list(_LIVE_SMOKE_ACTIONS)
    assert gate_bundle["action_verifications"] == dict.fromkeys(
        _LIVE_SMOKE_ACTIONS,
        "live_smoke",
    )
    assert gate_bundle["digest"] == canonical_gate_bundle_digest(
        {
            "actions": gate_bundle["actions"],
            "action_verifications": gate_bundle["action_verifications"],
            "recipes": gate_bundle["recipes"],
        }
    )
    assert payload["operation_trace"][0]["action"] == "doctor"
    assert payload["cleanup"] == {
        "gate_created_projects": {"confirmed": True, "leftovers": 0},
        "external_fixture": {
            "scope": "externally-managed-editable-offline-shell-task",
            "mutated_by_gate": True,
            "restored_by_gate": True,
            "exact_command_restored": True,
            "restored_dag_version_consistent": True,
            "non_owned_fields_restored": True,
            "dag_topology_preserved": True,
            "workflow_release_state_preserved": True,
        },
    }
    assert payload["secrets_recorded"] is False
    assert "super-secret-token" not in raw
    assert "secret-host.example" not in raw
    assert "fixture-project" not in raw
    assert "fixture-workflow" not in raw
    assert "adapter" not in raw


def test_exact_342_evidence_accepts_historical_and_current_manifest_schemas(
    tmp_path: Path,
) -> None:
    environment, _ = _gate_environment(tmp_path)
    config = load_exact_profile_gate_config(environment)
    write_exact_profile_evidence(
        config,
        version_data=_version_data(),
        installation=_installation(),
        operation_trace=_complete_trace(),
        cleanup=_cleanup_attestation(),
    )
    evidence = json.loads(config.evidence_path.read_text(encoding="utf-8"))

    for schema_version in (1, 2):
        candidate = deepcopy(evidence)
        candidate["contract"]["bundle_manifest_schema_version"] = schema_version
        validate_exact_profile_evidence_payload(candidate, ds_version="3.4.2")


def test_exact_342_evidence_rejects_unknown_bundle_manifest_schema(
    tmp_path: Path,
) -> None:
    environment, _ = _gate_environment(tmp_path)
    config = load_exact_profile_gate_config(environment)
    write_exact_profile_evidence(
        config,
        version_data=_version_data(),
        installation=_installation(),
        operation_trace=_complete_trace(),
        cleanup=_cleanup_attestation(),
    )
    evidence = json.loads(config.evidence_path.read_text(encoding="utf-8"))
    evidence["contract"]["bundle_manifest_schema_version"] = 3

    with pytest.raises(
        ValueError,
        match="contract bundle manifest schema must be 1 or 2",
    ):
        validate_exact_profile_evidence_payload(evidence, ds_version="3.4.2")


def test_schema_7_rejects_legacy_implementation_identity_fields(
    tmp_path: Path,
) -> None:
    environment, _ = _gate_environment(tmp_path)
    config = load_exact_profile_gate_config(environment)
    write_exact_profile_evidence(
        config,
        version_data=_version_data(),
        installation=_installation(),
        operation_trace=_complete_trace(),
        cleanup=_cleanup_attestation(),
    )
    evidence = json.loads(config.evidence_path.read_text(encoding="utf-8"))
    evidence["profile"]["full_adapter"] = False

    with pytest.raises(ValueError, match=r"exact 3\.4\.2 semantic profile"):
        validate_exact_profile_evidence_payload(evidence, ds_version="3.4.2")


def test_schema_3_receipt_cannot_attest_the_expanded_task_contract() -> None:
    evidence = json.loads(_HISTORICAL_SCHEMA_3_RECEIPT.read_text(encoding="utf-8"))
    validate_exact_profile_evidence_payload(evidence, ds_version="3.4.2")

    expanded_operations = sorted(
        {*evidence["contract"]["semantic_operations"], "task.get", "task.update"}
    )
    evidence["contract"]["semantic_operations"] = expanded_operations
    evidence["contract"]["operation_count"] = len(expanded_operations)

    with pytest.raises(ValueError, match="Schema-v3 evidence"):
        validate_exact_profile_evidence_payload(evidence, ds_version="3.4.2")


def test_schema_7_gate_bundle_is_digest_bound_and_required_for_current(
    tmp_path: Path,
) -> None:
    environment, _ = _gate_environment(tmp_path)
    config = load_exact_profile_gate_config(environment)
    write_exact_profile_evidence(
        config,
        version_data=_version_data(),
        installation=_installation(),
        operation_trace=_complete_trace(),
        cleanup=_cleanup_attestation(),
    )
    evidence = json.loads(config.evidence_path.read_text(encoding="utf-8"))
    validate_exact_profile_evidence_payload(evidence, ds_version="3.4.2")

    # The receipt alone cannot turn a missing legacy root into compiled evidence.
    with pytest.raises(ValueError, match="exact task-definition operations"):
        validate_schema(
            evidence, expected_cli_version=evidence["runner"]["cli_version"]
        )

    tampered = deepcopy(evidence)
    tampered["gate_bundle"]["recipes"][0]["fingerprints"]["source"] = (
        "sha256:" + "d" * 64
    )
    with pytest.raises(ValueError, match="gate bundle digest"):
        validate_exact_profile_evidence_payload(tampered, ds_version="3.4.2")

    downgraded = deepcopy(evidence)
    downgraded["schema_version"] = 5
    downgraded.pop("gate_bundle")
    downgraded["profile"] = {
        "contract_version": "3.4.2",
        "ds": "3.4.2",
        "family": "workflow-3.3-plus",
        "full_adapter": False,
        "identity_adapter": "GeneratedIdentityAdapter",
        "inspection_adapter": "DS342Adapter",
        "project_domain": "project",
        "project_domain_adapter": "_CodeProjectDomainAdapter",
        "read_adapter": "GeneratedReadAdapter",
        "selected_ds_version": "3.4.2",
        "support_level": "experimental",
        "task_definition_adapter": "GeneratedTaskDefinitionAdapter",
        "tested": False,
    }
    downgraded["contract"].pop("bundle_manifest_schema_version")
    downgraded["contract"]["semantic_operations"] = ["identity.current"]
    downgraded["contract"]["operation_count"] = 1
    historical_package_roots = frozenset(
        {
            "identity.current",
            "project.create",
            "project.delete",
            "project.get",
            "project.page",
            "project.update",
            "schedule.page",
            "workflow.get",
            "workflow.inspect",
            "workflow.page",
            "task.get",
            "task.update",
        }
    )
    with pytest.raises(ValueError, match="exact task-definition operations"):
        validate_schema(
            downgraded,
            expected_cli_version=downgraded["runner"]["cli_version"],
            compiled_semantic_operations=historical_package_roots,
        )
    # Historical schemas keep the original all-package ownership requirement.
    downgraded["contract"]["semantic_operations"] = sorted(
        {
            *downgraded["contract"]["semantic_operations"],
            *historical_package_roots,
        }
    )
    downgraded["contract"]["operation_count"] = len(historical_package_roots)
    validate_exact_profile_evidence_payload(downgraded, ds_version="3.4.2")
    assert downgraded["schema_version"] != 7


def test_write_exact_profile_evidence_requires_cleanup_and_complete_action_trace(
    tmp_path: Path,
) -> None:
    environment, _ = _gate_environment(tmp_path)
    config = load_exact_profile_gate_config(environment)

    with pytest.raises(ValueError, match="cleanup"):
        write_exact_profile_evidence(
            config,
            version_data=_version_data(),
            installation=_installation(),
            operation_trace=_complete_trace(),
            cleanup=CleanupAttestation(
                confirmed=False,
                project_leftovers=1,
                external_task=_external_task_cleanup(),
            ),
        )
    incomplete_trace = [
        entry
        for entry in _complete_trace()
        if entry.action not in {"schedule.list", "workflow.export"}
    ]
    incomplete_trace = [
        OperationTraceEntry(
            sequence=index,
            argv_shape=entry.argv_shape,
            action=entry.action,
            exit_code=entry.exit_code,
            ok=entry.ok,
            assertions=entry.assertions,
            selector_kind=entry.selector_kind,
            error_type=entry.error_type,
        )
        for index, entry in enumerate(incomplete_trace, start=1)
    ]
    with pytest.raises(ValueError, match="missing successful actions"):
        write_exact_profile_evidence(
            config,
            version_data=_version_data(),
            installation=_installation(),
            operation_trace=incomplete_trace,
            cleanup=_cleanup_attestation(),
        )
    outcome_trace = _complete_trace()
    describe_index = next(
        index
        for index, entry in enumerate(outcome_trace)
        if entry.action == "workflow.describe"
    )
    describe = outcome_trace[describe_index]
    outcome_trace[describe_index] = OperationTraceEntry(
        sequence=describe.sequence,
        argv_shape=describe.argv_shape,
        action=describe.action,
        exit_code=describe.exit_code,
        ok=describe.ok,
        assertions=("stable-result-verified",),
    )
    with pytest.raises(ValueError, match="missing required outcomes"):
        write_exact_profile_evidence(
            config,
            version_data=_version_data(),
            installation=_installation(),
            operation_trace=outcome_trace,
            cleanup=_cleanup_attestation(),
        )


def test_write_exact_profile_evidence_rejects_fixture_values_in_trace(
    tmp_path: Path,
) -> None:
    environment, _ = _gate_environment(tmp_path)
    config = load_exact_profile_gate_config(environment)
    trace = _complete_trace()
    trace[0] = OperationTraceEntry(
        sequence=1,
        argv_shape="workflow get fixture-workflow",
        action="doctor",
        exit_code=0,
        ok=True,
        assertions=(
            "current-user-ok",
            "principal-hmac-matched",
            "general-user-confirmed",
        ),
    )

    with pytest.raises(ValueError, match="protected"):
        write_exact_profile_evidence(
            config,
            version_data=_version_data(),
            installation=_installation(),
            operation_trace=trace,
            cleanup=_cleanup_attestation(),
        )


def test_write_exact_profile_evidence_rejects_fixture_codes_in_trace(
    tmp_path: Path,
) -> None:
    environment, paths = _gate_environment(tmp_path)
    fixture = json.loads(paths["fixture_manifest"].read_text(encoding="utf-8"))
    task_code = 180_609_347_532_064
    workflow = fixture["workflow"]
    assert isinstance(workflow, dict)
    task = workflow["editable_task"]
    assert isinstance(task, dict)
    task["code"] = task_code
    paths["fixture_manifest"].write_text(json.dumps(fixture), encoding="utf-8")
    config = load_exact_profile_gate_config(environment)
    trace = _complete_trace()
    trace[0] = OperationTraceEntry(
        sequence=1,
        argv_shape=f"doctor {task_code}",
        action="doctor",
        exit_code=0,
        ok=True,
        assertions=(
            "current-user-ok",
            "principal-hmac-matched",
            "general-user-confirmed",
        ),
    )

    with pytest.raises(ValueError, match="protected"):
        write_exact_profile_evidence(
            config,
            version_data=_version_data(),
            installation=_installation(),
            operation_trace=trace,
            cleanup=_cleanup_attestation(),
        )


def test_exported_profile_secrets_are_protected(tmp_path: Path) -> None:
    environment, paths = _gate_environment(tmp_path)
    paths["env_file"].write_text(
        "export DS_API_URL=https://secret-host.example/dolphinscheduler\n"
        "export DS_API_TOKEN=exported-super-secret-token\n"
        "export DS_VERSION=3.4.2\n",
        encoding="utf-8",
    )
    config = load_exact_profile_gate_config(environment)
    trace = _complete_trace()
    trace[0] = OperationTraceEntry(
        sequence=1,
        argv_shape="doctor exported-super-secret-token",
        action="doctor",
        exit_code=0,
        ok=True,
        assertions=("principal-hmac-matched", "general-user-confirmed"),
    )

    with pytest.raises(ValueError, match="protected"):
        write_exact_profile_evidence(
            config,
            version_data=_version_data(),
            installation=_installation(),
            operation_trace=trace,
            cleanup=_cleanup_attestation(),
        )


def test_exact_task_round_trip_restores_command_and_dag_version(
    tmp_path: Path,
) -> None:
    environment, _ = _gate_environment(tmp_path)
    config = load_exact_profile_gate_config(environment)
    cluster = _TaskRoundTripCluster(config)
    operation_trace: list[OperationTraceEntry] = []

    cleanup = _verify_task_definition_round_trip(
        cluster.invoke,
        config=config,
        operation_trace=operation_trace,
    )

    assert cluster.command == cluster.original_command
    assert len(cluster.update_commands) == 2
    assert cluster.update_commands[0] != cluster.original_command
    assert cluster.update_commands[0].endswith("\n")
    assert cluster.update_commands[1] == cluster.original_command
    assert cleanup == _external_task_cleanup()
    assert any(
        entry.action == "task.list"
        and "dag-task-version-matched-readback" in entry.assertions
        for entry in operation_trace
    )
    assert any(
        entry.action == "task.update"
        and "original-command-restored-exactly" in entry.assertions
        for entry in operation_trace
    )


def test_exact_task_round_trip_restores_after_post_update_failure(
    tmp_path: Path,
) -> None:
    environment, _ = _gate_environment(tmp_path)
    config = load_exact_profile_gate_config(environment)
    cluster = _TaskRoundTripCluster(config, fail_after_update=True)

    with pytest.raises(RuntimeError, match="injected post-update failure"):
        _verify_task_definition_round_trip(
            cluster.invoke,
            config=config,
            operation_trace=[],
        )

    assert cluster.command == cluster.original_command
    assert cluster.update_commands[-1] == cluster.original_command


def test_exact_task_round_trip_detects_and_restores_a_mutating_dry_run(
    tmp_path: Path,
) -> None:
    environment, _ = _gate_environment(tmp_path)
    config = load_exact_profile_gate_config(environment)
    cluster = _TaskRoundTripCluster(config, mutate_on_dry_run=True)

    with pytest.raises(AssertionError):
        _verify_task_definition_round_trip(
            cluster.invoke,
            config=config,
            operation_trace=[],
        )

    assert cluster.command == cluster.original_command
    assert cluster.update_commands == [cluster.original_command]


def test_exact_task_round_trip_does_not_overwrite_a_concurrent_command(
    tmp_path: Path,
) -> None:
    environment, _ = _gate_environment(tmp_path)
    config = load_exact_profile_gate_config(environment)
    cluster = _TaskRoundTripCluster(config, concurrent_on_cleanup=True)

    with pytest.raises(AssertionError, match="concurrent task command"):
        _verify_task_definition_round_trip(
            cluster.invoke,
            config=config,
            operation_trace=[],
        )

    assert cluster.command == 'echo "concurrent owner"\n'
    assert len(cluster.update_commands) == 1


def test_exact_task_round_trip_rejects_non_owned_field_loss(
    tmp_path: Path,
) -> None:
    environment, _ = _gate_environment(tmp_path)
    config = load_exact_profile_gate_config(environment)
    cluster = _TaskRoundTripCluster(config, mutate_non_owned_on_update=True)

    with pytest.raises(AssertionError):
        _verify_task_definition_round_trip(
            cluster.invoke,
            config=config,
            operation_trace=[],
        )

    assert cluster.command == cluster.original_command
    assert cluster.worker_group == "default"


def test_exact_task_round_trip_does_not_retry_a_failed_restore(
    tmp_path: Path,
) -> None:
    environment, _ = _gate_environment(tmp_path)
    config = load_exact_profile_gate_config(environment)
    cluster = _TaskRoundTripCluster(config, restore_failures=1)

    with pytest.raises(AssertionError, match="Task restore command failed"):
        _verify_task_definition_round_trip(
            cluster.invoke,
            config=config,
            operation_trace=[],
        )

    assert cluster.command != cluster.original_command
    assert len(cluster.update_commands) == 1


def test_failed_create_never_recovers_or_deletes_by_candidate_name() -> None:
    calls: list[list[str]] = []

    def invoke(argv: list[str]) -> DsctlCommandResult:
        calls.append(argv)
        message = "cleanup must not inspect a name after create failed"
        raise AssertionError(message)

    _cleanup_project_after_failure(
        invoke,
        project_code=None,
        candidate_names=("colliding-project",),
        ownership_descriptions=("owned",),
        create_may_have_committed=False,
        already_deleted=False,
    )

    assert calls == []


@pytest.mark.parametrize(
    "error_type",
    [
        "config_error",
        "confirmation_required",
        "conflict",
        "invalid_state",
        "not_found",
        "permission_denied",
        "resolution_error",
        "unsupported_feature",
        "user_input_error",
    ],
)
def test_create_rejection_classification_accepts_only_definitive_errors(
    error_type: str,
) -> None:
    result = DsctlCommandResult(
        argv=("project", "create"),
        exit_code=1,
        stdout="",
        stderr="",
        payload={
            "ok": False,
            "action": "project.create",
            "error": {"type": error_type},
        },
    )

    assert _is_confirmed_create_rejection(result)


@pytest.mark.parametrize(
    "error_type",
    [
        "api_http_error",
        "api_result_error",
        "api_transport_error",
        "dsctl_error",
        "timeout",
        "unsupported_operation",
        "unsupported_version",
    ],
)
def test_create_rejection_classification_recovers_ambiguous_errors(
    error_type: str,
) -> None:
    result = DsctlCommandResult(
        argv=("project", "create"),
        exit_code=1,
        stdout="",
        stderr="",
        payload={
            "ok": False,
            "action": "project.create",
            "error": {"type": error_type},
        },
    )

    assert not _is_confirmed_create_rejection(result)


def test_cleanup_retries_transport_then_deletes_owned_committed_project(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []
    lookup_attempts = 0
    ownership_marker = "exact-342-owner=transport-recovery"

    def invoke(argv: list[str]) -> DsctlCommandResult:
        nonlocal lookup_attempts
        calls.append(argv)
        action = "project.get"
        data: object
        if argv == ["project", "get", "candidate-project"]:
            lookup_attempts += 1
            if lookup_attempts < 3:
                return _error_result(
                    argv,
                    action=action,
                    error_type="api_transport_error",
                )
            data = {"code": 7, "name": "candidate-project"}
        elif argv == ["project", "get", "7"]:
            data = {
                "code": 7,
                "name": "candidate-project",
                "description": ownership_marker,
            }
        elif argv == ["project", "delete", "7", "--force"]:
            action = "project.delete"
            data = {"deleted": True}
        elif argv == ["project", "list", "--search", "candidate-project"]:
            action = "project.list"
            data = {"totalList": []}
        else:
            message = f"unexpected cleanup command: {argv}"
            raise AssertionError(message)
        return DsctlCommandResult(
            argv=tuple(argv),
            exit_code=0,
            stdout="",
            stderr="",
            payload={"ok": True, "action": action, "data": data},
        )

    monkeypatch.setattr(
        "tests.live.test_exact_profile.time.sleep",
        lambda _seconds: None,
    )

    _cleanup_project_after_failure(
        invoke,
        project_code=None,
        candidate_names=("candidate-project",),
        ownership_descriptions=(ownership_marker,),
        create_may_have_committed=True,
        already_deleted=False,
    )

    assert lookup_attempts == 3
    assert ["project", "delete", "7", "--force"] in calls


def test_failed_cleanup_refuses_to_delete_project_without_ownership_marker() -> None:
    calls: list[list[str]] = []

    def invoke(argv: list[str]) -> DsctlCommandResult:
        calls.append(argv)
        return DsctlCommandResult(
            argv=tuple(argv),
            exit_code=0,
            stdout="",
            stderr="",
            payload={
                "ok": True,
                "action": "project.get",
                "data": {
                    "code": 7,
                    "name": "foreign-project",
                    "description": "owned by someone else",
                },
            },
        )

    with pytest.raises(AssertionError, match="ownership marker"):
        _cleanup_project_after_failure(
            invoke,
            project_code=7,
            candidate_names=("candidate-project",),
            ownership_descriptions=("exact gate owner",),
            create_may_have_committed=True,
            already_deleted=False,
        )

    assert all(call[:2] != ["project", "delete"] for call in calls)


@pytest.mark.parametrize(
    ("relative_path", "expected_schemas"),
    [
        ("external-shell/3.4.2", {3, 4, 6}),
        ("history/external-shell/3.4.2", {6}),
    ],
)
def test_historical_exact_342_receipts_remain_auditable(
    relative_path: str, expected_schemas: set[int]
) -> None:
    evidence_dir = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "development"
        / "live-evidence"
        / relative_path
    )
    historical = []
    for path in evidence_dir.glob("*.json"):
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("schema_version") in {3, 4, 5, 6}:
            validate_exact_profile_evidence_payload(payload, ds_version="3.4.2")
            historical.append(payload["schema_version"])

    assert set(historical) == expected_schemas, (
        "the historical receipts must remain auditable in their scenario directory"
    )


def test_committed_evidence_validator_rejects_sensitive_or_extra_payload(
    tmp_path: Path,
) -> None:
    environment, _ = _gate_environment(tmp_path)
    config = load_exact_profile_gate_config(environment)
    write_exact_profile_evidence(
        config,
        version_data=_version_data(),
        installation=_installation(),
        operation_trace=_complete_trace(),
        cleanup=_cleanup_attestation(),
    )
    evidence = json.loads(config.evidence_path.read_text(encoding="utf-8"))
    evidence["runner"]["token"] = "leaked"

    with pytest.raises(ValueError, match="forbidden field"):
        validate_exact_profile_evidence_payload(evidence, ds_version="3.4.2")

    del evidence["runner"]["token"]
    evidence["dolphinscheduler"]["image_source"] = "https://internal.example"
    with pytest.raises(ValueError, match="URLs"):
        validate_exact_profile_evidence_payload(evidence, ds_version="3.4.2")

    evidence["dolphinscheduler"]["image_source"] = (
        "swarm service plus local image inspect"
    )
    evidence["runner"]["cli_version"] = "0.0.0"
    with pytest.raises(ValueError, match="current package version"):
        validate_exact_profile_evidence_payload(evidence, ds_version="3.4.2")


@pytest.mark.parametrize(
    "provisioner",
    [
        "Bearer super-secret-token",
        "DS_API_TOKEN=super-secret-token",
        "AWS_SECRET_ACCESS_KEY=super-secret-token",
        "db_password=super-secret-token",
        "-----BEGIN PRIVATE KEY-----",
    ],
)
def test_current_schema_validator_rejects_sensitive_provenance_text(
    tmp_path: Path,
    provisioner: str,
) -> None:
    environment, _ = _gate_environment(tmp_path)
    config = load_exact_profile_gate_config(environment)
    write_exact_profile_evidence(
        config,
        version_data=_version_data(),
        installation=_installation(),
        operation_trace=_complete_trace(),
        cleanup=_cleanup_attestation(),
    )
    evidence = json.loads(config.evidence_path.read_text(encoding="utf-8"))
    evidence["fixture"]["provisioner"] = provisioner

    with pytest.raises(ValueError, match="evidence contains sensitive text"):
        validate_exact_profile_evidence_payload(evidence, ds_version="3.4.2")


def test_current_schema_validator_requires_inspection_bound_projection_v2(
    tmp_path: Path,
) -> None:
    environment, _ = _gate_environment(tmp_path)
    config = load_exact_profile_gate_config(environment)
    write_exact_profile_evidence(
        config,
        version_data=_version_data(),
        installation=_installation(),
        operation_trace=_complete_trace(),
        cleanup=_cleanup_attestation(),
    )
    evidence = json.loads(config.evidence_path.read_text(encoding="utf-8"))
    evidence["fixture"]["provisioner"] = "dsmatrix-exact-read-state-projection/v1"

    with pytest.raises(ValueError, match="state-projection/v2"):
        validate_exact_profile_evidence_payload(evidence, ds_version="3.4.2")


def _gate_environment(tmp_path: Path) -> tuple[dict[str, str], dict[str, Path]]:
    env_file = tmp_path / "cluster.env"
    env_file.write_text(
        "DS_API_URL=https://secret-host.example/dolphinscheduler\n"
        "DS_API_TOKEN=super-secret-token\n"
        "DS_VERSION=3.4.2\n",
        encoding="utf-8",
    )
    bin_dir = tmp_path / "wheel-venv" / "bin"
    bin_dir.mkdir(parents=True)
    executable = bin_dir / "dsctl"
    python = bin_dir / "python"
    executable.touch()
    python.touch()
    wheel = tmp_path / "dolphinscheduler_cli-0.4.0-py3-none-any.whl"
    _write_manifest_wheel(wheel)
    cluster_manifest = tmp_path / "cluster.json"
    cluster_manifest.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "ds_version": "3.4.2",
                "image_tag": _IMAGE_TAG,
                "image_digest": _IMAGE_DIGEST,
                "image_source": "swarm service plus local image inspect",
                "image_observed_at": datetime.now(tz=UTC).isoformat(),
                "api_target_hmac_sha256": identity_hmac(
                    "https://secret-host.example/dolphinscheduler",
                    key=_ATTESTATION_KEY,
                ),
                "principal_hmac_sha256": identity_hmac(
                    "dsctl_live_etl",
                    key=_ATTESTATION_KEY,
                ),
                "persona": "etl-developer",
            }
        ),
        encoding="utf-8",
    )
    fixture_manifest = tmp_path / "fixture.json"
    fixture_manifest.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "ds_version": "3.4.2",
                "image_tag": _IMAGE_TAG,
                "image_digest": _IMAGE_DIGEST,
                "exclusive": True,
                "provisioner": "dsmatrix-exact-read-state-projection/v2",
                "project": {"name": "fixture-project", "code": 7},
                "workflow": {
                    "name": "fixture-workflow",
                    "code": 101,
                    "scheduled": True,
                    "schedule_id": 23,
                    "release_state": "OFFLINE",
                    "editable_task": {
                        "name": "fixture-shell",
                        "code": 202,
                        "type": "SHELL",
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    evidence = tmp_path / "evidence" / "ds-3.4.2.json"
    attestation_key_file = tmp_path / "attestation.key"
    attestation_key_file.write_bytes(_ATTESTATION_KEY)
    attestation_key_file.chmod(0o600)
    paths = {
        "env_file": env_file.resolve(),
        "executable": executable.resolve(),
        "python": python.resolve(),
        "wheel": wheel.resolve(),
        "cluster_manifest": cluster_manifest.resolve(),
        "fixture_manifest": fixture_manifest.resolve(),
        "attestation_key_file": attestation_key_file.resolve(),
        "evidence": evidence,
    }
    environment = {
        "DS_LIVE_EXACT_VERSION": "3.4.2",
        "DS_LIVE_EXACT_ENV_FILE": str(env_file),
        "DS_LIVE_EXACT_DSCTL": str(executable),
        "DS_LIVE_EXACT_PYTHON": str(python),
        "DS_LIVE_EXACT_WHEEL": str(wheel),
        "DS_LIVE_EXACT_CLUSTER_MANIFEST": str(cluster_manifest),
        "DS_LIVE_EXACT_FIXTURE_MANIFEST": str(fixture_manifest),
        "DS_LIVE_EXACT_ATTESTATION_KEY_FILE": str(attestation_key_file),
        "DS_LIVE_EXACT_EVIDENCE": str(evidence),
    }
    return environment, paths


def _error_result(
    argv: list[str],
    *,
    action: str,
    error_type: str,
) -> DsctlCommandResult:
    return DsctlCommandResult(
        argv=tuple(argv),
        exit_code=1,
        stdout="",
        stderr="",
        payload={
            "ok": False,
            "action": action,
            "error": {"type": error_type},
        },
    )


def _version_data() -> dict[str, object]:
    return {
        "cli": "0.4.0",
        "ds": "3.4.2",
        "selected_ds_version": "3.4.2",
        "contract_version": "3.4.2",
        "family": "workflow-3.3-plus",
        "support_level": "experimental",
    }


def _installation() -> InstalledExactProfileAttestation:
    return InstalledExactProfileAttestation(
        distribution_version="0.4.0",
        server_version="3.4.2",
        family="workflow-3.3-plus",
        support_level="experimental",
        tested=False,
        contract_version="3.4.2",
        profile_fingerprints=dict(_PROFILE_342["fingerprints"]),
        action_verifications={
            action: "live_smoke" for action, _operation in EXACT_342_GATE_RECIPES
        },
        gate_recipes=_test_gate_recipes(),
        manifest=_test_manifest_data(),
    )


def _installed_probe_payload(module_file: Path) -> dict[str, object]:
    action_verifications = {
        action: "live_smoke" for action, _operation in EXACT_342_GATE_RECIPES
    }
    return {
        "distribution_version": "0.4.0",
        "module_file": str(module_file),
        "server_version": "3.4.2",
        "family": "workflow-3.3-plus",
        "support_level": "experimental",
        "tested": False,
        "contract_version": "3.4.2",
        "profile_source": dict(_PROFILE_SOURCE_342),
        "profile_fingerprints": dict(_PROFILE_342["fingerprints"]),
        "action_verifications": action_verifications,
        "gate_recipes": list(_test_gate_recipes()),
        "manifest": _test_manifest_data(),
    }


def _test_gate_recipes() -> tuple[dict[str, object], ...]:
    return tuple(
        {
            "action": action,
            "semantic_operation": semantic_operation,
            "build_status": "accepted",
            "fingerprints": dict(
                _PROFILE_342["build_decisions"][semantic_operation]["fingerprints"]
            ),
        }
        for action, semantic_operation in EXACT_342_GATE_RECIPES
    )


def _test_manifest_data() -> dict[str, object]:
    return {
        "bundle_manifest_schema_version": 2,
        "ds_version": "3.4.2",
        "selection": "runtime-slice",
        "semantic_operations": list(_TEST_SEMANTIC_OPERATIONS),
        "source_tag": _PROFILE_SOURCE_342["tag"],
        "source_commit": _PROFILE_SOURCE_342["commit"],
        "source_tree": _PROFILE_SOURCE_342["tree"],
        "source_contract_digest": _SOURCE_DIGEST,
        "rendered_contract_digest": _RENDERED_DIGEST,
        "operation_count": len(_TEST_SEMANTIC_OPERATIONS),
    }


def _complete_trace() -> list[OperationTraceEntry]:
    action_assertions = (
        (
            "doctor",
            (
                "stable-result-verified",
                "principal-hmac-matched",
                "general-user-confirmed",
            ),
        ),
        ("project.create", ("stable-result-verified",)),
        ("project.delete", ("stable-result-verified",)),
        ("project.get", ("stable-result-verified",)),
        ("project.list", ("stable-result-verified", "cleanup-leftovers-zero")),
        ("project.update", ("stable-result-verified",)),
        ("workflow.get", ("stable-result-verified",)),
        ("workflow.list", ("stable-result-verified",)),
        (
            "workflow.describe",
            ("dag-task-set-nonempty", "dag-relations-valid", "schedule-hydrated"),
        ),
        (
            "workflow.digest",
            ("digest-counts-match-describe", "digest-topology-match-describe"),
        ),
        (
            "workflow.export",
            ("yaml-dag-matches-describe", "yaml-schedule-matches-live"),
        ),
        (
            "schedule.list",
            ("fixture-schedule-id-matched", "workflow-filter-matched"),
        ),
        ("task.list", ("editable-shell-task-matched",)),
        ("task.get", ("task-get-matched-list-version",)),
        (
            "task.update",
            (
                "exact-update-request-compiled",
                "dry-run-no-request-sent",
                "dry-run-state-unchanged",
                "dry-run-dag-unchanged",
            ),
        ),
        ("task.update", ("command-updated-exactly", "task-version-advanced")),
        ("task.get", ("update-readback-exact", "non-owned-fields-preserved")),
        ("task.list", ("dag-task-version-matched-readback",)),
        ("workflow.digest", ("task-update-topology-preserved",)),
        ("task.update", ("original-command-restored-exactly",)),
        (
            "task.get",
            ("original-command-readback-exact", "restored-non-owned-fields"),
        ),
        ("task.list", ("restored-dag-version-consistent",)),
        ("workflow.digest", ("restored-dag-topology-preserved",)),
        ("workflow.get", ("workflow-release-state-preserved",)),
    )
    trace = [
        OperationTraceEntry(
            sequence=index,
            argv_shape=action.replace(".", " "),
            action=action,
            exit_code=0,
            ok=True,
            assertions=assertions,
        )
        for index, (action, assertions) in enumerate(action_assertions, start=1)
    ]
    next_sequence = len(trace) + 1
    trace.extend(
        (
            OperationTraceEntry(
                sequence=next_sequence,
                argv_shape="project create --name <duplicate-project-name>",
                action="project.create",
                exit_code=1,
                ok=False,
                assertions=("stable-conflict", "actionable-suggestion"),
                error_type="conflict",
            ),
            OperationTraceEntry(
                sequence=next_sequence + 1,
                argv_shape="workflow get <missing-workflow-code>",
                action="workflow.get",
                exit_code=1,
                ok=False,
                assertions=("stable-not-found",),
                error_type="not_found",
            ),
            OperationTraceEntry(
                sequence=next_sequence + 2,
                argv_shape="project get <deleted-project-code>",
                action="project.get",
                exit_code=1,
                ok=False,
                assertions=("stable-not-found-after-delete",),
                error_type="not_found",
            ),
        )
    )
    return trace


def _write_manifest_wheel(
    path: Path,
    *,
    bundle_manifest_schema_version: int = 2,
    include_compiled: bool = True,
) -> None:
    semantic_operations = repr(_TEST_SEMANTIC_OPERATIONS)
    manifest = f"""\
BUNDLE_MANIFEST_SCHEMA_VERSION = {bundle_manifest_schema_version}
DS_VERSION = "3.4.2"
SELECTION = "runtime-slice"
SEMANTIC_OPERATIONS = {semantic_operations}
SOURCE_TAG = "{_PROFILE_SOURCE_342["tag"]}"
SOURCE_COMMIT = "{_PROFILE_SOURCE_342["commit"]}"
SOURCE_TREE = "{_PROFILE_SOURCE_342["tree"]}"
SOURCE_CONTRACT_DIGEST = "{_SOURCE_DIGEST}"
RENDERED_CONTRACT_DIGEST = "{_RENDERED_DIGEST}"
OPERATION_COUNT = {len(_TEST_SEMANTIC_OPERATIONS)}
"""
    with zipfile.ZipFile(path, mode="w") as archive:
        archive.writestr(
            "dsctl/generated/versions/ds_3_4_2/_manifest.py",
            manifest,
        )
        generated_root = Path(__file__).resolve().parents[2] / "src/dsctl/generated"
        for artifact in (
            *sorted((generated_root / "versions").glob("ds_*/_manifest.py")),
            *sorted((generated_root / "wire_programs").rglob("*.py")),
            *sorted((generated_root / "wire_runtime").rglob("*.py")),
        ):
            relative = artifact.relative_to(generated_root).as_posix()
            if not include_compiled and relative.startswith("wire_programs/"):
                continue
            if relative != "versions/ds_3_4_2/_manifest.py":
                archive.write(artifact, f"dsctl/generated/{relative}")


class _TaskRoundTripCluster:
    def __init__(
        self,
        config: ExactProfileGateConfig,
        *,
        fail_after_update: bool = False,
        mutate_on_dry_run: bool = False,
        mutate_non_owned_on_update: bool = False,
        concurrent_on_cleanup: bool = False,
        restore_failures: int = 0,
    ) -> None:
        self.fixture = config.fixture
        self.original_command = 'echo "original fixture"\n'
        self.command = self.original_command
        self.version = 3
        self.fail_after_update = fail_after_update
        self.mutate_on_dry_run = mutate_on_dry_run
        self.mutate_non_owned_on_update = mutate_non_owned_on_update
        self.concurrent_on_cleanup = concurrent_on_cleanup
        self.restore_failures = restore_failures
        self.task_list_calls = 0
        self.task_get_calls = 0
        self.worker_group = "dedicated"
        self.update_commands: list[str] = []

    def invoke(self, argv: list[str]) -> DsctlCommandResult:
        action = ".".join(argv[:2])
        if action == "task.list":
            return _success_result(argv, action=action, data=self._task_list())
        if action == "task.get":
            return _success_result(argv, action=action, data=self._task_get())
        if action == "task.update":
            return self._task_update(argv, action=action)
        if action == "workflow.digest":
            return _success_result(argv, action=action, data=self._workflow_digest())
        if action == "workflow.get":
            data = {
                "code": self.fixture.workflow_code,
                "name": self.fixture.workflow_name,
                "releaseState": "OFFLINE",
            }
            return _success_result(argv, action=action, data=data)
        message = f"unexpected exact task gate command: {argv}"
        raise AssertionError(message)

    def _task_list(self) -> list[dict[str, object]]:
        self.task_list_calls += 1
        if self.fail_after_update and self.task_list_calls == 3:
            message = "injected post-update failure"
            raise RuntimeError(message)
        return [self._task_ref() | {"version": self.version}]

    def _task_get(self) -> dict[str, object]:
        self.task_get_calls += 1
        if (
            self.concurrent_on_cleanup
            and len(self.update_commands) == 1
            and self.task_get_calls == 4
        ):
            self.command = 'echo "concurrent owner"\n'
            self.version += 1
        return self._task_data()

    def _task_update(
        self,
        argv: list[str],
        *,
        action: str,
    ) -> DsctlCommandResult:
        set_value = argv[argv.index("--set") + 1]
        command = set_value.removeprefix("command=")
        if "--dry-run" in argv:
            return self._task_update_dry_run(argv, action=action, command=command)
        if command == self.original_command and self.restore_failures > 0:
            self.restore_failures -= 1
            return _error_result(
                argv,
                action=action,
                error_type="api_transport_error",
            )
        self.command = command
        self.version += 1
        self.update_commands.append(command)
        if self.mutate_non_owned_on_update and len(self.update_commands) == 1:
            self.worker_group = "default"
        return _success_result(argv, action=action, data=self._task_data())

    def _task_update_dry_run(
        self,
        argv: list[str],
        *,
        action: str,
        command: str,
    ) -> DsctlCommandResult:
        assert argv[argv.index("--columns") + 1] == "*"
        if self.mutate_on_dry_run:
            self.command = command
            self.version += 1
        data = {
            "dry_run": True,
            "requests": [
                {
                    "method": "PUT",
                    "path": (
                        f"/projects/{self.fixture.project_code}/task-definition/"
                        f"{self.fixture.task_code}/with-upstream"
                    ),
                    "form": {
                        "taskDefinitionJsonObj": json.dumps(
                            {
                                "taskType": self.fixture.task_type,
                                "taskParams": json.dumps({"rawScript": command}),
                            }
                        )
                    },
                }
            ],
        }
        return _success_result(
            argv,
            action=action,
            data=data,
            extra={
                "warnings": [
                    {
                        "code": "dry_run_no_mutation_sent",
                        "mutation_sent": False,
                    }
                ]
            },
        )

    def _workflow_digest(self) -> dict[str, object]:
        task_ref = self._task_ref()
        return {
            "workflow": {
                "code": self.fixture.workflow_code,
                "name": self.fixture.workflow_name,
                "version": self.version,
                "releaseState": "OFFLINE",
            },
            "taskCount": 1,
            "relationCount": 0,
            "taskTypeCounts": {"SHELL": 1},
            "globalParamNames": [],
            "rootTasks": [task_ref],
            "leafTasks": [task_ref],
            "isolatedTasks": [task_ref],
            "tasks": [
                {
                    **task_ref,
                    "taskType": self.fixture.task_type,
                    "upstreamTasks": [],
                    "downstreamTasks": [],
                    "isRoot": True,
                    "isLeaf": True,
                }
            ],
        }

    def _task_data(self) -> dict[str, object]:
        return {
            "code": self.fixture.task_code,
            "name": self.fixture.task_name,
            "version": self.version,
            "projectCode": self.fixture.project_code,
            "taskType": self.fixture.task_type,
            "taskParams": {"rawScript": self.command},
            "workerGroup": self.worker_group,
        }

    def _task_ref(self) -> dict[str, object]:
        return {"code": self.fixture.task_code, "name": self.fixture.task_name}


def _success_result(
    argv: list[str],
    *,
    action: str,
    data: object,
    extra: dict[str, object] | None = None,
) -> DsctlCommandResult:
    payload = {"ok": True, "action": action, "data": data}
    if extra is not None:
        payload.update(extra)
    return DsctlCommandResult(
        argv=tuple(argv),
        exit_code=0,
        stdout="",
        stderr="",
        payload=payload,
    )


def _doctor_result(
    *,
    api_status: str,
    current_user_status: str,
) -> DsctlCommandResult:
    return _success_result(
        ["doctor"],
        action="doctor",
        data={
            "checks": [
                {"name": "profile", "status": "ok"},
                {"name": "adapter", "status": "warning"},
                {"name": "api", "status": api_status},
                {"name": "current_user", "status": current_user_status},
            ]
        },
    )


def _external_task_cleanup() -> ExternalTaskCleanupAttestation:
    return ExternalTaskCleanupAttestation(
        mutated=True,
        restored=True,
        exact_command_restored=True,
        restored_dag_version_consistent=True,
        non_owned_fields_restored=True,
        dag_topology_preserved=True,
        workflow_release_state_preserved=True,
    )


def _cleanup_attestation() -> CleanupAttestation:
    return CleanupAttestation(
        confirmed=True,
        project_leftovers=0,
        external_task=_external_task_cleanup(),
    )
