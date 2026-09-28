from __future__ import annotations

import copy
import hashlib
import importlib
import json
import subprocess
import sys
import zipfile
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from dsctl.generated.task_definition_cleanup_profiles import (
    CROSS_PROCESS_TASK_DEFINITION_RECOVERY_VERSIONS as _RECOVERY_VERSIONS,
)
from dsctl.generated.task_definition_cleanup_profiles import (
    TASK_DEFINITION_CLEANUP_SEMANTIC_OPERATION as _TASK_DEFINITION_CLEANUP_ROOT,
)
from dsctl.generated.version_profiles import VERSION_PROFILES

if TYPE_CHECKING:
    from types import ModuleType


def _load_module() -> ModuleType:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module("run_exact_conformance_bundle_gate")


def test_parse_args_resolves_one_exact_named_bundle(tmp_path: Path) -> None:
    gate = _load_module()

    inputs = gate._parse_args(
        [
            "--ds-version",
            "3.2.2",
            "--bundle",
            "legacy_core/v1",
            "--wheel",
            str(tmp_path / "runtime.whl"),
            "--env-file",
            str(tmp_path / "cluster.env"),
            "--attestation-key-file",
            str(tmp_path / "attestation.key"),
            "--cluster-manifest",
            str(tmp_path / "cluster.json"),
            "--fixture-manifest",
            str(tmp_path / "fixture.json"),
            "--evidence",
            str(tmp_path / "evidence.json"),
        ]
    )

    assert inputs.ds_version == "3.2.2"
    assert inputs.bundle == "legacy_core/v1"
    assert inputs.wheel == (tmp_path / "runtime.whl").resolve()
    assert inputs.evidence == (tmp_path / "evidence.json").resolve()


def test_parse_args_accepts_one_private_cross_process_recovery_identity(
    tmp_path: Path,
) -> None:
    gate = _load_module()
    recovery = tmp_path / "recovery.json"
    recovery.write_text(
        json.dumps({"schema_version": 1, "run_id": "0123456789abcdef"}),
        encoding="utf-8",
    )
    recovery.chmod(0o600)

    inputs = gate._parse_args(
        [
            "--ds-version",
            "2.0.9",
            "--bundle",
            "full_core/v1",
            "--wheel",
            str(tmp_path / "runtime.whl"),
            "--env-file",
            str(tmp_path / "cluster.env"),
            "--attestation-key-file",
            str(tmp_path / "attestation.key"),
            "--cluster-manifest",
            str(tmp_path / "cluster.json"),
            "--fixture-manifest",
            str(tmp_path / "fixture.json"),
            "--evidence",
            str(tmp_path / "must-not-be-written.json"),
            "--recovery-run-id-file",
            str(recovery),
        ]
    )

    assert inputs.recovery_run_id_file == recovery.resolve()


def test_parse_args_accepts_private_run_id_output(tmp_path: Path) -> None:
    gate = _load_module()
    base = _inputs(tmp_path, gate=gate, ds_version="2.0.0", bundle="full_core/v1")
    output = tmp_path / "run-id.json"
    inputs = gate._parse_args(
        [
            "--ds-version",
            base.ds_version,
            "--bundle",
            base.bundle,
            "--wheel",
            str(base.wheel),
            "--env-file",
            str(base.env_file),
            "--attestation-key-file",
            str(base.attestation_key_file),
            "--cluster-manifest",
            str(base.cluster_manifest),
            "--fixture-manifest",
            str(base.fixture_manifest),
            "--evidence",
            str(base.evidence),
            "--run-id-output-file",
            str(output),
        ]
    )
    assert inputs.run_id_output_file == output.absolute()


def test_parse_args_preserves_recovery_symlink_for_nofollow_rejection(
    tmp_path: Path,
) -> None:
    gate = _load_module()
    target = tmp_path / "recovery-target.json"
    target.write_text(
        json.dumps({"schema_version": 1, "run_id": "0123456789abcdef"}),
        encoding="utf-8",
    )
    target.chmod(0o600)
    recovery = tmp_path / "recovery-link.json"
    recovery.symlink_to(target)
    base = _inputs(
        tmp_path / "inputs",
        gate=gate,
        ds_version="2.0.9",
        bundle="full_core/v1",
    )

    inputs = gate._parse_args(
        [
            "--ds-version",
            base.ds_version,
            "--bundle",
            base.bundle,
            "--wheel",
            str(base.wheel),
            "--env-file",
            str(base.env_file),
            "--attestation-key-file",
            str(base.attestation_key_file),
            "--cluster-manifest",
            str(base.cluster_manifest),
            "--fixture-manifest",
            str(base.fixture_manifest),
            "--evidence",
            str(base.evidence),
            "--recovery-run-id-file",
            str(recovery),
        ]
    )

    assert inputs.recovery_run_id_file == recovery.absolute()
    with pytest.raises(ValueError, match="owner-private regular file"):
        gate._validated_recovery_run_id(inputs)


def test_direct_input_rejects_path_like_bundle_before_install(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate = _load_module()
    inputs = _inputs(tmp_path, gate=gate, bundle="../../full_core/v1")

    def unexpected_install(*args: object, **kwargs: object) -> Any:
        message = "unknown bundle reached installation"
        raise AssertionError(message)

    monkeypatch.setattr(gate.gate_harness, "install_wheel", unexpected_install)

    with pytest.raises(ValueError, match="Unknown conformance bundle"):
        gate.run_gate(inputs)


def test_preflight_derives_coordinate_status_from_tracked_generated_assessment(
    tmp_path: Path,
) -> None:
    gate = _load_module()
    inputs = _inputs(
        tmp_path,
        gate=gate,
        ds_version="1.3.9",
        bundle="full_core/v1",
    )
    assessment = copy.deepcopy(gate._TRACKED_CONFORMANCE_DATA)
    full_core = next(
        item for item in assessment["bundles"] if item["name"] == "full_core/v1"
    )
    coordinate = next(
        item for item in full_core["versions"] if item["version"] == "1.3.9"
    )
    coordinate["status"] = "ready"
    coordinate["blockers"] = []
    _refresh_assessment_digest(assessment)

    gate._require_tracked_static_ready_coordinate(inputs, assessment=assessment)


def test_preflight_and_parser_follow_new_tracked_named_bundle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate = _load_module()
    assessment = copy.deepcopy(gate._TRACKED_CONFORMANCE_DATA)
    added = copy.deepcopy(assessment["bundles"][0])
    added["name"] = "future_core/v1"
    added["bundle_digest"] = "sha256:" + "a" * 64
    assessment["bundles"].append(added)
    _refresh_assessment_digest(assessment)
    monkeypatch.setattr(gate, "_TRACKED_CONFORMANCE_DATA", assessment)
    inputs = _inputs(tmp_path, gate=gate, bundle="future_core/v1")

    parsed = gate._parse_args(
        [
            "--ds-version",
            "3.2.2",
            "--bundle",
            "future_core/v1",
            "--wheel",
            str(inputs.wheel),
            "--env-file",
            str(inputs.env_file),
            "--attestation-key-file",
            str(inputs.attestation_key_file),
            "--cluster-manifest",
            str(inputs.cluster_manifest),
            "--fixture-manifest",
            str(inputs.fixture_manifest),
            "--evidence",
            str(inputs.evidence),
        ]
    )

    assert parsed.bundle == "future_core/v1"
    assert gate._validate_inputs(inputs).bundle == "future_core/v1"


def test_profile_version_must_match_requested_coordinate_before_install(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate = _load_module()
    inputs = _inputs(tmp_path, gate=gate, ds_version="3.2.2")
    inputs.env_file.write_text(
        "DS_API_URL=http://matrix.test/dolphinscheduler\n"
        "DS_API_TOKEN=secret-token\n"
        "DS_VERSION=3.4.2\n",
        encoding="utf-8",
    )

    def unexpected_install(*args: object, **kwargs: object) -> Any:
        message = "mismatched profile reached installation"
        raise AssertionError(message)

    monkeypatch.setattr(gate.gate_harness, "install_wheel", unexpected_install)

    with pytest.raises(ValueError, match=r"profile DS_VERSION 3\.4\.2.*3\.2\.2"):
        gate.run_gate(inputs)


def test_malformed_tracked_assessment_fails_closed(tmp_path: Path) -> None:
    gate = _load_module()
    inputs = _inputs(tmp_path, gate=gate)
    malformed = {
        "schema_version": 1,
        "claim": "static-action-closure-only",
        "promotion_claimed": False,
        "bundles": [
            {
                "name": "legacy_core/v1",
                "versions": "not-an-array",
            }
        ],
    }

    with pytest.raises(ValueError, match="header is invalid"):
        gate._require_tracked_static_ready_coordinate(inputs, assessment=malformed)


def test_non_static_tracked_assessment_claim_fails_closed(tmp_path: Path) -> None:
    gate = _load_module()
    inputs = _inputs(tmp_path, gate=gate)
    assessment = copy.deepcopy(gate._TRACKED_CONFORMANCE_DATA)
    assessment["support_level_changes"] = True

    with pytest.raises(ValueError, match="header is invalid"):
        gate._require_tracked_static_ready_coordinate(inputs, assessment=assessment)


def test_stale_tracked_assessment_digest_fails_closed(tmp_path: Path) -> None:
    gate = _load_module()
    inputs = _inputs(tmp_path, gate=gate)
    assessment = copy.deepcopy(gate._TRACKED_CONFORMANCE_DATA)
    assessment["summary"]["ready_coordinate_count"] += 1

    with pytest.raises(ValueError, match="digest does not match"):
        gate._require_tracked_static_ready_coordinate(inputs, assessment=assessment)


def test_existing_evidence_fails_before_install(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate = _load_module()
    inputs = _inputs(tmp_path, gate=gate)
    inputs.evidence.write_text("existing", encoding="utf-8")

    def unexpected_install(*args: object, **kwargs: object) -> Any:
        message = "existing evidence reached installation"
        raise AssertionError(message)

    monkeypatch.setattr(gate.gate_harness, "install_wheel", unexpected_install)

    with pytest.raises(FileExistsError, match="overwrite"):
        gate.run_gate(inputs)


def test_recovery_mode_accepts_generated_319_full_coordinate(tmp_path: Path) -> None:
    gate = _load_module()
    inputs = _inputs(
        tmp_path,
        gate=gate,
        ds_version="3.1.9",
        bundle="full_core/v1",
    )
    recovery = tmp_path / "recovery.json"
    recovery.write_text(
        json.dumps({"schema_version": 1, "run_id": "0123456789abcdef"}),
        encoding="utf-8",
    )
    recovery.chmod(0o600)

    assert (
        gate._validated_recovery_run_id(replace(inputs, recovery_run_id_file=recovery))
        == "0123456789abcdef"
    )


@pytest.mark.parametrize(
    ("ds_version", "bundle"),
    [
        ("3.0.0", "full_core/v1"),
        ("2.0.9", "legacy_core/v1"),
        ("3.1.9", "legacy_core/v1"),
    ],
)
def test_recovery_mode_rejects_non_generated_coordinate_before_any_execution(
    tmp_path: Path,
    ds_version: str,
    bundle: str,
) -> None:
    gate = _load_module()
    inputs = _inputs(tmp_path, gate=gate, ds_version=ds_version, bundle=bundle)
    recovery = tmp_path / "recovery.json"
    recovery.write_text(
        json.dumps({"schema_version": 1, "run_id": "0123456789abcdef"}),
        encoding="utf-8",
    )
    recovery.chmod(0o600)
    inputs = replace(inputs, recovery_run_id_file=recovery)
    launches: list[object] = []

    def unexpected_launch(*args: object, **kwargs: object) -> Any:
        launches.append((args, kwargs))
        message = "invalid recovery coordinate launched a process"
        raise AssertionError(message)

    with pytest.raises(ValueError, match="Conformance recovery") as rejected:
        gate.run_gate(
            inputs,
            process_runner=unexpected_launch,
            install_wheel=unexpected_launch,
        )

    assert str(rejected.value) == (
        "Conformance recovery is only supported for "
        "2.0.0/full_core/v1, 2.0.1/full_core/v1, "
        "2.0.2/full_core/v1, 2.0.3/full_core/v1, 2.0.4/full_core/v1, "
        "2.0.5/full_core/v1, 2.0.6/full_core/v1, 2.0.7/full_core/v1, "
        "2.0.8/full_core/v1, 2.0.9/full_core/v1, 3.1.3/full_core/v1, "
        "3.1.4/full_core/v1, 3.1.5/full_core/v1, 3.1.6/full_core/v1, "
        "3.1.7/full_core/v1, 3.1.8/full_core/v1, 3.1.9/full_core/v1"
    )
    assert launches == []


@pytest.mark.parametrize(
    "payload",
    [
        '{"schema_version":1,"run_id":"0123456789abcdef","run_id":"fedcba9876543210"}',
        '{"schema_version":1,"run_id":"TOO-LOUD-RECOVERY"}',
        '{"schema_version":1,"run_id":"0123456789abcdef","extra":true}',
    ],
)
def test_recovery_identity_is_private_strict_exact_json_before_install(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    payload: str,
) -> None:
    gate = _load_module()
    inputs = _inputs(
        tmp_path,
        gate=gate,
        ds_version="2.0.9",
        bundle="full_core/v1",
    )
    recovery = tmp_path / "recovery.json"
    recovery.write_text(payload, encoding="utf-8")
    recovery.chmod(0o600)
    inputs = replace(inputs, recovery_run_id_file=recovery)
    launches: list[object] = []

    def unexpected_launch(*args: object, **kwargs: object) -> Any:
        launches.append((args, kwargs))
        message = "invalid recovery identity launched a process"
        raise AssertionError(message)

    monkeypatch.setattr(gate.gate_harness, "install_wheel", unexpected_launch)

    with pytest.raises((TypeError, ValueError)):
        gate.run_gate(inputs)

    assert launches == []


def test_missing_fixed_pytest_entry_fails_before_install(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate = _load_module()
    inputs = _inputs(tmp_path, gate=gate)
    launches: list[object] = []
    monkeypatch.setattr(
        gate,
        "_PYTEST_ENTRY_PATH",
        tmp_path / "missing-live-entry.py",
        raising=False,
    )

    def unexpected_launch(*args: object, **kwargs: object) -> Any:
        launches.append((args, kwargs))
        message = "missing pytest entry launched a process"
        raise AssertionError(message)

    monkeypatch.setattr(gate.gate_harness, "install_wheel", unexpected_launch)
    monkeypatch.setattr(gate.subprocess, "run", unexpected_launch)

    with pytest.raises(FileNotFoundError, match="fixed pytest entry"):
        gate.run_gate(inputs)

    assert launches == []


def test_snapshot_inputs_are_private_stable_bundle_bound_copies(
    tmp_path: Path,
) -> None:
    gate = _load_module()
    inputs = _inputs(tmp_path / "source", gate=gate)
    inputs.wheel.write_bytes(b"wheel-v1")

    snapshot = gate._snapshot_inputs(inputs, tmp_path / "workspace" / "inputs")
    inputs.wheel.write_bytes(b"wheel-v2")

    assert snapshot.ds_version == "3.2.2"
    assert snapshot.bundle == "legacy_core/v1"
    assert snapshot.wheel.read_bytes() == b"wheel-v1"
    assert snapshot.env_file.stat().st_mode & 0o077 == 0
    assert snapshot.evidence == inputs.evidence


def test_gate_environment_is_bundle_bound_and_removes_ambient_ds_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate = _load_module()
    inputs = _inputs(tmp_path, gate=gate)
    monkeypatch.setenv("PYTHONPATH", str(tmp_path / "source"))
    monkeypatch.setenv("DS_API_URL", "https://wrong.example")
    monkeypatch.setenv("DS_VERSION", "3.4.1")
    monkeypatch.setenv("DS_LIVE_CONFORMANCE_BUNDLE", "wrong")
    monkeypatch.setenv("NO_PROXY", "localhost")

    environment = gate._conformance_environment(
        inputs,
        executable=tmp_path / "venv" / "bin" / "dsctl",
        python=tmp_path / "venv" / "bin" / "python",
        attestation=tmp_path / "installed-attestation.json",
        candidate_evidence=tmp_path / "candidate-evidence.json",
    )

    assert "PYTHONPATH" not in environment
    assert "DS_API_URL" not in environment
    assert environment["DS_LIVE_CONFORMANCE_VERSION"] == "3.2.2"
    assert environment["DS_LIVE_CONFORMANCE_BUNDLE"] == "legacy_core/v1"
    assert environment["DS_LIVE_CONFORMANCE_DSCTL"].endswith("/venv/bin/dsctl")
    assert environment["NO_PROXY"] == "localhost,matrix.test"
    assert environment["no_proxy"] == "localhost,matrix.test"


def test_installed_probe_accepts_only_the_requested_ready_bundle_coordinate(
    tmp_path: Path,
) -> None:
    gate = _load_module()
    inputs = _inputs(tmp_path, gate=gate)
    coordinate = gate._require_tracked_static_ready_coordinate(inputs)
    installed = gate.gate_harness.InstalledWheel(
        python=tmp_path / "venv" / "bin" / "python",
        executable=tmp_path / "venv" / "bin" / "dsctl",
    )
    calls: list[tuple[list[str], dict[str, object]]] = []
    payload = _installed_probe_payload(
        tmp_path,
        bundle_digest=coordinate.bundle_digest,
        catalog_digest=coordinate.catalog_digest,
        assessment_digest=coordinate.assessment_digest,
    )
    _write_probe_wheel(
        inputs.wheel,
        assessment=gate._TRACKED_CONFORMANCE_DATA,
        manifest=payload["manifest"],
    )

    def fake_process(
        argv: list[str],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        calls.append((argv, kwargs))
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps(payload),
            stderr="",
        )

    attestation = gate.inspect_installed_conformance(
        inputs,
        installed=installed,
        coordinate=coordinate,
        process_runner=fake_process,
    )

    assert attestation.ds_version == "3.2.2"
    assert attestation.bundle == "legacy_core/v1"
    assert attestation.required_actions == (
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
    argv, kwargs = calls[0]
    assert argv[:2] == [str(installed.python), "-I"]
    assert argv[2:4] == ["-c", gate.INSTALLED_CONFORMANCE_PROBE_CODE]
    assert json.loads(argv[4]) == {
        "bundle": "legacy_core/v1",
        "ds_version": "3.2.2",
        "runtime_operations": sorted(
            gate.load_wheel_runtime_ownership(inputs.wheel)["3.2.2"]
        ),
        "task_cleanup_digest": gate._canonical_digest(gate._TRACKED_TASK_CLEANUP_DATA),
    }
    assert "set(cleanup_data) !=" not in gate.INSTALLED_CONFORMANCE_PROBE_CODE
    assert kwargs["cwd"] == installed.python.parent
    environment = kwargs["env"]
    assert isinstance(environment, dict)
    assert "PYTHONPATH" not in environment


def test_installed_probe_accepts_full_bundle_with_empty_semantic_root_set(
    tmp_path: Path,
) -> None:
    gate = _load_module()
    inputs = _inputs(
        tmp_path,
        gate=gate,
        ds_version="3.4.1",
        bundle="full_core/v1",
    )
    coordinate = gate._require_tracked_static_ready_coordinate(inputs)
    installed = gate.gate_harness.InstalledWheel(
        python=tmp_path / "venv" / "bin" / "python",
        executable=tmp_path / "venv" / "bin" / "dsctl",
    )
    payload = _installed_full_probe_payload(
        tmp_path,
        bundle_digest=coordinate.bundle_digest,
        catalog_digest=coordinate.catalog_digest,
        assessment_digest=coordinate.assessment_digest,
    )
    _write_probe_wheel(
        inputs.wheel,
        assessment=gate._TRACKED_CONFORMANCE_DATA,
        manifest=payload["manifest"],
    )

    def fake_process(
        argv: list[str],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps(payload),
            stderr="",
        )

    attestation = gate.inspect_installed_conformance(
        inputs,
        installed=installed,
        coordinate=coordinate,
        process_runner=fake_process,
    )

    assert attestation.manifest["selection"] == "full"
    assert attestation.manifest["semantic_operations"] == []
    assert attestation.manifest["operation_count"] == 298


@pytest.mark.parametrize("damage", [None, "missing_program", "changed_program"])
def test_zero_native_candidate_requires_its_own_verified_compiled_inventory(
    tmp_path: Path, damage: str | None
) -> None:
    gate = _load_module()
    inputs = _inputs(tmp_path, gate=gate)
    coordinate = gate._require_tracked_static_ready_coordinate(inputs)
    payload = _installed_probe_payload(
        tmp_path,
        bundle_digest=coordinate.bundle_digest,
        catalog_digest=coordinate.catalog_digest,
        assessment_digest=coordinate.assessment_digest,
    )
    manifest = payload["manifest"]
    assert isinstance(manifest, dict)
    manifest.update(semantic_operations=[], operation_count=0)
    program = "dsctl/generated/wire_programs/workflow_runtime.py"
    _write_probe_wheel(
        inputs.wheel,
        assessment=gate._TRACKED_CONFORMANCE_DATA,
        manifest=manifest,
        exclude=program if damage else None,
    )
    if damage == "changed_program":
        source = Path(__file__).resolve().parents[2] / "src" / program
        with zipfile.ZipFile(inputs.wheel, "a") as archive:
            archive.writestr(program, source.read_bytes() + b"\n# drift\n")
    launched = []

    def probe(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        launched.append(argv)
        return subprocess.CompletedProcess(argv, 0, stdout=json.dumps(payload))

    installed = gate.gate_harness.InstalledWheel(
        python=tmp_path / "venv" / "bin" / "python",
        executable=tmp_path / "venv" / "bin" / "dsctl",
    )
    if damage:
        with pytest.raises(ValueError, match=r"artifact inventory|artifact content"):
            gate.inspect_installed_conformance(
                inputs, installed=installed, coordinate=coordinate, process_runner=probe
            )
        assert launched == []
    else:
        result = gate.inspect_installed_conformance(
            inputs, installed=installed, coordinate=coordinate, process_runner=probe
        )
        assert result.manifest["operation_count"] == 0
        assert result.manifest["semantic_operations"] == []
        request = json.loads(launched[0][4])
        assert "workflow.get" in request["runtime_operations"]
        assert "runtime_operations" not in result.manifest


@pytest.mark.parametrize(
    ("version", "missing"),
    [
        ("3.2.2", None),
        ("3.2.2", "identity.current"),
        ("2.0.9", _TASK_DEFINITION_CLEANUP_ROOT),
    ],
)
def test_actual_installed_probe_enforces_verified_action_and_cleanup_owners(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    version: str,
    missing: str | None,
) -> None:
    gate = _load_module()
    wheel = tmp_path / "candidate.whl"
    _write_probe_wheel(
        wheel,
        assessment=gate._TRACKED_CONFORMANCE_DATA,
        manifest=_tracked_manifest(version),
    )
    owners = gate.load_wheel_runtime_ownership(wheel)[version]
    request = {
        "bundle": "legacy_core/v1",
        "ds_version": version,
        "task_cleanup_digest": gate._canonical_digest(gate._TRACKED_TASK_CLEANUP_DATA),
        "runtime_operations": sorted(owners - {missing} if missing else owners),
    }
    monkeypatch.setattr(sys, "argv", ["installed-probe", json.dumps(request)])
    code = compile(gate.INSTALLED_CONFORMANCE_PROBE_CODE, "installed-probe", "exec")
    if missing:
        with pytest.raises(AssertionError, match=r"semantic root|not executable"):
            exec(code, {"__name__": "__main__"})  # noqa: S102
    else:
        exec(code, {"__name__": "__main__"})  # noqa: S102
        payload = json.loads(capsys.readouterr().out)
        assert (
            payload["manifest"]["semantic_operations"]
            == _tracked_manifest(version)["semantic_operations"]
        )


@pytest.mark.parametrize("count", [-1, True])
def test_installed_manifest_rejects_invalid_zero_native_count(count: object) -> None:
    gate = _load_module()
    manifest = _tracked_manifest("3.2.2")
    manifest.update(semantic_operations=[], operation_count=count)
    with pytest.raises(AssertionError, match="operation count differs"):
        gate._validate_installed_manifest(manifest, ds_version="3.2.2")


def test_wheel_conformance_payload_rejects_safe_then_executable_duplicate(
    tmp_path: Path,
) -> None:
    gate = _load_module()
    inputs = _inputs(tmp_path, gate=gate)
    payload = _installed_probe_payload(
        tmp_path,
        bundle_digest="sha256:" + "1" * 64,
        catalog_digest="sha256:" + "2" * 64,
        assessment_digest="sha256:" + "3" * 64,
    )
    executed = tmp_path / "executed"
    malicious_duplicate = (
        "\n_CONFORMANCE_BUNDLE_JSON = "
        f"__import__('pathlib').Path({str(executed)!r}).write_text('secret')\n"
    )
    _write_probe_wheel(
        inputs.wheel,
        assessment=gate._TRACKED_CONFORMANCE_DATA,
        manifest=payload["manifest"],
        conformance_suffix=malicious_duplicate,
    )

    with pytest.raises(ValueError, match="more than once"):
        gate._load_wheel_conformance_data(inputs.wheel)

    assert not executed.exists()


def test_wheel_manifest_rejects_duplicate_literal_assignment(tmp_path: Path) -> None:
    gate = _load_module()
    inputs = _inputs(tmp_path, gate=gate)
    payload = _installed_probe_payload(
        tmp_path,
        bundle_digest="sha256:" + "1" * 64,
        catalog_digest="sha256:" + "2" * 64,
        assessment_digest="sha256:" + "3" * 64,
    )
    _write_probe_wheel(
        inputs.wheel,
        assessment=gate._TRACKED_CONFORMANCE_DATA,
        manifest=payload["manifest"],
        manifest_suffix="\nDS_VERSION = '3.2.2'\n",
    )

    with pytest.raises(ValueError, match="more than once"):
        gate._load_wheel_manifest(inputs.wheel, "3.2.2")


@pytest.mark.parametrize(
    "assignment",
    [
        "_CONFORMANCE_BUNDLE_JSON: str = {payload!r}",
        "(_CONFORMANCE_BUNDLE_JSON, ignored) = ({payload!r}, None)",
        "_CONFORMANCE_BUNDLE_JSON = make_payload()",
    ],
    ids=["annotated", "tuple", "call"],
)
def test_wheel_conformance_payload_requires_one_simple_literal_assignment(
    tmp_path: Path,
    assignment: str,
) -> None:
    gate = _load_module()
    inputs = _inputs(tmp_path, gate=gate)
    payload = _installed_probe_payload(
        tmp_path,
        bundle_digest="sha256:" + "1" * 64,
        catalog_digest="sha256:" + "2" * 64,
        assessment_digest="sha256:" + "3" * 64,
    )
    conformance_payload = json.dumps(gate._TRACKED_CONFORMANCE_DATA, sort_keys=True)
    _write_probe_wheel(
        inputs.wheel,
        assessment=gate._TRACKED_CONFORMANCE_DATA,
        manifest=payload["manifest"],
        conformance_source=assignment.format(payload=conformance_payload),
    )

    with pytest.raises(ValueError, match="simple literal assignment"):
        gate._load_wheel_conformance_data(inputs.wheel)


def test_runtime_slice_operation_count_is_not_the_semantic_root_count(
    tmp_path: Path,
) -> None:
    gate = _load_module()
    payload = _installed_probe_payload(
        tmp_path,
        bundle_digest="sha256:" + "1" * 64,
        catalog_digest="sha256:" + "2" * 64,
        assessment_digest="sha256:" + "3" * 64,
    )
    manifest = payload["manifest"]
    assert isinstance(manifest, dict)
    roots = manifest["semantic_operations"]
    assert isinstance(roots, list)
    original_count = manifest["operation_count"]
    original_roots = len(roots)
    roots.append("workflow.describe")

    validated = gate._validate_installed_manifest(manifest, ds_version="3.2.2")

    assert len(validated["semantic_operations"]) == original_roots + 1
    assert validated["operation_count"] == original_count


def test_installed_manifest_rejects_historical_bundle_schema() -> None:
    gate = _load_module()
    manifest = _installed_probe_payload(
        Path("unused"),
        bundle_digest="sha256:" + "1" * 64,
        catalog_digest="sha256:" + "2" * 64,
        assessment_digest="sha256:" + "3" * 64,
    )["manifest"]
    assert isinstance(manifest, dict)
    manifest["bundle_manifest_schema_version"] = 1

    with pytest.raises(
        AssertionError,
        match="does not match requested exact version",
    ):
        gate._validate_installed_manifest(manifest, ds_version="3.2.2")


def test_installed_attestation_requires_verified_private_cleanup_root_for_exact_six(
    tmp_path: Path,
) -> None:
    gate = _load_module()
    inputs = _inputs(tmp_path, gate=gate, ds_version="2.0.9", bundle="full_core/v1")
    coordinate = gate._require_tracked_static_ready_coordinate(inputs)
    payload = _installed_recovery_full_probe_payload(
        tmp_path,
        ds_version="2.0.9",
        bundle_digest=coordinate.bundle_digest,
        catalog_digest=coordinate.catalog_digest,
        assessment_digest=coordinate.assessment_digest,
    )
    _write_probe_wheel(
        inputs.wheel,
        assessment=gate._TRACKED_CONFORMANCE_DATA,
        manifest=payload["manifest"],
    )
    owners = gate.load_wheel_runtime_ownership(inputs.wheel)["2.0.9"]
    installed = gate.gate_harness.InstalledWheel(
        python=tmp_path / "venv" / "bin" / "python",
        executable=tmp_path / "venv" / "bin" / "dsctl",
    )
    with pytest.raises(AssertionError, match="private cleanup semantic root"):
        gate._validate_installed_attestation(
            payload,
            inputs=inputs,
            installed=installed,
            coordinate=coordinate,
            runtime_operations=owners - {_TASK_DEFINITION_CLEANUP_ROOT},
        )
    gate._validate_installed_attestation(
        payload,
        inputs=inputs,
        installed=installed,
        coordinate=coordinate,
        runtime_operations=owners,
    )


def test_installed_attestation_forbids_private_cleanup_root_outside_exact_six(
    tmp_path: Path,
) -> None:
    gate = _load_module()
    inputs = _inputs(tmp_path, gate=gate)
    coordinate = gate._require_tracked_static_ready_coordinate(inputs)
    payload = _installed_probe_payload(
        tmp_path,
        bundle_digest=coordinate.bundle_digest,
        catalog_digest=coordinate.catalog_digest,
        assessment_digest=coordinate.assessment_digest,
    )
    installed = gate.gate_harness.InstalledWheel(
        python=tmp_path / "venv" / "bin" / "python",
        executable=tmp_path / "venv" / "bin" / "dsctl",
    )
    with pytest.raises(AssertionError, match="private cleanup semantic root"):
        gate._validate_installed_attestation(
            payload,
            inputs=inputs,
            installed=installed,
            coordinate=coordinate,
            runtime_operations=frozenset({_TASK_DEFINITION_CLEANUP_ROOT}),
        )


def test_candidate_wheel_private_cleanup_profile_is_bound_to_tracked_truth(
    tmp_path: Path,
) -> None:
    gate = _load_module()
    inputs = _inputs(tmp_path, gate=gate)
    coordinate = gate._require_tracked_static_ready_coordinate(inputs)
    payload = _installed_probe_payload(
        tmp_path,
        bundle_digest=coordinate.bundle_digest,
        catalog_digest=coordinate.catalog_digest,
        assessment_digest=coordinate.assessment_digest,
    )
    cleanup_data = copy.deepcopy(gate._TRACKED_TASK_CLEANUP_DATA)
    cleanup_data["full_core_versions"] = ["2.0.9"]
    _write_probe_wheel(
        inputs.wheel,
        assessment=gate._TRACKED_CONFORMANCE_DATA,
        manifest=payload["manifest"],
        cleanup_source=(
            "_TASK_DEFINITION_CLEANUP_PROFILE_JSON = "
            + repr(json.dumps(cleanup_data, sort_keys=True))
            + "\n"
        ),
    )
    installed = gate.gate_harness.InstalledWheel(
        python=tmp_path / "venv" / "bin" / "python",
        executable=tmp_path / "venv" / "bin" / "dsctl",
    )

    with pytest.raises(AssertionError, match="tracked generated truth"):
        gate.inspect_installed_conformance(
            inputs,
            installed=installed,
            coordinate=coordinate,
        )


def test_installed_probe_rejects_wheel_archive_conformance_drift(
    tmp_path: Path,
) -> None:
    gate = _load_module()
    inputs = _inputs(tmp_path, gate=gate)
    coordinate = gate._require_tracked_static_ready_coordinate(inputs)
    installed = gate.gate_harness.InstalledWheel(
        python=tmp_path / "venv" / "bin" / "python",
        executable=tmp_path / "venv" / "bin" / "dsctl",
    )
    payload = _installed_probe_payload(
        tmp_path,
        bundle_digest=coordinate.bundle_digest,
        catalog_digest=coordinate.catalog_digest,
        assessment_digest=coordinate.assessment_digest,
    )
    drifted = copy.deepcopy(gate._TRACKED_CONFORMANCE_DATA)
    drifted["assessment_digest"] = "sha256:" + "f" * 64
    _write_probe_wheel(
        inputs.wheel,
        assessment=drifted,
        manifest=payload["manifest"],
    )

    def fake_process(
        argv: list[str],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps(payload),
            stderr="",
        )

    with pytest.raises(AssertionError, match="wheel archive conformance"):
        gate.inspect_installed_conformance(
            inputs,
            installed=installed,
            coordinate=coordinate,
            process_runner=fake_process,
        )


@pytest.mark.parametrize(
    "tamper",
    [
        "coordinate",
        "required-actions",
        "bundle-digest",
        "profile-support",
        "action-fingerprint",
    ],
)
def test_installed_probe_fails_closed_on_coordinate_or_recipe_drift(
    tmp_path: Path,
    tamper: str,
) -> None:
    gate = _load_module()
    inputs = _inputs(tmp_path, gate=gate)
    coordinate = gate._require_tracked_static_ready_coordinate(inputs)
    installed = gate.gate_harness.InstalledWheel(
        python=tmp_path / "venv" / "bin" / "python",
        executable=tmp_path / "venv" / "bin" / "dsctl",
    )
    payload = _installed_probe_payload(
        tmp_path,
        bundle_digest=coordinate.bundle_digest,
        catalog_digest=coordinate.catalog_digest,
        assessment_digest=coordinate.assessment_digest,
    )
    _write_probe_wheel(
        inputs.wheel,
        assessment=gate._TRACKED_CONFORMANCE_DATA,
        manifest=payload["manifest"],
    )
    if tamper == "coordinate":
        payload["coordinate_status"] = "blocked"
    elif tamper == "required-actions":
        payload["required_actions"] = list(coordinate.required_actions[:-1])
    elif tamper == "bundle-digest":
        payload["bundle_digest"] = "sha256:" + "f" * 64
    elif tamper == "profile-support":
        profile = payload["profile"]
        assert isinstance(profile, dict)
        profile["support_level"] = "full"
        profile["tested"] = True
    else:
        actions = payload["actions"]
        assert isinstance(actions, list)
        fingerprints = actions[0]["fingerprints"]
        assert isinstance(fingerprints, dict)
        fingerprints["source"] = "not-a-digest"

    def fake_process(
        argv: list[str],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps(payload),
            stderr="",
        )

    with pytest.raises((AssertionError, ValueError)):
        gate.inspect_installed_conformance(
            inputs,
            installed=installed,
            coordinate=coordinate,
            process_runner=fake_process,
        )


def test_run_gate_uses_fixed_pytest_entry_and_validates_before_private_publish(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate = _load_module()
    monkeypatch.setattr(gate, "_PYTEST_ENTRY_PATH", Path(__file__))
    inputs = _inputs(tmp_path, gate=gate)
    coordinate = gate._require_tracked_static_ready_coordinate(inputs)
    base_payload = _installed_probe_payload(
        tmp_path,
        bundle_digest=coordinate.bundle_digest,
        catalog_digest=coordinate.catalog_digest,
        assessment_digest=coordinate.assessment_digest,
    )
    _write_probe_wheel(
        inputs.wheel,
        assessment=gate._TRACKED_CONFORMANCE_DATA,
        manifest=base_payload["manifest"],
    )
    wheel_sha256 = "sha256:" + hashlib.sha256(inputs.wheel.read_bytes()).hexdigest()
    installed_wheels: list[Path] = []
    process_calls: list[list[str]] = []
    validator_calls: list[tuple[object, dict[str, object]]] = []

    def fake_install(
        wheel: Path,
        workspace: Path,
        **kwargs: object,
    ) -> Any:
        installed_wheels.append(wheel)
        return gate.gate_harness.InstalledWheel(
            python=workspace / "venv" / "bin" / "python",
            executable=workspace / "venv" / "bin" / "dsctl",
        )

    def fake_process(
        argv: list[str],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        process_calls.append(argv)
        if argv[1] == "-I":
            probe_root = Path(argv[0]).parents[2]
            payload = _installed_probe_payload(
                probe_root,
                bundle_digest=coordinate.bundle_digest,
                catalog_digest=coordinate.catalog_digest,
                assessment_digest=coordinate.assessment_digest,
            )
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=json.dumps(payload),
                stderr="",
            )
        assert argv == [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "tests/live/test_exact_conformance_bundle.py",
            "-m",
            "live_exact_conformance",
        ]
        environment = kwargs["env"]
        assert isinstance(environment, dict)
        assert Path(environment["DS_LIVE_CONFORMANCE_WHEEL"]) != inputs.wheel
        assert Path(environment["DS_LIVE_CONFORMANCE_ENV_FILE"]) != inputs.env_file
        attestation_path = Path(
            environment["DS_LIVE_CONFORMANCE_INSTALLED_ATTESTATION"]
        )
        assert attestation_path.stat().st_mode & 0o077 == 0
        attestation_data = json.loads(attestation_path.read_text(encoding="utf-8"))
        assert attestation_data["bundle"] == "legacy_core/v1"
        candidate = Path(environment["DS_LIVE_CONFORMANCE_EVIDENCE"])
        candidate.write_text('{ "status" : "passed" }', encoding="utf-8")
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    def fake_validator(receipt: object, **kwargs: object) -> object:
        assert not inputs.evidence.exists()
        validator_calls.append((receipt, kwargs))
        return object()

    result = gate.run_gate(
        inputs,
        process_runner=fake_process,
        install_wheel=fake_install,
        evidence_validator=fake_validator,
    )

    assert result == 0
    assert len(process_calls) == 2
    assert installed_wheels[0] != inputs.wheel
    assert not installed_wheels[0].exists()
    assert validator_calls == [
        (
            {"status": "passed"},
            {
                "expected_ds_version": "3.2.2",
                "expected_bundle": "legacy_core/v1",
                "expected_wheel_filename": "runtime.whl",
                "expected_wheel_sha256": wheel_sha256,
            },
        )
    ]
    assert json.loads(inputs.evidence.read_text(encoding="utf-8")) == {
        "status": "passed"
    }
    assert inputs.evidence.read_text(encoding="utf-8") == (
        '{\n  "status": "passed"\n}\n'
    )
    assert inputs.evidence.stat().st_mode & 0o077 == 0


@pytest.mark.parametrize("failure", ["nonzero", "baseexception"])
def test_run_id_is_private_and_persists_when_pytest_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    gate = _load_module()
    monkeypatch.setattr(gate, "_PYTEST_ENTRY_PATH", Path(__file__))
    inputs = _inputs(tmp_path, gate=gate, ds_version="2.0.0", bundle="full_core/v1")
    run_id_path = tmp_path / "run-id.json"
    inputs = replace(inputs, run_id_output_file=run_id_path)
    coordinate = gate._require_tracked_static_ready_coordinate(inputs)
    payload = _installed_recovery_full_probe_payload(
        tmp_path,
        ds_version=inputs.ds_version,
        bundle_digest=coordinate.bundle_digest,
        catalog_digest=coordinate.catalog_digest,
        assessment_digest=coordinate.assessment_digest,
    )
    _write_probe_wheel(
        inputs.wheel,
        assessment=gate._TRACKED_CONFORMANCE_DATA,
        manifest=payload["manifest"],
    )
    pytest_launches = 0

    def fake_install(wheel: Path, workspace: Path, **kwargs: object) -> Any:
        return gate.gate_harness.InstalledWheel(
            python=workspace / "venv" / "bin" / "python",
            executable=workspace / "venv" / "bin" / "dsctl",
        )

    def fake_process(
        argv: list[str], **kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        nonlocal pytest_launches
        if argv[1] == "-I":
            probed = copy.deepcopy(payload)
            probed["module_file"] = str(
                Path(argv[0]).parents[2]
                / "venv/lib/python3.12/site-packages/dsctl/__init__.py"
            )
            return subprocess.CompletedProcess(
                argv, 0, stdout=json.dumps(probed), stderr=""
            )
        pytest_launches += 1
        environment = kwargs["env"]
        assert isinstance(environment, dict)
        assert environment["DS_LIVE_CONFORMANCE_RUN_ID_FILE"] == str(run_id_path)
        assert run_id_path.is_file()
        assert run_id_path.stat().st_mode & 0o077 == 0
        if failure == "baseexception":
            raise KeyboardInterrupt
        return subprocess.CompletedProcess(argv, 1, stdout="", stderr="")

    if failure == "baseexception":
        with pytest.raises(KeyboardInterrupt):
            gate.run_gate(
                inputs, process_runner=fake_process, install_wheel=fake_install
            )
    else:
        assert (
            gate.run_gate(
                inputs, process_runner=fake_process, install_wheel=fake_install
            )
            == 1
        )
    assert pytest_launches == 1
    identity = json.loads(run_id_path.read_text(encoding="utf-8"))
    assert identity["schema_version"] == 1
    assert gate._RECOVERY_RUN_ID.fullmatch(identity["run_id"])
    assert not inputs.evidence.exists()


def test_existing_or_nonprivate_run_id_output_fails_before_install(
    tmp_path: Path,
) -> None:
    gate = _load_module()
    inputs = _inputs(tmp_path, gate=gate, ds_version="2.0.0", bundle="full_core/v1")
    output = tmp_path / "run-id.json"
    output.write_text("unchanged", encoding="utf-8")
    launches: list[object] = []

    def unexpected_launch(*args: object, **kwargs: object) -> Any:
        launches.append((args, kwargs))
        message = "run-id collision reached installation"
        raise AssertionError(message)

    with pytest.raises(FileExistsError, match="overwrite"):
        gate.run_gate(
            replace(inputs, run_id_output_file=output), install_wheel=unexpected_launch
        )
    assert output.read_text(encoding="utf-8") == "unchanged"
    assert launches == []

    real = tmp_path / "real"
    private = real / "private"
    private.mkdir(parents=True, mode=0o700)
    alias = tmp_path / "alias"
    alias.symlink_to(real, target_is_directory=True)
    with pytest.raises(ValueError, match="differ from evidence"):
        gate.run_gate(
            replace(
                inputs,
                evidence=private / "run-id.json",
                run_id_output_file=alias / "private" / "run-id.json",
            ),
            install_wheel=unexpected_launch,
        )
    assert launches == []

    output.unlink()
    public_parent = tmp_path / "public"
    public_parent.mkdir(mode=0o755)
    public_parent.chmod(0o755)
    with pytest.raises(PermissionError, match="owner-private"):
        gate.run_gate(
            replace(inputs, run_id_output_file=public_parent / "run-id.json"),
            install_wheel=unexpected_launch,
        )
    assert launches == []


@pytest.mark.parametrize("ds_version", _RECOVERY_VERSIONS)
def test_recovery_mode_uses_installed_wheel_and_never_publishes_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    ds_version: str,
) -> None:
    gate = _load_module()
    monkeypatch.setattr(gate, "_PYTEST_ENTRY_PATH", Path(__file__))
    inputs = _inputs(
        tmp_path,
        gate=gate,
        ds_version=ds_version,
        bundle="full_core/v1",
    )
    recovery = tmp_path / "recovery.json"
    recovery.write_text(
        json.dumps({"schema_version": 1, "run_id": "0123456789abcdef"}),
        encoding="utf-8",
    )
    recovery.chmod(0o600)
    inputs = replace(inputs, recovery_run_id_file=recovery)
    coordinate = gate._require_tracked_static_ready_coordinate(inputs)
    payload = _installed_recovery_full_probe_payload(
        tmp_path,
        ds_version=ds_version,
        bundle_digest=coordinate.bundle_digest,
        catalog_digest=coordinate.catalog_digest,
        assessment_digest=coordinate.assessment_digest,
    )
    _write_probe_wheel(
        inputs.wheel,
        assessment=gate._TRACKED_CONFORMANCE_DATA,
        manifest=payload["manifest"],
    )
    process_calls: list[list[str]] = []

    def fake_install(wheel: Path, workspace: Path, **kwargs: object) -> Any:
        return gate.gate_harness.InstalledWheel(
            python=workspace / "venv" / "bin" / "python",
            executable=workspace / "venv" / "bin" / "dsctl",
        )

    def fake_process(
        argv: list[str],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        process_calls.append(argv)
        if argv[1] == "-I":
            probe_root = Path(argv[0]).parents[2]
            probed = copy.deepcopy(payload)
            probed["module_file"] = str(
                probe_root
                / "venv"
                / "lib"
                / "python3.12"
                / "site-packages"
                / "dsctl"
                / "__init__.py"
            )
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=json.dumps(probed),
                stderr="",
            )
        environment = kwargs["env"]
        assert isinstance(environment, dict)
        recovery_snapshot = Path(
            environment["DS_LIVE_CONFORMANCE_RECOVERY_RUN_ID_FILE"]
        )
        assert recovery_snapshot != recovery
        assert recovery_snapshot.stat().st_mode & 0o077 == 0
        assert json.loads(recovery_snapshot.read_text(encoding="utf-8")) == {
            "schema_version": 1,
            "run_id": "0123456789abcdef",
        }
        assert not Path(environment["DS_LIVE_CONFORMANCE_EVIDENCE"]).exists()
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    def unexpected_validator(receipt: object, **kwargs: object) -> object:
        message = "recovery attempted to validate evidence"
        raise AssertionError(message)

    assert (
        gate.run_gate(
            inputs,
            process_runner=fake_process,
            install_wheel=fake_install,
            evidence_validator=unexpected_validator,
        )
        == 0
    )

    assert len(process_calls) == 2
    assert not inputs.evidence.exists()


@pytest.mark.parametrize(
    "candidate_text",
    [
        ('{"status":"https://duplicate.invalid/secret-token","status":"passed"}'),
        (
            '{"status":"passed","runner":{'
            '"artifact":"https://duplicate.invalid/secret-token",'
            '"artifact":"installed-wheel-console-script"}}'
        ),
    ],
    ids=["top-level", "nested"],
)
def test_run_gate_rejects_duplicate_candidate_keys_without_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    candidate_text: str,
) -> None:
    gate = _load_module()
    monkeypatch.setattr(gate, "_PYTEST_ENTRY_PATH", Path(__file__))
    inputs = _inputs(tmp_path, gate=gate)
    coordinate = gate._require_tracked_static_ready_coordinate(inputs)
    probe_payload = _installed_probe_payload(
        tmp_path,
        bundle_digest=coordinate.bundle_digest,
        catalog_digest=coordinate.catalog_digest,
        assessment_digest=coordinate.assessment_digest,
    )
    _write_probe_wheel(
        inputs.wheel,
        assessment=gate._TRACKED_CONFORMANCE_DATA,
        manifest=probe_payload["manifest"],
    )
    validator_calls: list[object] = []

    def fake_install(
        wheel: Path,
        workspace: Path,
        **kwargs: object,
    ) -> Any:
        return gate.gate_harness.InstalledWheel(
            python=workspace / "venv" / "bin" / "python",
            executable=workspace / "venv" / "bin" / "dsctl",
        )

    def fake_process(
        argv: list[str],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        if argv[1] == "-I":
            payload = _installed_probe_payload(
                Path(argv[0]).parents[2],
                bundle_digest=coordinate.bundle_digest,
                catalog_digest=coordinate.catalog_digest,
                assessment_digest=coordinate.assessment_digest,
            )
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=json.dumps(payload),
                stderr="",
            )
        environment = kwargs["env"]
        assert isinstance(environment, dict)
        Path(environment["DS_LIVE_CONFORMANCE_EVIDENCE"]).write_text(
            candidate_text,
            encoding="utf-8",
        )
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    def fake_validator(receipt: object, **kwargs: object) -> object:
        validator_calls.append(receipt)
        return object()

    with pytest.raises(ValueError, match="duplicate JSON key"):
        gate.run_gate(
            inputs,
            process_runner=fake_process,
            install_wheel=fake_install,
            evidence_validator=fake_validator,
        )

    assert validator_calls == []
    assert not inputs.evidence.exists()


@pytest.mark.parametrize(
    ("mode", "expected_error"),
    [
        ("pytest-failed", None),
        ("candidate-missing", RuntimeError),
        ("validator-rejected", ValueError),
    ],
)
def test_run_gate_never_publishes_failed_or_unvalidated_candidate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    expected_error: type[Exception] | None,
) -> None:
    gate = _load_module()
    monkeypatch.setattr(gate, "_PYTEST_ENTRY_PATH", Path(__file__))
    inputs = _inputs(tmp_path, gate=gate)
    coordinate = gate._require_tracked_static_ready_coordinate(inputs)
    base_payload = _installed_probe_payload(
        tmp_path,
        bundle_digest=coordinate.bundle_digest,
        catalog_digest=coordinate.catalog_digest,
        assessment_digest=coordinate.assessment_digest,
    )
    _write_probe_wheel(
        inputs.wheel,
        assessment=gate._TRACKED_CONFORMANCE_DATA,
        manifest=base_payload["manifest"],
    )
    validator_calls: list[object] = []

    def fake_install(
        wheel: Path,
        workspace: Path,
        **kwargs: object,
    ) -> Any:
        return gate.gate_harness.InstalledWheel(
            python=workspace / "venv" / "bin" / "python",
            executable=workspace / "venv" / "bin" / "dsctl",
        )

    def fake_process(
        argv: list[str],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        if argv[1] == "-I":
            payload = _installed_probe_payload(
                Path(argv[0]).parents[2],
                bundle_digest=coordinate.bundle_digest,
                catalog_digest=coordinate.catalog_digest,
                assessment_digest=coordinate.assessment_digest,
            )
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=json.dumps(payload),
                stderr="",
            )
        environment = kwargs["env"]
        assert isinstance(environment, dict)
        if mode != "candidate-missing":
            Path(environment["DS_LIVE_CONFORMANCE_EVIDENCE"]).write_text(
                json.dumps({"status": "passed"}),
                encoding="utf-8",
            )
        return subprocess.CompletedProcess(
            argv,
            7 if mode == "pytest-failed" else 0,
            stdout="",
            stderr="",
        )

    def fake_validator(receipt: object, **kwargs: object) -> object:
        validator_calls.append(receipt)
        if mode == "validator-rejected":
            message = "candidate receipt rejected"
            raise ValueError(message)
        return object()

    if expected_error is None:
        assert (
            gate.run_gate(
                inputs,
                process_runner=fake_process,
                install_wheel=fake_install,
                evidence_validator=fake_validator,
            )
            == 7
        )
    else:
        with pytest.raises(expected_error):
            gate.run_gate(
                inputs,
                process_runner=fake_process,
                install_wheel=fake_install,
                evidence_validator=fake_validator,
            )

    assert not inputs.evidence.exists()
    assert bool(validator_calls) is (mode == "validator-rejected")


@pytest.mark.parametrize("ds_version", ["1.3.9", "2.0.0"])
def test_blocked_full_core_coordinate_fails_before_install_or_test_launch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    ds_version: str,
) -> None:
    gate = _load_module()
    inputs = _inputs(
        tmp_path,
        gate=gate,
        ds_version=ds_version,
        bundle="full_core/v1",
    )
    assessment = copy.deepcopy(gate._TRACKED_CONFORMANCE_DATA)
    full_core = next(
        item for item in assessment["bundles"] if item["name"] == "full_core/v1"
    )
    coordinate = next(
        item for item in full_core["versions"] if item["version"] == ds_version
    )
    coordinate["status"] = "blocked"
    coordinate["blockers"] = [
        {
            "action": "workflow.create",
            "availability": "limited",
            "build_status": "terminal",
            "constraint": "synthetic preflight blocker",
            "semantic_operation": "workflow.create",
        }
    ]
    assessment["summary"]["ready_coordinate_count"] -= 1
    assessment["summary"]["blocked_coordinate_count"] += 1
    _refresh_assessment_digest(assessment)
    monkeypatch.setattr(gate, "_TRACKED_CONFORMANCE_DATA", assessment)
    launches: list[object] = []

    def unexpected_launch(*args: object, **kwargs: object) -> Any:
        launches.append((args, kwargs))
        message = "blocked coordinate launched a process"
        raise AssertionError(message)

    monkeypatch.setattr(gate.gate_harness, "install_wheel", unexpected_launch)
    monkeypatch.setattr(gate.subprocess, "run", unexpected_launch)

    with pytest.raises(ValueError, match="not static-ready"):
        gate.run_gate(inputs)

    assert launches == []


def _inputs(
    tmp_path: Path,
    *,
    gate: Any,
    ds_version: str = "3.2.2",
    bundle: str = "legacy_core/v1",
) -> Any:
    tmp_path.mkdir(parents=True, exist_ok=True)
    wheel = tmp_path / "runtime.whl"
    env_file = tmp_path / "cluster.env"
    attestation_key_file = tmp_path / "attestation.key"
    cluster_manifest = tmp_path / "cluster.json"
    fixture_manifest = tmp_path / "fixture.json"
    wheel.touch()
    env_file.write_text(
        "DS_API_URL=http://matrix.test/dolphinscheduler\n"
        "DS_API_TOKEN=secret-token\n"
        f"DS_VERSION={ds_version}\n",
        encoding="utf-8",
    )
    attestation_key_file.write_text("k" * 32, encoding="utf-8")
    cluster_manifest.write_text("{}", encoding="utf-8")
    fixture_manifest.write_text("{}", encoding="utf-8")
    env_file.chmod(0o600)
    attestation_key_file.chmod(0o600)
    return gate.ConformanceGateInputs(
        ds_version=ds_version,
        bundle=bundle,
        wheel=wheel,
        env_file=env_file,
        attestation_key_file=attestation_key_file,
        cluster_manifest=cluster_manifest,
        fixture_manifest=fixture_manifest,
        evidence=tmp_path / "evidence.json",
    )


def _installed_probe_payload(
    tmp_path: Path,
    *,
    bundle_digest: str,
    catalog_digest: str,
    assessment_digest: str,
) -> dict[str, object]:
    actions = (
        ("doctor", "identity.current", "diagnostic_recipe", "static"),
        ("project.create", "project.create", "generated_adapter", "contract_tested"),
        ("project.delete", "project.delete", "generated_adapter", "contract_tested"),
        ("project.get", "project.get", "generated_adapter", "live_smoke"),
        ("project.list", "project.page", "generated_adapter", "live_smoke"),
        ("project.update", "project.update", "generated_adapter", "contract_tested"),
        ("schedule.list", "schedule.page", "generated_adapter", "contract_tested"),
        ("workflow.get", "workflow.get", "generated_adapter", "live_smoke"),
        ("workflow.list", "workflow.page", "generated_adapter", "live_smoke"),
    )
    fingerprints = {
        "source": "sha256:" + "1" * 64,
        "effective_wire": "sha256:" + "2" * 64,
        "consumed_projection": "sha256:" + "3" * 64,
        "preservation": "sha256:" + "4" * 64,
    }
    manifest = _tracked_manifest("3.2.2")
    return {
        "distribution_version": "0.4.0",
        "module_file": str(
            tmp_path
            / "venv"
            / "lib"
            / "python3.12"
            / "site-packages"
            / "dsctl"
            / "__init__.py"
        ),
        "ds_version": "3.2.2",
        "bundle": "legacy_core/v1",
        "coordinate_status": "ready",
        "blockers": [],
        "required_actions": [action for action, *_rest in actions],
        "bundle_digest": bundle_digest,
        "catalog_digest": catalog_digest,
        "assessment_digest": assessment_digest,
        "profile": {
            "server_version": "3.2.2",
            "contract_version": "3.2.2",
            "family": "process-definition-3.2",
            "support_level": "experimental",
            "tested": False,
            "source": {
                "tag": "3.2.2",
                "commit": manifest["source_commit"],
                "tree": manifest["source_tree"],
            },
            "fingerprints": fingerprints,
        },
        "actions": [
            {
                "action": action,
                "availability": "supported",
                "execution_mode": mode,
                "verification": verification,
                "semantic_operation": operation,
                "build_status": "accepted",
                "fingerprints": fingerprints,
            }
            for action, operation, mode, verification in actions
        ],
        "manifest": manifest,
    }


def _installed_full_probe_payload(
    tmp_path: Path,
    *,
    bundle_digest: str,
    catalog_digest: str,
    assessment_digest: str,
) -> dict[str, object]:
    payload = _installed_probe_payload(
        tmp_path,
        bundle_digest=bundle_digest,
        catalog_digest=catalog_digest,
        assessment_digest=assessment_digest,
    )
    required_actions = (
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
    operations = {
        "doctor": "identity.current",
        "project.list": "project.page",
        "schedule.list": "schedule.page",
        "workflow.list": "workflow.page",
    }
    profile = payload["profile"]
    manifest = payload["manifest"]
    assert isinstance(profile, dict)
    assert isinstance(manifest, dict)
    manifest.update(_tracked_manifest("3.4.1"))
    fingerprints = profile["fingerprints"]
    assert isinstance(fingerprints, dict)
    payload.update(
        {
            "ds_version": "3.4.1",
            "bundle": "full_core/v1",
            "required_actions": list(required_actions),
            "actions": [
                {
                    "action": action,
                    "availability": "supported",
                    "execution_mode": (
                        "diagnostic_recipe"
                        if action == "doctor"
                        else "generated_adapter"
                    ),
                    "verification": "live_smoke",
                    "semantic_operation": operations.get(action, action),
                    "build_status": "accepted",
                    "fingerprints": fingerprints,
                }
                for action in required_actions
            ],
        }
    )
    profile.update(
        {
            "server_version": "3.4.1",
            "contract_version": "3.4.1",
            "family": "workflow-3.3-plus",
            "support_level": "full",
            "tested": True,
            "source": {
                "tag": "3.4.1",
                "commit": manifest["source_commit"],
                "tree": manifest["source_tree"],
            },
        }
    )
    manifest.update(
        {
            "ds_version": "3.4.1",
            "selection": "full",
            "semantic_operations": [],
            "source_tag": "3.4.1",
            "operation_count": 298,
        }
    )
    return payload


def _installed_recovery_full_probe_payload(
    tmp_path: Path,
    *,
    ds_version: str,
    bundle_digest: str,
    catalog_digest: str,
    assessment_digest: str,
) -> dict[str, object]:
    payload = _installed_full_probe_payload(
        tmp_path,
        bundle_digest=bundle_digest,
        catalog_digest=catalog_digest,
        assessment_digest=assessment_digest,
    )
    profile = payload["profile"]
    manifest = payload["manifest"]
    actions = payload["actions"]
    assert isinstance(profile, dict)
    assert isinstance(manifest, dict)
    assert isinstance(actions, list)
    manifest.update(_tracked_manifest(ds_version))
    family = VERSION_PROFILES[ds_version]["family"]
    payload["ds_version"] = ds_version
    profile.update(
        {
            "server_version": ds_version,
            "contract_version": ds_version,
            "family": family,
            "support_level": "experimental",
            "tested": False,
            "source": {
                "tag": ds_version,
                "commit": manifest["source_commit"],
                "tree": manifest["source_tree"],
            },
        }
    )
    return payload


def _tracked_manifest(ds_version: str) -> dict[str, object]:
    manifest = importlib.import_module(
        f"dsctl.generated.versions.ds_{ds_version.replace('.', '_')}._manifest"
    )
    fields = (
        "BUNDLE_MANIFEST_SCHEMA_VERSION",
        "DS_VERSION",
        "SELECTION",
        "SOURCE_TAG",
        "SOURCE_COMMIT",
        "SOURCE_TREE",
        "SOURCE_CONTRACT_DIGEST",
        "RENDERED_CONTRACT_DIGEST",
        "OPERATION_COUNT",
    )
    return {
        **{name.lower(): getattr(manifest, name) for name in fields},
        "semantic_operations": list(manifest.SEMANTIC_OPERATIONS),
    }


def _write_probe_wheel(
    path: Path,
    *,
    assessment: object,
    manifest: object,
    conformance_source: str | None = None,
    conformance_suffix: str = "",
    manifest_suffix: str = "",
    cleanup_source: str | None = None,
    exclude: str | None = None,
) -> None:
    assert isinstance(manifest, dict)
    manifest_names = {
        "bundle_manifest_schema_version": "BUNDLE_MANIFEST_SCHEMA_VERSION",
        "ds_version": "DS_VERSION",
        "selection": "SELECTION",
        "semantic_operations": "SEMANTIC_OPERATIONS",
        "source_tag": "SOURCE_TAG",
        "source_commit": "SOURCE_COMMIT",
        "source_tree": "SOURCE_TREE",
        "source_contract_digest": "SOURCE_CONTRACT_DIGEST",
        "rendered_contract_digest": "RENDERED_CONTRACT_DIGEST",
        "operation_count": "OPERATION_COUNT",
    }
    rendered_manifest = "\n".join(
        f"{python_name} = {tuple(value)!r}"
        if field == "semantic_operations"
        else f"{python_name} = {value!r}"
        for field, python_name in manifest_names.items()
        for value in [manifest[field]]
    )
    if conformance_source is None:
        conformance_source = (
            "_CONFORMANCE_BUNDLE_JSON = r'''\n"
            + json.dumps(assessment, sort_keys=True)
            + "\n'''\n"
        )
    with zipfile.ZipFile(path, "w") as archive:
        generated = Path(__file__).resolve().parents[2] / "src" / "dsctl" / "generated"
        target_manifest = (
            "dsctl/generated/versions/ds_"
            + str(manifest["ds_version"]).replace(".", "_")
            + "/_manifest.py"
        )
        sources = [
            *generated.glob("versions/ds_*/_manifest.py"),
            *generated.glob("wire_programs/**/*.py"),
            *generated.glob("wire_runtime/**/*.py"),
        ]
        for source in sources:
            name = "dsctl/generated/" + source.relative_to(generated).as_posix()
            if name not in {target_manifest, exclude}:
                archive.writestr(name, source.read_bytes())
        archive.writestr(
            "dsctl/generated/conformance_bundles.py",
            conformance_source + conformance_suffix,
        )
        archive.writestr(
            "dsctl/generated/versions/ds_"
            + str(manifest["ds_version"]).replace(".", "_")
            + "/_manifest.py",
            rendered_manifest + manifest_suffix,
        )
        if cleanup_source is None:
            cleanup_source = (
                Path(__file__).resolve().parents[2]
                / "src"
                / "dsctl"
                / "generated"
                / "task_definition_cleanup_profiles.py"
            ).read_text(encoding="utf-8")
        archive.writestr(
            "dsctl/generated/task_definition_cleanup_profiles.py",
            cleanup_source,
        )


def _refresh_assessment_digest(assessment: dict[str, Any]) -> None:
    payload = json.dumps(
        {key: value for key, value in assessment.items() if key != "assessment_digest"},
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    assessment["assessment_digest"] = "sha256:" + hashlib.sha256(payload).hexdigest()
