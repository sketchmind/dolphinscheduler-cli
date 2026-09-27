from __future__ import annotations

import importlib
import json
import subprocess
import sys
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest
from tests.live.exact_read_gate import (
    EXACT_PROFILE_READ_ACTIONS,
    EXACT_PROFILE_READ_RECIPES,
    INSTALLED_READ_PROBE_CODE,
    ClusterIdentity,
    ExactReadFixture,
    ExactReadGateConfig,
    ExactReadRuntimeConfig,
    InstalledReadAttestation,
    NativeFixtureIdentity,
    execute_exact_read_gate,
    identity_hmac,
    inspect_installed_read,
    load_exact_manifest,
    load_exact_read_gate_config,
    write_exact_read_evidence,
)
from tests.live.exact_read_gate import _read_profile_values as read_profile_values
from tests.live.exact_read_gate import _require_page_shape as require_page_shape
from tests.live.exact_read_gate import (
    _validate_installed_profile as validate_installed_profile,
)
from tests.live.support import DsctlCommandResult

if TYPE_CHECKING:
    from typing import Literal


def test_exact_read_scenario_covers_code_native_project_and_workflow() -> None:
    key = b"k" * 32
    config = ExactReadGateConfig(
        ds_version="3.2.2",
        family="process-definition-3.2",
        support_level="experimental",
        tested=False,
        attestation_key=key,
        cluster=ClusterIdentity(
            ds_version="3.2.2",
            image_ref="apache/dolphinscheduler-api:3.2.2",
            image_id="sha256:" + "1" * 64,
            image_source="swarm service inspection",
            image_observed_at="2026-08-06T08:00:00Z",
            api_target_hmac_sha256="hmac-sha256:" + "2" * 64,
            principal_hmac_sha256=identity_hmac("etl-reader", key=key),
            persona="etl-developer",
        ),
        fixture=ExactReadFixture(
            project_name="read-project",
            project_identity=NativeFixtureIdentity(kind="code", value=12001),
            workflow_name="scheduled-workflow",
            workflow_identity=NativeFixtureIdentity(kind="code", value=13001),
            schedule_id=14001,
            workflow_release_state="ONLINE",
            provisioner="external-version-matrix",
            manifest_sha256="3" * 64,
        ),
    )
    seen: list[tuple[str, ...]] = []

    def invoke(argv: list[str]) -> DsctlCommandResult:
        seen.append(tuple(argv))
        action, data = _read_command_response(argv)
        payload = {"ok": True, "action": action, "data": data}
        return DsctlCommandResult(
            argv=tuple(argv),
            exit_code=0,
            stdout=json.dumps(payload),
            stderr="",
            payload=payload,
        )

    result = execute_exact_read_gate(config, invoke=invoke)

    assert result.effects.to_data() == {
        "remote_mutations": 0,
        "fixture_mutated": False,
    }
    assert [entry.action for entry in result.operation_trace] == [
        "version",
        "capabilities",
        "capabilities",
        "capabilities",
        "capabilities",
        "doctor",
        "project.list",
        "project.get",
        "project.get",
        "workflow.list",
        "workflow.get",
        "workflow.get",
    ]
    assert (
        "project",
        "get",
        "12001",
    ) in seen
    assert (
        "workflow",
        "get",
        "13001",
        "--project",
        "read-project",
    ) in seen


def test_exact_read_config_rejects_wrong_native_identity_epoch() -> None:
    key = b"k" * 32

    with pytest.raises(
        ValueError,
        match=r"DS 1\.3\.9 fixtures must use id identities",
    ):
        ExactReadGateConfig(
            ds_version="1.3.9",
            family="process-definition-1.3",
            support_level="experimental",
            tested=False,
            attestation_key=key,
            cluster=ClusterIdentity(
                ds_version="1.3.9",
                image_ref="apache/dolphinscheduler:1.3.9",
                image_id="sha256:" + "1" * 64,
                image_source="swarm service inspection",
                image_observed_at="2026-08-06T08:00:00Z",
                api_target_hmac_sha256="hmac-sha256:" + "2" * 64,
                principal_hmac_sha256=identity_hmac("etl-reader", key=key),
                persona="etl-developer",
            ),
            fixture=ExactReadFixture(
                project_name="read-project",
                project_identity=NativeFixtureIdentity(kind="code", value=12001),
                workflow_name="scheduled-workflow",
                workflow_identity=NativeFixtureIdentity(kind="code", value=13001),
                schedule_id=14001,
                workflow_release_state="ONLINE",
                provisioner="external-version-matrix",
                manifest_sha256="3" * 64,
            ),
        )


def test_exact_read_fixture_rejects_unknown_release_state() -> None:
    with pytest.raises(ValueError, match="release state must be ONLINE or OFFLINE"):
        ExactReadFixture(
            project_name="read-project",
            project_identity=NativeFixtureIdentity(kind="code", value=12001),
            workflow_name="scheduled-workflow",
            workflow_identity=NativeFixtureIdentity(kind="code", value=13001),
            schedule_id=14001,
            workflow_release_state="BROKEN",
            provisioner="external-version-matrix",
            manifest_sha256="3" * 64,
        )


@pytest.mark.parametrize(
    ("project_name", "workflow_name"),
    [("001", "scheduled-workflow"), ("read-project", "+1")],
)
def test_exact_read_fixture_rejects_numeric_selector_names(
    project_name: str,
    workflow_name: str,
) -> None:
    with pytest.raises(ValueError, match="must not resolve as a numeric selector"):
        ExactReadFixture(
            project_name=project_name,
            project_identity=NativeFixtureIdentity(kind="code", value=12001),
            workflow_name=workflow_name,
            workflow_identity=NativeFixtureIdentity(kind="code", value=13001),
            schedule_id=14001,
            workflow_release_state="OFFLINE",
            provisioner="external-version-matrix",
            manifest_sha256="3" * 64,
        )


def test_exact_read_scenario_supports_legacy_id_identity_without_actuator() -> None:
    key = b"k" * 32
    config = ExactReadGateConfig(
        ds_version="1.3.9",
        family="process-definition-1.3",
        support_level="experimental",
        tested=False,
        attestation_key=key,
        cluster=ClusterIdentity(
            ds_version="1.3.9",
            image_ref="apache/dolphinscheduler:1.3.9",
            image_id="sha256:" + "1" * 64,
            image_source="swarm service inspection",
            image_observed_at="2026-08-06T08:00:00Z",
            api_target_hmac_sha256="hmac-sha256:" + "2" * 64,
            principal_hmac_sha256=identity_hmac("etl-reader", key=key),
            persona="etl-developer",
        ),
        fixture=ExactReadFixture(
            project_name="read-project",
            project_identity=NativeFixtureIdentity(kind="id", value=21),
            workflow_name="scheduled-workflow",
            workflow_identity=NativeFixtureIdentity(kind="id", value=31),
            schedule_id=14001,
            workflow_release_state="ONLINE",
            provisioner="external-version-matrix",
            manifest_sha256="3" * 64,
        ),
    )
    seen: list[tuple[str, ...]] = []

    def invoke(argv: list[str]) -> DsctlCommandResult:
        seen.append(tuple(argv))
        action, data = _read_command_response(
            argv,
            ds_version="1.3.9",
            family="process-definition-1.3",
            api_status="warning",
        )
        payload = {"ok": True, "action": action, "data": data}
        return DsctlCommandResult(
            argv=tuple(argv),
            exit_code=0,
            stdout=json.dumps(payload),
            stderr="",
            payload=payload,
        )

    result = execute_exact_read_gate(config, invoke=invoke)

    assert ("project", "get", "21") in seen
    assert (
        "workflow",
        "get",
        "31",
        "--project",
        "read-project",
    ) in seen
    doctor = next(entry for entry in result.operation_trace if entry.action == "doctor")
    assert "api-health-warning-recorded" in doctor.assertions


def test_exact_read_scenario_hydrates_schedule_when_list_omits_it() -> None:
    key = b"k" * 32
    config = ExactReadGateConfig(
        ds_version="3.2.0",
        family="process-definition-3.2",
        support_level="experimental",
        tested=False,
        attestation_key=key,
        cluster=_cluster_identity("3.2.0", key),
        fixture=_fixture("code"),
    )

    def invoke(argv: list[str]) -> DsctlCommandResult:
        action, data = _read_command_response(argv, ds_version="3.2.0")
        payload = {"ok": True, "action": action, "data": data}
        return DsctlCommandResult(
            argv=tuple(argv),
            exit_code=0,
            stdout=json.dumps(payload),
            stderr="",
            payload=payload,
        )

    result = execute_exact_read_gate(config, invoke=invoke)

    workflow_gets = [
        entry for entry in result.operation_trace if entry.action == "workflow.get"
    ]
    assert len(workflow_gets) == 2
    assert all("schedule-hydrated" in entry.assertions for entry in workflow_gets)


def test_exact_read_scenario_rejects_non_live_smoke_black_box_capability() -> None:
    key = b"k" * 32
    config = ExactReadGateConfig(
        ds_version="3.2.2",
        family="process-definition-3.2",
        support_level="experimental",
        tested=False,
        attestation_key=key,
        cluster=_cluster_identity("3.2.2", key),
        fixture=_fixture("code"),
    )

    def invoke(argv: list[str]) -> DsctlCommandResult:
        action, data = _read_command_response(argv)
        if argv[:2] == ["capabilities", "--action"]:
            assert isinstance(data, dict)
            capability = data["capability"]
            assert isinstance(capability, dict)
            capability["verification"] = "contract_tested"
        payload = {"ok": True, "action": action, "data": data}
        return DsctlCommandResult(
            argv=tuple(argv),
            exit_code=0,
            stdout=json.dumps(payload),
            stderr="",
            payload=payload,
        )

    with pytest.raises(AssertionError, match="not attested as live_smoke"):
        execute_exact_read_gate(config, invoke=invoke)


def test_exact_read_page_shape_requires_complete_positive_metadata() -> None:
    with pytest.raises(AssertionError, match="complete positive pagination metadata"):
        require_page_shape(
            {
                "pageNo": 1,
                "pageSize": 20,
                "currentPage": 1,
                "total": 1,
            }
        )


def test_load_exact_manifest_allows_full_341_bundle(tmp_path: Path) -> None:
    wheel = tmp_path / "dolphinscheduler_cli-0.4.0-py3-none-any.whl"
    manifest_path = "dsctl/generated/versions/ds_3_4_1/_manifest.py"
    with zipfile.ZipFile(wheel, mode="w") as archive:
        _write_ownership_artifacts(archive, exclude=manifest_path)
        archive.writestr(
            manifest_path,
            _manifest_source(
                ds_version="3.4.1",
                selection="full",
                semantic_operations=(),
                operation_count=298,
            ),
        )

    manifest = load_exact_manifest(wheel, ds_version="3.4.1")

    assert manifest["selection"] == "full"
    assert manifest["semantic_operations"] == []
    assert manifest["operation_count"] == 298


def test_load_exact_manifest_rejects_historical_bundle_schema_in_current_wheel(
    tmp_path: Path,
) -> None:
    wheel = tmp_path / "dolphinscheduler_cli-0.4.0-py3-none-any.whl"
    manifest_path = "dsctl/generated/versions/ds_3_4_1/_manifest.py"
    with zipfile.ZipFile(wheel, mode="w") as archive:
        _write_ownership_artifacts(archive, exclude=manifest_path)
        archive.writestr(
            manifest_path,
            _manifest_source(
                ds_version="3.4.1",
                selection="runtime-slice",
                semantic_operations=(
                    "project.get",
                    "project.page",
                    "workflow.get",
                    "workflow.page",
                ),
                operation_count=4,
                bundle_manifest_schema_version=1,
            ),
        )

    with pytest.raises(ValueError, match="manifest schema is unsupported"):
        load_exact_manifest(wheel, ds_version="3.4.1")


def test_load_exact_manifest_allows_341_runtime_slice(tmp_path: Path) -> None:
    wheel = tmp_path / "dolphinscheduler_cli-0.4.0-py3-none-any.whl"
    manifest_path = "dsctl/generated/versions/ds_3_4_1/_manifest.py"
    operations = _legacy_operations("3.4.1")
    with zipfile.ZipFile(wheel, mode="w") as archive:
        _write_ownership_artifacts(archive, exclude=manifest_path)
        archive.writestr(
            manifest_path,
            _manifest_source(
                ds_version="3.4.1",
                selection="runtime-slice",
                semantic_operations=operations,
                operation_count=137,
            ),
        )

    manifest = load_exact_manifest(wheel, ds_version="3.4.1")

    assert manifest["selection"] == "runtime-slice"
    assert manifest["semantic_operations"] == list(operations)


@pytest.mark.parametrize("count", [0, -1])
def test_runtime_slice_can_have_zero_native_operations_with_its_own_compiled_reads(
    tmp_path: Path, count: int
) -> None:
    wheel = tmp_path / "candidate.whl"
    manifest_path = "dsctl/generated/versions/ds_3_2_2/_manifest.py"
    with zipfile.ZipFile(wheel, mode="w") as archive:
        _write_ownership_artifacts(archive, exclude=manifest_path)
        archive.writestr(
            manifest_path,
            _manifest_source(
                ds_version="3.2.2",
                selection="runtime-slice",
                semantic_operations=(),
                operation_count=count,
            ),
        )
    if count < 0:
        with pytest.raises(ValueError, match="does not close the exact read"):
            load_exact_manifest(wheel, ds_version="3.2.2")
    else:
        manifest = load_exact_manifest(wheel, ds_version="3.2.2")
        assert manifest["semantic_operations"] == []
        assert manifest["operation_count"] == 0


@pytest.mark.parametrize(
    "missing", ["project_program", "workflow_program", "duplicate_legacy_owner"]
)
def test_read_wheel_requires_unique_owners_from_its_own_artifact(
    tmp_path: Path, missing: str
) -> None:
    wheel = tmp_path / "candidate.whl"
    manifest_path = "dsctl/generated/versions/ds_3_2_2/_manifest.py"
    with zipfile.ZipFile(wheel, mode="w") as archive:
        _write_ownership_artifacts(
            archive,
            exclude=(
                manifest_path
                if missing == "duplicate_legacy_owner"
                else "dsctl/generated/wire_programs/project.py"
                if missing == "project_program"
                else "dsctl/generated/wire_programs/workflow_runtime.py"
            ),
        )
        if missing == "duplicate_legacy_owner":
            archive.writestr(
                manifest_path,
                _manifest_source(
                    ds_version="3.2.2",
                    selection="runtime-slice",
                    semantic_operations=(*_legacy_operations("3.2.2"), "workflow.get"),
                    operation_count=3,
                ),
            )

    expected_error = (
        "duplicate compiled owners"
        if missing == "duplicate_legacy_owner"
        else "artifact inventory"
    )
    with pytest.raises(ValueError, match=expected_error):
        load_exact_manifest(wheel, ds_version="3.2.2")


def test_installed_read_probe_does_not_import_upstream_composition() -> None:
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
            import_blocker + INSTALLED_READ_PROBE_CODE,
            "3.4.1",
            json.dumps(EXACT_PROFILE_READ_RECIPES),
        ],
        capture_output=True,
        check=False,
        text=True,
        timeout=30,
    )

    assert completed.returncode == 0, completed.stderr
    payload = json.loads(completed.stdout)
    assert not any("adapter" in key for key in payload)
    assert "catalog_action_verifications" not in payload


def test_installed_read_probe_rejects_mismatched_action_recipe() -> None:
    mismatched_recipes = list(EXACT_PROFILE_READ_RECIPES)
    mismatched_recipes[0] = ("project.get", "workflow.get")

    completed = subprocess.run(  # noqa: S603 - fixed interpreter and probe source
        [
            sys.executable,
            "-I",
            "-c",
            INSTALLED_READ_PROBE_CODE,
            "3.4.1",
            json.dumps(mismatched_recipes),
        ],
        capture_output=True,
        check=False,
        text=True,
        timeout=30,
    )

    assert completed.returncode != 0
    assert "read action does not match generated decision" in completed.stderr


def test_load_exact_manifest_allows_root_count_to_differ_from_closure_count(
    tmp_path: Path,
) -> None:
    wheel = tmp_path / "dolphinscheduler_cli-0.4.0-py3-none-any.whl"
    manifest_path = "dsctl/generated/versions/ds_3_2_2/_manifest.py"
    operations = _legacy_operations("3.2.2")
    with zipfile.ZipFile(wheel, mode="w") as archive:
        _write_ownership_artifacts(archive, exclude=manifest_path)
        archive.writestr(
            manifest_path,
            _manifest_source(
                ds_version="3.2.2",
                selection="runtime-slice",
                semantic_operations=operations,
                operation_count=3,
            ),
        )

    manifest = load_exact_manifest(wheel, ds_version="3.2.2")

    semantic_operations = manifest["semantic_operations"]
    assert isinstance(semantic_operations, list)
    assert semantic_operations == list(operations)
    assert len(semantic_operations) != 3
    assert manifest["operation_count"] == 3


def test_load_exact_read_gate_config_cross_checks_profile_and_fixture(
    tmp_path: Path,
) -> None:
    key = b"attestation-key-material-32-bytes!"
    key_file = _write_private(tmp_path / "attestation.key", key)
    env_file = _write_private(
        tmp_path / "etl.env",
        b"DS_API_URL=http://matrix.test:12345/dolphinscheduler\n"
        b"DS_API_TOKEN=opaque-token\nDS_VERSION=3.2.2\n",
    )
    executable = _write_private(tmp_path / "dsctl", b"")
    python = _write_private(tmp_path / "python", b"")
    wheel = _write_private(tmp_path / "cli.whl", b"wheel")
    observed_at = datetime.now(tz=UTC).isoformat()
    image_ref = "apache/dolphinscheduler-api:3.2.2"
    image_id = "sha256:" + "1" * 64
    cluster = {
        "schema_version": 1,
        "ds_version": "3.2.2",
        "image_ref": image_ref,
        "image_id": image_id,
        "image_source": "swarm service inspection",
        "image_observed_at": observed_at,
        "api_target_hmac_sha256": identity_hmac(
            "http://matrix.test:12345/dolphinscheduler",
            key=key,
        ),
        "principal_hmac_sha256": identity_hmac("etl-reader", key=key),
        "persona": "etl-developer",
    }
    fixture = {
        "schema_version": 1,
        "ds_version": "3.2.2",
        "image_ref": image_ref,
        "image_id": image_id,
        "provisioner": "external-version-matrix",
        "project": {
            "name": "read-project",
            "identity": {"kind": "code", "value": 12001},
        },
        "workflow": {
            "name": "scheduled-workflow",
            "identity": {"kind": "code", "value": 13001},
            "schedule_id": 14001,
            "release_state": "ONLINE",
            "scheduled": True,
        },
    }
    cluster_file = tmp_path / "cluster.json"
    cluster_file.write_text(json.dumps(cluster), encoding="utf-8")
    fixture_file = tmp_path / "fixture.json"
    fixture_file.write_text(json.dumps(fixture), encoding="utf-8")

    config = load_exact_read_gate_config(
        {
            "DS_LIVE_EXACT_READ_VERSION": "3.2.2",
            "DS_LIVE_EXACT_READ_ENV_FILE": str(env_file),
            "DS_LIVE_EXACT_READ_DSCTL": str(executable),
            "DS_LIVE_EXACT_READ_PYTHON": str(python),
            "DS_LIVE_EXACT_READ_WHEEL": str(wheel),
            "DS_LIVE_EXACT_READ_CLUSTER_MANIFEST": str(cluster_file),
            "DS_LIVE_EXACT_READ_FIXTURE_MANIFEST": str(fixture_file),
            "DS_LIVE_EXACT_READ_ATTESTATION_KEY_FILE": str(key_file),
            "DS_LIVE_EXACT_READ_EVIDENCE": str(tmp_path / "receipt.json"),
        }
    )

    assert config.ds_version == "3.2.2"
    assert config.fixture.project_identity == NativeFixtureIdentity(
        kind="code",
        value=12001,
    )
    assert config.cluster.api_target_hmac_sha256 == cluster["api_target_hmac_sha256"]


def test_gate_profile_parser_only_removes_matching_outer_quotes(
    tmp_path: Path,
) -> None:
    profile = tmp_path / "profile.env"
    profile.write_text(
        'DS_VERSION=\'3.2.2"\nDS_API_TOKEN="quoted-token"\n',
        encoding="utf-8",
    )

    values = read_profile_values(profile)

    assert values["DS_VERSION"] == "'3.2.2\""
    assert values["DS_API_TOKEN"] == "quoted-token"


@pytest.mark.parametrize(
    ("verification", "expected_error"),
    [
        ("live_smoke", None),
        ("contract_tested", "not all live_smoke"),
    ],
)
def test_inspect_installed_read_cross_checks_profile_and_wheel_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    verification: str,
    expected_error: str | None,
) -> None:
    venv_dir = tmp_path / "venv"
    bin_dir = venv_dir / "bin"
    bin_dir.mkdir(parents=True)
    python = _write_private(bin_dir / "python", b"")
    executable = _write_private(bin_dir / "dsctl", b"")
    module_file = venv_dir / "lib" / "dsctl" / "__init__.py"
    module_file.parent.mkdir(parents=True)
    module_file.write_text("", encoding="utf-8")
    wheel = tmp_path / "cli.whl"
    operations = _legacy_operations("3.2.2")
    with zipfile.ZipFile(wheel, mode="w") as archive:
        _write_ownership_artifacts(
            archive, exclude="dsctl/generated/versions/ds_3_2_2/_manifest.py"
        )
        archive.writestr(
            "dsctl/generated/versions/ds_3_2_2/_manifest.py",
            _manifest_source(
                ds_version="3.2.2",
                selection="runtime-slice",
                semantic_operations=operations,
                operation_count=4,
            ),
        )
    manifest = load_exact_manifest(wheel, ds_version="3.2.2")
    recipes = [
        _installed_recipe("project.get", "project.get", "1"),
        _installed_recipe("project.list", "project.page", "2"),
        _installed_recipe("workflow.get", "workflow.get", "3"),
        _installed_recipe("workflow.list", "workflow.page", "4"),
    ]
    profile_fingerprints = {
        "source": "sha256:" + "1" * 64,
        "effective_wire": "sha256:" + "2" * 64,
        "consumed_projection": "sha256:" + "3" * 64,
        "preservation": "sha256:" + "4" * 64,
    }
    probe_payload = {
        "action_verifications": dict.fromkeys(
            EXACT_PROFILE_READ_ACTIONS,
            verification,
        ),
        "distribution_version": "0.4.0",
        "module_file": str(module_file),
        "server_version": "3.2.2",
        "contract_version": "3.2.2",
        "family": "process-definition-3.2",
        "support_level": "experimental",
        "tested": False,
        "profile_source": {
            "tag": manifest["source_tag"],
            "commit": manifest["source_commit"],
            "tree": manifest["source_tree"],
        },
        "profile_fingerprints": profile_fingerprints,
        "read_recipes": recipes,
        "manifest": manifest,
    }
    monkeypatch.setattr(
        "tests.live.exact_read_gate.subprocess.run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=0,
            stdout=json.dumps(probe_payload),
            stderr="",
        ),
    )
    runtime = ExactReadRuntimeConfig(
        ds_version="3.2.2",
        env_file=_write_private(tmp_path / "etl.env", b"DS_VERSION=3.2.2\n"),
        executable=executable,
        python=python,
        wheel=wheel,
        cluster_manifest=_write_private(tmp_path / "cluster.json", b"{}"),
        fixture_manifest=_write_private(tmp_path / "fixture.json", b"{}"),
        evidence_path=tmp_path / "receipt.json",
        attestation_key=b"k" * 32,
        cluster=_cluster_identity("3.2.2", b"k" * 32),
        fixture=_fixture("code"),
    )

    if expected_error is not None:
        with pytest.raises(AssertionError, match=expected_error):
            inspect_installed_read(runtime)
        return

    installation = inspect_installed_read(runtime)

    assert installation.distribution_version == "0.4.0"
    assert installation.contract_version == "3.2.2"
    assert installation.profile_fingerprints == profile_fingerprints
    assert set(installation.action_verifications.values()) == {"live_smoke"}
    assert list(installation.read_recipes) == recipes


def test_installed_profile_accepts_legacy_exact_contract() -> None:
    installation = InstalledReadAttestation(
        distribution_version="0.4.0",
        contract_version="1.3.9",
        family="process-definition-1.3",
        support_level="experimental",
        tested=False,
        profile_fingerprints={
            "source": "sha256:" + "1" * 64,
            "effective_wire": "sha256:" + "2" * 64,
            "consumed_projection": "sha256:" + "3" * 64,
            "preservation": "sha256:" + "4" * 64,
        },
        action_verifications=dict.fromkeys(
            EXACT_PROFILE_READ_ACTIONS,
            "live_smoke",
        ),
        read_recipes=(
            _installed_recipe("project.get", "project.get", "1"),
            _installed_recipe("project.list", "project.page", "2"),
            _installed_recipe("workflow.get", "workflow.get", "3"),
            _installed_recipe("workflow.list", "workflow.page", "4"),
        ),
    )

    validate_installed_profile(installation, ds_version="1.3.9")


def test_write_exact_read_evidence_is_sanitized_and_read_only(
    tmp_path: Path,
) -> None:
    key = b"k" * 32
    cluster = _cluster_identity("3.2.2", key)
    fixture = _fixture("code")
    env_file = _write_private(
        tmp_path / "etl.env",
        b"DS_API_URL=http://matrix.test:12345/dolphinscheduler\n"
        b"DS_API_TOKEN=super-secret-token\nDS_VERSION=3.2.2\n",
    )
    wheel = tmp_path / "cli.whl"
    operations = _legacy_operations("3.2.2")
    with zipfile.ZipFile(wheel, mode="w") as archive:
        _write_ownership_artifacts(
            archive, exclude="dsctl/generated/versions/ds_3_2_2/_manifest.py"
        )
        archive.writestr(
            "dsctl/generated/versions/ds_3_2_2/_manifest.py",
            _manifest_source(
                ds_version="3.2.2",
                selection="runtime-slice",
                semantic_operations=operations,
                operation_count=4,
            ),
        )
    runtime = ExactReadRuntimeConfig(
        ds_version="3.2.2",
        env_file=env_file,
        executable=_write_private(tmp_path / "dsctl", b""),
        python=_write_private(tmp_path / "python", b""),
        wheel=wheel,
        cluster_manifest=_write_private(tmp_path / "cluster.json", b"{}"),
        fixture_manifest=_write_private(tmp_path / "fixture.json", b"{}"),
        evidence_path=tmp_path / "receipt.json",
        attestation_key=key,
        cluster=cluster,
        fixture=fixture,
    )
    fingerprints = {
        "source": "sha256:" + "1" * 64,
        "effective_wire": "sha256:" + "2" * 64,
        "consumed_projection": "sha256:" + "3" * 64,
        "preservation": "sha256:" + "4" * 64,
    }
    installation = InstalledReadAttestation(
        distribution_version="0.4.0",
        contract_version="3.2.2",
        family="process-definition-3.2",
        support_level="experimental",
        tested=False,
        profile_fingerprints=fingerprints,
        action_verifications=dict.fromkeys(
            EXACT_PROFILE_READ_ACTIONS,
            "live_smoke",
        ),
        read_recipes=(
            _installed_recipe("project.get", "project.get", "1"),
            _installed_recipe("project.list", "project.page", "2"),
            _installed_recipe("workflow.get", "workflow.get", "3"),
            _installed_recipe("workflow.list", "workflow.page", "4"),
        ),
    )
    scenario = ExactReadGateConfig(
        ds_version="3.2.2",
        family=installation.family,
        support_level=installation.support_level,
        tested=installation.tested,
        attestation_key=key,
        cluster=cluster,
        fixture=fixture,
    )

    def invoke(argv: list[str]) -> DsctlCommandResult:
        action, data = _read_command_response(argv)
        payload = {"ok": True, "action": action, "data": data}
        return DsctlCommandResult(
            argv=tuple(argv),
            exit_code=0,
            stdout=json.dumps(payload),
            stderr="",
            payload=payload,
        )

    result = execute_exact_read_gate(scenario, invoke=invoke)
    receipt = write_exact_read_evidence(
        runtime,
        installation=installation,
        result=result,
        recorded_at=datetime.now(tz=UTC),
    )

    payload = json.loads(receipt.read_text(encoding="utf-8"))
    raw = json.dumps(payload)
    assert payload["schema_version"] == 2
    assert payload["gate"] == "exact-profile-read"
    assert set(payload["profile"]) == {
        "contract_version",
        "ds",
        "family",
        "fingerprints",
        "selected_ds_version",
        "support_level",
        "tested",
    }
    assert payload["effects"] == {
        "fixture_mutated": False,
        "remote_mutations": 0,
    }
    assert payload["read_bundle"]["action_verifications"] == {
        "project.get": "live_smoke",
        "project.list": "live_smoke",
        "workflow.get": "live_smoke",
        "workflow.list": "live_smoke",
    }
    assert "super-secret-token" not in raw
    assert "read-project" not in raw
    assert "matrix.test" not in raw
    assert "adapter" not in raw


def _write_private(path: Path, value: bytes) -> Path:
    path.write_bytes(value)
    path.chmod(0o600)
    return path


def _cluster_identity(version: str, key: bytes) -> ClusterIdentity:
    repository = "apache/dolphinscheduler-api"
    return ClusterIdentity(
        ds_version=version,
        image_ref=f"{repository}:{version}",
        image_id="sha256:" + "1" * 64,
        image_source="swarm service inspection",
        image_observed_at=datetime.now(tz=UTC).isoformat(),
        api_target_hmac_sha256="hmac-sha256:" + "2" * 64,
        principal_hmac_sha256=identity_hmac("etl-reader", key=key),
        persona="etl-developer",
    )


def _fixture(kind: Literal["id", "code"]) -> ExactReadFixture:
    return ExactReadFixture(
        project_name="read-project",
        project_identity=NativeFixtureIdentity(kind=kind, value=12001),
        workflow_name="scheduled-workflow",
        workflow_identity=NativeFixtureIdentity(kind=kind, value=13001),
        schedule_id=14001,
        workflow_release_state="ONLINE",
        provisioner="external-version-matrix",
        manifest_sha256="3" * 64,
    )


def _installed_recipe(
    action: str,
    semantic_operation: str,
    digit: str,
) -> dict[str, object]:
    return {
        "action": action,
        "semantic_operation": semantic_operation,
        "build_status": "accepted",
        "fingerprints": {
            "source": "sha256:" + digit * 64,
            "effective_wire": "sha256:" + digit * 64,
            "consumed_projection": "sha256:" + digit * 64,
            "preservation": "sha256:" + digit * 64,
        },
    }


def _legacy_operations(ds_version: str) -> tuple[str, ...]:
    manifest = importlib.import_module(
        f"dsctl.generated.versions.ds_{ds_version.replace('.', '_')}._manifest"
    )
    return tuple(manifest.SEMANTIC_OPERATIONS)


def _manifest_source(
    *,
    ds_version: str,
    selection: str,
    semantic_operations: tuple[str, ...],
    operation_count: int,
    bundle_manifest_schema_version: int = 2,
) -> str:
    manifest = importlib.import_module(
        f"dsctl.generated.versions.ds_{ds_version.replace('.', '_')}._manifest"
    )
    return "\n".join(
        (
            f"BUNDLE_MANIFEST_SCHEMA_VERSION = {bundle_manifest_schema_version}",
            f"DS_VERSION = {ds_version!r}",
            f"SELECTION = {selection!r}",
            f"SEMANTIC_OPERATIONS = {semantic_operations!r}",
            f"SOURCE_TAG = {ds_version!r}",
            f"SOURCE_COMMIT = {manifest.SOURCE_COMMIT!r}",
            f"SOURCE_TREE = {manifest.SOURCE_TREE!r}",
            f"SOURCE_CONTRACT_DIGEST = {manifest.SOURCE_CONTRACT_DIGEST!r}",
            f"RENDERED_CONTRACT_DIGEST = {'sha256:' + '4' * 64!r}",
            f"OPERATION_COUNT = {operation_count}",
        )
    )


def _write_ownership_artifacts(archive: zipfile.ZipFile, *, exclude: str) -> None:
    source_root = Path(__file__).resolve().parents[2] / "src"
    generated = source_root / "dsctl/generated"
    paths = [
        *sorted((generated / "versions").glob("*/_manifest.py")),
        *sorted((generated / "wire_programs").rglob("*.py")),
        *sorted((generated / "wire_runtime").rglob("*.py")),
    ]
    for path in paths:
        name = path.relative_to(source_root).as_posix()
        if name != exclude:
            archive.write(path, name)


def _read_command_response(
    argv: list[str],
    *,
    ds_version: str = "3.2.2",
    family: str = "process-definition-3.2",
    api_status: str = "ok",
) -> tuple[str, object]:
    if argv == ["version"]:
        return (
            "version",
            {
                "cli": "0.4.0",
                "ds": ds_version,
                "selected_ds_version": ds_version,
                "contract_version": ds_version,
                "family": family,
                "support_level": "experimental",
            },
        )
    if argv[:2] == ["capabilities", "--action"]:
        action = argv[2]
        return (
            "capabilities",
            {
                "capability": {
                    "action": action,
                    "availability": "supported",
                    "verification": "live_smoke",
                }
            },
        )
    if argv == ["doctor"]:
        return (
            "doctor",
            {
                "checks": [
                    {"name": "api", "status": api_status},
                    {
                        "name": "current_user",
                        "status": "ok",
                        "details": {
                            "userName": "etl-reader",
                            "userType": "GENERAL_USER",
                        },
                    },
                ]
            },
        )
    if argv[:2] == ["project", "list"]:
        return (
            "project.list",
            {
                "totalList": [{"id": 21, "code": 12001, "name": "read-project"}],
                "pageNo": 1,
                "pageSize": 20,
                "total": 1,
                "totalPage": 1,
                "currentPage": 1,
            },
        )
    if argv[:2] == ["project", "get"]:
        return (
            "project.get",
            {"id": 21, "code": 12001, "name": "read-project"},
        )
    if argv[:2] == ["workflow", "list"]:
        workflow_row = {
            "id": 31,
            "code": 13001,
            "name": "scheduled-workflow",
            "releaseState": "ONLINE",
        }
        if ds_version in {
            "3.2.1",
            "3.2.2",
            "3.3.1",
            "3.3.2",
            "3.4.0",
            "3.4.1",
            "3.4.2",
        }:
            workflow_row["scheduleId"] = 14001
        return (
            "workflow.list",
            {
                "totalList": [workflow_row],
                "pageNo": 1,
                "pageSize": 20,
                "total": 1,
                "totalPage": 1,
                "currentPage": 1,
            },
        )
    if argv[:2] == ["workflow", "get"]:
        return (
            "workflow.get",
            {
                "id": 31,
                "code": 13001,
                "name": "scheduled-workflow",
                "releaseState": "ONLINE",
                "schedule": {"id": 14001, "releaseState": "ONLINE"},
            },
        )
    message = f"unexpected command: {argv}"
    raise AssertionError(message)
