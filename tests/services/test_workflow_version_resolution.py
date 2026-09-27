"""Automatic target selection through real authoring and runtime boundaries."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

import httpx
import pytest

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import ConfigError
from dsctl.output import CommandResult
from dsctl.services import doctor, runtime, version_resolution
from dsctl.services.workflow import create, edit
from dsctl.services.workflow_instance import edit as instance_edit
from dsctl.upstream.runtime_instances import RuntimeInstanceDomain
from dsctl.upstream.version_discovery import DiscoveredVersion, VersionDiscoveryError
from dsctl.upstream.workflows import WorkflowDomain

if TYPE_CHECKING:
    from pathlib import Path

    from dsctl.config import ClusterProfile, ConnectionSettings
    from dsctl.services.task_authoring_catalog import TaskAuthoringCatalog


AuthoringAction = Literal["create", "edit", "instance-edit"]


@pytest.fixture(autouse=True, params=[None, "auto"])
def configured_auto_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.setenv("DS_API_URL", "https://auto.example.test/dolphinscheduler")
    monkeypatch.setenv("DS_API_TOKEN", "test-credential")
    if request.param is not None:
        monkeypatch.setenv("DS_VERSION", request.param)
    monkeypatch.chdir(tmp_path)


def _invoke(action: AuthoringAction, file: Path) -> CommandResult:
    if action == "create":
        return create.create_workflow_result(file=file, dry_run=True)
    if action == "edit":
        return edit.edit_workflow_result("daily-etl", file=file, dry_run=True)
    return instance_edit.edit_workflow_instance_result(1, file=file, dry_run=True)


def _workflow_file(tmp_path: Path) -> Path:
    path = tmp_path / "workflow.yaml"
    path.write_text(
        "workflow:\n  name: daily-etl\n  project: analytics\n"
        "tasks:\n  - name: extract\n    type: SHELL\n    command: echo extract\n",
        encoding="utf-8",
    )
    return path


def _replace_business_operation(
    monkeypatch: pytest.MonkeyPatch,
    action: AuthoringAction,
    callback: object,
) -> None:
    if action == "create":
        monkeypatch.setattr(create, "_create_workflow_result", callback)
    elif action == "edit":
        monkeypatch.setattr(edit, "_edit_workflow_result", callback)
    else:
        monkeypatch.setattr(instance_edit, "_edit_workflow_instance_result", callback)


@pytest.mark.parametrize("action", ["create", "edit", "instance-edit"])
def test_auto_authoring_binds_the_detected_profile_once_from_an_empty_cache(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    action: AuthoringAction,
) -> None:
    probe_connections: list[ConnectionSettings] = []
    client_profiles: list[ClusterProfile] = []
    operations: list[str] = []

    def discover(connection: ConnectionSettings) -> DiscoveredVersion:
        probe_connections.append(connection)
        return DiscoveredVersion(version="3.4.2", source="product_info")

    def unexpected_http(request: httpx.Request) -> httpx.Response:
        pytest.fail(f"Authoring setup must not execute business HTTP: {request.url}")

    def client(profile: ClusterProfile) -> DolphinSchedulerClient:
        client_profiles.append(profile)
        return DolphinSchedulerClient(
            profile, transport=httpx.MockTransport(unexpected_http)
        )

    def operation(
        service_runtime: runtime.BoundDomainServiceRuntime[object],
        *,
        catalog: TaskAuthoringCatalog,
        **_kwargs: object,
    ) -> CommandResult:
        assert service_runtime.profile is client_profiles[0]
        assert service_runtime.profile.ds_version == catalog.profile_version == "3.4.2"
        assert isinstance(service_runtime.http_client, DolphinSchedulerClient)
        assert service_runtime.http_client.profile is service_runtime.profile
        domain = service_runtime.domain
        if isinstance(domain, WorkflowDomain):
            assert domain.workflows.ds_version == "3.4.2"
        else:
            assert isinstance(domain, RuntimeInstanceDomain)
            assert domain.instances.ds_version == "3.4.2"
        operations.append(action)
        return CommandResult(data={"selected_version": catalog.profile_version})

    monkeypatch.setattr(version_resolution, "discover_target", discover)
    monkeypatch.setattr(runtime, "DolphinSchedulerClient", client)
    _replace_business_operation(monkeypatch, action, operation)
    assert not (tmp_path / "cache").exists()

    result = _invoke(action, _workflow_file(tmp_path))

    assert result.data == {"selected_version": "3.4.2"}
    assert operations == [action]
    assert len(probe_connections) == len(client_profiles) == 1
    assert probe_connections[0].api_url == client_profiles[0].api_url
    assert version_resolution.resolve_version().source == "cache"
    assert len(probe_connections) == 1


@pytest.mark.parametrize("action", ["create", "edit", "instance-edit"])
@pytest.mark.parametrize("failure", ["unavailable", "unknown-version"])
def test_auto_authoring_stops_before_runtime_binding_when_discovery_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    action: AuthoringAction,
    failure: str,
) -> None:
    probes: list[ConnectionSettings] = []

    def discover(connection: ConnectionSettings) -> DiscoveredVersion:
        probes.append(connection)
        if failure == "unknown-version":
            return DiscoveredVersion(version="9.9.9", source="product_info")
        raise VersionDiscoveryError(failure)

    def unexpected_business(*_args: object, **_kwargs: object) -> object:
        pytest.fail("A failed probe must stop before runtime or business operations")

    monkeypatch.setattr(version_resolution, "discover_target", discover)
    monkeypatch.setattr(runtime, "DolphinSchedulerClient", unexpected_business)
    _replace_business_operation(monkeypatch, action, unexpected_business)

    with pytest.raises(ConfigError):
        _invoke(action, _workflow_file(tmp_path))

    assert len(probes) == 1
    assert not list((tmp_path / "cache").rglob("*.json"))


def test_doctor_profile_details_describe_the_single_refreshed_observation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    probes: list[ConnectionSettings] = []

    def discover(connection: ConnectionSettings) -> DiscoveredVersion:
        probes.append(connection)
        return DiscoveredVersion(version="3.4.2", source="product_info")

    monkeypatch.setattr(version_resolution, "discover_target", discover)

    result = doctor._profile_check(env_file=None)

    assert result.profile is not None
    assert result.profile.ds_version == "3.4.2"
    assert result.check["status"] == "ok"
    details = result.check["details"]
    assert details["ds_version"] == "3.4.2"
    assert details["version_source"] == "probe"
    assert details["version_evidence"] == "product_info"
    assert isinstance(details["version_checked_at"], float)
    assert len(probes) == 1
