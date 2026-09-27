"""Unknown releases remain unknown even when a reviewed read can execute."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

import httpx
import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.errors import (
    ApiResultError,
    ApiTransportError,
    ConfigError,
    UnsupportedFeatureError,
    UserInputError,
)
from dsctl.output import error_payload, require_json_object
from dsctl.services import meta
from dsctl.services import version_resolution as resolution
from dsctl.upstream.version_discovery import DiscoveredTarget, VersionDiscoveryError

if TYPE_CHECKING:
    from collections.abc import Callable

    from dsctl.support.json_types import JsonObject


@pytest.fixture(autouse=True)
def target(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.setenv("DS_API_URL", "https://ds.example.test/dolphinscheduler")
    monkeypatch.setenv("DS_API_TOKEN", "private-test-token")
    monkeypatch.delenv("DS_VERSION", raising=False)


def _observed(
    monkeypatch: pytest.MonkeyPatch,
    *,
    candidates: tuple[str, ...] = ("1.3.9",),
    operations: tuple[str, ...] = ("ProjectController.queryProjectListPaging",),
    reported: str | None = None,
) -> list[object]:
    calls: list[object] = []

    def discover(connection: object) -> DiscoveredTarget:
        calls.append(connection)
        return DiscoveredTarget(
            version=None,
            source="contract",
            candidate_versions=candidates,
            reported_version=reported,
            evidence_summary="Observed public API request facts.",
            compatible_operations=operations,
        )

    monkeypatch.setattr(resolution, "discover_target", discover)
    return calls


def test_single_candidate_never_becomes_an_exact_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _observed(monkeypatch)
    with resolution.invocation_scope("project.list"):
        observed = resolution.resolve_target(mode="runtime")
        assert isinstance(observed, resolution.CompatibilityResolution)
        with pytest.raises(ConfigError, match="exact DolphinScheduler release"):
            resolution.resolve_version(mode="runtime")
        with pytest.raises(ConfigError):
            resolution.resolve_profile()
        selected = resolution.resolve_runtime_selection()
        assert selected.read_plan is not None
        assert selected.execution_profile.ds_version == "1.3.9"
        assert selected.read_plan.action == "project.list"
    assert len(calls) == 1
    data = meta.get_version_result().data
    assert isinstance(data, dict)
    assert data["ds"] is None
    assert data["selected_ds_version"] is None
    assert data["candidate_versions"] == ["1.3.9"]


@pytest.mark.parametrize(
    "action",
    [
        "project.create",
        "project.delete",
        "workflow.create",
        "workflow.edit",
        "workflow.run",
        "schedule.online",
        "task-instance.force-success",
    ],
)
def test_candidate_evidence_never_admits_mutation_or_authoring(
    monkeypatch: pytest.MonkeyPatch,
    action: str,
) -> None:
    _observed(monkeypatch)
    with resolution.invocation_scope(action), pytest.raises(UnsupportedFeatureError):
        resolution.resolve_runtime_selection()


def test_no_action_scope_cannot_open_compatibility_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _observed(monkeypatch)
    with pytest.raises(ConfigError):
        resolution.resolve_runtime_selection()


def test_mutating_invocation_cannot_override_its_action_with_a_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _observed(monkeypatch)
    with (
        resolution.invocation_scope("project.delete"),
        pytest.raises(ConfigError) as error,
    ):
        resolution.resolve_runtime_selection(action="project.list")
    assert error.value.details["reason"] == "invocation_action_mismatch"


def test_fresh_unknown_metadata_replaces_prior_positive_local_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _observed(monkeypatch)
    resolution.resolve_target(mode="runtime")
    _observed(monkeypatch, candidates=(), operations=(), reported="9.0.0")
    resolution.resolve_target(mode="runtime")
    cached = resolution.resolve_target(mode="local")
    assert isinstance(cached, resolution.CompatibilityResolution)
    assert cached.candidate_versions == ()
    assert cached.reported_version == "9.0.0"


def test_unobserved_required_operation_blocks_otherwise_reviewed_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _observed(monkeypatch, operations=())
    with (
        resolution.invocation_scope("project.list"),
        pytest.raises(UnsupportedFeatureError) as error,
    ):
        resolution.resolve_runtime_selection()
    assert error.value.details["reason"] == "read_operation_not_observed"


def test_reported_database_version_is_not_rewritten_as_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _observed(monkeypatch, candidates=("3.2.2",), reported="3.3.0")
    resolution.resolve_target(mode="runtime")
    data = meta.get_version_result().data
    assert isinstance(data, dict)
    assert data["reported_version"] == "3.3.0"
    assert data["candidate_versions"] == ["3.2.2"]
    assert data["selected_ds_version"] is None


def test_candidate_cache_requires_current_generated_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _observed(monkeypatch)
    resolution.resolve_target(mode="runtime")
    monkeypatch.setattr(resolution, "discovery_contract_digest", lambda: "changed")
    with pytest.raises(ConfigError) as error:
        resolution.resolve_target(mode="local")
    assert error.value.details["reason"] == "version_not_resolved"


def test_contract_change_within_invocation_invalidates_local_observation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _observed(monkeypatch)
    resolution.resolve_target(mode="runtime")
    _observed(monkeypatch, operations=())
    with resolution.invocation_scope("project.list"):
        resolution.resolve_target(mode="local")
        with pytest.raises(ConfigError) as error:
            resolution.resolve_target(mode="runtime")
    assert error.value.details["reason"] == "version_changed_during_invocation"


def _mock_http(
    monkeypatch: pytest.MonkeyPatch,
    handler: Callable[[httpx.Request], httpx.Response],
) -> None:
    original = httpx.Client

    def client(*args: object, **kwargs: object) -> httpx.Client:
        kwargs["transport"] = httpx.MockTransport(handler)
        return original(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(httpx, "Client", client)


def test_real_cli_discovers_legacy_contract_and_queries_without_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = (
        Path(__file__).parents[1] / "fixtures/version_discovery/legacy-swagger.json"
    )
    document = json.loads(fixture.read_text())
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.headers["token"] == "private-test-token"
        assert request.method == "GET"
        path = request.url.path.removeprefix("/dolphinscheduler/")
        if path == "v2/api-docs":
            return httpx.Response(200, json=document)
        if path == "projects/list-paging":
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "total": 1,
                        "totalList": [{"id": 7, "name": "analytics"}],
                        "pageNo": 1,
                        "pageSize": 100,
                        "totalPage": 1,
                    },
                },
            )
        return httpx.Response(404, json={"status": 404})

    _mock_http(monkeypatch, handler)
    result = CliRunner().invoke(app, ["project", "list"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["data"]["totalList"][0]["name"] == "analytics"
    target = payload["resolved"]["target"]
    assert target["ds_version"] is None
    assert target["candidate_versions"] == ["1.3.9"]
    assert target["execution"] == "read_only"
    assert payload["warnings"][-1]["code"] == "read_compatibility"
    assert sum(r.url.path.endswith("/projects/list-paging") for r in requests) == 1
    assert sum(r.url.path.endswith("/v2/api-docs") for r in requests) == 1


def test_cli_denies_candidate_mutation_before_business_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _observed(monkeypatch)

    def forbidden(request: httpx.Request) -> httpx.Response:
        pytest.fail(
            f"No mutation request is permitted: {request.method} {request.url.path}"
        )

    _mock_http(monkeypatch, forbidden)
    result = CliRunner().invoke(app, ["project", "create", "--name", "blocked"])
    assert result.exit_code == 1, result.output
    assert json.loads(result.stderr)["error"]["type"] == "unsupported_feature"


@pytest.mark.parametrize("prior", ["exact", "candidate"])
def test_fresh_negative_observation_replaces_positive_cache(
    monkeypatch: pytest.MonkeyPatch, prior: str
) -> None:
    if prior == "exact":
        monkeypatch.setattr(
            resolution,
            "discover_target",
            lambda connection: DiscoveredTarget("3.4.1", "product_info"),
        )
    else:
        _observed(monkeypatch)
    resolution.resolve_target(mode="runtime")
    calls = _observed(monkeypatch, candidates=(), operations=(), reported="9.0.0")
    resolution.resolve_target(mode="refresh")
    cached = resolution.resolve_target(mode="local")
    assert isinstance(cached, resolution.CompatibilityResolution)
    assert cached.candidate_versions == ()
    assert cached.reported_version == "9.0.0"
    assert cached.compatible_operations == ()
    assert len(calls) == 1
    data = meta.get_version_result().data
    assert isinstance(data, dict)
    assert data["ds"] is None
    assert data["identification"] == "unresolved"
    assert data["automatic_read_actions"] == []


def test_action_override_cannot_change_an_active_compatibility_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _observed(monkeypatch)
    with resolution.invocation_scope("project.list"):
        selected = resolution.resolve_runtime_selection()
        with pytest.raises(ConfigError) as error:
            resolution.resolve_runtime_selection(action="doctor")
        assert error.value.details["reason"] == "invocation_action_mismatch"
        assert resolution.resolve_runtime_selection() is selected
    assert len(calls) == 1


def test_failed_probe_clears_positive_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    _observed(monkeypatch)
    resolution.resolve_target(mode="runtime")

    def fail(connection: object) -> DiscoveredTarget:
        reason = "authentication_failed"
        raise VersionDiscoveryError(reason)

    monkeypatch.setattr(resolution, "discover_target", fail)
    with pytest.raises(ConfigError) as error:
        resolution.resolve_target(mode="runtime")
    assert error.value.details["reason"] == "authentication_failed"
    with pytest.raises(ConfigError) as local_error:
        resolution.resolve_target(mode="local")
    assert local_error.value.details["reason"] == "version_not_resolved"


def test_cache_cleanup_cannot_mask_discovery_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(connection: object) -> DiscoveredTarget:
        reason = "invalid_connection"
        raise VersionDiscoveryError(reason)

    def invalid_key(connection: object) -> str:
        message = "Invalid cache URL"
        raise ConfigError(message)

    monkeypatch.setattr(resolution, "discover_target", fail)
    monkeypatch.setattr(resolution, "_cache_key", invalid_key)
    with pytest.raises(ConfigError) as error:
        resolution.resolve_target(mode="runtime")
    assert error.value.details["reason"] == "invalid_connection"


def _unsupported_filter_payload() -> JsonObject:
    return error_payload(
        "task-instance.list",
        UnsupportedFeatureError(
            (
                "--workflow-instance-name is not available for task-instance.list "
                "on DolphinScheduler 1.3.9."
            ),
            details={
                "action": "task-instance.list",
                "flag": "--workflow-instance-name",
                "selected_version": "1.3.9",
                "reason": "upstream_capability_absent",
            },
        ),
        resolved={"project": {"id": 7}},
    )


def test_error_hook_labels_known_local_contract_failure_without_losing_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _observed(monkeypatch)
    payload = _unsupported_filter_payload()
    with resolution.invocation_scope("project.list"):
        resolution.resolve_runtime_selection()
        result = resolution.annotate_target_error(payload, None)
    resolved = result["resolved"]
    assert isinstance(resolved, dict)
    target = resolved["target"]
    assert isinstance(target, dict)
    assert target["ds_version"] is None
    assert resolved["project"] == {"id": 7}
    error = result["error"]
    assert isinstance(error, dict)
    assert error["message"] == (
        "--workflow-instance-name for task-instance.list is not available "
        "under the selected read contract."
    )
    details = error["details"]
    assert isinstance(details, dict)
    assert details["selected_version"] is None
    assert details["execution_contract_version"] == "1.3.9"
    original_error = payload["error"]
    assert isinstance(original_error, dict)
    original_details = original_error["details"]
    assert isinstance(original_details, dict)
    assert original_details["selected_version"] == "1.3.9"


@pytest.mark.parametrize("remote", [False, True])
def test_error_hook_preserves_unrelated_or_upstream_version_text(
    monkeypatch: pytest.MonkeyPatch, *, remote: bool
) -> None:
    _observed(monkeypatch)
    text = "User input named 'on DolphinScheduler 1.3.9' is invalid"
    failure = (
        ApiResultError(result_code=123, result_message=text)
        if remote
        else UserInputError(text, details={"selected_version": "1.3.9"})
    )
    payload = error_payload("project.list", failure)
    with resolution.invocation_scope("project.list"):
        resolution.resolve_runtime_selection()
        result = resolution.annotate_target_error(payload, None)
    assert result["error"] == payload["error"]


def test_error_hook_marks_projection_contract_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _observed(monkeypatch)
    payload = error_payload(
        "project.list",
        ApiTransportError(
            "DolphinScheduler returned an incompatible runtime-instance payload",
            details={
                "ds_version": "1.3.9",
                "resource": "task-instance",
                "field": "id",
                "reason": "missing",
            },
        ),
    )
    with resolution.invocation_scope("project.list"):
        resolution.resolve_runtime_selection()
        result = resolution.annotate_target_error(payload, None)
    error = result["error"]
    assert isinstance(error, dict)
    details = error["details"]
    assert isinstance(details, dict)
    assert details["ds_version"] is None
    assert details["execution_contract_version"] == "1.3.9"


def test_error_hook_path_failure_does_not_mask_original(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _observed(monkeypatch)
    payload = _unsupported_filter_payload()

    def unresolved(self: Path) -> Path:
        message = "path unavailable"
        raise OSError(message)

    with resolution.invocation_scope("project.list"):
        resolution.resolve_runtime_selection()
        monkeypatch.setattr(Path, "resolve", unresolved)
        assert resolution.annotate_target_error(payload, Path("changed.env")) == payload


@pytest.mark.parametrize(
    "command", [["project", "list"], ["task-instance", "log", "7", "--raw"]]
)
def test_structured_and_raw_cli_errors_keep_unknown_target_and_upstream_text(
    monkeypatch: pytest.MonkeyPatch, command: list[str]
) -> None:
    _observed(
        monkeypatch,
        operations=(
            "ProjectController.queryProjectListPaging",
            "LoggerController.queryLog",
        ),
    )
    message = "Upstream failed on DolphinScheduler 1.3.9"

    def failure(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"code": 999999, "msg": message, "data": None})

    _mock_http(monkeypatch, failure)
    result = CliRunner().invoke(app, command)
    assert result.exit_code == 1, result.output
    assert result.stdout == ""
    payload = json.loads(result.stderr)
    assert payload["resolved"]["target"]["ds_version"] is None
    assert payload["resolved"]["target"]["candidate_versions"] == ["1.3.9"]
    assert payload["error"]["source"]["result_message"] == message
    assert "private-test-token" not in result.stderr


def test_exact_error_hook_preserves_error_and_adds_selection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DS_VERSION", "1.3.9")
    payload = _unsupported_filter_payload()
    with resolution.invocation_scope("project.list"):
        resolution.resolve_runtime_selection()
        result = resolution.annotate_target_error(payload, None)
    assert result["error"] == payload["error"]
    resolved = require_json_object(result["resolved"], label="error resolved")
    selection = require_json_object(resolved["selection"], label="error selection")
    assert selection["source"] == "environment"
    assert "target" not in resolved


def test_known_contract_error_preserves_remote_source_and_original_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _observed(monkeypatch)
    payload = _unsupported_filter_payload()
    error = payload["error"]
    assert isinstance(error, dict)
    original_message = error["message"]
    source: JsonObject = {"layer": "result", "result_message": original_message}
    error["source"] = source
    with resolution.invocation_scope("project.list"):
        resolution.resolve_runtime_selection()
        result = resolution.annotate_target_error(payload, None)
    actual = result["error"]
    assert isinstance(actual, dict)
    assert actual["source"] == source
    assert actual["message"] == original_message


def test_non_scalar_reason_cannot_mask_original_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _observed(monkeypatch)
    payload = error_payload(
        "project.list",
        UnsupportedFeatureError(
            "Business rejection",
            details={
                "selected_version": "1.3.9",
                "reason": ["upstream_capability_absent"],
            },
        ),
    )
    with resolution.invocation_scope("project.list"):
        resolution.resolve_runtime_selection()
        result = resolution.annotate_target_error(payload, None)
    assert result["error"] == payload["error"]
