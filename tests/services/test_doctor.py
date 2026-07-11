from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

import pytest
from tests.support import make_profile

from dsctl.context import SessionContext
from dsctl.errors import ApiHttpError, ApiTransportError, ConfigError
from dsctl.services import doctor as doctor_service
from dsctl.upstream import get_version_support

if TYPE_CHECKING:
    from _pytest.monkeypatch import MonkeyPatch


@pytest.fixture(autouse=True)
def _isolate_context_layers(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setattr(
        doctor_service,
        "read_context_layer",
        _empty_context_layer,
    )


def _empty_context_layer(*, scope: str = "project") -> SessionContext:
    del scope
    return SessionContext()


@dataclass
class FakeDoctorClient:
    payload: dict[str, object] = field(default_factory=lambda: {"status": "UP"})
    error: Exception | None = None

    def __enter__(self) -> FakeDoctorClient:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None

    def healthcheck(self) -> dict[str, object]:
        if self.error is not None:
            raise self.error
        return dict(self.payload)


def _mapping(value: object) -> Mapping[str, object]:
    assert isinstance(value, Mapping)
    return value


def _sequence(value: object) -> Sequence[object]:
    assert isinstance(value, Sequence)
    assert not isinstance(value, (str, bytes, bytearray))
    return value


def test_get_doctor_result_reports_ok_profile_context_adapter_and_api(
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        doctor_service,
        "load_profile",
        lambda env_file=None: make_profile(),
    )
    monkeypatch.setattr(
        doctor_service,
        "load_context",
        lambda: SessionContext(
            project="etl-prod",
            workflow="daily-etl",
            set_at="2026-04-11T12:00:00+00:00",
        ),
    )
    monkeypatch.setattr(
        doctor_service,
        "DolphinSchedulerClient",
        lambda profile: FakeDoctorClient(
            payload={
                "status": "UP",
                "components": {"db": {"status": "UP"}},
            }
        ),
    )
    monkeypatch.setattr(
        doctor_service,
        "_current_user_defaults_details",
        lambda profile: {
            "userName": "alice",
            "tenantCode": "tenant-prod",
            "queue": "default",
            "queueName": "default",
            "timeZone": "Asia/Shanghai",
        },
    )

    result = doctor_service.get_doctor_result(env_file="cluster.env")
    data = _mapping(result.data)
    checks = _sequence(data["checks"])

    assert result.warnings == []
    assert result.warning_details == []
    assert data["status"] == "ok"
    assert data["summary"] == {"ok": 5, "warning": 0, "error": 0}

    profile_check = _mapping(checks[0])
    assert profile_check["name"] == "profile"
    assert profile_check["status"] == "ok"
    assert profile_check["suggestion"] is None
    assert (
        _mapping(profile_check["details"])["api_token"]
        == make_profile().redacted()["api_token"]
    )
    assert _mapping(profile_check["details"])["env_file"] == "cluster.env"

    context_check = _mapping(checks[1])
    assert context_check["name"] == "context"
    assert _mapping(context_check["details"])["project"] == "etl-prod"
    assert _mapping(context_check["details"])["workflow"] == "daily-etl"

    adapter_check = _mapping(checks[2])
    assert adapter_check["name"] == "adapter"
    assert _mapping(adapter_check["details"]) == {
        "ds_version": "3.4.1",
        "contract_version": "3.4.1",
        "family": "workflow-3.3-plus",
        "support_level": "full",
        "tested": True,
        "supported_versions": ["3.3.2", "3.4.0", "3.4.1"],
    }

    api_check = _mapping(checks[3])
    assert api_check["name"] == "api"
    assert api_check["status"] == "ok"
    assert _mapping(api_check["details"])["endpoint"] == (
        "http://example.test/dolphinscheduler/actuator/health"
    )

    current_user_check = _mapping(checks[4])
    assert current_user_check["name"] == "current_user"
    assert current_user_check["status"] == "ok"
    assert _mapping(current_user_check["details"]) == {
        "userName": "alice",
        "tenantCode": "tenant-prod",
        "queue": "default",
        "queueName": "default",
        "timeZone": "Asia/Shanghai",
    }


def test_context_check_warns_when_shadowed_user_layer_is_invalid(
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        doctor_service,
        "load_context",
        lambda: SessionContext(project="repo-project"),
    )

    def read_layer(*, scope: str = "project") -> SessionContext:
        if scope == "user":
            message = "User context workflow requires project"
            raise ConfigError(
                message,
                details={"scope": "user"},
                suggestion="Run `dsctl use --clear --scope user`.",
            )
        return SessionContext(project="repo-project")

    monkeypatch.setattr(doctor_service, "read_context_layer", read_layer)

    check = doctor_service._context_check()

    assert check["status"] == "warning"
    assert check["message"] == (
        "Effective context loaded, but a persisted context layer is invalid."
    )
    assert check["suggestion"] == "Run `dsctl use --clear --scope user`."
    details = _mapping(check["details"])
    assert details["project"] == "repo-project"
    assert _mapping(details["layer_errors"])["user"] == {
        "type": "config_error",
        "message": "User context workflow requires project",
        "details": {"scope": "user"},
        "suggestion": "Run `dsctl use --clear --scope user`.",
    }


def test_context_check_reports_every_invalid_layer_when_effective_load_fails(
    monkeypatch: MonkeyPatch,
) -> None:
    project_error = ConfigError(
        "Project context is invalid",
        details={"scope": "project"},
        suggestion="Clear project context.",
    )
    monkeypatch.setattr(
        doctor_service,
        "load_context",
        lambda: (_ for _ in ()).throw(project_error),
    )

    def read_layer(*, scope: str = "project") -> SessionContext:
        message = f"{scope.title()} context is invalid"
        raise ConfigError(
            message,
            details={"scope": scope},
            suggestion=f"Clear {scope} context.",
        )

    monkeypatch.setattr(doctor_service, "read_context_layer", read_layer)

    check = doctor_service._context_check()

    assert check["status"] == "error"
    assert check["suggestion"] == "Clear project context."
    details = _mapping(check["details"])
    layer_errors = _mapping(details["layer_errors"])
    assert set(layer_errors) == {"project", "user"}
    assert _mapping(layer_errors["project"])["message"] == (
        "Project context is invalid"
    )
    assert _mapping(layer_errors["user"])["message"] == "User context is invalid"


def test_get_doctor_result_warns_for_experimental_unverified_target(
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        doctor_service,
        "load_profile",
        lambda env_file=None: make_profile(ds_version="3.3.2"),
    )
    monkeypatch.setattr(doctor_service, "load_context", SessionContext)
    monkeypatch.setattr(
        doctor_service,
        "DolphinSchedulerClient",
        lambda profile: FakeDoctorClient(),
    )
    monkeypatch.setattr(
        doctor_service,
        "_current_user_defaults_details",
        lambda profile: {
            "userName": "alice",
            "tenantCode": "tenant-prod",
            "queue": "default",
            "queueName": "default",
            "timeZone": "Asia/Shanghai",
        },
    )

    result = doctor_service.get_doctor_result()
    data = _mapping(result.data)
    adapter_check = _mapping(_sequence(data["checks"])[2])
    message = (
        "DS 3.3.2 support is experimental and has not passed "
        "release-specific live smoke testing."
    )
    suggestion = (
        "Set `DS_VERSION=3.4.1` and rerun `dsctl doctor`, or complete the "
        "DS 3.3.2 live smoke suite before relying on this target."
    )

    assert data["status"] == "warning"
    assert data["summary"] == {"ok": 4, "warning": 1, "error": 0}
    assert result.warnings == [f"doctor adapter: {message}"]
    assert result.warning_details == [
        {
            "code": "doctor_check_not_ok",
            "check": "adapter",
            "status": "warning",
            "message": message,
            "suggestion": suggestion,
        }
    ]
    assert adapter_check["status"] == "warning"
    assert adapter_check["message"] == message
    assert adapter_check["suggestion"] == suggestion
    assert _mapping(adapter_check["details"]) == {
        "ds_version": "3.3.2",
        "contract_version": "3.4.1",
        "family": "workflow-3.3-plus",
        "support_level": "experimental",
        "tested": False,
        "supported_versions": ["3.3.2", "3.4.0", "3.4.1"],
    }


def test_adapter_check_warns_for_experimental_target_with_test_evidence(
    monkeypatch: MonkeyPatch,
) -> None:
    support = replace(
        get_version_support("3.3.2"),
        tested=True,
    )
    monkeypatch.setattr(
        doctor_service,
        "get_version_support",
        lambda _version: support,
    )

    check = doctor_service._adapter_check(make_profile(ds_version="3.3.2"))

    assert check["status"] == "warning"
    assert "support is experimental" in check["message"]
    assert _mapping(check["details"])["tested"] is True
    assert check["suggestion"] is not None


def test_get_doctor_result_reports_profile_error_and_skips_api(
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        doctor_service,
        "load_profile",
        lambda env_file=None: (_ for _ in ()).throw(
            ConfigError(
                "Missing required setting: DS_API_URL",
                details={"key": "DS_API_URL"},
            )
        ),
    )
    monkeypatch.setattr(
        doctor_service,
        "load_context",
        lambda: SessionContext(project="etl-prod"),
    )

    result = doctor_service.get_doctor_result(env_file="cluster.env")
    data = _mapping(result.data)
    checks = _sequence(data["checks"])

    assert data["status"] == "error"
    assert data["summary"] == {"ok": 2, "warning": 2, "error": 1}
    assert result.warnings == [
        "doctor profile: Missing required setting: DS_API_URL",
        "doctor api: Skipped because profile check failed.",
        "doctor current_user: Skipped because profile check failed.",
    ]
    assert result.warning_details == [
        {
            "code": "doctor_check_not_ok",
            "check": "profile",
            "status": "error",
            "message": "Missing required setting: DS_API_URL",
            "suggestion": (
                "Set DS_API_URL and DS_API_TOKEN in the profile or pass "
                "--env-file PATH."
            ),
        },
        {
            "code": "doctor_check_not_ok",
            "check": "api",
            "status": "warning",
            "message": "Skipped because profile check failed.",
            "suggestion": "Fix the profile check first, then rerun `dsctl doctor`.",
        },
        {
            "code": "doctor_check_not_ok",
            "check": "current_user",
            "status": "warning",
            "message": "Skipped because profile check failed.",
            "suggestion": "Fix the profile check first, then rerun `dsctl doctor`.",
        },
    ]

    profile_check = _mapping(checks[0])
    assert profile_check["status"] == "error"
    assert profile_check["suggestion"] == (
        "Set DS_API_URL and DS_API_TOKEN in the profile or pass --env-file PATH."
    )
    assert _mapping(_mapping(profile_check["details"])["error"]) == {
        "type": "config_error",
        "message": "Missing required setting: DS_API_URL",
        "details": {"key": "DS_API_URL"},
    }

    api_check = _mapping(checks[3])
    assert api_check["status"] == "warning"
    assert api_check["suggestion"] == (
        "Fix the profile check first, then rerun `dsctl doctor`."
    )
    assert _mapping(api_check["details"]) == {
        "reason": "profile_check_failed",
    }

    current_user_check = _mapping(checks[4])
    assert current_user_check["status"] == "warning"
    assert current_user_check["suggestion"] == (
        "Fix the profile check first, then rerun `dsctl doctor`."
    )
    assert _mapping(current_user_check["details"]) == {
        "reason": "profile_check_failed",
    }


def test_get_doctor_result_reports_unhealthy_api_status_as_warning(
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        doctor_service,
        "load_profile",
        lambda env_file=None: make_profile(),
    )
    monkeypatch.setattr(
        doctor_service,
        "load_context",
        SessionContext,
    )
    monkeypatch.setattr(
        doctor_service,
        "DolphinSchedulerClient",
        lambda profile: FakeDoctorClient(
            payload={
                "status": "DOWN",
                "components": {"db": {"status": "DOWN"}},
            }
        ),
    )
    monkeypatch.setattr(
        doctor_service,
        "_current_user_defaults_details",
        lambda profile: {
            "userName": "alice",
            "tenantCode": "tenant-prod",
            "queue": "default",
            "queueName": "default",
            "timeZone": "Asia/Shanghai",
        },
    )

    result = doctor_service.get_doctor_result()
    data = _mapping(result.data)
    checks = _sequence(data["checks"])
    api_check = _mapping(checks[3])

    assert data["status"] == "warning"
    assert data["summary"] == {"ok": 4, "warning": 1, "error": 0}
    assert result.warnings == ["doctor api: API actuator health is DOWN."]
    assert result.warning_details == [
        {
            "code": "doctor_check_not_ok",
            "check": "api",
            "status": "warning",
            "message": "API actuator health is DOWN.",
            "suggestion": (
                "Inspect the DolphinScheduler API service and actuator health "
                "payload, then rerun `dsctl doctor`."
            ),
        }
    ]
    assert api_check["status"] == "warning"
    assert api_check["suggestion"] == (
        "Inspect the DolphinScheduler API service and actuator health payload, "
        "then rerun `dsctl doctor`."
    )
    assert _mapping(api_check["details"])["payload"] == {
        "status": "DOWN",
        "components": {"db": {"status": "DOWN"}},
    }


def test_get_doctor_result_reports_transport_failures_on_api_check(
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        doctor_service,
        "load_profile",
        lambda env_file=None: make_profile(),
    )
    monkeypatch.setattr(
        doctor_service,
        "load_context",
        SessionContext,
    )
    monkeypatch.setattr(
        doctor_service,
        "DolphinSchedulerClient",
        lambda profile: FakeDoctorClient(
            error=ApiTransportError("Connection refused"),
        ),
    )
    monkeypatch.setattr(
        doctor_service,
        "_current_user_defaults_details",
        lambda profile: {
            "userName": "alice",
            "tenantCode": "tenant-prod",
            "queue": "default",
            "queueName": "default",
            "timeZone": "Asia/Shanghai",
        },
    )

    result = doctor_service.get_doctor_result()
    data = _mapping(result.data)
    checks = _sequence(data["checks"])
    api_check = _mapping(checks[3])

    assert data["status"] == "error"
    assert result.warnings == ["doctor api: Connection refused"]
    assert result.warning_details == [
        {
            "code": "doctor_check_not_ok",
            "check": "api",
            "status": "error",
            "message": "Connection refused",
            "suggestion": (
                "Check DS_API_URL, network reachability, and the API token, "
                "then rerun `dsctl doctor`."
            ),
        }
    ]
    assert api_check["status"] == "error"
    assert api_check["suggestion"] == (
        "Check DS_API_URL, network reachability, and the API token, then rerun "
        "`dsctl doctor`."
    )
    assert _mapping(api_check["details"])["endpoint"] == (
        "http://example.test/dolphinscheduler/actuator/health"
    )
    assert _mapping(_mapping(api_check["details"])["error"]) == {
        "type": "api_transport_error",
        "message": "Connection refused",
    }


def test_get_doctor_result_preserves_remote_source_in_nested_api_error(
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        doctor_service,
        "load_profile",
        lambda env_file=None: make_profile(),
    )
    monkeypatch.setattr(
        doctor_service,
        "load_context",
        SessionContext,
    )
    monkeypatch.setattr(
        doctor_service,
        "DolphinSchedulerClient",
        lambda profile: FakeDoctorClient(
            error=ApiHttpError(
                "Forbidden",
                status_code=403,
                body={"msg": "denied"},
            ),
        ),
    )
    monkeypatch.setattr(
        doctor_service,
        "_current_user_defaults_details",
        lambda profile: {
            "userName": "alice",
            "tenantCode": "tenant-prod",
            "queue": "default",
            "queueName": "default",
            "timeZone": "Asia/Shanghai",
        },
    )

    result = doctor_service.get_doctor_result()
    data = _mapping(result.data)
    checks = _sequence(data["checks"])
    api_check = _mapping(checks[3])
    error = _mapping(_mapping(api_check["details"])["error"])

    assert error["type"] == "api_http_error"
    assert error["source"] == {
        "kind": "remote",
        "system": "dolphinscheduler",
        "layer": "http",
        "status_code": 403,
    }
