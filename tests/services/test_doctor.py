from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

import pytest
from tests.support import make_profile
from tests.value_shape_assertions import assert_mapping as _mapping
from tests.value_shape_assertions import assert_sequence as _sequence

from dsctl.config import TargetSettings
from dsctl.errors import ApiHttpError, ApiTransportError, ConfigError
from dsctl.services import doctor as doctor_service
from dsctl.services.version_resolution import VersionResolution
from dsctl.upstream import (
    SUPPORTED_VERSIONS,
    Availability,
    get_action_capability,
    get_version_support,
)

if TYPE_CHECKING:
    from _pytest.monkeypatch import MonkeyPatch


@pytest.fixture(autouse=True)
def _isolate_selected_settings(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setattr(
        doctor_service,
        "resolve_settings",
        _empty_settings,
    )
    monkeypatch.setattr(
        doctor_service,
        "resolve_version",
        lambda env_file=None, mode="local": VersionResolution("3.4.1", "explicit"),
    )


def _empty_settings(env_file: str | None = None) -> TargetSettings:
    del env_file
    return TargetSettings()


def test_unexpected_context_failure_retains_other_checks_without_exception_text(
    monkeypatch: MonkeyPatch,
) -> None:
    def broken_settings(env_file: str | None = None) -> TargetSettings:
        message = "unexpected profile contents: secret-token-value"
        raise RuntimeError(message)

    monkeypatch.setattr(doctor_service, "resolve_settings", broken_settings)
    monkeypatch.setattr(
        doctor_service,
        "resolve_profile",
        lambda env_file=None, mode="refresh": make_profile(),
    )
    monkeypatch.setattr(
        doctor_service, "DolphinSchedulerClient", lambda profile: FakeDoctorClient()
    )
    monkeypatch.setattr(
        doctor_service, "_current_user_defaults_details", lambda profile: {}
    )
    result = doctor_service.get_doctor_result()
    checks = [_mapping(check) for check in _sequence(_mapping(result.data)["checks"])]
    assert len(checks) == 5
    assert result.failure is not None
    assert [check["name"] for check in checks if check["status"] == "error"] == [
        "context"
    ]
    assert "RuntimeError" in str(checks[1])
    assert "secret-token-value" not in str(result)
    assert checks[1]["suggestion"] == (
        "Inspect `dsctl config get default-context`, `dsctl context list` "
        "and the selected connection file."
    )


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


def test_get_doctor_result_reports_ok_profile_context_adapter_and_api(
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        doctor_service,
        "resolve_profile",
        lambda env_file=None, mode="refresh": make_profile(),
    )
    monkeypatch.setattr(
        doctor_service,
        "resolve_settings",
        lambda env_file=None: TargetSettings(project="etl-prod"),
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
            "userType": "GENERAL_USER",
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
    assert "workflow" not in _mapping(context_check["details"])

    adapter_check = _mapping(checks[2])
    assert adapter_check["name"] == "adapter"
    assert _mapping(adapter_check["details"]) == {
        "ds_version": "3.4.1",
        "contract_version": "3.4.1",
        "family": "workflow-3.3-plus",
        "support_level": "full",
        "tested": True,
        "supported_versions": list(SUPPORTED_VERSIONS),
        "catalog": get_version_support("3.4.1").catalog.summary_metadata(),
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
        "userType": "GENERAL_USER",
        "tenantCode": "tenant-prod",
        "queue": "default",
        "queueName": "default",
        "timeZone": "Asia/Shanghai",
    }


def test_doctor_uses_exact_identity_without_promoting_static_profile_evidence(
    monkeypatch: MonkeyPatch,
) -> None:
    profile = make_profile(ds_version="3.2.2")
    monkeypatch.setattr(
        doctor_service,
        "resolve_profile",
        lambda env_file=None, mode="refresh": profile,
    )
    monkeypatch.setattr(doctor_service, "resolve_settings", _empty_settings)
    monkeypatch.setattr(
        doctor_service,
        "DolphinSchedulerClient",
        lambda _profile: FakeDoctorClient(),
    )

    profiles_seen: list[object] = []

    def current_user_details(profile_value: object) -> dict[str, str | None]:
        profiles_seen.append(profile_value)
        return {
            "userName": "admin",
            "userType": "ADMIN_USER",
            "tenantCode": "tenant-a",
            "queue": "default",
            "queueName": "default",
            "timeZone": "UTC",
        }

    monkeypatch.setattr(
        doctor_service,
        "_current_user_defaults_details",
        current_user_details,
    )

    result = doctor_service.get_doctor_result(env_file="cluster.env")
    checks = {
        str(_mapping(item)["name"]): _mapping(item)
        for item in _sequence(_mapping(result.data)["checks"])
    }
    current_user_check = checks["current_user"]

    assert current_user_check["status"] == "ok"
    assert _mapping(current_user_check["details"])["userName"] == "admin"
    assert profiles_seen == [profile]
    doctor_capability = get_version_support("3.2.2").catalog.entries["doctor"]
    assert doctor_capability.verification.value == "static"


def test_doctor_uses_identity_port_for_partial_342_profile(
    monkeypatch: MonkeyPatch,
) -> None:
    profile = make_profile(ds_version="3.4.2")
    monkeypatch.setattr(
        doctor_service,
        "_current_user_defaults_details",
        lambda _profile: {
            "userName": "admin",
            "tenantCode": "tenant-a",
            "queue": "default",
            "queueName": "default",
            "timeZone": "Asia/Shanghai",
        },
    )

    check = doctor_service._current_user_check(profile)

    assert check["status"] == "ok"
    assert _mapping(check["details"])["userName"] == "admin"


def test_current_user_details_include_ds_native_user_type(
    monkeypatch: MonkeyPatch,
) -> None:
    current_user = type(
        "CurrentUser",
        (),
        {
            "userName": "alice",
            "userType": "GENERAL_USER",
            "tenantCode": "tenant-a",
            "queue": "default",
            "queueName": "default",
            "timeZone": "Asia/Shanghai",
        },
    )()
    operations = type(
        "IdentityOperations",
        (),
        {"current": lambda self: current_user},
    )()
    adapter = type(
        "IdentityAdapter",
        (),
        {"bind_identity": lambda self, profile, http_client: operations},
    )()
    monkeypatch.setattr(doctor_service, "get_identity_adapter", lambda version: adapter)
    monkeypatch.setattr(
        doctor_service,
        "DolphinSchedulerClient",
        lambda profile: FakeDoctorClient(),
    )

    details = doctor_service._current_user_defaults_details(make_profile())

    assert details["userType"] == "GENERAL_USER"


def test_context_check_reports_selected_context_and_redacts_credentials(
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        doctor_service,
        "resolve_settings",
        lambda env_file=None: TargetSettings(
            context_name="production",
            project="etl-prod",
            api_url="https://production.example/dolphinscheduler",
            api_token="private-token",
            env_file="/profiles/production.env",
            source="default",
        ),
    )

    check = doctor_service._context_check(env_file=None)

    assert check["status"] == "ok"
    assert check["details"] == {
        "source": "default",
        "context": "production",
        "env_file": "/profiles/production.env",
        "api_url": "https://production.example/dolphinscheduler",
        "project": "etl-prod",
    }
    assert "private-token" not in str(check)
    assert "api_token" not in _mapping(check["details"])


def test_context_check_reports_selected_source_failure(
    monkeypatch: MonkeyPatch,
) -> None:
    selection_error = ConfigError(
        "Selected context connection file is unavailable",
        details={"context": "production", "env_file": "/profiles/missing.env"},
        suggestion="Run `dsctl context update production --file FILE`.",
    )
    monkeypatch.setattr(
        doctor_service,
        "resolve_settings",
        lambda env_file=None: (_ for _ in ()).throw(selection_error),
    )

    check = doctor_service._context_check(env_file=None)

    assert check["status"] == "error"
    assert check["suggestion"] == selection_error.suggestion
    assert check["message"] == selection_error.message
    assert _mapping(_mapping(_mapping(check["details"])["error"])["details"]) == {
        "context": "production",
        "env_file": "/profiles/missing.env",
    }
    assert "layer_errors" not in _mapping(check["details"])


def test_get_doctor_result_warns_for_experimental_partially_verified_target(
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        doctor_service,
        "resolve_profile",
        lambda env_file=None, mode="refresh": make_profile(ds_version="3.3.2"),
    )
    monkeypatch.setattr(doctor_service, "resolve_settings", _empty_settings)
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
        "DS 3.3.2 support is experimental: 4 actions have live evidence, but "
        "the profile has not passed its complete release gate."
    )
    suggestion = (
        "Use `dsctl capabilities --action ACTION` to verify the required "
        "operation for DS 3.3.2 before relying on this target."
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
        "contract_version": "3.3.2",
        "family": "workflow-3.3-plus",
        "support_level": "experimental",
        "tested": False,
        "supported_versions": list(SUPPORTED_VERSIONS),
        "catalog": get_version_support("3.3.2").catalog.summary_metadata(),
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


def test_adapter_check_reports_incomplete_release_gate_despite_read_evidence() -> None:
    check = doctor_service._adapter_check(make_profile(ds_version="3.4.2"))

    assert check["status"] == "warning"
    assert check["message"] == (
        "DS 3.4.2 support is experimental: 15 actions have live evidence, but "
        "the profile has not passed its complete release gate."
    )
    assert check["suggestion"] == (
        "Use `dsctl capabilities --action ACTION` to verify the required "
        "operation for DS 3.4.2 before relying on this target."
    )


def test_get_doctor_result_reports_profile_error_and_skips_api(
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        doctor_service,
        "resolve_profile",
        lambda env_file=None, mode="refresh": (_ for _ in ()).throw(
            ConfigError(
                "Missing required setting: DS_API_URL",
                details={"key": "DS_API_URL"},
            )
        ),
    )
    monkeypatch.setattr(
        doctor_service,
        "resolve_settings",
        lambda env_file=None: TargetSettings(project="etl-prod"),
    )

    result = doctor_service.get_doctor_result(env_file="cluster.env")
    data = _mapping(result.data)
    checks = _sequence(data["checks"])

    assert data["status"] == "error"
    assert data["summary"] == {"ok": 1, "warning": 3, "error": 1}
    assert result.warnings == [
        "doctor profile: Missing required setting: DS_API_URL",
        (
            "doctor adapter: Adapter validation skipped because "
            "the target version was not resolved."
        ),
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
            "check": "adapter",
            "status": "warning",
            "message": (
                "Adapter validation skipped because "
                "the target version was not resolved."
            ),
            "suggestion": "Resolve the profile diagnostic, then rerun `dsctl doctor`.",
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


def test_get_doctor_result_reports_unhealthy_api_status_as_error(
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        doctor_service,
        "resolve_profile",
        lambda env_file=None, mode="refresh": make_profile(),
    )
    monkeypatch.setattr(
        doctor_service,
        "resolve_settings",
        _empty_settings,
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

    assert data["status"] == "error"
    assert data["summary"] == {"ok": 4, "warning": 0, "error": 1}
    assert result.warnings == ["doctor api: API actuator health is DOWN."]
    assert result.warning_details == [
        {
            "code": "doctor_check_not_ok",
            "check": "api",
            "status": "error",
            "message": "API actuator health is DOWN.",
            "suggestion": (
                "Inspect the DolphinScheduler API service and actuator health "
                "payload, then rerun `dsctl doctor`."
            ),
        }
    ]
    assert api_check["status"] == "error"
    assert api_check["suggestion"] == (
        "Inspect the DolphinScheduler API service and actuator health payload, "
        "then rerun `dsctl doctor`."
    )
    assert _mapping(api_check["details"])["payload"] == {
        "status": "DOWN",
        "components": {"db": {"status": "DOWN"}},
    }


@pytest.mark.parametrize(
    "version",
    [
        pytest.param(version, id=version)
        for version in SUPPORTED_VERSIONS
        if get_action_capability(version, "monitor.health").availability
        is not Availability.SUPPORTED
    ],
)
def test_doctor_records_upstream_absent_actuator_as_warning(
    monkeypatch: MonkeyPatch,
    version: str,
) -> None:
    profile = make_profile(ds_version=version)
    monkeypatch.setattr(
        doctor_service,
        "resolve_profile",
        lambda env_file=None, mode="refresh": profile,
    )
    monkeypatch.setattr(doctor_service, "resolve_settings", _empty_settings)

    def unexpected_health_client(profile_value: object) -> FakeDoctorClient:
        del profile_value
        message = "legacy exact profiles must not call Actuator"
        raise AssertionError(message)

    monkeypatch.setattr(
        doctor_service,
        "DolphinSchedulerClient",
        unexpected_health_client,
    )
    current_user_profiles: list[object] = []

    def current_user_details(profile_value: object) -> dict[str, str | None]:
        current_user_profiles.append(profile_value)
        return {
            "userName": "alice",
            "userType": "GENERAL_USER",
            "tenantCode": "tenant-prod",
            "queue": "default",
            "queueName": "default",
            "timeZone": None,
        }

    monkeypatch.setattr(
        doctor_service,
        "_current_user_defaults_details",
        current_user_details,
    )

    result = doctor_service.get_doctor_result()
    result_data = _mapping(result.data)
    checks = {
        check["name"]: check
        for raw in _sequence(result_data["checks"])
        for check in [_mapping(raw)]
    }
    api_check = _mapping(checks["api"])
    current_user_check = _mapping(checks["current_user"])
    health_capability = get_action_capability(version, "monitor.health")

    assert current_user_profiles == [profile]
    assert result_data["status"] == "warning"
    assert _mapping(result_data["summary"])["error"] == 0
    assert api_check["status"] == "warning"
    assert api_check["message"] == health_capability.constraint
    assert api_check["suggestion"] == (
        "Use the authenticated current-user check in this report to verify API "
        "readiness."
    )
    assert _mapping(api_check["details"]) == {
        "reason": "upstream_endpoint_absent",
        "ds_version": version,
        "operation": "monitor.health",
        "availability": "unsupported",
        "constraint": health_capability.constraint,
        "endpoint": profile.health_url,
        "verification_fallback": "current_user",
    }
    assert current_user_check["status"] == "ok"
    assert _mapping(current_user_check["details"])["userName"] == "alice"


def test_get_doctor_result_reports_transport_failures_on_api_check(
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        doctor_service,
        "resolve_profile",
        lambda env_file=None, mode="refresh": make_profile(),
    )
    monkeypatch.setattr(
        doctor_service,
        "resolve_settings",
        _empty_settings,
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
        "resolve_profile",
        lambda env_file=None, mode="refresh": make_profile(),
    )
    monkeypatch.setattr(
        doctor_service,
        "resolve_settings",
        _empty_settings,
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


def test_doctor_refresh_failure_does_not_validate_a_default_adapter(
    monkeypatch: MonkeyPatch,
) -> None:
    calls: list[tuple[str | None, str]] = []

    def unresolved_profile(env_file: str | None = None, *, mode: str) -> object:
        calls.append((env_file, mode))
        message = "The server did not identify an exact DolphinScheduler version."
        raise ConfigError(
            message,
            details={"reason": "version_discovery_failed"},
            suggestion=(
                "Verify the server version before setting DS_VERSION explicitly."
            ),
        )

    def unexpected_adapter(version: str) -> object:
        message = f"An unresolved target must not validate {version}"
        raise AssertionError(message)

    monkeypatch.setattr(doctor_service, "resolve_profile", unresolved_profile)
    monkeypatch.setattr(doctor_service, "get_version_support", unexpected_adapter)
    monkeypatch.setattr(doctor_service, "resolve_settings", _empty_settings)

    data = _mapping(doctor_service.get_doctor_result(env_file="auto.env").data)
    checks = [_mapping(check) for check in _sequence(data["checks"])]

    assert calls == [("auto.env", "refresh")]
    assert data["status"] == "error"
    assert data["summary"] == {"ok": 1, "warning": 3, "error": 1}
    profile_details = _mapping(checks[0]["details"])
    assert profile_details["env_file"] == "auto.env"
    error = _mapping(profile_details["error"])
    assert error["type"] == "config_error"
    assert error["details"] == {"reason": "version_discovery_failed"}
    assert checks[2]["details"] == {"skipped": True}
    assert all("3.4.1" not in str(check) for check in checks)
