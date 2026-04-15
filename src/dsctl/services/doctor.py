from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, TypedDict

from dsctl.client import DolphinSchedulerClient
from dsctl.config import ClusterProfile, load_profile
from dsctl.context import (
    SessionContext,
    load_context,
    project_context_path,
    user_context_path,
)
from dsctl.errors import DsctlError
from dsctl.output import CommandResult, require_json_object
from dsctl.services._serialization import optional_text
from dsctl.upstream import (
    SUPPORTED_VERSIONS,
    get_default_version_support,
    get_version_support,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

DoctorStatus = Literal["ok", "warning", "error"]


class DoctorCheckData(TypedDict):
    """One structured diagnostic check emitted by `dsctl doctor`."""

    name: str
    status: DoctorStatus
    message: str
    suggestion: str | None
    details: Mapping[str, object]


class DoctorSummaryData(TypedDict):
    """Aggregate status counts for one doctor run."""

    ok: int
    warning: int
    error: int


class DoctorData(TypedDict):
    """Stable payload returned by the doctor command."""

    status: DoctorStatus
    summary: DoctorSummaryData
    checks: list[DoctorCheckData]


@dataclass(frozen=True)
class _ProfileCheckResult:
    profile: ClusterProfile | None
    check: DoctorCheckData


class DoctorWarningDetail(TypedDict):
    """Structured warning derived from one non-ok doctor check."""

    code: str
    check: str
    status: DoctorStatus
    message: str
    suggestion: str | None


def get_doctor_result(*, env_file: str | None = None) -> CommandResult:
    """Return a structured runtime and local-environment diagnostic report."""
    profile_result = _profile_check(env_file=env_file)
    checks = [
        profile_result.check,
        _context_check(),
        _adapter_check(profile_result.profile),
        (
            _api_health_check(profile_result.profile)
            if profile_result.profile is not None
            else _skipped_api_health_check()
        ),
        (
            _current_user_check(profile_result.profile)
            if profile_result.profile is not None
            else _skipped_current_user_check()
        ),
    ]
    warnings, warning_details = _doctor_warning_payloads(checks)
    return CommandResult(
        data=require_json_object(_doctor_data(checks), label="doctor data"),
        warnings=warnings,
        warning_details=warning_details,
    )


def _profile_check(*, env_file: str | None) -> _ProfileCheckResult:
    try:
        profile = load_profile(env_file)
    except DsctlError as exc:
        return _ProfileCheckResult(
            profile=None,
            check=_doctor_check(
                "profile",
                "error",
                exc.message,
                _error_details(exc, env_file=env_file),
                suggestion=_error_suggestion(
                    exc,
                    fallback=(
                        "Set DS_API_URL and DS_API_TOKEN in the profile or pass "
                        "--env-file PATH."
                    ),
                ),
            ),
        )
    except Exception as exc:  # pragma: no cover - defensive doctor fallback
        return _ProfileCheckResult(
            profile=None,
            check=_doctor_check(
                "profile",
                "error",
                _unexpected_message(exc),
                _error_details(exc, env_file=env_file),
                suggestion=(
                    "Inspect local DS profile settings and rerun `dsctl doctor`."
                ),
            ),
        )

    details = dict(profile.redacted())
    if env_file is not None:
        details["env_file"] = env_file
    return _ProfileCheckResult(
        profile=profile,
        check=_doctor_check(
            "profile",
            "ok",
            "Profile loaded.",
            details,
        ),
    )


def _context_check() -> DoctorCheckData:
    try:
        session = load_context()
    except DsctlError as exc:
        return _doctor_check(
            "context",
            "error",
            exc.message,
            _error_details(exc),
            suggestion=_error_suggestion(
                exc,
                fallback="Fix or remove the invalid context file and retry.",
            ),
        )
    except Exception as exc:  # pragma: no cover - defensive doctor fallback
        return _doctor_check(
            "context",
            "error",
            _unexpected_message(exc),
            _error_details(exc),
            suggestion="Fix or remove the invalid context file and retry.",
        )

    if _context_is_empty(session):
        message = "No persisted context is set."
    else:
        message = "Context loaded."
    return _doctor_check(
        "context",
        "ok",
        message,
        {
            "project": session.project,
            "workflow": session.workflow,
            "set_at": session.set_at,
            "user_path": str(user_context_path()),
            "project_path": str(project_context_path()),
        },
    )


def _adapter_check(profile: ClusterProfile | None) -> DoctorCheckData:
    selected_version = (
        get_default_version_support().server_version
        if profile is None
        else profile.ds_version
    )
    try:
        support = get_version_support(selected_version)
    except DsctlError as exc:
        return _doctor_check(
            "adapter",
            "error",
            exc.message,
            _error_details(exc),
            suggestion=_error_suggestion(
                exc,
                fallback="Use one of the supported DolphinScheduler versions.",
            ),
        )
    except Exception as exc:  # pragma: no cover - defensive doctor fallback
        return _doctor_check(
            "adapter",
            "error",
            _unexpected_message(exc),
            _error_details(exc),
            suggestion="Use one of the supported DolphinScheduler versions.",
        )

    return _doctor_check(
        "adapter",
        "ok",
        "Adapter resolved.",
        {
            "ds_version": support.server_version,
            "contract_version": support.contract_version,
            "family": support.family,
            "support_level": support.support_level,
            "tested": support.tested,
            "supported_versions": list(SUPPORTED_VERSIONS),
        },
    )


def _api_health_check(profile: ClusterProfile) -> DoctorCheckData:
    try:
        with DolphinSchedulerClient(profile) as http_client:
            payload = require_json_object(
                http_client.healthcheck(),
                label="doctor api health payload",
            )
    except DsctlError as exc:
        return _doctor_check(
            "api",
            "error",
            exc.message,
            _error_details(exc, endpoint=profile.health_url),
            suggestion=_error_suggestion(
                exc,
                fallback=(
                    "Check DS_API_URL, network reachability, and the API token, "
                    "then rerun `dsctl doctor`."
                ),
            ),
        )
    except Exception as exc:  # pragma: no cover - defensive doctor fallback
        return _doctor_check(
            "api",
            "error",
            _unexpected_message(exc),
            _error_details(exc, endpoint=profile.health_url),
            suggestion=(
                "Check DS_API_URL, network reachability, and the API token, "
                "then rerun `dsctl doctor`."
            ),
        )

    health_status = payload.get("status")
    if health_status == "UP":
        return _doctor_check(
            "api",
            "ok",
            "API actuator health is UP.",
            {
                "endpoint": profile.health_url,
                "payload": payload,
            },
        )
    return _doctor_check(
        "api",
        "warning",
        (
            "API actuator health returned a non-UP status."
            if health_status is None
            else f"API actuator health is {health_status}."
        ),
        {
            "endpoint": profile.health_url,
            "payload": payload,
        },
        suggestion=(
            "Inspect the DolphinScheduler API service and actuator health payload, "
            "then rerun `dsctl doctor`."
        ),
    )


def _skipped_api_health_check() -> DoctorCheckData:
    return _doctor_check(
        "api",
        "warning",
        "Skipped because profile check failed.",
        {
            "reason": "profile_check_failed",
        },
        suggestion="Fix the profile check first, then rerun `dsctl doctor`.",
    )


def _current_user_check(profile: ClusterProfile) -> DoctorCheckData:
    try:
        details = _current_user_defaults_details(profile)
    except DsctlError as exc:
        return _doctor_check(
            "current_user",
            "error",
            exc.message,
            _error_details(exc),
            suggestion=_error_suggestion(
                exc,
                fallback=(
                    "Verify DS_API_TOKEN belongs to an active user and can call "
                    "the current-user endpoint."
                ),
            ),
        )
    except Exception as exc:  # pragma: no cover - defensive doctor fallback
        return _doctor_check(
            "current_user",
            "error",
            _unexpected_message(exc),
            _error_details(exc),
            suggestion=(
                "Verify DS_API_TOKEN belongs to an active user and can call "
                "the current-user endpoint."
            ),
        )

    return _doctor_check(
        "current_user",
        "ok",
        "Current user defaults loaded.",
        details,
    )


def _skipped_current_user_check() -> DoctorCheckData:
    return _doctor_check(
        "current_user",
        "warning",
        "Skipped because profile check failed.",
        {
            "reason": "profile_check_failed",
        },
        suggestion="Fix the profile check first, then rerun `dsctl doctor`.",
    )


def _current_user_defaults_details(profile: ClusterProfile) -> dict[str, str | None]:
    adapter = get_version_support(profile.ds_version).adapter
    with DolphinSchedulerClient(profile) as http_client:
        current_user = adapter.bind(profile, http_client=http_client).users.current()
    return {
        "userName": optional_text(current_user.userName),
        "tenantCode": optional_text(current_user.tenantCode),
        "queue": optional_text(current_user.queue),
        "queueName": optional_text(current_user.queueName),
        "timeZone": optional_text(current_user.timeZone),
    }


def _doctor_data(checks: list[DoctorCheckData]) -> DoctorData:
    summary = _doctor_summary(checks)
    return {
        "status": _doctor_status(summary),
        "summary": summary,
        "checks": checks,
    }


def _doctor_summary(checks: list[DoctorCheckData]) -> DoctorSummaryData:
    return {
        "ok": sum(1 for check in checks if check["status"] == "ok"),
        "warning": sum(1 for check in checks if check["status"] == "warning"),
        "error": sum(1 for check in checks if check["status"] == "error"),
    }


def _doctor_status(summary: DoctorSummaryData) -> DoctorStatus:
    if summary["error"] > 0:
        return "error"
    if summary["warning"] > 0:
        return "warning"
    return "ok"


def _doctor_warning_payloads(
    checks: list[DoctorCheckData],
) -> tuple[list[str], list[DoctorWarningDetail]]:
    warning_checks = [check for check in checks if check["status"] != "ok"]
    return (
        [f"doctor {check['name']}: {check['message']}" for check in warning_checks],
        [
            DoctorWarningDetail(
                code="doctor_check_not_ok",
                check=check["name"],
                status=check["status"],
                message=check["message"],
                suggestion=check["suggestion"],
            )
            for check in warning_checks
        ],
    )


def _doctor_check(
    name: str,
    status: DoctorStatus,
    message: str,
    details: Mapping[str, object],
    *,
    suggestion: str | None = None,
) -> DoctorCheckData:
    return {
        "name": name,
        "status": status,
        "message": message,
        "suggestion": suggestion,
        "details": require_json_object(
            details,
            label=f"doctor check {name} details",
        ),
    }


def _context_is_empty(session: SessionContext) -> bool:
    return (
        session.project is None and session.workflow is None and session.set_at is None
    )


def _error_details(
    error: Exception,
    *,
    env_file: str | None = None,
    endpoint: str | None = None,
) -> dict[str, object]:
    details: dict[str, object] = {}
    if env_file is not None:
        details["env_file"] = env_file
    if endpoint is not None:
        details["endpoint"] = endpoint
    if isinstance(error, DsctlError):
        details["error"] = error.to_payload()
        return details

    details["error"] = {
        "type": "unexpected_error",
        "message": _unexpected_message(error),
        "exception": error.__class__.__name__,
    }
    return details


def _error_suggestion(error: Exception, *, fallback: str | None = None) -> str | None:
    if isinstance(error, DsctlError) and error.suggestion is not None:
        return error.suggestion
    return fallback


def _unexpected_message(error: Exception) -> str:
    return str(error).strip() or error.__class__.__name__
