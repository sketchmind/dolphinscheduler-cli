from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, TypedDict

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import CheckFailedError, ConfigError, DsctlError
from dsctl.output import CommandResult, require_json_object
from dsctl.services.version_resolution import (
    CompatibilityResolution,
    compatibility_details,
    invocation_scope,
    resolve_profile,
    resolve_runtime_selection,
    resolve_settings,
    resolve_target,
    resolve_version,
    selection_details,
)
from dsctl.upstream import (
    SUPPORTED_VERSIONS,
    Availability,
    Verification,
    get_action_capability,
    get_identity_adapter,
    get_version_support,
)
from dsctl.upstream.read_compatibility import available_read_actions
from dsctl.upstream.serialization import enum_value, optional_text

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dsctl.client import ReadExecutionPolicy
    from dsctl.config import ClusterProfile
    from dsctl.support.yaml_io import JsonObject

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
    compatibility: CompatibilityResolution | None = None


class DoctorWarningDetail(TypedDict):
    """Structured warning derived from one non-ok doctor check."""

    code: str
    check: str
    status: DoctorStatus
    message: str
    suggestion: str | None


def get_doctor_result(*, env_file: str | None = None) -> CommandResult:
    """Return a structured runtime and local-environment diagnostic report."""
    with invocation_scope("doctor"):
        return _get_doctor_result(env_file=env_file)


def _get_doctor_result(*, env_file: str | None) -> CommandResult:
    profile_result = _profile_check(env_file=env_file)
    if profile_result.compatibility is not None:
        return _compatibility_doctor_result(
            profile_result.check, profile_result.compatibility, env_file=env_file
        )
    checks = [
        profile_result.check,
        _context_check(env_file=env_file),
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
    return _doctor_result(checks)


def _profile_check(*, env_file: str | None) -> _ProfileCheckResult:
    try:
        with invocation_scope():
            profile = resolve_profile(env_file, mode="refresh")
            resolution = resolve_version(env_file, mode="local")
    except DsctlError as exc:
        if exc.details.get("reason") == "exact_version_required" and exc.details.get(
            "candidate_versions"
        ):
            observed = resolve_target(env_file, mode="local")
            if isinstance(observed, CompatibilityResolution):
                return _ProfileCheckResult(
                    profile=None,
                    check=_doctor_check(
                        "profile",
                        "warning",
                        "API contract candidates found; exact release is unknown.",
                        {
                            **compatibility_details(observed),
                            "probes": list(observed.probes),
                        },
                        suggestion=(
                            "Use automatic read actions. Configure DS_VERSION "
                            "from the deployment's release for other operations."
                        ),
                    ),
                    compatibility=observed,
                )
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

    details: JsonObject = {
        **profile.redacted(),
        "version_source": resolution.source,
        "version_checked_at": resolution.checked_at,
        "version_evidence": resolution.evidence,
    }
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


def _compatibility_doctor_result(
    profile_check: DoctorCheckData,
    observed: CompatibilityResolution,
    *,
    env_file: str | None,
) -> CommandResult:
    """Diagnose bounded read readiness without inventing an exact adapter target."""
    actions = available_read_actions(
        observed.candidate_versions,
        compatible_operations=observed.compatible_operations,
    )
    checks = [
        profile_check,
        _context_check(env_file=env_file),
        _doctor_check(
            "adapter",
            "warning",
            "Only independently reviewed read contracts are available automatically.",
            {"automatic_read_actions": sorted(actions), "mutations_allowed": False},
            suggestion="Configure the actual DS_VERSION for other operations.",
        ),
    ]
    try:
        selection = resolve_runtime_selection(env_file, action="doctor")
        if selection.read_plan is None:
            message = "The discovery observation changed during the diagnostic"
            raise ConfigError(message, suggestion="Rerun `dsctl doctor`.")
        details = _current_user_defaults_details(
            selection.execution_profile, read_policy=selection.read_plan.policy
        )
        checks.append(
            _doctor_check(
                "current_user", "ok", "Authenticated read succeeded.", details
            )
        )
    except DsctlError as exc:
        checks.append(
            _doctor_check(
                "current_user",
                "error",
                exc.message,
                _error_details(exc),
                suggestion=exc.suggestion,
            )
        )
    return _doctor_result(checks)


def _context_check(*, env_file: str | None) -> DoctorCheckData:
    try:
        settings = resolve_settings(env_file)
    except DsctlError as exc:
        return _doctor_check(
            "context",
            "error",
            exc.message,
            _error_details(exc),
            suggestion=exc.suggestion,
        )
    except Exception as exc:
        return _doctor_check(
            "context",
            "error",
            "Connection selection could not be inspected.",
            {"error": {"type": "unexpected_error", "exception": type(exc).__name__}},
            suggestion=(
                "Inspect `dsctl config get default-context`, `dsctl context list` "
                "and the selected connection file."
            ),
        )
    return _doctor_check(
        "context",
        "ok",
        "Connection selection resolved.",
        {**selection_details(settings), "project": settings.project},
    )


def _adapter_check(profile: ClusterProfile | None) -> DoctorCheckData:
    if profile is None:
        return _doctor_check(
            "adapter",
            "warning",
            "Adapter validation skipped because the target version was not resolved.",
            {"skipped": True},
            suggestion="Resolve the profile diagnostic, then rerun `dsctl doctor`.",
        )
    selected_version = profile.ds_version
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
                fallback="Use one of the selectable DolphinScheduler versions.",
            ),
        )
    except Exception as exc:  # pragma: no cover - defensive doctor fallback
        return _doctor_check(
            "adapter",
            "error",
            _unexpected_message(exc),
            _error_details(exc),
            suggestion="Use one of the selectable DolphinScheduler versions.",
        )

    details = {
        "ds_version": support.server_version,
        "contract_version": support.contract_version,
        "family": support.family,
        "support_level": support.support_level,
        "tested": support.tested,
        "supported_versions": list(SUPPORTED_VERSIONS),
        "catalog": support.catalog.summary_metadata(),
    }
    if support.support_level == "experimental":
        live_action_count = sum(
            1
            for capability in support.catalog.entries.values()
            if capability.availability is not Availability.UNSUPPORTED
            and capability.verification
            in {Verification.LIVE_SMOKE, Verification.LIVE_FULL}
        )
        if support.tested:
            message = (
                f"DS {support.server_version} support is experimental even though "
                "release-specific live smoke testing has passed."
            )
            suggestion = (
                "Use `dsctl capabilities --action ACTION` to verify the required "
                f"operation for DS {support.server_version}; promotion requires "
                "its complete compatibility review."
            )
        elif live_action_count:
            message = (
                f"DS {support.server_version} support is experimental: "
                f"{live_action_count} actions have live evidence, but the "
                "profile has not passed its complete release gate."
            )
            suggestion = (
                "Use `dsctl capabilities --action ACTION` to verify the required "
                f"operation for DS {support.server_version} before relying on "
                "this target."
            )
        else:
            message = (
                f"DS {support.server_version} support is experimental and has not "
                "passed release-specific live smoke testing."
            )
            suggestion = (
                f"Complete the DS {support.server_version} live smoke suite before "
                "relying on this target."
            )
        return _doctor_check(
            "adapter",
            "warning",
            message,
            details,
            suggestion=suggestion,
        )

    return _doctor_check(
        "adapter",
        "ok",
        "Adapter resolved.",
        details,
    )


def _api_health_check(profile: ClusterProfile) -> DoctorCheckData:
    capability = get_action_capability(profile.ds_version, "monitor.health")
    if capability.availability is not Availability.SUPPORTED:
        constraint = capability.constraint
        reason = (
            "upstream_endpoint_absent"
            if capability.availability is Availability.UNSUPPORTED
            else "profile_operation_limited"
        )
        return _doctor_check(
            "api",
            "warning",
            constraint
            or (
                f"monitor.health is {capability.availability.value} on "
                f"DolphinScheduler {profile.ds_version}."
            ),
            {
                "reason": reason,
                "ds_version": profile.ds_version,
                "operation": "monitor.health",
                "availability": capability.availability.value,
                "constraint": constraint,
                "endpoint": profile.health_url,
                "verification_fallback": "current_user",
            },
            suggestion=(
                "Use the authenticated current-user check in this report to verify "
                "API readiness."
            ),
        )
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
        "error" if health_status in ("DOWN", "OUT_OF_SERVICE") else "warning",
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
    support = get_version_support(profile.ds_version)
    if support.identity_adapter is None:
        return _doctor_check(
            "current_user",
            "warning",
            (
                f"Skipped because the DS {support.server_version} profile does "
                "not provide the current-user operation."
            ),
            {
                "reason": "profile_operation_unsupported",
                "ds_version": support.server_version,
                "operation": "current_user",
            },
        )
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


def _current_user_defaults_details(
    profile: ClusterProfile, *, read_policy: ReadExecutionPolicy | None = None
) -> dict[str, str | None]:
    adapter = get_identity_adapter(profile.ds_version)
    client = (
        DolphinSchedulerClient(profile, read_policy=read_policy)
        if read_policy is not None
        else DolphinSchedulerClient(profile)
    )
    with client as http_client:
        current_user = adapter.bind_identity(
            profile,
            http_client=http_client,
        ).current()
    return {
        "userName": optional_text(current_user.userName),
        "userType": enum_value(current_user.userType),
        "tenantCode": optional_text(current_user.tenantCode),
        "queue": optional_text(current_user.queue),
        "queueName": optional_text(current_user.queueName),
        "timeZone": optional_text(current_user.timeZone),
    }


def _doctor_result(checks: list[DoctorCheckData]) -> CommandResult:
    """Preserve every diagnostic while failing the readiness check on errors."""
    data = _doctor_data(checks)
    warnings, warning_details = _doctor_warning_payloads(checks)
    return CommandResult(
        data=require_json_object(data, label="doctor data"),
        warnings=warnings,
        warning_details=warning_details,
        failure=(
            CheckFailedError(
                "DolphinScheduler readiness checks failed.",
                details={
                    "failed_checks": [
                        c["name"] for c in checks if c["status"] == "error"
                    ]
                },
                suggestion=(
                    "Resolve the errors in data.checks and rerun `dsctl doctor`."
                ),
            )
            if data["status"] == "error"
            else None
        ),
    )


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
    details["error"] = _error_payload(error)
    return details


def _error_payload(error: Exception) -> JsonObject:
    if isinstance(error, DsctlError):
        return error.to_payload()
    return {
        "type": "unexpected_error",
        "message": _unexpected_message(error),
        "exception": error.__class__.__name__,
    }


def _error_suggestion(error: Exception, *, fallback: str | None = None) -> str | None:
    if isinstance(error, DsctlError) and error.suggestion is not None:
        return error.suggestion
    return fallback


def _unexpected_message(error: Exception) -> str:
    return str(error).strip() or error.__class__.__name__
