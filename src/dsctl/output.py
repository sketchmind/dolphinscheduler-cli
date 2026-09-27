from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, TypeAlias, TypedDict

from dsctl.errors import DsctlError
from dsctl.result_navigation import error_navigation_for, navigation_for
from dsctl.support import json_types as _json_types

JsonObject: TypeAlias = _json_types.JsonObject
JsonValue: TypeAlias = _json_types.JsonValue

if TYPE_CHECKING:
    from collections.abc import Collection, Sequence


@dataclass(frozen=True)
class CommandResult:
    """Structured command output restricted to JSON-safe values."""

    data: JsonValue
    resolved: JsonObject = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    warning_details: Sequence[Mapping[str, object]] = field(default_factory=list)
    failure: DsctlError | None = None

    def __post_init__(self) -> None:
        """Validate that result payloads stay inside the JSON boundary."""
        object.__setattr__(
            self,
            "data",
            require_json_value(self.data, label="command result data"),
        )
        object.__setattr__(
            self,
            "resolved",
            require_json_object(self.resolved, label="command result resolved"),
        )
        object.__setattr__(self, "warnings", _require_warnings(self.warnings))
        object.__setattr__(
            self,
            "warning_details",
            _require_warning_details(
                self.warning_details,
                warning_count=len(self.warnings),
            ),
        )


class DryRunWarningDetail(TypedDict):
    """Structured warning emitted by the shared dry-run result builder."""

    code: str
    message: str
    mutation_sent: bool


def result_payload(
    action: str,
    result: CommandResult,
    *,
    env_file: str | None = None,
    available_actions: Collection[str] | None = None,
) -> JsonObject:
    """Build a completed command report, including any failed outcome."""
    payload: JsonObject = {
        "ok": result.failure is None,
        "action": action,
        "resolved": result.resolved,
        "data": result.data,
    }
    if result.warnings:
        payload["warnings"] = [
            {
                **require_json_object(detail, label="result payload warning"),
                "message": message,
            }
            for message, detail in zip(
                result.warnings, result.warning_details, strict=True
            )
        ]
    if result.failure is not None:
        payload["error"] = result.failure.to_payload()
    try:
        navigation = require_json_object(
            navigation_for(
                action,
                resolved=result.resolved,
                data=result.data,
                env_file=env_file,
                available_actions=available_actions,
            ),
            label="result payload navigation",
        )
    except Exception:
        navigation = {}
    payload.update(navigation)
    return payload


def error_payload(
    action: str,
    error: Exception,
    *,
    resolved: Mapping[str, JsonValue] | None = None,
) -> JsonObject:
    """Build the standard error envelope for a failed command."""
    if isinstance(error, DsctlError):
        error_data = require_json_object(
            error.to_payload(),
            label="error payload error",
        )
    else:
        message = str(error).strip() or error.__class__.__name__
        error_data = {
            "type": "unexpected_error",
            "message": message,
            "exception": error.__class__.__name__,
        }

    resolved_data = resolved
    if resolved_data is None and error_data.get("type") == "mutation_outcome_unknown":
        details = error_data.get("details")
        known_resources = (
            details.get("known_resources") if isinstance(details, dict) else None
        )
        if isinstance(known_resources, dict):
            resolved_data = known_resources

    return {
        "ok": False,
        "action": action,
        "resolved": require_json_object(
            resolved_data or {},
            label="error payload resolved",
        ),
        "data": {},
        "error": error_data,
    }


def annotate_error_navigation(
    payload: JsonObject,
    *,
    env_file: str | None = None,
) -> JsonObject:
    """Attach bounded recovery reads after target selection is available."""
    action = payload.get("action")
    error = payload.get("error")
    resolved = payload.get("resolved")
    if not isinstance(action, str) or not isinstance(error, dict):
        return payload
    error_type = error.get("type")
    if not isinstance(error_type, str) or not isinstance(resolved, dict):
        return payload
    try:
        navigation = require_json_object(
            error_navigation_for(
                action,
                error_type=error_type,
                resolved=resolved,
                env_file=env_file,
            ),
            label="error payload navigation",
        )
    except Exception:
        return payload
    return {**payload, **navigation}


def dry_run_result(
    *,
    method: str,
    path: str,
    params: Mapping[str, JsonValue] | None = None,
    json_body: JsonValue | None = None,
    form_data: Mapping[str, JsonValue] | None = None,
    files: Mapping[str, JsonValue] | None = None,
    resolved: Mapping[str, JsonValue] | None = None,
    requests: Sequence[Mapping[str, JsonValue]] | None = None,
    warnings: Sequence[str] | None = None,
    warning_details: Sequence[Mapping[str, object]] | None = None,
    extra_data: Mapping[str, JsonValue] | None = None,
) -> CommandResult:
    """Describe prepared mutations; lookup and verification reads may occur."""
    request = build_dry_run_request(
        method=method,
        path=path,
        params=params,
        json_body=json_body,
        form_data=form_data,
        files=files,
    )
    request_plan = [request]
    if requests is not None:
        request_plan = [
            require_json_object(item, label="dry-run request item") for item in requests
        ]
        if request_plan and request_plan[0] != request:
            message = "dry-run request plan must begin with the primary request"
            raise ValueError(message)
    data: JsonObject = {"dry_run": True, "requests": request_plan}
    if extra_data is not None:
        for key, value in extra_data.items():
            if key in {*data, "request"}:
                message = f"dry-run extra data cannot overwrite reserved key '{key}'"
                raise ValueError(message)
            data[key] = require_json_value(
                value,
                label=f"dry-run extra data '{key}'",
            )

    extra_warnings = list(warnings or [])
    extra_warning_details = [
        require_json_object(item, label="dry-run warning detail")
        for item in warning_details or []
    ]
    dry_run_warning = (
        "dry run: no mutation was sent; lookup and verification reads may occur"
    )
    return CommandResult(
        data=data,
        resolved=require_json_object(resolved or {}, label="dry-run resolved"),
        warnings=[
            dry_run_warning,
            *extra_warnings,
        ],
        warning_details=[
            require_json_object(
                DryRunWarningDetail(
                    code="dry_run_no_mutation_sent",
                    message=dry_run_warning,
                    mutation_sent=False,
                ),
                label="dry-run warning detail",
            ),
            *extra_warning_details,
        ],
    )


def build_dry_run_request(
    *,
    method: str,
    path: str,
    params: Mapping[str, JsonValue] | None = None,
    json_body: JsonValue | None = None,
    form_data: Mapping[str, JsonValue] | None = None,
    files: Mapping[str, JsonValue] | None = None,
) -> JsonObject:
    """Build one JSON-safe dry-run request description."""
    request: JsonObject = {
        "method": method.upper(),
        "path": path,
    }
    if params:
        request["params"] = require_json_object(params, label="dry-run params")
    if json_body is not None:
        request["json"] = require_json_value(json_body, label="dry-run json body")
    if form_data:
        request["form"] = require_json_object(form_data, label="dry-run form data")
    if files:
        request["files"] = require_json_object(files, label="dry-run files")
    return request


def require_json_value(value: object, *, label: str) -> JsonValue:
    """Validate one internal boundary value as JSON-safe data."""
    if not _json_types.is_json_value(value):
        message = f"{label} must contain only JSON-compatible values"
        raise TypeError(message)
    return value


def require_json_object(value: object, *, label: str) -> JsonObject:
    """Validate one internal boundary value as a JSON object."""
    if not isinstance(value, Mapping):
        message = f"{label} must be a JSON object"
        raise TypeError(message)
    copied: JsonObject = {}
    for key, item in value.items():
        if not isinstance(key, str):
            message = f"{label} must use string keys"
            raise TypeError(message)
        copied[key] = require_json_value(item, label=label)
    return copied


def _require_warnings(value: list[str]) -> list[str]:
    if not all(isinstance(item, str) for item in value):
        message = "command result warnings must be strings"
        raise TypeError(message)
    return list(value)


def _require_warning_details(
    value: Sequence[Mapping[str, object]],
    *,
    warning_count: int,
) -> list[JsonObject]:
    details = [
        require_json_object(item, label="command result warning detail")
        for item in value
    ]
    if len(details) != warning_count:
        message = "command result warning_details must align with warnings"
        raise ValueError(message)
    return details
