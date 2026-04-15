from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, TypedDict

import typer

from dsctl.errors import DsctlError
from dsctl.support.json_types import is_json_value

if TYPE_CHECKING:
    from collections.abc import Sequence

    from dsctl.support.json_types import JsonObject, JsonValue


@dataclass(frozen=True)
class CommandResult:
    """Structured command output restricted to JSON-safe values."""

    data: JsonValue
    resolved: JsonObject = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    warning_details: Sequence[Mapping[str, object]] = field(default_factory=list)

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
    request_sent: bool


def print_json(payload: JsonValue) -> None:
    """Render a JSON-safe payload to stdout using the standard CLI format."""
    typer.echo(
        json.dumps(
            require_json_value(payload, label="json payload"),
            indent=2,
            sort_keys=True,
            ensure_ascii=True,
        )
    )


def success_payload(action: str, result: CommandResult) -> JsonObject:
    """Build the standard success envelope for a completed command."""
    return {
        "ok": True,
        "action": action,
        "resolved": result.resolved,
        "data": result.data,
        "warnings": result.warnings,
        "warning_details": [
            require_json_object(item, label="success payload warning detail")
            for item in result.warning_details
        ],
    }


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

    return {
        "ok": False,
        "action": action,
        "resolved": require_json_object(
            resolved or {},
            label="error payload resolved",
        ),
        "data": {},
        "warnings": [],
        "warning_details": [],
        "error": error_data,
    }


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
    """Build a dry-run result that describes the request without sending it."""
    request = build_dry_run_request(
        method=method,
        path=path,
        params=params,
        json_body=json_body,
        form_data=form_data,
        files=files,
    )
    data: JsonObject = {
        "dry_run": True,
        "request": request,
    }
    if requests is not None:
        data["requests"] = [
            require_json_object(item, label="dry-run request item") for item in requests
        ]
    if extra_data is not None:
        for key, value in extra_data.items():
            if key in data:
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
    dry_run_warning = "dry run: no request was sent"
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
                    code="dry_run_no_request_sent",
                    message=dry_run_warning,
                    request_sent=False,
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
    if not is_json_value(value):
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
