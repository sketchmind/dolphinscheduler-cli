from __future__ import annotations

from collections.abc import Mapping as MappingABC
from collections.abc import Sequence as SequenceABC
from typing import TYPE_CHECKING, cast

from dsctl.cli_surface import (
    ACCESS_TOKEN_RESOURCE,
    ALERT_GROUP_RESOURCE,
    ALERT_PLUGIN_RESOURCE,
    CLUSTER_RESOURCE,
    DATASOURCE_RESOURCE,
    ENV_RESOURCE,
    NAMESPACE_RESOURCE,
    PROJECT_PARAMETER_RESOURCE,
    PROJECT_RESOURCE,
    QUEUE_RESOURCE,
    SCHEDULE_RESOURCE,
    TASK_GROUP_RESOURCE,
    TASK_INSTANCE_RESOURCE,
    TASK_RESOURCE,
    TENANT_RESOURCE,
    USER_RESOURCE,
    WORKER_GROUP_RESOURCE,
    WORKFLOW_INSTANCE_RESOURCE,
    WORKFLOW_RESOURCE,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dsctl.support.json_types import JsonObject, JsonValue

_RESOURCE_RESOLUTION_HINTS: dict[str, tuple[str, str, str | None]] = {
    PROJECT_RESOURCE: ("project list", "code", None),
    PROJECT_PARAMETER_RESOURCE: (
        "project-parameter list",
        "code",
        "in the selected project",
    ),
    WORKFLOW_RESOURCE: ("workflow list", "code", "in the selected project"),
    ENV_RESOURCE: ("environment list", "code", None),
    CLUSTER_RESOURCE: ("cluster list", "code", None),
    DATASOURCE_RESOURCE: ("datasource list", "id", None),
    NAMESPACE_RESOURCE: ("namespace list", "id", None),
    ALERT_PLUGIN_RESOURCE: ("alert-plugin list", "id", None),
    ALERT_GROUP_RESOURCE: ("alert-group list", "id", None),
    USER_RESOURCE: ("user list", "id", None),
    QUEUE_RESOURCE: ("queue list", "id", None),
    TASK_GROUP_RESOURCE: ("task-group list", "id", None),
    TENANT_RESOURCE: ("tenant list", "id", None),
    WORKER_GROUP_RESOURCE: ("worker-group list", "id", None),
    ACCESS_TOKEN_RESOURCE: ("access-token list", "id", None),
    TASK_RESOURCE: ("task list", "code", "in the selected workflow"),
    SCHEDULE_RESOURCE: ("schedule list", "id", "in the selected project"),
    WORKFLOW_INSTANCE_RESOURCE: ("workflow-instance list", "id", None),
    TASK_INSTANCE_RESOURCE: (
        "task-instance list",
        "id",
        "in the selected workflow instance",
    ),
}


class DsctlError(Exception):
    """Base class for structured CLI errors exposed to callers."""

    error_type = "dsctl_error"

    def __init__(
        self,
        message: str,
        *,
        details: Mapping[str, object] | None = None,
        source: Mapping[str, JsonValue] | None = None,
        suggestion: str | None = None,
    ) -> None:
        """Store a stable error message plus JSON-safe details."""
        super().__init__(message)
        self.message = message
        self.details = _json_object_or_empty(details, label="details")
        self.source = None if source is None else _json_object(source, label="source")
        self.suggestion = suggestion

    def to_payload(self) -> JsonObject:
        """Render the error into the standard CLI payload shape."""
        payload: JsonObject = {
            "type": self.error_type,
            "message": self.message,
        }
        if self.details:
            payload["details"] = self.details
        source = _error_source_payload(self)
        if source is not None:
            payload["source"] = source
        if self.suggestion is not None:
            payload["suggestion"] = self.suggestion
        return payload


class ConfigError(DsctlError):
    """Raised when local CLI configuration is invalid or incomplete."""

    error_type = "config_error"


class UserInputError(DsctlError):
    """Raised when a user-supplied value fails validation."""

    error_type = "user_input_error"


class ConflictError(DsctlError):
    """Raised when the requested mutation conflicts with current remote state."""

    error_type = "conflict"


class PermissionDeniedError(DsctlError):
    """Raised when the current user lacks permission for the requested action."""

    error_type = "permission_denied"


class InvalidStateError(DsctlError):
    """Raised when a resource state does not permit the requested operation."""

    error_type = "invalid_state"


class TaskNotDispatchedError(DsctlError):
    """Raised when DS has not dispatched a task instance and no log exists yet."""

    error_type = "task_not_dispatched"


class ConfirmationRequiredError(DsctlError):
    """Raised when a risky mutation requires one explicit confirmation token."""

    error_type = "confirmation_required"

    def __init__(
        self,
        message: str,
        *,
        details: Mapping[str, object] | None = None,
        suggestion: str | None = None,
    ) -> None:
        """Infer one retry hint from confirmation details when available."""
        resolved_suggestion = suggestion
        if resolved_suggestion is None and details is not None:
            confirm_flag = details.get("confirm_flag")
            if isinstance(confirm_flag, str):
                resolved_suggestion = f"Retry the same command with {confirm_flag}."
        super().__init__(
            message,
            details=details,
            suggestion=resolved_suggestion,
        )


class WaitTimeoutError(DsctlError):
    """Raised when one blocking wait command exceeds its timeout."""

    error_type = "timeout"


class ResolutionError(DsctlError):
    """Raised when a name-to-code lookup cannot be resolved."""

    error_type = "resolution_error"

    def __init__(
        self,
        message: str,
        *,
        details: Mapping[str, object] | None = None,
        suggestion: str | None = None,
    ) -> None:
        """Infer one selection or ambiguity hint when details are available."""
        super().__init__(
            message,
            details=details,
            suggestion=(
                suggestion
                if suggestion is not None
                else _resolution_suggestion(details)
            ),
        )


class NotFoundError(ResolutionError):
    """Raised when a requested resource does not exist."""

    error_type = "not_found"

    def __init__(
        self,
        message: str,
        *,
        details: Mapping[str, object] | None = None,
        suggestion: str | None = None,
    ) -> None:
        """Infer one lookup retry hint for selection-oriented not-found errors."""
        super().__init__(
            message,
            details=details,
            suggestion=(
                suggestion if suggestion is not None else _not_found_suggestion(details)
            ),
        )


class SearchIndexError(DsctlError):
    """Raised when a cached search/index view is invalid."""

    error_type = "search_index_error"


class ApiTransportError(DsctlError):
    """Raised for transport-layer or response-decoding failures."""

    error_type = "api_transport_error"


class ApiHttpError(DsctlError):
    """Raised when the remote API returns a non-success HTTP status."""

    error_type = "api_http_error"

    def __init__(
        self,
        message: str,
        *,
        status_code: int,
        body: JsonValue | None = None,
        details: Mapping[str, object] | None = None,
    ) -> None:
        """Attach HTTP status metadata to a transport failure."""
        error_details: dict[str, object] = {"status_code": status_code}
        if body is not None:
            error_details["body"] = body
        if details is not None:
            error_details.update(dict(details))
        super().__init__(message, details=error_details)
        self.status_code = status_code
        self.body = body
        self.source = _http_error_source(status_code=status_code)


class ApiResultError(DsctlError):
    """Raised when a DS result envelope reports a non-zero result code."""

    error_type = "api_result_error"

    def __init__(
        self,
        *,
        result_code: int | None,
        result_message: str,
        data: JsonValue | None = None,
        details: Mapping[str, object] | None = None,
    ) -> None:
        """Attach DS result-envelope metadata to an API-level failure."""
        error_details: dict[str, object] = {"result_code": result_code}
        if data is not None:
            error_details["data"] = data
        if details is not None:
            error_details.update(dict(details))
        super().__init__(
            result_message or "DolphinScheduler API returned an error",
            details=error_details,
            source=_result_error_source(
                result_code=result_code,
                result_message=result_message,
            ),
        )
        self.result_code = result_code
        self.result_message = result_message
        self.data = data


def _error_source_payload(error: BaseException) -> JsonObject | None:
    visited: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in visited:
        visited.add(id(current))
        if isinstance(current, DsctlError) and current.source is not None:
            return dict(current.source)
        current = (
            current.__cause__ if current.__cause__ is not None else current.__context__
        )
    return None


def _remote_error_source(*, layer: str) -> dict[str, str]:
    return {
        "kind": "remote",
        "system": "dolphinscheduler",
        "layer": layer,
    }


def _http_error_source(*, status_code: int) -> JsonObject:
    return {
        **_remote_error_source(layer="http"),
        "status_code": status_code,
    }


def _result_error_source(
    *,
    result_code: int | None,
    result_message: str,
) -> JsonObject:
    return {
        **_remote_error_source(layer="result"),
        "result_code": result_code,
        "result_message": result_message,
    }


def _resolution_suggestion(
    details: Mapping[str, object] | None,
) -> str | None:
    if details is None:
        return None
    resource_hint = _resource_resolution_hint(details)
    if resource_hint is None:
        return None
    _, numeric_label, _ = resource_hint
    candidate_values = _candidate_values(details)
    if candidate_values:
        candidates = ", ".join(str(value) for value in candidate_values)
        return (
            f"Retry with one explicit numeric {numeric_label} from the matching "
            f"results: {candidates}."
        )
    if _has_selector_value(details):
        return (
            "Retry with a more specific selector or use the numeric "
            f"{numeric_label} if known."
        )
    return None


def _not_found_suggestion(
    details: Mapping[str, object] | None,
) -> str | None:
    if details is None:
        return None
    resource_hint = _resource_resolution_hint(details)
    if resource_hint is None:
        return None
    list_command, numeric_label, scope_hint = resource_hint
    scope_text = "" if scope_hint is None else f" {scope_hint}"
    if _has_name_selector(details):
        return (
            f"Retry with `{list_command}`{scope_text} to inspect available "
            f"values, or pass the numeric {numeric_label} if known."
        )
    if _has_numeric_selector(details):
        return (
            f"Retry with `{list_command}`{scope_text} to inspect available "
            f"values and verify the selected {numeric_label}."
        )
    return None


def _resource_resolution_hint(
    details: Mapping[str, object],
) -> tuple[str, str, str | None] | None:
    resource = details.get("resource")
    if not isinstance(resource, str):
        return None
    return _RESOURCE_RESOLUTION_HINTS.get(resource)


def _has_name_selector(details: Mapping[str, object]) -> bool:
    return any(
        isinstance(details.get(key), str)
        for key in ("name", "userName", "tenantCode", "namespace")
    )


def _has_numeric_selector(details: Mapping[str, object]) -> bool:
    return any(isinstance(details.get(key), int) for key in ("id", "code"))


def _has_selector_value(details: Mapping[str, object]) -> bool:
    return _has_name_selector(details) or _has_numeric_selector(details)


def _candidate_values(details: Mapping[str, object]) -> list[int]:
    candidates = details.get("codes")
    if isinstance(candidates, list):
        return _normalized_candidate_values(candidates)
    candidates = details.get("ids")
    if isinstance(candidates, list):
        return _normalized_candidate_values(candidates)
    matches = details.get("matches")
    if not isinstance(matches, list):
        return []
    values = [item.get("id") for item in matches if isinstance(item, MappingABC)]
    return _normalized_candidate_values(values)


def _normalized_candidate_values(values: list[object]) -> list[int]:
    normalized: list[int] = []
    seen: set[int] = set()
    for item in values:
        if not isinstance(item, int):
            continue
        if item in seen:
            continue
        seen.add(item)
        normalized.append(item)
        if len(normalized) == 5:
            break
    return normalized


def _json_object_or_empty(
    value: Mapping[str, object] | None,
    *,
    label: str,
) -> JsonObject:
    if value is None:
        return {}
    return _json_object(value, label=label)


def _json_object(
    value: Mapping[str, object] | Mapping[str, JsonValue],
    *,
    label: str,
) -> JsonObject:
    normalized = dict(value)
    invalid_key = next(
        (key for key in normalized if not isinstance(key, str)),
        None,
    )
    if invalid_key is not None:
        message = f"DsctlError {label} must use string keys"
        raise TypeError(message)
    invalid_value = next(
        (key for key, item in normalized.items() if not _is_json_value(item)),
        None,
    )
    if invalid_value is not None:
        message = (
            "DsctlError "
            f"{label} must contain only JSON-compatible values: {invalid_value}"
        )
        raise TypeError(message)
    return cast("JsonObject", normalized)


def _is_json_value(value: object) -> bool:
    if value is None or isinstance(value, (str, int, float, bool)):
        return True
    if isinstance(value, MappingABC):
        return all(
            isinstance(key, str) and _is_json_value(item) for key, item in value.items()
        )
    if isinstance(value, SequenceABC) and not isinstance(
        value,
        (str, bytes, bytearray),
    ):
        return all(_is_json_value(item) for item in value)
    return False
