from typing import TYPE_CHECKING, cast

import pytest

from dsctl.errors import (
    ApiHttpError,
    ApiResultError,
    ApiTransportError,
    ConfigError,
    ConfirmationRequiredError,
    ConflictError,
    DsctlError,
    InvalidStateError,
    NotFoundError,
    PermissionDeniedError,
    ResolutionError,
    SearchIndexError,
    UserInputError,
    WaitTimeoutError,
)
from dsctl.output import error_payload

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonValue
else:
    JsonValue = object


def test_dsctl_error_rejects_non_json_details() -> None:
    with pytest.raises(TypeError, match="details must contain only JSON-compatible"):
        DsctlError(
            "base",
            details={"resource": "project", "bad": object()},
        )


def test_dsctl_error_rejects_non_json_source() -> None:
    with pytest.raises(TypeError, match="source must contain only JSON-compatible"):
        DsctlError(
            "base",
            source=cast("dict[str, JsonValue]", {"bad": object()}),
        )


@pytest.mark.parametrize(
    ("error", "expected_type"),
    [
        pytest.param(DsctlError("base"), "dsctl_error", id="base"),
        pytest.param(ConfigError("config"), "config_error", id="config"),
        pytest.param(UserInputError("input"), "user_input_error", id="user-input"),
        pytest.param(ConflictError("conflict"), "conflict", id="conflict"),
        pytest.param(
            PermissionDeniedError("denied"),
            "permission_denied",
            id="permission-denied",
        ),
        pytest.param(
            InvalidStateError("invalid"),
            "invalid_state",
            id="invalid-state",
        ),
        pytest.param(
            ConfirmationRequiredError("confirm"),
            "confirmation_required",
            id="confirmation-required",
        ),
        pytest.param(WaitTimeoutError("timeout"), "timeout", id="timeout"),
        pytest.param(
            ResolutionError("ambiguous"),
            "resolution_error",
            id="resolution",
        ),
        pytest.param(NotFoundError("missing"), "not_found", id="not-found"),
        pytest.param(
            SearchIndexError("search"),
            "search_index_error",
            id="search-index",
        ),
        pytest.param(
            ApiTransportError("transport"),
            "api_transport_error",
            id="api-transport",
        ),
        pytest.param(
            ApiHttpError(
                "http",
                status_code=500,
            ),
            "api_http_error",
            id="api-http",
        ),
        pytest.param(
            ApiResultError(
                result_code=10001,
                result_message="result",
            ),
            "api_result_error",
            id="api-result",
        ),
    ],
)
def test_error_payload_covers_every_error_type(
    error: DsctlError,
    expected_type: str,
) -> None:
    payload = error_payload("test", error)
    error_data = payload["error"]

    assert isinstance(error_data, dict)
    assert error_data["type"] == expected_type


def test_dsctl_error_preserves_structured_details_and_suggestion() -> None:
    payload = error_payload(
        "test",
        DsctlError(
            "base",
            details={"resource": "project", "name": "etl-prod"},
            suggestion="Retry with `project list`.",
        ),
    )

    assert payload["error"] == {
        "type": "dsctl_error",
        "message": "base",
        "details": {"resource": "project", "name": "etl-prod"},
        "suggestion": "Retry with `project list`.",
    }


def test_translated_error_preserves_remote_result_source_from_cause() -> None:
    upstream = ApiResultError(
        result_code=30001,
        result_message="user has no operation privilege",
    )
    message = "Project list requires additional permissions"

    with pytest.raises(PermissionDeniedError) as exc_info:
        raise PermissionDeniedError(
            message,
            details={"resource": "project", "operation": "list"},
        ) from upstream

    payload = error_payload("project.list", exc_info.value)

    assert payload["error"] == {
        "type": "permission_denied",
        "message": message,
        "details": {"resource": "project", "operation": "list"},
        "source": {
            "kind": "remote",
            "system": "dolphinscheduler",
            "layer": "result",
            "result_code": 30001,
            "result_message": "user has no operation privilege",
        },
    }


def test_confirmation_required_error_infers_retry_suggestion() -> None:
    payload = error_payload(
        "test",
        ConfirmationRequiredError(
            "Confirmation required",
            details={"confirm_flag": "--confirm-risk abc123"},
        ),
    )

    assert payload["error"] == {
        "type": "confirmation_required",
        "message": "Confirmation required",
        "details": {"confirm_flag": "--confirm-risk abc123"},
        "suggestion": "Retry the same command with --confirm-risk abc123.",
    }


def test_api_http_error_serializes_status_and_body() -> None:
    payload = error_payload(
        "test",
        ApiHttpError(
            "Upstream HTTP error",
            status_code=403,
            body={"msg": "denied"},
            details={"resource": "project"},
        ),
    )

    assert payload["error"] == {
        "type": "api_http_error",
        "message": "Upstream HTTP error",
        "details": {
            "status_code": 403,
            "body": {"msg": "denied"},
            "resource": "project",
        },
        "source": {
            "kind": "remote",
            "system": "dolphinscheduler",
            "layer": "http",
            "status_code": 403,
        },
    }


def test_api_result_error_serializes_result_code_and_data() -> None:
    payload = error_payload(
        "test",
        ApiResultError(
            result_code=10001,
            result_message="DS result error",
            data={"code": 7},
            details={"resource": "project"},
        ),
    )

    assert payload["error"] == {
        "type": "api_result_error",
        "message": "DS result error",
        "details": {
            "result_code": 10001,
            "data": {"code": 7},
            "resource": "project",
        },
        "source": {
            "kind": "remote",
            "system": "dolphinscheduler",
            "layer": "result",
            "result_code": 10001,
            "result_message": "DS result error",
        },
    }
