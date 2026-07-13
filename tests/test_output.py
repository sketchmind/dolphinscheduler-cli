from typing import TYPE_CHECKING, cast

import pytest

from dsctl.errors import ConfigError, NotFoundError, ResolutionError
from dsctl.output import CommandResult, dry_run_result, error_payload, success_payload

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject, JsonValue
else:
    JsonValue = object


def test_command_result_rejects_non_json_data() -> None:
    with pytest.raises(TypeError, match="JSON-compatible"):
        CommandResult(data=cast("JsonValue", {"bad": object()}))


def test_success_payload_uses_json_safe_result_shapes() -> None:
    payload = success_payload(
        "context",
        CommandResult(
            data={"project": "etl-prod"},
            resolved={"project": "etl-prod"},
            warnings=["dry run"],
            warning_details=[
                {
                    "code": "example_warning",
                    "message": "dry run",
                }
            ],
        ),
    )

    assert payload == {
        "ok": True,
        "action": "context",
        "resolved": {"project": "etl-prod"},
        "data": {"project": "etl-prod"},
        "warnings": ["dry run"],
        "warning_details": [
            {
                "code": "example_warning",
                "message": "dry run",
            }
        ],
    }


def test_success_payload_does_not_fail_when_optional_navigation_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_navigation(*args: object, **kwargs: object) -> object:
        del args, kwargs
        message = "optional discovery failed"
        raise RuntimeError(message)

    monkeypatch.setattr("dsctl.output.navigation_for", fail_navigation)

    payload = success_payload(
        "workflow-instance.list",
        CommandResult(data={"totalList": [{"id": 263, "state": "SUCCESS"}]}),
    )

    assert payload == {
        "ok": True,
        "action": "workflow-instance.list",
        "resolved": {},
        "data": {"totalList": [{"id": 263, "state": "SUCCESS"}]},
        "warnings": [],
        "warning_details": [],
    }


def test_success_payload_does_not_fail_when_optional_navigation_is_not_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "dsctl.output.navigation_for",
        lambda *args, **kwargs: {"action_index": object()},
    )

    payload = success_payload(
        "workflow-instance.list",
        CommandResult(data={"totalList": [{"id": 263, "state": "SUCCESS"}]}),
    )

    assert "action_index" not in payload
    assert payload["data"] == {"totalList": [{"id": 263, "state": "SUCCESS"}]}


def test_command_result_rejects_misaligned_warning_details() -> None:
    with pytest.raises(ValueError, match="warning_details must align"):
        CommandResult(
            data={"project": "etl-prod"},
            warnings=["dry run"],
            warning_details=[{}, {}],
        )


def test_command_result_rejects_warnings_without_details() -> None:
    with pytest.raises(ValueError, match="warning_details must align"):
        CommandResult(
            data={"project": "etl-prod"},
            warnings=["dry run"],
        )


def test_dry_run_result_emits_standard_warning_detail() -> None:
    result = dry_run_result(
        method="GET",
        path="/projects/7",
    )

    assert result.warnings == ["dry run: no request was sent"]
    assert result.warning_details == [
        {
            "code": "dry_run_no_request_sent",
            "message": "dry run: no request was sent",
            "request_sent": False,
        }
    ]


def test_dry_run_result_appends_extra_warning_details() -> None:
    result = dry_run_result(
        method="GET",
        path="/projects/7",
        warnings=["extra warning"],
        warning_details=[
            {
                "code": "extra_warning",
                "message": "extra warning",
            }
        ],
    )

    assert result.warnings == [
        "dry run: no request was sent",
        "extra warning",
    ]
    assert result.warning_details == [
        {
            "code": "dry_run_no_request_sent",
            "message": "dry run: no request was sent",
            "request_sent": False,
        },
        {
            "code": "extra_warning",
            "message": "extra warning",
        },
    ]


def test_dry_run_result_omits_duplicate_single_request_plan() -> None:
    request = cast(
        "JsonObject",
        {
            "method": "POST",
            "path": "/projects/7/workflow-definition",
            "form": {"name": "luna"},
        },
    )

    result = dry_run_result(
        method="POST",
        path="/projects/7/workflow-definition",
        form_data={"name": "luna"},
        requests=[request],
    )

    data = cast("JsonObject", result.data)
    assert data["request"] == request
    assert "requests" not in data


def test_dry_run_result_preserves_ordered_multi_request_plan() -> None:
    first = cast(
        "JsonObject",
        {
            "method": "POST",
            "path": "/projects/7/workflow-definition",
            "form": {"name": "luna"},
        },
    )
    second = cast(
        "JsonObject",
        {
            "method": "POST",
            "path": "/projects/7/workflow-definition/101/release",
            "form": {"releaseState": "ONLINE"},
        },
    )

    result = dry_run_result(
        method="POST",
        path="/projects/7/workflow-definition",
        form_data={"name": "luna"},
        requests=[first, second],
    )

    data = cast("JsonObject", result.data)
    assert data["request"] == first
    assert data["requests"] == [first, second]


def test_dry_run_result_rejects_request_plan_with_different_first_request() -> None:
    with pytest.raises(
        ValueError,
        match="must begin with the primary request",
    ):
        dry_run_result(
            method="POST",
            path="/projects/7/workflow-definition",
            form_data={"name": "luna"},
            requests=[
                {
                    "method": "POST",
                    "path": "/different",
                    "form": {"name": "luna"},
                }
            ],
        )


def test_dry_run_result_rejects_empty_request_plan() -> None:
    with pytest.raises(ValueError, match="request plan cannot be empty"):
        dry_run_result(
            method="POST",
            path="/projects/7/workflow-definition",
            form_data={"name": "luna"},
            requests=[],
        )


def test_error_payload_includes_exception_class_for_unexpected_errors() -> None:
    payload = error_payload("context", ValueError("boom"))

    assert payload["error"] == {
        "type": "unexpected_error",
        "message": "boom",
        "exception": "ValueError",
    }


def test_error_payload_uses_structured_dsctl_errors() -> None:
    message = "Missing required setting: DS_API_URL"
    payload = error_payload("context", ConfigError(message))

    assert payload["error"] == {
        "type": "config_error",
        "message": message,
    }


def test_error_payload_includes_structured_suggestion_when_present() -> None:
    payload = error_payload(
        "context",
        ConfigError(
            "Missing required setting: DS_API_URL",
            suggestion=(
                "Set DS_API_URL in the environment or provide it through --env-file."
            ),
        ),
    )

    assert payload["error"] == {
        "type": "config_error",
        "message": "Missing required setting: DS_API_URL",
        "suggestion": (
            "Set DS_API_URL in the environment or provide it through --env-file."
        ),
    }


def test_error_payload_infers_lookup_suggestion_from_not_found_details() -> None:
    payload = error_payload(
        "project.get",
        NotFoundError(
            "Project 'missing' was not found",
            details={"resource": "project", "name": "missing"},
        ),
    )

    assert payload["error"] == {
        "type": "not_found",
        "message": "Project 'missing' was not found",
        "details": {"resource": "project", "name": "missing"},
        "suggestion": (
            "Retry with `dsctl project list` to inspect available values, or pass "
            "the numeric code if known."
        ),
    }


@pytest.mark.parametrize(
    ("details", "expected_command"),
    [
        (
            {"resource": "workflow", "name": "missing", "project_code": 7},
            "dsctl workflow list --project 7",
        ),
        (
            {
                "resource": "task",
                "name": "missing",
                "project_code": 7,
                "workflow_code": 101,
            },
            "dsctl task list --project 7 --workflow 101",
        ),
        (
            {
                "resource": "task-instance",
                "id": 999,
                "workflow_instance_id": 901,
            },
            "dsctl task-instance list --workflow-instance 901",
        ),
    ],
)
def test_not_found_suggestion_preserves_numeric_scope_tuple(
    details: dict[str, object],
    expected_command: str,
) -> None:
    error = NotFoundError("missing", details=details)

    assert error.suggestion is not None
    assert f"`{expected_command}`" in error.suggestion


def test_error_payload_infers_lookup_suggestion_from_resolution_details() -> None:
    payload = error_payload(
        "project.get",
        ResolutionError(
            "Project name 'etl-prod' is ambiguous",
            details={"resource": "project", "name": "etl-prod", "codes": [7, 8]},
        ),
    )

    assert payload["error"] == {
        "type": "resolution_error",
        "message": "Project name 'etl-prod' is ambiguous",
        "details": {"resource": "project", "name": "etl-prod", "codes": [7, 8]},
        "suggestion": (
            "Retry with one explicit numeric code from the matching results: 7, 8."
        ),
    }
