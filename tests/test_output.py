from typing import TYPE_CHECKING, cast

import pytest

from dsctl.errors import ConfigError, NotFoundError, ResolutionError
from dsctl.output import CommandResult, dry_run_result, error_payload, success_payload

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonValue
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
            "Retry with `project list` to inspect available values, or pass "
            "the numeric code if known."
        ),
    }


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
