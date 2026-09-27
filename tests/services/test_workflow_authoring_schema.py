from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from dsctl.output import require_json_object, result_payload
from dsctl.services._workflow.authoring_schema import workflow_authoring_schema_data
from dsctl.services.schema import get_schema_result
from dsctl.upstream import SUPPORTED_VERSIONS

if TYPE_CHECKING:
    from collections.abc import Callable

    from dsctl.output import JsonObject, JsonValue


def _definition(payload: JsonObject, name: str) -> JsonObject:
    schema = _schema_object(payload["yaml_schema"])
    definitions = _schema_object(schema["$defs"])
    return _schema_object(definitions[name])


def test_workflow_metadata_schema_covers_authored_fields_without_tenant() -> None:
    payload = workflow_authoring_schema_data("3.4.1")
    workflow = _definition(payload, "WorkflowMetadataSpec")
    fields = _schema_object(workflow["properties"])

    assert set(fields) == {
        "name",
        "project",
        "description",
        "timeout",
        "global_params",
        "execution_type",
        "release_state",
    }
    assert workflow["required"] == ["name"]
    assert workflow["additionalProperties"] is False
    assert _schema_object(fields["timeout"])["minimum"] == 0
    assert _schema_object(fields["timeout"])["default"] == 0
    assert _schema_object(fields["execution_type"])["default"] == "PARALLEL"
    assert _schema_object(fields["release_state"])["default"] == "OFFLINE"
    assert "--project" in str(_schema_object(fields["project"])["description"])
    context = _schema_object(payload["execution_context"])
    assert context["runtime_schema_command"] == "dsctl schema --command workflow.run"
    assert context["schedule_schema_command"] == (
        "dsctl schema --command schedule.create"
    )


@pytest.mark.parametrize("version", ["1.3.9", "2.0.0", "2.0.9"])
def test_pre_execution_mode_profiles_advertise_only_parallel(version: str) -> None:
    mode = _definition(workflow_authoring_schema_data(version), "WorkflowExecutionType")
    assert mode["const"] == "PARALLEL"
    assert "enum" not in mode
    assert "fixed" in str(mode["description"])


@pytest.mark.parametrize("version", ["3.0.0", "3.1.1", "3.3.1", "3.4.1"])
def test_execution_mode_schema_uses_both_exact_enum_epochs(version: str) -> None:
    mode = _definition(workflow_authoring_schema_data(version), "WorkflowExecutionType")
    assert mode["enum"] == [
        "PARALLEL",
        "SERIAL_WAIT",
        "SERIAL_DISCARD",
        "SERIAL_PRIORITY",
    ]
    assert "const" not in mode


@pytest.mark.parametrize("version", ["1.3.9", "2.0.0", "3.4.1"])
def test_schedule_schema_projects_timezone_and_keeps_all_other_fields(
    version: str,
) -> None:
    schedule = _definition(
        workflow_authoring_schema_data(version), "WorkflowScheduleSpec"
    )
    fields = _schema_object(schedule["properties"])
    expected = {
        "cron",
        "start",
        "end",
        "failure_strategy",
        "priority",
        "release_state",
        "enabled",
    }
    if version == "1.3.9":
        assert set(fields) == expected
        assert schedule["required"] == ["cron", "start", "end"]
        assert "server-local" in str(schedule["description"])
    else:
        assert set(fields) == expected | {"timezone"}
        assert schedule["required"] == ["cron", "start", "end", "timezone"]
        assert _schema_object(fields["timezone"])["type"] == "string"
    assert schedule["additionalProperties"] is False
    assert "conflict" in str(_schema_object(fields["enabled"])["description"])


def test_global_parameter_choices_keep_exact_type_and_direction_holes() -> None:
    old = workflow_authoring_schema_data("1.3.9")
    process = workflow_authoring_schema_data("2.0.0")
    modern = workflow_authoring_schema_data("3.4.1")
    assert _definition(old, "DataType")["enum"] == [
        "VARCHAR",
        "INTEGER",
        "LONG",
        "FLOAT",
        "DOUBLE",
        "DATE",
        "TIME",
        "TIMESTAMP",
        "BOOLEAN",
    ]
    assert _definition(old, "Direct")["enum"] == ["IN"]
    process_types = _definition(process, "DataType")["enum"]
    modern_types = _definition(modern, "DataType")["enum"]
    assert isinstance(process_types, list)
    assert isinstance(modern_types, list)
    assert "LIST" in process_types
    assert "FILE" not in process_types
    assert "FILE" in modern_types
    assert _definition(modern, "Direct")["enum"] == ["IN", "OUT"]


def test_task_items_use_progressive_discovery_instead_of_inlining_task_models() -> None:
    payload = workflow_authoring_schema_data("3.4.1")
    schema = _schema_object(payload["yaml_schema"])
    fields = _schema_object(schema["properties"])
    tasks = _schema_object(fields["tasks"])
    assert tasks["minItems"] == 1
    item = _schema_object(tasks["items"])
    assert item["required"] == ["name", "type"]
    assert set(_schema_object(item["properties"])) == {"name", "type"}
    links = _schema_object(payload["task_authoring"])
    assert links["schema_command_pattern"] == "dsctl task-type schema TYPE"
    assert links["template_command_pattern"] == "dsctl template task TYPE --raw"
    assert "not a complete task" in str(payload["validation_scope"])
    assert "TaskParams" not in json.dumps(schema)


@pytest.mark.parametrize("version", SUPPORTED_VERSIONS)
def test_action_integration_has_complete_local_references_and_records_size(
    version: str,
    monkeypatch: pytest.MonkeyPatch,
    record_property: Callable[[str, object], None],
) -> None:
    monkeypatch.setenv("DS_VERSION", version)
    result = get_schema_result(command_action="workflow.create")
    data = _schema_object(result.data)
    command = _schema_object(data["command"])
    payload = _schema_object(command["payload"])
    assert payload["ds_version"] == version
    assert payload["source_option"] == "--file"
    assert command["options"]
    schema = _schema_object(payload["yaml_schema"])
    definitions = _schema_object(schema["$defs"])

    def check_refs(value: JsonValue) -> None:
        if isinstance(value, dict):
            if "$ref" in value:
                ref = value["$ref"]
                assert isinstance(ref, str)
                assert ref.removeprefix("#/$defs/") in definitions
            for item in value.values():
                check_refs(item)
        elif isinstance(value, list):
            for item in value:
                check_refs(item)

    check_refs(schema)
    encoded = json.dumps(result_payload("schema", result), separators=(",", ":"))
    record_property("output_bytes", len(encoded.encode()))
    record_property("review_threshold_bytes", 8 * 1024)


def test_workflow_group_index_does_not_expand_yaml_contract() -> None:
    data = _schema_object(get_schema_result(group="workflow").data)
    assert "yaml_schema" not in json.dumps(data)
    assert "workflow.create" in json.dumps(data)


def _schema_object(value: JsonValue) -> JsonObject:
    return require_json_object(value, label="workflow authoring schema")


@pytest.mark.parametrize("version", ["1.3.9", "3.4.1", "3.4.2", "3.4.3"])
def test_workflow_schedule_missed_fire_policy_follows_exact_capability(
    version: str,
) -> None:
    schedule = _definition(
        workflow_authoring_schema_data(version), "WorkflowScheduleSpec"
    )
    fields = _schema_object(schedule["properties"])
    if version == "3.4.3":
        assert _schema_object(fields["missed_fire_policy"])["enum"] == [
            "SKIP_MISSED",
            "FIRE_ONCE_NOW",
            "FIRE_ALL_MISSED",
        ]
        required = schedule["required"]
        assert isinstance(required, list)
        assert "missed_fire_policy" not in required
    else:
        assert "missed_fire_policy" not in fields
