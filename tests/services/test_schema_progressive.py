from __future__ import annotations

import json

import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.errors import UserInputError
from dsctl.output import success_payload
from dsctl.services.schema import get_schema_result

INDEX_COMPACT_BUDGET_BYTES = 16 * 1024
COMMAND_COMPACT_BUDGET_BYTES = 8 * 1024
UNKNOWN_ACTION_COMPACT_BUDGET_BYTES = 2 * 1024

runner = CliRunner()


def test_default_schema_is_a_bounded_progressive_index() -> None:
    result = get_schema_result()
    data = _require_dict(result.data)

    assert data["schema_version"] == 2
    assert data["view"] == "index"
    assert data["cli"] == {"name": "dsctl", "version": "0.2.0"}
    assert isinstance(data["ds"], dict)
    global_options = [
        _require_dict(item) for item in _require_list(data["global_options"])
    ]
    assert len(global_options) == 4
    output_format = next(
        item for item in global_options if item["flag"] == "--output-format"
    )
    assert output_format == {
        "flag": "--output-format",
        "value_name": "FORMAT",
        "choices": ["json", "table", "tsv"],
        "default": "json",
        "placement": "before_command",
    }
    assert all(item["placement"] == "before_command" for item in global_options)
    compact = next(item for item in global_options if item["flag"] == "--compact")
    assert compact["requires"] == {"--output-format": "json"}
    action_count = data["action_count"]
    assert isinstance(action_count, int)
    assert action_count > 100
    links = [_require_dict(item) for item in _require_list(data["links"])]
    assert {
        "rel": "action_schema",
        "command_pattern": "dsctl schema --command ACTION",
    } in links
    assert all(
        "command" not in item or "GROUP" not in str(item["command"]) for item in links
    )
    assert not {
        "commands",
        "rows",
        "capabilities",
        "selection",
        "output",
        "errors",
        "confirmation",
    }.intersection(data)

    groups = [_require_dict(item) for item in _require_list(data["groups"])]
    workflow = next(item for item in groups if item["name"] == "workflow")
    assert set(workflow) == {
        "name",
        "summary",
        "action_count",
        "actions",
        "schema_command",
        "help_command",
    }
    assert "workflow.edit" in _require_list(workflow["actions"])
    assert workflow["schema_command"] == "dsctl schema --group workflow"
    assert workflow["help_command"] == "dsctl workflow --help"

    root_actions = [_require_dict(item) for item in _require_list(data["root_actions"])]
    version = next(item for item in root_actions if item["action"] == "version")
    assert set(version) == {
        "action",
        "summary",
        "schema_command",
        "help_command",
    }
    assert version["schema_command"] == "dsctl schema --command version"

    indexed_actions = {str(item["action"]) for item in root_actions} | {
        str(action) for item in groups for action in _require_list(item["actions"])
    }
    listed_actions = {
        str(_require_dict(item)["action"])
        for item in _require_list(get_schema_result(list_commands=True).data)
    }
    assert indexed_actions == listed_actions
    assert action_count == len(indexed_actions) == 174


def test_full_schema_preserves_the_expanded_legacy_contract() -> None:
    result = runner.invoke(app, ["--compact", "schema", "--full"])

    assert result.exit_code == 0
    payload = _require_dict(json.loads(result.stdout))
    data = _require_dict(payload["data"])
    assert data["schema_version"] == 2
    assert data["view"] == "full"
    assert isinstance(data["commands"], list)
    assert isinstance(data["capabilities"], dict)
    assert isinstance(data["selection"], dict)
    assert isinstance(data["output"], dict)
    assert isinstance(data["errors"], dict)


def test_full_schema_can_preserve_legacy_scoped_contracts() -> None:
    results = [
        get_schema_result(group="workflow", full=True),
        get_schema_result(command_action="workflow.edit", full=True),
    ]

    for result in results:
        data = _require_dict(result.data)
        assert data["schema_version"] == 2
        assert data["view"] == "full"
        assert isinstance(data["commands"], list)
        assert isinstance(data["global_options"], list)


@pytest.mark.parametrize(
    "group_name",
    ["task-group", "alert-plugin", "user", "workflow"],
)
def test_full_group_rows_include_nested_action_groups(group_name: str) -> None:
    bounded = _require_dict(get_schema_result(group=group_name).data)
    expanded = _require_dict(
        get_schema_result(group=group_name, full=True).data,
    )

    bounded_actions = {
        str(_require_dict(item)["action"]) for item in _require_list(bounded["actions"])
    }
    expanded_actions = {
        str(_require_dict(item)["action"]) for item in _require_list(expanded["rows"])
    }
    assert expanded_actions == bounded_actions


def test_group_schema_is_a_bounded_action_index() -> None:
    result = get_schema_result(group="workflow")
    data = _require_dict(result.data)

    assert data["schema_version"] == 2
    assert data["view"] == "group"
    assert isinstance(data["cli"], dict)
    assert isinstance(data["ds"], dict)
    assert isinstance(data["links"], list)
    assert not {
        "commands",
        "rows",
        "global_options",
        "capabilities",
        "selection",
        "output",
        "errors",
        "confirmation",
    }.intersection(data)

    group = _require_dict(data["group"])
    assert group["name"] == "workflow"
    assert isinstance(group["summary"], str)
    group_action_count = group["action_count"]
    assert isinstance(group_action_count, int)
    assert group_action_count > 10

    actions = [_require_dict(item) for item in _require_list(data["actions"])]
    workflow_edit = next(item for item in actions if item["action"] == "workflow.edit")
    assert set(workflow_edit) == {
        "action",
        "name",
        "summary",
        "schema_command",
        "help_command",
    }
    assert workflow_edit["schema_command"] == ("dsctl schema --command workflow.edit")
    assert workflow_edit["help_command"] == "dsctl workflow edit --help"


def test_list_groups_and_group_indexes_use_the_same_recursive_action_count() -> None:
    rows = [
        _require_dict(item)
        for item in _require_list(get_schema_result(list_groups=True).data)
    ]

    for row in rows:
        group_name = row["name"]
        assert isinstance(group_name, str)
        group_data = _require_dict(get_schema_result(group=group_name).data)
        group = _require_dict(group_data["group"])
        assert row["action_count"] == group["action_count"]


def test_command_schema_is_action_local_without_legacy_duplication() -> None:
    result = get_schema_result(command_action="workflow.edit")
    data = _require_dict(result.data)

    assert data["schema_version"] == 2
    assert data["view"] == "command"
    assert isinstance(data["cli"], dict)
    assert isinstance(data["ds"], dict)
    global_options = [
        _require_dict(item) for item in _require_list(data["global_options"])
    ]
    assert len(global_options) == 4
    assert any(
        item["flag"] == "--output-format" and item["placement"] == "before_command"
        for item in global_options
    )
    links = [_require_dict(item) for item in _require_list(data["links"])]
    assert {
        "rel": "help",
        "command": "dsctl workflow edit --help",
    } in links
    assert all("--full" not in str(item.get("command", "")) for item in links)
    assert not {
        "commands",
        "rows",
        "capabilities",
        "supported_ds_versions",
        "ds_versions",
        "selection",
        "output",
        "errors",
        "confirmation",
    }.intersection(data)

    group = _require_dict(data["group"])
    assert group["name"] == "workflow"
    command = _require_dict(data["command"])
    assert command["kind"] == "command"
    assert command["name"] == "edit"
    assert command["action"] == "workflow.edit"
    assert command["invocation"] == "dsctl workflow edit [WORKFLOW] [OPTIONS]"
    assert isinstance(command["arguments"], list)
    assert isinstance(command["options"], list)
    assert isinstance(command["payload"], dict)


def test_action_local_invocation_handles_non_path_group_actions() -> None:
    data = _require_dict(get_schema_result(command_action="use.clear").data)
    command = _require_dict(data["command"])

    assert command["action"] == "use.clear"
    assert command["invocation"] == "dsctl use --clear [OPTIONS]"


def test_unknown_action_returns_at_most_three_local_corrections() -> None:
    with pytest.raises(UserInputError) as exc_info:
        get_schema_result(command_action="workflo.edit")

    error = exc_info.value
    details = _require_dict(error.details)
    assert details["requested"] == "workflo.edit"
    assert isinstance(details["available_count"], int)
    assert details["available_count"] > 100
    assert isinstance(details["discovery_command"], str)
    assert "available_commands" not in details

    candidates = [_require_dict(item) for item in _require_list(details["candidates"])]
    assert 1 <= len(candidates) <= 3
    assert any(item["action"] == "workflow.edit" for item in candidates)
    assert all(
        set(item) == {"action", "group", "schema_command"} for item in candidates
    )


@pytest.mark.parametrize(
    ("argv", "stream", "budget"),
    [
        (["--compact", "schema"], "stdout", INDEX_COMPACT_BUDGET_BYTES),
        (
            ["--compact", "schema", "--command", "workflow.edit"],
            "stdout",
            COMMAND_COMPACT_BUDGET_BYTES,
        ),
        (
            ["--compact", "schema", "--command", "workflo.edit"],
            "stderr",
            UNKNOWN_ACTION_COMPACT_BUDGET_BYTES,
        ),
    ],
)
def test_progressive_schema_compact_json_stays_within_its_byte_budget(
    argv: list[str],
    stream: str,
    budget: int,
) -> None:
    result = runner.invoke(app, argv)

    output = result.stdout if stream == "stdout" else result.stderr
    assert output
    assert output.count("\n") == 1
    assert len(output.encode("utf-8")) < budget
    assert result.exit_code == (0 if stream == "stdout" else 1)
    assert json.loads(output)["action"] == "schema"


def test_every_action_local_contract_stays_within_the_command_budget() -> None:
    actions = [
        _require_dict(item)["action"]
        for item in _require_list(get_schema_result(list_commands=True).data)
    ]

    sizes: dict[str, int] = {}
    for value in actions:
        assert isinstance(value, str)
        payload = success_payload(
            "schema",
            get_schema_result(command_action=value),
        )
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        sizes[value] = len(encoded)

    assert len(sizes) == 174
    assert max(sizes.values()) < COMMAND_COMPACT_BUDGET_BYTES, sorted(
        sizes.items(),
        key=lambda item: item[1],
        reverse=True,
    )[:5]


def _require_dict(value: object) -> dict[str, object]:
    assert isinstance(value, dict)
    return value


def _require_list(value: object) -> list[object]:
    assert isinstance(value, list)
    return value
