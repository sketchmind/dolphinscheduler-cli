from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.services._schema_constraints import constrained_actions
from dsctl.services.schema import get_schema_result

if TYPE_CHECKING:
    from collections.abc import Mapping

CONSTRAINT_KINDS = {
    "all_or_none",
    "at_least_one_of",
    "at_most_one_of",
    "exactly_one_of",
    "forbids",
    "requires",
    "requires_all",
    "requires_any",
}


def test_schema_exposes_high_value_cross_field_constraints() -> None:
    workflow_edit = _command_contract("workflow.edit")
    assert _constraints(workflow_edit) == [
        {
            "kind": "requires",
            "if_present": "--project",
            "fields": ["WORKFLOW"],
        },
        {
            "kind": "exactly_one_of",
            "fields": ["--patch", "--file"],
        },
        {
            "kind": "requires",
            "if_present": "--file",
            "fields": ["WORKFLOW"],
        },
    ]

    workflow_backfill = _command_contract("workflow.backfill")
    assert {
        "kind": "at_least_one_of",
        "alternatives": [["--date"], ["--start", "--end"]],
    } in _constraints(workflow_backfill)
    assert {
        "kind": "all_or_none",
        "fields": ["--start", "--end"],
    } in _constraints(workflow_backfill)

    for action in (
        "workflow.lineage.get",
        "workflow.lineage.dependent-tasks",
    ):
        assert _constraints(_command_contract(action)) == [
            {
                "kind": "requires",
                "if_present": "--project",
                "fields": ["WORKFLOW"],
            }
        ]

    use_clear = _command_contract("use.clear")
    assert _constraints(use_clear) == [{"kind": "requires_all", "fields": ["--clear"]}]

    project_delete = _command_contract("project.delete")
    assert {"kind": "requires_all", "fields": ["--force"]} in _constraints(
        project_delete
    )

    project_worker_group_set = _command_contract("project-worker-group.set")
    assert _constraints(project_worker_group_set) == [
        {"kind": "requires_all", "fields": ["--worker-group"]}
    ]


def test_schema_exposes_use_set_or_clear_constraints() -> None:
    assert _constraints(_command_contract("use.project")) == [
        {"kind": "exactly_one_of", "fields": ["NAME", "--clear"]}
    ]
    assert _constraints(_command_contract("use.workflow")) == [
        {"kind": "exactly_one_of", "fields": ["NAME", "--clear"]},
        {
            "kind": "forbids",
            "if_present": "--clear",
            "fields": ["--project"],
        },
    ]


def test_schema_marks_use_actions_as_local_only_mutations() -> None:
    for action in ("use.clear", "use.project", "use.workflow"):
        command = _command_contract(action)
        assert command["mutates"] is True
        assert command["mutation_target"] == "local_context"
        assert command["remote_requests"] is False


def test_every_constraint_references_fields_in_its_action_contract() -> None:
    for action in constrained_actions():
        command = _command_contract(action)
        valid_fields = _contract_fields(command)
        constraints = _constraints(command)

        assert constraints, action
        for constraint in constraints:
            assert constraint["kind"] in CONSTRAINT_KINDS, (action, constraint)
            references = _constraint_references(constraint)
            assert references <= valid_fields, (
                action,
                sorted(references - valid_fields),
            )


def test_every_force_guard_is_machine_readable() -> None:
    actions = get_schema_result(list_commands=True).data
    assert isinstance(actions, list)

    for row in actions:
        assert isinstance(row, dict)
        action = row["action"]
        assert isinstance(action, str)
        command = _command_contract(action)
        options = command["options"]
        assert isinstance(options, list)
        if not any(
            isinstance(option, dict) and option.get("flag") == "--force"
            for option in options
        ):
            continue
        assert {"kind": "requires_all", "fields": ["--force"]} in _constraints(command)


def _command_contract(action: str) -> dict[str, object]:
    data = get_schema_result(command_action=action).data
    assert isinstance(data, dict)
    command = data["command"]
    assert isinstance(command, dict)
    return command


def _constraints(command: Mapping[str, object]) -> list[dict[str, object]]:
    value = command.get("constraints")
    assert isinstance(value, list)
    assert all(isinstance(item, dict) for item in value)
    return value


def _contract_fields(command: Mapping[str, object]) -> set[str]:
    fields: set[str] = set()
    arguments = command.get("arguments")
    assert isinstance(arguments, list)
    for argument in arguments:
        assert isinstance(argument, dict)
        name = argument.get("name")
        assert isinstance(name, str)
        fields.add(name.replace("-", "_").upper())
    options = command.get("options")
    assert isinstance(options, list)
    for option in options:
        assert isinstance(option, dict)
        flag = option.get("flag")
        assert isinstance(flag, str)
        fields.add(flag)
    return fields


def _constraint_references(constraint: Mapping[str, object]) -> set[str]:
    references: set[str] = set()
    for condition in ("if_present", "if_absent"):
        value = constraint.get(condition)
        if isinstance(value, str):
            references.add(value)
    fields = constraint.get("fields")
    if isinstance(fields, list):
        references.update(str(field) for field in fields)
    alternatives = constraint.get("alternatives")
    if isinstance(alternatives, list):
        for alternative in alternatives:
            assert isinstance(alternative, list)
            references.update(str(field) for field in alternative)
    return references
