from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.services._schema_constraints import (
    constrained_actions,
    constraints_for_action,
)
from dsctl.services._schema_primitives import command as command_schema
from dsctl.services._schema_version_projection import project_command_for_version
from dsctl.services.capabilities import get_capabilities_result
from dsctl.services.schema import get_schema_result
from dsctl.upstream import SUPPORTED_VERSIONS

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path

CONSTRAINT_KINDS = {
    "all_or_none",
    "at_least_one_of",
    "at_most_one_of",
    "exactly_one_of",
    "forbids",
    "requires",
    "requires_all",
    "requires_any",
    "requires_default",
    "requires_changed_value",
}


def test_schema_exposes_high_value_cross_field_constraints() -> None:
    workflow_edit = _command_contract("workflow.edit")
    assert _constraints(workflow_edit) == [
        {
            "kind": "exactly_one_of",
            "fields": ["--patch", "--file"],
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
        assert "constraints" not in _command_contract(action)

    project_delete = _command_contract("project.delete")
    assert {"kind": "requires_all", "fields": ["--force"]} in _constraints(
        project_delete
    )

    project_worker_group_set = _command_contract("project-worker-group.set")
    assert _constraints(project_worker_group_set) == [
        {"kind": "requires_all", "fields": ["--worker-group"]}
    ]


def test_schema_exposes_context_update_constraints() -> None:
    assert _constraints(_command_contract("context.update")) == [
        {"kind": "at_most_one_of", "fields": ["--project", "--clear-project"]},
        {"kind": "requires_any", "fields": ["--file", "--project", "--clear-project"]},
    ]


def test_schema_exposes_task_instance_log_window_modes() -> None:
    assert _constraints(_command_contract("task-instance.log")) == [
        {
            "kind": "forbids",
            "if_present": "--tail",
            "fields": ["--start-line", "--limit"],
        }
    ]


def test_schema_projects_alert_group_update_constraints_by_version(
    tmp_path: Path,
) -> None:
    expected_by_version = {
        "1.3.9": [
            {
                "kind": "at_most_one_of",
                "fields": ["--description", "--clear-description"],
            },
            {
                "kind": "requires_any",
                "fields": [
                    "--name",
                    "--group-type",
                    "--description",
                    "--clear-description",
                ],
            },
        ],
        "3.4.1": [
            {
                "kind": "at_most_one_of",
                "fields": ["--instance-id", "--clear-instance-ids"],
            },
            {
                "kind": "at_most_one_of",
                "fields": ["--description", "--clear-description"],
            },
            {
                "kind": "requires_any",
                "fields": [
                    "--name",
                    "--instance-id",
                    "--clear-instance-ids",
                    "--description",
                    "--clear-description",
                ],
            },
        ],
    }
    for ds_version, expected in expected_by_version.items():
        env_file = tmp_path / f"ds-{ds_version}.env"
        env_file.write_text(f"DS_VERSION={ds_version}\n", encoding="utf-8")

        data = get_schema_result(
            env_file=str(env_file),
            command_action="alert-group.update",
        ).data
        assert isinstance(data, dict)
        command = data["command"]
        assert isinstance(command, dict)
        assert _constraints(command) == expected


def test_worker_group_same_name_update_discovery_is_exact_and_shared(
    tmp_path: Path,
) -> None:
    for ds_version in ("3.1.0", "3.1.1", "3.1.2"):
        env_file = tmp_path / f"ds-{ds_version}.env"
        env_file.write_text(f"DS_VERSION={ds_version}\n", encoding="utf-8")
        schema = get_schema_result(
            env_file=str(env_file), command_action="worker-group.update"
        ).data
        capabilities = get_capabilities_result(
            env_file=str(env_file), action="worker-group.update"
        ).data
        assert isinstance(schema, dict)
        assert isinstance(capabilities, dict)
        command = schema["command"]
        capability = schema["capability"]
        assert isinstance(command, dict)
        assert isinstance(capability, dict)
        assert capability == capabilities["capability"]
        assert capability["availability"] == "supported"
        constraints = _constraints(command)
        limitation = [
            item for item in constraints if item["kind"] == "requires_changed_value"
        ]
        if ds_version in ("3.1.0", "3.1.1"):
            assert limitation == [
                {
                    "kind": "requires_changed_value",
                    "fields": ["--name"],
                    "relative_to": "resolved_worker_group.name",
                    "reason": "upstream_same_name_update_self_collision",
                }
            ]
            assert capability["runtime_constraints"] == limitation
        else:
            assert limitation == []
            assert "runtime_constraints" not in capability


def test_schema_requires_explicit_workflow_for_tasks_and_schedule_create() -> None:
    for action in ("task.list", "task.get", "task.update", "schedule.create"):
        command = _command_contract(action)
        options = command["options"]
        assert isinstance(options, list)
        workflow = next(
            option
            for option in options
            if isinstance(option, dict) and option.get("name") == "workflow"
        )
        assert workflow["required"] is True
        assert "constraints" not in command
    create_requirement = next(
        constraint
        for constraint in _constraints(_command_contract("schedule.explain"))
        if constraint["kind"] == "requires"
        and constraint.get("if_absent") == "SCHEDULE_ID"
    )
    fields = create_requirement["fields"]
    assert isinstance(fields, list)
    assert "--workflow" in fields


def test_every_constraint_references_fields_in_its_action_contract() -> None:
    for ds_version in SUPPORTED_VERSIONS:
        for action in constrained_actions():
            command = project_command_for_version(
                command_schema(action),
                action=action,
                ds_version=ds_version,
            )
            valid_fields = _contract_fields(command)
            constraints = constraints_for_action(action, ds_version=ds_version)

            assert constraints, (ds_version, action)
            for constraint in constraints:
                assert constraint["kind"] in CONSTRAINT_KINDS, (
                    ds_version,
                    action,
                    constraint,
                )
                references = _constraint_references(constraint)
                assert references <= valid_fields, (
                    ds_version,
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


def test_every_matching_value_and_clear_option_is_mutually_exclusive() -> None:
    actions = get_schema_result(list_commands=True).data
    assert isinstance(actions, list)

    for row in actions:
        assert isinstance(row, dict)
        action = row["action"]
        assert isinstance(action, str)
        command = _command_contract(action)
        options = command["options"]
        assert isinstance(options, list)
        flags = {
            option["flag"]
            for option in options
            if isinstance(option, dict) and isinstance(option.get("flag"), str)
        }
        constraints = command.get("constraints", [])
        assert isinstance(constraints, list)
        for clear_flag in flags:
            if not clear_flag.startswith("--clear-"):
                continue
            value_flag = f"--{clear_flag.removeprefix('--clear-')}"
            if value_flag not in flags:
                continue
            assert {
                "kind": "at_most_one_of",
                "fields": [value_flag, clear_flag],
            } in constraints, (action, value_flag, clear_flag)


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
    defaults = constraint.get("defaults")
    if isinstance(defaults, dict):
        references.update(str(field) for field in defaults)
    return references
