from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass

import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.cli_surface import (
    GROUP_LEVEL_ACTIONS,
    RESOURCE_COMMAND_TREE,
    TOP_LEVEL_COMMANDS,
    SurfaceCommand,
)

runner = CliRunner()


EXPECTED_GROUP_NAMES = (
    "use",
    "enum",
    "lint",
    "environment",
    "cluster",
    "datasource",
    "namespace",
    "resource",
    "queue",
    "worker-group",
    "task-group",
    "alert-plugin",
    "alert-group",
    "tenant",
    "user",
    "access-token",
    "monitor",
    "audit",
    "project",
    "project-parameter",
    "project-preference",
    "project-worker-group",
    "schedule",
    "template",
    "task-type",
    "workflow",
    "workflow-instance",
    "task",
    "task-instance",
)
EXPECTED_GROUP_ROWS = tuple(
    (name, f"dsctl schema --group {name}") for name in EXPECTED_GROUP_NAMES
)
EXPECTED_PROJECT_ACTION_ROWS = (
    ("project.list", "dsctl schema --command project.list"),
    ("project.get", "dsctl schema --command project.get"),
    ("project.create", "dsctl schema --command project.create"),
    ("project.update", "dsctl schema --command project.update"),
    ("project.delete", "dsctl schema --command project.delete"),
)
EXPECTED_PROJECT_CREATE_CONTRACT_ROWS = (
    ("command", "project.create"),
    ("option", "name"),
    ("option", "description"),
)


@dataclass(frozen=True)
class SchemaViewCase:
    """One public schema view and its canonical row source."""

    name: str
    schema_args: tuple[str, ...]
    row_path: tuple[str, ...]
    columns: tuple[str, ...]


SCHEMA_VIEW_CASES = (
    SchemaViewCase(
        name="index",
        schema_args=(),
        row_path=("groups",),
        columns=("name", "schema_command"),
    ),
    SchemaViewCase(
        name="list-groups",
        schema_args=("--list-groups",),
        row_path=(),
        columns=("name", "schema_command"),
    ),
    SchemaViewCase(
        name="list-commands",
        schema_args=("--list-commands",),
        row_path=(),
        columns=("action",),
    ),
    SchemaViewCase(
        name="group",
        schema_args=("--group", "project"),
        row_path=("actions",),
        columns=("action", "schema_command"),
    ),
    SchemaViewCase(
        name="command",
        schema_args=("--command", "project.create"),
        row_path=("command",),
        columns=("kind", "name"),
    ),
)


@pytest.mark.parametrize(
    ("schema_args", "row_field", "row_type"),
    [
        ((), "groups", list),
        (("--group", "project"), "actions", list),
        (("--command", "project.create"), "command", dict),
    ],
    ids=("index", "group", "command"),
)
def test_schema_canonical_views_do_not_export_renderer_rows(
    schema_args: tuple[str, ...],
    row_field: str,
    row_type: type[object],
) -> None:
    result = runner.invoke(app, ["schema", *schema_args])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    data = payload["data"]
    assert isinstance(data, dict)
    assert "rows" not in data
    assert isinstance(data[row_field], row_type)


@pytest.mark.parametrize(
    "case",
    SCHEMA_VIEW_CASES,
    ids=lambda case: case.name,
)
def test_schema_json_columns_project_the_selected_view_rows(
    case: SchemaViewCase,
) -> None:
    result = runner.invoke(
        app,
        [
            "--columns",
            ",".join(case.columns),
            "schema",
            *case.schema_args,
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    rows = _rows_at_path(payload["data"], case.row_path)
    _assert_view_rows(case, rows)
    if isinstance(payload["data"], dict):
        assert "rows" not in payload["data"]


@pytest.mark.parametrize("output_format", ["table", "tsv"])
@pytest.mark.parametrize(
    "case",
    SCHEMA_VIEW_CASES,
    ids=lambda case: case.name,
)
def test_schema_tabular_formats_render_the_selected_view_rows(
    output_format: str,
    case: SchemaViewCase,
) -> None:
    result = runner.invoke(
        app,
        [
            "--output-format",
            output_format,
            "--columns",
            ",".join(case.columns),
            "schema",
            *case.schema_args,
        ],
    )

    assert result.exit_code == 0
    rows = _parse_rendered_rows(result.stdout, output_format=output_format)
    _assert_view_rows(case, rows)


@pytest.mark.parametrize(
    ("schema_args", "columns", "expected"),
    [
        (
            ("--group", "project", "--full"),
            ("kind", "action"),
            ("command", "project.list"),
        ),
        (
            ("--command", "project.create", "--full"),
            ("kind", "name"),
            ("command", "project.create"),
        ),
    ],
    ids=("full-group", "full-command"),
)
def test_schema_json_columns_project_scoped_full_derived_rows(
    schema_args: tuple[str, ...],
    columns: tuple[str, str],
    expected: tuple[str, str],
) -> None:
    result = runner.invoke(
        app,
        ["--columns", ",".join(columns), "schema", *schema_args],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    data = payload["data"]
    assert isinstance(data, dict)
    rows = data["rows"]
    assert isinstance(rows, list)
    assert all(isinstance(row, dict) and set(row) <= set(columns) for row in rows)
    assert expected in {
        (str(row.get(columns[0], "")), str(row.get(columns[1], ""))) for row in rows
    }
    assert isinstance(data["commands"], list)


def _rows_at_path(data: object, path: tuple[str, ...]) -> list[dict[str, object]]:
    current = data
    for part in path:
        assert isinstance(current, dict)
        current = current[part]
    assert isinstance(current, list)
    assert all(isinstance(row, dict) for row in current)
    return current


def _parse_rendered_rows(
    stdout: str,
    *,
    output_format: str,
) -> list[dict[str, object]]:
    if output_format == "tsv":
        return [
            dict(row) for row in csv.DictReader(io.StringIO(stdout), delimiter="\t")
        ]

    lines = stdout.rstrip("\n").splitlines()
    assert len(lines) >= 2
    columns = tuple(cell.strip() for cell in lines[0].split(" | "))
    return [
        dict(zip(columns, (cell.strip() for cell in line.split(" | ")), strict=True))
        for line in lines[2:]
    ]


def _assert_view_rows(
    case: SchemaViewCase,
    rows: list[dict[str, object]],
) -> None:
    assert all(set(row) == set(case.columns) for row in rows)
    values = tuple(tuple(str(row[column]) for column in case.columns) for row in rows)

    if case.name in {"index", "list-groups"}:
        assert values == EXPECTED_GROUP_ROWS
        return
    if case.name == "list-commands":
        actions = tuple(value[0] for value in values)
        assert len(actions) == 174
        assert actions == _natural_surface_actions()
        return
    if case.name == "group":
        assert values == EXPECTED_PROJECT_ACTION_ROWS
        return
    assert case.name == "command"
    assert values == EXPECTED_PROJECT_CREATE_CONTRACT_ROWS


def _natural_surface_actions() -> tuple[str, ...]:
    actions = list(TOP_LEVEL_COMMANDS)
    for group, commands in RESOURCE_COMMAND_TREE.items():
        actions.extend(
            action for action in GROUP_LEVEL_ACTIONS if action.startswith(f"{group}.")
        )
        for command in commands:
            actions.extend(_surface_command_actions((group,), command))
    return tuple(actions)


def _surface_command_actions(
    prefix: tuple[str, ...],
    command: SurfaceCommand,
) -> tuple[str, ...]:
    path = (*prefix, command.name)
    if not command.commands:
        return (".".join(path),)
    return tuple(
        action
        for child in command.commands
        for action in _surface_command_actions(path, child)
    )
