from __future__ import annotations

import json
from copy import deepcopy
from typing import TYPE_CHECKING

import pytest

from dsctl.data_shapes import (
    COLLECTION_DEFAULTS,
    PAGE_LIST_DEFAULTS,
    data_shape_for_action,
)
from dsctl.errors import OutputContractError, UserInputError
from dsctl.output import CommandResult, result_payload
from dsctl.output_formats import RenderOptions, render_command

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject, JsonValue


@pytest.mark.parametrize("action", [*PAGE_LIST_DEFAULTS, *COLLECTION_DEFAULTS])
@pytest.mark.parametrize("size", [0, 1, 2])
def test_business_lists_have_one_declared_encoding(action: str, size: int) -> None:
    rows: list[JsonValue] = [
        {"id": i, "name": "同步", "optional": None} for i in range(size)
    ]
    shape = data_shape_for_action(action)
    assert shape is not None
    assert shape.to_schema()["compact_rows"] is True
    data: JsonValue = (
        {"totalList": rows, "total": size, "pageNo": 1}
        if action in PAGE_LIST_DEFAULTS
        else rows
    )
    payload = result_payload(action, CommandResult(data=data))
    original = deepcopy(payload)
    regular = json.loads(
        render_command(payload, action=action, options=RenderOptions()).stdout
    )
    compact = json.loads(
        render_command(
            payload, action=action, options=RenderOptions(output_format="json-compact")
        ).stdout
    )
    table = (
        compact["data"]["totalList"]
        if action in PAGE_LIST_DEFAULTS
        else compact["data"]
    )
    assert table == {
        "columns": ["id", "name", "optional"] if size else [],
        "rows": [[i, "同步", None] for i in range(size)],
    }
    decoded = [dict(zip(table["columns"], row, strict=True)) for row in table["rows"]]
    if action in PAGE_LIST_DEFAULTS:
        compact["data"]["totalList"] = decoded
    else:
        compact["data"] = decoded
    assert compact == regular == original
    assert payload == original


def test_nested_cells_preserve_absence_types_and_business_text() -> None:
    rows: list[JsonValue] = [
        {
            "id": 9007199254741003,
            "params": {"host": None},
            "text": '中文\n\t"rows"',
            "value": False,
        },
        {
            "id": 9007199254741004,
            "params": {},
            "text": '{"columns":[],"rows":[]}',
            "value": 0,
        },
        {
            "id": 9007199254741005,
            "params": {"list": [None, "", {}]},
            "text": "",
            "value": None,
        },
    ]
    payload = result_payload(
        "task-instance.list", CommandResult(data={"totalList": rows})
    )
    result = render_command(
        payload,
        action="task-instance.list",
        options=RenderOptions(output_format="json-compact"),
    )
    encoded = json.loads(result.stdout)["data"]["totalList"]
    assert encoded["columns"] == ["id", "params", "text", "value"]
    assert [
        dict(zip(encoded["columns"], row, strict=True)) for row in encoded["rows"]
    ] == rows
    assert "missing" not in encoded
    assert len(result.stdout.splitlines()) == len(rows) + 2


@pytest.mark.parametrize("rows", [[], [{"name": "daily", "id": 7, "state": None}]])
def test_explicit_top_level_columns_keep_order_including_empty_lists(
    rows: list[JsonObject],
) -> None:
    payload = result_payload("workflow.list", CommandResult(data={"totalList": rows}))
    result = render_command(
        payload,
        action="workflow.list",
        options=RenderOptions(
            output_format="json-compact", columns=("state", "id", "state")
        ),
    )
    assert json.loads(result.stdout)["data"]["totalList"] == {
        "columns": ["state", "id"],
        "rows": [[None, 7]] if rows else [],
    }


@pytest.mark.parametrize("rows", [[], [{"id": 7, "details": {"state": None}}]])
def test_compact_list_dotted_columns_have_one_rule(rows: list[JsonObject]) -> None:
    payload = result_payload("workflow.list", CommandResult(data={"totalList": rows}))
    with pytest.raises(UserInputError, match="top-level") as caught:
        render_command(
            payload,
            action="workflow.list",
            options=RenderOptions(
                output_format="json-compact", columns=("id", "details.state")
            ),
        )
    assert "--format json" in (caught.value.suggestion or "")
    regular = render_command(
        payload,
        action="workflow.list",
        options=RenderOptions(columns=("id", "details.state")),
    )
    assert json.loads(regular.stdout)["data"]["totalList"] == rows


@pytest.mark.parametrize("rows", [[{"id": 1, "host": None}, {"id": 2}], [{"id": 1}, 2]])
def test_contract_violations_never_become_null_or_a_different_success_shape(
    rows: list[JsonValue],
) -> None:
    payload = result_payload("workflow.list", CommandResult(data={"totalList": rows}))
    with pytest.raises(OutputContractError) as caught:
        render_command(
            payload,
            action="workflow.list",
            options=RenderOptions(output_format="json-compact"),
        )
    assert caught.value.error_type == "output_contract_error"
    assert "--format json" in (caught.value.suggestion or "")


@pytest.mark.parametrize(
    ("action", "row_key"),
    [
        ("alert-plugin.definition.list", "definitions"),
        ("task-type.list", "taskTypes"),
        ("workflow.lineage.list", "workFlowRelationDetailList"),
    ],
)
def test_nested_business_lists_preserve_other_data(action: str, row_key: str) -> None:
    payload = result_payload(
        action,
        CommandResult(
            data={row_key: [{"id": 1}], "count": 1, "other": [{"key": "keep"}]}
        ),
    )
    result = render_command(
        payload, action=action, options=RenderOptions(output_format="json-compact")
    )
    data = json.loads(result.stdout)["data"]
    assert data == {
        row_key: {"columns": ["id"], "rows": [[1]]},
        "count": 1,
        "other": [{"key": "keep"}],
    }


@pytest.mark.parametrize(
    "action",
    ["workflow.get", "task-type.schema", "template.workflow", "doctor", "unknown.list"],
)
def test_non_tabular_views_preserve_the_original_json(action: str) -> None:
    payload: JsonObject = {
        "ok": True,
        "action": action,
        "resolved": {},
        "data": {
            "id": 1,
            "fields": [{"name": "type", "choices": ["SHELL"]}, {"name": "name"}],
            "yaml": "name: daily\n",
        },
    }
    encoded = render_command(
        payload, action=action, options=RenderOptions(output_format="json-compact")
    )
    assert json.loads(encoded.stdout) == payload


def test_failed_list_preserves_original_diagnostic_data_and_error() -> None:
    payload: JsonObject = {
        "ok": False,
        "action": "workflow.list",
        "resolved": {},
        "data": {"totalList": [{"id": 1}, {"id": 2, "error": "denied"}]},
        "error": {"type": "permission_denied", "message": "Unavailable"},
    }
    result = render_command(
        payload,
        action="workflow.list",
        options=RenderOptions(output_format="json-compact", columns=("nested.path",)),
    )
    assert result.stdout == ""
    assert result.exit_code == 1
    assert json.loads(result.stderr) == payload
