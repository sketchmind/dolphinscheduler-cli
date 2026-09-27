from __future__ import annotations

import importlib
import json
import sys
from dataclasses import asdict
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from types import ModuleType


def _ensure_tools_on_path() -> None:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))


def _load_module() -> ModuleType:
    _ensure_tools_on_path()
    return importlib.import_module("extract_dsctl_error_translation_matrix")


def test_collect_branch_mappings_extracts_codes_and_outcomes() -> None:
    matrix = _load_module()
    source = """
def _translate_demo(error: ApiResultError) -> Exception:
    result_code = error.result_code
    if result_code == USER_NO_OPERATION_PERM:
        return PermissionDeniedError("x")
    if error.result_code in {NOT_FOUND, 99}:
        raise NotFoundError("y")
    return error
"""
    tree = matrix.ast.parse(source)
    function = tree.body[0]
    assert isinstance(function, matrix.ast.FunctionDef)

    mappings = matrix.collect_branch_mappings(
        function,
        {"USER_NO_OPERATION_PERM": 30001, "NOT_FOUND": 100},
    )

    assert mappings == [
        matrix.BranchMapping(
            line=4,
            codes=[matrix.CodeReference(name="USER_NO_OPERATION_PERM", value=30001)],
            outcomes=["PermissionDeniedError"],
        ),
        matrix.BranchMapping(
            line=6,
            codes=[
                matrix.CodeReference(name="NOT_FOUND", value=100),
                matrix.CodeReference(name=None, value=99),
            ],
            outcomes=["NotFoundError"],
        ),
    ]


def test_real_report_contains_known_matrix_rows() -> None:
    matrix = _load_module()

    report = matrix.build_report()
    helpers = {(helper.module, helper.helper): helper for helper in report.helpers}

    access_token = helpers[("access_token", "_translate_access_token_api_error")]
    assert any(
        any(reference.value == 30001 for reference in mapping.codes)
        and "PermissionDeniedError" in mapping.outcomes
        for mapping in access_token.mappings
    )
    workflow_instance = helpers[
        ("workflow_instance._errors", "_raise_workflow_instance_action_error")
    ]
    assert any(
        "InvalidStateError" in mapping.outcomes
        for mapping in workflow_instance.mappings
    )


@pytest.mark.parametrize("domain", ["workflow", "workflow_instance"])
def test_workflow_package_matrix_preserves_frozen_codes_and_outcomes(
    domain: str,
) -> None:
    matrix = _load_module()
    fixture = (
        Path(__file__).resolve().parents[1]
        / "fixtures"
        / "error_translation"
        / "pre_package_workflow_inventory.json"
    )
    expected = json.loads(fixture.read_text(encoding="utf-8"))[domain][
        "branch_mappings"
    ]
    if domain == "workflow":
        # The reviewed RPC branch preserves uncertainty as a transport error;
        # other 50014 start errors retain their previous handling.
        assert expected["_raise_workflow_run_error"] == []
        expected["_raise_workflow_run_error"] = [
            {
                "codes": [{"name": "START_WORKFLOW_INSTANCE_ERROR", "value": 50014}],
                "outcomes": ["ApiTransportError"],
            }
        ]
    if domain == "workflow_instance":
        action_mappings = expected["_raise_workflow_instance_action_error"]
        assert all(
            mapping["codes"]
            != [{"name": "EXECUTE_WORKFLOW_INSTANCE_ERROR", "value": 50015}]
            for mapping in action_mappings
        )
        expected["_raise_workflow_instance_action_error"] = [
            {
                "codes": [{"name": "EXECUTE_WORKFLOW_INSTANCE_ERROR", "value": 50015}],
                "outcomes": ["MutationOutcomeUnknownError"],
            },
            *action_mappings,
        ]
    actual = {
        helper.helper: [
            {key: value for key, value in asdict(mapping).items() if key != "line"}
            for mapping in helper.mappings
        ]
        for helper in matrix.build_report().helpers
        if helper.module.startswith(f"{domain}.")
    }

    assert actual == expected
