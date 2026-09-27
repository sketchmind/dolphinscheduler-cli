from __future__ import annotations

import importlib
import sys
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest


def _load_module(name: str) -> Any:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module(name)


def test_inventory_is_order_independent_and_ignores_documentation() -> None:
    inventory = _load_module("ds_codegen.api")
    snapshot = _snapshot("3.4.1")
    operation = snapshot.operations[0]
    documented = replace(
        snapshot,
        operations=[
            replace(
                operation,
                summary="Updated user-facing wording",
                documentation="No wire-contract change.",
            )
        ],
    )

    report = inventory.build_contract_inventory(
        [
            _loaded(inventory, "3.4.2", _snapshot("3.4.2")),
            _loaded(inventory, "3.4.1", documented),
        ]
    )
    baseline = inventory.build_contract_inventory(
        [
            _loaded(inventory, "3.4.1", snapshot),
            _loaded(inventory, "3.4.2", _snapshot("3.4.2")),
        ]
    )
    assert report["schema_version"] == 2

    assert [
        (target["contract_fingerprint"], target["surfaces"])
        for target in report["targets"]
    ] == [
        (target["contract_fingerprint"], target["surfaces"])
        for target in baseline["targets"]
    ]
    assert [target["ds_version"] for target in report["targets"]] == [
        "3.4.1",
        "3.4.2",
    ]
    assert report["targets"][0]["surfaces"]["operations"][0] == {
        "key": "ProjectController.queryAllProjectList",
        "fingerprint": report["targets"][0]["surfaces"]["operations"][0]["fingerprint"],
        "http_method": "GET",
        "path": "projects/list",
    }


def test_inventory_fingerprint_changes_when_wire_contract_changes() -> None:
    inventory = _load_module("ds_codegen.api")
    snapshot = _snapshot("3.4.1")
    changed = replace(
        snapshot,
        operations=[replace(snapshot.operations[0], path="projects/all")],
    )

    original_target = inventory.build_contract_inventory(
        [_loaded(inventory, "3.4.1", snapshot)]
    )["targets"][0]
    changed_target = inventory.build_contract_inventory(
        [_loaded(inventory, "3.4.1", changed)]
    )["targets"][0]

    assert (
        original_target["contract_fingerprint"]
        != changed_target["contract_fingerprint"]
    )
    assert (
        original_target["surfaces"]["operations"][0]["fingerprint"]
        != changed_target["surfaces"]["operations"][0]["fingerprint"]
    )


def test_inventory_fingerprint_binds_qualified_reference_identity() -> None:
    inventory = _load_module("ds_codegen.api")
    ir = _load_module("ds_codegen.ir")
    base = _snapshot("3.4.1")
    operation = replace(
        base.operations[0],
        logical_return_type="example.first.Thing",
        inferred_return_type="example.first.Thing",
    )
    first = replace(
        base,
        operation_count=1,
        model_count=2,
        operations=[operation],
        models=[
            ir.ModelSpec(
                name="Thing",
                import_path=import_path,
                kind="other_class",
                documentation=None,
                extends=None,
                fields=[],
            )
            for import_path in ("example.first.Thing", "example.second.Thing")
        ],
    )
    second = replace(
        first,
        operations=[
            replace(
                operation,
                logical_return_type="example.second.Thing",
                inferred_return_type="example.second.Thing",
            )
        ],
    )

    first_target = inventory.build_contract_inventory(
        [_loaded(inventory, "3.4.1", first)]
    )["targets"][0]
    second_target = inventory.build_contract_inventory(
        [_loaded(inventory, "3.4.1", second)]
    )["targets"][0]

    assert first_target["contract_fingerprint"] != second_target["contract_fingerprint"]


def test_inventory_rejects_mislabeled_or_internally_inconsistent_snapshots() -> None:
    inventory = _load_module("ds_codegen.api")
    snapshot = _snapshot("3.4.1")

    with pytest.raises(
        ValueError,
        match=r"label '3\.4\.2'.*snapshot version '3\.4\.1'",
    ):
        inventory.build_contract_inventory([_loaded(inventory, "3.4.2", snapshot)])

    with pytest.raises(ValueError, match="operation_count declares 2 but contains 1"):
        inventory.build_contract_inventory(
            [_loaded(inventory, "3.4.1", replace(snapshot, operation_count=2))]
        )


def test_inventory_target_keeps_exact_input_and_git_provenance() -> None:
    inventory = _load_module("ds_codegen.api")
    snapshot = _snapshot("3.4.1")

    target = inventory.build_contract_inventory(
        [_loaded(inventory, "3.4.1", snapshot, input_kind="snapshot")]
    )["targets"][0]

    assert target["provenance"] == _provenance(
        inventory,
        "3.4.1",
        snapshot,
        input_kind="snapshot",
    )


def _snapshot(version: str) -> Any:
    ir = _load_module("ds_codegen.ir")
    operation = ir.OperationSpec(
        operation_id="ProjectController.queryAllProjectList",
        controller="ProjectController",
        method_name="queryAllProjectList",
        api_group="PROJECT_TAG",
        http_method="GET",
        path="projects/list",
        summary=None,
        description=None,
        documentation=None,
        parameter_docs={},
        returns_doc=None,
        consumes=[],
        return_type="Result",
        inferred_return_type="List<Project>",
        logical_return_type="List<Project>",
        response_projection="single_data_list",
        parameters=[],
    )
    return ir.ContractSnapshot(
        ds_version=version,
        operation_count=1,
        enum_count=0,
        dto_count=0,
        model_count=0,
        operations=[operation],
        enums=[],
        dtos=[],
        models=[],
    )


def _loaded(
    api: Any,
    label: str,
    snapshot: Any,
    *,
    input_kind: str = "ds-source",
) -> Any:
    return api.LoadedContract(
        label=label,
        snapshot=snapshot,
        provenance=_provenance(api, label, snapshot, input_kind=input_kind),
    )


def _provenance(
    api: Any,
    label: str,
    snapshot: Any,
    *,
    input_kind: str,
) -> dict[str, object]:
    tree = "b" * 40
    contract_digest = api.contract_snapshot_digest(snapshot)
    input_digest = contract_digest if input_kind == "snapshot" else f"git-tree:{tree}"
    return {
        "schema_version": 1,
        "exact": True,
        "input_kind": input_kind,
        "content_digest": input_digest,
        "contract_digest": contract_digest,
        "origin": {
            "kind": "ds-source",
            "content_digest": f"git-tree:{tree}",
            "git": {
                "commit": "a" * 40,
                "tree": tree,
                "tag": label,
                "ref": f"refs/tags/{label}",
                "dirty": False,
            },
        },
    }
