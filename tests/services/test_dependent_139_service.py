from __future__ import annotations

import json
from typing import TYPE_CHECKING, cast

import pytest
import yaml
from tests.fakes import (
    FakeProject,
    FakeProjectAdapter,
    FakeTaskAdapter,
    FakeWorkflow,
    FakeWorkflowAdapter,
)
from tests.request_assertions import first_dry_run_request
from tests.support import make_profile
from tests.value_shape_assertions import assert_mapping as _mapping
from tests.value_shape_assertions import assert_sequence as _sequence
from tests.workflow_domain_fakes import (
    _FakeWorkflowOperations,
    install_workflow_domain_runtime,
)

from dsctl.errors import ApiResultError, PermissionDeniedError, UserInputError
from dsctl.models.workflow_spec import validate_workflow_document
from dsctl.services import workflow as workflow_service
from dsctl.services._workflow import authoring as workflow_authoring_service
from dsctl.services._workflow.authoring import workflow_authoring_context
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.upstream.legacy_workflow_graph import (
    LegacyDependentRefIndex,
    prepare_legacy_workflow_graph,
)

if TYPE_CHECKING:
    from pathlib import Path


def test_create_resolves_cross_project_names_to_exact_split_wire(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _install_runtime(monkeypatch, task_names=("extract", "publish"))
    spec_path = _write_spec(tmp_path)

    result = workflow_service.create_workflow_result(file=spec_path, dry_run=True)

    request = _mapping(first_dry_run_request(_mapping(result.data)))
    process = _mapping(
        json.loads(cast("str", _mapping(request["form"])["processDefinitionJson"]))
    )
    task = _mapping(_sequence(process["tasks"])[0])
    assert task["params"] == {}
    assert task["dependence"] == {
        "relation": "AND",
        "dependTaskList": [
            {
                "relation": "OR",
                "dependItemList": [
                    {
                        "projectId": 7,
                        "definitionId": 101,
                        "depTasks": "publish",
                        "cycle": "day",
                        "dateValue": "last1Days",
                    }
                ],
            }
        ],
    }


def test_create_translates_missing_target_task_before_prepare(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _install_runtime(monkeypatch, task_names=("extract",))

    with pytest.raises(UserInputError, match=r"DEPENDENT target.*publish"):
        workflow_service.create_workflow_result(
            file=_write_spec(tmp_path),
            dry_run=True,
        )


def test_create_preserves_target_permission_error_type(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    operations = _install_runtime(monkeypatch, task_names=("publish",))

    def deny_project(_project_name: str) -> object:
        raise ApiResultError(
            result_code=30001,
            result_message="no target project permission",
        )

    monkeypatch.setattr(operations, "resolve_project_by_name", deny_project)

    with pytest.raises(PermissionDeniedError, match="DEPENDENT target"):
        workflow_service.create_workflow_result(
            file=_write_spec(tmp_path),
            dry_run=True,
        )


def test_export_reverse_binds_safe_native_target_to_canonical_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_runtime(
        monkeypatch,
        task_names=("extract", "publish"),
        include_parent=True,
    )

    result = workflow_service.export_workflow_yaml_result(
        "downstream",
        project="orchestration",
    )

    document = _mapping(yaml.safe_load(cast("str", _mapping(result.data)["yaml"])))
    task = _mapping(_sequence(document["tasks"])[0])
    params = _mapping(task["task_params"])
    dependence = _mapping(params["dependence"])
    group = _mapping(_sequence(dependence["dependTaskList"])[0])
    item = _mapping(_sequence(group["dependItemList"])[0])
    assert item == {
        "dependentType": "DEPENDENT_ON_TASK",
        "projectName": "analytics",
        "workflowName": "upstream-daily",
        "taskName": "publish",
        "cycle": "day",
        "dateValue": "last1Days",
    }


def test_edit_roundtrip_recompiles_canonical_target_to_same_split_wire(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _install_runtime(
        monkeypatch,
        task_names=("extract", "publish"),
        include_parent=True,
    )
    exported = workflow_service.export_workflow_yaml_result(
        "downstream",
        project="orchestration",
    )
    document = cast(
        "dict[str, object]",
        yaml.safe_load(cast("str", _mapping(exported.data)["yaml"])),
    )
    workflow = cast("dict[str, object]", document["workflow"])
    workflow["description"] = "metadata-only edit"
    spec_path = tmp_path / "roundtrip.yaml"
    spec_path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")

    result = workflow_service.edit_workflow_result(
        "downstream",
        project="orchestration",
        file=spec_path,
        dry_run=True,
    )

    request = _mapping(first_dry_run_request(_mapping(result.data)))
    process = _mapping(
        json.loads(cast("str", _mapping(request["form"])["processDefinitionJson"]))
    )
    task = _mapping(_sequence(process["tasks"])[0])
    item = _mapping(
        _sequence(
            _mapping(_sequence(_mapping(task["dependence"])["dependTaskList"])[0])[
                "dependItemList"
            ]
        )[0]
    )
    assert item == {
        "projectId": 7,
        "definitionId": 101,
        "depTasks": "publish",
        "cycle": "day",
        "dateValue": "last1Days",
    }


def test_metadata_patch_preserves_unresolved_richer_native_dependent(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    operations = _install_runtime(
        monkeypatch,
        task_names=("extract", "publish"),
        include_parent=True,
    )
    process_json, locations, connects = operations.legacy_definitions[303]
    process = cast("dict[str, object]", json.loads(process_json))
    native_task = cast(
        "dict[str, object]",
        cast("list[object]", process["tasks"])[0],
    )
    dependence = cast("dict[str, object]", native_task["dependence"])
    dependence["futureState"] = {"serverOwned": True}
    operations.legacy_definitions[303] = (
        json.dumps(process),
        locations,
        connects,
    )
    patch_path = tmp_path / "metadata.patch.yaml"
    patch_path.write_text(
        "patch:\n  workflow:\n    set:\n      description: metadata only\n",
        encoding="utf-8",
    )

    result = workflow_service.edit_workflow_result(
        "downstream",
        project="orchestration",
        patch=patch_path,
        dry_run=True,
    )

    request = _mapping(first_dry_run_request(_mapping(result.data)))
    compiled = _mapping(
        json.loads(cast("str", _mapping(request["form"])["processDefinitionJson"]))
    )
    compiled_task = _mapping(_sequence(compiled["tasks"])[0])
    assert compiled_task["dependence"] == dependence


def _install_runtime(
    monkeypatch: pytest.MonkeyPatch,
    *,
    task_names: tuple[str, ...],
    include_parent: bool = False,
) -> _FakeWorkflowOperations:
    workflows = [
        FakeWorkflow(
            code=101,
            id=101,
            name="upstream-daily",
            project_code_value=7,
            project_name_value="analytics",
        )
    ]
    if include_parent:
        workflows.append(
            FakeWorkflow(
                code=303,
                id=303,
                name="downstream",
                project_code_value=9,
                project_name_value="orchestration",
            )
        )
    operations = install_workflow_domain_runtime(
        monkeypatch,
        project_adapter=FakeProjectAdapter(
            projects=[
                FakeProject(code=9, name="orchestration"),
                FakeProject(code=7, name="analytics"),
            ]
        ),
        workflow_adapter=FakeWorkflowAdapter(
            workflows=workflows,
            dags={},
        ),
        task_adapter=FakeTaskAdapter(workflow_tasks={}),
        profile=make_profile(ds_version="1.3.9"),
    )
    operations.legacy_definitions[101] = (
        json.dumps(
            {
                "tasks": [
                    {
                        "id": f"tasks-{index}",
                        "name": task_name,
                        "type": "SHELL",
                        "params": {"rawScript": "true", "resourceList": []},
                        "preTasks": [],
                    }
                    for index, task_name in enumerate(task_names, start=1)
                ]
            }
        ),
        "{}",
        "[]",
    )
    if include_parent:
        parent = validate_workflow_document(
            {
                "workflow": {
                    "name": "downstream",
                    "project": "orchestration",
                },
                "tasks": [
                    {
                        "name": "wait-upstream",
                        "type": "DEPENDENT",
                        "task_params": {
                            "dependence": {
                                "relation": "AND",
                                "dependTaskList": [
                                    {
                                        "relation": "OR",
                                        "dependItemList": [
                                            {
                                                "dependentType": ("DEPENDENT_ON_TASK"),
                                                "projectName": "analytics",
                                                "workflowName": "upstream-daily",
                                                "taskName": "publish",
                                                "cycle": "day",
                                                "dateValue": "last1Days",
                                            }
                                        ],
                                    }
                                ],
                            }
                        },
                    }
                ],
            },
            authoring_context=workflow_authoring_context(
                catalog=get_task_authoring_catalog("1.3.9"),
                intent=TaskAuthoringIntent.TYPED_CREATE,
            ),
        )
        prepared = prepare_legacy_workflow_graph(
            parent,
            task_id_factory=lambda _name: "tasks-dependent",
            dependent_refs=LegacyDependentRefIndex.from_native_by_selector(
                {
                    ("analytics", "upstream-daily", "publish"): (
                        7,
                        101,
                        "publish",
                    )
                }
            ),
        ).materialize()
        operations.legacy_definitions[303] = (
            prepared["processDefinitionJson"],
            prepared["locations"],
            prepared["connects"],
        )
    monkeypatch.setattr(
        workflow_authoring_service,
        "load_selected_task_authoring_catalog",
        lambda _env_file: get_task_authoring_catalog("1.3.9"),
    )
    return operations


def _write_spec(tmp_path: Path) -> Path:
    spec_path = tmp_path / "dependent.yaml"
    spec_path.write_text(
        """
workflow:
  name: downstream
  project: orchestration
tasks:
  - name: wait-upstream
    type: DEPENDENT
    task_params:
      dependence:
        relation: AND
        dependTaskList:
          - relation: OR
            dependItemList:
              - dependentType: DEPENDENT_ON_TASK
                projectName: analytics
                workflowName: upstream-daily
                taskName: publish
                cycle: day
                dateValue: last1Days
""".strip(),
        encoding="utf-8",
    )
    return spec_path
