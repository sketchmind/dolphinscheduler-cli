"""DS 3.1.0 main-table identity and two-view readback safeguards."""

from __future__ import annotations

import json
from dataclasses import replace
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import pytest
from tests.fakes import FakeDag, FakeTaskDefinition
from tests.request_assertions import first_dry_run_request
from tests.support import make_profile
from tests.value_shape_assertions import assert_mapping as _mapping

from dsctl.errors import (
    ApiResultError,
    ApiTransportError,
    NotFoundError,
    PermissionDeniedError,
)
from dsctl.services import workflow as workflow_service
from dsctl.services._workflow.main_task_ids import (
    read_main_task_ids,
    verify_main_task_views,
)

if TYPE_CHECKING:
    from pathlib import Path

    from tests.services.workflow.harness import _WorkflowServiceHarness
    from tests.workflow_domain_fakes import _FakePreparedWorkflowMutation

    from dsctl.upstream.protocol import WorkflowDagRecord
    from dsctl.upstream.task_definition_wire import TaskDefinitionWire


class _Wire:
    def __init__(self, tasks: dict[int, FakeTaskDefinition]) -> None:
        self.tasks = tasks
        self.calls: list[int] = []
        self.error: ApiResultError | None = None

    def get(self, *, project_code: int, task_code: int) -> object:
        del project_code
        self.calls.append(task_code)
        if self.error is not None:
            raise self.error
        return SimpleNamespace(payload=self.tasks[task_code])


def _task(code: int, *, main_id: int | None, name: str = "run") -> FakeTaskDefinition:
    return FakeTaskDefinition(
        code=code,
        name=name,
        id=main_id,
        project_code_value=7,
        task_type_value="SHELL",
        task_params_value='{"rawScript":"echo ok"}',
    )


def _read(wire: _Wire) -> dict[int, int]:
    return read_main_task_ids(
        cast("TaskDefinitionWire", wire),
        project_code=7,
        task_codes=(202, 201, 201),
        resource="workflow",
    )


def test_reads_each_existing_main_task_once_and_ignores_log_ids() -> None:
    wire = _Wire({201: _task(201, main_id=901), 202: _task(202, main_id=902)})
    assert _read(wire) == {201: 901, 202: 902}
    assert wire.calls == [201, 202]


@pytest.mark.parametrize("bad", [None, 0, -1, True])
def test_rejects_missing_or_non_positive_main_id(bad: int | None) -> None:
    wire = _Wire(
        {201: replace(_task(201, main_id=901), id=bad), 202: _task(202, main_id=902)}
    )
    with pytest.raises(ApiTransportError, match="positive main id"):
        _read(wire)


@pytest.mark.parametrize(
    "wrong",
    [
        replace(_task(201, main_id=901), code=999),
        replace(_task(201, main_id=901), project_code_value=99),
    ],
)
def test_rejects_wrong_task_or_project_before_mutation(
    wrong: FakeTaskDefinition,
) -> None:
    wire = _Wire({201: wrong, 202: _task(202, main_id=902)})
    with pytest.raises(ApiTransportError, match="different identity"):
        _read(wire)


def test_rejects_one_main_id_reused_by_two_codes() -> None:
    wire = _Wire({201: _task(201, main_id=901), 202: _task(202, main_id=901)})
    with pytest.raises(ApiTransportError, match="reused one main id"):
        _read(wire)


@pytest.mark.parametrize(
    ("code", "error_type"),
    [
        (30002, PermissionDeniedError),
        (10190, NotFoundError),
        (10018, NotFoundError),
        (50030, NotFoundError),
        (50099, ApiTransportError),
    ],
)
def test_task_get_errors_are_translated(code: int, error_type: type[Exception]) -> None:
    wire = _Wire({201: _task(201, main_id=901), 202: _task(202, main_id=902)})
    wire.error = ApiResultError(
        result_code=code, result_message="private upstream detail"
    )
    with pytest.raises(error_type) as caught:
        _read(wire)
    assert "private upstream detail" not in str(caught.value)


def test_post_write_requires_main_detail_to_match_dag_and_original_id() -> None:
    main = _task(201, main_id=901, name="renamed")
    wire = _Wire({201: main})
    dag_task = replace(main, id=411)
    dag = cast(
        "WorkflowDagRecord",
        FakeDag(
            workflow_definition_value=None,
            task_definition_list_value=[dag_task],
            workflow_task_relation_list_value=[],
        ),
    )
    verify_main_task_views(
        cast("TaskDefinitionWire", wire),
        project_code=7,
        dag=dag,
        changed_names_by_code={201: "renamed"},
        original_ids={201: 901},
        resource="workflow",
    )
    wire.tasks[201] = replace(main, name="stale")
    with pytest.raises(ApiTransportError, match="main detail differs"):
        verify_main_task_views(
            cast("TaskDefinitionWire", wire),
            project_code=7,
            dag=dag,
            changed_names_by_code={201: "renamed"},
            original_ids={201: 901},
            resource="workflow",
        )


def test_workflow_edit_uses_main_ids_for_rename_and_never_ids_new_task(
    monkeypatch: pytest.MonkeyPatch,
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
) -> None:
    workflow_harness.workflow_adapter.offline(project_code=7, workflow_code=101)
    wire = _Wire(
        {
            201: _task(201, main_id=901, name="extract"),
            202: _task(202, main_id=902, name="load"),
        }
    )
    operations = workflow_harness.install(
        profile=make_profile(ds_version="3.1.0"),
        task_definition_wire=cast("TaskDefinitionWire", wire),
    )
    patch = tmp_path / "rename-create.yaml"
    patch.write_text(
        """patch:
  tasks:
    rename:
      - {from: extract, to: extract-v2}
    create:
      - {name: verify, type: SHELL, command: 'echo verify'}
""",
        encoding="utf-8",
    )
    preview = workflow_service.edit_workflow_result(
        "daily-sync", patch=patch, project="etl-prod", dry_run=True
    )
    form = _mapping(_mapping(first_dry_run_request(_mapping(preview.data)))["form"])
    tasks = json.loads(str(form["taskDefinitionJson"]))
    assert [(task["name"], task.get("id")) for task in tasks] == [
        ("extract-v2", 901),
        ("load", 902),
        ("verify", None),
    ]
    assert wire.calls == [201, 202]

    original_apply = operations.apply_update

    def apply_and_update_main(prepared: _FakePreparedWorkflowMutation) -> None:
        original_apply(prepared)
        for task in (
            workflow_harness.workflow_adapter.dags[101].taskDefinitionList or ()
        ):
            if task.code in {201, 202}:
                wire.tasks[task.code] = replace(
                    task, id={201: 901, 202: 902}[task.code]
                )
            else:
                wire.tasks[task.code] = replace(task, id=903)

    monkeypatch.setattr(operations, "apply_update", apply_and_update_main)
    result = workflow_service.edit_workflow_result(
        "daily-sync", patch=patch, project="etl-prod"
    )
    assert _mapping(result.resolved["mutation"])["mutation_applied"] is True
    assert wire.calls[2:4] == [201, 202]
    assert len(wire.calls) == 6  # two reads per preflight, then two readbacks


@pytest.mark.parametrize(
    ("mode", "error_type"),
    [
        ("missing_id", ApiTransportError),
        ("wrong_project", ApiTransportError),
        ("read_failed", NotFoundError),
    ],
)
def test_workflow_edit_rejects_unproved_main_id_before_mutation(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    mode: str,
    error_type: type[Exception],
) -> None:
    workflow_harness.workflow_adapter.offline(project_code=7, workflow_code=101)
    first = _task(201, main_id=901, name="extract")
    if mode == "missing_id":
        first = replace(first, id=None)
    elif mode == "wrong_project":
        first = replace(first, project_code_value=99)
    wire = _Wire({201: first, 202: _task(202, main_id=902, name="load")})
    if mode == "read_failed":
        wire.error = ApiResultError(result_code=50030, result_message="private")
    workflow_harness.install(
        profile=make_profile(ds_version="3.1.0"),
        task_definition_wire=cast("TaskDefinitionWire", wire),
    )
    patch = tmp_path / "rename.yaml"
    patch.write_text(
        "patch:\n  tasks:\n    rename:\n      - {from: extract, to: extract-v2}\n",
        encoding="utf-8",
    )
    with pytest.raises(error_type):
        workflow_service.edit_workflow_result(
            "daily-sync", patch=patch, project="etl-prod"
        )
    assert wire.calls == [201]
    assert workflow_harness.workflow_adapter.update_calls == []
