import json
from pathlib import Path

import pytest
from tests.fakes import (
    FakeDag,
    FakeEnumValue,
    FakeSchedule,
    FakeTaskDefinition,
    FakeWorkflow,
    FakeWorkflowTaskRelation,
)

from dsctl.errors import ApiTransportError, ConflictError, UserInputError
from dsctl.models import WorkflowSpec
from dsctl.services._workflow_mutation import (
    compile_workflow_mutation_plan,
    load_workflow_patch_or_error,
    prepare_workflow_file_edit,
)
from dsctl.services.resolver import ResolvedProject


def _unexpected_task_code_allocation(_count: int) -> list[int]:
    message = "existing-task mutation must not allocate task codes"
    raise AssertionError(message)


@pytest.fixture
def resolved_project() -> ResolvedProject:
    return ResolvedProject(
        code=7,
        name="etl-prod",
        description=None,
    )


@pytest.fixture
def workflow_dag() -> FakeDag:
    workflow_definition = FakeWorkflow(
        code=101,
        name="daily-sync",
        version=1,
        project_code_value=7,
        project_name_value="etl-prod",
        global_params_value='[{"prop":"env","value":"prod"}]',
        global_param_map_value={"env": "prod"},
        timeout=30,
        execution_type_value=FakeEnumValue("PARALLEL"),
    )
    return FakeDag(
        workflow_definition_value=workflow_definition,
        task_definition_list_value=[
            FakeTaskDefinition(
                code=201,
                name="extract",
                version=1,
                project_code_value=7,
                task_type_value="SHELL",
                task_params_value='{"rawScript":"echo extract"}',
                worker_group_value="default",
                project_name_value="etl-prod",
            ),
            FakeTaskDefinition(
                code=202,
                name="load",
                version=1,
                project_code_value=7,
                task_type_value="SHELL",
                task_params_value='{"rawScript":"echo load"}',
                worker_group_value="default",
                project_name_value="etl-prod",
            ),
        ],
        workflow_task_relation_list_value=[
            FakeWorkflowTaskRelation(
                pre_task_code_value=201,
                post_task_code_value=202,
            )
        ],
    )


def test_load_workflow_patch_or_error_preserves_file_context(
    tmp_path: Path,
) -> None:
    patch_file = tmp_path / "bad.patch.yaml"
    patch_file.write_text("patch: []\n", encoding="utf-8")

    with pytest.raises(
        UserInputError,
        match="valid dictionary or instance of WorkflowPatchSpec",
    ) as exc_info:
        load_workflow_patch_or_error(patch_file)

    assert exc_info.value.details == {"file": str(patch_file)}


def test_prepare_workflow_file_edit_rejects_schedule_only_online_intent() -> None:
    spec = WorkflowSpec.model_validate(
        {
            "workflow": {
                "name": "daily-sync",
                "release_state": "OFFLINE",
            },
            "tasks": [
                {
                    "name": "extract",
                    "type": "SHELL",
                    "command": "echo extract",
                }
            ],
            "schedule": {
                "cron": "0 0 0 * * ?",
                "timezone": "UTC",
                "start": "2026-01-01 00:00:00",
                "end": "2026-12-31 23:59:59",
                "release_state": "ONLINE",
            },
        }
    )
    attached_schedule = FakeSchedule(
        id=23,
        workflow_definition_code_value=101,
        project_code_value=7,
        start_time_value="2026-01-01 00:00:00",
        end_time_value="2026-12-31 23:59:59",
        timezone_id_value="UTC",
        crontab_value="0 0 0 * * ?",
        release_state_value=FakeEnumValue("OFFLINE"),
    )

    with pytest.raises(ConflictError) as captured:
        prepare_workflow_file_edit(
            spec,
            attached_schedule=attached_schedule,
            workflow_release_state="OFFLINE",
        )

    assert captured.value.details["mismatched_fields"] == ["release_state"]


@pytest.mark.parametrize(
    ("field_name", "explicit_null"),
    [
        ("failure_strategy", False),
        ("failure_strategy", True),
        ("priority", False),
        ("priority", True),
        ("release_state", False),
        ("release_state", True),
    ],
)
def test_prepare_workflow_file_edit_rejects_weakened_schedule_snapshot(
    field_name: str,
    *,
    explicit_null: bool,
) -> None:
    schedule: dict[str, object] = {
        "cron": "0 0 0 * * ?",
        "timezone": "UTC",
        "start": "2026-01-01 00:00:00",
        "end": "2026-12-31 23:59:59",
        "failure_strategy": "END",
        "priority": "HIGH",
        "release_state": "ONLINE",
    }
    if explicit_null:
        schedule[field_name] = None
    else:
        del schedule[field_name]
    spec = WorkflowSpec.model_validate(
        {
            "workflow": {
                "name": "daily-sync",
                "release_state": "ONLINE",
            },
            "tasks": [
                {
                    "name": "extract",
                    "type": "SHELL",
                    "command": "echo extract",
                }
            ],
            "schedule": schedule,
        }
    )
    attached_schedule = FakeSchedule(
        id=23,
        workflow_definition_code_value=101,
        project_code_value=7,
        start_time_value="2026-01-01 00:00:00",
        end_time_value="2026-12-31 23:59:59",
        timezone_id_value="UTC",
        crontab_value="0 0 0 * * ?",
        failure_strategy_value=FakeEnumValue("END"),
        workflow_instance_priority_value=FakeEnumValue("HIGH"),
        release_state_value=FakeEnumValue("ONLINE"),
    )

    with pytest.raises(ConflictError) as captured:
        prepare_workflow_file_edit(
            spec,
            attached_schedule=attached_schedule,
            workflow_release_state="ONLINE",
        )

    assert captured.value.details["mismatched_fields"] == [field_name]
    current_value = {
        "failure_strategy": "END",
        "priority": "HIGH",
        "release_state": "ONLINE",
    }[field_name]
    assert captured.value.details["mismatches"] == {
        field_name: {"file": None, "current": current_value}
    }


def test_compile_workflow_mutation_plan_preserves_existing_task_identity(
    tmp_path: Path,
    workflow_dag: FakeDag,
    resolved_project: ResolvedProject,
) -> None:
    patch_file = tmp_path / "rename.patch.yaml"
    patch_file.write_text(
        """
patch:
  workflow:
    set:
      timeout: 45
  tasks:
    rename:
      - from: extract
        to: extract-v2
""".strip(),
        encoding="utf-8",
    )
    patch = load_workflow_patch_or_error(patch_file)

    plan = compile_workflow_mutation_plan(
        workflow_dag,
        allocate_task_codes=_unexpected_task_code_allocation,
        project=resolved_project,
        patch=patch,
        release_state="OFFLINE",
    )
    task_definition_payload = json.loads(plan.payload["taskDefinitionJson"])

    assert plan.has_changes is True
    assert plan.merged_spec.workflow.timeout == 45
    assert [task.name for task in plan.merged_spec.tasks] == ["extract-v2", "load"]
    assert [item["code"] for item in task_definition_payload] == [201, 202]
    assert [item["name"] for item in task_definition_payload] == [
        "extract-v2",
        "load",
    ]
    assert plan.payload["releaseState"] == "OFFLINE"


def test_compile_workflow_mutation_plan_marks_noop_patch(
    tmp_path: Path,
    workflow_dag: FakeDag,
    resolved_project: ResolvedProject,
) -> None:
    patch_file = tmp_path / "noop.patch.yaml"
    patch_file.write_text(
        """
patch:
  workflow:
    set:
      timeout: 30
""".strip(),
        encoding="utf-8",
    )
    patch = load_workflow_patch_or_error(patch_file)

    plan = compile_workflow_mutation_plan(
        workflow_dag,
        allocate_task_codes=_unexpected_task_code_allocation,
        project=resolved_project,
        patch=patch,
        release_state=None,
    )

    assert plan.has_changes is False
    assert plan.diff["workflow_updated_fields"] == []


@pytest.mark.parametrize(
    ("patch_text", "allocated_code"),
    [
        (
            """
patch:
  tasks:
    create:
      - name: verify
        type: SHELL
        command: echo verify
        depends_on: [extract]
    delete:
      - load
""".strip(),
            202,
        ),
        (
            """
patch:
  tasks:
    create:
      - name: verify
        type: SHELL
        command: echo verify
        depends_on: [extract-v2]
    rename:
      - from: extract
        to: extract-v2
""".strip(),
            201,
        ),
    ],
)
def test_compile_workflow_mutation_rejects_codes_colliding_with_any_live_task(
    tmp_path: Path,
    workflow_dag: FakeDag,
    resolved_project: ResolvedProject,
    patch_text: str,
    allocated_code: int,
) -> None:
    patch_file = tmp_path / "collision.patch.yaml"
    patch_file.write_text(patch_text, encoding="utf-8")
    patch = load_workflow_patch_or_error(patch_file)

    with pytest.raises(
        ApiTransportError,
        match="collided with an existing task code",
    ):
        compile_workflow_mutation_plan(
            workflow_dag,
            allocate_task_codes=lambda _count: [allocated_code],
            project=resolved_project,
            patch=patch,
            release_state="OFFLINE",
        )
