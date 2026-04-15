import json
from pathlib import Path

import pytest
from tests.fakes import (
    FakeDag,
    FakeEnumValue,
    FakeTaskDefinition,
    FakeWorkflow,
    FakeWorkflowTaskRelation,
)

from dsctl.errors import UserInputError
from dsctl.services._workflow_mutation import (
    compile_workflow_mutation_plan,
    load_workflow_patch_or_error,
)
from dsctl.services.resolver import ResolvedProject


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
        project=resolved_project,
        patch=patch,
        release_state=None,
    )

    assert plan.has_changes is False
    assert plan.diff["workflow_updated_fields"] == []
