"""Legacy string-identity workflow creation, inspection, and edits."""

import json
from dataclasses import replace
from pathlib import Path

import pytest
import yaml
from tests.fakes import (
    FakeScheduleAdapter,
    FakeTaskAdapter,
    FakeWorkflowAdapter,
)
from tests.request_assertions import first_dry_run_request
from tests.services.workflow.harness import (
    _WorkflowServiceHarness,
)
from tests.support import make_profile
from tests.value_shape_assertions import assert_mapping as _mapping
from tests.value_shape_assertions import assert_sequence as _sequence

from dsctl.errors import (
    ConfirmationRequiredError,
    InvalidStateError,
)
from dsctl.models import WorkflowSpec
from dsctl.services import workflow as workflow_service
from dsctl.services._workflow import authoring as workflow_authoring_service
from dsctl.services.task_authoring_catalog import (
    get_task_authoring_catalog,
)
from dsctl.upstream.legacy_workflow_graph import prepare_legacy_workflow_graph


def test_139_create_workflow_dry_run_projects_string_native_graph(
    monkeypatch: pytest.MonkeyPatch,
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
) -> None:
    workflow_harness.install(
        profile=make_profile(ds_version="1.3.9"),
    )
    monkeypatch.setattr(
        workflow_authoring_service,
        "load_selected_task_authoring_catalog",
        lambda _env_file: get_task_authoring_catalog("1.3.9"),
    )
    spec_path = tmp_path / "legacy-workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: legacy-daily
  project: etl-prod
  description: Legacy graph
tasks:
  - name: extract
    type: SHELL
    command: echo extract
  - name: load
    type: SHELL
    command: echo load
    depends_on: [extract]
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.create_workflow_result(file=spec_path, dry_run=True)

    request = _mapping(first_dry_run_request(_mapping(result.data)))
    form = _mapping(request["form"])
    assert request["method"] == "POST"
    assert request["path"] == "/projects/etl-prod/process/save"
    assert set(form) == {
        "name",
        "description",
        "processDefinitionJson",
        "locations",
        "connects",
    }
    process_definition = _mapping(json.loads(str(form["processDefinitionJson"])))
    tasks = _sequence(process_definition["tasks"])
    assert [(_mapping(task)["name"], _mapping(task)["preTasks"]) for task in tasks] == [
        ("extract", []),
        ("load", ["extract"]),
    ]
    assert all("code" not in _mapping(task) for task in tasks)
    assert all("version" not in _mapping(task) for task in tasks)
    connects = _sequence(json.loads(str(form["connects"])))
    assert len(connects) == 1
    source_id = _mapping(tasks[0])["id"]
    target_id = _mapping(tasks[1])["id"]
    assert connects == [
        {
            "endPointSourceId": source_id,
            "endPointTargetId": target_id,
        }
    ]


def test_139_create_workflow_applies_without_allocating_modern_task_codes(
    monkeypatch: pytest.MonkeyPatch,
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    workflow_harness.install(
        profile=make_profile(ds_version="1.3.9"),
    )
    monkeypatch.setattr(
        workflow_authoring_service,
        "load_selected_task_authoring_catalog",
        lambda _env_file: get_task_authoring_catalog("1.3.9"),
    )
    spec_path = tmp_path / "legacy-workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: legacy-daily
  project: etl-prod
  description: Legacy graph
tasks:
  - name: extract
    type: SHELL
    command: echo extract
  - name: load
    type: SHELL
    command: echo load
    depends_on: [extract]
""".strip(),
        encoding="utf-8",
    )

    previous_codes = [workflow.code for workflow in fake_workflow_adapter.workflows]
    result = workflow_service.create_workflow_result(file=spec_path, dry_run=False)

    data = _mapping(result.data)
    assert data["id"] == max(previous_codes) + 1
    assert "code" not in data
    assert data["name"] == "legacy-daily"
    assert fake_task_adapter.generate_code_calls == []
    assert fake_workflow_adapter.workflows[-1].name == "legacy-daily"


def test_139_created_workflow_exports_describes_and_digests_without_codes(
    monkeypatch: pytest.MonkeyPatch,
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
) -> None:
    workflow_harness.install(
        profile=make_profile(ds_version="1.3.9"),
    )
    monkeypatch.setattr(
        workflow_authoring_service,
        "load_selected_task_authoring_catalog",
        lambda _env_file: get_task_authoring_catalog("1.3.9"),
    )
    spec_path = tmp_path / "legacy-workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: legacy-daily
  project: etl-prod
  description: Legacy graph
tasks:
  - name: extract
    type: SHELL
    command: echo extract
  - name: load
    type: SHELL
    command: echo load
    depends_on: [extract]
""".strip(),
        encoding="utf-8",
    )
    workflow_service.create_workflow_result(file=spec_path)

    exported = workflow_service.export_workflow_yaml_result(
        "legacy-daily",
        project="etl-prod",
    )
    described = workflow_service.describe_workflow_result(
        "legacy-daily",
        project="etl-prod",
    )
    digested = workflow_service.digest_workflow_result(
        "legacy-daily",
        project="etl-prod",
    )

    document = yaml.safe_load(str(_mapping(exported.data)["yaml"]))
    assert [
        (task["name"], task.get("depends_on", [])) for task in document["tasks"]
    ] == [("extract", []), ("load", ["extract"])]
    assert all("code" not in task for task in document["tasks"])
    described_tasks = _sequence(_mapping(described.data)["tasks"])
    assert all("id" in _mapping(task) for task in described_tasks)
    assert all("code" not in _mapping(task) for task in described_tasks)
    digest = _mapping(digested.data)
    assert digest["taskCount"] == 2
    assert digest["relationCount"] == 1
    assert all("id" in _mapping(task) for task in _sequence(digest["tasks"]))


def test_139_edit_workflow_dry_run_and_apply_preserve_string_native_graph(
    monkeypatch: pytest.MonkeyPatch,
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    operations = workflow_harness.install(
        profile=make_profile(ds_version="1.3.9"),
    )
    monkeypatch.setattr(
        workflow_authoring_service,
        "load_selected_task_authoring_catalog",
        lambda _env_file: get_task_authoring_catalog("1.3.9"),
    )
    spec_path = tmp_path / "legacy-workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: legacy-daily
  project: etl-prod
tasks:
  - name: extract
    type: SHELL
    command: echo extract
  - name: load
    type: SHELL
    command: echo load
    depends_on: [extract]
""".strip(),
        encoding="utf-8",
    )
    created = workflow_service.create_workflow_result(file=spec_path)
    workflow_id = _mapping(created.data)["id"]
    assert isinstance(workflow_id, int)
    original_process = json.loads(operations.legacy_definitions[workflow_id][0])
    original_ids = {task["name"]: task["id"] for task in original_process["tasks"]}
    patch_path = tmp_path / "legacy.patch.yaml"
    patch_path.write_text(
        """
patch:
  workflow:
    set:
      timeout: 45
  tasks:
    update:
      - match:
          name: load
        set:
          command: echo changed
""".strip(),
        encoding="utf-8",
    )

    dry_run = workflow_service.edit_workflow_result(
        "legacy-daily",
        patch=patch_path,
        project="etl-prod",
        dry_run=True,
    )

    request = _mapping(first_dry_run_request(_mapping(dry_run.data)))
    form = _mapping(request["form"])
    assert request["method"] == "POST"
    assert request["path"] == "/projects/etl-prod/process/update"
    assert form["id"] == workflow_id
    dry_run_process = json.loads(str(form["processDefinitionJson"]))
    assert {task["name"]: task["id"] for task in dry_run_process["tasks"]} == (
        original_ids
    )
    assert fake_task_adapter.generate_code_calls == []

    applied = workflow_service.edit_workflow_result(
        "legacy-daily",
        patch=patch_path,
        project="etl-prod",
    )

    assert _mapping(applied.data)["id"] == workflow_id
    assert "code" not in _mapping(applied.data)
    assert fake_task_adapter.generate_code_calls == []
    stored_process = json.loads(operations.legacy_definitions[workflow_id][0])
    stored_tasks = {task["name"]: task for task in stored_process["tasks"]}
    assert stored_process["timeout"] == 45
    assert stored_tasks["load"]["params"]["rawScript"] == "echo changed"
    assert {name: task["id"] for name, task in stored_tasks.items()} == original_ids


def test_139_edit_workflow_noop_does_not_send_update(
    monkeypatch: pytest.MonkeyPatch,
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
) -> None:
    operations = workflow_harness.install(
        profile=make_profile(ds_version="1.3.9"),
    )
    monkeypatch.setattr(
        workflow_authoring_service,
        "load_selected_task_authoring_catalog",
        lambda _env_file: get_task_authoring_catalog("1.3.9"),
    )
    graph = prepare_legacy_workflow_graph(
        WorkflowSpec.model_validate(
            {
                "workflow": {"name": "daily-sync", "project": "etl-prod"},
                "tasks": [
                    {
                        "name": "extract",
                        "type": "SHELL",
                        "command": "echo extract",
                    }
                ],
            }
        ),
        task_id_factory=lambda name: f"tasks-{name}",
    ).materialize()
    operations.legacy_definitions[101] = (
        graph["processDefinitionJson"],
        graph["locations"],
        graph["connects"],
    )
    monkeypatch.setattr(
        operations,
        "apply_update",
        lambda _prepared: pytest.fail("no-op legacy edit sent an update request"),
    )
    patch_path = tmp_path / "noop.patch.yaml"
    patch_path.write_text(
        """
patch:
  workflow:
    set:
      timeout: 0
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.edit_workflow_result(
        "daily-sync",
        patch=patch_path,
        project="etl-prod",
    )

    assert "no persistent workflow change" in result.warnings[0]
    assert _mapping(result.data)["id"] == 101


def test_139_edit_workflow_apply_keeps_online_constraint_id_native(
    monkeypatch: pytest.MonkeyPatch,
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    embedded_schedule = fake_workflow_adapter.workflows[0].schedule
    assert embedded_schedule is not None
    schedule_adapter = FakeScheduleAdapter(
        schedules=[
            replace(
                embedded_schedule,
                workflow_definition_code_value=101,
                project_code_value=7,
            )
        ]
    )
    operations = workflow_harness.install(
        schedule_adapter=schedule_adapter,
        profile=make_profile(ds_version="1.3.9"),
    )
    monkeypatch.setattr(
        workflow_authoring_service,
        "load_selected_task_authoring_catalog",
        lambda _env_file: get_task_authoring_catalog("1.3.9"),
    )
    graph = prepare_legacy_workflow_graph(
        WorkflowSpec.model_validate(
            {
                "workflow": {"name": "daily-sync", "project": "etl-prod"},
                "tasks": [
                    {
                        "name": "extract",
                        "type": "SHELL",
                        "command": "echo extract",
                    }
                ],
            }
        ),
        task_id_factory=lambda name: f"tasks-{name}",
    ).materialize()
    operations.legacy_definitions[101] = (
        graph["processDefinitionJson"],
        graph["locations"],
        graph["connects"],
    )
    patch_path = tmp_path / "change.patch.yaml"
    patch_path.write_text(
        """
patch:
  workflow:
    set:
      timeout: 1
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(InvalidStateError) as captured:
        workflow_service.edit_workflow_result(
            "daily-sync",
            patch=patch_path,
            project="etl-prod",
        )

    assert captured.value.details["id"] == 101
    assert "code" not in captured.value.details
    assert captured.value.details["current_release_state"] == "ONLINE"
    assert "attached schedule offline" in str(captured.value.details["schedule_impact"])


def test_139_full_file_edit_keeps_destructive_confirmation(
    monkeypatch: pytest.MonkeyPatch,
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    operations = workflow_harness.install(
        profile=make_profile(ds_version="1.3.9"),
    )
    monkeypatch.setattr(
        workflow_authoring_service,
        "load_selected_task_authoring_catalog",
        lambda _env_file: get_task_authoring_catalog("1.3.9"),
    )
    spec_path = tmp_path / "legacy-workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: legacy-confirm
  project: etl-prod
tasks:
  - name: extract
    type: SHELL
    command: echo extract
  - name: load
    type: SHELL
    command: echo load
    depends_on: [extract]
""".strip(),
        encoding="utf-8",
    )
    created = workflow_service.create_workflow_result(file=spec_path)
    workflow_id = _mapping(created.data)["id"]
    assert isinstance(workflow_id, int)
    exported = workflow_service.export_workflow_yaml_result(
        "legacy-confirm",
        project="etl-prod",
    )
    document = yaml.safe_load(str(_mapping(exported.data)["yaml"]))
    document["tasks"] = document["tasks"][:1]
    desired_path = tmp_path / "legacy-desired.yaml"
    desired_path.write_text(
        yaml.safe_dump(document, sort_keys=False),
        encoding="utf-8",
    )

    with pytest.raises(ConfirmationRequiredError) as captured:
        workflow_service.edit_workflow_result(
            "legacy-confirm",
            file=desired_path,
            project="etl-prod",
        )

    assert captured.value.details["deleted_tasks"] == ["load"]
    confirmation = str(captured.value.details["confirmation_token"])
    workflow_service.edit_workflow_result(
        "legacy-confirm",
        file=desired_path,
        project="etl-prod",
        confirm_risk=confirmation,
    )

    stored_process = json.loads(operations.legacy_definitions[workflow_id][0])
    assert [task["name"] for task in stored_process["tasks"]] == ["extract"]
    assert fake_task_adapter.generate_code_calls == []
