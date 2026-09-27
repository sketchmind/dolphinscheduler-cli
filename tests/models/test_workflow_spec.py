from pathlib import Path

import pytest

from dsctl.models import (
    ReleaseState,
    WorkflowExecutionType,
    load_workflow_patch,
    load_workflow_spec,
)
from dsctl.models.common import ModelValidationError, ModelValidationIssue
from dsctl.models.workflow_spec import WorkflowScheduleSpec


def test_load_workflow_spec_accepts_roundtrip_friendly_yaml(tmp_path: Path) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: nightly-sync
  project: etl-prod
  execution_type: SERIAL_WAIT
  release_state: ONLINE
  global_params:
    env: prod
tasks:
  - name: extract
    type: SHELL
    command: echo extract
  - name: load
    type: SHELL
    task_params:
      rawScript: echo load
      localParams: []
      resourceList: []
    depends_on: [extract]
""".strip(),
        encoding="utf-8",
    )

    spec = load_workflow_spec(spec_path)

    assert spec.workflow.execution_type == WorkflowExecutionType.SERIAL_WAIT
    assert spec.workflow.release_state == ReleaseState.ONLINE
    assert isinstance(spec.workflow.global_params, dict)
    assert spec.tasks[0].command == "echo extract"
    assert spec.tasks[1].task_params == {
        "rawScript": "echo load",
        "localParams": [],
        "resourceList": [],
    }


def test_load_workflow_spec_locates_unquoted_date_value(tmp_path: Path) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: dated-workflow
  global_params:
    business_date: 2026-09-20
tasks:
  - name: echo
    type: SHELL
    command: echo dated
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ModelValidationError) as captured:
        load_workflow_spec(spec_path)

    assert captured.value.issues == (
        ModelValidationIssue(
            code="yaml_date_time_not_string",
            path="workflow.global_params.business_date",
            message=("YAML date/time values must be quoted when a string is intended."),
        ),
    )

    spec_path.write_text(
        spec_path.read_text(encoding="utf-8").replace(
            "business_date: 2026-09-20",
            'business_date: "2026-09-20"',
        ),
        encoding="utf-8",
    )
    loaded = load_workflow_spec(spec_path)
    assert loaded.workflow.global_params == {"business_date": "2026-09-20"}


def test_load_workflow_spec_preserves_raw_script_trailing_newline(
    tmp_path: Path,
) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: nightly-sync
tasks:
  - name: extract
    type: SHELL
    task_params:
      rawScript: |
        echo extract
      localParams: []
      resourceList: []
""".strip(),
        encoding="utf-8",
    )

    spec = load_workflow_spec(spec_path)

    assert spec.tasks[0].task_params == {
        "rawScript": "echo extract\n",
        "localParams": [],
        "resourceList": [],
    }


def test_load_workflow_spec_rejects_system_managed_task_identity_fields(
    tmp_path: Path,
) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: nightly-sync
tasks:
  - name: extract
    type: SHELL
    command: echo extract
    version: 2
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="system-managed"):
        load_workflow_spec(spec_path)


@pytest.mark.parametrize(
    ("tasks", "message"),
    [
        (
            """
  - name: repeated
    type: SHELL
    command: echo first
  - name: repeated
    type: SHELL
    command: echo second
""",
            "Task 'repeated' is duplicated in workflow YAML",
        ),
        (
            """
  - name: recursive
    type: SHELL
    command: echo recursive
    depends_on: [recursive]
""",
            "Task 'recursive' cannot depend on itself",
        ),
    ],
)
def test_load_workflow_spec_keeps_graph_validation_outside_lint(
    tmp_path: Path,
    tasks: str,
    message: str,
) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        f"workflow:\n  name: invalid-graph\ntasks:\n{tasks}",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=message):
        load_workflow_spec(spec_path)


def test_load_workflow_patch_rejects_system_managed_task_identity_fields(
    tmp_path: Path,
) -> None:
    patch_path = tmp_path / "workflow.patch.yaml"
    patch_path.write_text(
        """
patch:
  tasks:
    update:
      - match:
          name: extract
        set:
          version: 2
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="system-managed"):
        load_workflow_patch(patch_path)


def test_load_workflow_patch_preserves_command_trailing_newline(
    tmp_path: Path,
) -> None:
    patch_path = tmp_path / "workflow.patch.yaml"
    patch_path.write_text(
        """
patch:
  tasks:
    update:
      - match:
          name: extract
        set:
          command: |
            echo updated
""".lstrip(),
        encoding="utf-8",
    )

    patch = load_workflow_patch(patch_path)

    assert patch.tasks is not None
    assert patch.tasks.update[0].set.command == "echo updated\n"


def test_load_workflow_patch_locates_unquoted_date_value(tmp_path: Path) -> None:
    patch_path = tmp_path / "workflow.patch.yaml"
    patch_path.write_text(
        """
patch:
  workflow:
    set:
      global_params:
        business_date: 2026-09-20
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ModelValidationError) as captured:
        load_workflow_patch(patch_path)

    assert captured.value.issues[0].code == "yaml_date_time_not_string"
    assert captured.value.issues[0].path == (
        "patch.workflow.set.global_params.business_date"
    )


def test_load_workflow_spec_rejects_conflicting_schedule_enabled_alias(
    tmp_path: Path,
) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: nightly-sync
tasks:
  - name: extract
    type: SHELL
    command: echo extract
schedule:
  cron: "0 0 0 * * ?"
  timezone: UTC
  start: "2026-01-01 00:00:00"
  end: "2026-12-31 23:59:59"
  enabled: true
  release_state: OFFLINE
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"schedule\.enabled conflicts"):
        load_workflow_spec(spec_path)


def test_load_workflow_spec_rejects_five_field_schedule_cron(tmp_path: Path) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: nightly-sync
tasks:
  - name: extract
    type: SHELL
    command: echo extract
schedule:
  cron: "0 2 * * *"
  timezone: UTC
  start: "2026-01-01 00:00:00"
  end: "2026-12-31 23:59:59"
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Quartz cron expression with 6 or 7 fields"):
        load_workflow_spec(spec_path)


def test_load_workflow_spec_accepts_task_group_fields(tmp_path: Path) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: nightly-sync
tasks:
  - name: extract
    type: SHELL
    command: echo extract
    task_group_id: 9
    task_group_priority: 2
""".strip(),
        encoding="utf-8",
    )

    spec = load_workflow_spec(spec_path)

    assert spec.tasks[0].task_group_id == 9
    assert spec.tasks[0].task_group_priority == 2


def test_load_workflow_spec_accepts_extended_task_execution_fields(
    tmp_path: Path,
) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: nightly-sync
tasks:
  - name: extract
    type: SHELL
    command: echo extract
    flag: NO
    environment_code: 42
    timeout: 15
    timeout_notify_strategy: FAILED
    cpu_quota: 50
    memory_max: 1024
""".strip(),
        encoding="utf-8",
    )

    spec = load_workflow_spec(spec_path)

    assert spec.tasks[0].flag.value == "NO"
    assert spec.tasks[0].environment_code == 42
    assert spec.tasks[0].timeout == 15
    assert spec.tasks[0].timeout_notify_strategy is not None
    assert spec.tasks[0].timeout_notify_strategy.value == "FAILED"
    assert spec.tasks[0].cpu_quota == 50
    assert spec.tasks[0].memory_max == 1024


def test_load_workflow_spec_accepts_reviewed_seatunnel_literal_config(
    tmp_path: Path,
) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: nightly-sync
tasks:
  - name: seatunnel-job
    type: SEATUNNEL
    task_params:
      rawScript: "env { execution.parallelism = 1 }"
""".strip(),
        encoding="utf-8",
    )

    spec = load_workflow_spec(spec_path)

    assert spec.tasks[0].type == "SEATUNNEL"
    assert spec.tasks[0].task_params == {
        "rawScript": "env { execution.parallelism = 1 }"
    }


def test_load_workflow_spec_rejects_task_group_priority_without_group_id(
    tmp_path: Path,
) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: nightly-sync
tasks:
  - name: extract
    type: SHELL
    command: echo extract
    task_group_priority: 2
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="requires task_group_id"):
        load_workflow_spec(spec_path)


def test_load_workflow_spec_rejects_timeout_notify_strategy_without_timeout(
    tmp_path: Path,
) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: nightly-sync
tasks:
  - name: extract
    type: SHELL
    command: echo extract
    timeout_notify_strategy: FAILED
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="requires timeout > 0"):
        load_workflow_spec(spec_path)


@pytest.mark.parametrize("value", [None, "", " "])
def test_workflow_schedule_rejects_explicit_null_or_empty_policy(
    value: str | None,
) -> None:
    with pytest.raises(ValueError, match="missed_fire_policy"):
        WorkflowScheduleSpec.model_validate(
            {
                "cron": "0 0 2 * * ?",
                "timezone": "Asia/Shanghai",
                "start": "2026-09-15 00:00:00",
                "end": "2027-09-15 00:00:00",
                "missed_fire_policy": value,
            }
        )


def test_workflow_schedule_normalization_keeps_policy_omitted() -> None:
    document = {
        "cron": "0 0 2 * * ?",
        "timezone": "Asia/Shanghai",
        "start": "2026-09-15 00:00:00",
        "end": "2027-09-15 00:00:00",
    }
    spec = WorkflowScheduleSpec.model_validate(document)
    normalized = spec.model_dump(mode="python", exclude_none=False)
    assert "missed_fire_policy" not in normalized
    assert WorkflowScheduleSpec.model_validate(normalized).missed_fire_policy is None
