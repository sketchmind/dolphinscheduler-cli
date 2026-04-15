from pathlib import Path

import pytest

from dsctl.models import (
    ReleaseState,
    WorkflowExecutionType,
    load_workflow_patch,
    load_workflow_spec,
)


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


def test_load_workflow_spec_accepts_generic_task_type_with_raw_task_params(
    tmp_path: Path,
) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: nightly-sync
tasks:
  - name: spark-job
    type: SPARK
    task_params: {}
""".strip(),
        encoding="utf-8",
    )

    spec = load_workflow_spec(spec_path)

    assert spec.tasks[0].type == "SPARK"
    assert spec.tasks[0].task_params == {}


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
