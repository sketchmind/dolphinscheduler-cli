from __future__ import annotations

from datetime import datetime, timedelta
from textwrap import dedent
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from tests.live.support import (
    require_mapping,
    require_ok_payload,
    result_error_code,
    wait_for_result,
)

if TYPE_CHECKING:
    from pathlib import Path


PROJECT_DELETE_DEFINITIONS_NOT_EMPTY = 10137
SHANGHAI_TIMEZONE = "Asia/Shanghai"


def _yaml_block(value: str, *, indent: str = "      ") -> str:
    normalized = value.rstrip("\n")
    lines = normalized.splitlines() or [""]
    return "\n".join(f"{indent}{line}" for line in lines)


def write_shell_workflow_spec(
    path: Path,
    *,
    project_name: str,
    workflow_name: str,
    extract_marker: str,
    load_marker: str,
) -> Path:
    """Write one two-task shell workflow YAML used by live workflow tests."""
    extract_command = _yaml_block(f'echo "{extract_marker}"')
    load_command = _yaml_block(f'echo "{load_marker}"')
    path.write_text(
        "\n".join(
            [
                "workflow:",
                f"  name: {workflow_name}",
                f"  project: {project_name}",
                "  description: live workflow/runtime round trip",
                "  timeout: 0",
                "  execution_type: PARALLEL",
                "  release_state: OFFLINE",
                "tasks:",
                "  - name: extract",
                "    type: SHELL",
                "    command: |",
                extract_command,
                "    worker_group: default",
                "    priority: MEDIUM",
                "    retry:",
                "      times: 0",
                "      interval: 0",
                "    timeout: 0",
                "    delay: 0",
                "    depends_on: []",
                "  - name: load",
                "    type: SHELL",
                "    command: |",
                load_command,
                "    worker_group: default",
                "    priority: MEDIUM",
                "    retry:",
                "      times: 0",
                "      interval: 0",
                "    timeout: 0",
                "    delay: 0",
                "    depends_on:",
                "      - extract",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return path


def write_single_shell_workflow_spec(
    path: Path,
    *,
    project_name: str,
    workflow_name: str,
    task_name: str,
    command: str,
    description: str,
    task_group_id: int | None = None,
    task_group_priority: int | None = None,
) -> Path:
    """Write one single-task shell workflow YAML for runtime-control live tests."""
    task_group_lines: list[str] = []
    if task_group_id is not None:
        task_group_lines.append(f"            task_group_id: {task_group_id}")
        if task_group_priority is not None:
            task_group_lines.append(
                f"            task_group_priority: {task_group_priority}"
            )
    command_block = _yaml_block(command)
    path.write_text(
        "\n".join(
            [
                "workflow:",
                f"  name: {workflow_name}",
                f"  project: {project_name}",
                f"  description: {description}",
                "  timeout: 0",
                "  execution_type: PARALLEL",
                "  release_state: OFFLINE",
                "tasks:",
                f"  - name: {task_name}",
                "    type: SHELL",
                "    command: |",
                command_block,
                "    worker_group: default",
                *task_group_lines,
                "    priority: MEDIUM",
                "    retry:",
                "      times: 0",
                "      interval: 0",
                "    timeout: 0",
                "    delay: 0",
                "    depends_on: []",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return path


def write_parallel_task_group_workflow_spec(
    path: Path,
    *,
    project_name: str,
    workflow_name: str,
    task_group_id: int,
    first_task_name: str = "slot-one",
    second_task_name: str = "slot-two",
    first_command: str = 'echo "slot one" && sleep 60',
    second_command: str = 'echo "slot two" && sleep 60',
    first_priority: int = 1,
    second_priority: int = 2,
    description: str = "live task-group queue round trip",
) -> Path:
    """Write one two-task parallel SHELL workflow that competes on one task group."""
    first_command_block = _yaml_block(first_command)
    second_command_block = _yaml_block(second_command)
    path.write_text(
        "\n".join(
            [
                "workflow:",
                f"  name: {workflow_name}",
                f"  project: {project_name}",
                f"  description: {description}",
                "  timeout: 0",
                "  execution_type: PARALLEL",
                "  release_state: OFFLINE",
                "tasks:",
                f"  - name: {first_task_name}",
                "    type: SHELL",
                "    command: |",
                first_command_block,
                "    worker_group: default",
                f"    task_group_id: {task_group_id}",
                f"    task_group_priority: {first_priority}",
                "    priority: MEDIUM",
                "    retry:",
                "      times: 0",
                "      interval: 0",
                "    timeout: 0",
                "    delay: 0",
                "    depends_on: []",
                f"  - name: {second_task_name}",
                "    type: SHELL",
                "    command: |",
                second_command_block,
                "    worker_group: default",
                f"    task_group_id: {task_group_id}",
                f"    task_group_priority: {second_priority}",
                "    priority: MEDIUM",
                "    retry:",
                "      times: 0",
                "      interval: 0",
                "    timeout: 0",
                "    delay: 0",
                "    depends_on: []",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return path


def write_dependent_workflow_spec(
    path: Path,
    *,
    project_name: str,
    workflow_name: str,
    upstream_project_code: int,
    upstream_workflow_code: int,
    task_name: str = "wait-upstream",
) -> Path:
    """Write one DEPENDENT-task workflow YAML anchored on one upstream workflow."""
    path.write_text(
        dedent(
            f"""\
            workflow:
              name: {workflow_name}
              project: {project_name}
              description: live workflow lineage round trip
              timeout: 0
              execution_type: PARALLEL
              release_state: OFFLINE
            tasks:
              - name: {task_name}
                type: DEPENDENT
                task_params:
                  dependence:
                    relation: AND
                    checkInterval: 10
                    failurePolicy: DEPENDENT_FAILURE_FAILURE
                    dependTaskList:
                      - relation: AND
                        dependItemList:
                          - dependentType: DEPENDENT_ON_WORKFLOW
                            projectCode: {upstream_project_code}
                            definitionCode: {upstream_workflow_code}
                            depTaskCode: 0
                            cycle: day
                            dateValue: last1Days
                worker_group: default
                priority: MEDIUM
                retry:
                  times: 0
                  interval: 0
                timeout: 0
                delay: 0
                depends_on: []
            """
        ),
        encoding="utf-8",
    )
    return path


def write_sub_workflow_parent_spec(
    path: Path,
    *,
    project_name: str,
    workflow_name: str,
    child_workflow_code: int,
    sub_workflow_task_name: str = "run-child",
    trailing_task_name: str = "after-child",
    trailing_marker: str,
) -> Path:
    """Write one parent workflow YAML that executes one child workflow task."""
    trailing_command = _yaml_block(f'echo "{trailing_marker}"')
    path.write_text(
        "\n".join(
            [
                "workflow:",
                f"  name: {workflow_name}",
                f"  project: {project_name}",
                "  description: live sub-workflow runtime round trip",
                "  timeout: 0",
                "  execution_type: PARALLEL",
                "  release_state: OFFLINE",
                "tasks:",
                f"  - name: {sub_workflow_task_name}",
                "    type: SUB_WORKFLOW",
                "    task_params:",
                f"      workflowDefinitionCode: {child_workflow_code}",
                "    worker_group: default",
                "    priority: MEDIUM",
                "    retry:",
                "      times: 0",
                "      interval: 0",
                "    timeout: 0",
                "    delay: 0",
                "    depends_on: []",
                f"  - name: {trailing_task_name}",
                "    type: SHELL",
                "    command: |",
                trailing_command,
                "    worker_group: default",
                "    priority: MEDIUM",
                "    retry:",
                "      times: 0",
                "      interval: 0",
                "    timeout: 0",
                "    delay: 0",
                "    depends_on:",
                f"      - {sub_workflow_task_name}",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return path


def write_workflow_patch(
    path: Path,
    *,
    workflow_description: str | None = None,
    renames: list[tuple[str, str]] | None = None,
) -> Path:
    """Write one workflow edit patch used by live workflow tests."""
    workflow_lines: list[str] = []
    if workflow_description is not None:
        workflow_lines.extend(
            [
                "  workflow:",
                "    set:",
                f"      description: {workflow_description}",
            ]
        )
    rename_lines: list[str] = []
    if renames:
        rename_lines.extend(["  tasks:", "    rename:"])
        for source_name, target_name in renames:
            rename_lines.extend(
                [
                    f"      - from: {source_name}",
                    f"        to: {target_name}",
                ]
            )
    if not workflow_lines and not rename_lines:
        message = "workflow patch must include at least one change"
        raise ValueError(message)
    path.write_text(
        "\n".join(["patch:", *workflow_lines, *rename_lines]) + "\n",
        encoding="utf-8",
    )
    return path


def near_future_schedule_window(
    *,
    timezone: str = SHANGHAI_TIMEZONE,
    start_offset_minutes: int = -1,
    end_offset_minutes: int = 15,
) -> tuple[str, str]:
    """Return one DS-formatted schedule window that starts near now."""
    tzinfo = ZoneInfo(timezone)
    base = datetime.now(tz=tzinfo).replace(second=0, microsecond=0)
    start = base + timedelta(minutes=start_offset_minutes)
    end = base + timedelta(minutes=end_offset_minutes)
    return (
        start.strftime("%Y-%m-%d %H:%M:%S"),
        end.strftime("%Y-%m-%d %H:%M:%S"),
    )


def delete_project_eventually(
    repo_root: Path,
    env_file: Path,
    *,
    project: str,
) -> None:
    """Delete one project after DS finishes cleaning dependent definitions."""
    result = wait_for_result(
        repo_root,
        ["project", "delete", project, "--force"],
        env_file=env_file,
        timeout_seconds=20.0,
        interval_seconds=2.0,
        accept=lambda current: (
            current.exit_code == 0
            or result_error_code(current) != PROJECT_DELETE_DEFINITIONS_NOT_EMPTY
        ),
    )
    payload = require_ok_payload(
        result,
        expected_action="project.delete",
        label="project delete",
    )
    data = require_mapping(
        payload["data"],
        label="project delete data",
    )
    assert data["deleted"] is True
