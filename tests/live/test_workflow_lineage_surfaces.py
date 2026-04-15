from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests.live.support import (
    require_int_value,
    require_list,
    require_mapping,
    require_ok_payload,
    run_dsctl,
    wait_for_result,
)
from tests.live.workflow_support import (
    delete_project_eventually,
    write_dependent_workflow_spec,
    write_shell_workflow_spec,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path


pytestmark = [pytest.mark.live, pytest.mark.destructive]


def _workflow_lineage_has_edge(
    result: object,
    *,
    source_workflow_code: int,
    target_workflow_code: int,
    action: str,
) -> bool:
    if not isinstance(result, dict):
        return False
    if result.get("ok") is not True or result.get("action") != action:
        return False
    data = result.get("data")
    if not isinstance(data, dict):
        return False
    relations = data.get("workFlowRelationList")
    if not isinstance(relations, list):
        return False
    return any(
        isinstance(item, dict)
        and item.get("sourceWorkFlowCode") == source_workflow_code
        and item.get("targetWorkFlowCode") == target_workflow_code
        for item in relations
    )


def _dependent_tasks_include_workflow(
    result: object,
    *,
    workflow_code: int,
    workflow_name: str,
    task_name: str,
) -> bool:
    if not isinstance(result, dict):
        return False
    if (
        result.get("ok") is not True
        or result.get("action") != "workflow.lineage.dependent-tasks"
    ):
        return False
    data = result.get("data")
    if not isinstance(data, list):
        return False
    return any(
        isinstance(item, dict)
        and item.get("workflowDefinitionCode") == workflow_code
        and item.get("workflowDefinitionName") == workflow_name
        and item.get("taskDefinitionName") == task_name
        for item in data
    )


def test_etl_workflow_lineage_surfaces_round_trip(
    live_repo_root: Path,
    live_etl_env_file: Path,
    live_name_factory: Callable[[str], str],
    tmp_path: Path,
) -> None:
    project_name = live_name_factory("lineage-project")
    upstream_workflow_name = live_name_factory("lineage-upstream")
    downstream_workflow_name = live_name_factory("lineage-downstream")
    dependency_task_name = "wait-upstream"
    upstream_spec = write_shell_workflow_spec(
        tmp_path / f"{upstream_workflow_name}.yaml",
        project_name=project_name,
        workflow_name=upstream_workflow_name,
        extract_marker=f"{upstream_workflow_name}-extract-marker",
        load_marker=f"{upstream_workflow_name}-load-marker",
    )

    project_created = False
    upstream_workflow_created = False
    downstream_workflow_created = False

    try:
        project_create_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "project",
                    "create",
                    "--name",
                    project_name,
                    "--description",
                    "workflow lineage live test project",
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="project.create",
            label="project create",
        )
        project_create_data = require_mapping(
            project_create_payload["data"],
            label="project create data",
        )
        project_code = require_int_value(
            project_create_data.get("code"),
            label="project code",
        )
        project_created = True

        upstream_workflow_create_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow",
                    "create",
                    "--file",
                    str(upstream_spec),
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.create",
            label="upstream workflow create",
        )
        upstream_workflow_create_data = require_mapping(
            upstream_workflow_create_payload["data"],
            label="upstream workflow create data",
        )
        upstream_workflow_code = require_int_value(
            upstream_workflow_create_data.get("code"),
            label="upstream workflow code",
        )
        assert upstream_workflow_create_data["name"] == upstream_workflow_name
        upstream_workflow_created = True

        downstream_spec = write_dependent_workflow_spec(
            tmp_path / f"{downstream_workflow_name}.yaml",
            project_name=project_name,
            workflow_name=downstream_workflow_name,
            upstream_project_code=project_code,
            upstream_workflow_code=upstream_workflow_code,
            task_name=dependency_task_name,
        )
        downstream_workflow_create_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow",
                    "create",
                    "--file",
                    str(downstream_spec),
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.create",
            label="downstream workflow create",
        )
        downstream_workflow_create_data = require_mapping(
            downstream_workflow_create_payload["data"],
            label="downstream workflow create data",
        )
        downstream_workflow_code = require_int_value(
            downstream_workflow_create_data.get("code"),
            label="downstream workflow code",
        )
        assert downstream_workflow_create_data["name"] == downstream_workflow_name
        downstream_workflow_created = True

        lineage_list_result = wait_for_result(
            live_repo_root,
            ["workflow", "lineage", "list", "--project", project_name],
            env_file=live_etl_env_file,
            timeout_seconds=20.0,
            interval_seconds=2.0,
            accept=lambda current: _workflow_lineage_has_edge(
                current.payload,
                source_workflow_code=upstream_workflow_code,
                target_workflow_code=downstream_workflow_code,
                action="workflow.lineage.list",
            ),
        )
        lineage_list_payload = require_ok_payload(
            lineage_list_result,
            expected_action="workflow.lineage.list",
            label="workflow lineage list",
        )
        lineage_list_data = require_mapping(
            lineage_list_payload["data"],
            label="workflow lineage list data",
        )
        lineage_list_relations = require_list(
            lineage_list_data["workFlowRelationList"],
            label="workflow lineage relations",
        )
        lineage_relation_pairs = {
            (
                require_mapping(item, label="workflow lineage relation").get(
                    "sourceWorkFlowCode"
                ),
                require_mapping(item, label="workflow lineage relation").get(
                    "targetWorkFlowCode"
                ),
            )
            for item in lineage_list_relations
        }
        assert (
            upstream_workflow_code,
            downstream_workflow_code,
        ) in lineage_relation_pairs, lineage_list_relations
        lineage_list_details = require_list(
            lineage_list_data["workFlowRelationDetailList"],
            label="workflow lineage details",
        )
        lineage_names = {
            require_mapping(item, label="workflow lineage detail").get("workFlowName")
            for item in lineage_list_details
        }
        assert upstream_workflow_name in lineage_names
        assert downstream_workflow_name in lineage_names

        lineage_get_result = wait_for_result(
            live_repo_root,
            [
                "workflow",
                "lineage",
                "get",
                str(upstream_workflow_code),
                "--project",
                project_name,
            ],
            env_file=live_etl_env_file,
            timeout_seconds=20.0,
            interval_seconds=2.0,
            accept=lambda current: _workflow_lineage_has_edge(
                current.payload,
                source_workflow_code=upstream_workflow_code,
                target_workflow_code=downstream_workflow_code,
                action="workflow.lineage.get",
            ),
        )
        lineage_get_payload = require_ok_payload(
            lineage_get_result,
            expected_action="workflow.lineage.get",
            label="workflow lineage get",
        )
        lineage_get_data = require_mapping(
            lineage_get_payload["data"],
            label="workflow lineage get data",
        )
        lineage_get_relations = require_list(
            lineage_get_data["workFlowRelationList"],
            label="workflow lineage get relations",
        )
        lineage_get_relation_pairs = {
            (
                require_mapping(item, label="workflow lineage get relation").get(
                    "sourceWorkFlowCode"
                ),
                require_mapping(item, label="workflow lineage get relation").get(
                    "targetWorkFlowCode"
                ),
            )
            for item in lineage_get_relations
        }
        assert (
            upstream_workflow_code,
            downstream_workflow_code,
        ) in lineage_get_relation_pairs, lineage_get_relations

        dependent_tasks_result = wait_for_result(
            live_repo_root,
            [
                "workflow",
                "lineage",
                "dependent-tasks",
                str(upstream_workflow_code),
                "--project",
                project_name,
            ],
            env_file=live_etl_env_file,
            timeout_seconds=20.0,
            interval_seconds=2.0,
            accept=lambda current: _dependent_tasks_include_workflow(
                current.payload,
                workflow_code=downstream_workflow_code,
                workflow_name=downstream_workflow_name,
                task_name=dependency_task_name,
            ),
        )
        dependent_tasks_payload = require_ok_payload(
            dependent_tasks_result,
            expected_action="workflow.lineage.dependent-tasks",
            label="workflow lineage dependent tasks",
        )
        dependent_tasks_data = require_list(
            dependent_tasks_payload["data"],
            label="workflow lineage dependent tasks data",
        )
        dependent_targets = {
            (
                require_mapping(item, label="workflow lineage dependent task").get(
                    "workflowDefinitionCode"
                ),
                require_mapping(item, label="workflow lineage dependent task").get(
                    "workflowDefinitionName"
                ),
                require_mapping(item, label="workflow lineage dependent task").get(
                    "taskDefinitionName"
                ),
            )
            for item in dependent_tasks_data
        }
        assert (
            downstream_workflow_code,
            downstream_workflow_name,
            dependency_task_name,
        ) in dependent_targets, dependent_tasks_data
    finally:
        if downstream_workflow_created:
            run_dsctl(
                live_repo_root,
                [
                    "workflow",
                    "delete",
                    downstream_workflow_name,
                    "--project",
                    project_name,
                    "--force",
                ],
                env_file=live_etl_env_file,
            )
        if upstream_workflow_created:
            run_dsctl(
                live_repo_root,
                [
                    "workflow",
                    "delete",
                    upstream_workflow_name,
                    "--project",
                    project_name,
                    "--force",
                ],
                env_file=live_etl_env_file,
            )
        if project_created:
            delete_project_eventually(
                live_repo_root,
                live_etl_env_file,
                project=project_name,
            )
