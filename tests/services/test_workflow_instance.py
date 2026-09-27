import json
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import pytest
import yaml
from tests.fakes import (
    FakeDag,
    FakeEnumValue,
    FakeProject,
    FakeProjectAdapter,
    FakeResourceAdapter,
    FakeResourceItem,
    FakeTaskAdapter,
    FakeTaskDefinition,
    FakeTaskInstance,
    FakeTaskInstanceAdapter,
    FakeWorkflow,
    FakeWorkflowAdapter,
    FakeWorkflowInstance,
    FakeWorkflowInstanceAdapter,
    FakeWorkflowTaskRelation,
)
from tests.request_assertions import first_dry_run_request
from tests.runtime_instance_domain_fakes import install_runtime_instance_domain_runtime
from tests.support import make_profile
from tests.value_shape_assertions import assert_mapping as _mapping
from tests.value_shape_assertions import assert_sequence as _sequence

from dsctl.config import ClusterProfile
from dsctl.errors import (
    ApiResultError,
    ApiTransportError,
    ConfirmationRequiredError,
    InvalidStateError,
    MutationOutcomeUnknownError,
    NotFoundError,
    PermissionDeniedError,
    UserInputError,
    WaitTimeoutError,
)
from dsctl.services import workflow_instance as workflow_instance_service
from dsctl.services._workflow import authoring as workflow_authoring_service
from dsctl.services.selection import ResourceDefaults
from dsctl.services.task_authoring_catalog import get_task_authoring_catalog
from dsctl.services.workflow_instance import _types as workflow_instance_types

if TYPE_CHECKING:
    from dsctl.upstream.task_definition_wire import TaskDefinitionWire

_PROJECT_CONTEXT = ResourceDefaults(project="etl-prod")


def _install_workflow_instance_service_fakes(
    monkeypatch: pytest.MonkeyPatch,
    *,
    project_adapter: FakeProjectAdapter,
    workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    workflow_adapter: FakeWorkflowAdapter | None = None,
    task_adapter: FakeTaskAdapter | None = None,
    task_instance_adapter: FakeTaskInstanceAdapter | None = None,
    context: ResourceDefaults = _PROJECT_CONTEXT,
    profile: ClusterProfile | None = None,
    resource_adapter: FakeResourceAdapter | None = None,
    task_definition_wire: object | None = None,
) -> None:
    install_runtime_instance_domain_runtime(
        monkeypatch,
        project_adapter=project_adapter,
        profile=make_profile() if profile is None else profile,
        workflow_adapter=workflow_adapter,
        workflow_instance_adapter=workflow_instance_adapter,
        task_adapter=task_adapter,
        task_instance_adapter=task_instance_adapter,
        context=context,
        resource_adapter=resource_adapter,
        task_definition_wire=cast("TaskDefinitionWire | None", task_definition_wire),
    )


@pytest.fixture
def fake_project_adapter() -> FakeProjectAdapter:
    return FakeProjectAdapter(projects=[FakeProject(code=7, name="etl-prod")])


@pytest.fixture
def fake_workflow_instance_adapter() -> FakeWorkflowInstanceAdapter:
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
    workflow_dag = FakeDag(
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
    return FakeWorkflowInstanceAdapter(
        workflow_instances=[
            FakeWorkflowInstance(
                id=901,
                workflow_definition_code_value=101,
                workflow_definition_version_value=1,
                project_code_value=7,
                dag_data_value=workflow_dag,
                state_value=FakeEnumValue("RUNNING_EXECUTION"),
                run_times_value=1,
                name="daily-sync-901",
                host="master-1",
                command_type_value=FakeEnumValue("START_PROCESS"),
                executor_id_value=11,
                executor_name_value="alice",
                workflow_instance_priority_value=FakeEnumValue("MEDIUM"),
                worker_group_value="default",
            ),
            FakeWorkflowInstance(
                id=902,
                workflow_definition_code_value=101,
                workflow_definition_version_value=1,
                project_code_value=7,
                dag_data_value=workflow_dag,
                state_value=FakeEnumValue("SUCCESS"),
                run_times_value=1,
                name="daily-sync-902",
                host="master-1",
                command_type_value=FakeEnumValue("START_PROCESS"),
                executor_id_value=11,
                executor_name_value="alice",
                workflow_instance_priority_value=FakeEnumValue("MEDIUM"),
                worker_group_value="default",
            ),
            FakeWorkflowInstance(
                id=903,
                workflow_definition_code_value=201,
                workflow_definition_version_value=1,
                project_code_value=7,
                state_value=FakeEnumValue("SUCCESS"),
                run_times_value=1,
                name="child-workflow-903",
                host="master-2",
                command_type_value=FakeEnumValue("COMPLEMENT_DATA"),
                executor_id_value=12,
                executor_name_value="bob",
                workflow_instance_priority_value=FakeEnumValue("MEDIUM"),
                worker_group_value="default",
            ),
        ],
        parent_workflow_instance_ids_by_sub_id={903: 902},
    )


def test_list_workflow_instances_result_returns_ds_page(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )

    result = workflow_instance_service.list_workflow_instances_result(
        state="running_execution"
    )
    data = _mapping(result.data)
    items = _sequence(data["totalList"])

    assert data["total"] == 1
    assert _mapping(items[0])["id"] == 901
    assert result.resolved["state"] == "RUNNING_EXECUTION"


def test_list_workflow_instances_result_supports_project_scoped_filters(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    timed_adapter = FakeWorkflowInstanceAdapter(
        workflow_instances=[
            replace(
                fake_workflow_instance_adapter.workflow_instances[0],
                start_time_value="2026-04-11 10:00:00",
            ),
            replace(
                fake_workflow_instance_adapter.workflow_instances[1],
                start_time_value="2026-04-11 11:00:00",
            ),
            replace(
                fake_workflow_instance_adapter.workflow_instances[2],
                start_time_value="2026-04-11 12:00:00",
            ),
        ],
    )
    workflow_adapter = FakeWorkflowAdapter(
        workflows=[
            FakeWorkflow(
                code=101,
                name="daily-sync",
                version=1,
                project_code_value=7,
            )
        ],
        dags={},
    )
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=workflow_adapter,
        workflow_instance_adapter=timed_adapter,
    )

    result = workflow_instance_service.list_workflow_instances_result(
        project="etl-prod",
        workflow="daily-sync",
        search="daily-sync",
        executor="alice",
        host="master-1",
        start="2026-04-11 10:30:00",
        end="2026-04-11 11:30:00",
    )
    data = _mapping(result.data)
    items = _sequence(data["totalList"])

    assert data["total"] == 1
    assert _mapping(items[0])["id"] == 902
    assert result.resolved["project"] == "etl-prod"
    assert result.resolved["project_code"] == 7
    assert result.resolved["workflow"] == "daily-sync"
    assert result.resolved["workflow_code"] == 101
    assert result.resolved["start"] == "2026-04-11 10:30:00"
    assert result.resolved["end"] == "2026-04-11 11:30:00"


def test_list_workflow_instances_result_requires_project_selection_before_domain_io(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    list_calls = 0

    def unexpected_list(**kwargs: object) -> object:
        nonlocal list_calls
        del kwargs
        list_calls += 1
        message = "workflow-instance page I/O must not run"
        raise AssertionError(message)

    monkeypatch.setattr(fake_workflow_instance_adapter, "list", unexpected_list)
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        context=ResourceDefaults(),
    )

    with pytest.raises(UserInputError, match="Project is required") as exc_info:
        workflow_instance_service.list_workflow_instances_result(search="daily")

    assert exc_info.value.suggestion == (
        "Pass --project NAME, or configure a project in the selected context."
    )
    assert list_calls == 0


def test_get_workflow_instance_result_requires_project_before_detail_io(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    detail_calls = 0

    def unexpected_get(*, workflow_instance_id: int) -> object:
        nonlocal detail_calls
        del workflow_instance_id
        detail_calls += 1
        message = "workflow-instance detail I/O must not run"
        raise AssertionError(message)

    monkeypatch.setattr(fake_workflow_instance_adapter, "get", unexpected_get)
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        context=ResourceDefaults(),
    )

    with pytest.raises(UserInputError, match="Project is required"):
        workflow_instance_service.get_workflow_instance_result(901)

    assert detail_calls == 0


def test_list_workflow_instances_result_translates_controller_fallback_error(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )

    def broken_list(**kwargs: object) -> object:
        del kwargs
        raise ApiResultError(
            result_code=10113,
            result_message="query workflow instance list paging error:null",
        )

    monkeypatch.setattr(fake_workflow_instance_adapter, "list", broken_list)

    with pytest.raises(UserInputError, match="rejected") as exc_info:
        workflow_instance_service.list_workflow_instances_result(
            project="etl-prod",
            search="daily-sync",
        )

    assert exc_info.value.details == {
        "filters": {"project": "etl-prod", "search": "daily-sync"}
    }
    assert exc_info.value.__cause__ is not None


def test_list_workflow_instances_result_reports_supported_state_names(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )

    with pytest.raises(
        UserInputError,
        match="Workflow instance state must be one of the DS execution status names",
    ) as exc_info:
        workflow_instance_service.list_workflow_instances_result(state="running")
    assert exc_info.value.suggestion == (
        "Run `dsctl enum list workflow-execution-status` to inspect the "
        "supported state names."
    )


def test_list_workflow_instances_result_can_auto_exhaust_pages(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )

    result = workflow_instance_service.list_workflow_instances_result(
        page_size=1,
        all_pages=True,
    )
    data = _mapping(result.data)
    items = _sequence(data["totalList"])

    assert result.resolved["all"] is True
    assert data["total"] == 3
    assert len(items) == 3


def test_get_workflow_instance_result_returns_one_payload(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )

    result = workflow_instance_service.get_workflow_instance_result(901)
    data = _mapping(result.data)

    assert result.resolved["workflowInstance"] == {"id": 901}
    assert _mapping(result.resolved["project"])["source"] == "context"
    assert data["state"] == "RUNNING_EXECUTION"
    assert data["projectCode"] == 7


def test_export_workflow_instance_yaml_result_exports_yaml_for_editing(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )

    result = workflow_instance_service.export_workflow_instance_yaml_result(902)
    data = _mapping(result.data)

    assert result.resolved["workflowInstance"] == {"id": 902}
    assert _mapping(result.resolved["project"])["name"] == "etl-prod"
    assert "workflow:" in str(data["yaml"])
    assert "project: etl-prod" in str(data["yaml"])
    assert "name: extract" in str(data["yaml"])


def _mr_workflow_instance_adapter() -> FakeWorkflowInstanceAdapter:
    workflow = FakeWorkflow(
        code=501,
        name="mr-workflow",
        version=1,
        project_code_value=7,
        project_name_value="etl-prod",
    )
    dag = FakeDag(
        workflow_definition_value=workflow,
        task_definition_list_value=[
            FakeTaskDefinition(
                code=601,
                name="wordcount",
                version=1,
                project_code_value=7,
                task_type_value="MR",
                task_params_value=json.dumps(
                    {
                        "localParams": [],
                        "mainJar": {"id": 731},
                        "mainClass": "com.example.WordCount",
                        "mainArgs": "hdfs:///input hdfs:///output",
                        "others": "",
                        "appName": "",
                        "resourceList": [],
                        "programType": "JAVA",
                    },
                    separators=(",", ":"),
                ),
                worker_group_value="default",
                project_name_value="etl-prod",
            )
        ],
        workflow_task_relation_list_value=[
            FakeWorkflowTaskRelation(
                pre_task_code_value=0,
                post_task_code_value=601,
            )
        ],
    )
    return FakeWorkflowInstanceAdapter(
        workflow_instances=[
            FakeWorkflowInstance(
                id=990,
                workflow_definition_code_value=501,
                workflow_definition_version_value=1,
                project_code_value=7,
                dag_data_value=dag,
                state_value=FakeEnumValue("SUCCESS"),
                name="mr-workflow-990",
                workflow_instance_priority_value=FakeEnumValue("MEDIUM"),
                worker_group_value="default",
            )
        ]
    )


def test_old_mr_instance_export_and_edit_bind_the_visible_positive_resource_id(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
) -> None:
    instance_adapter = _mr_workflow_instance_adapter()
    resource_adapter = FakeResourceAdapter(
        resources=[
            FakeResourceItem(
                alias="wordcount.jar",
                full_name_value="/jobs/wordcount.jar",
                is_directory_value=False,
                id_value=731,
            )
        ]
    )
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=instance_adapter,
        profile=make_profile(ds_version="3.1.9"),
        resource_adapter=resource_adapter,
    )
    monkeypatch.setattr(
        workflow_authoring_service,
        "load_selected_task_authoring_catalog",
        lambda _env_file: get_task_authoring_catalog("3.1.9"),
    )

    exported = workflow_instance_service.export_workflow_instance_yaml_result(990)
    document = yaml.safe_load(str(_mapping(exported.data)["yaml"]))
    assert document["tasks"][0]["task_params"] == {
        "mainJar": "/jobs/wordcount.jar",
        "mainClass": "com.example.WordCount",
        "mainArgs": ["hdfs:///input", "hdfs:///output"],
    }

    patch_file = tmp_path / "mr-instance.patch.yaml"
    patch_file.write_text(
        "patch:\n  workflow:\n    set:\n      timeout: 45\n",
        encoding="utf-8",
    )
    result = workflow_instance_service.edit_workflow_instance_result(
        990,
        patch=patch_file,
        dry_run=True,
    )
    form = _mapping(_mapping(first_dry_run_request(_mapping(result.data)))["form"])
    definitions = json.loads(str(form["taskDefinitionJson"]))
    assert json.loads(definitions[0]["taskParams"])["mainJar"] == {"id": 731}


def test_322_dynamic_instance_export_and_edit_bind_same_project_child_name(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
) -> None:
    parent = FakeWorkflow(
        code=501,
        name="dynamic-parent",
        version=1,
        project_code_value=7,
        project_name_value="etl-prod",
    )
    child = FakeWorkflow(
        code=9001,
        name="102",
        version=1,
        project_code_value=7,
        project_name_value="etl-prod",
    )
    parent_dag = FakeDag(
        workflow_definition_value=parent,
        task_definition_list_value=[
            FakeTaskDefinition(
                code=601,
                name="fanout-region",
                version=1,
                project_code_value=7,
                task_type_value="DYNAMIC",
                task_params_value=json.dumps(
                    {
                        "processDefinitionCode": 9001,
                        "maxNumOfSubWorkflowInstances": 2,
                        "degreeOfParallelism": 2,
                        "filterCondition": "",
                        "listParameters": [
                            {
                                "name": "region",
                                "value": "east,west",
                                "separator": ",",
                            }
                        ],
                    },
                    separators=(",", ":"),
                ),
                worker_group_value="default",
                project_name_value="etl-prod",
                is_cache_value=FakeEnumValue("NO"),
            )
        ],
        workflow_task_relation_list_value=[
            FakeWorkflowTaskRelation(pre_task_code_value=0, post_task_code_value=601)
        ],
    )
    instance_adapter = FakeWorkflowInstanceAdapter(
        workflow_instances=[
            FakeWorkflowInstance(
                id=991,
                workflow_definition_code_value=501,
                workflow_definition_version_value=1,
                project_code_value=7,
                dag_data_value=parent_dag,
                state_value=FakeEnumValue("SUCCESS"),
                name="dynamic-parent-991",
                workflow_instance_priority_value=FakeEnumValue("MEDIUM"),
                worker_group_value="default",
            )
        ]
    )
    workflow_adapter = FakeWorkflowAdapter(
        workflows=[parent, child],
        dags={
            501: parent_dag,
            9001: FakeDag(
                workflow_definition_value=child,
                task_definition_list_value=[],
                workflow_task_relation_list_value=[],
            ),
        },
    )
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=instance_adapter,
        workflow_adapter=workflow_adapter,
        profile=make_profile(ds_version="3.2.2"),
    )
    monkeypatch.setattr(
        workflow_authoring_service,
        "load_selected_task_authoring_catalog",
        lambda _env_file: get_task_authoring_catalog("3.2.2"),
    )

    exported = workflow_instance_service.export_workflow_instance_yaml_result(991)
    document = yaml.safe_load(str(_mapping(exported.data)["yaml"]))
    assert document["tasks"][0]["task_params"]["childWorkflowName"] == "102"
    assert "processDefinitionCode" not in document["tasks"][0]["task_params"]

    patch_file = tmp_path / "dynamic-instance.patch.yaml"
    patch_file.write_text(
        "patch:\n  workflow:\n    set:\n      timeout: 45\n",
        encoding="utf-8",
    )
    result = workflow_instance_service.edit_workflow_instance_result(
        991,
        patch=patch_file,
        dry_run=True,
    )
    form = _mapping(_mapping(first_dry_run_request(_mapping(result.data)))["form"])
    definitions = json.loads(str(form["taskDefinitionJson"]))
    assert json.loads(definitions[0]["taskParams"])["processDefinitionCode"] == 9001


def _waterdrop_workflow_instance_adapter() -> FakeWorkflowInstanceAdapter:
    workflow = FakeWorkflow(
        code=502,
        name="waterdrop-workflow",
        version=1,
        project_code_value=7,
        project_name_value="etl-prod",
    )
    dag = FakeDag(
        workflow_definition_value=workflow,
        task_definition_list_value=[
            FakeTaskDefinition(
                code=602,
                name="sync-orders",
                version=1,
                project_code_value=7,
                task_type_value="WATERDROP",
                task_params_value=json.dumps(
                    {
                        "localParams": [],
                        "resourceList": [{"id": 811}],
                        "rawScript": (
                            'sh "$WATERDROP_HOME/bin/start-waterdrop.sh" '
                            "--master local --deploy-mode client --queue default "
                            "--config waterdrop/orders.conf\n"
                        ),
                    },
                    separators=(",", ":"),
                ),
                worker_group_value="default",
                project_name_value="etl-prod",
            )
        ],
        workflow_task_relation_list_value=[
            FakeWorkflowTaskRelation(
                pre_task_code_value=0,
                post_task_code_value=602,
            )
        ],
    )
    return FakeWorkflowInstanceAdapter(
        workflow_instances=[
            FakeWorkflowInstance(
                id=991,
                workflow_definition_code_value=502,
                workflow_definition_version_value=1,
                project_code_value=7,
                dag_data_value=dag,
                state_value=FakeEnumValue("SUCCESS"),
                name="waterdrop-workflow-991",
                workflow_instance_priority_value=FakeEnumValue("MEDIUM"),
                worker_group_value="default",
            )
        ]
    )


def test_209_waterdrop_instance_export_and_edit_bind_one_config_resource(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
) -> None:
    instance_adapter = _waterdrop_workflow_instance_adapter()
    resource_adapter = FakeResourceAdapter(
        resources=[
            FakeResourceItem(
                alias="orders.conf",
                full_name_value="/waterdrop/orders.conf",
                is_directory_value=False,
                id_value=811,
            )
        ]
    )
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=instance_adapter,
        profile=make_profile(ds_version="2.0.9"),
        resource_adapter=resource_adapter,
    )
    monkeypatch.setattr(
        workflow_authoring_service,
        "load_selected_task_authoring_catalog",
        lambda _env_file: get_task_authoring_catalog("2.0.9"),
    )

    exported = workflow_instance_service.export_workflow_instance_yaml_result(991)
    document = yaml.safe_load(str(_mapping(exported.data)["yaml"]))
    assert document["tasks"][0]["task_params"] == {
        "configResource": "/waterdrop/orders.conf"
    }

    patch_file = tmp_path / "waterdrop-instance.patch.yaml"
    patch_file.write_text(
        "patch:\n  workflow:\n    set:\n      timeout: 45\n",
        encoding="utf-8",
    )
    result = workflow_instance_service.edit_workflow_instance_result(
        991,
        patch=patch_file,
        dry_run=True,
    )
    form = _mapping(_mapping(first_dry_run_request(_mapping(result.data)))["form"])
    definitions = json.loads(str(form["taskDefinitionJson"]))
    assert json.loads(definitions[0]["taskParams"]) == {
        "localParams": [],
        "resourceList": [{"id": 811}],
        "rawScript": (
            'sh "$WATERDROP_HOME/bin/start-waterdrop.sh" --master local '
            "--deploy-mode client --queue default "
            "--config waterdrop/orders.conf\n"
        ),
    }


_SEATUNNEL_INSTANCE_CONFIG = """env {
  execution.parallelism = 1
}
source { FakeSource {} }
sink { Console {} }
"""


def _seatunnel_workflow_instance_adapter(
    *,
    richer: bool = False,
    global_params: bool = False,
) -> FakeWorkflowInstanceAdapter:
    workflow = FakeWorkflow(
        code=503,
        name="seatunnel-workflow",
        version=1,
        project_code_value=7,
        project_name_value="etl-prod",
        global_params_value=(
            '[{"prop":"unsafe","value":"value"}]' if global_params else None
        ),
        global_param_map_value={"unsafe": "value"} if global_params else None,
    )
    task_params: dict[str, object] = {
        "localParams": [],
        "startupScript": "seatunnel.sh",
        "useCustom": True,
        "rawScript": _SEATUNNEL_INSTANCE_CONFIG,
        "resourceList": [],
        "deployMode": "local",
        "others": "",
    }
    if richer:
        task_params["futureField"] = {"preserve": True}
    dag = FakeDag(
        workflow_definition_value=workflow,
        task_definition_list_value=[
            FakeTaskDefinition(
                code=603,
                name="run-seatunnel",
                version=1,
                project_code_value=7,
                task_type_value="SEATUNNEL",
                task_params_value=json.dumps(task_params, separators=(",", ":")),
                worker_group_value="default",
                project_name_value="etl-prod",
                is_cache_value=FakeEnumValue("NO"),
            )
        ],
        workflow_task_relation_list_value=[
            FakeWorkflowTaskRelation(pre_task_code_value=0, post_task_code_value=603)
        ],
    )
    return FakeWorkflowInstanceAdapter(
        workflow_instances=[
            FakeWorkflowInstance(
                id=992,
                workflow_definition_code_value=503,
                workflow_definition_version_value=1,
                project_code_value=7,
                dag_data_value=dag,
                state_value=FakeEnumValue("SUCCESS"),
                name="seatunnel-workflow-992",
                workflow_instance_priority_value=FakeEnumValue("MEDIUM"),
                worker_group_value="default",
            )
        ]
    )


def _install_342_seatunnel_instance_fakes(
    monkeypatch: pytest.MonkeyPatch,
    *,
    project_adapter: FakeProjectAdapter,
    richer: bool = False,
    global_params: bool = False,
) -> None:
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=project_adapter,
        workflow_instance_adapter=_seatunnel_workflow_instance_adapter(
            richer=richer,
            global_params=global_params,
        ),
        profile=make_profile(ds_version="3.4.2"),
    )
    monkeypatch.setattr(
        workflow_authoring_service,
        "load_selected_task_authoring_catalog",
        lambda _env_file: get_task_authoring_catalog("3.4.2"),
    )


def _seatunnel_instance_metadata_patch(tmp_path: Path) -> Path:
    patch_file = tmp_path / "seatunnel-instance.patch.yaml"
    patch_file.write_text(
        """
patch:
  tasks:
    update:
      - match:
          name: run-seatunnel
        set:
          description: metadata only
""".strip(),
        encoding="utf-8",
    )
    return patch_file


def test_342_seatunnel_instance_export_and_edit_round_trip_typed_wire(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
) -> None:
    _install_342_seatunnel_instance_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
    )

    exported = workflow_instance_service.export_workflow_instance_yaml_result(992)
    document = yaml.safe_load(str(_mapping(exported.data)["yaml"]))
    assert document["tasks"][0]["task_params"] == {
        "rawScript": _SEATUNNEL_INSTANCE_CONFIG
    }

    result = workflow_instance_service.edit_workflow_instance_result(
        992,
        patch=_seatunnel_instance_metadata_patch(tmp_path),
        dry_run=True,
    )
    form = _mapping(_mapping(first_dry_run_request(_mapping(result.data)))["form"])
    definitions = json.loads(str(form["taskDefinitionJson"]))
    assert json.loads(definitions[0]["taskParams"]) == {
        "localParams": [],
        "startupScript": "seatunnel.sh",
        "useCustom": True,
        "rawScript": _SEATUNNEL_INSTANCE_CONFIG,
        "resourceList": [],
        "deployMode": "local",
        "others": "",
    }


def test_342_seatunnel_instance_export_and_metadata_edit_preserve_richer_wire(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
) -> None:
    _install_342_seatunnel_instance_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        richer=True,
        global_params=True,
    )
    expected = {
        "localParams": [],
        "startupScript": "seatunnel.sh",
        "useCustom": True,
        "rawScript": _SEATUNNEL_INSTANCE_CONFIG,
        "resourceList": [],
        "deployMode": "local",
        "others": "",
        "futureField": {"preserve": True},
    }

    exported = workflow_instance_service.export_workflow_instance_yaml_result(992)
    document = yaml.safe_load(str(_mapping(exported.data)["yaml"]))
    assert document["tasks"][0]["task_params"] == expected

    result = workflow_instance_service.edit_workflow_instance_result(
        992,
        patch=_seatunnel_instance_metadata_patch(tmp_path),
        dry_run=True,
    )
    form = _mapping(_mapping(first_dry_run_request(_mapping(result.data)))["form"])
    definitions = json.loads(str(form["taskDefinitionJson"]))
    assert json.loads(definitions[0]["taskParams"]) == expected


def test_old_mr_instance_new_missing_resource_fails_before_remote_update(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
) -> None:
    instance_adapter = _mr_workflow_instance_adapter()
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=instance_adapter,
        profile=make_profile(ds_version="3.1.9"),
        resource_adapter=FakeResourceAdapter(resources=[]),
    )
    monkeypatch.setattr(
        workflow_authoring_service,
        "load_selected_task_authoring_catalog",
        lambda _env_file: get_task_authoring_catalog("3.1.9"),
    )
    patch_file = tmp_path / "mr-instance-missing.patch.yaml"
    patch_file.write_text(
        """
patch:
  tasks:
    update:
      - match:
          name: wordcount
        set:
          task_params:
            mainJar: /jobs/missing.jar
            mainClass: com.example.WordCount
            mainArgs: []
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(NotFoundError, match="not found or visible"):
        workflow_instance_service.edit_workflow_instance_result(
            990,
            patch=patch_file,
        )

    assert instance_adapter.update_calls == []


def test_get_parent_workflow_instance_result_returns_parent_relation(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )

    result = workflow_instance_service.get_parent_workflow_instance_result(903)

    assert result.data == {"parentWorkflowInstance": 902}
    assert result.resolved["subWorkflowInstance"] == {"id": 903}
    assert _mapping(result.resolved["project"])["code"] == 7


def test_get_parent_workflow_instance_result_rejects_non_sub_workflow_instance(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )

    def not_sub_workflow(
        *,
        project_code: int,
        sub_workflow_instance_id: int,
    ) -> None:
        del project_code, sub_workflow_instance_id
        raise ApiResultError(
            result_code=50010,
            result_message="workflow instance is not sub workflow instance",
        )

    monkeypatch.setattr(
        fake_workflow_instance_adapter,
        "parent_instance_by_sub_workflow",
        not_sub_workflow,
    )

    with pytest.raises(InvalidStateError, match="sub-workflow instance") as exc_info:
        workflow_instance_service.get_parent_workflow_instance_result(901)
    assert exc_info.value.suggestion == (
        "Use `dsctl workflow-instance get 901 --project etl-prod` for regular "
        "workflow instances; "
        "`parent` only applies to sub-workflow instances."
    )


def test_digest_workflow_instance_result_returns_runtime_summary(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_instance_adapter=FakeTaskInstanceAdapter(
            task_instances=[
                FakeTaskInstance(
                    id=3001,
                    name="extract",
                    task_type_value="SHELL",
                    workflow_instance_id_value=901,
                    workflow_instance_name_value="daily-sync-901",
                    project_code_value=7,
                    task_code_value=201,
                    state_value=FakeEnumValue("RUNNING_EXECUTION"),
                    retry_times_value=0,
                    host="worker-1",
                    log_path_value="/logs/3001.log",
                    start_time_value="2026-04-11 10:00:00",
                    duration_value="15s",
                ),
                FakeTaskInstance(
                    id=3002,
                    name="load",
                    task_type_value="SQL",
                    workflow_instance_id_value=901,
                    workflow_instance_name_value="daily-sync-901",
                    project_code_value=7,
                    task_code_value=202,
                    state_value=FakeEnumValue("SUBMITTED_SUCCESS"),
                    retry_times_value=0,
                ),
                FakeTaskInstance(
                    id=3003,
                    name="validate",
                    task_type_value="SHELL",
                    workflow_instance_id_value=901,
                    workflow_instance_name_value="daily-sync-901",
                    project_code_value=7,
                    task_code_value=203,
                    state_value=FakeEnumValue("FAILURE"),
                    retry_times_value=2,
                    host="worker-2",
                    log_path_value="/logs/3003.log",
                    end_time_value="2026-04-11 10:01:00",
                    duration_value="8s",
                ),
                FakeTaskInstance(
                    id=3004,
                    name="notify",
                    task_type_value="HTTP",
                    workflow_instance_id_value=901,
                    workflow_instance_name_value="daily-sync-901",
                    project_code_value=7,
                    task_code_value=204,
                    state_value=FakeEnumValue("SUCCESS"),
                    retry_times_value=1,
                    end_time_value="2026-04-11 10:02:00",
                    duration_value="3s",
                ),
            ]
        ),
    )

    result = workflow_instance_service.digest_workflow_instance_result(901)
    data = _mapping(result.data)

    assert result.resolved["workflowInstance"] == {"id": 901}
    assert _mapping(result.resolved["project"])["source"] == "context"
    assert _mapping(data["workflowInstance"])["state"] == "RUNNING_EXECUTION"
    assert data["taskCount"] == 4
    assert data["taskStateCounts"] == {
        "FAILURE": 1,
        "RUNNING_EXECUTION": 1,
        "SUBMITTED_SUCCESS": 1,
        "SUCCESS": 1,
    }
    assert data["taskTypeCounts"] == {
        "HTTP": 1,
        "SHELL": 2,
        "SQL": 1,
    }
    assert data["progress"] == {
        "running": 1,
        "queued": 1,
        "paused": 0,
        "failed": 1,
        "success": 1,
        "other": 0,
        "finished": 2,
        "active": 2,
    }
    assert data["runningTasks"] == [
        {
            "id": 3001,
            "taskCode": 201,
            "name": "extract",
            "taskType": "SHELL",
            "state": "RUNNING_EXECUTION",
            "retryTimes": 0,
            "host": "worker-1",
            "startTime": "2026-04-11 10:00:00",
            "endTime": None,
            "duration": "15s",
            "logAvailable": True,
        }
    ]
    assert data["queuedTasks"] == [
        {
            "id": 3002,
            "taskCode": 202,
            "name": "load",
            "taskType": "SQL",
            "state": "SUBMITTED_SUCCESS",
            "retryTimes": 0,
            "host": None,
            "startTime": None,
            "endTime": None,
            "duration": None,
            "logAvailable": False,
        }
    ]
    assert data["failedTasks"] == [
        {
            "id": 3003,
            "taskCode": 203,
            "name": "validate",
            "taskType": "SHELL",
            "state": "FAILURE",
            "retryTimes": 2,
            "host": "worker-2",
            "startTime": None,
            "endTime": "2026-04-11 10:01:00",
            "duration": "8s",
            "logAvailable": True,
        }
    ]
    assert data["retriedTasks"] == [
        {
            "id": 3003,
            "taskCode": 203,
            "name": "validate",
            "taskType": "SHELL",
            "state": "FAILURE",
            "retryTimes": 2,
            "host": "worker-2",
            "startTime": None,
            "endTime": "2026-04-11 10:01:00",
            "duration": "8s",
            "logAvailable": True,
        },
        {
            "id": 3004,
            "taskCode": 204,
            "name": "notify",
            "taskType": "HTTP",
            "state": "SUCCESS",
            "retryTimes": 1,
            "host": None,
            "startTime": None,
            "endTime": "2026-04-11 10:02:00",
            "duration": "3s",
            "logAvailable": False,
        },
    ]


def test_get_workflow_instance_result_reports_missing_instance(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )

    with pytest.raises(NotFoundError, match="was not found"):
        workflow_instance_service.get_workflow_instance_result(999)


@pytest.mark.parametrize(
    "operation",
    [
        workflow_instance_service.get_workflow_instance_result,
        workflow_instance_service.digest_workflow_instance_result,
        workflow_instance_service.watch_workflow_instance_result,
    ],
)
def test_workflow_instance_reads_preserve_ambiguous_v2_lookup_fallback(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    operation: Callable[..., object],
) -> None:
    upstream_error = ApiResultError(
        result_code=10116,
        result_message="query workflow instance by id error:null",
    )

    def fail_get(*, workflow_instance_id: int) -> FakeWorkflowInstance:
        del workflow_instance_id
        raise upstream_error

    monkeypatch.setattr(fake_workflow_instance_adapter, "get", fail_get)
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )

    with pytest.raises(ApiResultError) as exc_info:
        operation(999)

    assert exc_info.value is upstream_error


def test_workflow_instance_read_preserves_non_missing_v2_fallback_error(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    upstream_error = ApiResultError(
        result_code=10116,
        result_message="query workflow instance by id error:database unavailable",
    )

    def fail_get(*, workflow_instance_id: int) -> FakeWorkflowInstance:
        del workflow_instance_id
        raise upstream_error

    monkeypatch.setattr(fake_workflow_instance_adapter, "get", fail_get)
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )

    with pytest.raises(ApiResultError) as exc_info:
        workflow_instance_service.get_workflow_instance_result(999)

    assert exc_info.value is upstream_error


@pytest.mark.parametrize("result_code", [30001, 30002])
def test_get_workflow_instance_result_translates_permission_errors(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    result_code: int,
) -> None:
    upstream_error = ApiResultError(
        result_code=result_code,
        result_message="workflow instance lookup failed",
    )

    def fail_get(*, workflow_instance_id: int) -> FakeWorkflowInstance:
        del workflow_instance_id
        raise upstream_error

    monkeypatch.setattr(fake_workflow_instance_adapter, "get", fail_get)
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )

    with pytest.raises(PermissionDeniedError) as exc_info:
        workflow_instance_service.get_workflow_instance_result(901)

    assert exc_info.value.details == {
        "resource": "workflow-instance",
        "id": 901,
    }
    assert exc_info.value.suggestion == (
        "Ask a DolphinScheduler administrator to grant access to the workflow "
        "instance's project, then retry."
    )


def test_get_workflow_instance_result_preserves_unknown_api_error(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    upstream_error = ApiResultError(
        result_code=999999,
        result_message="workflow instance lookup failed",
    )

    def fail_get(*, workflow_instance_id: int) -> FakeWorkflowInstance:
        del workflow_instance_id
        raise upstream_error

    monkeypatch.setattr(fake_workflow_instance_adapter, "get", fail_get)
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )

    with pytest.raises(ApiResultError) as exc_info:
        workflow_instance_service.get_workflow_instance_result(901)

    assert exc_info.value is upstream_error


def test_stop_workflow_instance_result_requests_stop_and_returns_refresh(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )

    result = workflow_instance_service.stop_workflow_instance_result(901)
    data = _mapping(result.data)

    assert fake_workflow_instance_adapter.stopped_ids == [901]
    assert data["state"] == "READY_STOP"
    assert result.warnings == [
        "stop requested; current workflow instance state is READY_STOP"
    ]
    assert result.warning_details == [
        {
            "code": "workflow_instance_action_state_after_request",
            "action": "stop",
            "message": "stop requested; current workflow instance state is READY_STOP",
            "current_state": "READY_STOP",
            "expect_non_final": False,
            "target_state": "STOP",
        }
    ]


@pytest.mark.parametrize(
    "ds_version",
    [
        "3.2.0",
        "3.2.1",
        "3.2.2",
        "3.3.1",
        "3.3.2",
        "3.4.0",
        "3.4.1",
        "3.4.2",
        "3.4.3",
    ],
)
def test_stop_generic_failure_reports_source_backed_unknown_outcome(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    ds_version: str,
) -> None:
    calls: list[int] = []

    def ambiguous_stop(*, workflow_instance_id: int) -> None:
        calls.append(workflow_instance_id)
        raise ApiResultError(
            result_code=workflow_instance_types.EXECUTE_WORKFLOW_INSTANCE_ERROR,
            result_message="execute workflow instance error",
        )

    monkeypatch.setattr(fake_workflow_instance_adapter, "stop", ambiguous_stop)
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        profile=make_profile(ds_version=ds_version),
    )

    with pytest.raises(MutationOutcomeUnknownError) as exc_info:
        workflow_instance_service.stop_workflow_instance_result(901)

    error = exc_info.value
    assert calls == [901]
    assert error.details == {
        "resource": "workflow-instance",
        "id": 901,
        "action": "stop",
        "operation": "workflow-instance.stop",
        "ds_version": ds_version,
        "state_before": "RUNNING_EXECUTION",
        "phase": "control_response",
        "mutation_may_have_applied": True,
        "request_replay_safe": False,
        "completed_stages": [],
        "failed_stage": "control_request",
        "project": {"code": 7, "name": "etl-prod", "description": None},
        "known_resources": {
            "project": {"code": 7, "name": "etl-prod", "description": None},
            "workflowInstance": {"id": 901},
        },
    }
    assert error.source == {
        "kind": "remote",
        "system": "dolphinscheduler",
        "layer": "result",
        "result_code": workflow_instance_types.EXECUTE_WORKFLOW_INSTANCE_ERROR,
        "result_message": "execute workflow instance error",
    }
    assert error.suggestion == (
        "Use `dsctl workflow-instance get 901 --project etl-prod` or "
        "`dsctl workflow-instance watch 901 --project etl-prod` to reconcile "
        "the current state. Do not blindly repeat the stop command."
    )


@pytest.mark.parametrize(
    ("ds_version", "result_code"),
    [
        ("3.1.9", workflow_instance_types.EXECUTE_WORKFLOW_INSTANCE_ERROR),
        ("3.2.0", 59999),
    ],
)
def test_stop_preserves_failures_outside_exact_unknown_outcome_scope(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    ds_version: str,
    result_code: int,
) -> None:
    upstream_error = ApiResultError(
        result_code=result_code,
        result_message="unrelated stop failure",
    )

    def fail_stop(*, workflow_instance_id: int) -> None:
        del workflow_instance_id
        raise upstream_error

    monkeypatch.setattr(fake_workflow_instance_adapter, "stop", fail_stop)
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        profile=make_profile(ds_version=ds_version),
    )

    with pytest.raises(ApiResultError) as exc_info:
        workflow_instance_service.stop_workflow_instance_result(901)

    assert exc_info.value is upstream_error


def test_stop_unknown_outcome_does_not_treat_pre_read_state_as_cas(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    running = fake_workflow_instance_adapter.workflow_instances[0]
    fake_workflow_instance_adapter.workflow_instances[0] = replace(
        running,
        state_value=FakeEnumValue("SERIAL_WAIT"),
    )

    def ambiguous_stop(*, workflow_instance_id: int) -> None:
        del workflow_instance_id
        raise ApiResultError(
            result_code=workflow_instance_types.EXECUTE_WORKFLOW_INSTANCE_ERROR,
            result_message="execute workflow instance error",
        )

    monkeypatch.setattr(fake_workflow_instance_adapter, "stop", ambiguous_stop)
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        profile=make_profile(ds_version="3.4.3"),
    )

    with pytest.raises(MutationOutcomeUnknownError) as exc_info:
        workflow_instance_service.stop_workflow_instance_result(901)

    assert exc_info.value.details["state_before"] == "SERIAL_WAIT"
    assert exc_info.value.details["mutation_may_have_applied"] is True


def test_rerun_generic_execute_failure_is_not_stop_uncertainty(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    upstream_error = ApiResultError(
        result_code=workflow_instance_types.EXECUTE_WORKFLOW_INSTANCE_ERROR,
        result_message="execute workflow instance error",
    )

    def fail_rerun(*, workflow_instance_id: int) -> None:
        del workflow_instance_id
        raise upstream_error

    monkeypatch.setattr(fake_workflow_instance_adapter, "rerun", fail_rerun)
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        profile=make_profile(ds_version="3.2.0"),
    )

    with pytest.raises(ApiResultError) as exc_info:
        workflow_instance_service.rerun_workflow_instance_result(902)

    assert exc_info.value is upstream_error


def test_stop_workflow_instance_result_rejects_final_state(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    fake_workflow_instance_adapter.workflow_instances = [
        fake_workflow_instance_adapter.workflow_instances[1]
    ]
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )

    with pytest.raises(InvalidStateError, match="cannot be stopped") as exc_info:
        workflow_instance_service.stop_workflow_instance_result(902)
    assert exc_info.value.suggestion == (
        "Use `dsctl workflow-instance get 902 --project etl-prod` or "
        "`dsctl workflow-instance watch 902 --project etl-prod` to inspect the "
        "current state before retrying stop."
    )


def test_workflow_instance_dynamic_suggestion_shell_quotes_selected_project(
    monkeypatch: pytest.MonkeyPatch,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    fake_workflow_instance_adapter.workflow_instances = [
        fake_workflow_instance_adapter.workflow_instances[1]
    ]
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=FakeProjectAdapter(
            projects=[FakeProject(code=7, name="etl prod")]
        ),
        workflow_instance_adapter=fake_workflow_instance_adapter,
        context=ResourceDefaults(project="etl prod"),
    )

    with pytest.raises(InvalidStateError, match="cannot be stopped") as exc_info:
        workflow_instance_service.stop_workflow_instance_result(902)

    suggestion = exc_info.value.suggestion
    assert suggestion is not None
    assert "--project 'etl prod'" in suggestion
    assert "--project PROJECT" not in suggestion


def test_rerun_workflow_instance_result_reports_runtime_control_conflict(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    def busy_rerun(*, workflow_instance_id: int) -> None:
        raise ApiResultError(
            result_code=workflow_instance_types.WORKFLOW_INSTANCE_EXECUTING_COMMAND,
            result_message=f"workflow instance id {workflow_instance_id} is busy",
        )

    monkeypatch.setattr(fake_workflow_instance_adapter, "rerun", busy_rerun)
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )

    with pytest.raises(
        InvalidStateError,
        match="already executing another runtime control command",
    ) as exc_info:
        workflow_instance_service.rerun_workflow_instance_result(902)
    assert exc_info.value.suggestion == (
        "Use `dsctl workflow-instance get 902 --project etl-prod` or "
        "`dsctl workflow-instance watch 902 --project etl-prod` to inspect the "
        "current state, wait for the active runtime control command to finish, "
        "then retry `dsctl workflow-instance rerun 902 --project etl-prod`."
    )


def test_rerun_workflow_instance_result_maps_missing_master_to_invalid_state(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    result_message = "master does not exist"

    def rerun_without_master(*, workflow_instance_id: int) -> None:
        del workflow_instance_id
        raise ApiResultError(
            result_code=10025,
            result_message=result_message,
        )

    monkeypatch.setattr(
        fake_workflow_instance_adapter,
        "rerun",
        rerun_without_master,
    )
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )

    with pytest.raises(InvalidStateError, match="no available master") as exc_info:
        workflow_instance_service.rerun_workflow_instance_result(902)

    assert exc_info.value.details == {
        "resource": "workflow-instance",
        "id": 902,
        "action": "rerun",
        "operation": "workflow-instance.rerun",
    }
    assert exc_info.value.source == {
        "kind": "remote",
        "system": "dolphinscheduler",
        "layer": "result",
        "result_code": 10025,
        "result_message": result_message,
    }
    assert exc_info.value.suggestion == (
        "Run `dsctl monitor server master` and wait until at least one master "
        "is listed, then retry `dsctl workflow-instance rerun 902 --project "
        "etl-prod`."
    )


def test_watch_workflow_instance_result_waits_for_final_state(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
) -> None:
    watch_adapter = FakeWorkflowInstanceAdapter(
        workflow_instances=[
            FakeWorkflowInstance(
                id=901,
                workflow_definition_code_value=101,
                workflow_definition_version_value=1,
                project_code_value=7,
                state_value=FakeEnumValue("RUNNING_EXECUTION"),
                run_times_value=1,
                name="daily-sync-901",
                executor_id_value=11,
            )
        ],
        workflow_instance_sequences_by_id={
            901: [
                FakeWorkflowInstance(
                    id=901,
                    workflow_definition_code_value=101,
                    workflow_definition_version_value=1,
                    project_code_value=7,
                    state_value=FakeEnumValue("RUNNING_EXECUTION"),
                    run_times_value=1,
                    name="daily-sync-901",
                    executor_id_value=11,
                ),
                FakeWorkflowInstance(
                    id=901,
                    workflow_definition_code_value=101,
                    workflow_definition_version_value=1,
                    project_code_value=7,
                    state_value=FakeEnumValue("SUCCESS"),
                    run_times_value=1,
                    name="daily-sync-901",
                    executor_id_value=11,
                ),
            ]
        },
    )
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=watch_adapter,
    )
    monkeypatch.setattr(
        "dsctl.services.workflow_instance.watch.time.sleep", lambda _: None
    )

    result = workflow_instance_service.watch_workflow_instance_result(
        901,
        interval_seconds=1,
        timeout_seconds=5,
    )

    assert _mapping(result.data)["state"] == "SUCCESS"


def test_watch_workflow_instance_result_times_out(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
) -> None:
    watch_adapter = FakeWorkflowInstanceAdapter(
        workflow_instances=[
            FakeWorkflowInstance(
                id=901,
                workflow_definition_code_value=101,
                workflow_definition_version_value=1,
                project_code_value=7,
                state_value=FakeEnumValue("RUNNING_EXECUTION"),
                run_times_value=1,
                name="daily-sync-901",
                executor_id_value=11,
            )
        ]
    )
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=watch_adapter,
    )
    monotonic_values = iter((0.0, 5.1))
    monkeypatch.setattr(
        "dsctl.services.workflow_instance.watch.time.monotonic",
        lambda: next(monotonic_values),
    )
    monkeypatch.setattr(
        "dsctl.services.workflow_instance.watch.time.sleep", lambda _: None
    )

    with pytest.raises(WaitTimeoutError, match="Timed out waiting") as exc_info:
        workflow_instance_service.watch_workflow_instance_result(
            901,
            interval_seconds=1,
            timeout_seconds=5,
        )
    assert exc_info.value.suggestion == (
        "Retry with a larger --timeout-seconds value or inspect the current "
        "state with `dsctl workflow-instance get 901`."
    )


def test_rerun_workflow_instance_result_requests_rerun(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )

    result = workflow_instance_service.rerun_workflow_instance_result(902)
    data = _mapping(result.data)

    assert fake_workflow_instance_adapter.rerun_ids == [902]
    assert data["state"] == "RUNNING_EXECUTION"
    assert result.warnings == []


def _install_kubeflow_instance_dag(
    workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    *,
    state: str,
) -> None:
    instance = workflow_instance_adapter.workflow_instances[1]
    dag = instance.dagData
    assert dag is not None
    tasks = list(dag.taskDefinitionList or ())
    tasks.append(
        FakeTaskDefinition(
            code=203,
            name="train-model",
            version=1,
            project_code_value=7,
            task_type_value="KUBEFLOW",
            task_params_value=json.dumps(
                {
                    "namespace": json.dumps(
                        {
                            "name": "kubeflow-team",
                            "cluster": "production",
                        },
                        separators=(",", ":"),
                    ),
                    "yamlContent": (
                        'apiVersion: "kubeflow.org/v1"\n'
                        "kind: TFJob\n"
                        "metadata:\n"
                        "  name: train-${system.workflow.instance.id}\n"
                        "  namespace: kubeflow-team\n"
                        "spec:\n"
                        "  tfReplicaSpecs:\n"
                        "    Worker:\n"
                        "      replicas: 1\n"
                    ),
                },
                separators=(",", ":"),
            ),
            worker_group_value="default",
            project_name_value="etl-prod",
        )
    )
    workflow_instance_adapter.workflow_instances[1] = replace(
        instance,
        dag_data_value=replace(dag, task_definition_list_value=tasks),
        state_value=FakeEnumValue(state),
    )


def _assert_kubeflow_replay_suggestion(
    error: UserInputError,
    *,
    action: str,
) -> None:
    assert error.details["reason"] == "kubeflow-workflow-instance-identity-reuse"
    assert error.details["action"] == action
    suggestion = error.suggestion
    assert suggestion is not None
    assert "new workflow instance" in suggestion
    assert "`dsctl workflow run daily-sync --project etl-prod`" in suggestion


def test_rerun_workflow_instance_rejects_kubeflow_dag_before_control_request(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    _install_kubeflow_instance_dag(
        fake_workflow_instance_adapter,
        state="RUNNING_EXECUTION",
    )
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )

    with pytest.raises(UserInputError, match="KUBEFLOW") as exc_info:
        workflow_instance_service.rerun_workflow_instance_result(902)

    assert fake_workflow_instance_adapter.rerun_ids == []
    _assert_kubeflow_replay_suggestion(exc_info.value, action="rerun")


def test_recover_failed_workflow_instance_rejects_kubeflow_dag_before_control_request(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    _install_kubeflow_instance_dag(
        fake_workflow_instance_adapter,
        state="SUCCESS",
    )
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )

    with pytest.raises(UserInputError, match="KUBEFLOW") as exc_info:
        workflow_instance_service.recover_failed_workflow_instance_result(902)

    assert fake_workflow_instance_adapter.recovered_failed_ids == []
    _assert_kubeflow_replay_suggestion(exc_info.value, action="recover-failed")


def test_execute_shell_task_rejects_kubeflow_dag_before_control_request(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    _install_kubeflow_instance_dag(
        fake_workflow_instance_adapter,
        state="RUNNING_EXECUTION",
    )
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )

    with pytest.raises(UserInputError, match="KUBEFLOW") as exc_info:
        workflow_instance_service.execute_task_in_workflow_instance_result(
            902,
            task="extract",
            scope="self",
        )

    assert fake_workflow_instance_adapter.executed_tasks == []
    _assert_kubeflow_replay_suggestion(exc_info.value, action="execute-task")


def test_rerun_workflow_instance_result_warns_if_state_remains_final(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
) -> None:
    workflow_instance_adapter = FakeWorkflowInstanceAdapter(
        workflow_instances=[
            FakeWorkflowInstance(
                id=902,
                workflow_definition_code_value=101,
                workflow_definition_version_value=1,
                project_code_value=7,
                state_value=FakeEnumValue("SUCCESS"),
                run_times_value=1,
                name="daily-sync-902",
                executor_id_value=11,
            )
        ]
    )

    def no_op_rerun(*, workflow_instance_id: int) -> None:
        workflow_instance_adapter.rerun_ids.append(workflow_instance_id)

    monkeypatch.setattr(workflow_instance_adapter, "rerun", no_op_rerun)
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=workflow_instance_adapter,
    )

    result = workflow_instance_service.rerun_workflow_instance_result(902)

    assert result.warnings == [
        "rerun requested; current workflow instance state is SUCCESS"
    ]
    assert result.warning_details == [
        {
            "code": "workflow_instance_action_state_after_request",
            "action": "rerun",
            "message": "rerun requested; current workflow instance state is SUCCESS",
            "current_state": "SUCCESS",
            "expect_non_final": True,
            "target_state": None,
        }
    ]


def test_rerun_workflow_instance_result_rejects_non_final_state(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    running = fake_workflow_instance_adapter.workflow_instances[0]
    fake_workflow_instance_adapter.workflow_instances[0] = replace(
        running,
        dag_data_value=None,
    )
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )

    with pytest.raises(InvalidStateError, match="final state") as exc_info:
        workflow_instance_service.rerun_workflow_instance_result(901)
    assert exc_info.value.suggestion == (
        "Wait for the workflow instance to reach a final state, then retry "
        "`dsctl workflow-instance rerun 901 --project etl-prod`."
    )


def test_execute_task_in_workflow_instance_result_requires_online_definition(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    task_adapter = FakeTaskAdapter(
        workflow_tasks={
            101: [
                FakeTaskDefinition(
                    code=201,
                    name="extract",
                    project_code_value=7,
                )
            ]
        }
    )

    def offline_definition_execute_task(
        *,
        project_code: int,
        workflow_instance_id: int,
        task_code: int,
        scope: str,
    ) -> None:
        del project_code, workflow_instance_id, task_code, scope
        raise ApiResultError(
            result_code=workflow_instance_types.WORKFLOW_DEFINITION_NOT_RELEASE,
            result_message="workflow definition not online",
        )

    monkeypatch.setattr(
        fake_workflow_instance_adapter,
        "execute_task",
        offline_definition_execute_task,
    )
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_adapter=task_adapter,
    )

    with pytest.raises(
        InvalidStateError,
        match="workflow definition must be online",
    ) as exc_info:
        workflow_instance_service.execute_task_in_workflow_instance_result(
            902,
            task="extract",
            scope="self",
        )
    assert exc_info.value.suggestion == (
        "Use `dsctl workflow-instance get 902 --project etl-prod` to inspect the "
        "referenced workflow definition, bring that workflow online with "
        "`dsctl workflow online`, then retry the runtime action."
    )


def test_execute_task_in_workflow_instance_result_suggests_task_discovery(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    task_adapter = FakeTaskAdapter(
        workflow_tasks={
            101: [
                FakeTaskDefinition(
                    code=201,
                    name="extract",
                    project_code_value=7,
                )
            ]
        }
    )

    def missing_task_execute_task(
        *,
        project_code: int,
        workflow_instance_id: int,
        task_code: int,
        scope: str,
    ) -> None:
        del project_code, workflow_instance_id, task_code, scope
        raise ApiResultError(
            result_code=workflow_instance_types.EXECUTE_NOT_DEFINE_TASK,
            result_message="task is not defined in this workflow instance",
        )

    monkeypatch.setattr(
        fake_workflow_instance_adapter,
        "execute_task",
        missing_task_execute_task,
    )
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_adapter=task_adapter,
    )

    with pytest.raises(NotFoundError, match="Task code 201 was not found") as exc_info:
        workflow_instance_service.execute_task_in_workflow_instance_result(
            902,
            task="extract",
            scope="self",
        )

    assert exc_info.value.suggestion == (
        "Run `dsctl task-instance list --workflow-instance 902 --project etl-prod` "
        "to inspect tasks in this workflow instance, then retry with one returned "
        "task name or code."
    )


def test_execute_task_preserves_unrelated_missing_master_code(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    task_adapter = FakeTaskAdapter(
        workflow_tasks={
            101: [
                FakeTaskDefinition(
                    code=201,
                    name="extract",
                    project_code_value=7,
                )
            ]
        }
    )

    def execute_task_error(
        *,
        project_code: int,
        workflow_instance_id: int,
        task_code: int,
        scope: str,
    ) -> None:
        del project_code, workflow_instance_id, task_code, scope
        raise ApiResultError(
            result_code=10025,
            result_message="master does not exist",
        )

    monkeypatch.setattr(
        fake_workflow_instance_adapter,
        "execute_task",
        execute_task_error,
    )
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_adapter=task_adapter,
    )

    with pytest.raises(ApiResultError, match="master does not exist") as exc_info:
        workflow_instance_service.execute_task_in_workflow_instance_result(
            902,
            task="extract",
        )

    assert exc_info.value.result_code == 10025


def test_recover_failed_workflow_instance_result_requests_recovery(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
) -> None:
    workflow_instance_adapter = FakeWorkflowInstanceAdapter(
        workflow_instances=[
            FakeWorkflowInstance(
                id=903,
                workflow_definition_code_value=101,
                workflow_definition_version_value=1,
                project_code_value=7,
                state_value=FakeEnumValue("FAILURE"),
                run_times_value=1,
                name="daily-sync-903",
                executor_id_value=11,
            )
        ]
    )
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=workflow_instance_adapter,
    )

    result = workflow_instance_service.recover_failed_workflow_instance_result(903)
    data = _mapping(result.data)

    assert workflow_instance_adapter.recovered_failed_ids == [903]
    assert data["state"] == "RUNNING_EXECUTION"


def test_recover_failed_workflow_instance_maps_missing_master(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
) -> None:
    workflow_instance_adapter = FakeWorkflowInstanceAdapter(
        workflow_instances=[
            FakeWorkflowInstance(
                id=903,
                workflow_definition_code_value=101,
                workflow_definition_version_value=1,
                project_code_value=7,
                state_value=FakeEnumValue("FAILURE"),
                run_times_value=1,
                name="daily-sync-903",
                executor_id_value=11,
            )
        ]
    )

    def recover_without_master(*, workflow_instance_id: int) -> None:
        del workflow_instance_id
        raise ApiResultError(
            result_code=10025,
            result_message="master does not exist",
        )

    monkeypatch.setattr(
        workflow_instance_adapter,
        "recover_failed",
        recover_without_master,
    )
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=workflow_instance_adapter,
    )

    with pytest.raises(InvalidStateError, match="no available master") as exc_info:
        workflow_instance_service.recover_failed_workflow_instance_result(903)

    assert exc_info.value.details["operation"] == "workflow-instance.recover-failed"
    assert exc_info.value.suggestion == (
        "Run `dsctl monitor server master` and wait until at least one master "
        "is listed, then retry `dsctl workflow-instance recover-failed 903 "
        "--project etl-prod`."
    )


def test_recover_failed_workflow_instance_result_rejects_non_failure_state(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    running = fake_workflow_instance_adapter.workflow_instances[0]
    fake_workflow_instance_adapter.workflow_instances[0] = replace(
        running,
        dag_data_value=None,
    )
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )

    with pytest.raises(
        InvalidStateError,
        match="FAILURE state before recover-failed",
    ) as exc_info:
        workflow_instance_service.recover_failed_workflow_instance_result(901)
    assert exc_info.value.suggestion == (
        "Use `dsctl workflow-instance get 901 --project etl-prod` or "
        "`dsctl workflow-instance watch 901 --project etl-prod` to confirm the "
        "instance is in FAILURE before retrying `recover-failed`."
    )


def test_recover_failed_workflow_instance_result_waits_for_final_state_on_race(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
) -> None:
    workflow_instance_adapter = FakeWorkflowInstanceAdapter(
        workflow_instances=[
            FakeWorkflowInstance(
                id=903,
                workflow_definition_code_value=101,
                workflow_definition_version_value=1,
                project_code_value=7,
                state_value=FakeEnumValue("FAILURE"),
                run_times_value=1,
                name="daily-sync-903",
                executor_id_value=11,
            )
        ]
    )

    def not_finished_recover_failed(*, workflow_instance_id: int) -> None:
        raise ApiResultError(
            result_code=workflow_instance_types.WORKFLOW_INSTANCE_NOT_FINISHED,
            result_message=f"workflow instance id {workflow_instance_id} not finished",
        )

    monkeypatch.setattr(
        workflow_instance_adapter,
        "recover_failed",
        not_finished_recover_failed,
    )
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=workflow_instance_adapter,
    )

    with pytest.raises(
        InvalidStateError,
        match="must be in a final state before this action can proceed",
    ) as exc_info:
        workflow_instance_service.recover_failed_workflow_instance_result(903)
    assert exc_info.value.suggestion == (
        "Wait for the workflow instance to reach a final state, then retry "
        "`dsctl workflow-instance recover-failed 903 --project etl-prod`."
    )


def test_execute_task_in_workflow_instance_result_requests_task_execution(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    task_adapter = FakeTaskAdapter(
        workflow_tasks={
            101: [
                FakeTaskDefinition(
                    code=201,
                    name="extract",
                    project_code_value=7,
                )
            ]
        }
    )
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_adapter=task_adapter,
    )

    result = workflow_instance_service.execute_task_in_workflow_instance_result(
        902,
        task="extract",
        scope="pre",
    )
    data = _mapping(result.data)

    assert fake_workflow_instance_adapter.executed_tasks == [(902, 201, "pre")]
    assert data["state"] == "RUNNING_EXECUTION"
    assert _mapping(result.resolved["task"])["code"] == 201
    assert result.resolved["scope"] == "pre"


def test_execute_task_in_workflow_instance_result_rejects_non_final_state(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    running = fake_workflow_instance_adapter.workflow_instances[0]
    fake_workflow_instance_adapter.workflow_instances[0] = replace(
        running,
        dag_data_value=None,
    )
    task_adapter = FakeTaskAdapter(
        workflow_tasks={
            101: [
                FakeTaskDefinition(
                    code=201,
                    name="extract",
                    project_code_value=7,
                )
            ]
        }
    )
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_adapter=task_adapter,
    )

    with pytest.raises(InvalidStateError, match="final state") as exc_info:
        workflow_instance_service.execute_task_in_workflow_instance_result(
            901,
            task="extract",
        )
    assert exc_info.value.suggestion == (
        "Wait for the workflow instance to reach a final state, then retry "
        "`dsctl workflow-instance execute-task 901 --project etl-prod --task "
        "extract`."
    )


def test_execute_task_in_workflow_instance_result_reports_scope_choices(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    task_adapter = FakeTaskAdapter(
        workflow_tasks={
            101: [
                FakeTaskDefinition(
                    code=201,
                    name="extract",
                    project_code_value=7,
                )
            ]
        }
    )
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_adapter=task_adapter,
    )

    with pytest.raises(
        UserInputError,
        match="Task execution scope must be one of: self, pre, post",
    ) as exc_info:
        workflow_instance_service.execute_task_in_workflow_instance_result(
            902,
            task="extract",
            scope="before",
        )

    assert exc_info.value.suggestion == (
        "Pass `--scope self`, `--scope pre`, or `--scope post`."
    )


def test_edit_workflow_instance_result_dry_run_compiles_patch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )
    patch_file = tmp_path / "workflow-instance.patch.yaml"
    patch_file.write_text(
        """
patch:
  workflow:
    set:
      timeout: 45
  tasks:
    update:
      - match:
          name: extract
        set:
          command: echo extract-v2
""".strip(),
        encoding="utf-8",
    )

    result = workflow_instance_service.edit_workflow_instance_result(
        902,
        patch=patch_file,
        dry_run=True,
    )
    data = _mapping(result.data)
    request = _mapping(first_dry_run_request(data))
    form = _mapping(request["form"])

    assert request["method"] == "PUT"
    assert request["path"] == "/projects/7/workflow-instances/902"
    assert form["syncDefine"] is False
    assert form["timeout"] == 45
    assert data["no_change"] is False
    assert _mapping(result.resolved["project"])["name"] == "etl-prod"
    assert _mapping(result.resolved["workflow"])["code"] == 101


def test_310_instance_sync_binds_main_ids_without_rejecting_historical_dag(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    instance_adapter = fake_workflow_instance_adapter
    instance = instance_adapter.workflow_instances[1]
    assert instance.dagData is not None
    historical = {task.code: task for task in instance.dagData.taskDefinitionList or ()}

    class MainWire:
        def __init__(self) -> None:
            self.tasks = {
                code: replace(
                    task, id=900 + code, version=9, name=f"current-{task.name}"
                )
                for code, task in historical.items()
            }
            self.calls: list[int] = []

        def get(self, *, project_code: int, task_code: int) -> object:
            assert project_code == 7
            self.calls.append(task_code)
            return SimpleNamespace(payload=self.tasks[task_code])

    wire = MainWire()
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=instance_adapter,
        profile=make_profile(ds_version="3.1.0"),
        task_definition_wire=wire,
    )
    patch = tmp_path / "instance-task.yaml"
    patch.write_text(
        "patch:\n  tasks:\n    update:\n      - match: {name: extract}\n"
        "        set: {command: 'echo repaired'}\n",
        encoding="utf-8",
    )

    unsynced = workflow_instance_service.edit_workflow_instance_result(
        902, patch=patch, sync_definition=False, dry_run=True
    )
    assert unsynced.failure is None
    assert wire.calls == []

    preview = workflow_instance_service.edit_workflow_instance_result(
        902, patch=patch, sync_definition=True, dry_run=True
    )
    form = _mapping(_mapping(first_dry_run_request(_mapping(preview.data)))["form"])
    tasks = json.loads(str(form["taskDefinitionJson"]))
    assert [(task["code"], task["id"]) for task in tasks] == [(201, 1101), (202, 1102)]
    assert wire.calls == [201, 202]

    original_update = instance_adapter.update

    def update_and_sync_main(
        *,
        project_code: int,
        workflow_instance_id: int,
        task_relation_json: str,
        task_definition_json: str,
        sync_define: bool,
        global_params: str | None = None,
        locations: str | None = None,
        timeout: int | None = None,
        schedule_time: str | None = None,
    ) -> FakeWorkflow:
        saved = original_update(
            project_code=project_code,
            workflow_instance_id=workflow_instance_id,
            task_relation_json=task_relation_json,
            task_definition_json=task_definition_json,
            sync_define=sync_define,
            global_params=global_params,
            locations=locations,
            timeout=timeout,
            schedule_time=schedule_time,
        )
        refreshed = instance_adapter.workflow_instances[1]
        assert refreshed.dagData is not None
        changed = next(
            task
            for task in refreshed.dagData.taskDefinitionList or ()
            if task.code == 201
        )
        wire.tasks[201] = replace(changed, id=1101)
        return saved

    monkeypatch.setattr(instance_adapter, "update", update_and_sync_main)
    applied = workflow_instance_service.edit_workflow_instance_result(
        902, patch=patch, sync_definition=True
    )
    assert applied.failure is None
    assert wire.calls == [201, 202, 201, 202, 201]


def test_edit_workflow_instance_result_dry_run_omits_no_change_request(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )
    patch_file = tmp_path / "workflow-instance.patch.yaml"
    patch_file.write_text(
        "patch:\n  workflow:\n    set:\n      timeout: 30\n",
        encoding="utf-8",
    )

    result = workflow_instance_service.edit_workflow_instance_result(
        902,
        patch=patch_file,
        dry_run=True,
    )

    data = _mapping(result.data)
    assert data["no_change"] is True
    assert data["requests"] == []
    assert fake_workflow_instance_adapter.update_calls == []


def test_edit_workflow_instance_result_updates_finished_instance(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )
    patch_file = tmp_path / "workflow-instance.patch.yaml"
    patch_file.write_text(
        """
patch:
  workflow:
    set:
      timeout: 45
      global_params:
        env: prod
        region: cn
  tasks:
    rename:
      - from: extract
        to: extract-v2
""".strip(),
        encoding="utf-8",
    )

    result = workflow_instance_service.edit_workflow_instance_result(
        902,
        patch=patch_file,
        sync_definition=True,
    )
    data = _mapping(result.data)
    update_call = fake_workflow_instance_adapter.update_calls[0]
    relation_payload = json.loads(str(update_call["task_relation_json"]))
    definition_payload = json.loads(str(update_call["task_definition_json"]))
    global_params_payload = json.loads(str(update_call["global_params"]))
    locations_payload = json.loads(str(update_call["locations"]))

    assert update_call["project_code"] == 7
    assert update_call["workflow_instance_id"] == 902
    assert update_call["sync_define"] is True
    assert update_call["timeout"] == 45
    assert update_call["schedule_time"] is None
    assert [item["postTaskCode"] for item in relation_payload] == [201, 202]
    assert [item["name"] for item in definition_payload] == ["extract-v2", "load"]
    assert definition_payload[0]["version"] == 1
    assert definition_payload[0]["taskParams"] == '{"rawScript":"echo extract"}'
    assert global_params_payload == [
        {
            "prop": "env",
            "direct": "IN",
            "type": "VARCHAR",
            "value": "prod",
        },
        {
            "prop": "region",
            "direct": "IN",
            "type": "VARCHAR",
            "value": "cn",
        },
    ]
    assert [item["taskCode"] for item in locations_payload] == [201, 202]
    assert data["workflowDefinitionVersion"] == 2
    assert data["timeout"] == 45
    assert result.resolved["syncDefine"] is True
    assert _mapping(result.resolved["workflow"])["version"] == 2


def test_edit_workflow_instance_allocates_codes_for_new_tasks(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    task_adapter = FakeTaskAdapter(
        workflow_tasks={},
        generated_codes=[8_201],
    )
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_adapter=task_adapter,
    )
    patch_file = tmp_path / "workflow-instance.patch.yaml"
    patch_file.write_text(
        """
patch:
  tasks:
    create:
      - name: verify
        type: SHELL
        command: echo verify
        depends_on: [load]
""".strip(),
        encoding="utf-8",
    )

    workflow_instance_service.edit_workflow_instance_result(
        902,
        patch=patch_file,
    )
    definition_payload = json.loads(
        str(fake_workflow_instance_adapter.update_calls[0]["task_definition_json"])
    )

    assert task_adapter.generate_code_calls == [{"project_code": 7, "count": 1}]
    assert [(task["name"], task["code"]) for task in definition_payload] == [
        ("extract", 201),
        ("load", 202),
        ("verify", 8_201),
    ]


def test_edit_workflow_instance_result_dry_run_compiles_full_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )
    workflow_file = tmp_path / "workflow-instance.yaml"
    workflow_file.write_text(
        """
workflow:
  name: daily-sync
  project: etl-prod
  timeout: 45
  global_params:
    env: prod
    region: cn
  execution_type: PARALLEL
  release_state: OFFLINE
tasks:
  - name: extract
    type: SHELL
    command: echo extract-v2
  - name: verify
    type: SHELL
    command: echo verify
    depends_on:
      - extract
""".strip(),
        encoding="utf-8",
    )

    result = workflow_instance_service.edit_workflow_instance_result(
        902,
        file=workflow_file,
        dry_run=True,
    )
    data = _mapping(result.data)
    diff = _mapping(data["diff"])
    form = _mapping(_mapping(first_dry_run_request(data))["form"])
    definition_payload = json.loads(str(form["taskDefinitionJson"]))

    assert data["dry_run"] is True
    assert data["no_change"] is False
    assert result.resolved["input_mode"] == "file"
    assert result.resolved["file"] == str(workflow_file)
    assert diff["added_tasks"] == ["verify"]
    assert diff["deleted_tasks"] == ["load"]
    assert diff["task_changes"] == [
        {
            "task": "extract",
            "changes": [
                {
                    "field": "task_params",
                    "before": {"rawScript": "echo extract"},
                    "after": {"rawScript": "echo extract-v2"},
                }
            ],
        }
    ]
    assert diff["workflow_changes"] == [
        {"field": "timeout", "before": 30, "after": 45},
        {
            "field": "global_params",
            "before": {"env": "prod"},
            "after": {"env": "prod", "region": "cn"},
        },
    ]
    assert [item["name"] for item in definition_payload] == ["extract", "verify"]
    assert form["timeout"] == 45


def test_edit_workflow_instance_result_full_file_requires_confirmation_for_deletion(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )
    workflow_file = tmp_path / "workflow-instance.yaml"
    workflow_file.write_text(
        """
workflow:
  name: daily-sync
  project: etl-prod
  timeout: 30
  execution_type: PARALLEL
  release_state: OFFLINE
tasks:
  - name: extract
    type: SHELL
    command: echo extract
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ConfirmationRequiredError) as captured:
        workflow_instance_service.edit_workflow_instance_result(
            902,
            file=workflow_file,
        )

    assert captured.value.details["risk_type"] == (
        "workflow_instance_full_edit_destructive_change"
    )
    assert captured.value.details["deleted_tasks"] == ["load"]
    confirmation = str(captured.value.details["confirmation_token"])

    result = workflow_instance_service.edit_workflow_instance_result(
        902,
        file=workflow_file,
        confirm_risk=confirmation,
    )
    task_payload = json.loads(
        str(fake_workflow_instance_adapter.update_calls[0]["task_definition_json"])
    )

    assert _mapping(result.data)["workflowDefinitionVersion"] == 2
    assert [item["name"] for item in task_payload] == ["extract"]


def test_edit_workflow_instance_result_rejects_non_final_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )
    patch_file = tmp_path / "workflow-instance.patch.yaml"
    patch_file.write_text(
        """
patch:
  workflow:
    set:
      timeout: 45
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(
        InvalidStateError,
        match="final state before edit",
    ) as exc_info:
        workflow_instance_service.edit_workflow_instance_result(
            901,
            patch=patch_file,
        )
    assert exc_info.value.suggestion == (
        "Wait for the workflow instance to reach a final state, then retry "
        f"`dsctl workflow-instance edit 901 --project etl-prod --patch "
        f"{patch_file}`."
    )


def test_edit_workflow_instance_result_rejects_unsupported_workflow_fields(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )
    patch_file = tmp_path / "workflow-instance.patch.yaml"
    patch_file.write_text(
        """
patch:
  workflow:
    set:
      name: renamed-instance-workflow
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(UserInputError, match="only supports") as exc_info:
        workflow_instance_service.edit_workflow_instance_result(
            902,
            patch=patch_file,
        )
    assert exc_info.value.suggestion == (
        "Use `dsctl workflow edit --patch ...` for definition-level fields "
        "such as name, description, or release_state."
    )


def test_edit_workflow_instance_result_rejects_full_file_definition_fields(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )
    workflow_file = tmp_path / "workflow-instance.yaml"
    workflow_file.write_text(
        """
workflow:
  name: renamed-workflow
  project: etl-prod
  timeout: 30
  execution_type: PARALLEL
  release_state: OFFLINE
tasks:
  - name: extract
    type: SHELL
    command: echo extract
  - name: load
    type: SHELL
    command: echo load
    depends_on:
      - extract
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(UserInputError, match="only supports") as exc_info:
        workflow_instance_service.edit_workflow_instance_result(
            902,
            file=workflow_file,
            dry_run=True,
        )

    assert exc_info.value.details["unsupported_fields"] == ["name"]
    assert exc_info.value.suggestion == (
        "Use `dsctl workflow edit --file ...` for definition-level fields "
        "such as name, description, execution_type, or release_state."
    )


def test_edit_workflow_instance_result_rejects_full_file_project_mismatch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )
    workflow_file = tmp_path / "workflow-instance.yaml"
    workflow_file.write_text(
        """
workflow:
  name: daily-sync
  project: other-project
  timeout: 30
  execution_type: PARALLEL
  release_state: OFFLINE
tasks:
  - name: extract
    type: SHELL
    command: echo extract
  - name: load
    type: SHELL
    command: echo load
    depends_on:
      - extract
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(UserInputError, match="project does not match"):
        workflow_instance_service.edit_workflow_instance_result(
            902,
            file=workflow_file,
            dry_run=True,
        )


def test_edit_workflow_instance_result_rejects_full_file_schedule_block(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DS_VERSION", "3.4.1")
    monkeypatch.setenv("DS_API_URL", "http://example.test/dolphinscheduler")
    monkeypatch.setenv("DS_API_TOKEN", "test-token")
    workflow_file = tmp_path / "workflow-instance.yaml"
    workflow_file.write_text(
        """
workflow:
  name: daily-sync
tasks:
  - name: extract
    type: SHELL
    command: echo extract
schedule:
  cron: "0 0 0 * * ?"
  timezone: Asia/Shanghai
  start: "2026-04-11 00:00:00"
  end: "2026-04-12 00:00:00"
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(UserInputError, match="does not mutate schedule blocks"):
        workflow_instance_service.edit_workflow_instance_result(
            902,
            file=workflow_file,
            dry_run=True,
        )


def test_edit_workflow_instance_result_requires_one_input_file(
    tmp_path: Path,
) -> None:
    patch_file = tmp_path / "workflow-instance.patch.yaml"
    workflow_file = tmp_path / "workflow-instance.yaml"
    patch_file.write_text("patch: {}\n", encoding="utf-8")
    workflow_file.write_text(
        """
workflow:
  name: daily-sync
tasks:
  - name: extract
    type: SHELL
    command: echo extract
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(UserInputError, match="exactly one"):
        workflow_instance_service.edit_workflow_instance_result(
            902,
            patch=patch_file,
            file=workflow_file,
        )


@pytest.mark.parametrize("version", ["2.0.0", "2.0.1", "2.0.2"])
@pytest.mark.parametrize("dry_run", [False, True])
@pytest.mark.parametrize("change", ["task", "edge"])
def test_instance_dag_changes_require_explicit_sync_on_early_20(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    version: str,
    *,
    dry_run: bool,
    change: str,
) -> None:
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        profile=make_profile(ds_version=version),
    )
    patch = tmp_path / "edit.yaml"
    field = "command: echo changed" if change == "task" else "depends_on: []"
    patch.write_text(
        "patch:\n  tasks:\n    update:\n      - match:\n          name: load\n"
        f"        set:\n          {field}\n",
        encoding="utf-8",
    )
    if dry_run:
        result = workflow_instance_service.edit_workflow_instance_result(
            902, patch=patch, dry_run=True
        )
        assert isinstance(result.failure, UserInputError)
        assert _mapping(result.data)["requests"]
        assert _mapping(result.data)["workflow_state_constraints"]
        failure = result.failure
    else:
        with pytest.raises(UserInputError) as error:
            workflow_instance_service.edit_workflow_instance_result(902, patch=patch)
        failure = error.value
    assert failure.details["required_option"] == "--sync-definition"
    assert "only if" in (failure.suggestion or "")
    assert fake_workflow_instance_adapter.update_calls == []


@pytest.mark.parametrize("version", ["2.0.0", "2.0.1", "2.0.2"])
@pytest.mark.parametrize("release_state", ["ONLINE", "OFFLINE", None])
@pytest.mark.parametrize("dry_run", [False, True])
def test_early_20_instance_sync_requires_online_definition_for_changes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    version: str,
    release_state: str | None,
    *,
    dry_run: bool,
) -> None:
    workflows = FakeWorkflowAdapter(
        dags={},
        workflows=[
            FakeWorkflow(
                code=101,
                name="daily-sync",
                project_code_value=7,
                release_state_value=None
                if release_state is None
                else FakeEnumValue(release_state),
            )
        ],
    )
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        workflow_adapter=workflows,
        profile=make_profile(ds_version=version),
    )
    patch = tmp_path / "edit.yaml"
    patch.write_text(
        "patch:\n  workflow:\n    set:\n      timeout: 45\n", encoding="utf-8"
    )
    if release_state != "ONLINE" and not dry_run:
        with pytest.raises(InvalidStateError) as error:
            workflow_instance_service.edit_workflow_instance_result(
                902, patch=patch, sync_definition=True
            )
        assert error.value.details["release_state"] == release_state
        assert "workflow edit" in (error.value.suggestion or "")
        assert fake_workflow_instance_adapter.update_calls == []
        return
    result = workflow_instance_service.edit_workflow_instance_result(
        902, patch=patch, sync_definition=True, dry_run=dry_run
    )
    if release_state != "ONLINE":
        assert isinstance(result.failure, InvalidStateError)
        assert _mapping(result.data)["workflow_state_constraints"]
    else:
        assert result.failure is None
    assert "may set" in str(result.resolved["native_definition_release_effect"])
    assert len(fake_workflow_instance_adapter.update_calls) == (0 if dry_run else 1)


@pytest.mark.parametrize("version", ["2.0.0", "2.0.1", "2.0.2"])
@pytest.mark.parametrize("sync_definition", [False, True])
def test_early_20_instance_unchanged_task_needs_no_sync_or_definition_lookup(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    version: str,
    *,
    sync_definition: bool,
) -> None:
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        profile=make_profile(ds_version=version),
    )
    patch = tmp_path / "edit.yaml"
    patch.write_text(
        "patch:\n  tasks:\n    update:\n      - match:\n          name: extract\n"
        "        set:\n          command: echo extract\n",
        encoding="utf-8",
    )
    result = workflow_instance_service.edit_workflow_instance_result(
        902, patch=patch, sync_definition=sync_definition, dry_run=True
    )
    assert result.failure is None
    assert _mapping(result.data)["no_change"] is True
    assert _mapping(result.data)["requests"] == []
    assert fake_workflow_instance_adapter.update_calls == []


@pytest.mark.parametrize("version", ["2.0.3", "2.0.9", "3.4.1"])
def test_later_instance_dag_edit_does_not_require_definition_sync(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    version: str,
) -> None:
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        profile=make_profile(ds_version=version),
    )
    patch = tmp_path / "edit.yaml"
    patch.write_text(
        "patch:\n  tasks:\n    update:\n      - match:\n          name: extract\n"
        "        set:\n          command: echo changed\n",
        encoding="utf-8",
    )
    result = workflow_instance_service.edit_workflow_instance_result(902, patch=patch)
    assert result.failure is None
    assert fake_workflow_instance_adapter.update_calls[0]["sync_define"] is False


@pytest.mark.parametrize("version", ["2.0.0", "2.0.1", "2.0.2"])
def test_scalar_instance_edit_reads_back_after_null_native_result(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    version: str,
) -> None:
    dag = fake_workflow_instance_adapter.workflow_instances[1].dagData
    assert dag is not None
    assert dag.workflowDefinition is not None
    original_globals = dag.workflowDefinition.globalParams
    install_runtime_instance_domain_runtime(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        profile=make_profile(ds_version=version),
        workflow_update_returns_none=True,
    )
    patch = tmp_path / "edit.yaml"
    patch.write_text(
        "patch:\n  workflow:\n    set:\n      timeout: 45\n", encoding="utf-8"
    )
    result = workflow_instance_service.edit_workflow_instance_result(902, patch=patch)
    assert result.failure is None
    assert _mapping(result.data)["timeout"] == 45
    assert _mapping(result.resolved["workflow"])["code"] == 101
    assert (
        _mapping(result.resolved["workflow"])["version"]
        == _mapping(result.data)["workflowDefinitionVersion"]
    )
    assert len(fake_workflow_instance_adapter.update_calls) == 1
    assert fake_workflow_instance_adapter.update_calls[0]["sync_define"] is False
    assert (
        fake_workflow_instance_adapter.update_calls[0]["global_params"]
        == original_globals
    )


def test_null_instance_edit_response_requires_successful_readback(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    original_get = fake_workflow_instance_adapter.get

    def get(*, workflow_instance_id: int) -> FakeWorkflowInstance:
        if fake_workflow_instance_adapter.update_calls:
            message = "post-update instance readback unavailable"
            raise ApiTransportError(message)
        return original_get(workflow_instance_id=workflow_instance_id)

    monkeypatch.setattr(fake_workflow_instance_adapter, "get", get)
    install_runtime_instance_domain_runtime(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        profile=make_profile(ds_version="2.0.0"),
        workflow_update_returns_none=True,
    )
    patch = tmp_path / "edit.yaml"
    patch.write_text(
        "patch:\n  workflow:\n    set:\n      timeout: 45\n", encoding="utf-8"
    )
    with pytest.raises(ApiTransportError, match="readback unavailable"):
        workflow_instance_service.edit_workflow_instance_result(902, patch=patch)
    assert len(fake_workflow_instance_adapter.update_calls) == 1


@pytest.mark.parametrize("dry_run", [False, True])
def test_instance_dag_sync_blocker_precedes_new_task_code_allocation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    *,
    dry_run: bool,
) -> None:
    tasks = FakeTaskAdapter(workflow_tasks={}, generated_codes=[8201])
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_adapter=tasks,
        profile=make_profile(ds_version="2.0.0"),
    )
    patch = tmp_path / "new-task.yaml"
    patch.write_text(
        "patch:\n  tasks:\n    create:\n      - name: verify\n        type: SHELL\n"
        "        command: echo verify\n        depends_on: [load]\n",
        encoding="utf-8",
    )
    if dry_run:
        result = workflow_instance_service.edit_workflow_instance_result(
            902, patch=patch, dry_run=True
        )
        assert isinstance(result.failure, UserInputError)
    else:
        with pytest.raises(UserInputError):
            workflow_instance_service.edit_workflow_instance_result(902, patch=patch)
    assert tasks.generate_code_calls == []
    assert fake_workflow_instance_adapter.update_calls == []


@pytest.mark.parametrize("input_mode", ["patch", "file"])
def test_instance_task_edits_preserve_unedited_global_parameter_metadata(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    input_mode: str,
) -> None:
    current = fake_workflow_instance_adapter.workflow_instances[1]
    dag = current.dagData
    assert dag is not None
    assert dag.workflowDefinition is not None
    raw = ' [{"prop":"env","value":"prod","type":"VARCHAR","direct":"OUT"}] '
    fake_workflow_instance_adapter.workflow_instances[1] = replace(
        current,
        dag_data_value=replace(
            dag,
            workflow_definition_value=replace(
                dag.workflowDefinition,
                global_params_value=raw,
            ),
        ),
    )
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )
    path = tmp_path / "edit.yaml"
    if input_mode == "file":
        exported = workflow_instance_service.export_workflow_instance_yaml_result(902)
        document = yaml.safe_load(str(_mapping(exported.data)["yaml"]))
        document["tasks"][0]["task_params"]["rawScript"] = "echo changed"
        path.write_text(yaml.safe_dump(document), encoding="utf-8")
    else:
        path.write_text(
            "patch:\n  tasks:\n    update:\n      - match:\n          name: extract\n"
            "        set:\n          command: echo changed\n",
            encoding="utf-8",
        )
    preview = workflow_instance_service.edit_workflow_instance_result(
        902,
        patch=path if input_mode == "patch" else None,
        file=path if input_mode == "file" else None,
        dry_run=True,
    )
    form = _mapping(_mapping(first_dry_run_request(_mapping(preview.data)))["form"])
    assert form["globalParams"] == raw
    workflow_instance_service.edit_workflow_instance_result(
        902,
        patch=path if input_mode == "patch" else None,
        file=path if input_mode == "file" else None,
    )
    assert fake_workflow_instance_adapter.update_calls[0]["global_params"] == raw
