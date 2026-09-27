from __future__ import annotations

import importlib
import importlib.util
import json
import sys
from collections import Counter
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, NoReturn, cast

import pytest
from pydantic import TypeAdapter, ValidationError

if TYPE_CHECKING:
    from types import ModuleType

    from tests.codegen.exact_contract_corpus import ExactContractCorpus


class _FakeSession:
    def __init__(self, payload: object) -> None:
        self.payload = payload
        self.calls: list[dict[str, object]] = []

    def request(
        self,
        method: str,
        url: str,
        headers: dict[str, str],
        **kwargs: object,
    ) -> object:
        self.calls.append(
            {
                "method": method,
                "url": url,
                "headers": headers,
                **kwargs,
            }
        )
        return self.payload

    def raise_payload_error(
        self,
        message: str,
        *,
        validation_error_count: int | None = None,
        cause: Exception | None = None,
    ) -> NoReturn:
        del validation_error_count
        error = RuntimeError(message)
        if cause is None:
            raise error
        raise error from cause

    def raise_result_error(
        self,
        *,
        code: int | None,
        message: str,
        data: object,
    ) -> NoReturn:
        del code, data
        raise RuntimeError(message)


def _load_codegen_module() -> ModuleType:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    module_path = tools_dir / "generate_ds_contract.py"
    spec = importlib.util.spec_from_file_location("generate_ds_contract", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_contract_generator_entrypoint_is_not_tied_to_one_ds_release() -> None:
    module = _load_codegen_module()

    parser = module._build_parser()
    args = parser.parse_args([])

    assert "3.4.1" not in parser.description
    assert args.output == Path("build/ds_contract/ds_raw_contract.json")


def test_contract_generator_accepts_one_exact_source_runtime_slice() -> None:
    module = _load_codegen_module()

    args = module._build_parser().parse_args(
        [
            "--ds-source",
            "3.2.2=build/upstream/ds-3.2.2",
            "--runtime-slice",
        ]
    )

    assert args.ds_source == "3.2.2=build/upstream/ds-3.2.2"
    assert args.runtime_slice is True


def test_rendered_multiline_docstrings_have_blank_lines_without_whitespace() -> None:
    _load_codegen_module()
    render_support = importlib.import_module("ds_codegen.render.package.render_support")

    lines = render_support.render_docstring_lines(
        "Summary\n\nDetails",
        indent="    ",
    )

    assert lines == ['    """', "    Summary", "", "    Details", '    """']


@pytest.mark.parametrize(
    ("java_package", "python_package"),
    [
        pytest.param(
            "org.apache.dolphinscheduler.extract.master.dto",
            ("extract", "master", "dto"),
            id="workflow-executor-dto",
        ),
        pytest.param(
            "org.apache.dolphinscheduler.task.executor.dto",
            ("task", "executor", "dto"),
            id="task-executor-dto",
        ),
        pytest.param(
            "org.apache.dolphinscheduler.common.process",
            ("common", "process"),
            id="legacy-common-process",
        ),
        pytest.param(
            "org.apache.dolphinscheduler.remote.dto",
            ("remote", "dto"),
            id="legacy-remote-dto",
        ),
        pytest.param(
            "org.springframework.core.io",
            ("external", "springframework", "core", "io"),
            id="spring-resource",
        ),
        pytest.param(
            "org.apache.dolphinscheduler.dao.vo",
            ("dao", "views"),
            id="legacy-dao-view",
        ),
    ],
)
def test_package_planner_maps_new_runtime_model_namespaces(
    java_package: str,
    python_package: tuple[str, ...],
) -> None:
    _load_codegen_module()
    planner = importlib.import_module("ds_codegen.render.package.planner")

    assert planner._map_package_parts(java_package.split(".")) == python_package


def test_contract_generator_exposes_only_snapshot_and_package_outputs() -> None:
    module = _load_codegen_module()
    help_text = module._build_parser().format_help()

    assert "--output" in help_text
    assert "--package-output" in help_text
    assert "--python-output" not in help_text
    assert "--requests-example-output" not in help_text
    assert "--requests-client-output" not in help_text
    assert "--requests-package-output" not in help_text


def test_contract_generator_keeps_modern_package_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_codegen_module()
    snapshot = SimpleNamespace(
        operation_count=1,
        dto_count=2,
        model_count=3,
        enum_count=4,
    )
    snapshot_outputs: list[tuple[object, Path, object]] = []
    package_outputs: list[tuple[object, Path]] = []
    monkeypatch.setattr(module, "build_contract_snapshot", lambda _root: snapshot)
    monkeypatch.setattr(
        module,
        "write_contract_snapshot",
        lambda item, path, *, provenance: snapshot_outputs.append(
            (item, path, provenance)
        ),
    )
    monkeypatch.setattr(
        module,
        "write_generated_package",
        lambda item, path: package_outputs.append((item, path)),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "generate_ds_contract.py",
            "--repo-root",
            str(tmp_path),
            "--package-output",
            "generated-output",
        ],
    )

    module.main()

    assert snapshot_outputs == [
        (
            snapshot,
            tmp_path / "build" / "ds_contract" / "ds_raw_contract.json",
            None,
        )
    ]
    assert package_outputs == [(snapshot, tmp_path / "generated-output")]


def test_package_renderer_does_not_import_retired_renderers() -> None:
    _load_codegen_module()
    package_root = (
        Path(__file__).resolve().parents[2]
        / "tools"
        / "ds_codegen"
        / "render"
        / "package"
    )

    rendered_source = "\n".join(
        path.read_text() for path in sorted(package_root.glob("*.py"))
    )
    assert "ds_codegen.render.requests_client" not in rendered_source
    assert "ds_codegen.render.requests_example" not in rendered_source


def test_java_literal_helpers_preserve_numeric_semantics() -> None:
    _load_codegen_module()
    java_literals_module = importlib.import_module("ds_codegen.java_literals")

    assert (
        java_literals_module.decode_java_literal_text(
            "4045513703397452451L",
            prefix_operators=["-"],
        )
        == -4045513703397452451
    )
    assert java_literals_module.parse_java_numeric_literal("1_000L") == 1000
    assert java_literals_module.parse_java_numeric_literal("0.0d") == 0.0
    assert java_literals_module.parse_java_numeric_literal("1.5f") == 1.5
    assert java_literals_module.infer_java_literal_type("4045513703397452451L") == (
        "Long"
    )
    assert java_literals_module.infer_java_literal_type("0.0d") == "Double"


@pytest.mark.source_rebuild
def test_contract_codegen_extracts_executor_operation_and_enum(
    tmp_path: Path,
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    module = _load_codegen_module()
    extract_pipeline_module = importlib.import_module("ds_codegen.extract.pipeline")

    with exact_contract_corpus.codegen_repo_root("3.4.1") as source_repo_root:
        snapshot = module.build_contract_snapshot(source_repo_root)

    assert snapshot.operation_count > 20
    assert max(Counter(op.operation_id for op in snapshot.operations).values()) == 1
    trigger_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "ExecutorController.triggerWorkflowDefinition"
    )
    assert trigger_operation.http_method == "POST"
    assert (
        trigger_operation.path
        == "projects/{projectCode}/executors/start-workflow-instance"
    )
    assert trigger_operation.return_type == "Result<List<Integer>>"
    assert trigger_operation.logical_return_type == "List<Integer>"
    assert trigger_operation.summary == "startWorkflowInstance"
    assert trigger_operation.documentation == "execute workflow instance"
    assert trigger_operation.returns_doc == "start workflow result code"

    task_depend_type = next(
        parameter
        for parameter in trigger_operation.parameters
        if parameter.name == "taskDependType"
    )
    assert task_depend_type.binding == "request_param"
    assert task_depend_type.wire_name == "taskDependType"
    assert task_depend_type.default_value == "TASK_POST"
    assert task_depend_type.required is False
    workflow_definition_code = next(
        parameter
        for parameter in trigger_operation.parameters
        if parameter.name == "workflowDefinitionCode"
    )
    assert workflow_definition_code.description == "workflow definition code"
    assert workflow_definition_code.example == "100"
    assert workflow_definition_code.schema_type == "Long"
    tenant_code = next(
        parameter
        for parameter in trigger_operation.parameters
        if parameter.name == "tenantCode"
    )
    assert tenant_code.description is None
    assert tenant_code.example == "default"
    assert tenant_code.schema_type == "String"

    cloud_list_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "CloudController.listDataFactory"
    )
    assert cloud_list_operation.return_type == "Result"
    assert cloud_list_operation.inferred_return_type == "List<String>"
    assert cloud_list_operation.logical_return_type == "List<String>"

    kerberos_startup_state_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "DataSourceController.getKerberosStartupState"
    )
    assert kerberos_startup_state_operation.return_type == "Result<Object>"
    assert kerberos_startup_state_operation.inferred_return_type == "boolean"
    get_tables_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "DataSourceController.getTables"
    )
    assert get_tables_operation.return_type == "Result<Object>"
    assert get_tables_operation.inferred_return_type == (
        "List<org.apache.dolphinscheduler.spi.params.base.ParamsOptions>"
    )
    get_table_columns_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "DataSourceController.getTableColumns"
    )
    assert get_table_columns_operation.return_type == "Result<Object>"
    assert get_table_columns_operation.inferred_return_type == (
        "List<org.apache.dolphinscheduler.spi.params.base.ParamsOptions>"
    )
    get_databases_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "DataSourceController.getDatabases"
    )
    assert get_databases_operation.return_type == "Result<Object>"
    assert get_databases_operation.inferred_return_type == (
        "List<org.apache.dolphinscheduler.spi.params.base.ParamsOptions>"
    )

    environment_query_by_code_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "EnvironmentController.queryEnvironmentByCode"
    )
    assert environment_query_by_code_operation.return_type == "Result"
    assert environment_query_by_code_operation.inferred_return_type == (
        "org.apache.dolphinscheduler.api.dto.EnvironmentDto"
    )

    project_list_paging_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "ProjectController.queryProjectListPaging"
    )
    assert project_list_paging_operation.return_type == "Result"
    assert project_list_paging_operation.inferred_return_type == (
        "org.apache.dolphinscheduler.api.utils.PageInfo<"
        "org.apache.dolphinscheduler.dao.entity.Project>"
    )
    assert project_list_paging_operation.logical_return_type == (
        "org.apache.dolphinscheduler.api.utils.PageInfo<"
        "org.apache.dolphinscheduler.dao.entity.Project>"
    )
    assert project_list_paging_operation.response_projection == "direct"

    queue_list_paging_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "QueueController.queryQueueListPaging"
    )
    assert queue_list_paging_operation.return_type == "Result<PageInfo<Queue>>"
    assert queue_list_paging_operation.logical_return_type == (
        "org.apache.dolphinscheduler.api.utils.PageInfo<"
        "org.apache.dolphinscheduler.dao.entity.Queue>"
    )

    queue_v2_list_paging_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "QueueV2Controller.queryQueueListPaging"
    )
    assert queue_v2_list_paging_operation.return_type == "Result<PageInfo<Queue>>"
    assert queue_v2_list_paging_operation.logical_return_type == (
        "org.apache.dolphinscheduler.api.utils.PageInfo<"
        "org.apache.dolphinscheduler.dao.entity.Queue>"
    )

    datasource_list_paging_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "DataSourceController.queryDataSourceListPaging"
    )
    assert datasource_list_paging_operation.return_type == "Result<Object>"
    assert (
        datasource_list_paging_operation.logical_return_type
        == "org.apache.dolphinscheduler.api.utils.PageInfo<"
        "org.apache.dolphinscheduler.dao.entity.DataSource>"
    )

    datasource_query_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "DataSourceController.queryDataSource"
    )
    assert datasource_query_operation.return_type == "Result<Object>"
    assert datasource_query_operation.logical_return_type == (
        "org.apache.dolphinscheduler.plugin.datasource.api.datasource."
        "BaseDataSourceParamDTO"
    )

    worker_group_list_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "WorkerGroupController.queryAllWorkerGroups"
    )
    assert worker_group_list_operation.return_type == "Result"
    assert worker_group_list_operation.logical_return_type == "List<String>"

    project_v2_create_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "ProjectV2Controller.createProject"
    )
    assert project_v2_create_operation.return_type == "ProjectCreateResponse"
    assert project_v2_create_operation.inferred_return_type is None
    assert project_v2_create_operation.logical_return_type == (
        "org.apache.dolphinscheduler.dao.entity.Project"
    )

    login_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "LoginController.login"
    )
    assert login_operation.return_type == "Result"
    assert login_operation.inferred_return_type == "Map<String, String>"

    verify_alert_plugin_name_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "AlertPluginInstanceController.verifyGroupName"
    )
    assert verify_alert_plugin_name_operation.return_type == "Result"
    assert verify_alert_plugin_name_operation.inferred_return_type == "Void"

    verify_alert_group_name_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "AlertGroupController.verifyGroupName"
    )
    assert verify_alert_group_name_operation.return_type == "Result"
    assert verify_alert_group_name_operation.inferred_return_type == "Void"

    add_fav_task_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "FavTaskController.addFavTask"
    )
    assert add_fav_task_operation.return_type == "Result"
    assert add_fav_task_operation.inferred_return_type == "Boolean"

    delete_environment_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "EnvironmentController.deleteEnvironment"
    )
    assert delete_environment_operation.return_type == "Result"
    assert delete_environment_operation.inferred_return_type == "Void"

    verify_environment_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "EnvironmentController.verifyEnvironment"
    )
    assert verify_environment_operation.return_type == "Result"
    assert verify_environment_operation.inferred_return_type == "Void"

    verify_namespace_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "K8sNamespaceController.verifyNamespace"
    )
    assert verify_namespace_operation.return_type == "Result"
    assert verify_namespace_operation.inferred_return_type == "Void"

    verify_user_name_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "UsersController.verifyUserName"
    )
    assert verify_user_name_operation.return_type == "Result"
    assert verify_user_name_operation.inferred_return_type == "Void"

    grant_project_by_code_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "UsersController.grantProjectByCode"
    )
    assert grant_project_by_code_operation.return_type == "Result"
    assert grant_project_by_code_operation.inferred_return_type == "Void"
    assert grant_project_by_code_operation.logical_return_type == "Void"

    revoke_project_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "UsersController.revokeProject"
    )
    assert revoke_project_operation.return_type == "Result"
    assert revoke_project_operation.inferred_return_type == "Void"
    assert revoke_project_operation.logical_return_type == "Void"

    delete_schedule_by_id_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "SchedulerController.deleteScheduleById"
    )
    assert delete_schedule_by_id_operation.return_type == "Result"
    assert delete_schedule_by_id_operation.inferred_return_type == "Void"

    execute_task_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "ExecutorController.executeTask"
    )
    assert execute_task_operation.return_type == "Result"
    assert execute_task_operation.inferred_return_type == "Void"

    scheduler_update_schedule_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "SchedulerController.updateSchedule"
    )
    assert scheduler_update_schedule_operation.return_type == "Result"
    assert scheduler_update_schedule_operation.inferred_return_type == (
        "org.apache.dolphinscheduler.dao.entity.Schedule"
    )

    scheduler_create_schedule_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "SchedulerController.createSchedule"
    )
    assert scheduler_create_schedule_operation.return_type == "Result"
    assert scheduler_create_schedule_operation.logical_return_type == (
        "org.apache.dolphinscheduler.dao.entity.Schedule"
    )

    gen_task_code_list_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "TaskDefinitionController.genTaskCodeList"
    )
    assert gen_task_code_list_operation.return_type == "Result"
    assert gen_task_code_list_operation.inferred_return_type == "List<Long>"

    task_definition_detail_operation = next(
        operation
        for operation in snapshot.operations
        if (
            operation.operation_id
            == "TaskDefinitionController.queryTaskDefinitionDetail"
        )
    )
    assert task_definition_detail_operation.return_type == "Result"
    assert task_definition_detail_operation.inferred_return_type == (
        "org.apache.dolphinscheduler.api.vo.TaskDefinitionVO"
    )

    release_task_definition_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "TaskDefinitionController.releaseTaskDefinition"
    )
    assert release_task_definition_operation.return_type == "Result"
    assert release_task_definition_operation.inferred_return_type == "Void"

    task_definition_versions_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id
        == "TaskDefinitionController.queryTaskDefinitionVersions"
    )
    assert task_definition_versions_operation.return_type == "Result"
    assert (
        task_definition_versions_operation.inferred_return_type
        == "org.apache.dolphinscheduler.api.utils.PageInfo<"
        "org.apache.dolphinscheduler.dao.entity.TaskDefinitionLog>"
    )

    delete_task_definition_version_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id
        == "TaskDefinitionController.deleteTaskDefinitionVersion"
    )
    assert delete_task_definition_version_operation.return_type == "Result"
    assert delete_task_definition_version_operation.inferred_return_type == "Void"

    switch_task_definition_version_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id
        == "TaskDefinitionController.switchTaskDefinitionVersion"
    )
    assert switch_task_definition_version_operation.return_type == "Result"
    assert switch_task_definition_version_operation.inferred_return_type == "Void"

    update_task_with_upstream_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "TaskDefinitionController.updateTaskWithUpstream"
    )
    assert update_task_with_upstream_operation.return_type == "Result"
    assert update_task_with_upstream_operation.inferred_return_type == "long"
    assert update_task_with_upstream_operation.logical_return_type == "Optional<long>"

    download_log_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id
        == "LoggerController.downloadTaskLog__get_log_download_log"
    )
    assert download_log_operation.return_type == "ResponseEntity"
    assert download_log_operation.inferred_return_type == "byte[]"

    force_task_success_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "TaskInstanceV2Controller.forceTaskSuccess"
    )
    assert force_task_success_operation.return_type == "TaskInstanceSuccessResponse"
    assert force_task_success_operation.inferred_return_type == "Void"

    workflow_instance_v2_query_by_id_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id
        == "WorkflowInstanceV2Controller.queryWorkflowInstanceById"
    )
    assert workflow_instance_v2_query_by_id_operation.return_type == "Result"
    assert (
        workflow_instance_v2_query_by_id_operation.inferred_return_type
        == "org.apache.dolphinscheduler.dao.entity.WorkflowInstance"
    )
    project_v2_query_list_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "ProjectV2Controller.queryProjectListPaging"
    )
    assert project_v2_query_list_operation.consumes == ["application/json"]
    task_instance_v2_query_list_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "TaskInstanceV2Controller.queryTaskListPaging"
    )
    assert task_instance_v2_query_list_operation.consumes == ["application/json"]
    workflow_definition_simple_list_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id
        == "WorkflowDefinitionController.queryWorkflowDefinitionSimpleList"
    )
    assert workflow_definition_simple_list_operation.return_type == "Result"
    assert workflow_definition_simple_list_operation.inferred_return_type == (
        "List<"
        "WorkflowDefinitionServiceImpl_queryWorkflowDefinitionSimpleList_"
        "arrayNodeItem>"
    )

    assign_worker_groups_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "ProjectWorkerGroupController.assignWorkerGroups"
    )
    worker_groups = next(
        parameter
        for parameter in assign_worker_groups_operation.parameters
        if parameter.name == "workerGroups"
    )
    assert worker_groups.binding == "request_param"
    assert worker_groups.wire_name == "workerGroups"

    query_assigned_worker_groups_operation = next(
        operation
        for operation in snapshot.operations
        if (
            operation.operation_id
            == "ProjectWorkerGroupController.queryAssignedWorkerGroups"
        )
    )
    assert query_assigned_worker_groups_operation.return_type == "Map<String, Object>"
    assert (
        query_assigned_worker_groups_operation.logical_return_type
        == "List<org.apache.dolphinscheduler.dao.entity.ProjectWorkerGroup>"
    )
    assert query_assigned_worker_groups_operation.response_projection == "status_data"

    query_workflow_lineage_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "WorkflowLineageController.queryWorkFlowLineage"
    )
    assert query_workflow_lineage_operation.return_type == "Result<Map<String, Object>>"
    assert (
        query_workflow_lineage_operation.logical_return_type
        == "WorkflowLineageController_queryWorkFlowLineage_result"
    )
    assert query_workflow_lineage_operation.response_projection == "single_data"

    query_dependent_tasks_operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "WorkflowLineageController.queryDependentTasks"
    )
    assert query_dependent_tasks_operation.return_type == "Result<Map<String, Object>>"
    assert (
        query_dependent_tasks_operation.logical_return_type
        == "WorkflowLineageController_queryDependentTasks_result"
    )
    assert query_dependent_tasks_operation.response_projection == "single_data"

    for operation_id in (
        "WorkflowDefinitionController.createWorkflowDefinition",
        "WorkflowDefinitionController.updateWorkflowDefinition",
    ):
        workflow_write_operation = next(
            operation
            for operation in snapshot.operations
            if operation.operation_id == operation_id
        )
        assert workflow_write_operation.logical_return_type == (
            "org.apache.dolphinscheduler.dao.entity.WorkflowDefinition"
        )
        assert workflow_write_operation.response_projection == "direct"

    run_mode_enum = next(
        enum_spec for enum_spec in snapshot.enums if enum_spec.name == "RunMode"
    )
    assert [value.name for value in run_mode_enum.values] == [
        "RUN_MODE_SERIAL",
        "RUN_MODE_PARALLEL",
    ]
    assert [field.name for field in run_mode_enum.fields] == ["code", "descp"]
    assert run_mode_enum.json_value_field is None
    assert run_mode_enum.values[0].arguments == ["0", "serial run"]

    assert snapshot.dto_count > 0
    task_instance_query_request = next(
        dto for dto in snapshot.dtos if dto.name == "TaskInstanceQueryRequest"
    )
    assert task_instance_query_request.extends == (
        "org.apache.dolphinscheduler.api.dto.PageQueryDto"
    )
    assert [field.name for field in task_instance_query_request.fields[:2]] == [
        "pageSize",
        "pageNo",
    ]
    task_execute_type = next(
        field
        for field in task_instance_query_request.fields
        if field.name == "taskExecuteType"
    )
    assert task_execute_type.wire_name == "taskExecuteType"
    assert task_execute_type.default_value == "BATCH"

    assert snapshot.model_count > 0
    task_instance_success_response = next(
        model
        for model in snapshot.models
        if model.name == "TaskInstanceSuccessResponse"
    )
    assert task_instance_success_response.kind == "api_dto"
    assert task_instance_success_response.extends == (
        "org.apache.dolphinscheduler.api.utils.Result"
    )

    task_instance_model = next(
        model for model in snapshot.models if model.name == "TaskInstance"
    )
    assert task_instance_model.kind == "dao_entity"
    assert any(field.name == "state" for field in task_instance_model.fields)
    workflow_instance_model = next(
        model for model in snapshot.models if model.name == "WorkflowInstance"
    )
    state_desc_list = next(
        field
        for field in workflow_instance_model.fields
        if field.name == "stateDescList"
    )
    assert state_desc_list.java_type == (
        "List<org.apache.dolphinscheduler.dao.entity.WorkflowInstance.StateDesc>"
    )

    workflow_instance_state_desc_model = next(
        model for model in snapshot.models if model.name == "WorkflowInstance.StateDesc"
    )
    assert any(
        field.name == "state"
        and field.java_type
        == "org.apache.dolphinscheduler.common.enums.WorkflowExecutionStatus"
        for field in workflow_instance_state_desc_model.fields
    )
    resource_type_enum = next(
        enum_spec for enum_spec in snapshot.enums if enum_spec.name == "ResourceType"
    )
    assert [value.name for value in resource_type_enum.values] == ["FILE", "ALL"]
    assert [field.name for field in resource_type_enum.fields] == ["code", "desc"]
    assert resource_type_enum.json_value_field is None

    with exact_contract_corpus.codegen_repo_root("3.4.1") as source_repo_root:
        additional_enums = extract_pipeline_module._extract_enum_specs(
            source_repo_root,
            {
                "org.apache.dolphinscheduler.spi.params.base.FormType",
                "org.apache.dolphinscheduler.plugin.task.api.enums.dp.DataType",
            },
            {},
        )
    form_type_enum = next(
        enum_spec for enum_spec in additional_enums if enum_spec.name == "FormType"
    )
    assert form_type_enum.json_value_field == "formType"
    assert [field.name for field in form_type_enum.fields] == ["formType"]
    dp_data_type_enum = next(
        enum_spec for enum_spec in additional_enums if enum_spec.name == "DataType"
    )
    assert dp_data_type_enum.json_value_field == "code"
    assert [field.name for field in dp_data_type_enum.fields] == ["code", "description"]
    resource_component_model = next(
        model for model in snapshot.models if model.name == "ResourceComponent"
    )
    assert any(
        field.name == "type"
        and field.java_type == "org.apache.dolphinscheduler.spi.enums.ResourceType"
        for field in resource_component_model.fields
    )
    environment_dto_model = next(
        model for model in snapshot.models if model.name == "EnvironmentDto"
    )
    assert any(
        field.name == "workerGroups" and field.java_type == "List<String>"
        for field in environment_dto_model.fields
    )
    project_model = next(model for model in snapshot.models if model.name == "Project")
    assert project_model.import_path == "org.apache.dolphinscheduler.dao.entity.Project"
    page_info_model = next(
        model for model in snapshot.models if model.name == "PageInfo"
    )
    assert (
        page_info_model.import_path == "org.apache.dolphinscheduler.api.utils.PageInfo"
    )
    assert any(
        field.wire_name == "currentPage" and field.nullable
        for field in page_info_model.fields
    )
    tree_view_dto_model = next(
        model for model in snapshot.models if model.name == "TreeViewDto"
    )
    assert (
        tree_view_dto_model.import_path
        == "org.apache.dolphinscheduler.api.dto.treeview.TreeViewDto"
    )
    schedule_model = next(
        model for model in snapshot.models if model.name == "Schedule"
    )
    assert (
        schedule_model.import_path == "org.apache.dolphinscheduler.dao.entity.Schedule"
    )
    workflow_definition_model = next(
        model for model in snapshot.models if model.name == "WorkflowDefinition"
    )
    assert (
        workflow_definition_model.import_path
        == "org.apache.dolphinscheduler.dao.entity.WorkflowDefinition"
    )
    workflow_instance_model_canonical = next(
        model for model in snapshot.models if model.name == "WorkflowInstance"
    )
    assert (
        workflow_instance_model_canonical.import_path
        == "org.apache.dolphinscheduler.dao.entity.WorkflowInstance"
    )
    task_definition_model_canonical = next(
        model for model in snapshot.models if model.name == "TaskDefinition"
    )
    assert (
        task_definition_model_canonical.import_path
        == "org.apache.dolphinscheduler.dao.entity.TaskDefinition"
    )
    assert any(
        field.name == "taskParams" and field.java_type == "JsonValue"
        for field in task_definition_model_canonical.fields
    )
    assert any(
        field.name == "taskParamMap"
        and field.java_type == "Map<String, Optional<String>>"
        for field in task_definition_model_canonical.fields
    )
    workflow_task_relation_model = next(
        model for model in snapshot.models if model.name == "WorkflowTaskRelation"
    )
    assert (
        workflow_task_relation_model.import_path
        == "org.apache.dolphinscheduler.dao.entity.WorkflowTaskRelation"
    )
    assert any(
        field.name == "conditionParams" and field.java_type == "JsonValue"
        for field in workflow_task_relation_model.fields
    )
    dag_data_model = next(model for model in snapshot.models if model.name == "DagData")
    assert (
        dag_data_model.import_path == "org.apache.dolphinscheduler.dao.entity.DagData"
    )
    duplicate_model_names = Counter(model.name for model in snapshot.models)
    assert {
        name: count for name, count in duplicate_model_names.items() if count > 1
    } == {}

    output_path = tmp_path / "contract.json"
    module.write_contract_snapshot(snapshot, output_path)
    payload = json.loads(output_path.read_text())
    assert payload["operation_count"] == snapshot.operation_count
    assert payload["enum_count"] == snapshot.enum_count
    assert payload["dto_count"] == snapshot.dto_count
    assert payload["model_count"] == snapshot.model_count

    package_output_root = tmp_path / "package"
    module.write_generated_package(snapshot, package_output_root)
    version_root = package_output_root / "generated" / "versions" / "ds_3_4_1"
    assert (version_root / "client.py").exists()
    assert (version_root / "api" / "operations" / "executor.py").exists()
    assert (version_root / "common" / "enums" / "priority.py").exists()
    assert (version_root / "dao" / "entities" / "workflow_instance.py").exists()
    assert not any(version_root.rglob("*_root.py"))
    assert (
        version_root / "api" / "contracts" / "project" / "project_query_request.py"
    ).exists()
    assert (version_root / "api" / "views" / "workflow_definition.py").exists()
    assert not (version_root / "api" / "views" / "pagination.py").exists()

    task_definition_vo_text = (
        version_root / "api" / "views" / "task_definition.py"
    ).read_text()
    task_definition_entity_text = (
        version_root / "dao" / "entities" / "task_definition.py"
    ).read_text()
    assert "class TaskDefinitionVO(TaskDefinition):" in task_definition_vo_text
    project_query_request_text = (
        version_root / "api" / "contracts" / "project" / "project_query_request.py"
    ).read_text()
    assert "class ProjectQueryRequest(PageQueryDto):" in project_query_request_text
    workflow_instance_text = (
        version_root / "dao" / "entities" / "workflow_instance.py"
    ).read_text()
    root_models_text = (version_root / "_models.py").read_text()
    assert "class WorkflowInstanceStateDesc(BaseEntityModel):" in workflow_instance_text
    workflow_definition_views_text = (
        version_root / "api" / "views" / "workflow_definition.py"
    ).read_text()
    workflow_create_request_text = (
        version_root / "api" / "contracts" / "workflow" / "workflow_create_request.py"
    ).read_text()
    views_init_text = (version_root / "api" / "views" / "__init__.py").read_text()
    executor_operations_text = (
        version_root / "api" / "operations" / "executor.py"
    ).read_text()
    task_definition_operations_text = (
        version_root / "api" / "operations" / "task_definition.py"
    ).read_text()
    operations_init_text = (
        version_root / "api" / "operations" / "__init__.py"
    ).read_text()
    environment_operations_text = (
        version_root / "api" / "operations" / "environment.py"
    ).read_text()
    logger_operations_text = (
        version_root / "api" / "operations" / "logger.py"
    ).read_text()
    result_contract_text = (
        version_root / "api" / "contracts" / "result.py"
    ).read_text()
    audit_model_type_text = (
        version_root / "api" / "contracts" / "audit_log" / "audit_model_type_dto.py"
    ).read_text()
    page_info_contract_text = (
        version_root / "api" / "contracts" / "page_info.py"
    ).read_text()
    fav_task_dto_text = (
        version_root / "api" / "contracts" / "fav_task_dto.py"
    ).read_text()
    resource_item_view_text = (
        version_root / "api" / "views" / "resource_item.py"
    ).read_text()
    statistics_state_request_text = (
        version_root / "api" / "contracts" / "project" / "statistics_state_request.py"
    ).read_text()
    workflow_task_relation_text = (
        version_root / "dao" / "entities" / "workflow_task_relation.py"
    ).read_text()
    project_entity_text = (version_root / "dao" / "entities" / "project.py").read_text()
    project_create_response_text = (
        version_root / "api" / "contracts" / "project" / "project_create_response.py"
    ).read_text()
    project_v2_operations_text = (
        version_root / "api" / "operations" / "project_v2.py"
    ).read_text()
    queue_v2_operations_text = (
        version_root / "api" / "operations" / "queue_v2.py"
    ).read_text()
    workflow_lineage_operations_text = (
        version_root / "api" / "operations" / "workflow_lineage.py"
    ).read_text()
    data_source_operations_text = (
        version_root / "api" / "operations" / "data_source.py"
    ).read_text()
    worker_group_operations_text = (
        version_root / "api" / "operations" / "worker_group.py"
    ).read_text()
    base_data_source_param_dto_text = (
        version_root
        / "plugin"
        / "datasource_api"
        / "datasource"
        / "base_data_source_param_dto.py"
    ).read_text()
    task_instance_v2_operations_text = (
        version_root / "api" / "operations" / "task_instance_v2.py"
    ).read_text()
    priority_enum_text = (version_root / "common" / "enums" / "priority.py").read_text()
    task_instance_dependent_details_text = (
        version_root / "dao" / "entities" / "task_instance_dependent_details.py"
    ).read_text()
    command_entity_text = (version_root / "dao" / "entities" / "command.py").read_text()
    task_instance_success_response_text = (
        version_root
        / "api"
        / "contracts"
        / "task_instance"
        / "task_instance_success_response.py"
    ).read_text()
    task_property_model_text = (
        version_root / "plugin" / "task_api" / "model" / "property.py"
    ).read_text()
    base_operations_text = (
        version_root / "api" / "operations" / "_base.py"
    ).read_text()
    generated_python_files = list(version_root.rglob("*.py"))
    generated_package_text = "\n".join(
        path.read_text() for path in generated_python_files
    )
    assert generated_python_files
    assert all(
        "disable-error-code=explicit-any" not in path.read_text()
        for path in generated_python_files
    )
    assert "serialVersionUID" not in task_property_model_text
    assert (
        "class WorkflowDefinitionSimpleItem(BaseViewModel):"
        in workflow_definition_views_text
    )
    assert '"""Workflow create request"""' in workflow_create_request_text
    assert (
        "class WorkflowCreateRequest(BaseContractModel):"
        in workflow_create_request_text
    )
    assert "name: str = Field(examples=['workflow name'])" in (
        workflow_create_request_text
    )
    assert "projectCode: int = Field(examples=[12345])" in workflow_create_request_text
    assert (
        "warningGroupId: int = Field(default=0, examples=[2])"
        in workflow_create_request_text
    )
    assert "timeout: int = Field(default=0, examples=[60])" in (
        workflow_create_request_text
    )
    assert (
        "json_schema_extra={'allowable_values': ['ONLINE', 'OFFLINE']}"
        in workflow_create_request_text
    )
    assert (
        "json_schema_extra={'allowable_values': ['PARALLEL', "
        "'SERIAL_WAIT', 'SERIAL_DISCARD', 'SERIAL_PRIORITY']}"
        in workflow_create_request_text
    )
    assert not (version_root / "api" / "views" / "pagination.py").exists()
    assert not (version_root / "api" / "views" / "data_source.py").exists()
    assert "PaginationWindow" not in views_init_text
    assert "DataSourceOption" not in views_init_text
    assert "class PageInfoDataSource(PageInfo[DataSource]):" in page_info_contract_text
    assert "class PageInfoQueue(PageInfo[Queue]):" in page_info_contract_text
    assert "class BaseContractModel(BaseModel):" in root_models_text
    assert "class BaseViewModel(BaseModel):" in root_models_text
    assert "class BaseEntityModel(BaseModel):" in root_models_text
    assert "from pydantic import BaseModel, ConfigDict, JsonValue" in (root_models_text)
    assert "JsonObject: TypeAlias = dict[str, JsonValue]" in root_models_text
    assert "disable-error-code=explicit-any" not in root_models_text
    assert 'extra="ignore"' in root_models_text
    assert (
        "child: list[AuditModelTypeDto] | None = Field(default=None)"
        in audit_model_type_text
    )
    assert (
        "taskDependType: TaskDependType | None = "
        "Field(default=TaskDependType.TASK_POST)" in command_entity_text
    )
    assert (
        "failureStrategy: FailureStrategy | None = "
        "Field(default=FailureStrategy.CONTINUE)" in command_entity_text
    )
    assert (
        "class TaskInstanceSuccessResponse(Result[object]):"
        in task_instance_success_response_text
    )
    assert (
        '"""AST-inferred view from '
        "generated.view."
        "WorkflowDefinitionServiceImpl_queryWorkflowDefinitionSimpleList_arrayNodeItem."
        '"""' in workflow_definition_views_text
    )
    assert "AccessTokenServiceImpl_PageInfo_created" not in generated_package_text
    assert "QueueServiceImpl_PageInfo_created" not in generated_package_text
    assert "DataSourceServiceImpl_PageInfo_created" not in generated_package_text
    assert "DataSourceServiceImpl_ParamsOptions_created" not in generated_package_text
    assert "PaginationWindow" not in generated_package_text
    assert "DataSourceOption" not in generated_package_text
    assert (version_root / "api" / "views" / "__init__.py").exists()
    assert (version_root / "common" / "enums" / "__init__.py").exists()
    assert (version_root / "api" / "operations" / "__init__.py").exists()
    assert "from typing import cast" not in executor_operations_text
    assert "from ._base import BaseRequestsClient, BaseParamsModel" in (
        executor_operations_text
    )
    assert "from ..contracts.page_info import PageInfoQueue" in queue_v2_operations_text
    assert "    ) -> PageInfoQueue:" in queue_v2_operations_text
    assert (
        "from ...spi.params.base.params_options import ParamsOptions"
        in data_source_operations_text
    )
    assert "    ) -> PageInfoDataSource:" in data_source_operations_text
    assert "    ) -> list[ParamsOptions]:" in data_source_operations_text
    assert "def query_all_worker_groups(" in worker_group_operations_text
    assert "    ) -> list[str]:" in worker_group_operations_text
    assert "from pydantic import ConfigDict, Field" in (base_data_source_param_dto_text)
    assert "from ....spi.enums.db_type import DbType" in (
        base_data_source_param_dto_text
    )
    assert "model_config = ConfigDict(" in base_data_source_param_dto_text
    assert 'extra="allow"' in base_data_source_param_dto_text
    assert (
        "type: DbType | None = Field(default=None, description='datasource type')"
        in base_data_source_param_dto_text
    )
    assert "from pydantic import Field, TypeAdapter" in (executor_operations_text)
    assert "class TriggerWorkflowDefinitionParams(BaseParamsModel):" in (
        executor_operations_text
    )
    assert "workflowDefinitionCode: int = Field(" in executor_operations_text
    assert "scheduleTime: str = Field(" in executor_operations_text
    assert "failureStrategy: FailureStrategy = Field(" in executor_operations_text
    assert "startNodeList: str | None = Field(default=None" in (
        executor_operations_text
    )
    assert "tenantCode: str | None = Field(default=None, examples=['default'])" in (
        executor_operations_text
    )
    assert "environmentCode: int | None = Field(default=None, examples=[-1])" in (
        executor_operations_text
    )
    assert (
        "expectedParallelismNumber: int | None = Field(default=None"
        in executor_operations_text
    )
    assert "examples=[8]" in executor_operations_text
    assert "dryRun: int | None = Field(default=None, examples=[0])" in (
        executor_operations_text
    )
    assert (
        "allLevelDependent: bool | None = Field(default=None, "
        "examples=[False])" in executor_operations_text
    )
    assert "from typing import Generic, TypeVar" in result_contract_text
    assert 'T = TypeVar("T")' in result_contract_text
    assert "class Result(BaseContractModel, Generic[T]):" in result_contract_text
    assert "data: T | None = Field(default=None, description='data')" in (
        result_contract_text
    )
    assert "from typing import Generic, TypeVar" in page_info_contract_text
    assert 'T = TypeVar("T")' in page_info_contract_text
    assert "class PageInfo(BaseContractModel, Generic[T]):" in page_info_contract_text
    assert (
        "totalList: list[T] = Field(default_factory=list, "
        "description='totalList')" in page_info_contract_text
    )
    assert (
        "total: int = Field(default=0, description='total')" in page_info_contract_text
    )
    assert (
        "pageSize: int = Field(default=20, description='page size')"
        in page_info_contract_text
    )
    assert (
        "currentPage: int | None = Field(default=0, description='current page')"
        in page_info_contract_text
    )
    assert "isCollection: bool = Field(default=False, alias='collection')" in (
        fav_task_dto_text
    )
    assert "isDirectory: bool = Field(default=False, alias='directory')" in (
        resource_item_view_text
    )
    assert "from ..._models import BaseEntityModel, JsonValue" in (
        task_definition_entity_text
    )
    assert (
        "taskParams: JsonValue | None = Field("
        "default=None, description='user defined parameters'"
        in task_definition_entity_text
    )
    assert "from ..._models import BaseEntityModel, JsonValue" in (
        workflow_task_relation_text
    )
    assert "conditionParams: JsonValue | None = Field(default=None)" in (
        workflow_task_relation_text
    )
    assert "isAll: bool = Field(default=False, examples=[True])" in (
        statistics_state_request_text
    )
    assert "startTime: str | None = Field(default=None, alias='startDate'" in (
        statistics_state_request_text
    )
    assert "endTime: str | None = Field(default=None, alias='endDate'" in (
        statistics_state_request_text
    )
    assert "class PageInfoProject(PageInfo[Project]):" in page_info_contract_text
    assert "code: int = Field(default=0, description='project code')" in (
        project_entity_text
    )
    assert "perm: int = Field(default=0, description='permission')" in (
        project_entity_text
    )
    assert (
        "defCount: int = Field(default=0, description='process define count')"
        in project_entity_text
    )
    assert (
        "class ProjectCreateResponse(Result[Project]):" in project_create_response_text
    )
    assert (
        "class TaskInstanceDependentDetails(TaskInstance, Generic[T]):"
        in task_instance_dependent_details_text
    )
    assert (
        "taskInstanceDependentResults: list[T] | None = Field(default=None)"
        in task_instance_dependent_details_text
    )
    assert (
        "class TaskInstanceDependentDetailsAbstractTaskInstanceContext("
        "TaskInstanceDependentDetails[AbstractTaskInstanceContext]"
        "):" in task_instance_dependent_details_text
    )
    assert "def __new__(cls, wire_value: str, code: int, descp: str) -> Priority:" in (
        priority_enum_text
    )
    assert '"""Define process and task priority"""' in priority_enum_text
    assert "# 0 highest priority" in priority_enum_text
    assert "Start Workflow Instance" in executor_operations_text
    assert "DS operation: ExecutorController.triggerWorkflowDefinition" in (
        executor_operations_text
    )
    assert "Form parameters for ExecutorController.triggerWorkflowDefinition." in (
        executor_operations_text
    )
    assert "disable-error-code=explicit-any" not in base_operations_text
    assert "import httpx" in base_operations_text
    assert "JsonScalar: TypeAlias = str | int | float | bool | None" in (
        base_operations_text
    )
    assert "    JsonValue as JsonValue," in base_operations_text
    assert "class RequestKwargs(TypedDict, total=False):" in base_operations_text
    assert "class ClientRequestKwargs(RequestKwargs, total=False):" in (
        base_operations_text
    )
    assert "RequestScalar: TypeAlias = str | int | float | bool | None" in (
        base_operations_text
    )
    assert 'RequestValue: TypeAlias = RequestScalar | list["RequestScalar"]' in (
        base_operations_text
    )
    assert "RequestMapping: TypeAlias = dict[str, RequestValue]" in (
        base_operations_text
    )
    assert "params: RequestMapping" in base_operations_text
    assert "json: JsonValue" in base_operations_text
    assert "data: RequestData" in base_operations_text
    assert "files: dict[str, UploadFileLike]" in base_operations_text
    assert 'JsonLike: TypeAlias = JsonScalar | Sequence["JsonLike"] | ' in (
        base_operations_text
    )
    assert "def _is_json_value(value: object) -> TypeGuard[JsonValue]:" in (
        base_operations_text
    )
    assert "def _require_json_value(value: object, *, label: str) -> JsonValue:" in (
        base_operations_text
    )
    assert (
        "def _require_json_object(value: object, *, label: str) -> JsonObject:"
        in base_operations_text
    )
    assert "def _require_request_mapping(" in base_operations_text
    assert "class ApiResultError(RuntimeError):" in base_operations_text
    assert "class ApiPayloadValidationError(RuntimeError):" in base_operations_text
    assert "class BinaryPayload(Protocol):" in base_operations_text
    assert (
        "class BinaryPayload(Protocol):\n"
        "    @property\n"
        "    def content(self) -> bytes: ...\n\n"
        "    @property\n"
        "    def headers(self) -> dict[str, str]: ...\n\n"
        "    @property\n"
        "    def content_type(self) -> str | None: ..."
    ) in base_operations_text
    assert "def request_binary(" in base_operations_text
    assert "def _request_binary(" in base_operations_text
    assert "class ResponseLike(" not in base_operations_text
    assert "class _RequestsSessionAdapter:" in base_operations_text
    assert "session = _RequestsSessionAdapter()" in base_operations_text
    assert "return self._unwrap_payload(payload)" in (base_operations_text)
    assert "def request(" in base_operations_text
    assert "    ) -> JsonValue: ..." in base_operations_text
    assert "def raise_payload_error(" in base_operations_text
    assert "def raise_result_error(" in base_operations_text
    assert (
        "def _request("
        "self, method: str, path: str, **kwargs: Unpack[ClientRequestKwargs]"
        ") -> JsonValue:" in base_operations_text
    )
    assert "def _model_mapping(self, value: BaseModel) -> RequestMapping:" in (
        base_operations_text
    )
    assert "def _json_payload(self, value: BaseModel | JsonLike) -> JsonValue:" in (
        base_operations_text
    )
    assert 'mode="json",' in base_operations_text
    assert (
        "def _validate_payload(self, value: object, adapter: TypeAdapter[T]) -> T:"
        in base_operations_text
    )
    assert 'T = TypeVar("T")' in base_operations_text
    assert "from pydantic import (" in base_operations_text
    assert "ValidationError," in base_operations_text
    assert "from typing import (" in base_operations_text
    assert "TypeGuard," in base_operations_text
    assert "TypedDict," in base_operations_text
    assert "Unpack," in base_operations_text
    assert "class BaseParamsModel(BaseModel):" in base_operations_text
    assert 'extra="forbid"' in base_operations_text
    assert "except ValidationError as error:" in base_operations_text
    assert "self._session.raise_payload_error(" in base_operations_text
    assert "headers.update(extra_headers)" in base_operations_text
    assert "return self._session.request(" in base_operations_text
    assert "return self._session.request_binary(" in base_operations_text
    assert "_require_json_value(" in base_operations_text
    assert 'label="response body"' in base_operations_text
    assert "_require_json_object(" in base_operations_text
    assert 'label="request payload"' in base_operations_text
    assert 'label="json payload"' in base_operations_text
    assert "raise ApiResultError(" in base_operations_text
    assert "from pydantic import Field, StrictInt, TypeAdapter" in (
        task_definition_operations_text
    )
    assert "TypeAdapter(list[StrictInt])" in task_definition_operations_text
    assert "TypeAdapter(StrictInt | None)" in task_definition_operations_text
    assert "TypeAdapter(list[int])" in executor_operations_text
    assert "def download_task_log_get_log_download_log(" in logger_operations_text
    assert ") -> BinaryPayload:" in logger_operations_text
    assert "return self._request_binary(" in logger_operations_text
    assert "TypeAdapter(bytes)" not in logger_operations_text
    assert "ApiPayloadValidationError, ApiResultError" in operations_init_text
    assert "query_params=query_params" not in project_v2_operations_text
    assert "params=query_params" in project_v2_operations_text
    assert "def create_project(" in project_v2_operations_text
    assert ") -> Project:" in project_v2_operations_text
    assert "-> list[Project]:" in project_v2_operations_text
    assert "def query_project_by_code(" in project_v2_operations_text
    assert "def update_project(" in project_v2_operations_text
    assert "def delete_project(" in project_v2_operations_text
    assert ") -> bool:" in project_v2_operations_text
    assert 'headers = {"Content-Type": "application/json"}' in (
        project_v2_operations_text
    )
    assert '"v2/projects",\n        params=query_params,\n        headers=headers,' in (
        project_v2_operations_text
    )
    assert "query_params=query_params" not in queue_v2_operations_text
    assert "params=query_params" in queue_v2_operations_text
    assert (
        "payload = self._project_single_data(payload)"
        in workflow_lineage_operations_text
    )
    assert "query_params=query_params" not in task_instance_v2_operations_text
    assert "params=query_params" in task_instance_v2_operations_text
    assert 'headers = {"Content-Type": "application/json"}' in (
        task_instance_v2_operations_text
    )
    assert "path,\n        params=query_params,\n        headers=headers," in (
        task_instance_v2_operations_text
    )
    assert "self._validate_payload(payload, TypeAdapter(EnvironmentDto))" in (
        environment_operations_text
    )
    sys.path.insert(0, str(package_output_root))
    try:
        package_client_module = importlib.import_module(
            "generated.versions.ds_3_4_1.client"
        )
        package_priority_module = importlib.import_module(
            "generated.versions.ds_3_4_1.common.enums.priority"
        )
        package_project_query_request_module = importlib.import_module(
            "generated.versions.ds_3_4_1.api.contracts.project.project_query_request"
        )
        package_environment_dto_module = importlib.import_module(
            "generated.versions.ds_3_4_1.api.contracts.environment_dto"
        )
        package_environment_operations_module = importlib.import_module(
            "generated.versions.ds_3_4_1.api.operations.environment"
        )
        package_db_type_module = importlib.import_module(
            "generated.versions.ds_3_4_1.spi.enums.db_type"
        )
        package_base_datasource_param_module = importlib.import_module(
            "generated.versions.ds_3_4_1.plugin.datasource_api.datasource."
            "base_data_source_param_dto"
        )
        package_base_operations_module = importlib.import_module(
            "generated.versions.ds_3_4_1.api.operations._base"
        )
        package_login_operations_module = importlib.import_module(
            "generated.versions.ds_3_4_1.api.operations.login"
        )
        package_workflow_lineage_operations_module = importlib.import_module(
            "generated.versions.ds_3_4_1.api.operations.workflow_lineage"
        )
        package_task_definition_operations_module = importlib.import_module(
            "generated.versions.ds_3_4_1.api.operations.task_definition"
        )
        package_workflow_definition_views_module = importlib.import_module(
            "generated.versions.ds_3_4_1.api.views.workflow_definition"
        )
        package_enums_module = importlib.import_module(
            "generated.versions.ds_3_4_1.common.enums"
        )
        package_views_module = importlib.import_module(
            "generated.versions.ds_3_4_1.api.views"
        )
        package_operations_module = importlib.import_module(
            "generated.versions.ds_3_4_1.api.operations"
        )
        assert hasattr(package_client_module, "DS341Client")
        assert package_priority_module.Priority.HIGH.code == 1
        assert (
            package_project_query_request_module.ProjectQueryRequest.__name__
            == "ProjectQueryRequest"
        )
        assert package_enums_module.Priority.HIGH.code == 1
        assert hasattr(
            package_workflow_definition_views_module,
            "WorkflowDefinitionSimpleItem",
        )
        assert hasattr(package_views_module, "WorkflowDefinitionSimpleItem")
        assert hasattr(package_operations_module, "ExecutorOperations")
        assert (
            package_operations_module.ApiResultError
            is package_base_operations_module.ApiResultError
        )
        issued_token = "-".join(["example", "token"])
        client = package_client_module.DS341Client(
            "http://example.test/dolphinscheduler",
            issued_token,
            session=object(),
        )
        assert isinstance(client, package_client_module.DS341Client)
        assert client.base_url == "http://example.test/dolphinscheduler"
        assert client.token == issued_token

        for invalid_task_code in (True, "1001", 1001.0):
            strict_task_code_session = _FakeSession([invalid_task_code])
            strict_task_code_client = package_client_module.DS341Client(
                "http://example.test/dolphinscheduler",
                issued_token,
                session=strict_task_code_session,
            )
            with pytest.raises(
                RuntimeError,
                match="Response payload did not match generated API contract",
            ) as exc_info:
                strict_task_code_client.task_definition.gen_task_code_list(
                    7,
                    package_task_definition_operations_module.GenTaskCodeListParams(
                        genNum=1
                    ),
                )
            assert isinstance(exc_info.value.__cause__, ValidationError)

        task_update_form = (
            package_task_definition_operations_module.UpdateTaskWithUpstreamParams(
                taskDefinitionJsonObj="{}"
            )
        )
        for valid_update_result in (1001, None):
            task_update_client = package_client_module.DS341Client(
                "http://example.test/dolphinscheduler",
                issued_token,
                session=_FakeSession(valid_update_result),
            )
            assert (
                task_update_client.task_definition.update_task_with_upstream(
                    7,
                    1001,
                    task_update_form,
                )
                is valid_update_result
            )

        for invalid_update_result in (True, "1001", 1001.0):
            task_update_client = package_client_module.DS341Client(
                "http://example.test/dolphinscheduler",
                issued_token,
                session=_FakeSession(invalid_update_result),
            )
            with pytest.raises(
                RuntimeError,
                match="Response payload did not match generated API contract",
            ) as exc_info:
                task_update_client.task_definition.update_task_with_upstream(
                    7,
                    1001,
                    task_update_form,
                )
            assert isinstance(exc_info.value.__cause__, ValidationError)

        fake_session = _FakeSession(
            {
                "id": 1,
                "code": 42,
                "name": "env-a",
                "config": "{}",
                "workerGroups": ["default"],
            }
        )
        runtime_client = package_client_module.DS341Client(
            "http://example.test/dolphinscheduler",
            issued_token,
            session=fake_session,
        )
        environment = runtime_client.environment.query_environment_by_code(
            package_environment_operations_module.QueryEnvironmentByCodeParams(
                environmentCode=42
            )
        )
        assert isinstance(environment, package_environment_dto_module.EnvironmentDto)
        assert environment.code == 42
        assert environment.workerGroups == ["default"]
        assert fake_session.calls[0]["params"] == {"environmentCode": 42}

        datasource_detail = (
            package_base_datasource_param_module.BaseDataSourceParamDTO.model_validate(
                {
                    "id": 7,
                    "name": "warehouse",
                    "type": "SSH",
                    "host": "db.example",
                    "privateKey": "******",
                }
            )
        )
        assert datasource_detail.type == package_db_type_module.DbType.SSH
        assert datasource_detail.model_dump(mode="json", by_alias=True) == {
            "id": 7,
            "name": "warehouse",
            "type": "SSH",
            "host": "db.example",
            "privateKey": "******",
            "note": None,
            "port": None,
            "database": None,
            "userName": None,
            "password": None,
            "other": None,
        }

        unwrapped_session = _FakeSession(
            {
                "code": "project-code",
                "msg": "raw message",
                "data": "keep",
            }
        )
        runtime_client = package_client_module.DS341Client(
            "http://example.test/dolphinscheduler",
            issued_token,
            session=unwrapped_session,
        )
        login_payload = runtime_client.login.login(
            package_login_operations_module.LoginParams(
                userName="demo",
                userPassword="secret",
            )
        )
        assert login_payload == {
            "code": "project-code",
            "msg": "raw message",
            "data": "keep",
        }

        projected_session = _FakeSession(
            {
                "status": "SUCCESS",
                "msg": "success",
                "data": [
                    {
                        "projectCode": 7,
                        "workerGroup": "default",
                    }
                ],
            }
        )
        runtime_client = package_client_module.DS341Client(
            "http://example.test/dolphinscheduler",
            issued_token,
            session=projected_session,
        )
        worker_groups = (
            runtime_client.project_worker_group.query_assigned_worker_groups(7)
        )
        assert [item.workerGroup for item in worker_groups] == ["default"]

        projected_lineage_session = _FakeSession(
            {
                "data": {
                    "workFlowRelationList": [
                        {
                            "sourceWorkFlowCode": 101,
                            "targetWorkFlowCode": 102,
                        }
                    ],
                    "workFlowRelationDetailList": [
                        {
                            "workFlowCode": 101,
                            "workFlowName": "daily-sync",
                            "schedulePublishStatus": 0,
                        }
                    ],
                }
            }
        )
        runtime_client = package_client_module.DS341Client(
            "http://example.test/dolphinscheduler",
            issued_token,
            session=projected_lineage_session,
        )
        lineage = runtime_client.workflow_lineage.query_work_flow_lineage(7)
        assert lineage.workFlowRelationList is not None
        assert lineage.workFlowRelationList[0].targetWorkFlowCode == 102

        projected_lineage_list_session = _FakeSession(
            {
                "data": [
                    {
                        "projectCode": 7,
                        "workflowDefinitionCode": 102,
                        "workflowDefinitionName": "quality-check",
                        "taskDefinitionCode": 302,
                        "taskDefinitionName": "depends-on-extract",
                    }
                ]
            }
        )
        runtime_client = package_client_module.DS341Client(
            "http://example.test/dolphinscheduler",
            issued_token,
            session=projected_lineage_list_session,
        )
        dependent_tasks = runtime_client.workflow_lineage.query_dependent_tasks(
            7,
            package_workflow_lineage_operations_module.QueryDependentTasksParams(
                workFlowCode=101
            ),
        )
        assert [task.taskDefinitionCode for task in dependent_tasks] == [302]

        class _FakeRawResponse:
            def raise_for_status(self) -> None:
                return None

            def json(self) -> object:
                return {"code": 10018, "msg": "project missing", "data": None}

        class _FakeRawHttpSession:
            def request(
                self,
                method: str,
                url: str,
                headers: dict[str, str],
                **kwargs: object,
            ) -> _FakeRawResponse:
                return _FakeRawResponse()

        session_adapter = package_base_operations_module._RequestsSessionAdapter(
            _FakeRawHttpSession()
        )
        with pytest.raises(package_base_operations_module.ApiResultError) as exc_info:
            session_adapter.request(
                "POST",
                "http://example.test/dolphinscheduler/login",
                headers={"token": issued_token},
                data={"userName": "demo", "userPassword": "secret"},
            )
        result_error = cast("Any", exc_info.value)
        assert result_error.code == 10018
        assert result_error.result_message == "project missing"
        assert result_error.data is None

        base_client = package_base_operations_module.BaseRequestsClient(
            "http://example.test/dolphinscheduler",
            issued_token,
            session=session_adapter,
        )
        with pytest.raises(
            package_base_operations_module.ApiPayloadValidationError
        ) as exc_info:
            base_client._validate_payload({}, TypeAdapter(int))
        payload_error = cast("Any", exc_info.value)
        assert payload_error.validation_error_count == 1
        assert isinstance(payload_error.__cause__, ValidationError)

        with pytest.raises(
            package_base_operations_module.ApiPayloadValidationError
        ) as exc_info:
            base_client._project_single_data({})
        payload_error = cast("Any", exc_info.value)
        assert payload_error.validation_error_count is None
        assert payload_error.__cause__ is None

        with pytest.raises(package_base_operations_module.ApiResultError) as exc_info:
            base_client._project_status_data(
                {"status": "FAILURE", "msg": "legacy failure", "data": []}
            )
        result_error = cast("Any", exc_info.value)
        assert result_error.code is None
        assert result_error.result_message == "legacy failure"
        assert result_error.data == []
    finally:
        sys.path.pop(0)
