"""Instance and logger exchanges in the jointly owned workflow runtime plan."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from ds_codegen.compatibility_impact import REVIEWED_DS_VERSIONS
from ds_codegen.compiled_domains import (
    CompiledPrimitive,
    CompiledRequestEpoch,
    CompiledResponsePolicy,
    require_model,
)
from ds_codegen.contract_visibility import is_client_supplied_parameter
from ds_codegen.operation_paths import executable_path_operation
from ds_codegen.runtime_instance_contract import (
    RUNTIME_INSTANCE_SEMANTIC_OPERATIONS,
    runtime_instance_contract,
    semantic_operation_sources,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ds_codegen.ir import ContractSnapshot, OperationSpec
    from dsctl.support.json_types import JsonValue

SEMANTIC_OPERATIONS = frozenset(RUNTIME_INSTANCE_SEMANTIC_OPERATIONS)
SEMANTIC_ABSENT_VERSIONS = {
    semantic: frozenset(
        version
        for version in REVIEWED_DS_VERSIONS
        if semantic not in semantic_operation_sources(version)
    )
    for semantic in SEMANTIC_OPERATIONS
    if any(semantic not in semantic_operation_sources(v) for v in REVIEWED_DS_VERSIONS)
}
_NAMES = (
    "instance_page",
    "instance_get",
    "instance_trigger",
    "instance_parent",
    "instance_sub",
    "instance_update_legacy",
    "instance_update",
    "instance_control",
    "instance_execute_task",
    "task_instance_page",
    "task_instance_force_success",
    "task_instance_savepoint",
    "task_instance_stop",
    "task_log",
)
_ENTITY = "org.apache.dolphinscheduler.dao.entity."
_PAGE = "org.apache.dolphinscheduler.api.utils.PageInfo"
_Fact = tuple[str, bool | None, str | None]


def classify(operation: OperationSpec) -> str | None:
    """Match the existing instance-controller and logger source identities."""
    if operation.operation_id in {
        "ProcessInstanceController.updateProcessInstance",
        "WorkflowInstanceController.updateWorkflowInstance",
    }:
        return (
            "instance_update_legacy"
            if operation.http_method == "POST"
            else "instance_update"
        )
    return {
        "ProcessInstanceController.queryProcessInstanceList": "instance_page",
        "WorkflowInstanceController.queryWorkflowInstanceList": "instance_page",
        "ProcessInstanceController.queryProcessInstancesByTriggerCode": (
            "instance_trigger"
        ),
        "WorkflowInstanceController.queryWorkflowInstancesByTriggerCode": (
            "instance_trigger"
        ),
        "ProcessInstanceController.queryProcessInstanceById": "instance_get",
        "WorkflowInstanceController.queryWorkflowInstanceById": "instance_get",
        "ProcessInstanceController.queryParentInstanceBySubId": "instance_parent",
        "WorkflowInstanceController.queryParentInstanceBySubId": "instance_parent",
        "ProcessInstanceController.querySubProcessInstanceByTaskId": "instance_sub",
        "WorkflowInstanceController.querySubWorkflowInstanceByTaskId": "instance_sub",
        "ExecutorController.execute": "instance_control",
        "ExecutorController.controlWorkflowInstance": "instance_control",
        "ExecutorController.executeTask": "instance_execute_task",
        "TaskInstanceController.queryTaskListPaging": "task_instance_page",
        "TaskInstanceController.forceTaskSuccess": "task_instance_force_success",
        "TaskInstanceController.taskSavePoint": "task_instance_savepoint",
        "TaskInstanceController.stopTask": "task_instance_stop",
        "LoggerController.queryLog": "task_log",
        "LoggerController.queryLog__get_log_detail": "task_log",
    }.get(operation.operation_id)


def _facts(version: str, primitive: str) -> dict[str, _Fact] | None:
    recipe = runtime_instance_contract(version)
    legacy = recipe.project_route == "name"
    workflow = recipe.workflow_controller == "WorkflowInstanceController"
    required = True if recipe.execute_task else None
    if primitive == "task_log":
        return dict.fromkeys(
            ("taskInstanceId", "skipLineNum", "limit"), ("int", required, None)
        )
    available = {
        "instance_update_legacy": legacy,
        "instance_update": not legacy,
        "instance_execute_task": recipe.execute_task,
        "instance_trigger": recipe.execute_task,
        "task_instance_force_success": recipe.force_success,
        "task_instance_savepoint": recipe.savepoint,
        "task_instance_stop": recipe.stop_task,
    }
    if not available.get(primitive, True):
        return None
    project = "projectName" if legacy else "projectCode"
    fields: dict[str, _Fact] = {
        project: ("String" if legacy else "long", required, None)
    }
    if primitive == "instance_trigger":
        fields["triggerCode"] = ("Long", True, None)
    elif primitive == "instance_control":
        if workflow:
            fields[project] = ("int", True, None)
        fields["workflowInstanceId" if workflow else "processInstanceId"] = (
            "Integer",
            required,
            None,
        )
        fields["executeType"] = (
            "org.apache.dolphinscheduler.api.enums.ExecuteType",
            required,
            None,
        )
    elif primitive == "instance_execute_task":
        fields.update(
            {
                "workflowInstanceId" if workflow else "processInstanceId": (
                    "Integer",
                    True,
                    None,
                ),
                "startNodeList": ("String", True, None),
                "taskDependType": (
                    "org.apache.dolphinscheduler.common.enums.TaskDependType",
                    True,
                    None,
                ),
            }
        )
    elif primitive.startswith("task_instance_") and primitive != "task_instance_page":
        if primitive == "task_instance_force_success":
            fields[project] = ("long", None, None)
        fields["id"] = ("Integer", required, None)
    elif primitive == "instance_get":
        fields["processInstanceId" if legacy else "id"] = ("Integer", required, None)
    elif primitive == "instance_parent":
        fields["subId"] = ("Integer", required, None)
    elif primitive == "instance_sub":
        fields["taskId"] = ("Integer", None, None)
    elif primitive == "instance_update_legacy":
        fields.update(
            processInstanceJson=("String", False, None),
            processInstanceId=("Integer", None, None),
            scheduleTime=("String", False, None),
            syncDefine=("Boolean", True, None),
            locations=("String", False, None),
            connects=("String", False, None),
            flag=("org.apache.dolphinscheduler.common.enums.Flag", False, None),
        )
    elif primitive == "instance_update":
        fields.update(
            taskRelationJson=("String", True, None),
            taskDefinitionJson=("String", True, None),
            id=("Integer", required, None),
            scheduleTime=("String", False, None),
            syncDefine=("Boolean", True, None),
            globalParams=("String", False, "[]"),
            locations=("String", False, None),
            timeout=("int", False, "0"),
        )
        if recipe.update_shape == "modern-with-tenant":
            fields["tenantCode"] = ("String", True, None)
        if version in {"2.0.0", "2.0.1", "2.0.2"}:
            fields["flag"] = (
                "org.apache.dolphinscheduler.common.enums.Flag",
                False,
                None,
            )
    else:
        fields.update(_page_fields(version, primitive))
    return fields


def _page_fields(version: str, primitive: str) -> dict[str, _Fact]:
    recipe = runtime_instance_contract(version)
    workflow = recipe.workflow_controller == "WorkflowInstanceController"
    required = True if recipe.execute_task else None
    fields: dict[str, _Fact] = {}
    if primitive == "instance_page":
        identity = (
            "processDefinitionId"
            if recipe.definition_identity == "id"
            else "workflowDefinitionCode"
            if workflow
            else "processDefineCode"
        )
        fields[identity] = (
            "Integer" if recipe.definition_identity == "id" else "long",
            False,
            "0",
        )
    else:
        fields["workflowInstanceId" if workflow else "processInstanceId"] = (
            "Integer",
            False,
            "0",
        )
        if recipe.task_workflow_name_filter:
            fields["workflowInstanceName" if workflow else "processInstanceName"] = (
                "String",
                False,
                None,
            )
        if recipe.task_definition_name_filter:
            fields[
                "workflowDefinitionName" if workflow else "processDefinitionName"
            ] = ("String", False, None)
    fields["searchVal"] = ("String", False, None)
    if primitive == "task_instance_page":
        fields["taskName"] = ("String", False, None)
        if recipe.task_code_filter:
            fields["taskCode"] = ("Long", False, None)
    fields.update(
        executorName=("String", False, None),
        stateType=(
            recipe.workflow_state_enum
            if primitive == "instance_page"
            else recipe.task_state_enum,
            False,
            None,
        ),
        host=("String", False, None),
        startDate=("String", False, None),
        endDate=("String", False, None),
    )
    if primitive == "instance_page" and recipe.other_workflow_filters:
        fields["otherParamsJson"] = ("String", False, None)
    if primitive == "task_instance_page" and recipe.task_execute_type_filter:
        if recipe.task_execute_type_enum is None:
            message = "reviewed task execute-type filter has no native enum"
            raise ValueError(message)
        fields["taskExecuteType"] = (recipe.task_execute_type_enum, False, "BATCH")
    fields.update(
        pageNo=("Integer", required, None), pageSize=("Integer", required, None)
    )
    return fields


def _request(version: str, primitive: str) -> CompiledRequestEpoch | None:
    fields = _facts(version, primitive)
    if fields is None:
        return None
    recipe = runtime_instance_contract(version)
    legacy = recipe.project_route == "name"
    project = "projectName" if legacy else "projectCode"
    scope = f"projects/{{{project}}}"
    family = (
        "workflow"
        if recipe.workflow_controller == "WorkflowInstanceController"
        else "process"
    )
    root = f"{scope}/instance" if legacy else f"{scope}/{family}-instances"
    method: Literal["GET", "POST", "PUT"] = "GET"
    channel: Literal["query", "path", "path_query", "path_form"] = "path_query"
    paths: tuple[str, ...] = (project,)
    path = root
    if primitive == "instance_page":
        path += "/list-paging" if legacy else ""
    elif primitive == "instance_trigger":
        path += "/trigger"
    elif primitive == "instance_get":
        path += "/select-by-id" if legacy else "/{id}"
        if not legacy:
            channel, paths = "path", (project, "id")
    elif primitive == "instance_parent":
        path += "/select-parent-process" if legacy else "/query-parent-by-sub"
    elif primitive == "instance_sub":
        path += "/select-sub-process" if legacy else "/query-sub-by-parent"
    elif primitive == "instance_update_legacy":
        path, method, channel = f"{root}/update", "POST", "path_form"
    elif primitive == "instance_update":
        path, method, channel, paths = (
            f"{root}/{{id}}",
            "PUT",
            "path_form",
            (project, "id"),
        )
    elif primitive in {"instance_control", "instance_execute_task"}:
        suffix = "execute" if primitive == "instance_control" else "execute-task"
        path, method, channel = f"{scope}/executors/{suffix}", "POST", "path_form"
    elif primitive == "task_instance_page":
        path = (
            f"{scope}/task-instance/list-paging"
            if legacy
            else f"{scope}/task-instances"
        )
    elif primitive == "task_log":
        path, channel, paths = "log/detail", "query", ()
    else:
        suffix = {
            "task_instance_force_success": "force-success",
            "task_instance_savepoint": "savepoint",
            "task_instance_stop": "stop",
        }[primitive]
        path, method, channel, paths = (
            f"{scope}/task-instances/{{id}}/{suffix}",
            "POST",
            "path",
            (project, "id"),
        )
    return CompiledRequestEpoch(
        method=method,
        path=path,
        channel=channel,
        request_schema=primitive,
        request_model="".join(word.title() for word in primitive.split("_")) + "Params",
        request_fields=tuple(fields),
        path_fields=paths,
        versions=frozenset({version}),
        path_encoding="legacy-url-interpolation-v1"
        if legacy and paths
        else "percent-encoded-utf8-segment-v1",
        content_addressed=True,
    )


def _primitive(name: str) -> CompiledPrimitive:
    return CompiledPrimitive(
        name=name,
        requests=tuple(
            request
            for version in REVIEWED_DS_VERSIONS
            if (request := _request(version, name)) is not None
        ),
        result_envelope="optional"
        if name
        in {
            "instance_page",
            "instance_trigger",
            "instance_get",
            "instance_parent",
            "instance_sub",
            "instance_update_legacy",
            "task_instance_page",
            "task_log",
        }
        else "required",
        absent_versions=frozenset(
            version for version in REVIEWED_DS_VERSIONS if _facts(version, name) is None
        ),
    )


PRIMITIVES = tuple(_primitive(name) for name in _NAMES)


def _response_root(version: str, primitive: str) -> str:
    recipe = runtime_instance_contract(version)
    if primitive == "instance_page":
        return f"{_PAGE}<{recipe.workflow_summary_model or recipe.workflow_model}>"
    if primitive == "instance_trigger":
        return f"List<{recipe.workflow_summary_model or recipe.workflow_model}>"
    if primitive == "instance_get":
        return recipe.workflow_model
    if primitive == "instance_update":
        model = recipe.workflow_definition_model
        return (
            "Optional<ProcessDefinition>"
            if recipe.instance_dag_edit_requires_sync
            else model
        )
    if primitive == "task_instance_page":
        row = (
            f"{_ENTITY}TaskInstance"
            if recipe.task_page_shape == "entity"
            else "Map<String, Object>"
        )
        return f"{_PAGE}<{row}>"
    if primitive == "task_log":
        return "String" if recipe.log_shape == "string" else f"{_ENTITY}ResponseTaskLog"
    if primitive in {"instance_parent", "instance_sub"}:
        if version in {"3.4.2", "3.4.3"}:
            return "Map<String, Integer>"
        service = recipe.workflow_controller.removesuffix("Controller") + "Service"
        if version != "1.3.9":
            service += "Impl"
        method = (
            recipe.workflow_parent_operation
            if primitive == "instance_parent"
            else recipe.workflow_sub_operation
        ).split(".")[1]
        return f"{service}_{method}_dataMap"
    return "Void"


def response_policy(
    snapshot: ContractSnapshot, operation: OperationSpec, primitive: str
) -> CompiledResponsePolicy:
    """Keep reviewed native roles; the renderer owns complete closure identity."""
    version = snapshot.ds_version
    actual = {
        parameter.wire_name: (
            parameter.java_type,
            parameter.required,
            parameter.default_value,
        )
        for parameter in executable_path_operation(operation).parameters
        if is_client_supplied_parameter(parameter)
    }
    if actual != _facts(version, primitive) or operation.consumes:
        message = f"compiled workflow {primitive} request fields changed"
        raise ValueError(message)
    expected = _response_root(version, primitive)
    if (
        operation.logical_return_type != expected
        or operation.response_projection != "direct"
    ):
        message = f"compiled workflow {primitive} response role changed"
        raise ValueError(message)
    _require_consumed_fields(snapshot, primitive)
    capture: JsonValue = {} if expected != "Void" else None
    if expected == "String":
        capture = ""
    elif primitive == "instance_trigger":
        capture = []
    return CompiledResponsePolicy(
        codec=f"{primitive}_{version.replace('.', '_')}",
        schema=None if expected == "Void" else primitive,
        capture=capture,
        content_addressed=expected != "Void",
    )


def _require_consumed_fields(snapshot: ContractSnapshot, primitive: str) -> None:
    recipe = runtime_instance_contract(snapshot.ds_version)
    roles: dict[str, set[str]] = {}
    root: str | None = None
    if primitive in {"instance_page", "instance_get", "instance_trigger"}:
        root = (
            recipe.workflow_summary_model
            if primitive != "instance_get" and recipe.workflow_summary_model is not None
            else recipe.workflow_model
        )
        roles = {"id": {"int", "Integer"}}
        if primitive == "instance_get" and recipe.definition_identity == "code":
            roles.update({"globalParams": {"String"}, "timeout": {"int"}})
    elif primitive == "instance_update":
        root, roles = (
            recipe.workflow_definition_model,
            {"code": {"long"}, "version": {"int"}, "name": {"String"}},
        )
    elif primitive == "task_instance_page" and recipe.task_page_shape == "entity":
        identity = (
            "workflowInstanceId"
            if recipe.workflow_controller == "WorkflowInstanceController"
            else "processInstanceId"
        )
        root, roles = (
            f"{_ENTITY}TaskInstance",
            {"id": {"int", "Integer"}, identity: {"int", "Integer"}},
        )
    elif primitive == "task_log" and recipe.log_shape != "string":
        root, roles = (
            f"{_ENTITY}ResponseTaskLog",
            {"lineNum": {"int"}, "message": {"String"}},
        )
    elif primitive in {
        "instance_parent",
        "instance_sub",
    } and snapshot.ds_version not in {"3.4.2", "3.4.3"}:
        root = "generated.view." + _response_root(snapshot.ds_version, primitive)
        roles = {
            "parentWorkflowInstance"
            if primitive == "instance_parent"
            else recipe.workflow_sub_result_key: {"int", "Integer"}
        }
    if root is not None:
        model = require_model(snapshot, root, domain="workflow_runtime")
        fields = {field.name: field.java_type for field in model.fields}
        if any(fields.get(name) not in types for name, types in roles.items()):
            message = f"compiled workflow {primitive} consumed response fields changed"
            raise ValueError(message)


def recipe_policy(codecs: Mapping[str, str]) -> str:
    """Accept only one complete existing exact runtime-instance recipe."""
    selected = {name: codec for name, codec in codecs.items() if name in _NAMES}
    for version in REVIEWED_DS_VERSIONS:
        stamp = version.replace(".", "_")
        expected = {
            name: f"{name}_{stamp}"
            for name in _NAMES
            if _facts(version, name) is not None
        }
        if selected == expected:
            return stamp
    message = "compiled workflow instance codecs do not form an exact reviewed recipe"
    raise ValueError(message)
