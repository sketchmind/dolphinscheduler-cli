"""Task-definition exchanges owned by the joint workflow runtime compiler."""

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
from ds_codegen.task_definition_cleanup_contract import (
    TASK_DEFINITION_CLEANUP_SEMANTIC_OPERATION,
    TASK_DEFINITION_CLEANUP_VERSIONS,
    task_definition_cleanup_contract,
)
from ds_codegen.task_definition_contract import (
    TASK_SEMANTIC_OPERATIONS,
    task_definition_contract,
    task_top_level_field_policy,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ds_codegen.ir import ContractSnapshot, OperationSpec
    from dsctl.support.json_types import JsonValue

SEMANTIC_OPERATIONS = frozenset(
    (*TASK_SEMANTIC_OPERATIONS, TASK_DEFINITION_CLEANUP_SEMANTIC_OPERATION)
)
SEMANTIC_ABSENT_VERSIONS = {
    TASK_DEFINITION_CLEANUP_SEMANTIC_OPERATION: frozenset(REVIEWED_DS_VERSIONS)
    - frozenset(TASK_DEFINITION_CLEANUP_VERSIONS)
}
_ROOT = "projects/{projectCode}/task-definition"
_DETAIL = f"{_ROOT}/{{code}}"
_PAGE = "org.apache.dolphinscheduler.api.utils.PageInfo"
_TYPED_REQUIRED = frozenset(REVIEWED_DS_VERSIONS[REVIEWED_DS_VERSIONS.index("3.2.0") :])
_NAMES = (
    "task_code_allocate",
    "task_get",
    "task_update",
    "task_cleanup_page",
    "task_cleanup_history",
    "task_cleanup_release",
    "task_cleanup_delete",
)
_Fact = tuple[str, bool | None, str | None]


def classify(operation: OperationSpec) -> str | None:
    """Classify only the task source operations in the reviewed joint closure."""
    return {
        "TaskDefinitionController.genTaskCodeList": "task_code_allocate",
        "TaskDefinitionController.queryTaskDefinitionDetail": "task_get",
        "TaskDefinitionController.updateTaskDefinition": "task_update",
        "TaskDefinitionController.updateTaskWithUpstream": "task_update",
        "TaskDefinitionController.queryTaskDefinitionListPaging": "task_cleanup_page",
        "TaskDefinitionController.queryTaskDefinitionVersions": "task_cleanup_history",
        "TaskDefinitionController.releaseTaskDefinition": "task_cleanup_release",
        "TaskDefinitionController.deleteTaskDefinitionByCode": "task_cleanup_delete",
    }.get(operation.operation_id)


def _facts(version: str, primitive: str) -> dict[str, _Fact] | None:
    task = task_definition_contract(version).task
    if not task.executable:
        return None
    annotated = True if version in _TYPED_REQUIRED else None
    fields: dict[str, _Fact] = {"projectCode": ("long", annotated, None)}
    if primitive == "task_code_allocate":
        # The method omits its inherited route parameter; native rendering adds it.
        fields["projectCode"] = ("int", True, None)
        fields["genNum"] = ("Integer", annotated, None)
        return fields
    if primitive == "task_update" and not task.update_executable:
        return None
    if primitive.startswith("task_cleanup_"):
        if version not in TASK_DEFINITION_CLEANUP_VERSIONS:
            return None
        cleanup = task_definition_cleanup_contract(version)
        if primitive == "task_cleanup_delete" and cleanup.strategy != "direct-delete":
            return None
        if (
            primitive == "task_cleanup_release"
            and cleanup.pre_delete_release != "offline"
        ):
            return None
        if primitive == "task_cleanup_history" and cleanup.history_model is None:
            return None
        if primitive == "task_cleanup_page":
            if cleanup.page_params_epoch == "task-search":
                fields.update(
                    taskType=("String", False, None),
                    searchVal=("String", False, None),
                    userId=("Integer", False, "0"),
                )
            else:
                fields.update(
                    searchWorkflowName=("String", False, None),
                    searchTaskName=("String", False, None),
                    taskType=("String", False, None),
                )
                if cleanup.page_params_epoch == "execute-type":
                    fields["taskExecuteType"] = (
                        "org.apache.dolphinscheduler.common.enums.TaskExecuteType",
                        False,
                        "BATCH",
                    )
            fields.update(
                pageNo=("Integer", annotated, None),
                pageSize=("Integer", annotated, None),
            )
            return fields
    fields["code"] = ("long", annotated, None)
    if primitive == "task_update":
        fields["taskDefinitionJsonObj"] = ("String", True, None)
        if task.dependency_update:
            fields["upstreamCodes"] = ("String", False, None)
    elif primitive == "task_cleanup_history":
        fields.update(pageNo=("int", None, None), pageSize=("int", None, None))
    elif primitive == "task_cleanup_release":
        fields["code"] = ("long", True, None)
        fields["releaseState"] = (
            "org.apache.dolphinscheduler.common.enums.ReleaseState",
            True,
            "OFFLINE",
        )
    return fields


def _request(version: str, primitive: str) -> CompiledRequestEpoch | None:
    fields = _facts(version, primitive)
    if fields is None:
        return None
    task = task_definition_contract(version).task
    path = _DETAIL
    method: Literal["GET", "POST", "PUT", "DELETE"] = "GET"
    channel: Literal["path", "path_query", "path_form"] = "path"
    paths: tuple[str, ...] = ("projectCode", "code")
    if primitive == "task_code_allocate":
        path, channel, paths = f"{_ROOT}/gen-task-codes", "path_query", ("projectCode",)
    elif primitive == "task_update":
        method, channel = "PUT", "path_form"
        if task.dependency_update:
            path += "/with-upstream"
    elif primitive == "task_cleanup_page":
        path, channel, paths = _ROOT, "path_query", ("projectCode",)
    elif primitive == "task_cleanup_history":
        path, channel = f"{_DETAIL}/versions", "path_query"
    elif primitive == "task_cleanup_release":
        path, method, channel = f"{_DETAIL}/release", "POST", "path_form"
    elif primitive == "task_cleanup_delete":
        method = "DELETE"
    return CompiledRequestEpoch(
        method=method,
        path=path,
        channel=channel,
        request_schema=primitive,
        request_model="".join(word.title() for word in primitive.split("_")) + "Params",
        request_fields=tuple(fields),
        path_fields=paths,
        versions=frozenset({version}),
        content_addressed=True,
    )


def _primitive(name: str) -> CompiledPrimitive:
    requests = tuple(
        request
        for version in REVIEWED_DS_VERSIONS
        if (request := _request(version, name)) is not None
    )
    return CompiledPrimitive(
        name=name,
        requests=requests,
        result_envelope="optional",
        absent_versions=frozenset(
            version for version in REVIEWED_DS_VERSIONS if _facts(version, name) is None
        ),
    )


PRIMITIVES = tuple(_primitive(name) for name in _NAMES)


def response_policy(
    snapshot: ContractSnapshot, operation: OperationSpec, primitive: str
) -> CompiledResponsePolicy:
    """Guard consumed task roles, leaving full schema identity to the renderer."""
    version = snapshot.ds_version
    fields = _facts(version, primitive)
    actual = {
        parameter.wire_name: (
            parameter.java_type,
            parameter.required,
            parameter.default_value,
        )
        for parameter in executable_path_operation(operation).parameters
        if is_client_supplied_parameter(parameter)
    }
    if fields is None or actual != fields or operation.consumes:
        message = f"compiled workflow {primitive} request fields changed"
        raise ValueError(message)
    task = task_definition_contract(version).task
    expected = task.detail_model
    capture: JsonValue = {}
    if primitive == "task_code_allocate":
        expected = "List<long>" if version in {"3.4.2", "3.4.3"} else "List<Long>"
        capture = []
    elif primitive == "task_update":
        expected = "Optional<long>" if task.update_response_optional else "long"
        if version in {"3.4.2", "3.4.3"}:
            expected = "Long"
        capture = None if task.update_response_optional else 1
    elif primitive.startswith("task_cleanup_"):
        cleanup = task_definition_cleanup_contract(version)
        if primitive == "task_cleanup_page":
            expected = f"{_PAGE}<{cleanup.page_model}>"
            row = require_model(snapshot, cleanup.page_model, domain="workflow_runtime")
            names = {field.name: field.java_type for field in row.fields}
            code, name, revision = cleanup.row_fields
            if (
                names.get(code) not in {"long", "Long"}
                or names.get(name) != "String"
                or names.get(revision) not in {"int", "Integer"}
            ):
                message = "compiled cleanup row identity fields changed"
                raise ValueError(message)
        elif primitive == "task_cleanup_history":
            expected = f"{_PAGE}<{cleanup.history_model}>"
        elif primitive == "task_cleanup_release":
            expected = "Void"
            capture = None
        else:
            expected = (
                "Void"
                if cleanup.delete_response == "void"
                else "Optional<ProcessDefinition>"
            )
            capture = None
    if (
        operation.logical_return_type != expected
        or operation.response_projection != "direct"
    ):
        message = f"compiled workflow {primitive} response role changed"
        raise ValueError(message)
    if primitive == "task_get":
        model = require_model(snapshot, task.detail_model, domain="workflow_runtime")
        names = {field.name: field.java_type for field in model.fields}
        policy = task_top_level_field_policy(version)
        if (
            set(names) - policy.classified_fields
            or set(names).intersection(task.computed_response_fields)
            or names.get("code") != "long"
            or names.get("projectCode") != "long"
            or names.get("version") != "int"
            or names.get("taskParams") != "JsonValue"
        ):
            message = "compiled task detail field policy or native identity changed"
            raise ValueError(message)
    return CompiledResponsePolicy(
        codec=f"{primitive}_{version.replace('.', '_')}",
        schema=None if expected == "Void" else primitive,
        capture=capture,
        content_addressed=expected != "Void",
    )


def recipe_policy(codecs: Mapping[str, str]) -> str:
    """Accept only one complete existing exact task/cleanup recipe."""
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
    message = "compiled workflow task codecs do not form an exact reviewed recipe"
    raise ValueError(message)
