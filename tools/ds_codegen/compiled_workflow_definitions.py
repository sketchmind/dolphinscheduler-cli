"""Reviewed definition/executor fragment of the shared workflow source owner."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from ds_codegen.compatibility_impact import REVIEWED_DS_VERSIONS
from ds_codegen.compiled_domains import (
    CompiledPrimitive,
    CompiledRequestEpoch,
    CompiledResponsePolicy,
    require_model,
)
from ds_codegen.contract_visibility import (
    is_client_supplied_parameter,
    is_required_parameter,
)
from ds_codegen.operation_paths import executable_path_operation
from ds_codegen.workflow_contract import (
    WARNING_GROUP_DEFAULT_ZERO_VERSIONS,
    WARNING_GROUP_PRIMITIVE_VERSIONS,
    workflow_contract,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ds_codegen.ir import ContractSnapshot, OperationSpec

SEMANTIC_OPERATIONS = frozenset(
    {
        "workflow.page",
        "workflow.get",
        "workflow.inspect",
        "workflow.create",
        "workflow.edit",
        "workflow.delete",
        "workflow.online",
        "workflow.offline",
        "workflow.run",
        "workflow.run-task",
        "workflow.backfill",
        "workflow.describe",
        "workflow.digest",
        "workflow.export",
    }
)
SEMANTIC_ABSENT_VERSIONS = {
    "workflow.inspect": frozenset(REVIEWED_DS_VERSIONS) - {"3.4.2"},
}
_NAMES = (
    "definition_refs",
    "definition_page",
    "definition_get",
    "definition_create",
    "definition_update",
    "definition_delete",
    "definition_release",
    "workflow_execute",
)
_ENUM = "org.apache.dolphinscheduler.common.enums."
_ENTITY = "org.apache.dolphinscheduler.dao.entity."
_EARLY = frozenset(
    {
        "1.3.9",
        "2.0.0",
        "2.0.1",
        "2.0.2",
        "2.0.3",
        "2.0.4",
        "2.0.5",
        "2.0.6",
        "2.0.7",
        "2.0.8",
        "2.0.9",
    }
)
_ZERO_ID = _EARLY | {"3.0.0", "3.0.1", "3.0.2", "3.0.3", "3.0.4", "3.0.5", "3.0.6"}
_IMPLICIT_EXECUTOR_PROJECT = frozenset({"3.3.1", "3.3.2", "3.4.0", "3.4.1"})
_Field = tuple[str, str, bool, str | None]


def _field(
    name: str,
    java_type: str = "String",
    *,
    required: bool = False,
    default: str | None = None,
) -> _Field:
    return name, java_type, required, default


def _fields(version: str, primitive: str) -> tuple[_Field, ...]:
    """Project the reviewed recipe into the finite native request vocabulary."""
    contract = workflow_contract(version)
    definition = contract.definition
    legacy = definition.family == "legacy-json"
    project = (
        _field("projectName", required=True)
        if legacy
        else _field("projectCode", "long", required=True)
    )
    if primitive == "definition_refs":
        return (project,)
    if primitive in {"definition_get", "definition_delete"}:
        identity = "code"
        if legacy:
            identity = (
                "processId" if primitive == "definition_get" else "processDefinitionId"
            )
        return project, _field(identity, "Integer" if legacy else "long", required=True)
    if primitive == "definition_page":
        if legacy:
            return (
                project,
                _field("pageNo", "Integer", required=True),
                _field("searchVal"),
                _field("userId", "Integer", default="0"),
                _field("pageSize", "Integer", required=True),
            )
        fields = [project, _field("searchVal")]
        if definition.create_other_params:
            fields.append(_field("otherParamsJson"))
        return (
            *fields,
            _field("userId", "Integer", default="0"),
            _field("pageNo", "Integer", required=True),
            _field("pageSize", "Integer", required=True),
        )
    if primitive == "definition_release":
        return (
            project,
            _field(
                "processId" if legacy else "code",
                "int" if legacy else "long",
                required=True,
            ),
            _field(
                "releaseState",
                "int" if legacy else _ENUM + "ReleaseState",
                required=True,
            ),
        )
    if primitive in {"definition_create", "definition_update"}:
        update = primitive == "definition_update"
        fields = [project, _field("name", required=True)]
        if legacy:
            if update:
                fields.append(_field("id", "int", required=True))
            return (
                *fields,
                _field("processDefinitionJson", required=True),
                _field("locations", required=not update),
                _field("connects", required=not update),
                _field("description"),
            )
        if update:
            fields.append(_field("code", "long", required=True))
        fields.extend(
            (
                _field("description"),
                _field("globalParams", default="[]"),
                _field("locations"),
                _field("timeout", "int", default="0"),
            )
        )
        if definition.tenant_code:
            fields.append(_field("tenantCode", required=True))
        fields.extend(
            (
                _field("taskRelationJson", required=True),
                _field("taskDefinitionJson", required=True),
            )
        )
        other = (
            definition.update_other_params if update else definition.create_other_params
        )
        if other:
            fields.append(_field("otherParamsJson"))
        if definition.execution_type:
            enum = (
                "WorkflowExecutionTypeEnum"
                if definition.family == "workflow"
                else "ProcessExecutionTypeEnum"
            )
            fields.append(_field("executionType", _ENUM + enum, default="PARALLEL"))
        if update:
            fields.append(
                _field("releaseState", _ENUM + "ReleaseState", default="OFFLINE")
            )
        return tuple(fields)
    if primitive != "workflow_execute":
        msg = f"Unknown workflow definition primitive {primitive}"
        raise ValueError(msg)
    execution = contract.execution
    workflow = definition.family == "workflow"
    if version in _IMPLICIT_EXECUTOR_PROJECT:
        project = _field("projectCode", "int", required=True)
    identity = "workflowDefinitionCode" if workflow else "processDefinitionCode"
    fields = [
        project,
        _field(
            "processDefinitionId" if legacy else identity,
            "int" if legacy else "long",
            required=True,
        ),
        _field("scheduleTime", required=version not in _EARLY),
        _field("failureStrategy", _ENUM + "FailureStrategy", required=True),
        _field("startNodeList"),
        _field(
            "taskDependType",
            _ENUM + "TaskDependType",
            default="TASK_POST" if workflow else None,
        ),
        _field(
            "execType",
            _ENUM + "CommandType",
            default="START_PROCESS" if workflow else None,
        ),
        _field("warningType", _ENUM + "WarningType", required=True),
        _field(
            "warningGroupId",
            "int" if version in WARNING_GROUP_PRIMITIVE_VERSIONS else "Integer",
            default=("0" if version in WARNING_GROUP_DEFAULT_ZERO_VERSIONS else None),
        ),
    ]
    if legacy:
        fields.extend((_field("receivers"), _field("receiversCc")))
    fields.extend(
        (
            _field("runMode", _ENUM + "RunMode"),
            _field(
                "workflowInstancePriority" if workflow else "processInstancePriority",
                _ENUM + "Priority",
            ),
            _field("workerGroup", default="default"),
        )
    )
    if execution.tenant_code:
        fields.append(_field("tenantCode", default="default"))
    if execution.environment_code:
        fields.append(_field("environmentCode", "Long", default="-1"))
    if execution.timeout:
        fields.append(_field("timeout", "Integer"))
    if execution.start_params:
        fields.append(_field("startParams"))
    if execution.expected_parallelism_number:
        fields.append(_field("expectedParallelismNumber", "Integer"))
    if execution.execution_dry_run:
        fields.append(_field("dryRun", "int", default="0"))
    if execution.test_flag:
        fields.append(_field("testFlag", "int", default="0"))
    if execution.complement_dependent_mode:
        fields.append(
            _field("complementDependentMode", _ENUM + "ComplementDependentMode")
        )
    if execution.version:
        fields.append(_field("version", "Integer"))
    if execution.all_level_dependent:
        fields.append(_field("allLevelDependent", "boolean", default="false"))
    if execution.execution_order:
        fields.append(_field("executionOrder", _ENUM + "ExecutionOrder"))
    return tuple(fields)


def _request(version: str, primitive: str) -> CompiledRequestEpoch:
    definition = workflow_contract(version).definition
    legacy = definition.family == "legacy-json"
    fields = _fields(version, primitive)
    project = "projectName" if legacy else "projectCode"
    base = f"projects/{{{project}}}/"
    noun = (
        "workflow-definition"
        if definition.family == "workflow"
        else "process-definition"
    )
    path = base + ("process" if legacy else noun)
    method: Literal["GET", "POST", "PUT", "DELETE"] = "GET"
    path_fields: tuple[str, ...] = (project,)
    if primitive == "definition_refs":
        path += "/list" if legacy else "/simple-list"
    elif primitive == "definition_page":
        path += "/list-paging" if legacy else ""
    elif primitive in {"definition_get", "definition_delete"}:
        if legacy:
            path += "/select-by-id" if primitive == "definition_get" else "/delete"
        else:
            path += "/{code}"
            path_fields = project, "code"
            method = "GET" if primitive == "definition_get" else "DELETE"
    elif primitive == "definition_create":
        method = "POST"
        path += "/save" if legacy else ""
    elif primitive == "definition_update":
        method = "POST" if legacy else "PUT"
        path += "/update" if legacy else "/{code}"
        if not legacy:
            path_fields = project, "code"
    elif primitive == "definition_release":
        method = "POST"
        path += "/release" if legacy else "/{code}/release"
        if not legacy:
            path_fields = project, "code"
    elif primitive == "workflow_execute":
        method = "POST"
        path = (
            base
            + "executors/"
            + (
                "start-workflow-instance"
                if definition.family == "workflow"
                else "start-process-instance"
            )
        )
    channel: Literal["path", "path_query", "path_form"] = "path"
    if len(fields) != len(path_fields):
        channel = "path_query" if method == "GET" else "path_form"
    schema = primitive
    return CompiledRequestEpoch(
        method=method,
        path=path,
        channel=channel,
        request_schema=schema,
        request_model="".join(part.title() for part in schema.split("_")) + "Params",
        request_fields=tuple(item[0] for item in fields),
        path_fields=path_fields,
        required_fields=frozenset(item[0] for item in fields if item[2]),
        versions=frozenset({version}),
        path_encoding="legacy-url-interpolation-v1"
        if legacy
        else "percent-encoded-utf8-segment-v1",
        content_addressed=True,
    )


PRIMITIVES = tuple(
    CompiledPrimitive(
        name=name,
        requests=tuple(_request(version, name) for version in REVIEWED_DS_VERSIONS),
        result_envelope="required" if name == "definition_delete" else "optional",
    )
    for name in _NAMES
)


def classify(operation: OperationSpec) -> str | None:
    for version in REVIEWED_DS_VERSIONS:
        contract = workflow_contract(version)
        definition = contract.definition
        sources = {
            definition.detail_operation: "definition_get",
            definition.create_operation: "definition_create",
            definition.update_operation: "definition_update",
            definition.delete_operation: "definition_delete",
            definition.release_operation: "definition_release",
            contract.execution.operation: "workflow_execute",
        }
        if operation.operation_id in sources:
            return sources[operation.operation_id]
    return {
        "ProcessDefinitionController.queryProcessDefinitionList": "definition_refs",
        "ProcessDefinitionController.queryProcessDefinitionSimpleList": (
            "definition_refs"
        ),
        "WorkflowDefinitionController.queryWorkflowDefinitionSimpleList": (
            "definition_refs"
        ),
        "ProcessDefinitionController.queryProcessDefinitionListPaging": (
            "definition_page"
        ),
        "WorkflowDefinitionController.queryWorkflowDefinitionListPaging": (
            "definition_page"
        ),
    }.get(operation.operation_id)


def _require_fields(
    snapshot: ContractSnapshot, name: str, expected: Mapping[str, str]
) -> None:
    model = require_model(snapshot, name, domain="workflow_runtime")
    fields = {item.name: item.java_type for item in model.fields}
    if any(fields.get(key) != value for key, value in expected.items()):
        msg = f"compiled workflow consumed response fields changed: {name}"
        raise ValueError(msg)


def _require_definition(snapshot: ContractSnapshot) -> None:
    contract = workflow_contract(snapshot.ds_version).definition
    legacy = contract.family == "legacy-json"
    noun = (
        "WorkflowDefinition" if contract.family == "workflow" else "ProcessDefinition"
    )
    fields = dict.fromkeys(
        ("name", "description", "globalParams", "userName", "projectName"), "String"
    )
    fields.update(
        {
            "createTime": "Date",
            "updateTime": "Date",
            "globalParamMap": "Map<String, String>",
        }
    )
    if legacy:
        fields.update(dict.fromkeys(("id", "projectId", "userId", "timeout"), "int"))
        fields.update(
            dict.fromkeys(
                (
                    "processDefinitionJson",
                    "locations",
                    "connects",
                    "receivers",
                    "receiversCc",
                ),
                "String",
            )
        )
    else:
        fields.update({"code": "long", "projectCode": "long"})
        fields.update(dict.fromkeys(("version", "timeout", "userId"), "int"))
        fields["id"] = "int" if snapshot.ds_version in _ZERO_ID else "Integer"
    fields["releaseState"] = _ENUM + "ReleaseState"
    fields["scheduleReleaseState"] = _ENUM + "ReleaseState"
    if contract.execution_type:
        fields["executionType"] = _ENUM + (
            "WorkflowExecutionTypeEnum"
            if contract.family == "workflow"
            else "ProcessExecutionTypeEnum"
        )
    if contract.release_result == "boolean":
        fields["schedule"] = _ENTITY + "Schedule"
    _require_fields(snapshot, _ENTITY + noun, fields)


def response_policy(
    snapshot: ContractSnapshot, operation: OperationSpec, primitive: str
) -> CompiledResponsePolicy:
    version = snapshot.ds_version
    contract = workflow_contract(version)
    actual = tuple(
        (
            parameter.wire_name,
            parameter.java_type,
            is_required_parameter(parameter),
            parameter.default_value,
        )
        for parameter in executable_path_operation(operation).parameters
        if is_client_supplied_parameter(parameter)
    )
    if actual != _fields(version, primitive):
        msg = f"compiled workflow {primitive} request field contract changed"
        raise ValueError(msg)
    if operation.response_projection != "direct":
        msg = f"compiled workflow {primitive} response projection changed"
        raise ValueError(msg)
    legacy = contract.definition.family == "legacy-json"
    noun = (
        "WorkflowDefinition"
        if contract.definition.family == "workflow"
        else "ProcessDefinition"
    )
    logical = operation.logical_return_type
    codec = f"{primitive}_{version.replace('.', '_')}"
    if primitive == "definition_delete" or (
        primitive == "definition_update" and legacy
    ):
        expected = {"Void", "void"}
    elif primitive == "workflow_execute":
        expected = {
            "none": {"Void", "void"},
            "trigger-code": {"long"},
            "id-list": {"List<Integer>"},
        }[contract.execution.result]
    elif primitive == "definition_release":
        expected = {
            "none": {"Void", "void"},
            "boolean": {"Boolean"},
            "map": {"Optional<HashMap<String, Object>>"},
        }[contract.definition.release_result]
    elif primitive == "definition_create" and legacy:
        expected = {"Optional<ProcessDefinitionService_createProcessDefinition_result>"}
    elif primitive == "definition_refs":
        expected = (
            {f"List<{_ENTITY}{noun}>"}
            if legacy
            else {f"List<{noun}ServiceImpl_query{noun}SimpleList_arrayNodeItem>"}
        )
    elif primitive == "definition_page":
        expected = {
            f"PageInfo<{noun}>",
            f"org.apache.dolphinscheduler.api.utils.PageInfo<{_ENTITY}{noun}>",
        }
    elif primitive == "definition_get" and not legacy:
        expected = {_ENTITY + "DagData"}
    else:
        expected = {_ENTITY + noun}
    if logical not in expected:
        msg = f"compiled workflow {primitive} logical response changed"
        raise ValueError(msg)
    if logical in {"Void", "void"}:
        return CompiledResponsePolicy(codec=codec, schema=None, capture=None)
    if logical in {"Boolean", "long"}:
        annotation = "bool" if logical == "Boolean" else "int"
        return CompiledResponsePolicy(
            codec=codec,
            schema=annotation,
            capture=True if annotation == "bool" else 1,
            scalar_annotation=annotation,
        )
    if primitive in {"definition_get", "definition_page"} or (
        primitive == "definition_refs" and legacy
    ):
        _require_definition(snapshot)
    if primitive == "definition_refs" and not legacy:
        _require_fields(
            snapshot,
            f"generated.view.{noun}ServiceImpl_query{noun}SimpleList_arrayNodeItem",
            {
                "code": "long",
                "projectCode": "long",
                "name": "String",
            },
        )
    if primitive == "definition_page":
        _require_fields(
            snapshot,
            "org.apache.dolphinscheduler.api.utils.PageInfo",
            {
                "totalList": "List<T>",
                "total": "Integer",
                "totalPage": "Integer",
                "currentPage": "Integer",
            },
        )
    if primitive == "definition_get" and not legacy:
        prefix = "workflow" if contract.definition.family == "workflow" else "process"
        _require_fields(
            snapshot,
            _ENTITY + "DagData",
            {
                prefix + "Definition": _ENTITY + noun,
                prefix + "TaskRelationList": (
                    f"List<{_ENTITY}{noun.removesuffix('Definition')}TaskRelation>"
                ),
                "taskDefinitionList": f"List<{_ENTITY}TaskDefinition>",
            },
        )
    return CompiledResponsePolicy(
        codec=codec,
        schema=primitive,
        capture=None,
        content_addressed=True,
    )


def recipe_policy(codecs: Mapping[str, str]) -> str:
    for version in REVIEWED_DS_VERSIONS:
        stamp = version.replace(".", "_")
        if dict(codecs) == {name: f"{name}_{stamp}" for name in _NAMES}:
            workflow_contract(version)
            return f"exact_{stamp}"
    msg = "compiled workflow definition codecs cross reviewed exact recipes"
    raise ValueError(msg)
