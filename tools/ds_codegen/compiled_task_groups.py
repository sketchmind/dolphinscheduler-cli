"""Compile task-group wire epochs against the reviewed exact recipes."""

from __future__ import annotations

from typing import TYPE_CHECKING

from ds_codegen.compiled_domains import (
    CompiledDomainDefinition,
    CompiledPrimitive,
    CompiledRequestEpoch,
    CompiledResponsePolicy,
    model_field_facts,
    require_model,
)
from ds_codegen.contract_visibility import is_client_supplied_parameter
from ds_codegen.task_definition_cleanup_contract import cleanup_strict_integer_fields
from ds_codegen.task_group_contract import (
    TARGET_TASK_GROUP_VERSIONS,
    TASK_GROUP_SEMANTIC_OPERATIONS,
    task_group_contract,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ds_codegen.ir import ContractSnapshot, OperationSpec
    from ds_codegen.task_group_contract import TaskGroupRecipe

COMPILED_TASK_GROUP_SCHEMA_VERSION = 1
COMPILED_TASK_GROUP_SEMANTIC_OPERATIONS = frozenset(TASK_GROUP_SEMANTIC_OPERATIONS)
_ABSENT_VERSIONS = frozenset(
    version
    for version in TARGET_TASK_GROUP_VERSIONS
    if task_group_contract(version).task_group.support == "absent"
)

_PAGE = "org.apache.dolphinscheduler.api.utils.PageInfo"
_GROUP = "org.apache.dolphinscheduler.dao.entity.TaskGroup"
_QUEUE = "org.apache.dolphinscheduler.dao.entity.TaskGroupQueue"
_FLAG = "org.apache.dolphinscheduler.common.enums.Flag"
_QUEUE_STATUS = "org.apache.dolphinscheduler.common.enums.TaskGroupQueueStatus"
_REQUIRED_ID = ("id", "int", False, "0", None)
_NULLABLE_ID = ("id", "Integer", True, None, None)
_TIMES = (
    ("createTime", "Date", True, None, None),
    ("updateTime", "Date", True, None, None),
)
_GROUP_NAME = ("name", "String", True, None, None)
_PROJECT_CODE = ("projectCode", "long", False, "0", None)
_GROUP_SIZES = (
    ("description", "String", True, None, None),
    ("groupSize", "int", False, "0", None),
    ("useSize", "int", False, "0", None),
    ("userId", "int", False, "0", None),
)
_NUMERIC_GROUP_FIELDS = (
    _REQUIRED_ID,
    _GROUP_NAME,
    *_GROUP_SIZES,
    ("status", "Integer", True, None, None),
    *_TIMES,
    _PROJECT_CODE,
)
_FLAG_GROUP_FIELDS = (
    _NULLABLE_ID,
    _GROUP_NAME,
    _PROJECT_CODE,
    *_GROUP_SIZES,
    ("status", _FLAG, True, None, None),
    *_TIMES,
)
_QUEUE_TASK_FIELDS = (
    ("taskId", "int", False, "0", None),
    ("taskName", "String", True, None, None),
    ("projectName", "String", True, None, None),
    ("projectCode", "String", True, None, None),
)
_QUEUE_GROUP_ID = ("groupId", "int", False, "0", None)
_QUEUE_STATE_FIELDS = (
    ("priority", "int", False, "0", None),
    ("forceStart", "int", False, "0", None),
    ("inQueue", "int", False, "0", None),
    ("status", _QUEUE_STATUS, True, None, None),
    *_TIMES,
)
_PROCESS_QUEUE_FIELDS = (
    _REQUIRED_ID,
    *_QUEUE_TASK_FIELDS,
    ("processInstanceName", "String", True, None, None),
    _QUEUE_GROUP_ID,
    ("processId", "int", False, "0", None),
    *_QUEUE_STATE_FIELDS,
)
_WORKFLOW_QUEUE_FIELDS = (
    _NULLABLE_ID,
    *_QUEUE_TASK_FIELDS,
    ("workflowInstanceName", "String", True, None, None),
    _QUEUE_GROUP_ID,
    ("workflowInstanceId", "Integer", True, None, None),
    *_QUEUE_STATE_FIELDS,
)
_NULLABLE_PAGE_FIELDS = (
    ("totalList", "List<T>", True, None, None),
    ("total", "Integer", False, "0", None),
    ("totalPage", "Integer", True, None, None),
    ("pageSize", "Integer", False, "20", None),
    ("currentPage", "Integer", True, "0", None),
    ("pageNo", "Integer", True, None, None),
)
_LIST_PAGE_FIELDS = (
    ("totalList", "List<T>", False, None, "list"),
    *_NULLABLE_PAGE_FIELDS[1:],
)

_PRIMITIVES = (
    CompiledPrimitive(
        name="page",
        requests=(
            CompiledRequestEpoch(
                method="GET",
                path="task-group/list-paging",
                channel="query",
                request_schema="page",
                request_model="TaskGroupPageParams",
                request_fields=("name", "status", "pageNo", "pageSize"),
                required_fields=frozenset({"pageNo", "pageSize"}),
            ),
        ),
        result_envelope="optional",
    ),
    CompiledPrimitive(
        name="project_page",
        requests=(
            CompiledRequestEpoch(
                method="GET",
                path="task-group/query-list-by-projectCode",
                channel="query",
                request_schema="project_page",
                request_model="TaskGroupProjectPageParams",
                request_fields=("pageNo", "projectCode", "pageSize"),
                required_fields=frozenset({"pageNo", "pageSize"}),
            ),
        ),
        result_envelope="optional",
    ),
    CompiledPrimitive(
        name="create",
        requests=(
            CompiledRequestEpoch(
                method="POST",
                path="task-group/create",
                channel="form",
                request_schema="create",
                request_model="TaskGroupCreateParams",
                request_fields=("name", "projectCode", "description", "groupSize"),
                required_fields=frozenset({"name", "description", "groupSize"}),
            ),
        ),
        result_envelope="required",
    ),
    CompiledPrimitive(
        name="update",
        requests=(
            CompiledRequestEpoch(
                method="POST",
                path="task-group/update",
                channel="form",
                request_schema="update",
                request_model="TaskGroupUpdateParams",
                request_fields=("id", "name", "description", "groupSize"),
                required_fields=frozenset({"id", "name", "description", "groupSize"}),
            ),
        ),
        result_envelope="required",
    ),
    *(
        CompiledPrimitive(
            name=action,
            requests=(
                CompiledRequestEpoch(
                    method="POST",
                    path=f"task-group/{action}-task-group",
                    channel="form",
                    request_schema="id",
                    request_model="TaskGroupIdParams",
                    request_fields=("id",),
                    required_fields=frozenset(),
                ),
            ),
            result_envelope="required",
        )
        for action in ("close", "start")
    ),
    CompiledPrimitive(
        name="queue_page",
        requests=tuple(
            CompiledRequestEpoch(
                method="GET",
                path="task-group/query-list-by-group-id",
                channel="query",
                request_schema=f"queue_{identity}",
                request_model=f"TaskGroup{identity.title()}QueueParams",
                request_fields=(
                    "groupId",
                    "taskInstanceName",
                    f"{identity}InstanceName",
                    "status",
                    "pageNo",
                    "pageSize",
                ),
                required_fields=frozenset({"pageNo", "pageSize"}),
            )
            for identity in ("process", "workflow")
        ),
        result_envelope="optional",
    ),
    CompiledPrimitive(
        name="force_start",
        requests=(
            CompiledRequestEpoch(
                method="POST",
                path="task-group/forceStart",
                channel="form",
                request_schema="queue_id",
                request_model="TaskGroupQueueIdParams",
                request_fields=("queueId",),
                required_fields=frozenset({"queueId"}),
            ),
        ),
        result_envelope="required",
    ),
    CompiledPrimitive(
        name="priority",
        requests=(
            CompiledRequestEpoch(
                method="POST",
                path="task-group/modifyPriority",
                channel="form",
                request_schema="priority",
                request_model="TaskGroupPriorityParams",
                request_fields=("queueId", "priority"),
                required_fields=frozenset({"queueId", "priority"}),
            ),
        ),
        result_envelope="required",
    ),
)


def _classify(operation: OperationSpec) -> str | None:
    if operation.controller != "TaskGroupController":
        return None
    return {
        "queryAllTaskGroup": "page",
        "queryTaskGroupByCode": "project_page",
        "createTaskGroup": "create",
        "updateTaskGroup": "update",
        "closeTaskGroup": "close",
        "startTaskGroup": "start",
        "queryTasksByGroupId": "queue_page",
        "queryTaskGroupQueues": "queue_page",
        "forceStart": "force_start",
        "modifyPriority": "priority",
    }.get(operation.method_name)


def _response(
    snapshot: ContractSnapshot, operation: OperationSpec, primitive: str
) -> CompiledResponsePolicy:
    reviewed = task_group_contract(snapshot.ds_version)
    recipe = reviewed.task_group
    if recipe.support != "supported":
        message = "compiled task_group reviewed support changed"
        raise ValueError(message)
    if primitive == "queue_page" and reviewed.queue_page_projection is None:
        message = "compiled task_group queue page has no reviewed paging projection"
        raise ValueError(message)
    _require_request_fields(operation, primitive, recipe)
    _require_result_type(operation, primitive, recipe)
    if primitive in {"page", "project_page", "queue_page"}:
        entity_epoch = (
            _queue_epoch(snapshot, recipe)
            if primitive == "queue_page"
            else _group_epoch(snapshot, recipe)
        )
        epoch = f"{entity_epoch}_{_page_epoch(snapshot)}"
        schema = f"page_{epoch}"
        return CompiledResponsePolicy(
            codec=f"{primitive}_{epoch}", schema=schema, capture={}
        )
    if primitive in {"create", "update"}:
        result = recipe.create_result if primitive == "create" else recipe.update_result
        if result == "entity":
            schema = f"entity_{_group_epoch(snapshot, recipe)}"
            return CompiledResponsePolicy(
                codec=f"{primitive}_{schema}", schema=schema, capture={}
            )
    return CompiledResponsePolicy(codec=f"{primitive}_void", schema=None, capture=None)


def _require_request_fields(
    operation: OperationSpec, primitive: str, recipe: TaskGroupRecipe
) -> None:
    types = {
        "id": "Integer",
        "name": "String",
        "projectCode": "Long",
        "description": "String",
        "groupSize": "Integer",
        "status": "Integer",
        "pageNo": "Integer",
        "pageSize": "Integer",
        "groupId": "Integer",
        "taskInstanceName": "String",
        "processInstanceName": "String",
        "workflowInstanceName": "String",
        "queueId": "Integer",
        "priority": "Integer",
    }
    defaults = {"projectCode": "0"} if primitive == "create" else {}
    if primitive == "queue_page":
        defaults = {"groupId": "-1"}
    parameters = tuple(
        item for item in operation.parameters if is_client_supplied_parameter(item)
    )
    if any(
        parameter.java_type != types.get(parameter.wire_name or "")
        or parameter.default_value != defaults.get(parameter.wire_name or "")
        for parameter in parameters
    ):
        message = "compiled task_group request type or default changed"
        raise ValueError(message)
    if primitive == "queue_page" and (
        operation.operation_id != recipe.queue_operation
        or recipe.queue_workflow_filter
        not in tuple(item.wire_name for item in parameters)
    ):
        message = "compiled task_group queue request contradicts reviewed recipe"
        raise ValueError(message)


def _require_result_type(
    operation: OperationSpec, primitive: str, recipe: TaskGroupRecipe
) -> None:
    if primitive in {"page", "project_page"}:
        logical, declared = f"{_PAGE}<{_GROUP}>", "PageInfo<TaskGroup>"
    elif primitive == "queue_page":
        logical, declared = f"{_PAGE}<{_QUEUE}>", "PageInfo<TaskGroupQueue>"
    elif (primitive == "create" and recipe.create_result == "entity") or (
        primitive == "update" and recipe.update_result == "entity"
    ):
        logical, declared = _GROUP, "TaskGroup"
    else:
        logical, declared = "Void", "Void"
    expected_return = f"Result<{declared}>" if recipe.typed_results else "Result"
    if (
        operation.logical_return_type != logical
        or operation.return_type != expected_return
        or operation.response_projection != "direct"
        or operation.consumes
    ):
        message = (
            f"compiled task_group {primitive} response contradicts reviewed recipe"
        )
        raise ValueError(message)


def _group_epoch(snapshot: ContractSnapshot, recipe: TaskGroupRecipe) -> str:
    fields = model_field_facts(require_model(snapshot, _GROUP, domain="task_group"))
    if recipe.numeric_group_status:
        if fields == _NUMERIC_GROUP_FIELDS:
            return "group_required_id"
        if fields == (_NULLABLE_ID, *_NUMERIC_GROUP_FIELDS[1:]):
            return "group_nullable_id"
    elif fields == _FLAG_GROUP_FIELDS:
        _require_enum(snapshot, _FLAG, (("NO", ("0", "no")), ("YES", ("1", "yes"))))
        return "group_flag"
    message = "compiled task_group entity response contradicts reviewed status"
    raise ValueError(message)


def _queue_epoch(snapshot: ContractSnapshot, recipe: TaskGroupRecipe) -> str:
    fields = model_field_facts(require_model(snapshot, _QUEUE, domain="task_group"))
    _require_enum(
        snapshot,
        _QUEUE_STATUS,
        (
            ("WAIT_QUEUE", ("-1", "wait queue")),
            ("ACQUIRE_SUCCESS", ("1", "acquire success")),
            ("RELEASE", ("2", "release")),
        ),
    )
    if recipe.queue_identity == "process":
        if fields == _PROCESS_QUEUE_FIELDS:
            return "queue_required_id"
        if fields == (_NULLABLE_ID, *_PROCESS_QUEUE_FIELDS[1:]):
            return "queue_nullable_id"
    elif recipe.queue_identity == "workflow" and fields == _WORKFLOW_QUEUE_FIELDS:
        return "queue_workflow"
    message = "compiled task_group queue response contradicts reviewed identity"
    raise ValueError(message)


def _page_epoch(snapshot: ContractSnapshot) -> str:
    fields = model_field_facts(require_model(snapshot, _PAGE, domain="task_group"))
    strict = bool(cleanup_strict_integer_fields(snapshot.ds_version).get(_PAGE))
    if fields == _NULLABLE_PAGE_FIELDS and strict:
        return "nullable_strict"
    if fields == _LIST_PAGE_FIELDS:
        return "list_strict" if strict else "list"
    message = "compiled task_group page response epoch changed"
    raise ValueError(message)


def _require_enum(
    snapshot: ContractSnapshot,
    import_path: str,
    expected_values: tuple[tuple[str, tuple[str, ...]], ...],
) -> None:
    matches = tuple(item for item in snapshot.enums if item.import_path == import_path)
    if len(matches) != 1:
        message = f"compiled task_group requires the exact {import_path} enum"
        raise ValueError(message)
    enum = matches[0]
    if (
        tuple((field.name, field.java_type) for field in enum.fields)
        != (("code", "int"), ("descp", "String"))
        or tuple((item.name, tuple(item.arguments)) for item in enum.values)
        != expected_values
        or enum.json_value_field is not None
    ):
        message = f"compiled task_group {enum.name} enum changed"
        raise ValueError(message)


def _recipe(codecs: Mapping[str, str]) -> str:
    fixed = {
        action: f"{action}_void"
        for action in ("close", "start", "force_start", "priority")
    }
    if all(codecs.get(name) == codec for name, codec in fixed.items()):
        for group, queue, mutation, recipe in (
            (
                "group_required_id_nullable_strict",
                "queue_required_id_nullable_strict",
                "void",
                "legacy_queue_void",
            ),
            (
                "group_nullable_id_list_strict",
                "queue_nullable_id_list_strict",
                "void",
                "legacy_queue_void",
            ),
            (
                "group_nullable_id_list",
                "queue_nullable_id_list",
                "void",
                "legacy_queue_void",
            ),
            (
                "group_flag_list",
                "queue_nullable_id_list",
                "void",
                "renamed_queue_void",
            ),
            (
                "group_flag_list",
                "queue_nullable_id_list",
                "entity_group_flag",
                "renamed_queue_entity",
            ),
            (
                "group_flag_list",
                "queue_workflow_list",
                "entity_group_flag",
                "workflow_queue_entity",
            ),
        ):
            expected = {
                **fixed,
                "page": f"page_{group}",
                "project_page": f"project_page_{group}",
                "queue_page": f"queue_page_{queue}",
                "create": f"create_{mutation}",
                "update": f"update_{mutation}",
            }
            if codecs == expected:
                return recipe
    message = f"compiled task_group recipe is unsupported: {dict(codecs)!r}"
    raise ValueError(message)


TASK_GROUP_COMPILED_DOMAIN = CompiledDomainDefinition(
    name="task_group",
    schema_constant="COMPILED_TASK_GROUP_SCHEMA_VERSION",
    schema_version=COMPILED_TASK_GROUP_SCHEMA_VERSION,
    semantic_operations=COMPILED_TASK_GROUP_SEMANTIC_OPERATIONS,
    absent_versions=_ABSENT_VERSIONS,
    primitives=_PRIMITIVES,
    classify_operation=_classify,
    response_policy=_response,
    recipe_policy=_recipe,
)

__all__ = [
    "COMPILED_TASK_GROUP_SCHEMA_VERSION",
    "COMPILED_TASK_GROUP_SEMANTIC_OPERATIONS",
    "TASK_GROUP_COMPILED_DOMAIN",
]
