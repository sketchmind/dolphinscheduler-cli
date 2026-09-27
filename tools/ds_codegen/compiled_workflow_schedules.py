"""Schedule source policy inside the jointly owned workflow runtime domain."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

from ds_codegen.compatibility_impact import REVIEWED_DS_VERSIONS
from ds_codegen.compiled_domains import (
    CompiledPrimitive,
    CompiledRequestEpoch,
    CompiledResponsePolicy,
    model_field_facts,
    require_model,
)
from ds_codegen.contract_visibility import is_client_supplied_parameter

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ds_codegen.ir import ContractSnapshot, OperationSpec
    from dsctl.support.json_types import JsonValue

SEMANTIC_OPERATIONS = frozenset(
    {
        "schedule.create",
        "schedule.delete",
        "schedule.explain",
        "schedule.get",
        "schedule.offline",
        "schedule.online",
        "schedule.page",
        "schedule.preview",
        "schedule.update",
    }
)
SEMANTIC_ABSENT_VERSIONS: dict[str, frozenset[str]] = {}
_LEGACY = frozenset({"1.3.9"})
_CODE = frozenset(
    {
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
        "3.0.0",
        "3.0.1",
        "3.0.2",
        "3.0.3",
        "3.0.4",
        "3.0.5",
        "3.0.6",
        "3.1.0",
        "3.1.1",
        "3.1.2",
        "3.1.3",
        "3.1.4",
        "3.1.5",
        "3.1.6",
        "3.1.7",
        "3.1.8",
        "3.1.9",
    }
)
_TENANT = frozenset({"3.2.0", "3.2.1", "3.2.2"})
_MISSED_FIRE = frozenset({"3.4.3"})
_WORKFLOW = frozenset({"3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2"}) | _MISSED_FIRE
_MODERN = _CODE | _TENANT | _WORKFLOW
_ANNOTATED = _TENANT | _WORKFLOW
_EARLY_UPDATE = frozenset(
    {
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
        "3.0.0",
    }
)
_INTEGER_ENTITY_ID = frozenset(
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
        "3.0.0",
        "3.0.1",
        "3.0.2",
        "3.0.3",
        "3.0.4",
        "3.0.5",
        "3.0.6",
    }
)
_ENTITY_PAGE = frozenset(
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
_LOWER_VO = frozenset(
    {
        "3.0.0",
        "3.0.1",
        "3.0.2",
        "3.0.3",
        "3.0.4",
        "3.0.5",
        "3.0.6",
        "3.1.0",
        "3.1.1",
        "3.1.2",
        "3.1.3",
        "3.1.4",
        "3.1.5",
        "3.1.6",
        "3.1.7",
        "3.1.8",
        "3.1.9",
        "3.2.0",
    }
)
_BOOLEAN = frozenset({"3.2.1", "3.2.2"}) | _WORKFLOW
_ENTITY_UPDATE = frozenset({"3.2.2"}) | _WORKFLOW
_GLOBAL_WARNING = frozenset({"3.2.1", "3.2.2"})
_NAMED_LIFECYCLE = (
    frozenset(
        {
            "3.1.0",
            "3.1.1",
            "3.1.2",
            "3.1.3",
            "3.1.4",
            "3.1.5",
            "3.1.6",
            "3.1.7",
            "3.1.8",
            "3.1.9",
        }
    )
    | _ANNOTATED
)
_ENTITY = "org.apache.dolphinscheduler.dao.entity.Schedule"
_VO = "org.apache.dolphinscheduler.api.vo.ScheduleVo"
_PAGE = "org.apache.dolphinscheduler.api.utils.PageInfo"
_ENUM = "org.apache.dolphinscheduler.common.enums."
_BASE = "projects/{projectCode}/schedules"
_LEGACY_BASE = "projects/{projectName}/schedule"
_PRIMITIVE_METHODS = {
    "queryScheduleListPaging": "schedule_page",
    "createSchedule": "schedule_create",
    "updateSchedule": "schedule_update",
    "deleteScheduleById": "schedule_delete",
    "previewSchedule": "schedule_preview",
    "online": "schedule_online",
    "publishScheduleOnline": "schedule_online",
    "offline": "schedule_offline",
    "offlineSchedule": "schedule_offline",
}
_WRITES = (
    "schedule",
    "warningType",
    "warningGroupId",
    "failureStrategy",
    "workerGroup",
)


def _requests(primitive: str) -> tuple[CompiledRequestEpoch, ...]:
    """Finite request epochs; the compiler checks shared schemas byte-for-byte."""
    action = primitive.removeprefix("schedule_")
    legacy_fields = {
        "page": ("processDefinitionId", "searchVal", "pageNo", "pageSize"),
        "create": (
            "processDefinitionId",
            "schedule",
            "warningType",
            "warningGroupId",
            "failureStrategy",
            "receivers",
            "receiversCc",
            "workerGroup",
            "processInstancePriority",
        ),
        "update": (
            "id",
            "schedule",
            "warningType",
            "warningGroupId",
            "failureStrategy",
            "receivers",
            "receiversCc",
            "workerGroup",
            "processInstancePriority",
        ),
        "delete": ("scheduleId",),
        "preview": ("schedule",),
        "online": ("id",),
        "offline": ("id",),
    }[action]
    legacy = CompiledRequestEpoch(
        method="GET" if action in {"page", "delete"} else "POST",
        path=f"{_LEGACY_BASE}/{'list-paging' if action == 'page' else action}",
        channel="path_query" if action in {"page", "delete"} else "path_form",
        request_schema=primitive,
        request_model=f"Schedule{action.title()}Params",
        content_addressed=True,
        request_fields=("projectName", *legacy_fields),
        path_fields=("projectName",),
        versions=_LEGACY,
        path_encoding="legacy-url-interpolation-v1",
    )
    if action in {"delete", "online", "offline"}:
        modern = replace(
            legacy,
            method="DELETE" if action == "delete" else "POST",
            path=f"{_BASE}/{{id}}" + ("" if action == "delete" else f"/{action}"),
            channel="path",
            request_fields=("projectCode", "id"),
            path_fields=("projectCode", "id"),
            versions=_MODERN,
            path_encoding="percent-encoded-utf8-segment-v1",
        )
        return legacy, modern
    if action == "preview":
        return legacy, replace(
            legacy,
            path=f"{_BASE}/preview",
            request_fields=("projectCode", "schedule"),
            path_fields=("projectCode",),
            versions=_MODERN,
            path_encoding="percent-encoded-utf8-segment-v1",
        )
    epochs: list[CompiledRequestEpoch] = [legacy]
    for name, versions in (
        ("code", _CODE),
        ("tenant", frozenset({"3.2.0"})),
        ("tenant_global", _GLOBAL_WARNING),
        ("workflow", _WORKFLOW),
    ):
        workflow_field = (
            "workflowDefinitionCode" if name == "workflow" else "processDefinitionCode"
        )
        priority_field = (
            "workflowInstancePriority"
            if name == "workflow"
            else "processInstancePriority"
        )
        if action == "page":
            fields = ("projectCode", workflow_field, "searchVal", "pageNo", "pageSize")
        else:
            identity = workflow_field if action == "create" else "id"
            fields = (
                "projectCode",
                identity,
                *_WRITES,
                *(("tenantCode",) if name != "code" else ()),
                "environmentCode",
                priority_field,
            )
        groups: tuple[tuple[str, frozenset[str]], ...] = ((name, versions),)
        if action == "update" and name == "code":
            groups = (
                ("code_early", _EARLY_UPDATE),
                ("code_priority", _CODE - _EARLY_UPDATE),
            )
        for _, selected_versions in groups:
            epochs.append(
                CompiledRequestEpoch(
                    method="GET"
                    if action == "page"
                    else "POST"
                    if action == "create"
                    else "PUT",
                    path=_BASE + ("/{id}" if action == "update" else ""),
                    channel="path_query" if action == "page" else "path_form",
                    request_schema=primitive,
                    request_model=f"Schedule{action.title()}Params",
                    content_addressed=True,
                    request_fields=fields,
                    path_fields=("projectCode", "id")
                    if action == "update"
                    else ("projectCode",),
                    versions=selected_versions,
                )
            )
    return tuple(epochs)


PRIMITIVES = tuple(
    CompiledPrimitive(
        name, _requests(name), "optional" if name == "schedule_page" else "required"
    )
    for name in (
        "schedule_page",
        "schedule_create",
        "schedule_update",
        "schedule_delete",
        "schedule_online",
        "schedule_offline",
        "schedule_preview",
    )
)


def classify(operation: OperationSpec) -> str | None:
    if operation.controller != "SchedulerController":
        return None
    primitive = _PRIMITIVE_METHODS.get(operation.method_name)
    if operation.operation_id != f"SchedulerController.{operation.method_name}":
        return None
    return primitive


def response_policy(
    snapshot: ContractSnapshot, operation: OperationSpec, primitive: str
) -> CompiledResponsePolicy:
    version = snapshot.ds_version
    if version not in REVIEWED_DS_VERSIONS or classify(operation) != primitive:
        msg = "compiled schedule source identity is unreviewed"
        raise ValueError(msg)
    _validate_request(snapshot, operation, primitive)
    if operation.response_projection != "direct":
        msg = "compiled schedule response projection changed"
        raise ValueError(msg)
    action = primitive.removeprefix("schedule_")
    expected = "Void"
    capture: JsonValue = None
    role: str | None = None
    if action == "page":
        model = (
            _ENTITY
            if version in _ENTITY_PAGE
            else _VO
            if version in _LOWER_VO
            else _VO[:-1] + "O"
        )
        expected = f"{_PAGE}<{model}>"
        _validate_schedule_model(snapshot, model)
        page = require_model(snapshot, _PAGE, domain="schedule")
        if page.extends is not None or not any(
            field.name == "totalList" and field.java_type == "List<T>"
            for field in page.fields
        ):
            msg = "compiled schedule page closure changed"
            raise ValueError(msg)
        capture, role = {"totalList": []}, "schedule_page"
    elif (action == "create" and version not in _LEGACY) or (
        action == "update" and version in _ENTITY_UPDATE
    ):
        expected, role, capture = _ENTITY, "schedule_entity", {}
        _validate_schedule_model(snapshot, _ENTITY)
    elif action == "preview":
        expected, role, capture = "List<String>", "schedule_preview", []
    elif action in {"online", "offline"} and version in _BOOLEAN:
        expected, role, capture = "Boolean", "schedule_boolean", False
    if operation.logical_return_type != expected:
        msg = f"compiled schedule {primitive} response type changed"
        raise ValueError(msg)
    return CompiledResponsePolicy(
        codec=f"{primitive}_{version.replace('.', '_')}",
        schema=role,
        capture=capture,
        content_addressed=role is not None,
    )


def recipe_policy(codecs: Mapping[str, str]) -> str:
    expected_primitives = {primitive.name for primitive in PRIMITIVES}
    versions = [
        version
        for version in REVIEWED_DS_VERSIONS
        if dict(codecs)
        == {name: f"{name}_{version.replace('.', '_')}" for name in expected_primitives}
    ]
    if len(versions) != 1:
        msg = "compiled schedule codec recipe is incomplete or mixed"
        raise ValueError(msg)
    version = versions[0]
    if version in _LEGACY:
        return "legacy_id"
    if version in _CODE:
        return "code"
    if version == "3.2.0":
        return "tenant_void"
    if version == "3.2.1":
        return "tenant_boolean"
    if version in _MISSED_FIRE:
        return "workflow_missed_fire"
    return "tenant_entity" if version == "3.2.2" else "workflow"


def _validate_request(
    snapshot: ContractSnapshot, operation: OperationSpec, primitive: str
) -> None:
    version, action = snapshot.ds_version, primitive.removeprefix("schedule_")
    expected_method = {
        "page": "queryScheduleListPaging",
        "create": "createSchedule",
        "update": "updateSchedule",
        "preview": "previewSchedule",
        "delete": "deleteScheduleById",
        "online": "publishScheduleOnline" if version in _NAMED_LIFECYCLE else "online",
        "offline": "offlineSchedule" if version in _NAMED_LIFECYCLE else "offline",
    }[action]
    if operation.method_name != expected_method or operation.consumes:
        msg = "compiled schedule method or content type changed"
        raise ValueError(msg)
    project_required = True if version in _ANNOTATED else None
    facts: list[tuple[str, str, str, bool | None, str | None]] = []
    if action != "preview" or version in _LEGACY:
        facts.append(
            (
                "projectName" if version in _LEGACY else "projectCode",
                "String" if version in _LEGACY else "long",
                "path_variable",
                project_required,
                None,
            )
        )
    if action == "page":
        workflow = (
            "workflowDefinitionCode"
            if version in _WORKFLOW
            else "processDefinitionId"
            if version in _LEGACY
            else "processDefinitionCode"
        )
        facts.extend(
            (
                (
                    workflow,
                    "Integer" if version in _LEGACY else "long",
                    "request_param",
                    False if version in _ANNOTATED else None,
                    "0" if version in _ANNOTATED else None,
                ),
                ("searchVal", "String", "request_param", False, None),
                ("pageNo", "Integer", "request_param", None, None),
                ("pageSize", "Integer", "request_param", None, None),
            )
        )
    elif action in {"delete", "online", "offline"}:
        facts.append(
            (
                "scheduleId" if version in _LEGACY and action == "delete" else "id",
                "Integer",
                "request_param" if version in _LEGACY else "path_variable",
                project_required,
                None,
            )
        )
    elif action == "preview":
        facts.append(("schedule", "String", "request_param", None, None))
    else:
        if action == "create":
            facts.append(
                (
                    "workflowDefinitionCode"
                    if version in _WORKFLOW
                    else "processDefinitionId"
                    if version in _LEGACY
                    else "processDefinitionCode",
                    "Integer" if version in _LEGACY else "long",
                    "request_param",
                    project_required,
                    None,
                )
            )
        else:
            facts.append(
                (
                    "id",
                    "Integer",
                    "request_param" if version in _LEGACY else "path_variable",
                    project_required,
                    None,
                )
            )
        facts.extend(
            (
                ("schedule", "String", "request_param", None, None),
                (
                    "warningType",
                    _ENUM + "WarningType",
                    "request_param",
                    False,
                    "DEFAULT_WARNING_TYPE",
                ),
                (
                    "warningGroupId",
                    "int",
                    "request_param",
                    False,
                    "DEFAULT_NOTIFY_GROUP_ID"
                    if action == "create" or version in _ANNOTATED
                    else None,
                ),
                (
                    "failureStrategy",
                    _ENUM + "FailureStrategy",
                    "request_param",
                    False,
                    "DEFAULT_FAILURE_POLICY" if action == "create" else "END",
                ),
            )
        )
        if version in _LEGACY:
            facts.extend(
                (
                    ("receivers", "String", "request_param", False, None),
                    ("receiversCc", "String", "request_param", False, None),
                )
            )
        facts.append(("workerGroup", "String", "request_param", False, "default"))
        if version in _ANNOTATED:
            facts.append(("tenantCode", "String", "request_param", False, "default"))
        if version not in _LEGACY:
            facts.append(("environmentCode", "Long", "request_param", False, "-1"))
        priority_default = (
            None
            if version in _LEGACY or (action == "update" and version in _EARLY_UPDATE)
            else "DEFAULT_WORKFLOW_INSTANCE_PRIORITY"
            if version in _WORKFLOW
            else "DEFAULT_PROCESS_INSTANCE_PRIORITY"
        )
        facts.append(
            (
                "workflowInstancePriority"
                if version in _WORKFLOW
                else "processInstancePriority",
                _ENUM + "Priority",
                "request_param",
                False,
                priority_default,
            )
        )
        _validate_enums(snapshot)
        if version in _MISSED_FIRE:
            _validate_schedule_param(snapshot)
    actual = [
        (p.wire_name, p.java_type, p.binding, p.required, p.default_value)
        for p in operation.parameters
        if is_client_supplied_parameter(p)
    ]
    if actual != facts:
        msg = f"compiled schedule {primitive} request fields changed"
        raise ValueError(msg)


def _validate_schedule_param(snapshot: ContractSnapshot) -> None:
    """Lock the native JSON DTO default and explicit-presence tracking shape."""
    model = require_model(
        snapshot, "org.apache.dolphinscheduler.api.dto.ScheduleParam", domain="schedule"
    )
    expected = (
        ("startTime", "Date", True, None, None),
        ("endTime", "Date", True, None, None),
        ("crontab", "String", True, None, None),
        ("timezoneId", "String", True, None, None),
        (
            "missedFirePolicy",
            _ENUM + "ScheduleMissedFirePolicy",
            True,
            "ScheduleMissedFirePolicy.FIRE_ALL_MISSED",
            None,
        ),
        ("missedFirePolicySet", "boolean", False, "false", None),
    )
    if model.extends is not None or model_field_facts(model) != expected:
        message = "compiled schedule JSON DTO default or presence tracking changed"
        raise ValueError(message)


def _validate_enums(snapshot: ContractSnapshot) -> None:
    names = {
        "FailureStrategy": ("END", "CONTINUE"),
        "Priority": ("HIGHEST", "HIGH", "MEDIUM", "LOW", "LOWEST"),
        "ReleaseState": ("OFFLINE", "ONLINE"),
        "WarningType": (
            "NONE",
            "SUCCESS",
            "FAILURE",
            "ALL",
            *(("GLOBAL",) if snapshot.ds_version in _GLOBAL_WARNING else ()),
        ),
    }
    if snapshot.ds_version in _MISSED_FIRE:
        enum = next(
            item
            for item in snapshot.enums
            if item.import_path == _ENUM + "ScheduleMissedFirePolicy"
        )
        if (
            enum.json_value_field is not None
            or tuple((field.name, field.java_type) for field in enum.fields)
            != (("code", "int"),)
            or tuple((value.name, tuple(value.arguments)) for value in enum.values)
            != (
                ("SKIP_MISSED", ("0",)),
                ("FIRE_ONCE_NOW", ("1",)),
                ("FIRE_ALL_MISSED", ("2",)),
            )
        ):
            msg = "compiled schedule missed fire policy enum changed"
            raise ValueError(msg)
    for name, values in names.items():
        enum = next(item for item in snapshot.enums if item.import_path == _ENUM + name)
        if (
            enum.json_value_field is not None
            or tuple((field.name, field.java_type) for field in enum.fields)
            != (("code", "int"), ("descp", "String"))
            or tuple((value.name, tuple(value.arguments)) for value in enum.values)
            != tuple(
                (value, (str(index), value.lower()))
                for index, value in enumerate(values)
            )
        ):
            msg = f"compiled schedule {name} enum changed"
            raise ValueError(msg)


def _validate_schedule_model(snapshot: ContractSnapshot, import_path: str) -> None:
    version = snapshot.ds_version
    view = import_path != _ENTITY
    workflow = "workflowDefinition" if version in _WORKFLOW else "processDefinition"
    integer_id = view or version in _INTEGER_ENTITY_ID
    fields = [
        (
            "id",
            "int" if integer_id else "Integer",
            not integer_id,
            "0" if integer_id else None,
            None,
        ),
        (
            workflow + ("Id" if version in _LEGACY else "Code"),
            "int" if version in _LEGACY else "long",
            False,
            "0",
            None,
        ),
        (workflow + "Name", "String", True, None, None),
        ("projectName", "String", True, None, None),
        ("definitionDescription", "String", True, None, None),
        ("startTime", "String" if view else "Date", True, None, None),
        ("endTime", "String" if view else "Date", True, None, None),
    ]
    if version not in _LEGACY:
        fields.append(("timezoneId", "String", True, None, None))
    fields.append(("crontab", "String", True, None, None))
    if version in _MISSED_FIRE:
        fields.append(
            ("missedFirePolicy", _ENUM + "ScheduleMissedFirePolicy", True, None, None)
        )
    fields.extend(
        (
            ("failureStrategy", _ENUM + "FailureStrategy", True, None, None),
            ("warningType", _ENUM + "WarningType", True, None, None),
            ("createTime", "Date", True, None, None),
            ("updateTime", "Date", True, None, None),
            ("userId", "int", False, "0", None),
            ("userName", "String", True, None, None),
            ("releaseState", _ENUM + "ReleaseState", True, None, None),
            ("warningGroupId", "int", False, "0", None),
            (
                "workflowInstancePriority"
                if version in _WORKFLOW
                else "processInstancePriority",
                _ENUM + "Priority",
                True,
                None,
                None,
            ),
            ("workerGroup", "String", True, None, None),
        )
    )
    if version in _ANNOTATED:
        fields.append(("tenantCode", "String", True, None, None))
    if version not in _LEGACY:
        fields.append(("environmentCode", "Long", True, None, None))
    if version in _ANNOTATED:
        fields.append(("environmentName", "String", True, None, None))
    model = require_model(snapshot, import_path, domain="schedule")
    if model.extends is not None or model_field_facts(model) != tuple(fields):
        msg = "compiled schedule response fields changed"
        raise ValueError(msg)
    _validate_enums(snapshot)


__all__ = [
    "PRIMITIVES",
    "SEMANTIC_ABSENT_VERSIONS",
    "SEMANTIC_OPERATIONS",
    "classify",
    "recipe_policy",
    "response_policy",
]
