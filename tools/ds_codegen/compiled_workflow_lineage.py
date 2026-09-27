"""Exact lineage graph and dependent-task reads in the workflow runtime domain."""

from __future__ import annotations

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
from ds_codegen.workflow_contract import workflow_contract

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ds_codegen.ir import ContractSnapshot, OperationSpec
    from dsctl.support.json_types import JsonValue

_ABSENT = frozenset({"1.3.9"})
_DEPENDENT_ABSENT = frozenset(
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
        "3.2.1",
    }
)
_CANONICAL = frozenset({"3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2", "3.4.3"})
_ANNOTATED = frozenset({"3.2.0", "3.2.1", "3.2.2"}) | _CANONICAL
_BASE = "projects/{projectCode}/lineages"
_ENTITY = "org.apache.dolphinscheduler.dao.entity."
_VIEW = "generated.view."
_GRAPH = _ENTITY + "WorkFlowLineage"
_RELATION = _ENTITY + "WorkFlowRelation"
SEMANTIC_OPERATIONS = frozenset(
    {
        "workflow.lineage.list",
        "workflow.lineage.get",
        "workflow.lineage.dependent-tasks",
    }
)
SEMANTIC_ABSENT_VERSIONS = {
    "workflow.lineage.list": _ABSENT,
    "workflow.lineage.get": _ABSENT,
    "workflow.lineage.dependent-tasks": _DEPENDENT_ABSENT,
}
PRIMITIVES = (
    CompiledPrimitive(
        "lineage_list",
        (
            CompiledRequestEpoch(
                method="GET",
                path=f"{_BASE}/list",
                channel="path",
                request_schema="lineage_list",
                request_model="LineageListParams",
                content_addressed=True,
                request_fields=("projectCode",),
                path_fields=("projectCode",),
            ),
        ),
        "optional",
        _ABSENT,
    ),
    CompiledPrimitive(
        "lineage_get",
        (
            CompiledRequestEpoch(
                method="GET",
                path=f"{_BASE}/{{workFlowCode}}",
                channel="path",
                request_schema="lineage_get",
                request_model="LineageGetParams",
                content_addressed=True,
                request_fields=("projectCode", "workFlowCode"),
                path_fields=("projectCode", "workFlowCode"),
            ),
        ),
        "optional",
        _ABSENT,
    ),
    CompiledPrimitive(
        "lineage_dependent_tasks",
        tuple(
            CompiledRequestEpoch(
                method="GET",
                path=f"{_BASE}/query-dependent-tasks",
                channel="path_query",
                request_schema="lineage_dependent_tasks",
                request_model="LineageDependentTasksParams",
                content_addressed=True,
                request_fields=("projectCode", "workFlowCode", "taskCode"),
                path_fields=("projectCode",),
                versions=versions,
            )
            for versions in (
                frozenset({"3.2.2"}),
                _CANONICAL,
            )
        ),
        "optional",
        _DEPENDENT_ABSENT,
    ),
)
_METHODS = {
    "queryWorkFlowLineage": "lineage_list",
    "queryWorkFlowLineageByCode": "lineage_get",
    "queryDownstreamDependentTaskList": "lineage_dependent_tasks",
    "queryDependentTasks": "lineage_dependent_tasks",
}
_DETAIL_FIELDS = (
    ("workFlowCode", "long", False, "0", None),
    ("workFlowName", "String", True, None, None),
    ("workFlowPublishStatus", "String", True, None, None),
    ("scheduleStartTime", "Date", True, None, None),
    ("scheduleEndTime", "Date", True, None, None),
    ("crontab", "String", True, None, None),
    ("schedulePublishStatus", "int", False, "0", None),
    ("sourceWorkFlowCode", "String", True, None, None),
)


def classify(operation: OperationSpec) -> str | None:
    if (
        operation.controller
        not in {"WorkFlowLineageController", "WorkflowLineageController"}
        or operation.operation_id != f"{operation.controller}.{operation.method_name}"
    ):
        return None
    return _METHODS.get(operation.method_name)


def response_policy(
    snapshot: ContractSnapshot, operation: OperationSpec, primitive: str
) -> CompiledResponsePolicy:
    version = snapshot.ds_version
    contract = workflow_contract(version).lineage
    sources = {
        "lineage_list": contract.list_operation,
        "lineage_get": contract.get_operation,
        "lineage_dependent_tasks": contract.dependent_operation,
    }
    if (
        sources.get(primitive) != operation.operation_id
        or classify(operation) != primitive
    ):
        msg = "compiled lineage source identity or support changed"
        raise ValueError(msg)
    _validate_request(snapshot, operation, primitive)
    expected_projection = "single_data" if version in _CANONICAL else "direct"
    if operation.response_projection != expected_projection:
        msg = "compiled lineage response projection changed"
        raise ValueError(msg)
    dependent = primitive == "lineage_dependent_tasks"
    capture: JsonValue = (
        [] if dependent else {"workFlowList": [], "workFlowRelationList": []}
    )
    if version in _CANONICAL:
        expected = f"WorkflowLineageController_{operation.method_name}_result"
        data_type = f"List<{_ENTITY}DependentLineageTask>" if dependent else _GRAPH
        _require_fields(
            snapshot, _VIEW + expected, (("data", data_type, True, None, None),)
        )
        if dependent:
            _require_fields(
                snapshot,
                _ENTITY + "DependentLineageTask",
                (
                    ("projectCode", "long", False, "0", None),
                    ("workflowDefinitionCode", "long", False, "0", None),
                    ("workflowDefinitionName", "String", True, None, None),
                    ("taskDefinitionCode", "long", False, "0", None),
                    ("taskDefinitionName", "String", True, None, None),
                ),
            )
        else:
            _require_fields(
                snapshot,
                _GRAPH,
                (
                    ("workFlowRelationList", f"List<{_RELATION}>", True, None, None),
                    (
                        "workFlowRelationDetailList",
                        f"List<{_ENTITY}WorkFlowRelationDetail>",
                        True,
                        None,
                        None,
                    ),
                ),
            )
            _require_fields(
                snapshot, _ENTITY + "WorkFlowRelationDetail", _DETAIL_FIELDS
            )
        capture = {
            "data": []
            if dependent
            else {"workFlowRelationList": [], "workFlowRelationDetailList": []}
        }
    elif dependent:
        expected = f"List<{_ENTITY}TaskMainInfo>"
        _require_fields(snapshot, _ENTITY + "TaskMainInfo", _TASK_MAIN_FIELDS)
    else:
        expected = f"WorkFlowLineageServiceImpl_{operation.method_name}_workFlowLists"
        collection = (
            "List"
            if primitive == "lineage_get"
            and version
            not in {
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
            else "Collection"
        )
        _require_fields(
            snapshot,
            _VIEW + expected,
            (
                ("workFlowList", f"{collection}<{_GRAPH}>", True, None, None),
                ("workFlowRelationList", f"Set<{_RELATION}>", True, None, None),
            ),
        )
        _require_fields(snapshot, _GRAPH, _DETAIL_FIELDS)
    if not dependent:
        _require_fields(
            snapshot,
            _RELATION,
            (
                ("sourceWorkFlowCode", "long", False, "0", None),
                ("targetWorkFlowCode", "long", False, "0", None),
            ),
        )
    if operation.logical_return_type != expected:
        msg = "compiled lineage response type changed"
        raise ValueError(msg)
    return CompiledResponsePolicy(
        codec=f"{primitive}_{version.replace('.', '_')}",
        schema="lineage_dependent" if dependent else "lineage_graph",
        capture=capture,
        content_addressed=True,
    )


def recipe_policy(codecs: Mapping[str, str]) -> str:
    matches = [
        version
        for version in REVIEWED_DS_VERSIONS
        if dict(codecs)
        == {
            primitive.name: f"{primitive.name}_{version.replace('.', '_')}"
            for primitive in PRIMITIVES
            if version not in primitive.absent_versions
        }
    ]
    if len(matches) != 1:
        msg = "compiled lineage codec recipe is incomplete or mixed"
        raise ValueError(msg)
    return workflow_contract(matches[0]).lineage.graph_projection.replace("-", "_")


def _validate_request(
    snapshot: ContractSnapshot, operation: OperationSpec, primitive: str
) -> None:
    version = snapshot.ds_version
    fields: list[tuple[str, str, str, bool | None, str | None]] = []
    if not (version == "3.2.2" and primitive == "lineage_dependent_tasks"):
        fields.append(
            (
                "projectCode",
                "long",
                "path_variable",
                True if version in _ANNOTATED else None,
                None,
            )
        )
    if primitive == "lineage_get":
        fields.append(
            (
                "workFlowCode",
                "long",
                "path_variable",
                None if version in _CANONICAL else True,
                None,
            )
        )
    elif primitive == "lineage_dependent_tasks":
        fields.extend(
            (
                (
                    "workFlowCode",
                    "Long" if version == "3.2.2" else "long",
                    "request_param",
                    True,
                    None,
                ),
                (
                    "taskCode",
                    "Long",
                    "request_param",
                    False,
                    "0" if version == "3.2.2" else None,
                ),
            )
        )
    actual = [
        (p.wire_name, p.java_type, p.binding, p.required, p.default_value)
        for p in operation.parameters
        if is_client_supplied_parameter(p)
    ]
    if operation.http_method != "GET" or operation.consumes or actual != fields:
        msg = "compiled lineage request fields changed"
        raise ValueError(msg)


_TASK_MAIN_FIELDS = (
    ("id", "long", False, "0", None),
    ("taskName", "String", True, None, None),
    ("taskCode", "long", False, "0", None),
    ("taskVersion", "int", False, "0", None),
    ("taskType", "String", True, None, None),
    ("taskCreateTime", "Date", True, None, None),
    ("taskUpdateTime", "Date", True, None, None),
    ("projectCode", "long", False, "0", None),
    ("processDefinitionCode", "long", False, "0", None),
    ("processDefinitionVersion", "int", False, "0", None),
    ("processDefinitionName", "String", True, None, None),
    (
        "processReleaseState",
        "org.apache.dolphinscheduler.common.enums.ReleaseState",
        True,
        None,
        None,
    ),
    ("upstreamTaskMap", "Map<Long, String>", True, None, None),
    ("upstreamTaskCode", "long", False, "0", None),
    ("upstreamTaskName", "String", True, None, None),
)


def _require_fields(
    snapshot: ContractSnapshot,
    import_path: str,
    fields: tuple[tuple[str, str, bool, str | None, str | None], ...],
) -> None:
    model = require_model(snapshot, import_path, domain="lineage")
    if model.extends is not None or model_field_facts(model) != fields:
        msg = f"compiled lineage {import_path} fields changed"
        raise ValueError(msg)


__all__ = [
    "PRIMITIVES",
    "SEMANTIC_ABSENT_VERSIONS",
    "SEMANTIC_OPERATIONS",
    "classify",
    "recipe_policy",
    "response_policy",
]
