"""Map reviewed semantic read bindings onto exact source inventories."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any, Literal

from ds_codegen import (
    alert_contract,
    governance_contract,
    observability_contract,
    resource_contract,
    runtime_instance_contract,
    security_contract,
    task_definition_cleanup_contract,
    task_definition_contract,
    task_group_contract,
    workflow_contract,
)
from ds_codegen.profile_ledger import reviewed_profile_versions

REVIEWED_DS_VERSIONS = reviewed_profile_versions()
PROJECT_CODE_IDENTITY_VERSIONS = (
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
    "3.2.2",
    "3.3.1",
    "3.3.2",
    "3.4.0",
    "3.4.1",
    "3.4.2",
    "3.4.3",
)

WireTypeSurface = Literal["dtos", "models", "enums"]
WIRE_TYPE_SURFACES: tuple[WireTypeSurface, ...] = ("dtos", "models", "enums")
EvidenceKind = Literal["controller", "ui", "mapper"]


@dataclass(frozen=True)
class WireTypeRef:
    """One reviewed member of an operation's transitive wire-type closure."""

    surface: WireTypeSurface
    key: str


@dataclass(frozen=True)
class SelectorSemantics:
    """Selector inputs consumed and identities exposed by one stable action."""

    resource: str
    consumed_selectors: tuple[str, ...]
    exposed_identities: tuple[str, ...]
    native_identity: str
    resolution: str
    parent_identity: str | None = None


@dataclass(frozen=True)
class EvidenceSource:
    """One source reviewed when establishing a semantic binding."""

    kind: EvidenceKind
    reference: str


@dataclass(frozen=True)
class ReviewedBinding:
    """Reviewed wire closure and selection contract for one stable action."""

    source_operations: tuple[str, ...]
    type_closure: tuple[WireTypeRef, ...]
    selector_semantics: tuple[SelectorSemantics, ...]
    evidence_sources: tuple[EvidenceSource, ...]
    paging_projection: tuple[tuple[str, bool], ...] = ()


@dataclass(frozen=True)
class _SemanticBindingSchema:
    """Fail-closed validation rules for one stable semantic operation."""

    minimum_source_operations: int
    selector_resources: frozenset[str]


_PROJECT_PAGE_OPERATION = "ProjectController.queryProjectListPaging"
_SCHEDULE_PAGE_OPERATION = "SchedulerController.queryScheduleListPaging"

_LEGACY_PROJECT_UI = (
    "dolphinscheduler-ui/src/js/conf/home/pages/projects/pages/list/_source/list.vue"
)
_LEGACY_WORKFLOW_UI = (
    "dolphinscheduler-ui/src/js/conf/home/pages/projects/pages/definition/"
    "pages/list/_source/list.vue"
)
_LEGACY_WORKFLOW_REF_UI = "dolphinscheduler-ui/src/js/conf/home/store/dag/actions.js"
_LEGACY_SCHEDULE_UI = (
    "dolphinscheduler-ui/src/js/conf/home/pages/projects/pages/definition/"
    "timing/_source/list.vue"
)
_PROJECT_UI = "dolphinscheduler-ui/src/service/modules/projects/index.ts"
_PROCESS_WORKFLOW_UI = (
    "dolphinscheduler-ui/src/service/modules/process-definition/index.ts"
)
_WORKFLOW_UI = "dolphinscheduler-ui/src/service/modules/workflow-definition/index.ts"
_SCHEDULE_UI = "dolphinscheduler-ui/src/service/modules/schedules/index.ts"
_LEGACY_TENANT_UI = (
    "dolphinscheduler-ui/src/js/conf/home/pages/security/pages/tenement/"
    "_source/createTenement.vue"
)
_TENANT_UI = "dolphinscheduler-ui/src/service/modules/tenants/index.ts"
_FAV_TASK_UI = "dolphinscheduler-ui/src/service/modules/dag-menu/index.ts"
_LEGACY_MONITOR_UI = "dolphinscheduler-ui/src/js/conf/home/store/monitor/actions.js"
_MONITOR_UI = "dolphinscheduler-ui/src/service/modules/monitor/index.ts"
_AUDIT_UI = "dolphinscheduler-ui/src/service/modules/audit/index.ts"


def _wire_types(
    *,
    dtos: tuple[str, ...] = (),
    models: tuple[str, ...] = (),
    enums: tuple[str, ...] = (),
) -> tuple[WireTypeRef, ...]:
    return tuple(
        sorted(
            (
                *(WireTypeRef("dtos", key) for key in dtos),
                *(WireTypeRef("models", key) for key in models),
                *(WireTypeRef("enums", key) for key in enums),
            ),
            key=lambda item: (item.surface, item.key),
        )
    )


def _merge_wire_types(
    *closures: tuple[WireTypeRef, ...],
) -> tuple[WireTypeRef, ...]:
    return tuple(
        sorted(
            {item for closure in closures for item in closure},
            key=lambda item: (item.surface, item.key),
        )
    )


def _evidence(
    source_operations: tuple[str, ...],
    ui_references: tuple[str, ...],
) -> tuple[EvidenceSource, ...]:
    return (
        EvidenceSource(
            "controller",
            ";".join(
                _controller_reference(operation) for operation in source_operations
            ),
        ),
        EvidenceSource("ui", ";".join(ui_references)),
    )


def _controller_reference(operation: str) -> str:
    controller, separator, method = operation.partition(".")
    if not separator or not controller or not method:
        message = f"invalid controller operation {operation!r}"
        raise ValueError(message)
    return (
        "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
        f"api/controller/{controller}.java#{method}"
    )


def _reviewed_read_bindings(
    *,
    project_get_operation: str,
    workflow_page_operation: str,
    workflow_ref_operation: str,
    workflow_get_operation: str,
    native_identity: str,
    workflow_parent_identity: str,
    project_type_closure: tuple[WireTypeRef, ...],
    workflow_page_type_closure: tuple[WireTypeRef, ...],
    workflow_get_type_closure: tuple[WireTypeRef, ...],
    project_ui: str,
    workflow_page_ui: str,
    workflow_ref_ui: str,
    schedule_ui: str,
) -> dict[str, ReviewedBinding]:
    project_page_operations = (_PROJECT_PAGE_OPERATION,)
    project_get_operations = (*project_page_operations, project_get_operation)
    workflow_page_operations = (*project_get_operations, workflow_page_operation)
    workflow_get_operations = (
        *project_get_operations,
        workflow_ref_operation,
        workflow_get_operation,
        _SCHEDULE_PAGE_OPERATION,
    )
    exposed_identities = ("name", native_identity)
    project_list_selector = SelectorSemantics(
        resource="project",
        consumed_selectors=(),
        exposed_identities=exposed_identities,
        native_identity=native_identity,
        resolution="paged-search-discovery",
    )
    project_get_selector = SelectorSemantics(
        resource="project",
        consumed_selectors=exposed_identities,
        exposed_identities=exposed_identities,
        native_identity=native_identity,
        resolution="direct-native-or-exact-name-via-project.page",
    )
    workflow_list_selector = SelectorSemantics(
        resource="workflow",
        consumed_selectors=(),
        exposed_identities=exposed_identities,
        native_identity=native_identity,
        resolution="paged-search-discovery",
        parent_identity=workflow_parent_identity,
    )
    workflow_get_selector = SelectorSemantics(
        resource="workflow",
        consumed_selectors=exposed_identities,
        exposed_identities=exposed_identities,
        native_identity=native_identity,
        resolution="direct-native-or-exact-name-via-workflow.refs",
        parent_identity=workflow_parent_identity,
    )
    return {
        "project.page": ReviewedBinding(
            source_operations=project_page_operations,
            type_closure=project_type_closure,
            selector_semantics=(project_list_selector,),
            evidence_sources=_evidence(project_page_operations, (project_ui,)),
        ),
        "project.get": ReviewedBinding(
            source_operations=project_get_operations,
            type_closure=project_type_closure,
            selector_semantics=(project_get_selector,),
            evidence_sources=_evidence(project_get_operations, (project_ui,)),
        ),
        "workflow.page": ReviewedBinding(
            source_operations=workflow_page_operations,
            type_closure=workflow_page_type_closure,
            selector_semantics=(project_get_selector, workflow_list_selector),
            evidence_sources=_evidence(
                workflow_page_operations,
                (project_ui, workflow_page_ui),
            ),
        ),
        "workflow.get": ReviewedBinding(
            source_operations=workflow_get_operations,
            type_closure=workflow_get_type_closure,
            selector_semantics=(project_get_selector, workflow_get_selector),
            evidence_sources=_evidence(
                workflow_get_operations,
                (project_ui, workflow_ref_ui, schedule_ui),
            ),
        ),
    }


# PageInfo and the 1.3.x domain models are source-real wire members even though
# the current legacy extractor does not discover them. Keeping them here makes
# that evidence gap explicit instead of silently producing a false match.
_PROJECT_TYPE_CLOSURE = _wire_types(
    models=(
        "org.apache.dolphinscheduler.api.utils.PageInfo",
        "org.apache.dolphinscheduler.api.utils.Result",
        "org.apache.dolphinscheduler.dao.entity.Project",
    )
)

_SCHEDULE_ENUM_TYPES = _wire_types(
    enums=(
        "org.apache.dolphinscheduler.common.enums.FailureStrategy",
        "org.apache.dolphinscheduler.common.enums.Priority",
        "org.apache.dolphinscheduler.common.enums.ReleaseState",
        "org.apache.dolphinscheduler.common.enums.WarningType",
    )
)
_SCHEDULE_ENTITY_TYPES = _merge_wire_types(
    _SCHEDULE_ENUM_TYPES,
    _wire_types(models=("org.apache.dolphinscheduler.dao.entity.Schedule",)),
)
_SCHEDULE_VO_TYPES = _merge_wire_types(
    _SCHEDULE_ENUM_TYPES,
    _wire_types(models=("org.apache.dolphinscheduler.api.vo.ScheduleVo",)),
)
_SCHEDULE_UPPER_VO_TYPES = _merge_wire_types(
    _SCHEDULE_ENUM_TYPES,
    _wire_types(models=("org.apache.dolphinscheduler.api.vo.ScheduleVO",)),
)

_LEGACY_WORKFLOW_PAGE_TYPES = _merge_wire_types(
    _PROJECT_TYPE_CLOSURE,
    _wire_types(
        models=(
            "org.apache.dolphinscheduler.common.process.Property",
            "org.apache.dolphinscheduler.dao.entity.ProcessDefinition",
        ),
        enums=(
            "org.apache.dolphinscheduler.common.enums.DataType",
            "org.apache.dolphinscheduler.common.enums.Direct",
            "org.apache.dolphinscheduler.common.enums.Flag",
            "org.apache.dolphinscheduler.common.enums.ReleaseState",
        ),
    ),
)

_LEGACY_WORKFLOW_GET_TYPES = _merge_wire_types(
    _LEGACY_WORKFLOW_PAGE_TYPES,
    _SCHEDULE_ENTITY_TYPES,
)

_PROCESS_WORKFLOW_REF_TYPES = _wire_types(
    models=(
        "generated.view."
        "ProcessDefinitionServiceImpl_"
        "queryProcessDefinitionSimpleList_arrayNodeItem",
    )
)

_WORKFLOW_WORKFLOW_REF_TYPES = _wire_types(
    models=(
        "generated.view."
        "WorkflowDefinitionServiceImpl_"
        "queryWorkflowDefinitionSimpleList_arrayNodeItem",
    )
)

_PROCESS_2_0_WORKFLOW_GET_TYPES = _merge_wire_types(
    _LEGACY_WORKFLOW_PAGE_TYPES,
    _SCHEDULE_ENTITY_TYPES,
    _PROCESS_WORKFLOW_REF_TYPES,
    _wire_types(
        models=(
            "org.apache.dolphinscheduler.dao.entity.DagData",
            "org.apache.dolphinscheduler.dao.entity.ProcessTaskRelation",
            "org.apache.dolphinscheduler.dao.entity.TaskDefinition",
        ),
        enums=(
            "org.apache.dolphinscheduler.common.enums.ConditionType",
            "org.apache.dolphinscheduler.common.enums.Priority",
            "org.apache.dolphinscheduler.common.enums.TaskTimeoutStrategy",
            "org.apache.dolphinscheduler.common.enums.TimeoutFlag",
        ),
    ),
)

_PROCESS_3_0_WORKFLOW_PAGE_TYPES = _merge_wire_types(
    _PROJECT_TYPE_CLOSURE,
    _wire_types(
        models=(
            "org.apache.dolphinscheduler.dao.entity.ProcessDefinition",
            "org.apache.dolphinscheduler.plugin.task.api.model.Property",
        ),
        enums=(
            "org.apache.dolphinscheduler.common.enums.Flag",
            "org.apache.dolphinscheduler.common.enums.ProcessExecutionTypeEnum",
            "org.apache.dolphinscheduler.common.enums.ReleaseState",
            "org.apache.dolphinscheduler.plugin.task.api.enums.DataType",
            "org.apache.dolphinscheduler.plugin.task.api.enums.Direct",
        ),
    ),
)

_PROCESS_3_0_WORKFLOW_GET_CORE_TYPES = _merge_wire_types(
    _PROCESS_3_0_WORKFLOW_PAGE_TYPES,
    _PROCESS_WORKFLOW_REF_TYPES,
    _wire_types(
        models=(
            "org.apache.dolphinscheduler.dao.entity.DagData",
            "org.apache.dolphinscheduler.dao.entity.ProcessTaskRelation",
            "org.apache.dolphinscheduler.dao.entity.TaskDefinition",
        ),
        enums=(
            "org.apache.dolphinscheduler.common.enums.ConditionType",
            "org.apache.dolphinscheduler.common.enums.Priority",
            "org.apache.dolphinscheduler.common.enums.TimeoutFlag",
            "org.apache.dolphinscheduler.plugin.task.api.enums.TaskTimeoutStrategy",
        ),
    ),
)

_PROCESS_3_0_WORKFLOW_GET_TYPES = _merge_wire_types(
    _PROCESS_3_0_WORKFLOW_GET_CORE_TYPES,
    _SCHEDULE_VO_TYPES,
)

_PROCESS_3_1_WORKFLOW_GET_CORE_TYPES = _merge_wire_types(
    _PROCESS_3_0_WORKFLOW_GET_CORE_TYPES,
    _wire_types(
        enums=("org.apache.dolphinscheduler.common.enums.TaskExecuteType",),
    ),
)

_PROCESS_3_1_WORKFLOW_GET_TYPES = _merge_wire_types(
    _PROCESS_3_1_WORKFLOW_GET_CORE_TYPES,
    _SCHEDULE_VO_TYPES,
)

_PROCESS_3_2_1_WORKFLOW_PAGE_TYPES = _merge_wire_types(
    _PROCESS_3_0_WORKFLOW_PAGE_TYPES,
    _SCHEDULE_ENTITY_TYPES,
)

_PROCESS_3_2_1_WORKFLOW_GET_TYPES = _merge_wire_types(
    _PROCESS_3_2_1_WORKFLOW_PAGE_TYPES,
    _PROCESS_3_1_WORKFLOW_GET_CORE_TYPES,
    _SCHEDULE_UPPER_VO_TYPES,
)

_WORKFLOW_3_3_PAGE_TYPES = _merge_wire_types(
    _PROJECT_TYPE_CLOSURE,
    _wire_types(
        models=(
            "org.apache.dolphinscheduler.dao.entity.Schedule",
            "org.apache.dolphinscheduler.dao.entity.WorkflowDefinition",
            "org.apache.dolphinscheduler.plugin.task.api.model.Property",
        ),
        enums=(
            "org.apache.dolphinscheduler.common.enums.FailureStrategy",
            "org.apache.dolphinscheduler.common.enums.Flag",
            "org.apache.dolphinscheduler.common.enums.Priority",
            "org.apache.dolphinscheduler.common.enums.ReleaseState",
            "org.apache.dolphinscheduler.common.enums.WarningType",
            "org.apache.dolphinscheduler.common.enums.WorkflowExecutionTypeEnum",
            "org.apache.dolphinscheduler.plugin.task.api.enums.DataType",
            "org.apache.dolphinscheduler.plugin.task.api.enums.Direct",
        ),
    ),
)

_WORKFLOW_3_3_GET_TYPES = _merge_wire_types(
    _WORKFLOW_3_3_PAGE_TYPES,
    _WORKFLOW_WORKFLOW_REF_TYPES,
    _SCHEDULE_UPPER_VO_TYPES,
    _wire_types(
        models=(
            "org.apache.dolphinscheduler.dao.entity.DagData",
            "org.apache.dolphinscheduler.dao.entity.TaskDefinition",
            "org.apache.dolphinscheduler.dao.entity.WorkflowTaskRelation",
        ),
        enums=(
            "org.apache.dolphinscheduler.common.enums.ConditionType",
            "org.apache.dolphinscheduler.common.enums.TaskExecuteType",
            "org.apache.dolphinscheduler.common.enums.TimeoutFlag",
            "org.apache.dolphinscheduler.plugin.task.api.enums.TaskTimeoutStrategy",
        ),
    ),
)

READ_OPERATION_BINDINGS: dict[str, dict[str, ReviewedBinding]] = {
    "1.3.9": _reviewed_read_bindings(
        project_get_operation="ProjectController.queryProjectById",
        workflow_page_operation=(
            "ProcessDefinitionController.queryProcessDefinitionListPaging"
        ),
        workflow_ref_operation=(
            "ProcessDefinitionController.queryProcessDefinitionList"
        ),
        workflow_get_operation=(
            "ProcessDefinitionController.queryProcessDefinitionById"
        ),
        native_identity="id",
        workflow_parent_identity="project_name",
        project_type_closure=_PROJECT_TYPE_CLOSURE,
        workflow_page_type_closure=_LEGACY_WORKFLOW_PAGE_TYPES,
        workflow_get_type_closure=_LEGACY_WORKFLOW_GET_TYPES,
        project_ui=_LEGACY_PROJECT_UI,
        workflow_page_ui=_LEGACY_WORKFLOW_UI,
        workflow_ref_ui=_LEGACY_WORKFLOW_REF_UI,
        schedule_ui=_LEGACY_SCHEDULE_UI,
    ),
    **{
        version: _reviewed_read_bindings(
            project_get_operation="ProjectController.queryProjectByCode",
            workflow_page_operation=(
                "ProcessDefinitionController.queryProcessDefinitionListPaging"
            ),
            workflow_ref_operation=(
                "ProcessDefinitionController.queryProcessDefinitionSimpleList"
            ),
            workflow_get_operation=(
                "ProcessDefinitionController.queryProcessDefinitionByCode"
            ),
            native_identity="code",
            workflow_parent_identity="project_code",
            project_type_closure=_PROJECT_TYPE_CLOSURE,
            workflow_page_type_closure=_LEGACY_WORKFLOW_PAGE_TYPES,
            workflow_get_type_closure=_PROCESS_2_0_WORKFLOW_GET_TYPES,
            project_ui=_LEGACY_PROJECT_UI,
            workflow_page_ui=_LEGACY_WORKFLOW_UI,
            workflow_ref_ui=_LEGACY_WORKFLOW_REF_UI,
            schedule_ui=_LEGACY_SCHEDULE_UI,
        )
        for version in (
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
        )
    },
    **{
        version: _reviewed_read_bindings(
            project_get_operation="ProjectController.queryProjectByCode",
            workflow_page_operation=(
                "ProcessDefinitionController.queryProcessDefinitionListPaging"
            ),
            workflow_ref_operation=(
                "ProcessDefinitionController.queryProcessDefinitionSimpleList"
            ),
            workflow_get_operation=(
                "ProcessDefinitionController.queryProcessDefinitionByCode"
            ),
            native_identity="code",
            workflow_parent_identity="project_code",
            project_type_closure=_PROJECT_TYPE_CLOSURE,
            workflow_page_type_closure=_PROCESS_3_0_WORKFLOW_PAGE_TYPES,
            workflow_get_type_closure=_PROCESS_3_0_WORKFLOW_GET_TYPES,
            project_ui=_PROJECT_UI,
            workflow_page_ui=_PROCESS_WORKFLOW_UI,
            workflow_ref_ui=_PROCESS_WORKFLOW_UI,
            schedule_ui=_SCHEDULE_UI,
        )
        for version in ("3.0.0", "3.0.1", "3.0.2", "3.0.3", "3.0.4", "3.0.5", "3.0.6")
    },
    **{
        version: _reviewed_read_bindings(
            project_get_operation="ProjectController.queryProjectByCode",
            workflow_page_operation=(
                "ProcessDefinitionController.queryProcessDefinitionListPaging"
            ),
            workflow_ref_operation=(
                "ProcessDefinitionController.queryProcessDefinitionSimpleList"
            ),
            workflow_get_operation=(
                "ProcessDefinitionController.queryProcessDefinitionByCode"
            ),
            native_identity="code",
            workflow_parent_identity="project_code",
            project_type_closure=_PROJECT_TYPE_CLOSURE,
            workflow_page_type_closure=_PROCESS_3_0_WORKFLOW_PAGE_TYPES,
            workflow_get_type_closure=_PROCESS_3_1_WORKFLOW_GET_TYPES,
            project_ui=_PROJECT_UI,
            workflow_page_ui=_PROCESS_WORKFLOW_UI,
            workflow_ref_ui=_PROCESS_WORKFLOW_UI,
            schedule_ui=_SCHEDULE_UI,
        )
        for version in (
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
        )
    },
    "3.2.0": _reviewed_read_bindings(
        project_get_operation="ProjectController.queryProjectByCode",
        workflow_page_operation=(
            "ProcessDefinitionController.queryProcessDefinitionListPaging"
        ),
        workflow_ref_operation=(
            "ProcessDefinitionController.queryProcessDefinitionSimpleList"
        ),
        workflow_get_operation=(
            "ProcessDefinitionController.queryProcessDefinitionByCode"
        ),
        native_identity="code",
        workflow_parent_identity="project_code",
        project_type_closure=_PROJECT_TYPE_CLOSURE,
        workflow_page_type_closure=_PROCESS_3_0_WORKFLOW_PAGE_TYPES,
        workflow_get_type_closure=_PROCESS_3_1_WORKFLOW_GET_TYPES,
        project_ui=_PROJECT_UI,
        workflow_page_ui=_PROCESS_WORKFLOW_UI,
        workflow_ref_ui=_PROCESS_WORKFLOW_UI,
        schedule_ui=_SCHEDULE_UI,
    ),
    **{
        version: _reviewed_read_bindings(
            project_get_operation="ProjectController.queryProjectByCode",
            workflow_page_operation=(
                "ProcessDefinitionController.queryProcessDefinitionListPaging"
            ),
            workflow_ref_operation=(
                "ProcessDefinitionController.queryProcessDefinitionSimpleList"
            ),
            workflow_get_operation=(
                "ProcessDefinitionController.queryProcessDefinitionByCode"
            ),
            native_identity="code",
            workflow_parent_identity="project_code",
            project_type_closure=_PROJECT_TYPE_CLOSURE,
            workflow_page_type_closure=_PROCESS_3_2_1_WORKFLOW_PAGE_TYPES,
            workflow_get_type_closure=_PROCESS_3_2_1_WORKFLOW_GET_TYPES,
            project_ui=_PROJECT_UI,
            workflow_page_ui=_PROCESS_WORKFLOW_UI,
            workflow_ref_ui=_PROCESS_WORKFLOW_UI,
            schedule_ui=_SCHEDULE_UI,
        )
        for version in ("3.2.1", "3.2.2")
    },
    **{
        version: _reviewed_read_bindings(
            project_get_operation="ProjectController.queryProjectByCode",
            workflow_page_operation=(
                "WorkflowDefinitionController.queryWorkflowDefinitionListPaging"
            ),
            workflow_ref_operation=(
                "WorkflowDefinitionController.queryWorkflowDefinitionSimpleList"
            ),
            workflow_get_operation=(
                "WorkflowDefinitionController.queryWorkflowDefinitionByCode"
            ),
            native_identity="code",
            workflow_parent_identity="project_code",
            project_type_closure=_PROJECT_TYPE_CLOSURE,
            workflow_page_type_closure=_WORKFLOW_3_3_PAGE_TYPES,
            workflow_get_type_closure=_WORKFLOW_3_3_GET_TYPES,
            project_ui=_PROJECT_UI,
            workflow_page_ui=_WORKFLOW_UI,
            workflow_ref_ui=_WORKFLOW_UI,
            schedule_ui=_SCHEDULE_UI,
        )
        for version in ("3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2", "3.4.3")
    },
}


_CURRENT_USER_TYPE_CLOSURE = _wire_types(
    models=(
        "org.apache.dolphinscheduler.api.utils.Result",
        "org.apache.dolphinscheduler.dao.entity.User",
    ),
    enums=("org.apache.dolphinscheduler.common.enums.UserType",),
)
_LEGACY_USERS_UI = "dolphinscheduler-ui/src/js/conf/home/store/user/actions.js"
_USERS_UI = "dolphinscheduler-ui/src/service/modules/users/index.ts"
_LEGACY_ACCESS_TOKEN_UI = (
    "dolphinscheduler-ui/src/js/conf/home/pages/user/pages/token/_source/list.vue"  # noqa: S105
)
_ACCESS_TOKEN_UI = "dolphinscheduler-ui/src/service/modules/token/index.ts"  # noqa: S105
_TASK_DEFINITION_UI = (
    "dolphinscheduler-ui/src/views/projects/task/components/node/format-data.ts"
)
_PROJECT_CODE_IDENTITIES = ("name", "code")
_PROJECT_ID_IDENTITIES = ("name", "id")
_PROJECT_CREATE_DIRECT_OPERATIONS = ("ProjectController.createProject",)
_PROJECT_CREATE_139_OPERATIONS = (
    "ProjectController.createProject",
    "ProjectController.queryProjectListPaging",
    "ProjectController.queryProjectById",
)
_PROJECT_CREATE_200_OPERATIONS = (
    "ProjectController.createProject",
    "ProjectController.queryProjectCreatedAndAuthorizedByUser",
    "ProjectController.queryProjectByCode",
)
_PROJECT_UPDATE_OPERATIONS = (
    "ProjectController.queryProjectListPaging",
    "ProjectController.queryProjectByCode",
    "ProjectController.updateProject",
)
_PROJECT_DELETE_OPERATIONS = (
    "ProjectController.queryProjectListPaging",
    "ProjectController.queryProjectByCode",
    "ProjectController.deleteProject",
)
_PROJECT_UPDATE_139_OPERATIONS = (
    "ProjectController.queryProjectListPaging",
    "ProjectController.queryProjectById",
    "ProjectController.updateProject",
)
_PROJECT_DELETE_139_OPERATIONS = (
    "ProjectController.queryProjectListPaging",
    "ProjectController.queryProjectById",
    "ProjectController.deleteProject",
)

_PROJECT_RUNTIME_OPERATION_RECIPES: dict[
    str,
    dict[str, tuple[str, ...]],
] = {
    "1.3.9": {
        "project.create": _PROJECT_CREATE_139_OPERATIONS,
        "project.update": _PROJECT_UPDATE_139_OPERATIONS,
        "project.delete": _PROJECT_DELETE_139_OPERATIONS,
    },
    "2.0.0": {
        "project.create": _PROJECT_CREATE_200_OPERATIONS,
        "project.update": _PROJECT_UPDATE_OPERATIONS,
        "project.delete": _PROJECT_DELETE_OPERATIONS,
    },
    "2.0.1": {
        "project.create": _PROJECT_CREATE_200_OPERATIONS,
        "project.update": _PROJECT_UPDATE_OPERATIONS,
        "project.delete": _PROJECT_DELETE_OPERATIONS,
    },
    **{
        version: {
            "project.create": _PROJECT_CREATE_DIRECT_OPERATIONS,
            "project.update": _PROJECT_UPDATE_OPERATIONS,
            "project.delete": _PROJECT_DELETE_OPERATIONS,
        }
        for version in PROJECT_CODE_IDENTITY_VERSIONS
        if version not in {"2.0.0", "2.0.1"}
    },
}


def _project_create_selector(version: str) -> SelectorSemantics:
    legacy_id = version == "1.3.9"
    return SelectorSemantics(
        resource="project",
        consumed_selectors=("name",),
        exposed_identities=(
            _PROJECT_ID_IDENTITIES if legacy_id else _PROJECT_CODE_IDENTITIES
        ),
        native_identity="id" if legacy_id else "code",
        resolution=(
            "create-returned-id-verified-by-visible-page-before-detail-readback"
            if legacy_id
            else (
                "create-locate-by-returned-id-and-exact-name-readback"
                if version in {"2.0.0", "2.0.1"}
                else "create-and-return-native-identity"
            )
        ),
    )


def _project_ui_evidence(version: str) -> tuple[str, ...]:
    return tuple(
        source.reference
        for source in READ_OPERATION_BINDINGS[version]["project.page"].evidence_sources
        if source.kind == "ui"
    )


def _project_lifecycle_bindings(version: str) -> dict[str, ReviewedBinding]:
    """Compile one exact project's reviewed lifecycle dependency recipes."""
    project_get = READ_OPERATION_BINDINGS[version]["project.get"]
    recipes = _PROJECT_RUNTIME_OPERATION_RECIPES[version]

    def binding(
        semantic_operation: str,
        *,
        selector_semantics: tuple[SelectorSemantics, ...],
    ) -> ReviewedBinding:
        source_operations = recipes[semantic_operation]
        return ReviewedBinding(
            source_operations=source_operations,
            type_closure=project_get.type_closure,
            selector_semantics=selector_semantics,
            evidence_sources=_evidence(
                source_operations,
                _project_ui_evidence(version),
            ),
        )

    return {
        "project.create": binding(
            "project.create",
            selector_semantics=(_project_create_selector(version),),
        ),
        "project.delete": binding(
            "project.delete",
            selector_semantics=project_get.selector_semantics,
        ),
        "project.update": binding(
            "project.update",
            selector_semantics=project_get.selector_semantics,
        ),
    }


_PROJECT_PARAMETER_UI = (
    "dolphinscheduler-ui/src/service/modules/projects-parameter/index.ts"
)
_PROJECT_PREFERENCE_UI = (
    "dolphinscheduler-ui/src/service/modules/projects-preference/index.ts"
)
_PROJECT_WORKER_GROUP_UI = (
    "dolphinscheduler-ui/src/service/modules/projects-worker-group/index.ts"
)
_PROJECT_CONFIGURATION_VERSIONS = frozenset(
    REVIEWED_DS_VERSIONS[REVIEWED_DS_VERSIONS.index("3.2.0") :]
)
_PROJECT_WORKER_GROUP_VERSIONS = frozenset(
    REVIEWED_DS_VERSIONS[REVIEWED_DS_VERSIONS.index("3.2.2") :]
)


def _project_configuration_bindings(version: str) -> dict[str, ReviewedBinding]:
    """Compile exact project-scoped configuration recipes when upstream exists."""
    if version not in _PROJECT_CONFIGURATION_VERSIONS:
        return {}
    project_get = READ_OPERATION_BINDINGS[version]["project.get"]
    project_selector = project_get.selector_semantics[0]

    def selector(resource: str) -> SelectorSemantics:
        return SelectorSemantics(
            resource=resource,
            consumed_selectors=("name", "code"),
            exposed_identities=("name", "code"),
            native_identity="code",
            resolution=f"exact-name-or-code-via-{resource}.page",
            parent_identity="project_code",
        )

    def singleton_selector(resource: str) -> SelectorSemantics:
        return SelectorSemantics(
            resource=resource,
            consumed_selectors=(),
            exposed_identities=("project_code",),
            native_identity="project_code",
            resolution="singleton-by-resolved-project",
            parent_identity="project_code",
        )

    def binding(
        source_operations: tuple[str, ...],
        *,
        resource: str,
        type_model: str,
        ui: str,
        singleton: bool = False,
    ) -> ReviewedBinding:
        return ReviewedBinding(
            source_operations=source_operations,
            type_closure=_merge_wire_types(
                project_get.type_closure,
                _wire_types(models=(type_model,)),
            ),
            selector_semantics=(
                project_selector,
                singleton_selector(resource) if singleton else selector(resource),
            ),
            evidence_sources=_evidence(
                source_operations,
                (*_project_ui_evidence(version), ui),
            ),
        )

    project_ops = project_get.source_operations
    parameter_page = "ProjectParameterController.queryProjectParameterListPaging"
    parameter_get = "ProjectParameterController.queryProjectParameterByCode"
    parameter_model = "org.apache.dolphinscheduler.dao.entity.ProjectParameter"
    preference_get = "ProjectPreferenceController.queryProjectPreferenceByProjectCode"
    preference_model = "org.apache.dolphinscheduler.dao.entity.ProjectPreference"
    bindings = {
        "project-parameter.page": binding(
            (*project_ops, parameter_page),
            resource="project-parameter",
            type_model=parameter_model,
            ui=_PROJECT_PARAMETER_UI,
        ),
        "project-parameter.get": binding(
            (*project_ops, parameter_page, parameter_get),
            resource="project-parameter",
            type_model=parameter_model,
            ui=_PROJECT_PARAMETER_UI,
        ),
        "project-parameter.create": binding(
            (*project_ops, "ProjectParameterController.createProjectParameter"),
            resource="project-parameter",
            type_model=parameter_model,
            ui=_PROJECT_PARAMETER_UI,
        ),
        "project-parameter.update": binding(
            (
                *project_ops,
                parameter_page,
                parameter_get,
                "ProjectParameterController.updateProjectParameter",
            ),
            resource="project-parameter",
            type_model=parameter_model,
            ui=_PROJECT_PARAMETER_UI,
        ),
        "project-parameter.delete": binding(
            (
                *project_ops,
                parameter_page,
                "ProjectParameterController.deleteProjectParametersByCode",
            ),
            resource="project-parameter",
            type_model=parameter_model,
            ui=_PROJECT_PARAMETER_UI,
        ),
        "project-preference.get": binding(
            (*project_ops, preference_get),
            resource="project-preference",
            type_model=preference_model,
            ui=_PROJECT_PREFERENCE_UI,
            singleton=True,
        ),
        "project-preference.update": binding(
            (*project_ops, "ProjectPreferenceController.updateProjectPreference"),
            resource="project-preference",
            type_model=preference_model,
            ui=_PROJECT_PREFERENCE_UI,
            singleton=True,
        ),
        **{
            f"project-preference.{action}": binding(
                (
                    *project_ops,
                    "ProjectPreferenceController.enableProjectPreference",
                    preference_get,
                ),
                resource="project-preference",
                type_model=preference_model,
                ui=_PROJECT_PREFERENCE_UI,
                singleton=True,
            )
            for action in ("enable", "disable")
        },
    }
    if version not in _PROJECT_WORKER_GROUP_VERSIONS:
        return bindings
    worker_group_list = (
        "ProjectWorkerGroupController.queryWorkerGroups"
        if version == "3.2.2"
        else "ProjectWorkerGroupController.queryAssignedWorkerGroups"
    )
    worker_group_model = "org.apache.dolphinscheduler.dao.entity.ProjectWorkerGroup"
    bindings["project-worker-group.page"] = binding(
        (*project_ops, worker_group_list),
        resource="project-worker-group",
        type_model=worker_group_model,
        ui=_PROJECT_WORKER_GROUP_UI,
        singleton=True,
    )
    worker_group_mutations = ("set",) if version == "3.2.2" else ("set", "clear")
    for action in worker_group_mutations:
        bindings[f"project-worker-group.{action}"] = binding(
            (
                *project_ops,
                "ProjectWorkerGroupController.assignWorkerGroups",
                worker_group_list,
            ),
            resource="project-worker-group",
            type_model=worker_group_model,
            ui=_PROJECT_WORKER_GROUP_UI,
            singleton=True,
        )
    return bindings


_SCHEDULE_PAGE = "SchedulerController.queryScheduleListPaging"
_SCHEDULE_CREATE = "SchedulerController.createSchedule"
_SCHEDULE_PREVIEW = "SchedulerController.previewSchedule"
_SCHEDULE_DELETE = "SchedulerController.deleteScheduleById"
_SCHEDULE_UPDATE = "SchedulerController.updateSchedule"
_PROJECT_PREFERENCE_GET = (
    "ProjectPreferenceController.queryProjectPreferenceByProjectCode"
)
_PROJECT_PREFERENCE_VERSIONS = frozenset(
    REVIEWED_DS_VERSIONS[REVIEWED_DS_VERSIONS.index("3.2.0") :]
)


def _schedule_lifecycle_operations(version: str) -> tuple[str, str]:
    """Return the exact controller method names for offline and online."""
    if version in REVIEWED_DS_VERSIONS[: REVIEWED_DS_VERSIONS.index("3.1.0")]:
        return "SchedulerController.offline", "SchedulerController.online"
    return (
        "SchedulerController.offlineSchedule",
        "SchedulerController.publishScheduleOnline",
    )


def _schedule_type_closure(version: str) -> tuple[WireTypeRef, ...]:
    """Return the exact schedule page/mutation types for one release."""
    models = [
        "org.apache.dolphinscheduler.api.utils.PageInfo",
        "org.apache.dolphinscheduler.api.utils.Result",
    ]
    if (
        version in REVIEWED_DS_VERSIONS[: REVIEWED_DS_VERSIONS.index("3.0.0")]
        or version in REVIEWED_DS_VERSIONS[REVIEWED_DS_VERSIONS.index("3.2.0") :]
    ):
        models.append("org.apache.dolphinscheduler.dao.entity.Schedule")
    if (
        version
        in REVIEWED_DS_VERSIONS[
            REVIEWED_DS_VERSIONS.index("3.0.0") : REVIEWED_DS_VERSIONS.index("3.2.1")
        ]
    ):
        models.append("org.apache.dolphinscheduler.api.vo.ScheduleVo")
    if version in REVIEWED_DS_VERSIONS[REVIEWED_DS_VERSIONS.index("3.2.1") :]:
        models.append("org.apache.dolphinscheduler.api.vo.ScheduleVO")
    if version in _PROJECT_PREFERENCE_VERSIONS:
        models.append("org.apache.dolphinscheduler.dao.entity.ProjectPreference")
    if version == "3.4.3":
        models.append("org.apache.dolphinscheduler.api.dto.ScheduleParam")
    return _wire_types(
        models=tuple(models),
        enums=(
            "org.apache.dolphinscheduler.common.enums.FailureStrategy",
            "org.apache.dolphinscheduler.common.enums.Priority",
            "org.apache.dolphinscheduler.common.enums.ReleaseState",
            "org.apache.dolphinscheduler.common.enums.WarningType",
        ),
    )


def _schedule_bindings(version: str) -> dict[str, ReviewedBinding]:
    """Compile the reviewed main-controller schedule recipe for every profile."""
    offline_operation, online_operation = _schedule_lifecycle_operations(version)
    parent_identity = "project_name" if version == "1.3.9" else "project_code"
    list_selector = SelectorSemantics(
        resource="schedule",
        consumed_selectors=(),
        exposed_identities=("id",),
        native_identity="id",
        resolution="project-scoped-server-page",
        parent_identity=parent_identity,
    )
    selector = SelectorSemantics(
        resource="schedule",
        consumed_selectors=("id",),
        exposed_identities=("id",),
        native_identity="id",
        resolution="direct-id-with-project-scoped-page-readback",
        parent_identity=parent_identity,
    )
    create_selector = SelectorSemantics(
        resource="schedule",
        consumed_selectors=(),
        exposed_identities=("id",),
        native_identity="id",
        resolution="create-returned-id-plus-project-scoped-readback",
        parent_identity=parent_identity,
    )
    preview_selector = SelectorSemantics(
        resource="schedule",
        consumed_selectors=(),
        exposed_identities=("id",),
        native_identity="id",
        resolution="server-validated-cron-preview",
        parent_identity=parent_identity,
    )
    type_closure = _schedule_type_closure(version)
    project_preference = (
        (_PROJECT_PREFERENCE_GET,) if version in _PROJECT_PREFERENCE_VERSIONS else ()
    )
    ui_evidence = (
        _LEGACY_SCHEDULE_UI
        if version
        in {
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
        else _SCHEDULE_UI
    )

    def binding(
        source_operations: tuple[str, ...],
        selected: SelectorSemantics,
    ) -> ReviewedBinding:
        return ReviewedBinding(
            source_operations=source_operations,
            type_closure=type_closure,
            selector_semantics=(selected,),
            evidence_sources=_evidence(source_operations, (ui_evidence,)),
        )

    return {
        "schedule.page": binding((_SCHEDULE_PAGE,), list_selector),
        "schedule.get": binding((_SCHEDULE_PAGE,), selector),
        "schedule.create": binding(
            (
                _SCHEDULE_PREVIEW,
                *project_preference,
                _SCHEDULE_CREATE,
                _SCHEDULE_PAGE,
            ),
            create_selector,
        ),
        "schedule.delete": binding(
            (_SCHEDULE_PAGE, _SCHEDULE_DELETE),
            selector,
        ),
        "schedule.update": binding(
            (_SCHEDULE_PAGE, _SCHEDULE_PREVIEW, _SCHEDULE_UPDATE),
            selector,
        ),
        "schedule.offline": binding(
            (_SCHEDULE_PAGE, offline_operation),
            selector,
        ),
        "schedule.online": binding(
            (_SCHEDULE_PAGE, online_operation),
            selector,
        ),
        "schedule.preview": binding((_SCHEDULE_PREVIEW,), preview_selector),
        "schedule.explain": binding(
            (_SCHEDULE_PAGE, _SCHEDULE_PREVIEW, *project_preference),
            selector,
        ),
    }


def _task_definition_bindings(version: str) -> dict[str, ReviewedBinding]:
    """Compile exact task contracts with their project/workflow resolver closure."""
    recipe = task_definition_contract.task_definition_contract(version).task
    if not recipe.executable and not recipe.graph_backed:
        return {}
    workflow_get = READ_OPERATION_BINDINGS[version]["workflow.get"]
    workflow_operations = tuple(
        operation
        for operation in workflow_get.source_operations
        if operation != "SchedulerController.queryScheduleListPaging"
    )
    sources = task_definition_contract.semantic_operation_sources(version)
    roots = task_definition_contract.semantic_operation_type_roots(version)
    evidence = task_definition_contract.semantic_operation_evidence(version)
    resolver_ui_references = tuple(
        reference
        for source in workflow_get.evidence_sources
        if source.kind == "ui"
        for reference in source.reference.split(";")
        if reference not in {_LEGACY_SCHEDULE_UI, _SCHEDULE_UI}
    )
    bindings: dict[str, ReviewedBinding] = {}
    for semantic_operation in task_definition_contract.TASK_SEMANTIC_OPERATIONS:
        if (
            semantic_operation == "task.update"
            and not recipe.update_executable
            and not recipe.whole_workflow_update
            and not recipe.graph_backed
        ):
            continue
        source_operations = tuple(
            dict.fromkeys((*workflow_operations, *sources[semantic_operation]))
        )
        legacy = recipe.graph_backed
        task_selector = SelectorSemantics(
            resource="task",
            consumed_selectors=(
                ()
                if semantic_operation == "task.list"
                else ("name",)
                if legacy
                else ("name", "code")
            ),
            exposed_identities=("name", "id") if legacy else ("name", "code"),
            native_identity="id" if legacy else "code",
            resolution=(
                "enumerated-within-legacy-workflow-graph"
                if legacy and semantic_operation == "task.list"
                else "exact-name-within-legacy-workflow-graph"
                if legacy
                else "enumerated-within-workflow-dag"
                if semantic_operation == "task.list"
                else "direct-native-or-exact-name-via-workflow-dag"
            ),
            parent_identity=("process_definition_id" if legacy else "workflow_code"),
        )
        ui_references = tuple(
            dict.fromkeys(
                (
                    *resolver_ui_references,
                    *(
                        item.reference
                        for item in evidence[semantic_operation]
                        if item.kind == "ui"
                    ),
                )
            )
        )
        bindings[semantic_operation] = ReviewedBinding(
            source_operations=source_operations,
            type_closure=_wire_types(models=roots[semantic_operation]),
            selector_semantics=(*workflow_get.selector_semantics, task_selector),
            evidence_sources=_evidence(source_operations, ui_references),
        )
    return bindings


def _identity_bindings(version: str) -> dict[str, ReviewedBinding]:
    """Bind the exact current-user endpoint and its complete wire closure."""
    users_ui = (
        _LEGACY_USERS_UI
        if version
        in {
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
        else _USERS_UI
    )
    source_operations = ("UsersController.getUserInfo",)
    return {
        "identity.current": ReviewedBinding(
            source_operations=source_operations,
            type_closure=_CURRENT_USER_TYPE_CLOSURE,
            selector_semantics=(),
            evidence_sources=_evidence(source_operations, (users_ui,)),
        )
    }


_TENANT_TYPE_CLOSURE = _wire_types(
    models=(
        "org.apache.dolphinscheduler.api.utils.PageInfo",
        "org.apache.dolphinscheduler.api.utils.Result",
        "org.apache.dolphinscheduler.dao.entity.Queue",
        "org.apache.dolphinscheduler.dao.entity.Tenant",
    )
)
_LEGACY_TENANT_METHOD_VERSIONS = frozenset(
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
    }
)
_QUEUE_PAGE_OPERATION = "QueueController.queryQueueListPaging"
_TENANT_CREATE_OPERATION = "TenantController.createTenant"
_TENANT_UPDATE_OPERATION = "TenantController.updateTenant"
_TENANT_DELETE_OPERATION = "TenantController.deleteTenantById"


def _tenant_page_operation(version: str) -> str:
    return (
        "TenantController.queryTenantlistPaging"
        if version in _LEGACY_TENANT_METHOD_VERSIONS
        else "TenantController.queryTenantListPaging"
    )


def _tenant_ui_evidence(version: str) -> tuple[str, ...]:
    return (
        (_LEGACY_TENANT_UI,)
        if version
        in {
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
        else (_TENANT_UI,)
    )


def _tenant_lifecycle_bindings(version: str) -> dict[str, ReviewedBinding]:
    """Compile tenant-plus-queue recipes from exact controller/UI evidence."""
    tenant_page = _tenant_page_operation(version)
    tenant_list_selector = SelectorSemantics(
        resource="tenant",
        consumed_selectors=(),
        exposed_identities=("tenant_code", "id"),
        native_identity="id",
        resolution="paged-search-discovery",
    )
    tenant_selector = SelectorSemantics(
        resource="tenant",
        consumed_selectors=("tenant_code", "id"),
        exposed_identities=("tenant_code", "id"),
        native_identity="id",
        resolution="direct-id-or-exact-code-via-tenant.page",
    )
    create_selector = SelectorSemantics(
        resource="tenant",
        consumed_selectors=("tenant_code",),
        exposed_identities=("tenant_code", "id"),
        native_identity="id",
        resolution="create-with-exact-code-readback",
    )
    queue_selector = SelectorSemantics(
        resource="queue",
        consumed_selectors=("name", "id"),
        exposed_identities=("name", "id"),
        native_identity="id",
        resolution="direct-id-or-exact-name-via-queue.page",
    )
    ui_evidence = _tenant_ui_evidence(version)

    def binding(
        source_operations: tuple[str, ...],
        selectors: tuple[SelectorSemantics, ...],
    ) -> ReviewedBinding:
        return ReviewedBinding(
            source_operations=source_operations,
            type_closure=_TENANT_TYPE_CLOSURE,
            selector_semantics=selectors,
            evidence_sources=_evidence(source_operations, ui_evidence),
        )

    return {
        "tenant.page": binding((tenant_page,), (tenant_list_selector,)),
        "tenant.get": binding((tenant_page,), (tenant_selector,)),
        "tenant.create": binding(
            (_QUEUE_PAGE_OPERATION, _TENANT_CREATE_OPERATION, tenant_page),
            (create_selector, queue_selector),
        ),
        "tenant.update": binding(
            (tenant_page, _QUEUE_PAGE_OPERATION, _TENANT_UPDATE_OPERATION),
            (tenant_selector, queue_selector),
        ),
        "tenant.delete": binding(
            (tenant_page, _TENANT_DELETE_OPERATION),
            (tenant_selector,),
        ),
    }


_SECURITY_COMMON_MODELS = (
    "org.apache.dolphinscheduler.api.utils.PageInfo",
    "org.apache.dolphinscheduler.api.utils.Result",
)
_SECURITY_COMMON_ENUMS = ("org.apache.dolphinscheduler.common.enums.UserType",)


def _security_selector(
    resource: str,
    *,
    consumed: tuple[str, ...],
    exposed: tuple[str, ...],
    native: str,
    resolution: str,
) -> SelectorSemantics:
    return SelectorSemantics(
        resource=resource,
        consumed_selectors=consumed,
        exposed_identities=exposed,
        native_identity=native,
        resolution=resolution,
    )


def _security_selectors(
    version: str,
    semantic_operation: str,
) -> tuple[SelectorSemantics, ...]:
    """Describe the caller identities resolved by one security-domain recipe."""
    if semantic_operation.startswith("access-token."):
        action_suffix = semantic_operation.removeprefix("access-token.")
        if action_suffix == "generate":
            token_selector = _security_selector(
                "access-token",
                consumed=(),
                exposed=("token",),
                native="token",
                resolution="generate-nonpersisted-token",
            )
        else:
            token_selector = _security_selector(
                "access-token",
                consumed=(
                    ("id",) if action_suffix in {"get", "update", "delete"} else ()
                ),
                exposed=("id",),
                native="id",
                resolution=(
                    "server-page"
                    if action_suffix == "list"
                    else "create-plus-page-readback"
                    if action_suffix == "create"
                    else "direct-id-via-bounded-page"
                ),
            )
        if action_suffix not in {"create", "update", "generate"}:
            return (token_selector,)
        user_selector = _security_selector(
            "user",
            consumed=("name", "id"),
            exposed=("name", "id"),
            native="id",
            resolution=(
                "current-user-or-exact-identity-via-user-simple-list"
                if security_contract.security_contract(version).user.simple_user_list
                else "direct-id-or-exact-name-via-user-read-merge"
            ),
        )
        return (token_selector, user_selector)

    user_operation = semantic_operation.removeprefix("user.")
    user_selector = _security_selector(
        "user",
        consumed=(
            ()
            if user_operation == "list"
            else ("name",)
            if user_operation == "create"
            else ("name", "id")
        ),
        exposed=("name", "id"),
        native="id",
        resolution=(
            "server-page"
            if user_operation == "list"
            else "create-plus-merged-readback"
            if user_operation == "create"
            else "current-user-or-merged-full-user-read"
            if security_contract.security_contract(version).user.simple_user_list
            and user_operation in {"get", "update"}
            else "current-user-or-exact-identity-via-user-simple-list"
            if security_contract.security_contract(version).user.simple_user_list
            else "direct-id-or-exact-name-via-user-read-merge"
        ),
    )
    selectors = [user_selector]
    if semantic_operation in {"user.create", "user.update"}:
        selectors.append(
            _security_selector(
                "tenant",
                consumed=("tenant_code", "id"),
                exposed=("tenant_code", "id"),
                native="id",
                resolution="direct-id-or-exact-code-via-tenant-page",
            )
        )
    elif semantic_operation.endswith(".project"):
        project_identity = security_contract.security_contract(
            version
        ).user.project_identity
        selectors.append(
            _security_selector(
                "project",
                consumed=("name", project_identity),
                exposed=("name", project_identity),
                native=project_identity,
                resolution="exact-permission-set-member",
            )
        )
    elif semantic_operation.endswith(".datasource"):
        selectors.append(
            _security_selector(
                "datasource",
                consumed=("name", "id"),
                exposed=("name", "id"),
                native="id",
                resolution="exact-authorized-or-unauthorized-set-member",
            )
        )
    elif semantic_operation.endswith(".namespace"):
        selectors.append(
            _security_selector(
                "namespace",
                consumed=("name", "id"),
                exposed=("name", "id"),
                native="id",
                resolution="exact-authorized-or-unauthorized-set-member",
            )
        )
    return tuple(selectors)


def _security_bindings(version: str) -> dict[str, ReviewedBinding]:
    """Compile exact user, permission, and token recipes from reviewed facts."""
    source_recipes = security_contract.semantic_operation_sources(version)
    type_roots = security_contract.semantic_operation_type_roots(version)
    users_ui = _LEGACY_USERS_UI if version in _LEGACY_ADMIN_UI_VERSIONS else _USERS_UI
    token_ui = (
        _LEGACY_ACCESS_TOKEN_UI
        if version in _LEGACY_ADMIN_UI_VERSIONS
        else _ACCESS_TOKEN_UI
    )
    return {
        semantic_operation: ReviewedBinding(
            source_operations=source_operations,
            type_closure=_wire_types(
                models=tuple(
                    dict.fromkeys(
                        (*_SECURITY_COMMON_MODELS, *type_roots[semantic_operation])
                    )
                ),
                enums=_SECURITY_COMMON_ENUMS,
            ),
            selector_semantics=_security_selectors(version, semantic_operation),
            evidence_sources=_evidence(
                source_operations,
                (
                    token_ui
                    if semantic_operation.startswith("access-token.")
                    else users_ui,
                ),
            ),
        )
        for semantic_operation, source_operations in source_recipes.items()
    }


def _governance_selector(
    version: str,
    semantic_operation: str,
) -> tuple[SelectorSemantics, ...]:
    """Describe exact datasource/namespace identities consumed by one action."""
    resource, _, operation = semantic_operation.partition(".")
    if resource == "datasource":
        exposed = ("name", "id")
        if operation == "page":
            consumed: tuple[str, ...] = ()
            resolution = "server-page"
        elif operation == "create":
            consumed = ("name",)
            resolution = "create-plus-page-and-detail-readback"
        else:
            consumed = exposed
            resolution = "direct-id-or-exact-name-via-page"
        return (
            SelectorSemantics(
                resource=resource,
                consumed_selectors=consumed,
                exposed_identities=exposed,
                native_identity="id",
                resolution=resolution,
            ),
        )
    if resource != "namespace":
        message = f"unknown governance semantic operation {semantic_operation!r}"
        raise ValueError(message)
    namespace_exposed = ("namespace", "id", "code")
    if operation in {"page", "available"}:
        consumed = ()
        resolution = (
            "server-page" if operation == "page" else "authenticated-available-list"
        )
    elif operation == "create":
        recipe = governance_contract.governance_contract(version).namespace
        consumed = ("namespace",)
        resolution = f"create-by-{recipe.selector}-plus-page-readback"
    else:
        consumed = ("namespace", "id")
        resolution = "exact-namespace-or-id-via-page"
    return (
        SelectorSemantics(
            resource=resource,
            consumed_selectors=consumed,
            exposed_identities=namespace_exposed,
            native_identity="id",
            resolution=resolution,
            parent_identity=(
                governance_contract.governance_contract(version).namespace.selector
                if operation == "create"
                else None
            ),
        ),
    )


def _governance_bindings(version: str) -> dict[str, ReviewedBinding]:
    """Compile datasource and namespace recipes from exact reviewed facts."""
    sources = governance_contract.semantic_operation_sources(version)
    roots = governance_contract.semantic_operation_type_roots(version)
    evidence = governance_contract.semantic_operation_evidence(version)
    return {
        semantic_operation: ReviewedBinding(
            source_operations=source_operations,
            type_closure=_wire_types(
                models=tuple(
                    dict.fromkeys(
                        (
                            "org.apache.dolphinscheduler.api.utils.Result",
                            *roots[semantic_operation],
                        )
                    )
                )
            ),
            selector_semantics=_governance_selector(version, semantic_operation),
            evidence_sources=tuple(
                EvidenceSource(
                    "controller" if source.kind == "controller" else "ui",
                    source.reference,
                )
                for source in evidence[semantic_operation]
                if source.kind in {"controller", "ui"}
            ),
        )
        for semantic_operation, source_operations in sources.items()
    }


def _resource_selectors(
    version: str,
    semantic_operation: str,
) -> tuple[SelectorSemantics, ...]:
    """Describe the stable path identity over exact id/path wire epochs."""
    operation = semantic_operation.partition(".")[2]
    recipe = resource_contract.resource_contract(version).resource
    if operation == "page":
        consumed: tuple[str, ...] = ("directory",)
        resolution = (
            "exact-directory-path-via-id-lookup"
            if recipe.identity_wire == "id"
            else "direct-directory-full-name"
        )
    elif operation in {"create", "mkdir", "upload"}:
        consumed = ("directory", "name")
        resolution = (
            "exact-parent-path-via-id-lookup"
            if recipe.identity_wire == "id"
            else "direct-parent-full-name"
        )
    else:
        consumed = ("fullName",)
        resolution = (
            "exact-full-name-via-id-lookup"
            if recipe.identity_wire == "id"
            else "direct-full-name"
        )
    native_identity = "id" if recipe.identity_wire == "id" else "fullName"
    return (
        SelectorSemantics(
            resource="resource",
            consumed_selectors=consumed,
            exposed_identities=tuple(
                dict.fromkeys((*consumed, "fullName", native_identity))
            ),
            native_identity=native_identity,
            resolution=resolution,
            parent_identity=(
                recipe.parent_directory_wire
                if operation in {"create", "mkdir", "upload"}
                else None
            ),
        ),
    )


def _resource_bindings(version: str) -> dict[str, ReviewedBinding]:
    """Compile exact resource wire recipes into runtime-slice bindings."""
    sources = resource_contract.semantic_operation_sources(version)
    roots = resource_contract.semantic_operation_type_roots(version)
    evidence = resource_contract.semantic_operation_evidence(version)
    return {
        semantic_operation: ReviewedBinding(
            source_operations=source_operations,
            type_closure=_wire_types(models=roots[semantic_operation]),
            selector_semantics=_resource_selectors(version, semantic_operation),
            evidence_sources=tuple(
                EvidenceSource(source.kind, source.reference)
                for source in evidence[semantic_operation]
            ),
        )
        for semantic_operation, source_operations in sources.items()
    }


def _task_group_selectors(
    semantic_operation: str,
) -> tuple[SelectorSemantics, ...]:
    """Describe stable task-group identities independently of queue wire epochs."""
    project = SelectorSemantics(
        resource="project",
        consumed_selectors=("name", "code"),
        exposed_identities=("name", "code"),
        native_identity="code",
        resolution="direct-code-or-exact-name-via-project.page",
    )
    task_group_operation = semantic_operation.removeprefix("task-group.")
    task_group = SelectorSemantics(
        resource="task-group",
        consumed_selectors=(
            ("name",)
            if task_group_operation == "create"
            else ("search", "status")
            if task_group_operation == "page"
            else ("name", "id")
        ),
        exposed_identities=(
            "name",
            "id",
            "projectCode",
            "search",
            "status",
        ),
        native_identity="id",
        resolution=(
            "server-page-or-project-scoped-page"
            if task_group_operation == "page"
            else "create-plus-project-page-readback"
            if task_group_operation == "create"
            else "direct-id-or-exact-name-via-task-group.page"
        ),
        parent_identity="project_code",
    )
    queue = SelectorSemantics(
        resource="task-group-queue",
        consumed_selectors=(
            ("task_instance_name", "workflow_instance_name", "status")
            if task_group_operation == "queue.page"
            else ("id",)
        ),
        exposed_identities=(
            "id",
            "groupId",
            "workflowInstanceId",
            "task_instance_name",
            "workflow_instance_name",
            "status",
        ),
        native_identity="id",
        resolution=(
            "task-group-scoped-server-page"
            if task_group_operation == "queue.page"
            else "direct-queue-id"
        ),
        parent_identity=(
            "task_group_id" if task_group_operation == "queue.page" else None
        ),
    )
    if task_group_operation in {"page", "create"}:
        return (project, task_group)
    if task_group_operation == "queue.page":
        return (task_group, queue)
    if task_group_operation.startswith("queue."):
        return (queue,)
    return (task_group,)


def _task_group_bindings(version: str) -> dict[str, ReviewedBinding]:
    """Compile exact task-group and queue contracts into runtime bindings."""
    sources = task_group_contract.semantic_operation_sources(version)
    model_roots = task_group_contract.semantic_operation_type_roots(version)
    evidence = task_group_contract.semantic_operation_evidence(version)
    projection = task_group_contract.task_group_contract(version).queue_page_projection
    if sources and projection is None:
        message = f"DS {version} has no reviewed task-group queue paging projection"
        raise ValueError(message)
    return {
        semantic_operation: ReviewedBinding(
            source_operations=source_operations,
            type_closure=_wire_types(
                models=tuple(
                    dict.fromkeys(
                        (
                            "org.apache.dolphinscheduler.api.utils.Result",
                            *model_roots[semantic_operation],
                        )
                    )
                )
            ),
            selector_semantics=_task_group_selectors(semantic_operation),
            evidence_sources=tuple(
                EvidenceSource(source.kind, source.reference)
                for source in evidence[semantic_operation]
                if source.kind != "snapshot"
            ),
            paging_projection=(
                (
                    ("taskId", projection.task_id_projected),
                    ("inQueue", projection.in_queue_projected),
                )
                if (
                    semantic_operation == "task-group.queue.page"
                    and projection is not None
                )
                else ()
            ),
        )
        for semantic_operation, source_operations in sources.items()
    }


def _alert_selectors(
    semantic_operation: str,
) -> tuple[SelectorSemantics, ...]:
    """Describe stable alert identities without exposing wire-version details."""
    resource, _, operation = semantic_operation.partition(".")
    if resource == "alert-group":
        consumed = (
            ()
            if operation == "page"
            else ("name",)
            if operation == "create"
            else ("name", "id")
        )
        resolution = (
            "server-page"
            if operation == "page"
            else "create-plus-exact-name-readback"
            if operation == "create"
            else "direct-id-or-exact-name-via-alert-group.page"
        )
        return (
            SelectorSemantics(
                resource=resource,
                consumed_selectors=consumed,
                exposed_identities=("name", "id"),
                native_identity="id",
                resolution=resolution,
            ),
        )
    if resource != "alert-plugin":
        message = f"unknown alert semantic operation {semantic_operation!r}"
        raise ValueError(message)
    definition_selector = SelectorSemantics(
        resource="alert-plugin-definition",
        consumed_selectors=("plugin_name", "id"),
        exposed_identities=("plugin_name", "id"),
        native_identity="id",
        resolution="direct-id-or-exact-name-via-definition-list",
    )
    if operation == "definition.list":
        return (
            SelectorSemantics(
                resource="alert-plugin-definition",
                consumed_selectors=(),
                exposed_identities=("plugin_name", "id"),
                native_identity="id",
                resolution="authenticated-plugin-type-list",
            ),
        )
    if operation == "schema":
        return (definition_selector,)
    instance_selector = SelectorSemantics(
        resource=resource,
        consumed_selectors=(
            ()
            if operation == "page"
            else ("instance_name",)
            if operation == "create"
            else ("instance_name", "id")
        ),
        exposed_identities=("instance_name", "id"),
        native_identity="id",
        resolution=(
            "server-page"
            if operation == "page"
            else "create-plus-instance-list-readback"
            if operation == "create"
            else "direct-id-or-exact-name-via-instance-page"
        ),
    )
    if operation in {"create", "update"}:
        return (instance_selector, definition_selector)
    return (instance_selector,)


def _alert_bindings(version: str) -> dict[str, ReviewedBinding]:
    """Compile exact alert-group/plugin recipes from reviewed source facts."""
    sources = alert_contract.semantic_operation_sources(version)
    model_roots = alert_contract.semantic_operation_type_roots(version)
    enum_roots = alert_contract.semantic_operation_enum_roots(version)
    evidence = alert_contract.semantic_operation_evidence(version)
    return {
        semantic_operation: ReviewedBinding(
            source_operations=source_operations,
            type_closure=_wire_types(
                models=tuple(
                    dict.fromkeys(
                        (
                            "org.apache.dolphinscheduler.api.utils.Result",
                            *model_roots[semantic_operation],
                        )
                    )
                ),
                enums=enum_roots[semantic_operation],
            ),
            selector_semantics=_alert_selectors(semantic_operation),
            evidence_sources=tuple(
                EvidenceSource(
                    "controller" if source.kind == "controller" else "ui",
                    source.reference,
                )
                for source in evidence[semantic_operation]
                if source.kind in {"controller", "ui"}
            ),
        )
        for semantic_operation, source_operations in sources.items()
    }


def _observability_selectors(
    semantic_operation: str,
) -> tuple[SelectorSemantics, ...]:
    """Describe stable audit and monitor inputs independently of wire epochs."""
    if semantic_operation == "monitor.database":
        return (
            SelectorSemantics(
                resource="monitor-database",
                consumed_selectors=(),
                exposed_identities=("db_type", "state"),
                native_identity="db_type",
                resolution="current-server-metrics",
            ),
        )
    if semantic_operation == "monitor.server":
        return (
            SelectorSemantics(
                resource="monitor-server",
                consumed_selectors=("node_type",),
                exposed_identities=("node_type", "id", "host", "port"),
                native_identity="id",
                resolution="exact-node-type-route",
            ),
        )
    if semantic_operation == "audit.list":
        return (
            SelectorSemantics(
                resource="audit",
                consumed_selectors=(
                    "model_type",
                    "operation_type",
                    "user_name",
                    "model_name",
                ),
                exposed_identities=(
                    "model_type",
                    "operation_type",
                    "user_name",
                    "model_name",
                    "create_time",
                ),
                native_identity="create_time",
                resolution="server-page",
            ),
        )
    if semantic_operation == "audit.model-types":
        resource = "audit-model-type"
    elif semantic_operation == "audit.operation-types":
        resource = "audit-operation-type"
    else:
        message = f"unknown observability semantic operation {semantic_operation!r}"
        raise ValueError(message)
    return (
        SelectorSemantics(
            resource=resource,
            consumed_selectors=(),
            exposed_identities=("name",),
            native_identity="name",
            resolution="authenticated-metadata-list",
        ),
    )


def _observability_bindings(version: str) -> dict[str, ReviewedBinding]:
    """Compile exact audit and monitor source closures into runtime bindings."""
    sources = observability_contract.semantic_operation_sources(version)
    model_roots = observability_contract.semantic_operation_type_roots(version)
    enum_roots = observability_contract.semantic_operation_enum_roots(version)
    monitor_ui = (
        _LEGACY_MONITOR_UI
        if version
        in {
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
        else _MONITOR_UI
    )
    return {
        semantic_operation: ReviewedBinding(
            source_operations=source_operations,
            type_closure=_wire_types(
                models=tuple(
                    dict.fromkeys(
                        (
                            "org.apache.dolphinscheduler.api.utils.Result",
                            *model_roots[semantic_operation],
                        )
                    )
                ),
                enums=enum_roots[semantic_operation],
            ),
            selector_semantics=_observability_selectors(semantic_operation),
            evidence_sources=_evidence(
                source_operations,
                (
                    monitor_ui
                    if semantic_operation.startswith("monitor.")
                    else _AUDIT_UI,
                ),
            ),
        )
        for semantic_operation, source_operations in sources.items()
    }


_ENVIRONMENT_VERSIONS = frozenset(
    REVIEWED_DS_VERSIONS[REVIEWED_DS_VERSIONS.index("2.0.0") :]
)
_CLUSTER_VERSIONS = frozenset(
    REVIEWED_DS_VERSIONS[REVIEWED_DS_VERSIONS.index("3.1.0") :]
)
_QUEUE_DELETE_VERSIONS = frozenset(
    REVIEWED_DS_VERSIONS[REVIEWED_DS_VERSIONS.index("3.2.0") :]
)
_ENVIRONMENT_CREATE_PROJECT_VERSIONS = frozenset(
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
    }
)
_CLUSTER_CREATE_PROJECT_VERSIONS = frozenset(
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
        "3.2.0",
    }
)
_LEGACY_ADMIN_UI_VERSIONS = frozenset(
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

_ENVIRONMENT_UI = "dolphinscheduler-ui/src/service/modules/environment/index.ts"
_LEGACY_ENVIRONMENT_UI = (
    "dolphinscheduler-ui/src/js/conf/home/pages/security/pages/environment/"
    "_source/createEnvironment.vue"
)
_CLUSTER_UI = "dolphinscheduler-ui/src/service/modules/cluster/index.ts"
_QUEUE_UI = "dolphinscheduler-ui/src/service/modules/queues/index.ts"
_LEGACY_QUEUE_UI = (
    "dolphinscheduler-ui/src/js/conf/home/pages/security/pages/queue/"
    "_source/createQueue.vue"
)
_WORKER_GROUP_UI = "dolphinscheduler-ui/src/service/modules/worker-groups/index.ts"
_LEGACY_WORKER_GROUP_UI = (
    "dolphinscheduler-ui/src/js/conf/home/pages/security/pages/workerGroups/"
    "_source/createWorker.vue"
)


def _admin_code_lifecycle_bindings(
    *,
    resource: str,
    page_operation: str,
    detail_operation: str | None,
    create_operation: str,
    update_operation: str,
    delete_operation: str,
    type_closure: tuple[WireTypeRef, ...],
    ui_evidence: str,
) -> dict[str, ReviewedBinding]:
    """Build one reviewed page/code CRUD recipe without hiding wire choices."""
    list_selector = SelectorSemantics(
        resource=resource,
        consumed_selectors=(),
        exposed_identities=("name", "code"),
        native_identity="code",
        resolution="paged-search-discovery",
    )
    selector = SelectorSemantics(
        resource=resource,
        consumed_selectors=("name", "code"),
        exposed_identities=("name", "code"),
        native_identity="code",
        resolution=(
            f"direct-code-or-exact-name-via-{resource}.page"
            if detail_operation
            else f"exact-code-or-name-via-{resource}.page"
        ),
    )
    create_selector = SelectorSemantics(
        resource=resource,
        consumed_selectors=("name",),
        exposed_identities=("name", "code"),
        native_identity="code",
        resolution=(
            "create-returned-code-plus-direct-detail-readback"
            if detail_operation
            else "create-returned-code-plus-paged-code-readback"
        ),
    )

    def binding(
        source_operations: tuple[str, ...],
        selected: SelectorSemantics,
    ) -> ReviewedBinding:
        return ReviewedBinding(
            source_operations=source_operations,
            type_closure=type_closure,
            selector_semantics=(selected,),
            evidence_sources=_evidence(source_operations, (ui_evidence,)),
        )

    detail_sources = (detail_operation,) if detail_operation else ()
    readback_sources = detail_sources or (page_operation,)
    return {
        f"{resource}.page": binding((page_operation,), list_selector),
        f"{resource}.get": binding(
            (page_operation, *detail_sources),
            selector,
        ),
        f"{resource}.create": binding(
            (create_operation, *readback_sources),
            create_selector,
        ),
        f"{resource}.update": binding(
            (page_operation, *detail_sources, update_operation),
            selector,
        ),
        f"{resource}.delete": binding(
            (page_operation, delete_operation),
            selector,
        ),
    }


def _environment_bindings(version: str) -> dict[str, ReviewedBinding]:
    if version not in _ENVIRONMENT_VERSIONS:
        return {}
    models = (
        "org.apache.dolphinscheduler.api.utils.PageInfo",
        "org.apache.dolphinscheduler.api.utils.Result",
        "org.apache.dolphinscheduler.api.dto.EnvironmentDto",
        *(
            ("org.apache.dolphinscheduler.dao.entity.Environment",)
            if version in REVIEWED_DS_VERSIONS[REVIEWED_DS_VERSIONS.index("3.2.1") :]
            else ()
        ),
    )
    return _admin_code_lifecycle_bindings(
        resource="environment",
        page_operation="EnvironmentController.queryEnvironmentListPaging",
        detail_operation="EnvironmentController.queryEnvironmentByCode",
        create_operation=(
            "EnvironmentController.createProject"
            if version in _ENVIRONMENT_CREATE_PROJECT_VERSIONS
            else "EnvironmentController.createEnvironment"
        ),
        update_operation="EnvironmentController.updateEnvironment",
        delete_operation="EnvironmentController.deleteEnvironment",
        type_closure=_wire_types(models=models),
        ui_evidence=(
            _LEGACY_ENVIRONMENT_UI
            if version
            in {
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
            else _ENVIRONMENT_UI
        ),
    )


def _cluster_bindings(version: str) -> dict[str, ReviewedBinding]:
    if version not in _CLUSTER_VERSIONS:
        return {}
    models = (
        "org.apache.dolphinscheduler.api.utils.PageInfo",
        "org.apache.dolphinscheduler.api.utils.Result",
        "org.apache.dolphinscheduler.api.dto.ClusterDto",
        *(
            ("org.apache.dolphinscheduler.dao.entity.Cluster",)
            if version in REVIEWED_DS_VERSIONS[REVIEWED_DS_VERSIONS.index("3.2.1") :]
            else ()
        ),
    )
    return _admin_code_lifecycle_bindings(
        resource="cluster",
        page_operation="ClusterController.queryClusterListPaging",
        detail_operation=(
            None if version == "3.4.3" else "ClusterController.queryClusterByCode"
        ),
        create_operation=(
            "ClusterController.createProject"
            if version in _CLUSTER_CREATE_PROJECT_VERSIONS
            else "ClusterController.createCluster"
        ),
        update_operation="ClusterController.updateCluster",
        delete_operation="ClusterController.deleteCluster",
        type_closure=_wire_types(models=models),
        ui_evidence=_CLUSTER_UI,
    )


def _task_type_bindings(version: str) -> dict[str, ReviewedBinding]:
    """Return the exact favourite-task catalog binding when upstream has it."""
    if version not in REVIEWED_DS_VERSIONS[REVIEWED_DS_VERSIONS.index("3.1.0") :]:
        return {}
    source_operations = ("FavTaskController.listTaskType",)
    return {
        "task-type.list": ReviewedBinding(
            source_operations=source_operations,
            type_closure=_wire_types(
                models=("org.apache.dolphinscheduler.api.dto.FavTaskDto",)
            ),
            selector_semantics=(
                SelectorSemantics(
                    resource="task-type",
                    consumed_selectors=(),
                    exposed_identities=("task_type",),
                    native_identity="task_type",
                    resolution="server-catalog",
                ),
            ),
            evidence_sources=_evidence(source_operations, (_FAV_TASK_UI,)),
        )
    }


def _paged_id_lifecycle_bindings(
    *,
    resource: str,
    page_operation: str,
    create_operation: str,
    update_operation: str,
    delete_operation: str | None,
    type_closure: tuple[WireTypeRef, ...],
    ui_evidence: str,
) -> dict[str, ReviewedBinding]:
    """Build one reviewed page/id CRUD recipe with optional upstream delete."""
    list_selector = SelectorSemantics(
        resource=resource,
        consumed_selectors=(),
        exposed_identities=("name", "id"),
        native_identity="id",
        resolution="paged-search-discovery",
    )
    selector = SelectorSemantics(
        resource=resource,
        consumed_selectors=("name", "id"),
        exposed_identities=("name", "id"),
        native_identity="id",
        resolution=f"direct-id-or-exact-name-via-{resource}.page",
    )
    create_selector = SelectorSemantics(
        resource=resource,
        consumed_selectors=("name",),
        exposed_identities=("name", "id"),
        native_identity="id",
        resolution="create-plus-exact-name-readback",
    )

    def binding(
        source_operations: tuple[str, ...],
        selected: SelectorSemantics,
    ) -> ReviewedBinding:
        return ReviewedBinding(
            source_operations=source_operations,
            type_closure=type_closure,
            selector_semantics=(selected,),
            evidence_sources=_evidence(source_operations, (ui_evidence,)),
        )

    bindings = {
        f"{resource}.page": binding((page_operation,), list_selector),
        f"{resource}.get": binding((page_operation,), selector),
        f"{resource}.create": binding(
            (create_operation, page_operation),
            create_selector,
        ),
        f"{resource}.update": binding(
            (page_operation, update_operation),
            selector,
        ),
    }
    if delete_operation is not None:
        bindings[f"{resource}.delete"] = binding(
            (page_operation, delete_operation),
            selector,
        )
    return bindings


def _queue_bindings(version: str) -> dict[str, ReviewedBinding]:
    return _paged_id_lifecycle_bindings(
        resource="queue",
        page_operation=_QUEUE_PAGE_OPERATION,
        create_operation="QueueController.createQueue",
        update_operation="QueueController.updateQueue",
        delete_operation=(
            "QueueController.deleteQueueById"
            if version in _QUEUE_DELETE_VERSIONS
            else None
        ),
        type_closure=_wire_types(
            models=(
                "org.apache.dolphinscheduler.api.utils.PageInfo",
                "org.apache.dolphinscheduler.api.utils.Result",
                "org.apache.dolphinscheduler.dao.entity.Queue",
            )
        ),
        ui_evidence=(
            _LEGACY_QUEUE_UI if version in _LEGACY_ADMIN_UI_VERSIONS else _QUEUE_UI
        ),
    )


def _worker_group_bindings(version: str) -> dict[str, ReviewedBinding]:
    models = (
        "org.apache.dolphinscheduler.api.utils.PageInfo",
        "org.apache.dolphinscheduler.api.utils.Result",
        "org.apache.dolphinscheduler.dao.entity.WorkerGroup",
        *(
            ("org.apache.dolphinscheduler.dao.entity.WorkerGroupPageDetail",)
            if version in REVIEWED_DS_VERSIONS[REVIEWED_DS_VERSIONS.index("3.3.1") :]
            else ()
        ),
    )
    enums = (
        ("org.apache.dolphinscheduler.common.enums.WorkerGroupSource",)
        if version in REVIEWED_DS_VERSIONS[REVIEWED_DS_VERSIONS.index("3.3.1") :]
        else ()
    )
    return _paged_id_lifecycle_bindings(
        resource="worker-group",
        page_operation="WorkerGroupController.queryAllWorkerGroupsPaging",
        create_operation="WorkerGroupController.saveWorkerGroup",
        update_operation="WorkerGroupController.saveWorkerGroup",
        delete_operation=(
            "WorkerGroupController.deleteById"
            if version in REVIEWED_DS_VERSIONS[: REVIEWED_DS_VERSIONS.index("3.1.0")]
            else "WorkerGroupController.deleteWorkerGroupById"
        ),
        type_closure=_wire_types(models=models, enums=enums),
        ui_evidence=(
            _LEGACY_WORKER_GROUP_UI
            if version in _LEGACY_ADMIN_UI_VERSIONS
            else _WORKER_GROUP_UI
        ),
    )


def _runtime_instance_selectors(
    version: str,
    semantic_operation: str,
) -> tuple[SelectorSemantics, ...]:
    """Describe instance identities and their project-scoped resolution path."""
    recipe = runtime_instance_contract.runtime_instance_contract(version)
    parent_identity = (
        "project_name" if recipe.project_route == "name" else "project_code"
    )
    project = SelectorSemantics(
        resource="project",
        consumed_selectors=("name", recipe.project_identity),
        exposed_identities=("name", recipe.project_identity),
        native_identity=recipe.project_identity,
        resolution="direct-native-or-exact-name-via-project.page",
    )
    workflow = SelectorSemantics(
        resource="workflow",
        consumed_selectors=("name", recipe.definition_identity),
        exposed_identities=("name", recipe.definition_identity),
        native_identity=recipe.definition_identity,
        resolution="direct-native-or-exact-name-via-workflow.refs",
        parent_identity=parent_identity,
    )
    workflow_instance = SelectorSemantics(
        resource="workflow-instance",
        consumed_selectors=("id",),
        exposed_identities=("id",),
        native_identity="id",
        resolution="bounded-visible-project-scan-via-project-scoped-page",
        parent_identity=parent_identity,
    )
    task_instance = SelectorSemantics(
        resource="task-instance",
        consumed_selectors=("id",),
        exposed_identities=("id",),
        native_identity="id",
        resolution="workflow-scoped-task-page",
        parent_identity="workflow_instance_id",
    )
    if semantic_operation == "workflow-instance.list":
        return (project, workflow, workflow_instance)
    if semantic_operation == "task-instance.list":
        return (project, workflow_instance, task_instance)
    if semantic_operation == "task-instance.log":
        return (task_instance,)
    if semantic_operation.startswith("task-instance."):
        return (workflow_instance, task_instance)
    return (workflow_instance,)


def _runtime_instance_bindings(version: str) -> dict[str, ReviewedBinding]:
    """Compile exact project-scoped instance recipes into runtime bindings."""
    sources = runtime_instance_contract.semantic_operation_sources(version)
    model_roots = runtime_instance_contract.semantic_operation_type_roots(version)
    enum_roots = runtime_instance_contract.semantic_operation_enum_roots(version)
    evidence = runtime_instance_contract.semantic_operation_evidence(version)
    bindings = {
        semantic_operation: ReviewedBinding(
            source_operations=source_operations,
            type_closure=_wire_types(
                models=model_roots[semantic_operation],
                enums=enum_roots[semantic_operation],
            ),
            selector_semantics=_runtime_instance_selectors(
                version,
                semantic_operation,
            ),
            evidence_sources=tuple(
                EvidenceSource(
                    "controller" if source.kind == "controller" else "ui",
                    source.reference,
                )
                for source in evidence[semantic_operation]
                if source.kind in {"controller", "ui"}
            ),
        )
        for semantic_operation, source_operations in sources.items()
    }
    if "workflow-instance.edit" in bindings:
        bindings["workflow-instance.edit"] = _with_datasource_resolution(
            bindings["workflow-instance.edit"],
            version=version,
        )
    return bindings


def _workflow_family_selectors(
    version: str,
    semantic_operation: str,
) -> tuple[SelectorSemantics, ...]:
    """Describe exact workflow-family identities consumed above the wire."""
    recipe = workflow_contract.workflow_contract(version)
    native_identity = recipe.definition.native_identity
    parent_identity = (
        "project_name" if recipe.definition.project_route == "name" else "project_code"
    )
    project = SelectorSemantics(
        resource="project",
        consumed_selectors=("name", native_identity),
        exposed_identities=("name", native_identity),
        native_identity=native_identity,
        resolution="direct-native-or-exact-name-via-project.page",
    )
    workflow = SelectorSemantics(
        resource="workflow",
        consumed_selectors=("name", native_identity),
        exposed_identities=("name", native_identity),
        native_identity=native_identity,
        resolution=(
            "server-created-and-read-back-by-exact-name"
            if semantic_operation == "workflow.create"
            else "direct-native-or-exact-name-via-workflow.refs"
        ),
        parent_identity=parent_identity,
    )
    if recipe.execution.start_node_identity == "name":
        task = SelectorSemantics(
            resource="task",
            consumed_selectors=("name",),
            exposed_identities=("name",),
            native_identity="name",
            resolution="exact-name-within-legacy-workflow-graph",
            parent_identity="process_definition_id",
        )
    else:
        task = SelectorSemantics(
            resource="task",
            consumed_selectors=("name", "code"),
            exposed_identities=("name", "code"),
            native_identity="code",
            resolution="exact-name-or-code-within-workflow-dag",
            parent_identity=(
                "process_definition_code"
                if recipe.definition.family == "process"
                else "workflow_definition_code"
            ),
        )
    if semantic_operation == "workflow.lineage.list":
        return (project,)
    if semantic_operation in {
        "workflow.run-task",
        "workflow.backfill",
        "workflow.lineage.dependent-tasks",
    }:
        return (project, workflow, task)
    return (project, workflow)


def _workflow_family_bindings(version: str) -> dict[str, ReviewedBinding]:
    """Compile reviewed definition/execution/lineage recipes into bindings."""
    sources = workflow_contract.semantic_operation_sources(version)
    model_roots = workflow_contract.semantic_operation_type_roots(version)
    enum_roots = workflow_contract.semantic_operation_enum_roots(version)
    evidence = workflow_contract.semantic_operation_evidence(version)
    bindings = {
        semantic_operation: ReviewedBinding(
            source_operations=source_operations,
            type_closure=_wire_types(
                models=model_roots[semantic_operation],
                enums=enum_roots[semantic_operation],
            ),
            selector_semantics=_workflow_family_selectors(
                version,
                semantic_operation,
            ),
            evidence_sources=tuple(
                EvidenceSource(
                    "controller" if source.kind == "controller" else "ui",
                    source.reference,
                )
                for source in evidence[semantic_operation]
                if source.kind in {"controller", "ui"}
            ),
        )
        for semantic_operation, source_operations in sources.items()
    }
    for semantic_operation in ("workflow.create", "workflow.edit"):
        if semantic_operation in bindings:
            bindings[semantic_operation] = _with_datasource_resolution(
                bindings[semantic_operation],
                version=version,
            )
    return bindings


def _with_datasource_resolution(
    binding: ReviewedBinding,
    *,
    version: str,
) -> ReviewedBinding:
    """Compose the exact get-by-id/page-by-name datasource lookup closure."""
    datasource_binding = _governance_bindings(version)["datasource.get"]
    return ReviewedBinding(
        source_operations=tuple(
            dict.fromkeys(
                (*binding.source_operations, *datasource_binding.source_operations)
            )
        ),
        type_closure=tuple(
            sorted(
                {*binding.type_closure, *datasource_binding.type_closure},
                key=lambda item: (item.surface, item.key),
            )
        ),
        selector_semantics=(
            *binding.selector_semantics,
            *datasource_binding.selector_semantics,
        ),
        evidence_sources=tuple(
            sorted(
                {*binding.evidence_sources, *datasource_binding.evidence_sources},
                key=lambda item: (item.kind, item.reference),
            )
        ),
    )


def _runtime_bindings(version: str) -> dict[str, ReviewedBinding]:
    bindings = {
        **_identity_bindings(version),
        **_project_lifecycle_bindings(version),
        **_project_configuration_bindings(version),
        **_tenant_lifecycle_bindings(version),
        **_environment_bindings(version),
        **_cluster_bindings(version),
        **_task_type_bindings(version),
        **_queue_bindings(version),
        **_worker_group_bindings(version),
        **_schedule_bindings(version),
        **_security_bindings(version),
        **_governance_bindings(version),
        **_alert_bindings(version),
        **_observability_bindings(version),
        **_resource_bindings(version),
        **_task_group_bindings(version),
        **_runtime_instance_bindings(version),
        **_workflow_family_bindings(version),
    }
    bindings.update(_task_definition_bindings(version))
    return bindings


RUNTIME_OPERATION_BINDINGS: dict[str, dict[str, ReviewedBinding]] = {
    version: _runtime_bindings(version) for version in REVIEWED_DS_VERSIONS
}

_TASK_CLEANUP_OPERATION = (
    task_definition_cleanup_contract.TASK_DEFINITION_CLEANUP_SEMANTIC_OPERATION
)

_SEMANTIC_BINDING_SCHEMAS = {
    _TASK_CLEANUP_OPERATION: _SemanticBindingSchema(
        3,
        frozenset({"project", "task"}),
    ),
    **{
        f"{resource}.{operation}": _SemanticBindingSchema(
            minimum,
            frozenset({resource}),
        )
        for resource in ("cluster", "environment")
        for operation, minimum in (
            ("create", 2),
            ("delete", 2),
            ("get", 2),
            ("page", 1),
            ("update", 3),
        )
    },
    "identity.current": _SemanticBindingSchema(1, frozenset()),
    "user.identity": _SemanticBindingSchema(3, frozenset({"user"})),
    **{
        semantic_operation: _SemanticBindingSchema(1, frozenset({"resource"}))
        for semantic_operation in resource_contract.RESOURCE_SEMANTIC_OPERATIONS
    },
    **{
        semantic_operation: _SemanticBindingSchema(
            {
                "task-group.page": 2,
                "task-group.get": 1,
                "task-group.create": 2,
                "task-group.update": 3,
                "task-group.close": 2,
                "task-group.start": 2,
                "task-group.queue.page": 2,
                "task-group.queue.force-start": 1,
                "task-group.queue.set-priority": 1,
            }[semantic_operation],
            frozenset(
                selector.resource
                for selector in _task_group_selectors(semantic_operation)
            ),
        )
        for semantic_operation in task_group_contract.TASK_GROUP_SEMANTIC_OPERATIONS
    },
    **{
        semantic_operation: _SemanticBindingSchema(
            1,
            frozenset(
                {
                    *(
                        selector.resource
                        for selector in _runtime_instance_selectors(
                            version,
                            semantic_operation,
                        )
                    ),
                    *(
                        ("datasource",)
                        if semantic_operation == "workflow-instance.edit"
                        else ()
                    ),
                }
            ),
        )
        for version in REVIEWED_DS_VERSIONS
        for semantic_operation in runtime_instance_contract.semantic_operation_sources(
            version
        )
    },
    **{
        semantic_operation: _SemanticBindingSchema(
            1,
            frozenset(
                {
                    *(
                        selector.resource
                        for selector in _workflow_family_selectors(
                            version,
                            semantic_operation,
                        )
                    ),
                    *(
                        ("datasource",)
                        if semantic_operation in {"workflow.create", "workflow.edit"}
                        else ()
                    ),
                }
            ),
        )
        for version in REVIEWED_DS_VERSIONS
        for semantic_operation in workflow_contract.semantic_operation_sources(version)
    },
    "monitor.database": _SemanticBindingSchema(
        1,
        frozenset({"monitor-database"}),
    ),
    "monitor.server": _SemanticBindingSchema(
        1,
        frozenset({"monitor-server"}),
    ),
    "audit.list": _SemanticBindingSchema(1, frozenset({"audit"})),
    "audit.model-types": _SemanticBindingSchema(
        1,
        frozenset({"audit-model-type"}),
    ),
    "audit.operation-types": _SemanticBindingSchema(
        1,
        frozenset({"audit-operation-type"}),
    ),
    "task-type.list": _SemanticBindingSchema(1, frozenset({"task-type"})),
    "project.create": _SemanticBindingSchema(1, frozenset({"project"})),
    "project.delete": _SemanticBindingSchema(3, frozenset({"project"})),
    "project.get": _SemanticBindingSchema(2, frozenset({"project"})),
    "project.page": _SemanticBindingSchema(1, frozenset({"project"})),
    "project.update": _SemanticBindingSchema(3, frozenset({"project"})),
    "project-preference.read": _SemanticBindingSchema(
        1, frozenset({"project-preference"})
    ),
    **{
        f"project-parameter.{operation}": _SemanticBindingSchema(
            minimum,
            frozenset({"project", "project-parameter"}),
        )
        for operation, minimum in (
            ("create", 3),
            ("delete", 4),
            ("get", 4),
            ("page", 3),
            ("update", 5),
        )
    },
    **{
        f"project-preference.{operation}": _SemanticBindingSchema(
            minimum,
            frozenset({"project", "project-preference"}),
        )
        for operation, minimum in (
            ("disable", 4),
            ("enable", 4),
            ("get", 3),
            ("update", 3),
        )
    },
    **{
        f"project-worker-group.{operation}": _SemanticBindingSchema(
            minimum,
            frozenset({"project", "project-worker-group"}),
        )
        for operation, minimum in (
            ("clear", 4),
            ("page", 3),
            ("set", 4),
        )
    },
    "user.list": _SemanticBindingSchema(1, frozenset({"user"})),
    "user.get": _SemanticBindingSchema(3, frozenset({"user"})),
    "user.create": _SemanticBindingSchema(5, frozenset({"tenant", "user"})),
    "user.update": _SemanticBindingSchema(5, frozenset({"tenant", "user"})),
    "user.delete": _SemanticBindingSchema(4, frozenset({"user"})),
    **{
        f"user.{change}.{resource}": _SemanticBindingSchema(
            6,
            frozenset({resource, "user"}),
        )
        for change in ("grant", "revoke")
        for resource in ("project", "datasource", "namespace")
    },
    "access-token.list": _SemanticBindingSchema(
        1,
        frozenset({"access-token"}),
    ),
    "access-token.get": _SemanticBindingSchema(
        1,
        frozenset({"access-token"}),
    ),
    "access-token.create": _SemanticBindingSchema(
        5,
        frozenset({"access-token", "user"}),
    ),
    "access-token.update": _SemanticBindingSchema(
        5,
        frozenset({"access-token", "user"}),
    ),
    "access-token.delete": _SemanticBindingSchema(
        2,
        frozenset({"access-token"}),
    ),
    "access-token.generate": _SemanticBindingSchema(
        4,
        frozenset({"access-token", "user"}),
    ),
    **{
        f"schedule.{operation}": _SemanticBindingSchema(
            minimum,
            frozenset({"schedule"}),
        )
        for operation, minimum in (
            ("create", 3),
            ("delete", 2),
            ("explain", 2),
            ("get", 1),
            ("offline", 2),
            ("online", 2),
            ("page", 1),
            ("preview", 1),
            ("update", 3),
        )
    },
    **{
        f"datasource.{operation}": _SemanticBindingSchema(
            minimum,
            frozenset({"datasource"}),
        )
        for operation, minimum in (
            ("create", 3),
            ("delete", 3),
            ("get", 2),
            ("page", 1),
            ("saved-test", 3),
            ("update", 3),
        )
    },
    **{
        f"namespace.{operation}": _SemanticBindingSchema(
            minimum,
            frozenset({"namespace"}),
        )
        for operation, minimum in (
            ("available", 1),
            ("create", 2),
            ("delete", 2),
            ("get", 1),
            ("page", 1),
        )
    },
    **{
        f"alert-group.{operation}": _SemanticBindingSchema(
            minimum,
            frozenset({"alert-group"}),
        )
        for operation, minimum in (
            ("create", 2),
            ("delete", 2),
            ("get", 1),
            ("page", 1),
            ("update", 2),
        )
    },
    "alert-plugin.page": _SemanticBindingSchema(
        1,
        frozenset({"alert-plugin"}),
    ),
    "alert-plugin.get": _SemanticBindingSchema(
        2,
        frozenset({"alert-plugin"}),
    ),
    "alert-plugin.definition.list": _SemanticBindingSchema(
        1,
        frozenset({"alert-plugin-definition"}),
    ),
    "alert-plugin.schema": _SemanticBindingSchema(
        2,
        frozenset({"alert-plugin-definition"}),
    ),
    "alert-plugin.create": _SemanticBindingSchema(
        4,
        frozenset({"alert-plugin", "alert-plugin-definition"}),
    ),
    "alert-plugin.update": _SemanticBindingSchema(
        5,
        frozenset({"alert-plugin", "alert-plugin-definition"}),
    ),
    "alert-plugin.delete": _SemanticBindingSchema(
        3,
        frozenset({"alert-plugin"}),
    ),
    "alert-plugin.test": _SemanticBindingSchema(
        3,
        frozenset({"alert-plugin"}),
    ),
    "task.list": _SemanticBindingSchema(
        3,
        frozenset({"project", "workflow", "task"}),
    ),
    "task.get": _SemanticBindingSchema(
        3,
        frozenset({"project", "workflow", "task"}),
    ),
    "task.update": _SemanticBindingSchema(
        4,
        frozenset({"project", "workflow", "task"}),
    ),
    **{
        f"{resource}.{operation}": _SemanticBindingSchema(
            minimum,
            frozenset({resource}),
        )
        for resource in ("queue", "worker-group")
        for operation, minimum in (
            ("create", 2),
            ("delete", 2),
            ("get", 1),
            ("page", 1),
            ("update", 2),
        )
    },
    "tenant.create": _SemanticBindingSchema(
        3,
        frozenset({"queue", "tenant"}),
    ),
    "tenant.delete": _SemanticBindingSchema(2, frozenset({"tenant"})),
    "tenant.get": _SemanticBindingSchema(1, frozenset({"tenant"})),
    "tenant.page": _SemanticBindingSchema(1, frozenset({"tenant"})),
    "tenant.update": _SemanticBindingSchema(
        3,
        frozenset({"queue", "tenant"}),
    ),
    "workflow.get": _SemanticBindingSchema(
        5,
        frozenset({"project", "workflow"}),
    ),
    "workflow.inspect": _SemanticBindingSchema(
        5,
        frozenset({"project", "workflow"}),
    ),
    "workflow.page": _SemanticBindingSchema(
        3,
        frozenset({"project", "workflow"}),
    ),
}


def analyze_semantic_bindings(
    inventory: Mapping[str, object],
    *,
    bindings: Mapping[str, Mapping[str, ReviewedBinding]] = READ_OPERATION_BINDINGS,
) -> dict[str, object]:
    """Validate reviewed bindings and group equal exact-source contracts."""
    validate_reviewed_bindings(bindings)
    source_inventory_complete = _require_bool(
        inventory.get("complete", True),
        label="inventory.complete",
    )
    targets = _target_index(inventory)
    semantic_names = sorted(
        {
            semantic_operation
            for version_bindings in bindings.values()
            for semantic_operation in version_bindings
        }
    )
    results: dict[str, dict[str, object]] = {
        semantic_operation: {"bindings": [], "contract_groups": []}
        for semantic_operation in semantic_names
    }
    diagnostics: list[dict[str, str]] = []

    for version in sorted(bindings, key=_version_sort_key):
        target = targets.get(version)
        version_bindings = sorted(bindings[version].items())
        if target is None:
            diagnostics.extend(
                {
                    "version": version,
                    "semantic_operation": semantic_operation,
                    "error": "source_version_not_found",
                }
                for semantic_operation, _binding in version_bindings
            )
            continue

        operations = _operation_index(target)
        type_indexes = {
            surface: _type_surface_index(target, surface=surface)
            for surface in WIRE_TYPE_SURFACES
        }
        for semantic_operation, binding in version_bindings:
            missing_operations = sorted(
                {
                    operation_id
                    for operation_id in binding.source_operations
                    if operation_id not in operations
                }
            )
            missing_types = sorted(
                {
                    type_ref
                    for type_ref in binding.type_closure
                    if type_ref.key not in type_indexes[type_ref.surface]
                },
                key=lambda item: (item.surface, item.key),
            )
            if missing_operations or missing_types:
                diagnostics.extend(
                    {
                        "version": version,
                        "semantic_operation": semantic_operation,
                        "source_operation": operation_id,
                        "error": "source_operation_not_found",
                    }
                    for operation_id in missing_operations
                )
                diagnostics.extend(
                    {
                        "version": version,
                        "semantic_operation": semantic_operation,
                        "source_type": f"{type_ref.surface}:{type_ref.key}",
                        "error": "source_type_not_found",
                    }
                    for type_ref in missing_types
                )
                continue

            source_operations = [
                operations[operation_id]
                for operation_id in sorted(set(binding.source_operations))
            ]
            source_types = [
                (
                    type_ref,
                    type_indexes[type_ref.surface][type_ref.key],
                )
                for type_ref in sorted(
                    set(binding.type_closure),
                    key=lambda item: (item.surface, item.key),
                )
            ]
            closure_fingerprint = _closure_fingerprint(
                source_operations=source_operations,
                source_types=source_types,
            )
            result_bindings = _require_list(
                results[semantic_operation]["bindings"],
                label=f"semantic_operations.{semantic_operation}.bindings",
            )
            result_bindings.append(
                {
                    "version": version,
                    "source_operations": [
                        {
                            "key": _require_text(
                                source.get("key"),
                                label="source operation.key",
                            ),
                            "fingerprint": _require_text(
                                source.get("fingerprint"),
                                label="source operation.fingerprint",
                            ),
                            "http_method": _require_text(
                                source.get("http_method"),
                                label="source operation.http_method",
                            ),
                            "path": _require_text(
                                source.get("path"),
                                label="source operation.path",
                            ),
                        }
                        for source in source_operations
                    ],
                    "type_closure": [
                        {
                            "surface": type_ref.surface,
                            "key": type_ref.key,
                            "fingerprint": _require_text(
                                source.get("fingerprint"),
                                label="source type.fingerprint",
                            ),
                        }
                        for type_ref, source in source_types
                    ],
                    "closure_fingerprint": closure_fingerprint,
                    "selector_semantics": [
                        asdict(item)
                        for item in sorted(
                            binding.selector_semantics,
                            key=lambda item: (
                                item.resource,
                                item.consumed_selectors,
                                item.exposed_identities,
                                item.native_identity,
                                item.resolution,
                                item.parent_identity or "",
                            ),
                        )
                    ],
                    "evidence_sources": [
                        asdict(item)
                        for item in sorted(
                            binding.evidence_sources,
                            key=lambda item: (item.kind, item.reference),
                        )
                    ],
                }
            )

    for semantic_operation, result in results.items():
        operation_bindings = _require_list(
            result["bindings"],
            label=f"semantic_operations.{semantic_operation}.bindings",
        )
        groups: dict[str, list[str]] = {}
        for item in operation_bindings:
            binding_payload = _require_mapping(item, label="semantic binding")
            fingerprint = _require_text(
                binding_payload.get("closure_fingerprint"),
                label="semantic binding.closure_fingerprint",
            )
            version = _require_text(
                binding_payload.get("version"),
                label="semantic binding.version",
            )
            groups.setdefault(fingerprint, []).append(version)
        result["contract_groups"] = [
            {
                "closure_fingerprint": fingerprint,
                "versions": sorted(versions, key=_version_sort_key),
            }
            for fingerprint, versions in sorted(groups.items())
        ]

    return {
        "schema_version": 2,
        "kind": "dolphinscheduler-semantic-binding-impact",
        "source_inventory_complete": source_inventory_complete,
        "complete": source_inventory_complete and not diagnostics,
        "semantic_operations": results,
        "diagnostics": diagnostics,
    }


def validate_reviewed_bindings(
    bindings: Mapping[str, Mapping[str, ReviewedBinding]],
) -> None:
    """Validate reviewed bindings against the fail-closed semantic schema."""
    for version in sorted(bindings, key=_version_sort_key):
        for semantic_operation, binding in sorted(bindings[version].items()):
            _validate_reviewed_binding(
                binding,
                version=version,
                semantic_operation=semantic_operation,
            )


def _validate_reviewed_binding(
    binding: object,
    *,
    version: str,
    semantic_operation: str,
) -> None:
    label = f"binding {version}:{semantic_operation}"
    if not isinstance(binding, ReviewedBinding):
        message = f"{label} must be a ReviewedBinding"
        raise TypeError(message)
    if binding.paging_projection and (
        semantic_operation != "task-group.queue.page"
        or tuple(name for name, _ in binding.paging_projection) != ("taskId", "inQueue")
        or any(type(value) is not bool for _, value in binding.paging_projection)
    ):
        message = f"{label}.paging_projection is invalid"
        raise ValueError(message)
    schema = _SEMANTIC_BINDING_SCHEMAS.get(semantic_operation)
    if schema is None:
        message = f"{label} has no semantic binding schema"
        raise ValueError(message)
    minimum_operations = schema.minimum_source_operations
    if version == "3.4.3" and semantic_operation in {"cluster.get", "cluster.update"}:
        # This exact release removed queryClusterByCode. The page endpoint
        # supplies both discovery and readback, so one source role is shared.
        minimum_operations -= 1
    if len(binding.source_operations) < minimum_operations:
        message = (
            f"{label}.source_operations must contain at least {minimum_operations}"
        )
        raise ValueError(message)
    if any(not operation for operation in binding.source_operations):
        message = f"{label}.source_operations must not contain empty values"
        raise ValueError(message)
    if len(binding.source_operations) != len(set(binding.source_operations)):
        message = f"{label}.source_operations must not contain duplicates"
        raise ValueError(message)
    project_recipes = _PROJECT_RUNTIME_OPERATION_RECIPES.get(version)
    expected_project_recipe = (
        None if project_recipes is None else project_recipes.get(semantic_operation)
    )
    if (
        expected_project_recipe is not None
        and binding.source_operations != expected_project_recipe
    ):
        message = (
            f"{label}.source_operations must match the exact reviewed project recipe"
        )
        raise ValueError(message)
    if not binding.type_closure:
        message = f"{label}.type_closure must not be empty"
        raise ValueError(message)
    if any(
        not isinstance(type_ref, WireTypeRef)
        or type_ref.surface not in WIRE_TYPE_SURFACES
        or not type_ref.key
        for type_ref in binding.type_closure
    ):
        message = f"{label}.type_closure must contain non-empty WireTypeRef values"
        raise ValueError(message)
    if len(binding.type_closure) != len(set(binding.type_closure)):
        message = f"{label}.type_closure must not contain duplicates"
        raise ValueError(message)
    if schema.selector_resources and not binding.selector_semantics:
        message = f"{label}.selector_semantics must not be empty"
        raise ValueError(message)
    if any(
        not isinstance(selector, SelectorSemantics)
        for selector in binding.selector_semantics
    ):
        message = f"{label}.selector_semantics must contain SelectorSemantics values"
        raise ValueError(message)
    selector_resources = frozenset(
        selector.resource for selector in binding.selector_semantics
    )
    if selector_resources != schema.selector_resources:
        expected = ", ".join(sorted(schema.selector_resources)) or "none"
        actual = ", ".join(sorted(selector_resources)) or "none"
        message = (
            f"{label}.selector_semantics resources must be exactly {expected}; "
            f"got {actual}"
        )
        raise ValueError(message)
    if len(selector_resources) != len(binding.selector_semantics):
        message = f"{label}.selector_semantics must not repeat a resource"
        raise ValueError(message)
    for selector in binding.selector_semantics:
        if (
            not selector.resource
            or not selector.native_identity
            or not selector.resolution
            or not selector.exposed_identities
        ):
            message = f"{label}.selector_semantics contains an incomplete selector"
            raise ValueError(message)
        if selector.native_identity not in selector.exposed_identities:
            message = f"{label}.selector_semantics must expose its native identity"
            raise ValueError(message)
        for identity_kind, identities in (
            ("consumed_selectors", selector.consumed_selectors),
            ("exposed_identities", selector.exposed_identities),
        ):
            if any(not identity for identity in identities):
                message = (
                    f"{label}.selector_semantics.{identity_kind} "
                    "must not contain empty values"
                )
                raise ValueError(message)
            if len(identities) != len(set(identities)):
                message = (
                    f"{label}.selector_semantics.{identity_kind} "
                    "must not contain duplicates"
                )
                raise ValueError(message)
        if not set(selector.consumed_selectors).issubset(selector.exposed_identities):
            message = f"{label}.selector_semantics consumes an unexposed identity"
            raise ValueError(message)
    if not binding.evidence_sources:
        message = f"{label}.evidence_sources must not be empty"
        raise ValueError(message)
    if any(
        not isinstance(source, EvidenceSource) or not source.reference
        for source in binding.evidence_sources
    ):
        message = f"{label}.evidence_sources contains incomplete evidence"
        raise ValueError(message)
    evidence_kinds = {source.kind for source in binding.evidence_sources}
    if not {"controller", "ui"} <= evidence_kinds or not evidence_kinds <= {
        "controller",
        "ui",
        "mapper",
    }:
        message = f"{label}.evidence_sources must include controller and ui evidence"
        raise ValueError(message)


def _target_index(
    inventory: Mapping[str, object],
) -> dict[str, Mapping[str, object]]:
    targets: dict[str, Mapping[str, object]] = {}
    for item in _require_list(inventory.get("targets"), label="inventory.targets"):
        target = _require_mapping(item, label="inventory target")
        version = _require_text(
            target.get("ds_version"),
            label="inventory target.ds_version",
        )
        if version in targets:
            message = f"duplicate inventory target {version!r}"
            raise ValueError(message)
        targets[version] = target
    return targets


def _operation_index(
    target: Mapping[str, object],
) -> dict[str, Mapping[str, object]]:
    surfaces = _require_mapping(target.get("surfaces"), label="target.surfaces")
    operations: dict[str, Mapping[str, object]] = {}
    for item in _require_list(surfaces.get("operations"), label="surface.operations"):
        operation = _require_mapping(item, label="source operation")
        key = _require_text(operation.get("key"), label="source operation.key")
        if key in operations:
            message = f"duplicate source operation {key!r}"
            raise ValueError(message)
        operations[key] = operation
    return operations


def _type_surface_index(
    target: Mapping[str, object],
    *,
    surface: WireTypeSurface,
) -> dict[str, Mapping[str, object]]:
    surfaces = _require_mapping(target.get("surfaces"), label="target.surfaces")
    entries: dict[str, Mapping[str, object]] = {}
    for item in _require_list(surfaces.get(surface), label=f"surface.{surface}"):
        entry = _require_mapping(item, label=f"source {surface[:-1]}")
        key = _require_text(entry.get("key"), label=f"source {surface[:-1]}.key")
        if key in entries:
            message = f"duplicate source {surface[:-1]} {key!r}"
            raise ValueError(message)
        entries[key] = entry
    return entries


def _closure_fingerprint(
    *,
    source_operations: list[Mapping[str, object]],
    source_types: list[tuple[WireTypeRef, Mapping[str, object]]],
) -> str:
    payload = {
        "operations": [
            {
                "key": _require_text(item.get("key"), label="source operation.key"),
                "fingerprint": _require_text(
                    item.get("fingerprint"),
                    label="source operation.fingerprint",
                ),
            }
            for item in source_operations
        ],
        "types": [
            {
                "surface": type_ref.surface,
                "key": type_ref.key,
                "fingerprint": _require_text(
                    item.get("fingerprint"),
                    label="source type.fingerprint",
                ),
            }
            for type_ref, item in source_types
        ],
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _require_mapping(value: object, *, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        message = f"{label} must be an object"
        raise TypeError(message)
    return value


def _require_list(value: object, *, label: str) -> list[Any]:
    if not isinstance(value, list):
        message = f"{label} must be a list"
        raise TypeError(message)
    return value


def _require_text(value: object, *, label: str) -> str:
    if not isinstance(value, str):
        message = f"{label} must be text"
        raise TypeError(message)
    return value


def _require_bool(value: object, *, label: str) -> bool:
    if not isinstance(value, bool):
        message = f"{label} must be a boolean"
        raise TypeError(message)
    return value


def _version_sort_key(version: str) -> tuple[tuple[int, int | str], ...]:
    return tuple(
        (0, int(part)) if part.isdigit() else (1, part.casefold())
        for part in re.findall(r"\d+|\D+", version)
    )
