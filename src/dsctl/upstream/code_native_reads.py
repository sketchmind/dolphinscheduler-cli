from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, cast

from dsctl.errors import ApiTransportError, UnsupportedFeatureError
from dsctl.upstream._compiled_project import PROJECT_PROGRAMS
from dsctl.upstream._compiled_workflow_runtime import (
    WORKFLOW_PROGRAMS,
    WorkflowPrimitive,
)
from dsctl.upstream.definition_reads import DefinitionReads
from dsctl.upstream.definition_wire import CodeDefinitionWire
from dsctl.upstream.project_reads import CompiledProjectReads
from dsctl.upstream.read_models import (
    CanonicalSchedule,
    CanonicalWorkflow,
    ReadPage,
    WorkflowReference,
)
from dsctl.upstream.wire import WireContractError

if TYPE_CHECKING:
    from collections.abc import Sequence

    from dsctl.client import DolphinSchedulerClient
    from dsctl.config import ClusterProfile
    from dsctl.upstream._generated_types import OpaqueGeneratedValue
    from dsctl.upstream.compiled_domain import BoundCompiledPrograms
    from dsctl.upstream.protocol import (
        ReadUpstreamSession,
        SchedulePageRecord,
        StringEnumValue,
        WorkflowPageRecord,
        WorkflowRecord,
    )
    from dsctl.upstream.wire import CompiledWireProfile


_ReadFamily = Literal["process", "workflow"]


@dataclass(frozen=True)
class _ReadProjection:
    workflow_schedule: bool
    workflow_execution_type: bool
    schedule_tenant_code: bool
    schedule_environment_name: bool
    schedule_filter_required: bool
    workflow_tenant_identity: bool = False
    workflow_ref_project_code_reliable: bool = True


@dataclass(frozen=True)
class _ReadVersionSpec:
    family: _ReadFamily
    projection: _ReadProjection


_PROCESS_200_PROJECTION = _ReadProjection(
    workflow_schedule=False,
    workflow_execution_type=False,
    schedule_tenant_code=False,
    schedule_environment_name=False,
    schedule_filter_required=True,
    workflow_tenant_identity=True,
    # DS 2.0.0 serializes the workflow code into this field. The endpoint's
    # project-scoped route remains authoritative for the lookup scope.
    workflow_ref_project_code_reliable=False,
)
_PROCESS_209_PROJECTION = _ReadProjection(
    workflow_schedule=False,
    workflow_execution_type=False,
    schedule_tenant_code=False,
    schedule_environment_name=False,
    schedule_filter_required=True,
    workflow_tenant_identity=True,
)
_PROCESS_30_31_PROJECTION = _ReadProjection(
    workflow_schedule=False,
    workflow_execution_type=True,
    schedule_tenant_code=False,
    schedule_environment_name=False,
    schedule_filter_required=True,
    workflow_tenant_identity=True,
)
_PROCESS_320_PROJECTION = _ReadProjection(
    workflow_schedule=False,
    workflow_execution_type=True,
    schedule_tenant_code=True,
    schedule_environment_name=True,
    schedule_filter_required=False,
)
_CURRENT_PROJECTION = _ReadProjection(
    workflow_schedule=True,
    workflow_execution_type=True,
    schedule_tenant_code=True,
    schedule_environment_name=True,
    schedule_filter_required=False,
)

# Every key is an explicitly reviewed source version. Shared values record
# proven semantic sameness; no family or feature is inferred from a range.
_READ_SPEC_BY_VERSION: dict[str, _ReadVersionSpec] = {
    "2.0.0": _ReadVersionSpec("process", _PROCESS_200_PROJECTION),
    "2.0.1": _ReadVersionSpec("process", _PROCESS_200_PROJECTION),
    "2.0.2": _ReadVersionSpec("process", _PROCESS_200_PROJECTION),
    "2.0.3": _ReadVersionSpec("process", _PROCESS_209_PROJECTION),
    "2.0.4": _ReadVersionSpec("process", _PROCESS_209_PROJECTION),
    "2.0.5": _ReadVersionSpec("process", _PROCESS_209_PROJECTION),
    "2.0.6": _ReadVersionSpec("process", _PROCESS_209_PROJECTION),
    "2.0.7": _ReadVersionSpec("process", _PROCESS_209_PROJECTION),
    "2.0.8": _ReadVersionSpec("process", _PROCESS_209_PROJECTION),
    "2.0.9": _ReadVersionSpec("process", _PROCESS_209_PROJECTION),
    "3.0.0": _ReadVersionSpec("process", _PROCESS_30_31_PROJECTION),
    "3.0.1": _ReadVersionSpec("process", _PROCESS_30_31_PROJECTION),
    "3.0.2": _ReadVersionSpec("process", _PROCESS_30_31_PROJECTION),
    "3.0.3": _ReadVersionSpec("process", _PROCESS_30_31_PROJECTION),
    "3.0.4": _ReadVersionSpec("process", _PROCESS_30_31_PROJECTION),
    "3.0.5": _ReadVersionSpec("process", _PROCESS_30_31_PROJECTION),
    "3.0.6": _ReadVersionSpec("process", _PROCESS_30_31_PROJECTION),
    "3.1.0": _ReadVersionSpec("process", _PROCESS_30_31_PROJECTION),
    "3.1.1": _ReadVersionSpec("process", _PROCESS_30_31_PROJECTION),
    "3.1.2": _ReadVersionSpec("process", _PROCESS_30_31_PROJECTION),
    "3.1.3": _ReadVersionSpec("process", _PROCESS_30_31_PROJECTION),
    "3.1.4": _ReadVersionSpec("process", _PROCESS_30_31_PROJECTION),
    "3.1.5": _ReadVersionSpec("process", _PROCESS_30_31_PROJECTION),
    "3.1.6": _ReadVersionSpec("process", _PROCESS_30_31_PROJECTION),
    "3.1.7": _ReadVersionSpec("process", _PROCESS_30_31_PROJECTION),
    "3.1.8": _ReadVersionSpec("process", _PROCESS_30_31_PROJECTION),
    "3.1.9": _ReadVersionSpec("process", _PROCESS_30_31_PROJECTION),
    "3.2.0": _ReadVersionSpec("process", _PROCESS_320_PROJECTION),
    "3.2.1": _ReadVersionSpec("process", _CURRENT_PROJECTION),
    "3.2.2": _ReadVersionSpec("process", _CURRENT_PROJECTION),
    "3.3.1": _ReadVersionSpec("workflow", _CURRENT_PROJECTION),
    "3.3.2": _ReadVersionSpec("workflow", _CURRENT_PROJECTION),
    "3.4.0": _ReadVersionSpec("workflow", _CURRENT_PROJECTION),
    "3.4.1": _ReadVersionSpec("workflow", _CURRENT_PROJECTION),
    "3.4.2": _ReadVersionSpec("workflow", _CURRENT_PROJECTION),
    "3.4.3": _ReadVersionSpec("workflow", _CURRENT_PROJECTION),
}


@dataclass(frozen=True)
class _ReadRecipe:
    detail_field: str
    schedule_code_field: str
    schedule_name_field: str
    schedule_priority_field: str
    schedule_filter_field: str


_READ_RECIPE_BY_FAMILY: dict[_ReadFamily, _ReadRecipe] = {
    "process": _ReadRecipe(
        detail_field="processDefinition",
        schedule_code_field="processDefinitionCode",
        schedule_name_field="processDefinitionName",
        schedule_priority_field="processInstancePriority",
        schedule_filter_field="processDefinitionCode",
    ),
    "workflow": _ReadRecipe(
        detail_field="workflowDefinition",
        schedule_code_field="workflowDefinitionCode",
        schedule_name_field="workflowDefinitionName",
        schedule_priority_field="workflowInstancePriority",
        schedule_filter_field="workflowDefinitionCode",
    ),
}


@dataclass(frozen=True)
class _CodeNativeReadBindings:
    ds_version: str
    compiled: CompiledWireProfile
    recipe: _ReadRecipe
    projection: _ReadProjection


class CodeNativeReadAdapter:
    """One exact-package adapter for every reviewed code-identity read dialect."""

    def __init__(self, ds_version: str, *, version_slug: str | None = None) -> None:
        """Load and validate one exact generated package without remote I/O."""
        self._bindings = _load_bindings(ds_version, version_slug=version_slug)
        self.ds_version = ds_version
        self.version_slug = f"ds_{ds_version.replace('.', '_')}"

    @classmethod
    def for_version(cls, ds_version: str) -> CodeNativeReadAdapter:
        """Return the adapter for an explicitly reviewed exact version."""
        return cls(ds_version)

    def bind_read(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> ReadUpstreamSession:
        """Bind the exact project, workflow, and schedule read closure."""
        programs = WORKFLOW_PROGRAMS.bind(
            self._bindings.compiled, profile, http_client=http_client
        )
        projects = CompiledProjectReads(
            PROJECT_PROGRAMS.bind(
                PROJECT_PROGRAMS.profile(self.ds_version),
                profile,
                http_client=http_client,
            )
        )
        workflows = _CodeNativeWorkflowReads(programs, self._bindings)
        schedules = _CodeNativeScheduleReads(programs, self._bindings)
        return _CodeNativeReadSession(
            definitions=DefinitionReads(
                CodeDefinitionWire(
                    projects=projects,
                    workflows=workflows,
                    schedules=schedules,
                )
            ),
        )


@dataclass(frozen=True)
class _CodeNativeReadSession:
    definitions: DefinitionReads


@dataclass(frozen=True)
class _CodeNativeWorkflowReads:
    programs: BoundCompiledPrograms[WorkflowPrimitive]
    bindings: _CodeNativeReadBindings

    def list_refs(self, *, project_code: int) -> Sequence[WorkflowRecord]:
        payload = self.programs.call("definition_refs", {"projectCode": project_code})
        if not isinstance(payload, list):
            raise _projection_error(
                ds_version=self.bindings.ds_version,
                resource="workflow.ref",
                field="data",
                reason="generated reference payload is not a list",
            )
        return [
            _workflow_reference(
                item,
                bindings=self.bindings,
                expected_project_code=project_code,
            )
            for item in payload
        ]

    def list_page(
        self,
        *,
        project_code: int,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> WorkflowPageRecord:
        page = self.programs.call(
            "definition_page",
            {
                "projectCode": project_code,
                "searchVal": search,
                "pageNo": page_no,
                "pageSize": page_size,
            },
        )
        items = _response_items(
            page,
            ds_version=self.bindings.ds_version,
            resource="workflow.page",
        )
        projected = (
            None
            if items is None
            else [
                _workflow_record(
                    item,
                    bindings=self.bindings,
                    expected_project_code=project_code,
                    expected_code=None,
                )
                for item in items
            ]
        )
        return cast(
            "WorkflowPageRecord",
            _read_page(
                page,
                projected,
                ds_version=self.bindings.ds_version,
                resource="workflow.page",
            ),
        )

    def get(self, *, project_code: int, code: int) -> CanonicalWorkflow:
        recipe = self.bindings.recipe
        dag = self.programs.call(
            "definition_get", {"projectCode": project_code, "code": code}
        )
        definition = _response_field(
            dag,
            recipe.detail_field,
            ds_version=self.bindings.ds_version,
            resource="workflow",
        )
        if definition is None:
            raise _projection_error(
                ds_version=self.bindings.ds_version,
                resource="workflow",
                field=recipe.detail_field,
                reason="generated detail field is null",
            )
        return _workflow_record(
            definition,
            bindings=self.bindings,
            expected_project_code=project_code,
            expected_code=code,
        )


@dataclass(frozen=True)
class _CodeNativeScheduleReads:
    programs: BoundCompiledPrograms[WorkflowPrimitive]
    bindings: _CodeNativeReadBindings

    def list(
        self,
        *,
        project_code: int,
        page_no: int,
        page_size: int,
        workflow_code: int | None = None,
        search: str | None = None,
    ) -> SchedulePageRecord:
        recipe = self.bindings.recipe
        if self.bindings.projection.schedule_filter_required and workflow_code is None:
            message = (
                f"DolphinScheduler {self.bindings.ds_version} schedule paging "
                "requires an explicit workflow."
            )
            raise UnsupportedFeatureError(
                message,
                details={
                    "ds_version": self.bindings.ds_version,
                    "semantic_operation": "schedule.page",
                    "required_selector": "workflow",
                },
                suggestion="Retry with an explicit workflow name or code.",
            )
        page = self.programs.call(
            "schedule_page",
            {
                "projectCode": project_code,
                recipe.schedule_filter_field: workflow_code,
                "searchVal": search,
                "pageNo": page_no,
                "pageSize": page_size,
            },
        )
        items = _response_items(
            page,
            ds_version=self.bindings.ds_version,
            resource="schedule.page",
        )
        projected = (
            None
            if items is None
            else [
                _schedule_record(
                    item,
                    bindings=self.bindings,
                    expected_workflow_code=workflow_code,
                )
                for item in items
            ]
        )
        return cast(
            "SchedulePageRecord",
            _read_page(
                page,
                projected,
                ds_version=self.bindings.ds_version,
                resource="schedule.page",
            ),
        )


def _load_bindings(
    ds_version: str,
    *,
    version_slug: str | None,
) -> _CodeNativeReadBindings:
    version_spec = _READ_SPEC_BY_VERSION.get(ds_version)
    if version_spec is None:
        message = f"DS {ds_version} has no reviewed generated code-identity recipe"
        raise WireContractError(message)

    recipe = _READ_RECIPE_BY_FAMILY[version_spec.family]
    expected_slug = f"ds_{ds_version.replace('.', '_')}"
    if version_slug is not None and version_slug != expected_slug:
        msg = "Compiled workflow reads require the exact version slug"
        raise WireContractError(msg)
    return _CodeNativeReadBindings(
        ds_version=ds_version,
        compiled=WORKFLOW_PROGRAMS.profile(ds_version),
        recipe=recipe,
        projection=version_spec.projection,
    )


def _workflow_reference(
    item: OpaqueGeneratedValue,
    *,
    bindings: _CodeNativeReadBindings,
    expected_project_code: int,
) -> WorkflowReference:
    ds_version = bindings.ds_version
    resource = "workflow.ref"
    code = _positive_identity(
        _response_field(item, "code", ds_version=ds_version, resource=resource),
        ds_version=ds_version,
        resource=resource,
        field="code",
    )
    if bindings.projection.workflow_ref_project_code_reliable:
        project_code = _positive_identity(
            _response_field(
                item,
                "projectCode",
                ds_version=ds_version,
                resource=resource,
            ),
            ds_version=ds_version,
            resource=resource,
            field="projectCode",
        )
        _require_expected_identity(
            project_code,
            expected=expected_project_code,
            ds_version=ds_version,
            resource=resource,
            field="projectCode",
        )
    name = cast(
        "str | None",
        _response_field(item, "name", ds_version=ds_version, resource=resource),
    )
    return WorkflowReference(code=code, name=name)


def _workflow_record(
    item: OpaqueGeneratedValue,
    *,
    bindings: _CodeNativeReadBindings,
    expected_project_code: int,
    expected_code: int | None,
) -> CanonicalWorkflow:
    version = bindings.ds_version
    resource = "workflow"

    def field(name: str) -> OpaqueGeneratedValue:
        return _response_field(
            item,
            name,
            ds_version=version,
            resource=resource,
        )

    code = _positive_identity(
        field("code"),
        ds_version=version,
        resource=resource,
        field="code",
    )
    project_code = _positive_identity(
        field("projectCode"),
        ds_version=version,
        resource=resource,
        field="projectCode",
    )
    _require_expected_identity(
        code,
        expected=expected_code,
        ds_version=version,
        resource=resource,
        field="code",
    )
    _require_expected_identity(
        project_code,
        expected=expected_project_code,
        ds_version=version,
        resource=resource,
        field="projectCode",
    )
    schedule = field("schedule") if bindings.projection.workflow_schedule else None
    execution_type = (
        field("executionType") if bindings.projection.workflow_execution_type else None
    )
    tenant_id = (
        field("tenantId") if bindings.projection.workflow_tenant_identity else None
    )
    tenant_code = (
        field("tenantCode") if bindings.projection.workflow_tenant_identity else None
    )
    return CanonicalWorkflow(
        id=cast("int | None", field("id")),
        code=code,
        name=cast("str | None", field("name")),
        version=cast("int | None", field("version")),
        projectCode=project_code,
        description=cast("str | None", field("description")),
        globalParams=cast("str | None", field("globalParams")),
        globalParamMap=cast("dict[str, str] | None", field("globalParamMap")),
        createTime=cast("str | None", field("createTime")),
        updateTime=cast("str | None", field("updateTime")),
        userId=cast("int", field("userId")),
        userName=cast("str | None", field("userName")),
        projectName=cast("str | None", field("projectName")),
        timeout=cast("int", field("timeout")),
        releaseState=cast("StringEnumValue | None", field("releaseState")),
        scheduleReleaseState=cast(
            "StringEnumValue | None",
            field("scheduleReleaseState"),
        ),
        schedule=(
            None
            if schedule is None
            else _schedule_record(
                schedule,
                bindings=bindings,
                expected_workflow_code=code,
            )
        ),
        executionType=cast("StringEnumValue | None", execution_type),
        tenantId=cast("int | None", tenant_id),
        tenantCode=cast("str | None", tenant_code),
    )


def _schedule_record(
    item: OpaqueGeneratedValue,
    *,
    bindings: _CodeNativeReadBindings,
    expected_workflow_code: int | None,
) -> CanonicalSchedule:
    version = bindings.ds_version
    recipe = bindings.recipe
    resource = "schedule"

    def field(name: str) -> OpaqueGeneratedValue:
        return _response_field(
            item,
            name,
            ds_version=version,
            resource=resource,
        )

    workflow_code = _positive_identity(
        field(recipe.schedule_code_field),
        ds_version=version,
        resource=resource,
        field=recipe.schedule_code_field,
    )
    _require_expected_identity(
        workflow_code,
        expected=expected_workflow_code,
        ds_version=version,
        resource=resource,
        field=recipe.schedule_code_field,
    )
    tenant_code = (
        field("tenantCode") if bindings.projection.schedule_tenant_code else None
    )
    environment_name = (
        field("environmentName")
        if bindings.projection.schedule_environment_name
        else None
    )
    return CanonicalSchedule(
        id=cast("int | None", field("id")),
        workflowDefinitionCode=workflow_code,
        workflowDefinitionName=cast(
            "str | None",
            field(recipe.schedule_name_field),
        ),
        projectName=cast("str | None", field("projectName")),
        definitionDescription=cast(
            "str | None",
            field("definitionDescription"),
        ),
        startTime=cast("str | None", field("startTime")),
        endTime=cast("str | None", field("endTime")),
        timezoneId=cast("str | None", field("timezoneId")),
        crontab=cast("str | None", field("crontab")),
        failureStrategy=cast("StringEnumValue | None", field("failureStrategy")),
        warningType=cast("StringEnumValue | None", field("warningType")),
        createTime=cast("str | None", field("createTime")),
        updateTime=cast("str | None", field("updateTime")),
        userId=cast("int", field("userId")),
        userName=cast("str | None", field("userName")),
        releaseState=cast("StringEnumValue | None", field("releaseState")),
        warningGroupId=cast("int", field("warningGroupId")),
        workflowInstancePriority=cast(
            "StringEnumValue | None",
            field(recipe.schedule_priority_field),
        ),
        workerGroup=cast("str | None", field("workerGroup")),
        tenantCode=cast("str | None", tenant_code),
        environmentCode=cast("int | None", field("environmentCode")),
        environmentName=cast("str | None", environment_name),
    )


def _response_items(
    page: OpaqueGeneratedValue,
    *,
    ds_version: str,
    resource: str,
) -> list[OpaqueGeneratedValue] | None:
    items = _response_field(
        page,
        "totalList",
        ds_version=ds_version,
        resource=resource,
    )
    if items is None or isinstance(items, list):
        return items
    raise _projection_error(
        ds_version=ds_version,
        resource=resource,
        field="totalList",
        reason="generated page items are not a list",
    )


def _read_page(
    page: OpaqueGeneratedValue,
    items: Sequence[OpaqueGeneratedValue] | None,
    *,
    ds_version: str,
    resource: str,
) -> ReadPage[OpaqueGeneratedValue]:
    def field(name: str) -> OpaqueGeneratedValue:
        return _response_field(
            page,
            name,
            ds_version=ds_version,
            resource=resource,
        )

    return ReadPage(
        totalList=items,
        total=cast("int | None", field("total")),
        totalPage=cast("int | None", field("totalPage")),
        pageSize=cast("int | None", field("pageSize")),
        currentPage=cast("int | None", field("currentPage")),
        pageNo=cast("int | None", field("pageNo")),
    )


def _positive_identity(
    value: OpaqueGeneratedValue,
    *,
    ds_version: str,
    resource: str,
    field: str,
) -> int:
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return value
    raise _projection_error(
        ds_version=ds_version,
        resource=resource,
        field=field,
        reason="identity must be a positive integer",
    )


def _require_expected_identity(
    value: int,
    *,
    expected: int | None,
    ds_version: str,
    resource: str,
    field: str,
) -> None:
    if expected is None or value == expected:
        return
    raise _projection_error(
        ds_version=ds_version,
        resource=resource,
        field=field,
        reason="response identity does not match the requested scope",
    )


def _response_field(
    value: OpaqueGeneratedValue,
    name: str,
    *,
    ds_version: str,
    resource: str,
) -> OpaqueGeneratedValue:
    try:
        return getattr(value, name)
    except AttributeError as exc:
        raise _projection_error(
            ds_version=ds_version,
            resource=resource,
            field=name,
            reason="generated payload is missing a canonical field",
        ) from exc


def _projection_error(
    *,
    ds_version: str,
    resource: str,
    field: str,
    reason: str,
) -> ApiTransportError:
    return ApiTransportError(
        "DolphinScheduler response cannot be projected to the canonical read contract.",
        details={
            "ds_version": ds_version,
            "resource": resource,
            "field": field,
            "reason": reason,
        },
        source={
            "kind": "remote",
            "system": "dolphinscheduler",
            "layer": "response",
        },
        suggestion=(
            "Verify DS_VERSION matches the server and retry after checking API health."
        ),
    )


__all__ = ["CodeNativeReadAdapter"]
