from __future__ import annotations

import json
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Literal, TypeAlias, cast

from dsctl.cli_surface import SCHEDULE_RESOURCE
from dsctl.errors import (
    ApiResultError,
    ApiTransportError,
    ConflictError,
    UserInputError,
)
from dsctl.upstream._compiled_workflow_runtime import WORKFLOW_PROGRAMS
from dsctl.upstream.bound_domain import BoundDomain
from dsctl.upstream.code_native_reads import CodeNativeReadAdapter
from dsctl.upstream.definition_models import (
    NativeCode,
    NativeId,
    NativeIdentity,
    ProjectRef,
    WorkflowRef,
    WorkflowScope,
)
from dsctl.upstream.enums import get_enum_spec
from dsctl.upstream.environments import EnvironmentAdapter
from dsctl.upstream.id_native_reads import IdNativeReadAdapter
from dsctl.upstream.identity import IdentityAdapter
from dsctl.upstream.mutation_outcomes import mutation_call, verify_mutation
from dsctl.upstream.pagination import collect_all_pages, collect_pages
from dsctl.upstream.project_preferences import (
    ProjectPreferenceAdapter,
)
from dsctl.upstream.read_models import ReadPage
from dsctl.upstream.response_projection import (
    optional_int_field,
    optional_text_field,
    positive_int,
    project_page,
    projection_error,
    response_field,
    sequence_field,
)
from dsctl.upstream.serialization import enum_value
from dsctl.upstream.wire import WireContractError

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from dsctl.client import DolphinSchedulerClient
    from dsctl.config import ClusterProfile
    from dsctl.support.json_types import JsonObject, JsonValue
    from dsctl.upstream._compiled_workflow_runtime import WorkflowPrimitive
    from dsctl.upstream._generated_types import OpaqueGeneratedValue
    from dsctl.upstream.compiled_domain import BoundCompiledPrograms
    from dsctl.upstream.definition_reads import DefinitionReads
    from dsctl.upstream.project_preferences import ProjectPreferenceWireRead
    from dsctl.upstream.protocol import (
        CurrentUserOperations,
        CurrentUserRecord,
        EnvironmentOperations,
        ScheduleCreateRequestPlan,
        ScheduleCreateSpec,
        StringEnumValue,
    )
    from dsctl.upstream.wire import PreparedCompiledWireCall


_SCHEDULE_NOT_EXISTS = 10203
_SCHEDULE_ALREADY_EXISTS = 10204
_SCHEDULE_PAGE_SIZE = 100


class _UnsetValue:
    """Sentinel that distinguishes an omitted patch field from explicit null."""


_UNSET = _UnsetValue()
_PROJECT_PREFERENCE_VERSIONS = frozenset(
    {
        "3.2.0",
        "3.2.1",
        "3.2.2",
        "3.3.1",
        "3.3.2",
        "3.4.0",
        "3.4.1",
        "3.4.2",
        "3.4.3",
    }
)

_IdentityKind = Literal["id", "code"]
_EntityResult = Literal["none", "entity"]
_LifecycleResult = Literal["none", "boolean"]
ScheduleFingerprintPart: TypeAlias = (
    str | int | None | type[NativeId] | type[NativeCode]
)


@dataclass(frozen=True)
class ScheduleState:
    """One complete schedule state independent of an exact wire vocabulary."""

    crontab: str
    start_time: str
    end_time: str
    timezone_id: str | None
    failure_strategy: str = "CONTINUE"
    warning_type: str = "NONE"
    warning_group_id: int = 0
    workflow_instance_priority: str = "MEDIUM"
    worker_group: str = "default"
    tenant_code: str | None = None
    environment_code: int | None = None
    missed_fire_policy: str | None = None
    provided_fields: frozenset[str] = frozenset()

    def to_data(self) -> JsonObject:
        """Render the stable mutation vocabulary used by explain and confirmation."""
        data: JsonObject = {
            "crontab": self.crontab,
            "startTime": self.start_time,
            "endTime": self.end_time,
            "timezoneId": self.timezone_id,
            "failureStrategy": self.failure_strategy,
            "warningType": self.warning_type,
            "warningGroupId": self.warning_group_id,
            "workflowInstancePriority": self.workflow_instance_priority,
            "workerGroup": self.worker_group,
            "tenantCode": self.tenant_code,
            "environmentCode": self.environment_code,
        }
        if self.missed_fire_policy is not None:
            data["missedFirePolicy"] = self.missed_fire_policy
        return data


@dataclass(frozen=True)
class ScheduleUpdatePatch:
    """Omission-preserving fields for one read/merge/write update."""

    crontab: str | _UnsetValue = _UNSET
    start_time: str | _UnsetValue = _UNSET
    end_time: str | _UnsetValue = _UNSET
    timezone_id: str | None | _UnsetValue = _UNSET
    failure_strategy: str | _UnsetValue = _UNSET
    warning_type: str | _UnsetValue = _UNSET
    warning_group_id: int | _UnsetValue = _UNSET
    workflow_instance_priority: str | _UnsetValue = _UNSET
    worker_group: str | _UnsetValue = _UNSET
    environment_code: int | None | _UnsetValue = _UNSET
    missed_fire_policy: str | None | _UnsetValue = _UNSET

    def requested_fields(self) -> frozenset[str]:
        """Return caller-provided DS field names without inferring from values."""
        requested: set[str] = set()
        for attribute, field in _PATCH_FIELDS.items():
            if getattr(self, attribute) is not _UNSET:
                requested.add(field)
        return frozenset(requested)


_PATCH_FIELDS: Mapping[str, str] = {
    "crontab": "crontab",
    "start_time": "startTime",
    "end_time": "endTime",
    "timezone_id": "timezoneId",
    "failure_strategy": "failureStrategy",
    "warning_type": "warningType",
    "warning_group_id": "warningGroupId",
    "workflow_instance_priority": "workflowInstancePriority",
    "worker_group": "workerGroup",
    "environment_code": "environmentCode",
    "missed_fire_policy": "missedFirePolicy",
}
_SCHEDULE_EXPRESSION_FIELDS = frozenset(
    {"crontab", "startTime", "endTime", "timezoneId", "missedFirePolicy"}
)


@dataclass(frozen=True)
class ScheduleSnapshot:
    """Exact schedule response projected without inventing an id/code alias."""

    ds_version: str
    id: int
    workflow_native: NativeIdentity
    workflow_name: str | None
    project_name: str | None
    definition_description: str | None
    start_time: str | None
    end_time: str | None
    timezone_id: str | None
    crontab: str | None
    failure_strategy: str | None
    warning_type: str | None
    create_time: str | None
    update_time: str | None
    user_id: int
    user_name: str | None
    release_state: str | None
    warning_group_id: int
    workflow_instance_priority: str | None
    worker_group: str | None
    tenant_code: str | None
    environment_code: int | None
    environment_name: str | None
    workflow_field: str
    workflow_name_field: str
    priority_field: str
    has_timezone: bool
    has_tenant: bool
    has_environment: bool
    has_environment_name: bool
    missed_fire_policy: str | None = None
    has_missed_fire_policy: bool = False

    @property
    def startTime(self) -> str | None:  # noqa: N802
        """Expose the stable DS-facing schedule field name."""
        return self.start_time

    @property
    def endTime(self) -> str | None:  # noqa: N802
        """Expose the stable DS-facing schedule field name."""
        return self.end_time

    @property
    def timezoneId(self) -> str | None:  # noqa: N802
        """Expose the stable DS-facing schedule field name."""
        return self.timezone_id

    @property
    def failureStrategy(self) -> str | None:  # noqa: N802
        """Expose the stable DS-facing schedule field name."""
        return self.failure_strategy

    @property
    def workflowInstancePriority(self) -> str | None:  # noqa: N802
        """Expose the stable DS-facing schedule field name."""
        return self.workflow_instance_priority

    @property
    def releaseState(self) -> str | None:  # noqa: N802
        """Expose the stable DS-facing schedule field name."""
        return self.release_state

    @property
    def missedFirePolicy(self) -> str | None:  # noqa: N802
        """Expose a stored missed fire policy without inventing a default."""
        return self.missed_fire_policy

    def to_data(self) -> JsonObject:
        """Render the stable CLI projection above the native wire seam."""
        data: JsonObject = {
            "id": self.id,
            "workflowDefinitionCode": self.workflow_native.value,
            "workflowDefinitionName": self.workflow_name,
            "projectName": self.project_name,
            "definitionDescription": self.definition_description,
            "startTime": self.start_time,
            "endTime": self.end_time,
            "timezoneId": self.timezone_id,
            "crontab": self.crontab,
            "failureStrategy": self.failure_strategy,
            "warningType": self.warning_type,
            "createTime": self.create_time,
            "updateTime": self.update_time,
            "userId": self.user_id,
            "userName": self.user_name,
            "releaseState": self.release_state,
            "warningGroupId": self.warning_group_id,
            "workflowInstancePriority": self.workflow_instance_priority,
            "workerGroup": self.worker_group,
            "tenantCode": self.tenant_code,
            "environmentCode": self.environment_code,
            "environmentName": self.environment_name,
        }
        if self.has_missed_fire_policy:
            data["missedFirePolicy"] = self.missed_fire_policy
        return data

    def state(self) -> ScheduleState:
        """Return the complete mutable state, failing closed on missing fields."""
        required = {
            "crontab": self.crontab,
            "startTime": self.start_time,
            "endTime": self.end_time,
            "failureStrategy": self.failure_strategy,
            "warningType": self.warning_type,
            self.priority_field: self.workflow_instance_priority,
            "workerGroup": self.worker_group,
        }
        missing = sorted(field for field, value in required.items() if value is None)
        if self.has_timezone and self.timezone_id is None:
            missing.append("timezoneId")
        if self.has_tenant and self.tenant_code is None:
            missing.append("tenantCode")
        if missing:
            raise projection_error(
                ds_version=self.ds_version,
                resource=SCHEDULE_RESOURCE,
                field=",".join(missing),
                reason="schedule update readback omitted required mutable fields",
            )
        return ScheduleState(
            crontab=cast("str", self.crontab),
            start_time=cast("str", self.start_time),
            end_time=cast("str", self.end_time),
            timezone_id=self.timezone_id,
            failure_strategy=cast("str", self.failure_strategy),
            warning_type=cast("str", self.warning_type),
            warning_group_id=self.warning_group_id,
            workflow_instance_priority=cast("str", self.workflow_instance_priority),
            worker_group=cast("str", self.worker_group),
            tenant_code=self.tenant_code,
            environment_code=self.environment_code,
            missed_fire_policy=self.missed_fire_policy,
        )


@dataclass(frozen=True)
class ScheduleListing:
    """One project-scoped schedule page plus resolved identities."""

    project: ProjectRef
    workflow: WorkflowRef | None
    page: ReadPage[ScheduleSnapshot]


@dataclass(frozen=True)
class SchedulePreview:
    """One resolved project and exact preview fire-time sequence."""

    project: ProjectRef
    times: tuple[str, ...]


@dataclass(frozen=True)
class LocatedSchedule:
    """One globally identified schedule with its true visible project scope."""

    project: ProjectRef
    schedule: ScheduleSnapshot


@dataclass(frozen=True)
class PreparedScheduleCreate:
    """Source-validated create recipe safe to confirm before dispatch."""

    ds_version: str
    scope: WorkflowScope
    state: ScheduleState
    preview_times: tuple[str, ...]
    form: PreparedCompiledWireCall
    tenant_source: str | None
    project_preference_used_fields: tuple[str, ...]


@dataclass(frozen=True)
class PreparedScheduleUpdate:
    """Source-validated read/merge/write recipe safe to confirm before dispatch."""

    ds_version: str
    current: LocatedSchedule
    state: ScheduleState
    requested_fields: frozenset[str]
    preview_times: tuple[str, ...]
    form: PreparedCompiledWireCall
    fingerprint: tuple[ScheduleFingerprintPart, ...]


@dataclass(frozen=True)
class _ScheduleRecipe:
    identity_kind: _IdentityKind
    workflow_field: str
    workflow_name_field: str
    priority_field: str
    has_timezone: bool
    has_tenant: bool
    has_environment: bool
    has_environment_name: bool
    preserves_receivers: bool
    create_result: _EntityResult
    update_result: _EntityResult
    lifecycle_result: _LifecycleResult
    legacy_routes: bool = False
    workflow_filter_required: bool = False
    has_missed_fire_policy: bool = False
    missed_fire_policy_default: str | None = None


@dataclass(frozen=True)
class ScheduleContractFeatures:
    """Selected-version schedule fields exposed above the exact wire seam."""

    timezone: bool
    tenant: bool
    environment: bool
    missed_fire_policy: bool = False
    missed_fire_policy_choices: tuple[str, ...] = ()
    missed_fire_policy_default: str | None = None


_LEGACY_RECIPE = _ScheduleRecipe(
    identity_kind="id",
    workflow_field="processDefinitionId",
    workflow_name_field="processDefinitionName",
    priority_field="processInstancePriority",
    has_timezone=False,
    has_tenant=False,
    has_environment=False,
    has_environment_name=False,
    preserves_receivers=True,
    create_result="none",
    update_result="none",
    lifecycle_result="none",
    legacy_routes=True,
    workflow_filter_required=True,
)
_CODE_VOID_RECIPE = _ScheduleRecipe(
    identity_kind="code",
    workflow_field="processDefinitionCode",
    workflow_name_field="processDefinitionName",
    priority_field="processInstancePriority",
    has_timezone=True,
    has_tenant=False,
    has_environment=True,
    has_environment_name=False,
    preserves_receivers=False,
    create_result="entity",
    update_result="none",
    lifecycle_result="none",
    workflow_filter_required=True,
)
_TENANT_VOID_RECIPE = replace(
    _CODE_VOID_RECIPE,
    has_tenant=True,
    has_environment_name=True,
    workflow_filter_required=False,
)
_TENANT_BOOLEAN_RECIPE = replace(
    _TENANT_VOID_RECIPE,
    lifecycle_result="boolean",
)
_TENANT_ENTITY_RECIPE = replace(
    _TENANT_BOOLEAN_RECIPE,
    update_result="entity",
)
_WORKFLOW_RECIPE = replace(
    _TENANT_ENTITY_RECIPE,
    workflow_field="workflowDefinitionCode",
    workflow_name_field="workflowDefinitionName",
    priority_field="workflowInstancePriority",
)

_SCHEDULE_RECIPES = {
    "legacy_id": _LEGACY_RECIPE,
    "code": _CODE_VOID_RECIPE,
    "tenant_void": _TENANT_VOID_RECIPE,
    "tenant_boolean": _TENANT_BOOLEAN_RECIPE,
    "tenant_entity": _TENANT_ENTITY_RECIPE,
    "workflow": _WORKFLOW_RECIPE,
    "workflow_missed_fire": replace(
        _WORKFLOW_RECIPE,
        has_missed_fire_policy=True,
        missed_fire_policy_default="FIRE_ALL_MISSED",
    ),
}


def schedule_contract_features(ds_version: str) -> ScheduleContractFeatures:
    """Project exact recipe evidence for version-aware CLI discovery."""
    recipe = _schedule_recipe(WORKFLOW_PROGRAMS.profile(ds_version).recipe_id)
    return ScheduleContractFeatures(
        timezone=recipe.has_timezone,
        tenant=recipe.has_tenant,
        environment=recipe.has_environment,
        missed_fire_policy=recipe.has_missed_fire_policy,
        missed_fire_policy_choices=_missed_fire_choices(ds_version, recipe),
        missed_fire_policy_default=recipe.missed_fire_policy_default,
    )


@dataclass(frozen=True)
class ScheduleDomain:
    """Deep schedule surface: resolution, wire adaptation, and reconciliation."""

    schedules: ScheduleOperations


class ScheduleAdapter:
    """Schedule behavior backed by the exact compiled workflow runtime."""

    def __init__(self, ds_version: str) -> None:
        """Load one exact profile and its reviewed schedule recipe."""
        self._profile = WORKFLOW_PROGRAMS.profile(ds_version)
        self._recipe = _schedule_recipe(self._profile.recipe_id)
        self.ds_version = ds_version

    @classmethod
    def for_version(cls, ds_version: str) -> ScheduleAdapter:
        """Return the adapter for one explicitly reviewed DS version."""
        return cls(ds_version)

    def bind(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> ScheduleDomain:
        """Bind the deep schedule operations and definition-read dependency."""
        read_adapter = (
            IdNativeReadAdapter()
            if self.ds_version == "1.3.9"
            else CodeNativeReadAdapter.for_version(self.ds_version)
        )
        definitions = read_adapter.bind_read(
            profile,
            http_client=http_client,
        ).definitions
        current_user = IdentityAdapter.for_version(self.ds_version).bind_identity(
            profile, http_client=http_client
        )
        environments = (
            None
            if self.ds_version == "1.3.9"
            else EnvironmentAdapter.for_version(self.ds_version)
            .bind(profile, http_client=http_client)
            .environments
        )
        project_preferences = (
            ProjectPreferenceAdapter.for_version(self.ds_version).bind_read(
                profile, http_client=http_client
            )
            if self.ds_version in _PROJECT_PREFERENCE_VERSIONS
            else None
        )
        return ScheduleDomain(
            schedules=ScheduleOperations(
                WORKFLOW_PROGRAMS.bind(self._profile, profile, http_client=http_client),
                self._recipe,
                definitions,
                current_user,
                environments,
                project_preferences,
            )
        )


SCHEDULE_DOMAIN = BoundDomain[ScheduleDomain](
    name=SCHEDULE_RESOURCE,
    adapter_for_version=ScheduleAdapter.for_version,
)


@dataclass(frozen=True)
class ScheduleOperations:
    """Caller-cohesive schedule operations backed by exact compiled programs."""

    programs: BoundCompiledPrograms[WorkflowPrimitive]
    recipe: _ScheduleRecipe
    definitions: DefinitionReads
    current_user: CurrentUserOperations
    environments: EnvironmentOperations | None
    project_preferences: ProjectPreferenceWireRead | None

    @property
    def ds_version(self) -> str:
        """Return the exact DS version selected by the compiled profile."""
        return self.programs.ds_version

    @property
    def supports_timezone(self) -> bool:
        """Return whether the exact wire carries a schedule timezone."""
        return self.recipe.has_timezone

    @property
    def supports_tenant(self) -> bool:
        """Return whether the exact wire carries a schedule tenant."""
        return self.recipe.has_tenant

    @property
    def supports_environment(self) -> bool:
        """Return whether the exact wire carries a schedule environment."""
        return self.recipe.has_environment

    def current_user_record(self) -> CurrentUserRecord:
        """Return authenticated-user defaults through the exact identity adapter."""
        return self.current_user.current()

    def state_data(self, state: ScheduleState) -> JsonObject:
        """Render one state using the stable CLI projection vocabulary."""
        data = state.to_data()
        if self.recipe.has_missed_fire_policy:
            data["missedFirePolicy"] = state.missed_fire_policy
        return data

    def plan_create(
        self,
        *,
        spec: ScheduleCreateSpec[int | str],
    ) -> ScheduleCreateRequestPlan:
        """Describe the exact generated form request without sending it."""
        recipe = self.recipe
        if spec.missed_fire_policy is not None and not recipe.has_missed_fire_policy:
            raise _unsupported_schedule_field(
                self.ds_version,
                "missedFirePolicy",
                "This exact schedule contract has no missed fire policy field.",
            )
        if recipe.legacy_routes:
            if spec.project_name is None or not spec.project_name.strip():
                message = "Legacy schedule create planning requires a project name"
                raise WireContractError(message)
            if spec.timezone_id is not None:
                raise _unsupported_schedule_field(
                    self.ds_version,
                    "timezoneId",
                    (
                        "DolphinScheduler 1.3 uses the server-local timezone and "
                        "has no timezone field."
                    ),
                )
            schedule_state = ScheduleState(
                crontab=spec.crontab,
                start_time=spec.start_time,
                end_time=spec.end_time,
                timezone_id=None,
            )
            legacy_form: JsonObject = {
                recipe.workflow_field: spec.workflow_code,
                "schedule": _schedule_json(schedule_state, recipe),
                "warningGroupId": spec.warning_group_id,
            }
            optional_fields = {
                "failureStrategy": spec.failure_strategy or "CONTINUE",
                "warningType": spec.warning_type or "NONE",
                recipe.priority_field: spec.workflow_instance_priority or "MEDIUM",
                "workerGroup": spec.worker_group or "default",
            }
            legacy_form.update(
                {
                    field: value
                    for field, value in optional_fields.items()
                    if value is not None
                }
            )
            return {
                "method": "POST",
                "path": f"/projects/{spec.project_name.strip()}/schedule/create",
                "form": legacy_form,
            }
        schedule_state = ScheduleState(
            crontab=spec.crontab,
            start_time=spec.start_time,
            end_time=spec.end_time,
            timezone_id=spec.timezone_id,
            missed_fire_policy=(
                recipe.missed_fire_policy_default
                if spec.missed_fire_policy is None
                else spec.missed_fire_policy
            ),
        )
        self._validate_state_values(schedule_state)
        form: JsonObject = {
            recipe.workflow_field: spec.workflow_code,
            "schedule": _schedule_json(schedule_state, recipe),
            "warningGroupId": spec.warning_group_id,
        }
        modern_optional_fields: JsonObject = {
            "failureStrategy": spec.failure_strategy,
            "warningType": spec.warning_type,
            recipe.priority_field: spec.workflow_instance_priority,
            "workerGroup": spec.worker_group,
            "tenantCode": spec.tenant_code if recipe.has_tenant else None,
            "environmentCode": (
                spec.environment_code if recipe.has_environment else None
            ),
        }
        form.update(
            {
                field: value
                for field, value in modern_optional_fields.items()
                if value is not None
            }
        )
        return {
            "method": "POST",
            "path": f"/projects/{spec.project_code}/schedules",
            "form": form,
        }

    def plan_release(
        self,
        project: ProjectRef,
        *,
        schedule_id: int | str,
        state: Literal["ONLINE", "OFFLINE"],
    ) -> JsonObject:
        """Describe the exact generated lifecycle request without sending it."""
        recipe = self.recipe
        operation = "online" if state == "ONLINE" else "offline"
        if recipe.legacy_routes:
            project_name = _project_route_arg(project, recipe)
            return {
                "method": "POST",
                "path": f"/projects/{project_name}/schedule/{operation}",
                "form": {"id": schedule_id},
            }
        project_code = _project_route_arg(project, recipe)
        return {
            "method": "POST",
            "path": f"/projects/{project_code}/schedules/{schedule_id}/{operation}",
        }

    def list(
        self,
        project_selector: str,
        *,
        workflow_selector: str | None,
        search: str | None,
        page_no: int,
        page_size: int,
        all_pages: bool,
    ) -> ScheduleListing:
        """Resolve one project/workflow scope and return the requested page."""
        project = self.definitions.resolve_project(project_selector)
        workflow = (
            None
            if workflow_selector is None
            else self.definitions.resolve_workflow(
                project_selector,
                workflow_selector,
            ).workflow
        )
        page = self._requested_page(
            project,
            workflow=workflow,
            search=search,
            page_no=page_no,
            page_size=page_size,
            all_pages=all_pages,
        )
        return ScheduleListing(project=project, workflow=workflow, page=page)

    def get(
        self,
        schedule_id: int,
        *,
        project_selector: str | None = None,
    ) -> LocatedSchedule:
        """Find one schedule through one selected or bounded visible-project scope."""
        if project_selector is not None:
            project = self.definitions.resolve_project(project_selector)
            return LocatedSchedule(
                project=project,
                schedule=self._find_in_project(project, schedule_id),
            )

        matches: list[LocatedSchedule] = []
        for project in self.definitions.visible_project_refs():
            matches.extend(
                LocatedSchedule(project, schedule)
                for schedule in self._all_in_project(project)
                if schedule.id == schedule_id
            )
        if len(matches) == 1:
            return matches[0]
        if not matches:
            raise ApiResultError(
                result_code=_SCHEDULE_NOT_EXISTS,
                result_message=f"schedule {schedule_id} does not exist",
            )
        message = "Schedule id matched more than one visible project"
        raise ApiTransportError(
            message,
            details={"schedule_id": schedule_id, "match_count": len(matches)},
        )

    def attached(self, scope: WorkflowScope) -> ScheduleSnapshot | None:
        """Return the zero-or-one schedule attached to a resolved workflow."""
        matches = tuple(self._for_workflow(scope.project, scope.workflow))
        if not matches:
            return None
        if len(matches) == 1:
            return matches[0]
        message = "Workflow has more than one attached schedule"
        raise ApiTransportError(
            message,
            details={
                "project": scope.project.native.value,
                "workflow": scope.workflow.native.value,
                "schedule_ids": [item.id for item in matches],
            },
        )

    def preview(
        self,
        project_selector: str,
        state: ScheduleState,
    ) -> SchedulePreview:
        """Preview one complete state through the exact project-scoped route."""
        self._validate_state_support(state)
        project = self.definitions.resolve_project(project_selector)
        return SchedulePreview(project=project, times=self._preview(project, state))

    def preview_existing(
        self,
        schedule_id: int,
        *,
        project_selector: str | None = None,
    ) -> tuple[LocatedSchedule, tuple[str, ...]]:
        """Locate one schedule and preview its complete current state."""
        located = self.get(schedule_id, project_selector=project_selector)
        return located, self._preview(located.project, located.schedule.state())

    def prepare_create(
        self,
        project_selector: str,
        workflow_selector: str,
        state: ScheduleState,
    ) -> PreparedScheduleCreate:
        """Resolve, reject duplicates, and preview without dispatching mutation."""
        self._validate_state_support(state)
        scope = self.definitions.resolve_workflow(
            project_selector,
            workflow_selector,
        )
        state, tenant_source, preference_fields = self._apply_create_defaults(
            scope.project,
            state,
        )
        self._validate_state_values(state)
        if self._for_workflow(scope.project, scope.workflow):
            raise ApiResultError(
                result_code=_SCHEDULE_ALREADY_EXISTS,
                result_message="schedule already exists for workflow",
            )
        preview_times = self._preview(scope.project, state)
        return PreparedScheduleCreate(
            ds_version=self.ds_version,
            scope=scope,
            state=state,
            preview_times=preview_times,
            form=self._mutation_form(
                state,
                project=scope.project,
                workflow=scope.workflow,
                receivers=scope.view.receivers,
                receivers_cc=scope.view.receivers_cc,
                create=True,
            ),
            tenant_source=tenant_source,
            project_preference_used_fields=preference_fields,
        )

    def effective_create_state(
        self,
        project: ProjectRef,
        state: ScheduleState,
    ) -> ScheduleState:
        """Resolve project preferences for a schedule before its workflow exists."""
        effective, _, _ = self._apply_create_defaults(project, state)
        return effective

    def create(self, prepared: PreparedScheduleCreate) -> ScheduleSnapshot:
        """Revalidate absence, dispatch once, and return authoritative readback."""
        self._require_prepared_version(prepared.ds_version)
        scope = prepared.scope
        if self._for_workflow(scope.project, scope.workflow):
            message = (
                "Schedule state changed after preview; the workflow now has a schedule"
            )
            raise ConflictError(
                message,
                details={"workflow": scope.workflow.native.value},
                suggestion="Inspect the workflow schedule before retrying create.",
            )
        result = mutation_call(
            lambda: self.programs.execute("schedule_create", prepared.form).payload,
            ds_version=self.ds_version,
            resource=SCHEDULE_RESOURCE,
            operation="create",
        )

        def verify() -> ScheduleSnapshot:
            _require_entity_result(
                result,
                expected=self.recipe.create_result,
                operation="create",
                ds_version=self.ds_version,
            )
            matches = self._for_workflow(scope.project, scope.workflow)
            if len(matches) != 1:
                message = (
                    "Schedule create readback did not return one exact workflow match"
                )
                raise ApiTransportError(
                    message,
                    details={
                        "workflow": scope.workflow.native.value,
                        "match_count": len(matches),
                    },
                )
            _require_state_match(matches[0], prepared.state, ds_version=self.ds_version)
            return matches[0]

        return verify_mutation(
            verify,
            ds_version=self.ds_version,
            resource=SCHEDULE_RESOURCE,
            operation="create",
        )

    def prepare_update(
        self,
        schedule_id: int,
        patch: ScheduleUpdatePatch,
        *,
        project_selector: str | None = None,
    ) -> PreparedScheduleUpdate:
        """Read, merge omissions, preview, and preserve one stale-state guard."""
        self._validate_patch_support(patch)
        current = self.get(schedule_id, project_selector=project_selector)
        self._validate_explicit_update_environment(patch)
        state = _merge_patch(current.schedule.state(), patch)
        state = replace(state, provided_fields=patch.requested_fields())
        self._validate_state_support(state, update=True)
        self._validate_state_values(state)
        preview_times = self._preview(current.project, state)
        workflow = WorkflowRef(
            native=current.schedule.workflow_native,
            name=current.schedule.workflow_name,
            version=None,
        )
        receivers: str | None = None
        receivers_cc: str | None = None
        if self.recipe.preserves_receivers:
            workflow_scope = self.definitions.resolve_workflow(
                _project_selector(current.project),
                str(workflow.native.value),
            )
            receivers = workflow_scope.view.receivers
            receivers_cc = workflow_scope.view.receivers_cc
        return PreparedScheduleUpdate(
            ds_version=self.ds_version,
            current=current,
            state=state,
            requested_fields=patch.requested_fields(),
            preview_times=preview_times,
            form=self._mutation_form(
                state,
                project=current.project,
                workflow=workflow,
                schedule_id=current.schedule.id,
                receivers=receivers,
                receivers_cc=receivers_cc,
                create=False,
            ),
            fingerprint=_schedule_fingerprint(current.schedule),
        )

    def update(self, prepared: PreparedScheduleUpdate) -> ScheduleSnapshot:
        """Reject stale state, dispatch once, and verify every merged field."""
        self._require_prepared_version(prepared.ds_version)
        current = self._find_in_project(
            prepared.current.project,
            prepared.current.schedule.id,
        )
        if _schedule_fingerprint(current) != prepared.fingerprint:
            message = "Schedule changed after preview; update was not sent"
            raise ConflictError(
                message,
                details={"schedule_id": prepared.current.schedule.id},
                suggestion="Run schedule explain again against the current state.",
            )
        schedule_id = prepared.current.schedule.id
        result = mutation_call(
            lambda: self.programs.execute("schedule_update", prepared.form).payload,
            ds_version=self.ds_version,
            resource=SCHEDULE_RESOURCE,
            operation="update",
        )

        def verify() -> ScheduleSnapshot:
            _require_entity_result(
                result,
                expected=self.recipe.update_result,
                operation="update",
                ds_version=self.ds_version,
            )
            refreshed = self._find_in_project(
                prepared.current.project,
                schedule_id,
            )
            _require_state_match(refreshed, prepared.state, ds_version=self.ds_version)
            return refreshed

        return verify_mutation(
            verify,
            ds_version=self.ds_version,
            resource=SCHEDULE_RESOURCE,
            operation="update",
        )

    def delete(
        self,
        schedule_id: int,
        *,
        project_selector: str | None = None,
    ) -> tuple[ScheduleSnapshot, bool]:
        """Delete once through the true project scope and verify absence."""
        located = self.get(schedule_id, project_selector=project_selector)
        prepared = self.programs.prepare(
            "schedule_delete",
            {
                **self._project_args(located.project),
                "scheduleId" if self.recipe.legacy_routes else "id": schedule_id,
            },
        )
        result = mutation_call(
            lambda: self.programs.execute("schedule_delete", prepared).payload,
            ds_version=self.ds_version,
            resource=SCHEDULE_RESOURCE,
            operation="delete",
        )

        def verify() -> bool:
            _require_none_result(result, operation="delete", ds_version=self.ds_version)
            try:
                self._find_in_project(located.project, schedule_id)
            except ApiResultError as error:
                if error.result_code == _SCHEDULE_NOT_EXISTS:
                    return True
                raise
            message = "Schedule deletion readback still returned the deleted id"
            raise ApiTransportError(
                message,
                details={"schedule_id": schedule_id},
            )

        deleted = verify_mutation(
            verify,
            ds_version=self.ds_version,
            resource=SCHEDULE_RESOURCE,
            operation="delete",
        )
        return located.schedule, deleted

    def online(
        self,
        schedule_id: int,
        *,
        project_selector: str | None = None,
    ) -> ScheduleSnapshot:
        """Publish one schedule online and verify its refreshed state."""
        return self._set_release_state(
            schedule_id,
            online=True,
            project_selector=project_selector,
        )

    def offline(
        self,
        schedule_id: int,
        *,
        project_selector: str | None = None,
    ) -> ScheduleSnapshot:
        """Take one schedule offline and verify its refreshed state."""
        return self._set_release_state(
            schedule_id,
            online=False,
            project_selector=project_selector,
        )

    def _set_release_state(
        self,
        schedule_id: int,
        *,
        online: bool,
        project_selector: str | None,
    ) -> ScheduleSnapshot:
        located = self.get(schedule_id, project_selector=project_selector)
        recipe = self.recipe
        operation = "online" if online else "offline"
        primitive: WorkflowPrimitive = (
            "schedule_online" if online else "schedule_offline"
        )
        prepared = self.programs.prepare(
            primitive, {**self._project_args(located.project), "id": schedule_id}
        )
        result = mutation_call(
            lambda: self.programs.execute(primitive, prepared).payload,
            ds_version=self.ds_version,
            resource=SCHEDULE_RESOURCE,
            operation=operation,
        )

        def verify() -> ScheduleSnapshot:
            _require_lifecycle_result(
                result,
                expected=recipe.lifecycle_result,
                operation=operation,
                ds_version=self.ds_version,
            )
            refreshed = self._find_in_project(located.project, schedule_id)
            expected = "ONLINE" if online else "OFFLINE"
            if refreshed.release_state != expected:
                message = "Schedule lifecycle readback did not reach requested state"
                raise ApiTransportError(
                    message,
                    details={
                        "schedule_id": schedule_id,
                        "expected": expected,
                        "actual": refreshed.release_state,
                    },
                )
            return refreshed

        return verify_mutation(
            verify,
            ds_version=self.ds_version,
            resource=SCHEDULE_RESOURCE,
            operation=operation,
        )

    def _requested_page(
        self,
        project: ProjectRef,
        *,
        workflow: WorkflowRef | None,
        search: str | None,
        page_no: int,
        page_size: int,
        all_pages: bool,
    ) -> ReadPage[ScheduleSnapshot]:
        if workflow is None and self.recipe.workflow_filter_required:
            # SchedulerService.querySchedule requires a real definition through
            # 3.1.9. Its 3.2.0 project-wide query is the first to accept code 0.
            # Build the project view from scoped pages, then apply CLI paging.
            schedules = self._all_in_project(project, search=search)
            start = (page_no - 1) * page_size
            stop = None if all_pages else start + page_size
            return ReadPage(
                totalList=schedules[start:stop],
                total=len(schedules),
                totalPage=(len(schedules) + page_size - 1) // page_size,
                pageSize=page_size,
                currentPage=page_no,
                pageNo=page_no,
            )
        if not all_pages:
            return self._page(
                project,
                workflow=workflow,
                search=search,
                page_no=page_no,
                page_size=page_size,
            )
        collected = collect_all_pages(
            lambda current_page_no, current_page_size: self._page(
                project,
                workflow=workflow,
                search=search,
                page_no=current_page_no,
                page_size=current_page_size,
            ),
            page_no=page_no,
            page_size=page_size,
        )
        return ReadPage(
            totalList=collected.items,
            total=collected.total,
            totalPage=collected.total_pages,
            pageSize=collected.page_size,
            currentPage=collected.page_no,
            pageNo=collected.page_no,
        )

    def _page(
        self,
        project: ProjectRef,
        *,
        workflow: WorkflowRef | None,
        search: str | None,
        page_no: int,
        page_size: int,
    ) -> ReadPage[ScheduleSnapshot]:
        if workflow is None and self.recipe.workflow_filter_required:
            message = "This exact schedule controller requires a workflow filter"
            raise WireContractError(message)
        workflow_value = 0 if workflow is None else workflow.native.value
        page = self.programs.call(
            "schedule_page",
            {
                **self._project_args(project),
                self.recipe.workflow_field: workflow_value,
                "searchVal": search,
                "pageNo": page_no,
                "pageSize": page_size,
            },
        )
        items = sequence_field(
            page,
            "totalList",
            ds_version=self.ds_version,
            resource=SCHEDULE_RESOURCE,
        )
        return cast(
            "ReadPage[ScheduleSnapshot]",
            project_page(
                page,
                [self._snapshot(item) for item in items],
                requested_page_no=page_no,
                requested_page_size=page_size,
                ds_version=self.ds_version,
                resource=SCHEDULE_RESOURCE,
            ),
        )

    def _all_in_project(
        self, project: ProjectRef, *, search: str | None = None
    ) -> Sequence[ScheduleSnapshot]:
        if self.recipe.workflow_filter_required:
            schedules = [
                schedule
                for workflow in self.definitions.visible_workflow_refs(project)
                for schedule in self._for_workflow(project, workflow, search=search)
            ]
            return tuple(sorted(schedules, key=lambda schedule: schedule.id))
        return collect_pages(
            lambda page_no, page_size: self._page(
                project,
                workflow=None,
                search=search,
                page_no=page_no,
                page_size=page_size,
            ),
            resource=SCHEDULE_RESOURCE,
        )

    def _for_workflow(
        self,
        project: ProjectRef,
        workflow: WorkflowRef,
        *,
        search: str | None = None,
    ) -> Sequence[ScheduleSnapshot]:
        return collect_pages(
            lambda page_no, page_size: self._page(
                project,
                workflow=workflow,
                search=search,
                page_no=page_no,
                page_size=page_size,
            ),
            resource=SCHEDULE_RESOURCE,
        )

    def _find_in_project(
        self,
        project: ProjectRef,
        schedule_id: int,
    ) -> ScheduleSnapshot:
        matches = [
            item for item in self._all_in_project(project) if item.id == schedule_id
        ]
        if len(matches) == 1:
            return matches[0]
        if not matches:
            raise ApiResultError(
                result_code=_SCHEDULE_NOT_EXISTS,
                result_message=f"schedule {schedule_id} does not exist",
            )
        message = "Schedule id appeared more than once in one project page closure"
        raise ApiTransportError(
            message,
            details={"schedule_id": schedule_id, "match_count": len(matches)},
        )

    def _preview(self, project: ProjectRef, state: ScheduleState) -> tuple[str, ...]:
        payload = self.programs.call(
            "schedule_preview",
            {
                **self._project_args(project),
                "schedule": _schedule_json(state, self.recipe),
            },
        )
        if isinstance(payload, list) and all(isinstance(item, str) for item in payload):
            return tuple(payload)
        raise projection_error(
            ds_version=self.ds_version,
            resource=SCHEDULE_RESOURCE,
            field="previewResult",
            reason="preview result is not a list of fire-time strings",
        )

    def _mutation_form(
        self,
        state: ScheduleState,
        *,
        project: ProjectRef,
        workflow: WorkflowRef,
        schedule_id: int | None = None,
        receivers: str | None,
        receivers_cc: str | None,
        create: bool,
    ) -> PreparedCompiledWireCall:
        recipe = self.recipe
        values: JsonObject = {
            **self._project_args(project),
            # Every exact update requires this parameter; an empty value keeps
            # the stored calendar without revalidating its original start time.
            "schedule": (
                _schedule_json(state, recipe)
                if create or state.provided_fields & _SCHEDULE_EXPRESSION_FIELDS
                else ""
            ),
            "warningType": state.warning_type,
            "warningGroupId": state.warning_group_id,
            "failureStrategy": state.failure_strategy,
            "workerGroup": state.worker_group,
            recipe.priority_field: state.workflow_instance_priority,
        }
        if create:
            values[recipe.workflow_field] = workflow.native.value
        # The source codec alone decides whether id belongs to path or form.
        else:
            if schedule_id is None:
                message = "Schedule update requires its schedule id"
                raise WireContractError(message)
            values["id"] = schedule_id
        if recipe.preserves_receivers:
            values["receivers"] = receivers
            values["receiversCc"] = receivers_cc
        if recipe.has_tenant:
            values["tenantCode"] = state.tenant_code
        if recipe.has_environment:
            values["environmentCode"] = (
                -1 if state.environment_code is None else state.environment_code
            )
        return self.programs.prepare(
            "schedule_create" if create else "schedule_update", values
        )

    def _snapshot(self, item: OpaqueGeneratedValue) -> ScheduleSnapshot:
        recipe = self.recipe
        workflow_value = positive_int(
            response_field(
                item,
                recipe.workflow_field,
                ds_version=self.ds_version,
                resource=SCHEDULE_RESOURCE,
            ),
            ds_version=self.ds_version,
            resource=SCHEDULE_RESOURCE,
            field=recipe.workflow_field,
        )
        environment_code = (
            optional_int_field(
                item,
                "environmentCode",
                ds_version=self.ds_version,
                resource=SCHEDULE_RESOURCE,
            )
            if recipe.has_environment
            else None
        )
        if environment_code in {-1, 0}:
            environment_code = None
        return ScheduleSnapshot(
            ds_version=self.ds_version,
            id=positive_int(
                response_field(
                    item,
                    "id",
                    ds_version=self.ds_version,
                    resource=SCHEDULE_RESOURCE,
                ),
                ds_version=self.ds_version,
                resource=SCHEDULE_RESOURCE,
                field="id",
            ),
            workflow_native=(
                NativeId(workflow_value)
                if recipe.identity_kind == "id"
                else NativeCode(workflow_value)
            ),
            workflow_name=optional_text_field(
                item,
                recipe.workflow_name_field,
                ds_version=self.ds_version,
                resource=SCHEDULE_RESOURCE,
            ),
            project_name=_optional_text(item, "projectName", self.ds_version),
            definition_description=_optional_text(
                item, "definitionDescription", self.ds_version
            ),
            start_time=_optional_text(item, "startTime", self.ds_version),
            end_time=_optional_text(item, "endTime", self.ds_version),
            timezone_id=(
                _optional_text(item, "timezoneId", self.ds_version)
                if recipe.has_timezone
                else None
            ),
            crontab=_optional_text(item, "crontab", self.ds_version),
            failure_strategy=_optional_enum(item, "failureStrategy", self.ds_version),
            warning_type=_optional_enum(item, "warningType", self.ds_version),
            create_time=_optional_text(item, "createTime", self.ds_version),
            update_time=_optional_text(item, "updateTime", self.ds_version),
            user_id=_non_negative_int(item, "userId", self.ds_version),
            user_name=_optional_text(item, "userName", self.ds_version),
            release_state=_optional_enum(item, "releaseState", self.ds_version),
            warning_group_id=_non_negative_int(item, "warningGroupId", self.ds_version),
            workflow_instance_priority=_optional_enum(
                item, recipe.priority_field, self.ds_version
            ),
            worker_group=_optional_text(item, "workerGroup", self.ds_version),
            tenant_code=(
                _optional_text(item, "tenantCode", self.ds_version)
                if recipe.has_tenant
                else None
            ),
            environment_code=environment_code,
            environment_name=(
                _optional_text(item, "environmentName", self.ds_version)
                if recipe.has_environment_name
                else None
            ),
            workflow_field=recipe.workflow_field,
            workflow_name_field=recipe.workflow_name_field,
            priority_field=recipe.priority_field,
            has_timezone=recipe.has_timezone,
            has_tenant=recipe.has_tenant,
            has_environment=recipe.has_environment,
            has_environment_name=recipe.has_environment_name,
            has_missed_fire_policy=recipe.has_missed_fire_policy,
            missed_fire_policy=(
                _optional_enum(item, "missedFirePolicy", self.ds_version)
                if recipe.has_missed_fire_policy
                else None
            ),
        )

    def _validate_state_support(
        self,
        state: ScheduleState,
        *,
        update: bool = False,
    ) -> None:
        recipe = self.recipe
        provided = state.provided_fields
        if recipe.has_timezone:
            if state.timezone_id is None:
                message = (
                    "Schedule timezone is required for this DolphinScheduler version"
                )
                raise UserInputError(
                    message,
                    details={"ds_version": self.ds_version, "field": "timezoneId"},
                    suggestion="Pass --timezone with an IANA timezone id.",
                )
        elif state.timezone_id is not None or "timezoneId" in provided:
            raise _unsupported_schedule_field(
                self.ds_version,
                "timezoneId",
                (
                    "DolphinScheduler 1.3 uses the server-local timezone and "
                    "has no timezone field."
                ),
            )
        if not recipe.has_tenant and (
            state.tenant_code is not None or "tenantCode" in provided
        ):
            raise _unsupported_schedule_field(
                self.ds_version,
                "tenantCode",
                "This DolphinScheduler schedule contract has no tenant field.",
            )
        if not recipe.has_environment and (
            state.environment_code is not None or "environmentCode" in provided
        ):
            raise _unsupported_schedule_field(
                self.ds_version,
                "environmentCode",
                "DolphinScheduler 1.3 has no schedule environment field.",
            )
        if not recipe.has_missed_fire_policy and (
            state.missed_fire_policy is not None or "missedFirePolicy" in provided
        ):
            raise _unsupported_schedule_field(
                self.ds_version,
                "missedFirePolicy",
                (
                    "This exact DolphinScheduler schedule contract has no "
                    "missed fire policy field."
                ),
            )
        if "missedFirePolicy" in provided and state.missed_fire_policy is None:
            msg = "Schedule missedFirePolicy cannot be null"
            raise UserInputError(
                msg,
                details={"field": "missedFirePolicy"},
            )
        if update and not provided:
            message = "Schedule update requires at least one field change"
            raise UserInputError(
                message,
                suggestion="Pass at least one schedule update flag.",
            )

    def _validate_state_values(self, state: ScheduleState) -> None:
        enum_fields = {
            "failureStrategy": (
                state.failure_strategy,
                frozenset({"CONTINUE", "END"}),
            ),
            "warningType": (
                state.warning_type,
                frozenset({"NONE", "SUCCESS", "FAILURE", "ALL"}),
            ),
            "workflowInstancePriority": (
                state.workflow_instance_priority,
                frozenset({"HIGHEST", "HIGH", "MEDIUM", "LOW", "LOWEST"}),
            ),
        }
        for field, (value, allowed) in enum_fields.items():
            if value not in allowed:
                message = f"Effective schedule field {field} is invalid"
                raise ConflictError(
                    message,
                    details={"field": field, "value": value},
                    suggestion=(
                        "Fix the enabled project preference or pass an explicit "
                        f"valid {field} value."
                    ),
                )
        if self.recipe.has_missed_fire_policy and state.missed_fire_policy is not None:
            choices = _missed_fire_choices(self.ds_version, self.recipe)
            if state.missed_fire_policy not in choices:
                msg = "Schedule missedFirePolicy is invalid"
                raise UserInputError(
                    msg,
                    details={
                        "field": "missedFirePolicy",
                        "value": state.missed_fire_policy,
                        "allowed": list(choices),
                    },
                    suggestion=(
                        "Choose an exact value from `dsctl schema --group schedule`."
                    ),
                )
        if state.warning_group_id < 0:
            message = "Effective schedule warningGroupId must be non-negative"
            raise ConflictError(message, details={"field": "warningGroupId"})
        if state.environment_code is not None and state.environment_code <= 0:
            message = "Effective schedule environmentCode must be positive or null"
            raise ConflictError(message, details={"field": "environmentCode"})
        text_fields = {
            "failureStrategy": state.failure_strategy,
            "warningType": state.warning_type,
            "workflowInstancePriority": state.workflow_instance_priority,
            "workerGroup": state.worker_group,
        }
        if self.recipe.has_tenant:
            text_fields["tenantCode"] = state.tenant_code or ""
        empty = sorted(
            field for field, value in text_fields.items() if not value.strip()
        )
        if empty:
            message = "Effective schedule defaults contain empty required fields"
            raise ConflictError(message, details={"fields": empty})

    def _apply_create_defaults(
        self,
        project: ProjectRef,
        state: ScheduleState,
    ) -> tuple[ScheduleState, str | None, tuple[str, ...]]:
        preferences = self._project_preference(project)
        used_fields: list[str] = []

        def preferred(field: str, fallback: JsonValue) -> JsonValue:
            if field in state.provided_fields or preferences is None:
                return fallback
            value = preferences.get(field)
            if value is None:
                return fallback
            used_fields.append(
                "workflowInstancePriority" if field == "taskPriority" else field
            )
            return value

        warning_type = _preference_text(
            preferred("warningType", state.warning_type),
            field="warningType",
        )
        warning_group_id = _preference_int(
            _preferred_alert_group(
                preferences,
                fallback=state.warning_group_id,
                provided="warningGroupId" in state.provided_fields,
                used_fields=used_fields,
            ),
            field="warningGroupId",
        )
        priority = _preference_text(
            preferred("taskPriority", state.workflow_instance_priority),
            field="taskPriority",
        )
        worker_group = _preference_text(
            preferred("workerGroup", state.worker_group),
            field="workerGroup",
        )
        environment_code = _preference_optional_int(
            preferred("environmentCode", state.environment_code),
            field="environmentCode",
        )

        tenant_source: str | None = None
        tenant_code = state.tenant_code
        if self.recipe.has_tenant:
            if "tenantCode" in state.provided_fields:
                tenant_source = "flag"
            elif preferences is not None and preferences.get("tenant") is not None:
                tenant_code = _preference_text(
                    preferences["tenant"],
                    field="tenant",
                )
                used_fields.append("tenantCode")
                tenant_source = "project_preference"
            else:
                current_tenant = self.current_user.current().tenantCode
                tenant_code = (
                    current_tenant.strip()
                    if isinstance(current_tenant, str) and current_tenant.strip()
                    else "default"
                )
                tenant_source = (
                    "current_user" if tenant_code != "default" else "default"
                )

        effective = replace(
            state,
            warning_type=warning_type,
            warning_group_id=warning_group_id,
            workflow_instance_priority=priority,
            worker_group=worker_group,
            tenant_code=tenant_code,
            environment_code=environment_code,
            missed_fire_policy=(
                self.recipe.missed_fire_policy_default
                if state.missed_fire_policy is None
                else state.missed_fire_policy
            ),
        )
        return effective, tenant_source, tuple(used_fields)

    def _project_preference(self, project: ProjectRef) -> JsonObject | None:
        if self.project_preferences is None:
            return None
        if not isinstance(project.native, NativeCode):
            message = "Project preference requires a code-native project"
            raise WireContractError(message)
        payload = self.project_preferences.get(project_code=project.native.value)
        if payload is None:
            return None
        fields_set = getattr(payload, "model_fields_set", None)
        if isinstance(fields_set, (set, frozenset)) and "state" not in fields_set:
            raise projection_error(
                ds_version=self.ds_version,
                resource="project_preference",
                field="state",
                reason="generated payload omitted a required stored-state field",
            )
        state = response_field(
            payload,
            "state",
            ds_version=self.ds_version,
            resource="project_preference",
        )
        if state != 1:
            return None
        text = optional_text_field(
            payload,
            "preferences",
            ds_version=self.ds_version,
            resource="project_preference",
        )
        if text is None:
            return None
        try:
            decoded = json.loads(text)
        except json.JSONDecodeError as error:
            message = "Stored project preference must be valid JSON"
            raise ConflictError(
                message,
                details={"projectCode": project.native.value},
                suggestion="Fix the remote project preference before retrying.",
            ) from error
        if not isinstance(decoded, dict) or not all(
            isinstance(key, str) for key in decoded
        ):
            message = "Stored project preference must be one JSON object"
            raise ConflictError(
                message,
                details={"projectCode": project.native.value},
            )
        return cast("JsonObject", decoded)

    def _validate_patch_support(self, patch: ScheduleUpdatePatch) -> None:
        requested = patch.requested_fields()
        recipe = self.recipe
        if "timezoneId" in requested and not recipe.has_timezone:
            raise _unsupported_schedule_field(
                self.ds_version,
                "timezoneId",
                (
                    "DolphinScheduler 1.3 uses the server-local timezone and "
                    "has no timezone field."
                ),
            )
        if "environmentCode" in requested and not recipe.has_environment:
            raise _unsupported_schedule_field(
                self.ds_version,
                "environmentCode",
                "DolphinScheduler 1.3 has no schedule environment field.",
            )
        if "missedFirePolicy" in requested:
            if not recipe.has_missed_fire_policy:
                raise _unsupported_schedule_field(
                    self.ds_version,
                    "missedFirePolicy",
                    (
                        "This exact DolphinScheduler schedule contract has no "
                        "missed fire policy field."
                    ),
                )
            if patch.missed_fire_policy is None:
                msg = "Schedule missedFirePolicy cannot be null"
                raise UserInputError(
                    msg,
                    details={"field": "missedFirePolicy"},
                )
        if (
            "missedFirePolicy" in requested
            and patch.missed_fire_policy
            not in _missed_fire_choices(self.ds_version, recipe)
        ):
            message = "Schedule missedFirePolicy is invalid"
            raise UserInputError(
                message,
                details={
                    "field": "missedFirePolicy",
                    "value": (
                        patch.missed_fire_policy
                        if isinstance(patch.missed_fire_policy, str)
                        else None
                    ),
                },
                suggestion=(
                    "Choose an exact value from `dsctl schema --group schedule`."
                ),
            )
        if not requested:
            message = "Schedule update requires at least one field change"
            raise UserInputError(
                message,
                suggestion="Pass at least one schedule update flag.",
            )

    def _validate_explicit_update_environment(
        self,
        patch: ScheduleUpdatePatch,
    ) -> None:
        """Preflight only an explicitly selected positive update environment.

        This is an action-specific compatibility dependency retained from the
        stable update/explain contract. Create intentionally relies on the
        schedule endpoint and does not add an environment lookup.
        """
        if "environmentCode" not in patch.requested_fields():
            return
        code = patch.environment_code
        if code is None:
            return
        if not isinstance(code, int) or isinstance(code, bool) or code <= 0:
            return
        if self.environments is None:
            message = "Environment preflight is unavailable for this schedule profile"
            raise WireContractError(message)
        self.environments.get(code=code)

    def _project_args(self, project: ProjectRef) -> JsonObject:
        field = "projectName" if self.recipe.legacy_routes else "projectCode"
        return {field: _project_route_arg(project, self.recipe)}

    def _require_prepared_version(self, ds_version: str) -> None:
        if ds_version != self.ds_version:
            message = "Prepared schedule mutation belongs to a different DS profile"
            raise WireContractError(message)


def _schedule_recipe(recipe_id: str | None) -> _ScheduleRecipe:
    if recipe_id is None or recipe_id not in _SCHEDULE_RECIPES:
        message = f"Compiled schedule recipe is unsupported: {recipe_id!r}"
        raise WireContractError(message)
    return _SCHEDULE_RECIPES[recipe_id]


def _missed_fire_choices(ds_version: str, recipe: _ScheduleRecipe) -> tuple[str, ...]:
    if not recipe.has_missed_fire_policy:
        return ()
    spec = get_enum_spec(ds_version, "schedule-missed-fire-policy")
    if spec is None:
        msg = "Exact schedule missed fire policy enum is missing"
        raise WireContractError(msg)
    return tuple(member.name for member in spec.members)


def _schedule_json(state: ScheduleState, recipe: _ScheduleRecipe) -> str:
    payload: dict[str, str] = {
        "startTime": state.start_time,
        "endTime": state.end_time,
        "crontab": state.crontab,
    }
    if recipe.has_timezone:
        if state.timezone_id is None:
            message = "Timezone-bearing schedule recipe received no timezone"
            raise WireContractError(message)
        payload["timezoneId"] = state.timezone_id
    if recipe.has_missed_fire_policy and state.missed_fire_policy is not None:
        payload["missedFirePolicy"] = state.missed_fire_policy
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False)


def _project_route_arg(project: ProjectRef, recipe: _ScheduleRecipe) -> int | str:
    if recipe.identity_kind == "id":
        if not isinstance(project.native, NativeId) or project.name is None:
            message = "Legacy schedule route requires a named id project"
            raise WireContractError(message)
        return project.name
    if not isinstance(project.native, NativeCode):
        message = "Code-native schedule route received a project id"
        raise WireContractError(message)
    return project.native.value


def _project_selector(project: ProjectRef) -> str:
    return project.name or str(project.native.value)


def _preference_text(value: JsonValue, *, field: str) -> str:
    if isinstance(value, str) and value.strip():
        return value.strip()
    message = f"Stored project preference field {field} must be non-empty text"
    raise ConflictError(
        message,
        details={"field": field},
        suggestion="Fix the enabled project preference before retrying.",
    )


def _preference_int(value: JsonValue, *, field: str) -> int:
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    message = f"Stored project preference field {field} must be non-negative"
    raise ConflictError(
        message,
        details={"field": field},
        suggestion="Fix the enabled project preference before retrying.",
    )


def _preference_optional_int(value: JsonValue, *, field: str) -> int | None:
    if value is None or value == 0:
        return None
    resolved = _preference_int(value, field=field)
    if resolved > 0:
        return resolved
    return None


def _preferred_alert_group(
    preferences: JsonObject | None,
    *,
    fallback: int,
    provided: bool,
    used_fields: list[str],
) -> JsonValue:
    if preferences is None or provided:
        return fallback
    plural = preferences.get("alertGroups")
    singular = preferences.get("alertGroup")
    if plural is not None and singular is not None and plural != singular:
        message = "Stored project preference alert-group fields disagree"
        raise ConflictError(
            message,
            details={"alertGroups": plural, "alertGroup": singular},
            suggestion="Keep one alert-group field or make both values match.",
        )
    selected = plural if plural is not None else singular
    if selected is None:
        return fallback
    used_fields.append("warningGroupId")
    return selected


def _merge_patch(current: ScheduleState, patch: ScheduleUpdatePatch) -> ScheduleState:
    return ScheduleState(
        crontab=_patch_text(patch.crontab, current.crontab, field="crontab"),
        start_time=_patch_text(
            patch.start_time,
            current.start_time,
            field="startTime",
        ),
        end_time=_patch_text(patch.end_time, current.end_time, field="endTime"),
        timezone_id=_patch_optional_text(
            patch.timezone_id,
            current.timezone_id,
            field="timezoneId",
        ),
        failure_strategy=_patch_text(
            patch.failure_strategy,
            current.failure_strategy,
            field="failureStrategy",
        ),
        warning_type=_patch_text(
            patch.warning_type,
            current.warning_type,
            field="warningType",
        ),
        warning_group_id=_patch_int(
            patch.warning_group_id,
            current.warning_group_id,
            field="warningGroupId",
        ),
        workflow_instance_priority=_patch_text(
            patch.workflow_instance_priority,
            current.workflow_instance_priority,
            field="workflowInstancePriority",
        ),
        worker_group=_patch_text(
            patch.worker_group,
            current.worker_group,
            field="workerGroup",
        ),
        tenant_code=current.tenant_code,
        environment_code=_patch_optional_int(
            patch.environment_code,
            current.environment_code,
            field="environmentCode",
        ),
        missed_fire_policy=_patch_optional_text(
            patch.missed_fire_policy,
            current.missed_fire_policy,
            field="missedFirePolicy",
        ),
        provided_fields=patch.requested_fields(),
    )


def _patch_text(value: str | _UnsetValue, fallback: str, *, field: str) -> str:
    resolved = fallback if value is _UNSET else value
    if isinstance(resolved, str):
        return resolved
    message = f"Schedule patch field {field} must be text"
    raise TypeError(message)


def _patch_optional_text(
    value: str | None | _UnsetValue,
    fallback: str | None,
    *,
    field: str,
) -> str | None:
    resolved = fallback if value is _UNSET else value
    if resolved is None or isinstance(resolved, str):
        return resolved
    message = f"Schedule patch field {field} must be text or null"
    raise TypeError(message)


def _patch_int(value: int | _UnsetValue, fallback: int, *, field: str) -> int:
    resolved = fallback if value is _UNSET else value
    if isinstance(resolved, int) and not isinstance(resolved, bool):
        return resolved
    message = f"Schedule patch field {field} must be an integer"
    raise TypeError(message)


def _patch_optional_int(
    value: int | None | _UnsetValue,
    fallback: int | None,
    *,
    field: str,
) -> int | None:
    resolved = fallback if value is _UNSET else value
    if resolved is None or (
        isinstance(resolved, int) and not isinstance(resolved, bool)
    ):
        return resolved
    message = f"Schedule patch field {field} must be an integer or null"
    raise TypeError(message)


def _schedule_fingerprint(
    schedule: ScheduleSnapshot,
) -> tuple[ScheduleFingerprintPart, ...]:
    return (
        schedule.id,
        type(schedule.workflow_native),
        schedule.workflow_native.value,
        schedule.start_time,
        schedule.end_time,
        schedule.timezone_id,
        schedule.crontab,
        schedule.failure_strategy,
        schedule.warning_type,
        schedule.warning_group_id,
        schedule.workflow_instance_priority,
        schedule.worker_group,
        schedule.tenant_code,
        schedule.environment_code,
        schedule.release_state,
        schedule.update_time,
        schedule.missed_fire_policy,
    )


def _require_state_match(
    schedule: ScheduleSnapshot,
    state: ScheduleState,
    *,
    ds_version: str,
) -> None:
    expected: JsonObject = {
        "crontab": state.crontab,
        "startTime": state.start_time,
        "endTime": state.end_time,
        "failureStrategy": state.failure_strategy,
        "warningType": state.warning_type,
        "warningGroupId": state.warning_group_id,
        schedule.priority_field: state.workflow_instance_priority,
        "workerGroup": state.worker_group,
    }
    actual: JsonObject = {
        "crontab": schedule.crontab,
        "startTime": schedule.start_time,
        "endTime": schedule.end_time,
        "failureStrategy": schedule.failure_strategy,
        "warningType": schedule.warning_type,
        "warningGroupId": schedule.warning_group_id,
        schedule.priority_field: schedule.workflow_instance_priority,
        "workerGroup": schedule.worker_group,
    }
    if schedule.has_timezone:
        expected["timezoneId"] = state.timezone_id
        actual["timezoneId"] = schedule.timezone_id
    if schedule.has_tenant:
        expected["tenantCode"] = state.tenant_code
        actual["tenantCode"] = schedule.tenant_code
    if schedule.has_environment:
        expected["environmentCode"] = state.environment_code
        actual["environmentCode"] = schedule.environment_code
    if schedule.has_missed_fire_policy:
        expected["missedFirePolicy"] = state.missed_fire_policy
        actual["missedFirePolicy"] = schedule.missed_fire_policy
    mismatches = {
        field: {"expected": expected[field], "actual": actual[field]}
        for field in expected
        if not _schedule_state_field_matches(
            field,
            expected[field],
            actual[field],
            ds_version=ds_version,
        )
    }
    if mismatches:
        message = "Schedule mutation readback did not match requested fields"
        suggestion = None
        if ds_version == "1.3.9" and {"startTime", "endTime"} & mismatches.keys():
            suggestion = (
                f"Use `dsctl schedule get {schedule.id}` for the exact readback. For a "
                "later DS 1.3.9 create or update, pass both `--start` and `--end` with "
                "explicit UTC offsets so they can be compared by instant."
            )
        raise ApiTransportError(
            message,
            details={"ds_version": ds_version, "mismatches": mismatches},
            suggestion=suggestion,
        )


def _schedule_state_field_matches(
    field: str,
    expected: JsonValue,
    actual: JsonValue,
    *,
    ds_version: str,
) -> bool:
    if expected == actual:
        return True
    if ds_version != "1.3.9" or field not in {"startTime", "endTime"}:
        return False
    expected_instant = _explicit_offset_instant(expected)
    actual_instant = _explicit_offset_instant(actual)
    return (
        expected_instant is not None
        and actual_instant is not None
        and expected_instant == actual_instant
    )


def _explicit_offset_instant(value: JsonValue) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return None
        return parsed.astimezone(UTC)
    except (OverflowError, ValueError):
        return None


def _require_entity_result(
    result: OpaqueGeneratedValue,
    *,
    expected: _EntityResult,
    operation: str,
    ds_version: str,
) -> None:
    if (expected == "none" and result is None) or (
        expected == "entity" and result is not None
    ):
        return
    raise projection_error(
        ds_version=ds_version,
        resource=SCHEDULE_RESOURCE,
        field=f"{operation}Result",
        reason="mutation result did not match the exact source contract",
    )


def _require_none_result(
    result: OpaqueGeneratedValue, *, operation: str, ds_version: str
) -> None:
    if result is None:
        return
    raise projection_error(
        ds_version=ds_version,
        resource=SCHEDULE_RESOURCE,
        field=f"{operation}Result",
        reason="void mutation returned a non-null payload",
    )


def _require_lifecycle_result(
    result: OpaqueGeneratedValue,
    *,
    expected: _LifecycleResult,
    operation: str,
    ds_version: str,
) -> None:
    if (expected == "none" and result is None) or (
        expected == "boolean" and result is True
    ):
        return
    raise projection_error(
        ds_version=ds_version,
        resource=SCHEDULE_RESOURCE,
        field=f"{operation}Result",
        reason="lifecycle result did not confirm success",
    )


def _optional_text(
    item: OpaqueGeneratedValue, field: str, ds_version: str
) -> str | None:
    return optional_text_field(
        item,
        field,
        ds_version=ds_version,
        resource=SCHEDULE_RESOURCE,
    )


def _optional_enum(
    item: OpaqueGeneratedValue, field: str, ds_version: str
) -> str | None:
    value = response_field(
        item,
        field,
        ds_version=ds_version,
        resource=SCHEDULE_RESOURCE,
    )
    if value is None:
        return None
    rendered = enum_value(cast("StringEnumValue", value))
    if rendered is None and isinstance(value, str):
        return value
    if isinstance(rendered, str):
        return rendered
    raise projection_error(
        ds_version=ds_version,
        resource=SCHEDULE_RESOURCE,
        field=field,
        reason="schedule enum field is not string-like",
    )


def _non_negative_int(item: OpaqueGeneratedValue, field: str, ds_version: str) -> int:
    value = response_field(
        item,
        field,
        ds_version=ds_version,
        resource=SCHEDULE_RESOURCE,
    )
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    raise projection_error(
        ds_version=ds_version,
        resource=SCHEDULE_RESOURCE,
        field=field,
        reason="schedule field is not a non-negative integer",
    )


def _unsupported_schedule_field(
    ds_version: str,
    field: str,
    reason: str,
) -> UserInputError:
    return UserInputError(
        f"Schedule field {field} is unavailable on DolphinScheduler {ds_version}",
        details={
            "ds_version": ds_version,
            "resource": SCHEDULE_RESOURCE,
            "field": field,
            "reason": "upstream_field_absent",
        },
        suggestion=reason,
    )


__all__ = [
    "SCHEDULE_DOMAIN",
    "LocatedSchedule",
    "PreparedScheduleCreate",
    "PreparedScheduleUpdate",
    "ScheduleAdapter",
    "ScheduleContractFeatures",
    "ScheduleDomain",
    "ScheduleListing",
    "ScheduleOperations",
    "SchedulePreview",
    "ScheduleSnapshot",
    "ScheduleState",
    "ScheduleUpdatePatch",
    "schedule_contract_features",
]
