from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from functools import cached_property
from typing import TYPE_CHECKING, Literal, cast

import httpx

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import ApiResultError, UnsupportedFeatureError
from dsctl.generated.workflow_profiles import WORKFLOW_PROFILE_FACTS
from dsctl.services import workflow_lineage as workflow_lineage_service
from dsctl.services._workflow.schedule import load_attached_schedule
from dsctl.services.runtime import BoundDomainServiceRuntime
from dsctl.services.selection import ResourceDefaults
from dsctl.services.version_resolution import RuntimeSelection, resolve_version
from dsctl.services.workflow import create, edit, execution, lifecycle, reads
from dsctl.upstream.definition_models import (
    NativeCode,
    NativeId,
    ProjectRef,
    WorkflowRef,
    WorkflowScope,
    WorkflowView,
)
from dsctl.upstream.execution_receipts import WorkflowExecutionReceipt
from dsctl.upstream.workflows import (
    LegacyWorkflowDefinitionSnapshot,
    WorkflowAdapter,
    WorkflowDomain,
    WorkflowOperations,
)
from tests.fakes import (
    FakeDataSourceAdapter,
    FakeHttpClient,
    FakeProject,
    FakeProjectAdapter,
    FakeProjectPreferenceAdapter,
    FakeResourceAdapter,
    FakeScheduleAdapter,
    FakeTaskAdapter,
    FakeUser,
    FakeUserAdapter,
    FakeWorkflow,
    FakeWorkflowAdapter,
    FakeWorkflowLineageAdapter,
)
from tests.schedule_domain_fakes import _FakeScheduleOperations
from tests.support import make_profile

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    import pytest

    from dsctl.config import ClusterProfile
    from dsctl.upstream.protocol import (
        CurrentUserOperations,
        DataQualityAuthoringInspector,
        DataSourceOperations,
        DependentLineageTaskRecord,
        ScheduleCreateRequestPlan,
        ScheduleCreateSpec,
        ScheduleRecord,
        TaskPayloadRecord,
        WorkflowDagRecord,
        WorkflowLineageRecord,
        WorkflowPayloadRecord,
    )
    from dsctl.upstream.protocols.governance import TaskResourceResolver
    from dsctl.upstream.task_definition_wire import TaskDefinitionWire
    from dsctl.upstream.wire import WireRequest


def install_workflow_domain_runtime(
    monkeypatch: pytest.MonkeyPatch,
    *,
    project_adapter: FakeProjectAdapter,
    workflow_adapter: FakeWorkflowAdapter,
    task_adapter: FakeTaskAdapter,
    schedule_adapter: FakeScheduleAdapter | None = None,
    user_adapter: FakeUserAdapter | None = None,
    context: ResourceDefaults | None = None,
    profile: ClusterProfile | None = None,
    project_preference_adapter: FakeProjectPreferenceAdapter | None = None,
    workflow_lineage_adapter: FakeWorkflowLineageAdapter | None = None,
    resource_adapter: FakeResourceAdapter | None = None,
    data_quality_authoring_inspector: DataQualityAuthoringInspector | None = None,
    datasource_adapter: FakeDataSourceAdapter | None = None,
    task_definition_wire: TaskDefinitionWire | None = None,
) -> _FakeWorkflowOperations:
    """Bind workflow service tests directly to the exact domain seam."""
    selected_profile = profile or make_profile()
    selected_schedule_adapter = schedule_adapter or FakeScheduleAdapter(schedules=[])
    workflow_adapter.schedule_adapter = selected_schedule_adapter
    schedules = _WorkflowScheduleOperations(
        project_adapter=project_adapter,
        workflow_adapter=workflow_adapter,
        schedule_adapter=selected_schedule_adapter,
        user_adapter=user_adapter,
        project_preference_adapter=project_preference_adapter,
        environment_adapter=None,
        ds_version=selected_profile.ds_version,
        explicit_adapter=schedule_adapter,
    )
    operations = _FakeWorkflowOperations(
        project_adapter=project_adapter,
        workflow_adapter=workflow_adapter,
        task_adapter=task_adapter,
        schedule_operations=schedules,
        project_preference_adapter=project_preference_adapter,
        workflow_lineage_adapter=workflow_lineage_adapter,
        ds_version=selected_profile.ds_version,
    )
    runtime = BoundDomainServiceRuntime(
        profile=selected_profile,
        context=context or ResourceDefaults(),
        http_client=FakeHttpClient(),
        domain=WorkflowDomain(
            workflows=cast("WorkflowOperations", operations),
            task_resource_resolver=cast(
                "TaskResourceResolver | None",
                resource_adapter,
            ),
            data_quality_authoring_inspector=data_quality_authoring_inspector,
            task_datasources=cast(
                "DataSourceOperations | None",
                datasource_adapter,
            ),
            task_definitions=task_definition_wire,
        ),
    )

    def run(
        env_file: str | None,
        domain: object,
        operation: Callable[..., object],
        /,
        *args: object,
        **kwargs: object,
    ) -> object:
        del env_file, domain
        return operation(runtime, *args, **kwargs)

    for service in (execution, lifecycle, reads):
        monkeypatch.setattr(service, "run_with_bound_domain_service_runtime", run)
    for service in (create, edit):
        monkeypatch.setattr(service, "run_with_bound_domain_selection", run)
        monkeypatch.setattr(
            service,
            "resolve_runtime_selection",
            lambda env_file=None: RuntimeSelection(
                selected_profile
                if env_file is None
                else make_profile(
                    ds_version=resolve_version(env_file, mode="local").version,
                ),
                project=runtime.context.project,
            ),
        )
    monkeypatch.setattr(
        workflow_lineage_service,
        "run_with_bound_domain_service_runtime",
        run,
    )
    return operations


@dataclass
class _FakePreparedWorkflowMutation:
    request: WireRequest
    apply: Callable[[], None]


@dataclass
class _FakePreparedWorkflowExecution:
    request: WireRequest
    apply: Callable[[], tuple[int, ...]]


@dataclass
class _FakeWorkflowOperations:
    project_adapter: FakeProjectAdapter
    workflow_adapter: FakeWorkflowAdapter
    task_adapter: FakeTaskAdapter
    schedule_operations: _WorkflowScheduleOperations
    project_preference_adapter: FakeProjectPreferenceAdapter | None
    workflow_lineage_adapter: FakeWorkflowLineageAdapter | None
    ds_version: str
    legacy_definitions: dict[int, tuple[str, str, str]] = field(default_factory=dict)
    prepared_executions: list[_FakePreparedWorkflowExecution] = field(
        default_factory=list
    )
    applied_executions: list[_FakePreparedWorkflowExecution] = field(
        default_factory=list
    )

    @cached_property
    def _wire_operations(self) -> WorkflowOperations:
        """Use the real exact request compiler; fake outcomes never use HTTP."""

        def reject_http(_request: httpx.Request) -> httpx.Response:
            message = "Workflow service fake must not dispatch HTTP"
            raise AssertionError(message)

        profile = make_profile(ds_version=self.ds_version)
        with DolphinSchedulerClient(
            profile, transport=httpx.MockTransport(reject_http)
        ) as client:
            operations = (
                WorkflowAdapter.for_version(self.ds_version)
                .bind(profile, http_client=client)
                .workflows
            )
        current_user = self.schedule_operations.user_adapter or FakeUserAdapter(
            users=[FakeUser(1, "admin", None, tenant_code_value="default")]
        )
        return replace(
            operations, current_user=cast("CurrentUserOperations", current_user)
        )

    @property
    def project_preferences(self) -> FakeProjectPreferenceAdapter | None:
        return self.project_preference_adapter

    @property
    def schedules(self) -> _WorkflowScheduleOperations:
        """Expose the composed schedule seam owned by the workflow domain."""
        return self.schedule_operations

    @property
    def workflow_graph_family(self) -> str:
        """Expose the selected exact graph dialect to service orchestration tests."""
        return "legacy-json" if self.ds_version == "1.3.9" else "code-native"

    @property
    def execution_schedule_time_shape(self) -> str:
        """Expose the selected executor scheduleTime consumer epoch."""
        return cast(
            "str",
            WORKFLOW_PROFILE_FACTS[self.ds_version]["execution_schedule_time_shape"],
        )

    def require_action(self, action: str) -> None:
        del action

    def normalize_expected_parallelism_number(self, value: int | None) -> int | None:
        if self.ds_version == "1.3.9":
            if value is None:
                return None
            message = (
                "--expected-parallelism-number is not available in "
                "DolphinScheduler 1.3.9."
            )
            raise UnsupportedFeatureError(message)
        return 2 if value is None else value

    def require_execution_options(
        self,
        *,
        action: str,
        tenant_code: str | None = None,
        environment_code: int | None = None,
        execution_dry_run: bool = False,
        run_mode: str | None = None,
        expected_parallelism_number: int | None = None,
        complement_dependent_mode: str | None = None,
        all_level_dependent: bool | None = None,
        execution_order: str | None = None,
    ) -> None:
        del run_mode
        if self.ds_version != "1.3.9":
            return
        options = (
            ("tenantCode", tenant_code is not None, "--tenant"),
            ("environmentCode", environment_code is not None, "--environment-code"),
            ("dryRun", execution_dry_run, "--execution-dry-run"),
            (
                "expectedParallelismNumber",
                expected_parallelism_number is not None,
                "--expected-parallelism-number",
            ),
            (
                "complementDependentMode",
                complement_dependent_mode is not None,
                "--complement-dependent-mode",
            ),
            ("allLevelDependent", bool(all_level_dependent), "--all-level-dependent"),
            ("executionOrder", execution_order is not None, "--execution-order"),
        )
        for parameter, explicit, flag in options:
            if not explicit:
                continue
            message = f"{flag} is not available in DolphinScheduler {self.ds_version}."
            raise UnsupportedFeatureError(
                message,
                details={
                    "resource": "workflow",
                    "action": action,
                    "parameter": parameter,
                    "ds_version": self.ds_version,
                    "reason": "upstream_capability_absent",
                },
                suggestion=f"Omit {flag} for this exact DS version.",
            )

    def normalize_start_params(
        self,
        value: str | None,
        *,
        action: str,
    ) -> str | None:
        if self.ds_version != "1.3.9" or value is None:
            return value
        message = (
            "--param is not available in DolphinScheduler 1.3.9 because its "
            "workflow executor has no startParams field."
        )
        raise UnsupportedFeatureError(
            message,
            details={
                "resource": "workflow",
                "action": action,
                "parameter": "startParams",
                "ds_version": self.ds_version,
                "reason": "upstream_capability_absent",
            },
            suggestion="Omit --param for this exact DS version.",
        )

    def resolve_project(self, selector: str) -> ProjectRef:
        return _project_ref(
            _select_project(self.project_adapter.projects, selector),
            legacy=self.ds_version == "1.3.9",
        )

    def resolve_project_by_name(self, project_name: str) -> ProjectRef:
        selected = next(
            (
                project
                for project in self.project_adapter.projects
                if project.name == project_name
            ),
            None,
        )
        if selected is None:
            raise ApiResultError(
                result_code=10018,
                result_message=f"project name {project_name} not found",
            )
        return _project_ref(
            selected,
            legacy=self.ds_version == "1.3.9",
        )

    def visible_project_refs(self) -> tuple[ProjectRef, ...]:
        return tuple(
            _project_ref(project, legacy=self.ds_version == "1.3.9")
            for project in self.project_adapter.projects
        )

    def resolve_workflow(
        self,
        project_selector: str,
        workflow_selector: str,
    ) -> WorkflowScope:
        project = self.resolve_project(project_selector)
        selected = _select_workflow(
            self.workflow_adapter.workflows,
            project.native.value,
            workflow_selector,
        )
        workflow = self.workflow_adapter.get(
            project_code=project.native.value,
            code=selected.code,
        )
        return _workflow_scope(
            project,
            workflow,
            legacy=self.ds_version == "1.3.9",
        )

    def resolve_workflow_by_name(
        self,
        project: ProjectRef,
        workflow_name: str,
    ) -> WorkflowScope:
        selected = next(
            (
                workflow
                for workflow in self.workflow_adapter.workflows
                if workflow.projectCode == project.native.value
                and workflow.name == workflow_name
            ),
            None,
        )
        if selected is None:
            raise ApiResultError(
                result_code=50003,
                result_message=f"workflow name {workflow_name} not found",
            )
        workflow = self.workflow_adapter.get(
            project_code=project.native.value,
            code=selected.code,
        )
        return _workflow_scope(
            project,
            workflow,
            legacy=self.ds_version == "1.3.9",
        )

    def find_workflow_ref_by_name(
        self,
        project: ProjectRef,
        workflow_name: str,
    ) -> WorkflowRef | None:
        selected = next(
            (
                workflow
                for workflow in self.workflow_adapter.workflows
                if workflow.projectCode == project.native.value
                and workflow.name == workflow_name
            ),
            None,
        )
        if selected is None:
            return None
        return _workflow_scope(
            project,
            selected,
            legacy=self.ds_version == "1.3.9",
        ).workflow

    def visible_workflow_refs(
        self,
        project: ProjectRef,
    ) -> tuple[WorkflowRef, ...]:
        return tuple(
            _workflow_scope(
                project,
                workflow,
                legacy=self.ds_version == "1.3.9",
            ).workflow
            for workflow in self.workflow_adapter.workflows
            if workflow.projectCode == project.native.value
        )

    def resolve_workflow_by_id(
        self,
        project: ProjectRef,
        workflow_id: int,
    ) -> WorkflowScope:
        workflow = self.workflow_adapter.get(
            project_code=project.native.value,
            code=workflow_id,
        )
        return _workflow_scope(
            project,
            workflow,
            legacy=self.ds_version == "1.3.9",
        )

    def resolve_workflow_by_code(
        self,
        project: ProjectRef,
        workflow_code: int,
    ) -> WorkflowScope:
        workflow = self.workflow_adapter.get(
            project_code=project.native.value,
            code=workflow_code,
        )
        return _workflow_scope(project, workflow, legacy=False)

    def detail(
        self,
        scope: WorkflowScope,
        *,
        action: str,
    ) -> WorkflowPayloadRecord:
        del action
        return cast(
            "WorkflowPayloadRecord",
            self.workflow_adapter.get(
                project_code=scope.project.native.value,
                code=scope.workflow.native.value,
            ),
        )

    def legacy_definition(
        self,
        scope: WorkflowScope,
        *,
        action: str,
    ) -> LegacyWorkflowDefinitionSnapshot:
        del action
        graph = self.legacy_definitions.get(scope.workflow.native.value)
        if graph is None:
            raise ApiResultError(
                result_code=50003,
                result_message=(
                    f"legacy workflow {scope.workflow.native.value} not found"
                ),
            )
        workflow = self.workflow_adapter.get(
            project_code=scope.project.native.value,
            code=scope.workflow.native.value,
        )
        return LegacyWorkflowDefinitionSnapshot(
            scope=scope,
            name=workflow.name,
            description=workflow.description,
            release_state=_enum(workflow.releaseState),
            process_definition_json=graph[0],
            locations=graph[1],
            connects=graph[2],
        )

    def dag(self, scope: WorkflowScope, *, action: str) -> WorkflowDagRecord:
        del action
        return cast(
            "WorkflowDagRecord",
            self.workflow_adapter.describe(
                project_code=scope.project.native.value,
                code=scope.workflow.native.value,
            ),
        )

    def resolve_task(
        self,
        scope: WorkflowScope,
        selector: str,
        *,
        action: str,
    ) -> TaskPayloadRecord:
        del action
        numeric = int(selector) if selector.isdigit() else None
        matches = [
            task
            for task in self.task_adapter.list(
                project_code=scope.project.native.value,
                workflow_code=scope.workflow.native.value,
            )
            if (numeric is not None and task.code == numeric) or task.name == selector
        ]
        if len(matches) != 1:
            return cast("TaskPayloadRecord", self.task_adapter.get(code=int(selector)))
        return cast("TaskPayloadRecord", matches[0])

    def allocate_task_codes(self, project: ProjectRef, count: int) -> tuple[int, ...]:
        return tuple(
            self.task_adapter.generate_codes(
                project_code=project.native.value,
                count=count,
            )
        )

    def create(
        self,
        project: ProjectRef,
        *,
        name: str,
        description: str | None,
        global_params: str,
        locations: str,
        timeout: int,
        task_relation_json: str,
        task_definition_json: str,
        execution_type: str | None,
    ) -> None:
        self.apply_create(
            self.prepare_create(
                project,
                name=name,
                description=description,
                global_params=global_params,
                locations=locations,
                timeout=timeout,
                task_relation_json=task_relation_json,
                task_definition_json=task_definition_json,
                execution_type=execution_type,
            )
        )

    def prepare_create(
        self,
        project: ProjectRef,
        *,
        name: str,
        description: str | None,
        global_params: str,
        locations: str,
        timeout: int,
        task_relation_json: str,
        task_definition_json: str,
        execution_type: str | None,
    ) -> _FakePreparedWorkflowMutation:
        return _FakePreparedWorkflowMutation(
            request=self._wire_operations.prepare_create(
                project,
                name=name,
                description=description,
                global_params=global_params,
                locations=locations,
                timeout=timeout,
                task_relation_json=task_relation_json,
                task_definition_json=task_definition_json,
                execution_type=execution_type,
            ).request,
            apply=lambda: self.workflow_adapter.create(
                project_code=project.native.value,
                name=name,
                description=description,
                global_params=global_params,
                locations=locations,
                timeout=timeout,
                task_relation_json=task_relation_json,
                task_definition_json=task_definition_json,
                execution_type=execution_type,
            ),
        )

    def prepare_legacy_create(
        self,
        project: ProjectRef,
        *,
        name: str,
        description: str | None,
        process_definition_json: str,
        locations: str,
        connects: str,
    ) -> _FakePreparedWorkflowMutation:
        return _FakePreparedWorkflowMutation(
            request=self._wire_operations.prepare_legacy_create(
                project,
                name=name,
                description=description,
                process_definition_json=process_definition_json,
                locations=locations,
                connects=connects,
            ).request,
            apply=lambda: self._create_legacy_workflow(
                project=project,
                name=name,
                description=description,
                process_definition_json=process_definition_json,
                locations=locations,
                connects=connects,
            ),
        )

    def _create_legacy_workflow(
        self,
        *,
        project: ProjectRef,
        name: str,
        description: str | None,
        process_definition_json: str,
        locations: str,
        connects: str,
    ) -> None:
        next_id = (
            max(
                (workflow.code for workflow in self.workflow_adapter.workflows),
                default=100,
            )
            + 1
        )
        process_data = json.loads(process_definition_json)
        global_params = json.dumps(
            process_data.get("globalParams", []),
            ensure_ascii=False,
            separators=(",", ":"),
        )
        self.workflow_adapter.workflows.append(
            FakeWorkflow(
                code=next_id,
                id=next_id,
                name=name,
                project_code_value=project.native.value,
                project_name_value=project.name,
                description=description,
                global_params_value=global_params,
                timeout=int(process_data.get("timeout", 0)),
                release_state_value=None,
            )
        )
        self.legacy_definitions[next_id] = (
            process_definition_json,
            locations,
            connects,
        )

    def apply_create(self, prepared: _FakePreparedWorkflowMutation) -> None:
        prepared.apply()

    def update(
        self,
        scope: WorkflowScope,
        *,
        name: str,
        description: str | None,
        global_params: str,
        locations: str,
        timeout: int,
        task_relation_json: str,
        task_definition_json: str,
        execution_type: str | None,
        release_state: str | None,
        tenant_code: str | None,
    ) -> None:
        self.apply_update(
            self.prepare_update(
                scope,
                name=name,
                description=description,
                global_params=global_params,
                locations=locations,
                timeout=timeout,
                task_relation_json=task_relation_json,
                task_definition_json=task_definition_json,
                execution_type=execution_type,
                release_state=release_state,
                tenant_code=tenant_code,
            )
        )

    def prepare_update(
        self,
        scope: WorkflowScope,
        *,
        name: str,
        description: str | None,
        global_params: str,
        locations: str,
        timeout: int,
        task_relation_json: str,
        task_definition_json: str,
        execution_type: str | None,
        release_state: str | None,
        tenant_code: str | None,
    ) -> _FakePreparedWorkflowMutation:
        return _FakePreparedWorkflowMutation(
            request=self._wire_operations.prepare_update(
                scope,
                name=name,
                description=description,
                global_params=global_params,
                locations=locations,
                timeout=timeout,
                task_relation_json=task_relation_json,
                task_definition_json=task_definition_json,
                execution_type=execution_type,
                release_state=release_state,
                tenant_code=tenant_code,
            ).request,
            apply=lambda: self.workflow_adapter.update(
                project_code=scope.project.native.value,
                workflow_code=scope.workflow.native.value,
                name=name,
                description=description,
                global_params=global_params,
                locations=locations,
                timeout=timeout,
                task_relation_json=task_relation_json,
                task_definition_json=task_definition_json,
                execution_type=execution_type,
                release_state=release_state,
            ),
        )

    def prepare_legacy_update(
        self,
        scope: WorkflowScope,
        *,
        name: str,
        description: str | None,
        process_definition_json: str,
        locations: str,
        connects: str,
    ) -> _FakePreparedWorkflowMutation:
        return _FakePreparedWorkflowMutation(
            request=self._wire_operations.prepare_legacy_update(
                scope,
                name=name,
                description=description,
                process_definition_json=process_definition_json,
                locations=locations,
                connects=connects,
            ).request,
            apply=lambda: self._update_legacy_workflow(
                scope=scope,
                name=name,
                description=description,
                process_definition_json=process_definition_json,
                locations=locations,
                connects=connects,
            ),
        )

    def _update_legacy_workflow(
        self,
        *,
        scope: WorkflowScope,
        name: str,
        description: str | None,
        process_definition_json: str,
        locations: str,
        connects: str,
    ) -> None:
        workflow = self.workflow_adapter.get(
            project_code=scope.project.native.value,
            code=scope.workflow.native.value,
        )
        process_data = json.loads(process_definition_json)
        global_params = json.dumps(
            process_data.get("globalParams", []),
            ensure_ascii=False,
            separators=(",", ":"),
        )
        updated = replace(
            workflow,
            name=name,
            version=(workflow.version or 1) + 1,
            description=description,
            global_params_value=global_params,
            timeout=int(process_data.get("timeout", 0)),
            update_time_value="2026-04-10 12:00:00",
        )
        for index, existing in enumerate(self.workflow_adapter.workflows):
            if existing.code == scope.workflow.native.value:
                self.workflow_adapter.workflows[index] = updated
                break
        self.legacy_definitions[scope.workflow.native.value] = (
            process_definition_json,
            locations,
            connects,
        )

    def apply_update(self, prepared: _FakePreparedWorkflowMutation) -> None:
        prepared.apply()

    def delete(self, scope: WorkflowScope) -> None:
        self.workflow_adapter.delete(
            project_code=scope.project.native.value,
            workflow_code=scope.workflow.native.value,
        )

    def release(self, scope: WorkflowScope, *, state: str) -> None:
        operation = (
            self.workflow_adapter.online
            if state == "ONLINE"
            else self.workflow_adapter.offline
        )
        operation(
            project_code=scope.project.native.value,
            workflow_code=scope.workflow.native.value,
        )

    def prepare_release(
        self,
        project: ProjectRef,
        *,
        workflow_code: int | str,
        state: Literal["ONLINE", "OFFLINE"],
    ) -> _FakePreparedWorkflowMutation:
        operation = (
            self.workflow_adapter.online
            if state == "ONLINE"
            else self.workflow_adapter.offline
        )
        return _FakePreparedWorkflowMutation(
            request=self._wire_operations.prepare_release(
                project, workflow_code=workflow_code, state=state
            ).request,
            apply=lambda: operation(
                project_code=project.native.value, workflow_code=int(workflow_code)
            ),
        )

    def apply_release(self, prepared: _FakePreparedWorkflowMutation) -> None:
        prepared.apply()

    def run(
        self,
        scope: WorkflowScope,
        *,
        schedule_time: str,
        command_type: Literal["START_PROCESS", "COMPLEMENT_DATA"],
        worker_group: str,
        tenant_code: str,
        start_node_list: Sequence[int] | Sequence[str] | None = None,
        task_scope: str | None = None,
        failure_strategy: str = "CONTINUE",
        warning_type: str = "NONE",
        workflow_instance_priority: str = "MEDIUM",
        warning_group_id: int | None = None,
        environment_code: int | None = None,
        start_params: str | None = None,
        execution_dry_run: bool = False,
        run_mode: str | None = None,
        expected_parallelism_number: int | None = None,
        complement_dependent_mode: str | None = None,
        all_level_dependent: bool | None = None,
        execution_order: str | None = None,
    ) -> WorkflowExecutionReceipt:
        return self.apply_execution(
            self.prepare_execution(
                scope,
                schedule_time=schedule_time,
                command_type=command_type,
                worker_group=worker_group,
                tenant_code=tenant_code,
                start_node_list=start_node_list,
                task_scope=task_scope,
                failure_strategy=failure_strategy,
                warning_type=warning_type,
                workflow_instance_priority=workflow_instance_priority,
                warning_group_id=warning_group_id,
                environment_code=environment_code,
                start_params=start_params,
                execution_dry_run=execution_dry_run,
                run_mode=run_mode,
                expected_parallelism_number=expected_parallelism_number,
                complement_dependent_mode=complement_dependent_mode,
                all_level_dependent=all_level_dependent,
                execution_order=execution_order,
            )
        )

    def prepare_execution(
        self,
        scope: WorkflowScope,
        *,
        schedule_time: str,
        command_type: Literal["START_PROCESS", "COMPLEMENT_DATA"],
        worker_group: str,
        tenant_code: str,
        start_node_list: Sequence[int] | Sequence[str] | None = None,
        task_scope: str | None = None,
        failure_strategy: str = "CONTINUE",
        warning_type: str = "NONE",
        workflow_instance_priority: str = "MEDIUM",
        warning_group_id: int | None = None,
        environment_code: int | None = None,
        start_params: str | None = None,
        execution_dry_run: bool = False,
        run_mode: str | None = None,
        expected_parallelism_number: int | None = None,
        complement_dependent_mode: str | None = None,
        all_level_dependent: bool | None = None,
        execution_order: str | None = None,
    ) -> _FakePreparedWorkflowExecution:
        request = self._wire_operations.prepare_execution(
            scope=scope,
            schedule_time=schedule_time,
            command_type=command_type,
            worker_group=worker_group,
            tenant_code=tenant_code,
            start_node_list=start_node_list,
            task_scope=task_scope,
            failure_strategy=failure_strategy,
            warning_type=warning_type,
            workflow_instance_priority=workflow_instance_priority,
            warning_group_id=warning_group_id,
            environment_code=environment_code,
            start_params=start_params,
            execution_dry_run=execution_dry_run,
            run_mode=run_mode,
            expected_parallelism_number=expected_parallelism_number,
            complement_dependent_mode=complement_dependent_mode,
            all_level_dependent=all_level_dependent,
            execution_order=execution_order,
        ).request

        def apply() -> tuple[int, ...]:
            if command_type != "COMPLEMENT_DATA":
                result = tuple(
                    self.workflow_adapter.run(
                        project_code=scope.project.native.value,
                        workflow_code=scope.workflow.native.value,
                        worker_group=worker_group,
                        tenant_code=tenant_code,
                        start_node_list=start_node_list,
                        task_scope=task_scope,
                        failure_strategy=failure_strategy,
                        warning_type=warning_type,
                        workflow_instance_priority=workflow_instance_priority,
                        warning_group_id=warning_group_id,
                        environment_code=environment_code,
                        start_params=start_params,
                        dry_run=execution_dry_run,
                    )
                )
                return () if self.workflow_graph_family == "legacy-json" else result
            result = tuple(
                self.workflow_adapter.backfill(
                    project_code=scope.project.native.value,
                    workflow_code=scope.workflow.native.value,
                    schedule_time=schedule_time,
                    run_mode=run_mode or "RUN_MODE_SERIAL",
                    expected_parallelism_number=(
                        2
                        if expected_parallelism_number is None
                        else expected_parallelism_number
                    ),
                    complement_dependent_mode=(complement_dependent_mode or "OFF_MODE"),
                    all_level_dependent=bool(all_level_dependent),
                    execution_order=execution_order or "DESC_ORDER",
                    worker_group=worker_group,
                    tenant_code=tenant_code,
                    start_node_list=start_node_list,
                    task_scope=task_scope,
                    failure_strategy=failure_strategy,
                    warning_type=warning_type,
                    workflow_instance_priority=workflow_instance_priority,
                    warning_group_id=warning_group_id,
                    environment_code=environment_code,
                    start_params=start_params,
                    dry_run=execution_dry_run,
                )
            )
            return () if self.workflow_graph_family == "legacy-json" else result

        prepared = _FakePreparedWorkflowExecution(request=request, apply=apply)
        self.prepared_executions.append(prepared)
        return prepared

    def apply_execution(
        self,
        prepared: _FakePreparedWorkflowExecution,
    ) -> WorkflowExecutionReceipt:
        self.applied_executions.append(prepared)
        ids = prepared.apply()
        return WorkflowExecutionReceipt(ids, "resolved" if ids else "unavailable")

    def lineage_list_resolved(
        self,
        project: ProjectRef,
    ) -> WorkflowLineageRecord:
        assert self.workflow_lineage_adapter is not None
        return cast(
            "WorkflowLineageRecord",
            self.workflow_lineage_adapter.list(project_code=project.native.value),
        )

    def lineage_get_resolved(
        self,
        scope: WorkflowScope,
    ) -> WorkflowLineageRecord:
        assert self.workflow_lineage_adapter is not None
        return cast(
            "WorkflowLineageRecord",
            self.workflow_lineage_adapter.get(
                project_code=scope.project.native.value,
                workflow_code=scope.workflow.native.value,
            ),
        )

    def dependent_tasks_resolved(
        self,
        scope: WorkflowScope,
        *,
        task_selector: str | None,
    ) -> tuple[TaskPayloadRecord | None, tuple[DependentLineageTaskRecord, ...]]:
        assert self.workflow_lineage_adapter is not None
        task = (
            None
            if task_selector is None
            else self.resolve_task(scope, task_selector, action="workflow.lineage")
        )
        dependent = self.workflow_lineage_adapter.query_dependent_tasks(
            project_code=scope.project.native.value,
            workflow_code=scope.workflow.native.value,
            task_code=None if task is None else task.code,
        )
        return task, tuple(cast("Sequence[DependentLineageTaskRecord]", dependent))


@dataclass
class _WorkflowScheduleOperations(_FakeScheduleOperations):
    explicit_adapter: FakeScheduleAdapter | None

    def plan_create(
        self,
        *,
        spec: ScheduleCreateSpec[int | str],
    ) -> ScheduleCreateRequestPlan:
        """Return the selected profile's pure schedule-create request plan."""
        if self.ds_version == "1.3.9":
            return {
                "method": "POST",
                "path": f"/projects/{spec.project_name}/schedule/create",
                "form": {
                    "processDefinitionId": spec.workflow_code,
                    "schedule": json.dumps(
                        {
                            "startTime": spec.start_time,
                            "endTime": spec.end_time,
                            "crontab": spec.crontab,
                        },
                        separators=(",", ":"),
                    ),
                    "warningGroupId": spec.warning_group_id,
                    "failureStrategy": spec.failure_strategy or "CONTINUE",
                    "warningType": spec.warning_type or "NONE",
                    "processInstancePriority": (
                        spec.workflow_instance_priority or "MEDIUM"
                    ),
                    "workerGroup": spec.worker_group or "default",
                },
            }
        return self.schedule_adapter.plan_create(spec=spec)

    def plan_release(
        self,
        project: ProjectRef,
        *,
        schedule_id: int | str,
        state: str,
    ) -> dict[str, object]:
        operation = "online" if state == "ONLINE" else "offline"
        if self.ds_version == "1.3.9":
            return {
                "method": "POST",
                "path": f"/projects/{project.name}/schedule/{operation}",
                "form": {"id": schedule_id},
            }
        return {
            "method": "POST",
            "path": (
                f"/projects/{project.native.value}/schedules/{schedule_id}/{operation}"
            ),
        }

    def attached(self, scope: WorkflowScope) -> ScheduleRecord | None:
        if self.explicit_adapter is None:
            workflow = _select_workflow(
                self.workflow_adapter.workflows,
                scope.project.native.value,
                str(scope.workflow.native.value),
            )
            return cast("ScheduleRecord | None", workflow.schedule)
        if self.ds_version == "1.3.9":
            page = self.explicit_adapter.list(
                project_code=scope.project.native.value,
                workflow_code=scope.workflow.native.value,
                search=None,
                page_no=1,
                page_size=2,
            )
            schedules = list(page.totalList or [])
            if len(schedules) > 1:
                message = "fake legacy workflow has multiple attached schedules"
                raise AssertionError(message)
            return cast(
                "ScheduleRecord | None",
                None if not schedules else schedules[0],
            )
        return load_attached_schedule(
            adapter=self.explicit_adapter,
            project_code=scope.project.native.value,
            workflow_code=scope.workflow.native.value,
            workflow_name=scope.workflow.name,
            action="workflow.read",
            phase="read",
        )


def _select_project(projects: Sequence[FakeProject], selector: str) -> FakeProject:
    numeric = int(selector) if selector.isdigit() else None
    matches = [
        project
        for project in projects
        if (numeric is not None and project.code == numeric) or project.name == selector
    ]
    if len(matches) != 1:
        raise ApiResultError(
            result_code=10018,
            result_message=f"project {selector} not found",
        )
    return matches[0]


def _select_workflow(
    workflows: Sequence[FakeWorkflow],
    project_code: int,
    selector: str,
) -> FakeWorkflow:
    numeric = int(selector) if selector.isdigit() else None
    matches = [
        workflow
        for workflow in workflows
        if workflow.projectCode == project_code
        and (
            (numeric is not None and workflow.code == numeric)
            or workflow.name == selector
        )
    ]
    if len(matches) != 1:
        raise ApiResultError(
            result_code=50003,
            result_message=f"workflow {selector} not found",
        )
    return matches[0]


def _project_ref(project: FakeProject, *, legacy: bool = False) -> ProjectRef:
    return ProjectRef(
        native=NativeId(project.code) if legacy else NativeCode(project.code),
        name=project.name,
        description=project.description,
    )


def _workflow_scope(
    project: ProjectRef,
    workflow: FakeWorkflow,
    *,
    legacy: bool = False,
) -> WorkflowScope:
    ref = WorkflowRef(
        native=NativeId(workflow.code) if legacy else NativeCode(workflow.code),
        name=workflow.name,
        version=workflow.version,
    )
    return WorkflowScope(
        project=project,
        workflow=ref,
        view=WorkflowView(
            ref=ref,
            project_native=project.native,
            id=workflow.code if legacy else workflow.id,
            description=workflow.description,
            global_params=workflow.globalParams,
            global_param_map=workflow.globalParamMap,
            create_time=workflow.createTime,
            update_time=workflow.updateTime,
            user_id=workflow.userId,
            user_name=workflow.userName,
            project_name=workflow.projectName or project.name,
            timeout=workflow.timeout,
            release_state=_enum(workflow.releaseState),
            execution_type=_enum(workflow.executionType),
            include_execution_type=workflow.executionType is not None,
        ),
    )


def _enum(value: object) -> str | None:
    member = getattr(value, "value", None)
    return member if isinstance(member, str) else None


__all__ = ["install_workflow_domain_runtime"]
