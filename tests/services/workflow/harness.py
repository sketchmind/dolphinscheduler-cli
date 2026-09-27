"""Install workflow collaborators around the public service interface."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from tests.fakes import (
    FakeDataSourceAdapter,
    FakeProjectAdapter,
    FakeProjectPreferenceAdapter,
    FakeResourceAdapter,
    FakeScheduleAdapter,
    FakeTaskAdapter,
    FakeUserAdapter,
    FakeWorkflowAdapter,
    fake_read_service_runtime,
)
from tests.support import make_profile
from tests.workflow_domain_fakes import (
    _FakeWorkflowOperations,
    install_workflow_domain_runtime,
)

from dsctl.services import runtime as runtime_service
from dsctl.services.selection import ResourceDefaults

if TYPE_CHECKING:
    import pytest

    from dsctl.config import ClusterProfile
    from dsctl.upstream.task_definition_wire import TaskDefinitionWire


@dataclass(frozen=True)
class _WorkflowServiceHarness:
    monkeypatch: pytest.MonkeyPatch
    project_adapter: FakeProjectAdapter
    workflow_adapter: FakeWorkflowAdapter
    task_adapter: FakeTaskAdapter
    schedule_adapter: FakeScheduleAdapter

    def install(
        self,
        *,
        workflow_adapter: FakeWorkflowAdapter | None = None,
        task_adapter: FakeTaskAdapter | None = None,
        schedule_adapter: FakeScheduleAdapter | None = None,
        user_adapter: FakeUserAdapter | None = None,
        context: ResourceDefaults | None = None,
        profile: ClusterProfile | None = None,
        project_preference_adapter: FakeProjectPreferenceAdapter | None = None,
        resource_adapter: FakeResourceAdapter | None = None,
        data_quality_authoring_inspector: object | None = None,
        datasource_adapter: FakeDataSourceAdapter | None = None,
        task_definition_wire: TaskDefinitionWire | None = None,
    ) -> _FakeWorkflowOperations:
        selected_workflow_adapter = (
            self.workflow_adapter if workflow_adapter is None else workflow_adapter
        )
        selected_task_adapter = (
            self.task_adapter if task_adapter is None else task_adapter
        )

        def read_runtime_factory(
            *,
            env_file: str | None = None,
            cwd: object = None,
        ) -> object:
            del env_file, cwd
            selected_profile = make_profile() if profile is None else profile
            return fake_read_service_runtime(
                self.project_adapter,
                profile=selected_profile,
                context=context or ResourceDefaults(),
                workflow_adapter=selected_workflow_adapter,
                schedule_adapter=schedule_adapter,
            )

        self.monkeypatch.setattr(
            runtime_service,
            "open_read_service_runtime",
            read_runtime_factory,
        )
        return install_workflow_domain_runtime(
            self.monkeypatch,
            project_adapter=self.project_adapter,
            workflow_adapter=selected_workflow_adapter,
            task_adapter=selected_task_adapter,
            schedule_adapter=schedule_adapter,
            user_adapter=user_adapter,
            context=context,
            profile=profile,
            project_preference_adapter=project_preference_adapter,
            resource_adapter=resource_adapter,
            data_quality_authoring_inspector=cast(
                "Any",
                data_quality_authoring_inspector,
            ),
            datasource_adapter=datasource_adapter,
            task_definition_wire=task_definition_wire,
        )
