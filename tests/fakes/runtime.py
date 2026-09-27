"""In-memory runtime collaborators with explicit test-owned state."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING

from dsctl.services.runtime import (
    BoundDomainServiceRuntime,
    ReadServiceRuntime,
    TaskDefinitionServiceRuntime,
)
from dsctl.services.selection import ResourceDefaults
from dsctl.upstream.definition_reads import DefinitionReads
from dsctl.upstream.definition_wire import CodeDefinitionWire
from dsctl.upstream.task_definitions import TaskDefinitions
from dsctl.upstream.task_update import compile_task_update
from tests.fakes.common import (
    FakeHttpClient,
)
from tests.fakes.schedules import (
    FakeScheduleAdapter,
    empty_schedule_adapter,
    schedule_adapter_from_workflows,
)
from tests.fakes.task_definitions import (
    FakeTaskAdapter,
    FakeTaskDefinitionWire,
)
from tests.fakes.workflows import (
    FakeWorkflowAdapter,
    empty_workflow_adapter,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from dsctl.config import ClusterProfile
    from tests.fakes.projects import (
        FakeProjectAdapter,
    )


def fake_project_definitions(project_adapter: FakeProjectAdapter) -> DefinitionReads:
    """Build the canonical code-native project selector over in-memory fakes."""
    return DefinitionReads(
        CodeDefinitionWire(
            projects=project_adapter,
            workflows=empty_workflow_adapter(),
            schedules=empty_schedule_adapter(),
        )
    )


@dataclass(frozen=True)
class FakeReadSession:
    """Minimal exact-version read session used by service and command tests."""

    definitions: DefinitionReads


@contextmanager
def fake_read_service_runtime(
    project_adapter: FakeProjectAdapter,
    *,
    profile: ClusterProfile,
    workflow_adapter: FakeWorkflowAdapter | None = None,
    schedule_adapter: FakeScheduleAdapter | None = None,
    context: ResourceDefaults | None = None,
    http_client: FakeHttpClient | None = None,
) -> Iterator[ReadServiceRuntime]:
    """Yield the narrow definition-read runtime over in-memory wire fakes."""
    bound_workflows = workflow_adapter or empty_workflow_adapter()
    bound_schedules = (
        schedule_adapter
        if schedule_adapter is not None
        else schedule_adapter_from_workflows(bound_workflows)
    )
    bound_workflows.schedule_adapter = bound_schedules
    yield ReadServiceRuntime(
        profile=profile,
        context=context or ResourceDefaults(),
        http_client=http_client or FakeHttpClient(),
        upstream=FakeReadSession(
            definitions=DefinitionReads(
                CodeDefinitionWire(
                    projects=project_adapter,
                    workflows=bound_workflows,
                    schedules=bound_schedules,
                )
            ),
        ),
    )


@contextmanager
def fake_bound_domain_service_runtime(
    domain: object,
    *,
    profile: ClusterProfile,
    context: ResourceDefaults | None = None,
    http_client: FakeHttpClient | None = None,
) -> Iterator[BoundDomainServiceRuntime[object]]:
    """Yield one caller-owned bound domain over in-memory runtime context."""
    yield BoundDomainServiceRuntime(
        profile=profile,
        context=context or ResourceDefaults(),
        http_client=http_client or FakeHttpClient(),
        domain=domain,
    )


@contextmanager
def fake_task_definition_service_runtime(
    project_adapter: FakeProjectAdapter,
    *,
    profile: ClusterProfile,
    workflow_adapter: FakeWorkflowAdapter,
    task_adapter: FakeTaskAdapter,
    context: ResourceDefaults | None = None,
) -> Iterator[TaskDefinitionServiceRuntime]:
    """Yield the deep task module over in-memory selector and wire fakes."""
    yield TaskDefinitionServiceRuntime(
        profile=profile,
        context=context or ResourceDefaults(),
        definitions=TaskDefinitions(
            profile_version=profile.ds_version,
            definitions=DefinitionReads(
                CodeDefinitionWire(
                    projects=project_adapter,
                    workflows=workflow_adapter,
                    schedules=(
                        workflow_adapter.schedule_adapter or FakeScheduleAdapter([])
                    ),
                )
            ),
            wire=FakeTaskDefinitionWire(
                task_adapter,
                ds_version=profile.ds_version,
                workflow_adapter=workflow_adapter,
            ),
            compile_update=compile_task_update,
        ),
    )
