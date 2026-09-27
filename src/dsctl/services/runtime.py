from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Concatenate,
    Generic,
    ParamSpec,
    Protocol,
    TypeVar,
    cast,
)

from dsctl.client import DolphinSchedulerClient
from dsctl.services._legacy_workflow_mutation import (
    prepare_legacy_workflow_mutation_plan,
)
from dsctl.services._whole_workflow_task_update import (
    CodeNativeWholeWorkflowTaskUpdate,
)
from dsctl.services._workflow.mutation import prepare_workflow_mutation_plan
from dsctl.services.selection import ResourceDefaults
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringCatalog,
    get_task_authoring_catalog,
)
from dsctl.services.version_resolution import (
    RuntimeSelection,
    resolve_runtime_selection,
)
from dsctl.upstream import (
    get_read_adapter,
    get_task_definition_adapter,
)
from dsctl.upstream.legacy_task_definitions import LegacyTaskDefinitions
from dsctl.upstream.task_definition_wire import task_update_contract_features
from dsctl.upstream.task_definitions import TaskDefinitions
from dsctl.upstream.task_update import compile_task_update
from dsctl.upstream.workflows import WORKFLOW_DOMAIN

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping

    from dsctl.config import ClusterProfile
    from dsctl.services._whole_workflow_task_update import WorkflowUpdateOperations
    from dsctl.upstream.bound_domain import BoundDomain
    from dsctl.upstream.protocol import (
        ReadUpstreamSession,
    )


P = ParamSpec("P")
ResultT = TypeVar("ResultT")
DomainT = TypeVar("DomainT")


class HealthcheckClient(Protocol):
    """Minimal client interface needed by service-level health commands."""

    def healthcheck(self) -> Mapping[str, object]:
        """Return the API server health payload."""


@dataclass(frozen=True)
class ReadServiceRuntime:
    """Narrow runtime for exact-version project and workflow read actions."""

    profile: ClusterProfile
    context: ResourceDefaults
    http_client: HealthcheckClient
    upstream: ReadUpstreamSession


@dataclass(frozen=True)
class TaskDefinitionServiceRuntime:
    """Narrow runtime for exact task discovery, detail, and mutation."""

    profile: ClusterProfile
    context: ResourceDefaults
    definitions: TaskDefinitions | LegacyTaskDefinitions[TaskAuthoringCatalog]


@dataclass(frozen=True)
class BoundDomainServiceRuntime(Generic[DomainT]):
    """Shared transport plus one caller-selected, exact-version domain."""

    profile: ClusterProfile
    context: ResourceDefaults
    http_client: HealthcheckClient
    domain: DomainT


@contextmanager
def open_bound_domain_service_runtime(
    domain: BoundDomain[DomainT],
    *,
    env_file: str | None = None,
    selection: RuntimeSelection | None = None,
) -> Iterator[BoundDomainServiceRuntime[DomainT]]:
    """Open one exact domain without widening the broad upstream session."""
    if selection is None:
        selection = resolve_runtime_selection(env_file)
    profile = selection.execution_profile
    resource_defaults = ResourceDefaults(project=selection.project)
    client = (
        DolphinSchedulerClient(profile, read_policy=selection.read_plan.policy)
        if selection.read_plan is not None
        else DolphinSchedulerClient(profile)
    )
    with client as http_client:
        yield BoundDomainServiceRuntime(
            profile=profile,
            context=resource_defaults,
            http_client=http_client,
            domain=domain.bind(profile, http_client=http_client),
        )


def run_with_bound_domain_service_runtime(
    env_file: str | None,
    domain: BoundDomain[DomainT],
    operation: Callable[
        Concatenate[BoundDomainServiceRuntime[DomainT], P],
        ResultT,
    ],
    /,
    *args: P.args,
    **kwargs: P.kwargs,
) -> ResultT:
    """Open one exact bound domain and invoke one service operation."""
    with open_bound_domain_service_runtime(domain, env_file=env_file) as runtime:
        return operation(runtime, *args, **kwargs)


def run_with_bound_domain_selection(
    selection: RuntimeSelection,
    domain: BoundDomain[DomainT],
    operation: Callable[
        Concatenate[BoundDomainServiceRuntime[DomainT], P],
        ResultT,
    ],
    /,
    *args: P.args,
    **kwargs: P.kwargs,
) -> ResultT:
    """Use the same resolved target for authoring and its subsequent operation."""
    with open_bound_domain_service_runtime(domain, selection=selection) as runtime:
        return operation(runtime, *args, **kwargs)


@contextmanager
def open_read_service_runtime(
    *,
    env_file: str | None = None,
) -> Iterator[ReadServiceRuntime]:
    """Open the narrow exact-version runtime used by supported read actions."""
    selection = resolve_runtime_selection(env_file)
    profile = selection.execution_profile
    resource_defaults = ResourceDefaults(project=selection.project)
    adapter = get_read_adapter(profile.ds_version)
    client = (
        DolphinSchedulerClient(profile, read_policy=selection.read_plan.policy)
        if selection.read_plan is not None
        else DolphinSchedulerClient(profile)
    )
    with client as http_client:
        yield ReadServiceRuntime(
            profile=profile,
            context=resource_defaults,
            http_client=http_client,
            upstream=adapter.bind_read(profile, http_client=http_client),
        )


def run_with_read_service_runtime(
    env_file: str | None,
    operation: Callable[Concatenate[ReadServiceRuntime, P], ResultT],
    /,
    *args: P.args,
    **kwargs: P.kwargs,
) -> ResultT:
    """Open one narrow read runtime and invoke one service operation."""
    with open_read_service_runtime(env_file=env_file) as runtime:
        return operation(runtime, *args, **kwargs)


@contextmanager
def open_task_definition_service_runtime(
    *,
    env_file: str | None = None,
) -> Iterator[TaskDefinitionServiceRuntime]:
    """Open the exact-profile deep task-definition runtime."""
    selection = resolve_runtime_selection(env_file)
    profile = selection.execution_profile
    resource_defaults = ResourceDefaults(project=selection.project)
    with DolphinSchedulerClient(profile) as http_client:
        if profile.ds_version == "1.3.9":
            workflow_domain = WORKFLOW_DOMAIN.bind(
                profile,
                http_client=http_client,
            )
            yield TaskDefinitionServiceRuntime(
                profile=profile,
                context=resource_defaults,
                definitions=LegacyTaskDefinitions(
                    profile_version=profile.ds_version,
                    operations=workflow_domain.workflows,
                    catalog=get_task_authoring_catalog(profile.ds_version),
                    compile_update=prepare_legacy_workflow_mutation_plan,
                ),
            )
            return
        adapter = get_task_definition_adapter(profile.ds_version)
        upstream = adapter.bind_task_definitions(
            profile,
            http_client=http_client,
        )
        whole_workflow_update = None
        if task_update_contract_features(profile.ds_version).whole_workflow_update:
            workflow_domain = WORKFLOW_DOMAIN.bind(
                profile,
                http_client=http_client,
            )
            whole_workflow_update = CodeNativeWholeWorkflowTaskUpdate(
                profile_version=profile.ds_version,
                operations=cast(
                    "WorkflowUpdateOperations",
                    workflow_domain.workflows,
                ),
                catalog=get_task_authoring_catalog(profile.ds_version),
                compile_update=prepare_workflow_mutation_plan,
                task_request_fields=(
                    upstream.task_definitions.top_level_field_policy.request_payload
                ),
            )
        yield TaskDefinitionServiceRuntime(
            profile=profile,
            context=resource_defaults,
            definitions=TaskDefinitions(
                profile_version=profile.ds_version,
                definitions=upstream.definitions,
                wire=upstream.task_definitions,
                compile_update=compile_task_update,
                whole_workflow_update=whole_workflow_update,
            ),
        )


def run_with_task_definition_service_runtime(
    env_file: str | None,
    operation: Callable[Concatenate[TaskDefinitionServiceRuntime, P], ResultT],
    /,
    *args: P.args,
    **kwargs: P.kwargs,
) -> ResultT:
    """Open one deep task runtime and invoke one service operation."""
    with open_task_definition_service_runtime(env_file=env_file) as runtime:
        return operation(runtime, *args, **kwargs)
