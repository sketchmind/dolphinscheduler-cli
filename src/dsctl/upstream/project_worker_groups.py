from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, cast

from dsctl.errors import UnsupportedFeatureError
from dsctl.upstream.bound_domain import BoundDomain, BoundDomainAdapter
from dsctl.upstream.code_native_reads import CodeNativeReadAdapter
from dsctl.upstream.compiled_domain import (
    MUTATION_ONCE_REQUIRED,
    READ_RETRY_OPTIONAL,
    BoundCompiledPrograms,
    CompiledDomainPrograms,
)
from dsctl.upstream.mutation_outcomes import mutation_call, verify_mutation
from dsctl.upstream.response_projection import (
    optional_int_field,
    optional_text_field,
    positive_int,
    projection_error,
    require_none,
    response_field,
)
from dsctl.upstream.wire import WireContractError

if TYPE_CHECKING:
    from collections.abc import Sequence

    from dsctl.client import DolphinSchedulerClient
    from dsctl.config import ClusterProfile
    from dsctl.upstream._generated_types import OpaqueGeneratedValue
    from dsctl.upstream.definition_reads import DefinitionReads
    from dsctl.upstream.protocol import (
        ProjectWorkerGroupOperations,
        ProjectWorkerGroupRecord,
    )


_RESOURCE = "project-worker-group"
_INTRODUCED_IN = "3.2.2"
_Primitive = Literal["list", "assign"]
_PROJECT_WORKER_GROUP_PROGRAMS = CompiledDomainPrograms[_Primitive](
    name="project_worker_group",
    schema_constant="COMPILED_PROJECT_WORKER_GROUP_SCHEMA_VERSION",
    schema_version=1,
    expectations={
        "list": READ_RETRY_OPTIONAL,
        "assign": MUTATION_ONCE_REQUIRED,
    },
)


@dataclass(frozen=True)
class ProjectWorkerGroupDomain:
    """Project resolution plus exact project worker-group assignment."""

    definitions: DefinitionReads
    worker_groups: ProjectWorkerGroupOperations


@dataclass(frozen=True)
class ProjectWorkerGroupSnapshot:
    """Version-neutral assignment projection consumed by stable services."""

    id: int | None
    projectCode: int  # noqa: N815
    workerGroup: str  # noqa: N815
    createTime: str | None  # noqa: N815
    updateTime: str | None  # noqa: N815


class ProjectWorkerGroupAdapter:
    """Exact generated project worker-group adapter for supporting profiles."""

    def __init__(self, ds_version: str) -> None:
        """Load one exact compiled profile and its reviewed recipe."""
        self._profile = _PROJECT_WORKER_GROUP_PROGRAMS.profile(ds_version)
        if self._profile.recipe_id != "status_data":
            message = (
                "Compiled project-worker-group recipe is unsupported: "
                f"{self._profile.recipe_id!r}"
            )
            raise WireContractError(message)
        self.ds_version = ds_version

    @classmethod
    def for_version(cls, ds_version: str) -> ProjectWorkerGroupAdapter:
        """Return the exact adapter for one reviewed source version."""
        return cls(ds_version)

    def bind(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> ProjectWorkerGroupDomain:
        """Bind project selection and worker-group assignment operations."""
        read = CodeNativeReadAdapter.for_version(self.ds_version).bind_read(
            profile,
            http_client=http_client,
        )
        return ProjectWorkerGroupDomain(
            definitions=read.definitions,
            worker_groups=cast(
                "ProjectWorkerGroupOperations",
                _Operations(
                    _PROJECT_WORKER_GROUP_PROGRAMS.bind(
                        self._profile, profile, http_client=http_client
                    ),
                ),
            ),
        )


@dataclass(frozen=True)
class _AbsentAdapter:
    ds_version: str

    def bind(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> ProjectWorkerGroupDomain:
        del profile, http_client
        message = (
            "Project worker-group assignment does not exist in DolphinScheduler "
            f"{self.ds_version}"
        )
        raise UnsupportedFeatureError(
            message,
            details={
                "resource": _RESOURCE,
                "ds_version": self.ds_version,
                "reason": "upstream_capability_absent",
                "introduced_in": _INTRODUCED_IN,
            },
            suggestion=(
                "Use DolphinScheduler 3.2.2 or newer for project worker-group "
                "assignment."
            ),
        )


def _adapter_for_version(
    ds_version: str,
) -> BoundDomainAdapter[ProjectWorkerGroupDomain]:
    profile = _PROJECT_WORKER_GROUP_PROGRAMS.profile(ds_version)
    if profile.status == "upstream_absent":
        return _AbsentAdapter(ds_version)
    if profile.status == "supported":
        return ProjectWorkerGroupAdapter.for_version(ds_version)
    message = (
        f"DS {ds_version} has no reviewed project-worker-group capability decision"
    )
    raise WireContractError(message)


PROJECT_WORKER_GROUP_DOMAIN = BoundDomain[ProjectWorkerGroupDomain](
    name=_RESOURCE,
    adapter_for_version=_adapter_for_version,
)


@dataclass(frozen=True)
class _Operations:
    programs: BoundCompiledPrograms[_Primitive]

    def list(self, *, project_code: int) -> Sequence[ProjectWorkerGroupRecord]:
        payload = self.programs.call("list", {"projectCode": project_code})
        if not isinstance(payload, list):
            raise projection_error(
                ds_version=self.ds_version,
                resource=_RESOURCE,
                field="data",
                reason="generated response is not a list",
            )
        return cast(
            "Sequence[ProjectWorkerGroupRecord]",
            [
                _snapshot(
                    item,
                    ds_version=self.ds_version,
                    expected_project_code=project_code,
                )
                for item in payload
            ],
        )

    def set(
        self,
        *,
        project_code: int,
        worker_groups: Sequence[str],
    ) -> None:
        # The retained UI sends a single comma-joined form field.  A one-item
        # generated list preserves that exact encoding, including "" for clear.
        payload = mutation_call(
            lambda: self.programs.call(
                "assign",
                {
                    "projectCode": project_code,
                    "workerGroups": [",".join(worker_groups)],
                },
            ),
            ds_version=self.ds_version,
            resource=_RESOURCE,
            operation="set",
        )
        verify_mutation(
            lambda: require_none(
                payload,
                ds_version=self.ds_version,
                resource=_RESOURCE,
                field="assignmentResult",
            ),
            ds_version=self.ds_version,
            resource=_RESOURCE,
            operation="set",
            phase="mutation_response",
        )

    @property
    def ds_version(self) -> str:
        return self.programs.ds_version


def _snapshot(
    item: OpaqueGeneratedValue,
    *,
    ds_version: str,
    expected_project_code: int,
) -> ProjectWorkerGroupSnapshot:
    resource = _RESOURCE
    project_code = positive_int(
        response_field(
            item,
            "projectCode",
            ds_version=ds_version,
            resource=resource,
        ),
        ds_version=ds_version,
        resource=resource,
        field="projectCode",
    )
    worker_group_value = response_field(
        item,
        "workerGroup",
        ds_version=ds_version,
        resource=resource,
    )
    if not isinstance(worker_group_value, str) or not worker_group_value:
        raise projection_error(
            ds_version=ds_version,
            resource=resource,
            field="workerGroup",
            reason="worker-group name is missing",
        )
    if project_code != expected_project_code:
        raise projection_error(
            ds_version=ds_version,
            resource=resource,
            field="projectCode",
            reason="response escaped the requested project scope",
        )
    return ProjectWorkerGroupSnapshot(
        id=optional_int_field(item, "id", ds_version=ds_version, resource=resource),
        projectCode=project_code,
        workerGroup=worker_group_value,
        createTime=optional_text_field(
            item,
            "createTime",
            ds_version=ds_version,
            resource=resource,
        ),
        updateTime=optional_text_field(
            item,
            "updateTime",
            ds_version=ds_version,
            resource=resource,
        ),
    )


__all__ = [
    "PROJECT_WORKER_GROUP_DOMAIN",
    "ProjectWorkerGroupAdapter",
    "ProjectWorkerGroupDomain",
    "ProjectWorkerGroupSnapshot",
]
