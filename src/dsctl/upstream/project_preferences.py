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
    from dsctl.client import DolphinSchedulerClient
    from dsctl.config import ClusterProfile
    from dsctl.upstream._generated_types import OpaqueGeneratedValue
    from dsctl.upstream.definition_reads import DefinitionReads
    from dsctl.upstream.protocol import (
        ProjectPreferenceOperations,
        ProjectPreferenceRecord,
    )


_RESOURCE = "project-preference"
_INTRODUCED_IN = "3.2.0"
_Primitive = Literal["get", "update", "set_state"]
_PROJECT_PREFERENCE_PROGRAMS = CompiledDomainPrograms[_Primitive](
    name="project_preference",
    schema_constant="COMPILED_PROJECT_PREFERENCE_SCHEMA_VERSION",
    schema_version=1,
    expectations={
        "get": READ_RETRY_OPTIONAL,
        "update": MUTATION_ONCE_REQUIRED,
        "set_state": MUTATION_ONCE_REQUIRED,
    },
)


@dataclass(frozen=True)
class ProjectPreferenceDomain:
    """Project resolution plus one exact project-preference lifecycle."""

    definitions: DefinitionReads
    preferences: ProjectPreferenceOperations


@dataclass(frozen=True)
class ProjectPreferenceSnapshot:
    """Version-neutral preference projection consumed by stable services."""

    id: int | None
    code: int
    projectCode: int  # noqa: N815
    preferences: str | None
    userId: int | None  # noqa: N815
    state: int
    createTime: str | None  # noqa: N815
    updateTime: str | None  # noqa: N815


class ProjectPreferenceAdapter:
    """Compiled project-preference adapter for supporting profiles."""

    def __init__(self, ds_version: str) -> None:
        """Load one exact compiled profile and its reviewed recipe."""
        self._profile = _PROJECT_PREFERENCE_PROGRAMS.profile(ds_version)
        if self._profile.recipe_id != "singleton":
            message = (
                "Compiled project-preference recipe is unsupported: "
                f"{self._profile.recipe_id!r}"
            )
            raise WireContractError(message)
        self.ds_version = ds_version

    @classmethod
    def for_version(cls, ds_version: str) -> ProjectPreferenceAdapter:
        """Return the exact adapter for one reviewed source version."""
        return cls(ds_version)

    def bind_read(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> ProjectPreferenceWireRead:
        """Bind native singleton reads without CRUD identity projection."""
        return ProjectPreferenceWireRead(
            _PROJECT_PREFERENCE_PROGRAMS.bind(
                self._profile, profile, http_client=http_client
            )
        )

    def bind(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> ProjectPreferenceDomain:
        """Bind project selection and preference operations."""
        read = CodeNativeReadAdapter.for_version(self.ds_version).bind_read(
            profile,
            http_client=http_client,
        )
        return ProjectPreferenceDomain(
            definitions=read.definitions,
            preferences=cast(
                "ProjectPreferenceOperations",
                _Operations(
                    _PROJECT_PREFERENCE_PROGRAMS.bind(
                        self._profile, profile, http_client=http_client
                    ),
                ),
            ),
        )


@dataclass(frozen=True)
class ProjectPreferenceWireRead:
    """Shared native read for adapters that own their own field projection."""

    programs: BoundCompiledPrograms[_Primitive]

    def get(self, *, project_code: int) -> OpaqueGeneratedValue:
        """Validate the exact response without imposing CRUD identity checks."""
        return self.programs.call("get", {"projectCode": project_code})


@dataclass(frozen=True)
class _AbsentAdapter:
    ds_version: str

    def bind(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> ProjectPreferenceDomain:
        del profile, http_client
        message = (
            f"Project preferences do not exist in DolphinScheduler {self.ds_version}"
        )
        raise UnsupportedFeatureError(
            message,
            details={
                "resource": _RESOURCE,
                "ds_version": self.ds_version,
                "reason": "upstream_capability_absent",
                "introduced_in": _INTRODUCED_IN,
            },
            suggestion=("Use DolphinScheduler 3.2.0 or newer for project preferences."),
        )


def _adapter_for_version(
    ds_version: str,
) -> BoundDomainAdapter[ProjectPreferenceDomain]:
    profile = _PROJECT_PREFERENCE_PROGRAMS.profile(ds_version)
    if profile.status == "upstream_absent":
        return _AbsentAdapter(ds_version)
    if profile.status == "supported":
        return ProjectPreferenceAdapter.for_version(ds_version)
    message = f"DS {ds_version} has no reviewed project-preference capability decision"
    raise WireContractError(message)


PROJECT_PREFERENCE_DOMAIN = BoundDomain[ProjectPreferenceDomain](
    name=_RESOURCE,
    adapter_for_version=_adapter_for_version,
)


@dataclass(frozen=True)
class _Operations:
    programs: BoundCompiledPrograms[_Primitive]

    def get(self, *, project_code: int) -> ProjectPreferenceRecord | None:
        payload = self.programs.call("get", {"projectCode": project_code})
        if payload is None:
            return None
        return cast(
            "ProjectPreferenceRecord",
            _snapshot(
                payload,
                ds_version=self.ds_version,
                expected_project_code=project_code,
            ),
        )

    def update(
        self,
        *,
        project_code: int,
        preferences: str,
    ) -> ProjectPreferenceRecord:
        payload = mutation_call(
            lambda: self.programs.call(
                "update",
                {"projectCode": project_code, "projectPreferences": preferences},
            ),
            ds_version=self.ds_version,
            resource=_RESOURCE,
            operation="update",
        )
        return cast(
            "ProjectPreferenceRecord",
            verify_mutation(
                lambda: _snapshot(
                    payload,
                    ds_version=self.ds_version,
                    expected_project_code=project_code,
                    expected_preferences=preferences,
                ),
                ds_version=self.ds_version,
                resource=_RESOURCE,
                operation="update",
                phase="mutation_response",
            ),
        )

    def set_state(self, *, project_code: int, state: int) -> None:
        payload = mutation_call(
            lambda: self.programs.call(
                "set_state", {"projectCode": project_code, "state": state}
            ),
            ds_version=self.ds_version,
            resource=_RESOURCE,
            operation="set-state",
        )
        verify_mutation(
            lambda: require_none(
                payload,
                ds_version=self.ds_version,
                resource=_RESOURCE,
                field="stateResult",
            ),
            ds_version=self.ds_version,
            resource=_RESOURCE,
            operation="set-state",
            phase="mutation_response",
        )

    @property
    def ds_version(self) -> str:
        return self.programs.ds_version


def _snapshot(
    item: OpaqueGeneratedValue,
    *,
    ds_version: str,
    expected_project_code: int | None = None,
    expected_preferences: str | None = None,
) -> ProjectPreferenceSnapshot:
    resource = _RESOURCE
    code = positive_int(
        response_field(item, "code", ds_version=ds_version, resource=resource),
        ds_version=ds_version,
        resource=resource,
        field="code",
    )
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
    preferences = optional_text_field(
        item,
        "preferences",
        ds_version=ds_version,
        resource=resource,
    )
    state_value = response_field(
        item,
        "state",
        ds_version=ds_version,
        resource=resource,
    )
    if not isinstance(state_value, int) or isinstance(state_value, bool):
        raise projection_error(
            ds_version=ds_version,
            resource=resource,
            field="state",
            reason="preference state is not an integer",
        )
    if expected_project_code is not None and project_code != expected_project_code:
        raise projection_error(
            ds_version=ds_version,
            resource=resource,
            field="projectCode",
            reason="response escaped the requested project scope",
        )
    if expected_preferences is not None and preferences != expected_preferences:
        raise projection_error(
            ds_version=ds_version,
            resource=resource,
            field="preferences",
            reason="response content does not match the requested mutation",
        )
    return ProjectPreferenceSnapshot(
        id=optional_int_field(item, "id", ds_version=ds_version, resource=resource),
        code=code,
        projectCode=project_code,
        preferences=preferences,
        userId=optional_int_field(
            item,
            "userId",
            ds_version=ds_version,
            resource=resource,
        ),
        state=state_value,
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
    "PROJECT_PREFERENCE_DOMAIN",
    "ProjectPreferenceAdapter",
    "ProjectPreferenceDomain",
    "ProjectPreferenceSnapshot",
    "ProjectPreferenceWireRead",
]
