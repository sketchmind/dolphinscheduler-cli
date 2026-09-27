from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Protocol, cast

from dsctl.errors import ApiHttpError, ApiResultError, ApiTransportError, DsctlError
from dsctl.upstream._compiled_project import PROJECT_PROGRAMS, ProjectPrimitive
from dsctl.upstream.bound_domain import BoundDomain, BoundDomainAdapter
from dsctl.upstream.code_native_reads import CodeNativeReadAdapter
from dsctl.upstream.definition_models import (
    NativeCode,
    NativeId,
    NativeIdentity,
    ProjectRef,
    ProjectView,
)
from dsctl.upstream.id_native_reads import IdNativeReadAdapter
from dsctl.upstream.mutation_outcomes import mutation_call, verify_mutation
from dsctl.upstream.project_reads import (
    CompiledProjectReads,
)
from dsctl.upstream.response_projection import (
    positive_int,
    projection_error,
    require_none,
)
from dsctl.upstream.wire import WireContractError

if TYPE_CHECKING:
    from collections.abc import Callable

    from dsctl.client import DolphinSchedulerClient
    from dsctl.config import ClusterProfile
    from dsctl.support.json_types import JsonObject
    from dsctl.upstream._generated_types import OpaqueGeneratedValue
    from dsctl.upstream.compiled_domain import BoundCompiledPrograms
    from dsctl.upstream.definition_reads import DefinitionReads
    from dsctl.upstream.protocol import (
        ProjectOperations,
        ProjectPageRecord,
        ProjectPayloadRecord,
    )


class NativeProjectMutations(Protocol):
    """Project mutations addressed by the exact profile's native identity."""

    def create(self, *, name: str, description: str | None) -> ProjectView:
        """Create a project and return its canonical view."""
        ...

    def update(
        self,
        *,
        native: NativeIdentity,
        name: str,
        description: str | None,
    ) -> ProjectView:
        """Update one project addressed by its native identity."""
        ...

    def delete(self, *, native: NativeIdentity) -> bool:
        """Delete one project addressed by its native identity."""
        ...


@dataclass(frozen=True)
class ProjectDomain:
    """Native project reads and mutations behind one exact-version seam."""

    definitions: DefinitionReads
    mutations: NativeProjectMutations


_CreateResult = Literal["project", "id"]
_UpdateResult = Literal["project", "none"]


@dataclass(frozen=True)
class _ProjectRecipe:
    create_result: _CreateResult
    update_result: _UpdateResult
    update_requires_owner: bool
    create_request_budget: int
    update_request_budget: int


_MODERN_RECIPE = _ProjectRecipe(
    create_result="project",
    update_result="project",
    update_requires_owner=False,
    create_request_budget=1,
    update_request_budget=1,
)

_ID_CREATE_VOID_UPDATE_RECIPE = _ProjectRecipe(
    create_result="id",
    update_result="none",
    update_requires_owner=True,
    create_request_budget=3,
    update_request_budget=4,
)

_PROJECT_CREATE_VOID_UPDATE_RECIPE = _ProjectRecipe(
    create_result="project",
    update_result="none",
    update_requires_owner=True,
    create_request_budget=1,
    update_request_budget=4,
)

_OWNER_PRESERVING_PROJECT_RECIPE = _ProjectRecipe(
    create_result="project",
    update_result="project",
    update_requires_owner=True,
    create_request_budget=1,
    update_request_budget=3,
)

_OWNER_LOOKUP_PAGE_SIZE = 100
_OWNER_LOOKUP_MAX_PAGES = 100

_PROJECT_RECIPES = {
    "id_create_void_update": _ID_CREATE_VOID_UPDATE_RECIPE,
    "project_create_void_update": _PROJECT_CREATE_VOID_UPDATE_RECIPE,
    "owner_preserving_project": _OWNER_PRESERVING_PROJECT_RECIPE,
    "modern": _MODERN_RECIPE,
}


@dataclass(frozen=True)
class _ProjectOwnerLookup:
    name: str
    pages_read: int


class ProjectAdapter:
    """Compiled project lifecycle adapter for code-identity profiles."""

    def __init__(self, ds_version: str) -> None:
        """Select one source-compiled profile and its lifecycle recipe."""
        self._profile = PROJECT_PROGRAMS.profile(ds_version)
        recipe_id = self._profile.recipe_id
        if recipe_id is None or recipe_id not in _PROJECT_RECIPES:
            message = f"DS {ds_version} has no reviewed code-native project recipe"
            raise WireContractError(message)
        self._recipe = _PROJECT_RECIPES[recipe_id]
        self.ds_version = ds_version

    @classmethod
    def for_version(cls, ds_version: str) -> ProjectAdapter:
        """Return the project adapter for one explicitly reviewed version."""
        return cls(ds_version)

    def bind_projects(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> ProjectOperations:
        """Bind caller-oriented project lifecycle operations."""
        return cast(
            "ProjectOperations",
            _ProjectOperations(
                PROJECT_PROGRAMS.bind(self._profile, profile, http_client=http_client),
                self._recipe,
                record_projector=_project_record,
                error_factory=_project_read_error,
            ),
        )


@dataclass(frozen=True)
class _ProjectOperations(CompiledProjectReads):
    recipe: _ProjectRecipe

    def create(
        self,
        *,
        name: str,
        description: str | None = None,
    ) -> ProjectPayloadRecord:
        payload = self._mutation_call(
            "create",
            self.recipe.create_request_budget,
            lambda: self.programs.call(
                "create", {"projectName": name, "description": description}
            ),
        )
        if self.recipe.create_result == "id":
            return self._read_created_project(
                payload,
                name=name,
                description=description,
            )
        try:
            return _project_record(
                payload,
                ds_version=self.programs.ds_version,
                expected_code=None,
                expected_id=None,
                expected_name=name,
                expected_description=description,
            )
        except ApiTransportError as exc:
            raise _mutation_verification_error(
                exc,
                ds_version=self.programs.ds_version,
                operation="create",
                phase="mutation_response",
                request_budget=self.recipe.create_request_budget,
            ) from exc

    def update(
        self,
        *,
        code: int,
        name: str,
        description: str | None = None,
    ) -> ProjectPayloadRecord:
        request_budget = self.recipe.update_request_budget
        values: JsonObject = {
            "code": code,
            "projectName": name,
            "description": description,
        }
        if self.recipe.update_requires_owner:
            current = self.get(code=code)
            try:
                owner_lookup = self._current_owner_name(
                    current,
                    code=code,
                )
            except ApiTransportError as exc:
                if not _is_mutation_response_error(exc):
                    raise
                request_budget += _owner_lookup_extra_pages(exc)
                raise _mutation_precondition_error(
                    exc,
                    ds_version=self.programs.ds_version,
                    operation="update",
                    request_budget=request_budget,
                    code=code,
                ) from exc
            values["userName"] = owner_lookup.name
            request_budget += owner_lookup.pages_read - 1
        payload = self._mutation_call(
            "update",
            request_budget,
            lambda: self.programs.call("update", values),
        )
        if self.recipe.update_result == "none":
            try:
                return _project_record(
                    self.programs.call("get", {"code": code}),
                    ds_version=self.programs.ds_version,
                    expected_code=code,
                    expected_id=None,
                    expected_name=name,
                    expected_description=description,
                )
            except (DsctlError, TypeError, WireContractError) as exc:
                raise _mutation_verification_error(
                    exc,
                    ds_version=self.programs.ds_version,
                    operation="update",
                    phase="readback",
                    request_budget=request_budget,
                    code=code,
                ) from exc
        try:
            return _project_record(
                payload,
                ds_version=self.programs.ds_version,
                expected_code=code,
                expected_id=None,
                expected_name=name,
                expected_description=description,
            )
        except ApiTransportError as exc:
            raise _mutation_verification_error(
                exc,
                ds_version=self.programs.ds_version,
                operation="update",
                phase="mutation_response",
                request_budget=request_budget,
                code=code,
            ) from exc

    def delete(self, *, code: int) -> bool:
        self._mutation_call(
            "delete",
            1,
            lambda: self.programs.call("delete", {"code": code}),
        )
        return True

    def _current_owner_name(
        self,
        current: ProjectPayloadRecord,
        *,
        code: int,
    ) -> _ProjectOwnerLookup:
        ds_version = self.programs.ds_version
        # DS 2.0.0 through 3.2.0 require userName on update, but their
        # query-by-code mapper omits the joined owner name.  The exact paging
        # route is the reviewed owner-bearing read for those profiles.
        current_name = _non_empty_text(
            _response_field(current, "name", ds_version=ds_version),
            ds_version=ds_version,
            field="name",
        )
        current_id = _positive_int(
            _response_field(current, "id", ds_version=ds_version),
            ds_version=ds_version,
            field="id",
        )
        current_user_id = _positive_int(
            _response_field(current, "userId", ds_version=ds_version),
            ds_version=ds_version,
            field="userId",
        )
        matches: list[ProjectPayloadRecord] = []
        expected_total_pages: int | None = None
        pages_read = 0
        for page_no in range(1, _OWNER_LOOKUP_MAX_PAGES + 1):
            pages_read = page_no
            try:
                page = self.list(
                    page_no=page_no,
                    page_size=_OWNER_LOOKUP_PAGE_SIZE,
                    search=current_name,
                )
                total_pages = _positive_int(
                    page.totalPage,
                    ds_version=ds_version,
                    field="totalPage",
                )
                if total_pages > _OWNER_LOOKUP_MAX_PAGES:
                    raise _projection_error(
                        ds_version=ds_version,
                        field="totalPage",
                        reason="project owner search exceeded its page safety limit",
                    )
                if expected_total_pages is None:
                    expected_total_pages = total_pages
                elif total_pages != expected_total_pages:
                    raise _projection_error(
                        ds_version=ds_version,
                        field="totalPage",
                        reason="project owner search page count changed during lookup",
                    )
                matches.extend(
                    _owner_lookup_page_matches(
                        page,
                        ds_version=ds_version,
                        code=code,
                        name=current_name,
                        project_id=current_id,
                        user_id=current_user_id,
                    )
                )
            except ApiTransportError as exc:
                if not _is_mutation_response_error(exc):
                    raise
                raise _owner_lookup_response_error(
                    exc,
                    pages_read=pages_read,
                ) from exc
            if page_no >= total_pages:
                break

        try:
            if len(matches) != 1:
                raise _projection_error(
                    ds_version=ds_version,
                    field="userName",
                    reason="project owner pages did not contain one exact identity",
                )
            owner_name = _non_empty_text(
                _response_field(matches[0], "userName", ds_version=ds_version),
                ds_version=ds_version,
                field="userName",
            )
        except ApiTransportError as exc:
            raise _owner_lookup_response_error(
                exc,
                pages_read=pages_read,
            ) from exc
        return _ProjectOwnerLookup(name=owner_name, pages_read=pages_read)

    def _mutation_call(
        self,
        operation: str,
        request_budget: int,
        call: Callable[[], OpaqueGeneratedValue],
    ) -> OpaqueGeneratedValue:
        try:
            return call()
        except ApiTransportError as exc:
            if _is_mutation_response_error(exc):
                raise _mutation_verification_error(
                    exc,
                    ds_version=self.programs.ds_version,
                    operation=operation,
                    phase="mutation_response",
                    request_budget=request_budget,
                ) from exc
            raise _mutation_dispatch_error(
                exc,
                ds_version=self.programs.ds_version,
                operation=operation,
                request_budget=request_budget,
            ) from exc
        except ApiHttpError as exc:
            raise _mutation_dispatch_error(
                exc,
                ds_version=self.programs.ds_version,
                operation=operation,
                request_budget=request_budget,
            ) from exc
        except ApiResultError as exc:
            if exc.result_code is not None:
                raise
            raise _mutation_dispatch_error(
                exc,
                ds_version=self.programs.ds_version,
                operation=operation,
                request_budget=request_budget,
            ) from exc

    def _read_created_project(
        self,
        payload: OpaqueGeneratedValue,
        *,
        name: str,
        description: str | None,
    ) -> ProjectPayloadRecord:
        try:
            created_id = _positive_int(
                payload,
                ds_version=self.programs.ds_version,
                field="id",
            )
        except ApiTransportError as exc:
            raise _mutation_verification_error(
                exc,
                ds_version=self.programs.ds_version,
                operation="create",
                phase="mutation_response",
                request_budget=self.recipe.create_request_budget,
            ) from exc

        try:
            candidates = self.programs.call("created_and_authed", {})
            created_code = _locate_created_project(
                candidates,
                created_id=created_id,
                name=name,
                ds_version=self.programs.ds_version,
            )
        except (DsctlError, TypeError, WireContractError) as exc:
            raise _mutation_verification_error(
                exc,
                ds_version=self.programs.ds_version,
                operation="create",
                phase="locate",
                request_budget=self.recipe.create_request_budget,
            ) from exc

        try:
            return _project_record(
                self.programs.call("get", {"code": created_code}),
                ds_version=self.programs.ds_version,
                expected_code=created_code,
                expected_id=created_id,
                expected_name=name,
                expected_description=description,
            )
        except (DsctlError, TypeError, WireContractError) as exc:
            raise _mutation_verification_error(
                exc,
                ds_version=self.programs.ds_version,
                operation="create",
                phase="readback",
                request_budget=self.recipe.create_request_budget,
                code=created_code,
            ) from exc


class _DescriptionUnset:
    """Sentinel distinguishing an omitted expected description from null."""


_DESCRIPTION_UNSET = _DescriptionUnset()


def _project_record(
    item: OpaqueGeneratedValue,
    ds_version: str,
    expected_code: int | None,
    *,
    expected_name: str | None = None,
    expected_id: int | None = None,
    expected_description: str | None | _DescriptionUnset = _DESCRIPTION_UNSET,
) -> ProjectPayloadRecord:
    code = _positive_int(
        _response_field(item, "code", ds_version=ds_version),
        ds_version=ds_version,
        field="code",
    )
    name = _non_empty_text(
        _response_field(item, "name", ds_version=ds_version),
        ds_version=ds_version,
        field="name",
    )
    if expected_code is not None and code != expected_code:
        raise _projection_error(
            ds_version=ds_version,
            field="code",
            reason="response identity does not match the requested project",
        )
    if expected_id is not None:
        project_id = _positive_int(
            _response_field(item, "id", ds_version=ds_version),
            ds_version=ds_version,
            field="id",
        )
        if project_id != expected_id:
            raise _projection_error(
                ds_version=ds_version,
                field="id",
                reason="response id does not match the created project",
            )
    if expected_name is not None and name != expected_name:
        raise _projection_error(
            ds_version=ds_version,
            field="name",
            reason="response name does not match the requested project mutation",
        )
    if expected_description is not _DESCRIPTION_UNSET:
        description = _response_field(
            item,
            "description",
            ds_version=ds_version,
        )
        if description != expected_description:
            raise _projection_error(
                ds_version=ds_version,
                field="description",
                reason="response description does not match the requested mutation",
            )
    return cast("ProjectPayloadRecord", item)


def _locate_created_project(
    candidates: OpaqueGeneratedValue,
    *,
    created_id: int,
    name: str,
    ds_version: str,
) -> int:
    if not isinstance(candidates, list):
        raise _projection_error(
            ds_version=ds_version,
            field="created-and-authed",
            reason="created project candidates are not a list",
        )
    identities = [
        (
            _positive_int(
                _response_field(item, "id", ds_version=ds_version),
                ds_version=ds_version,
                field="id",
            ),
            _positive_int(
                _response_field(item, "code", ds_version=ds_version),
                ds_version=ds_version,
                field="code",
            ),
            _non_empty_text(
                _response_field(item, "name", ds_version=ds_version),
                ds_version=ds_version,
                field="name",
            ),
        )
        for item in candidates
    ]
    exact = [item for item in identities if item[0] == created_id and item[2] == name]
    id_matches = [item for item in identities if item[0] == created_id]
    name_matches = [item for item in identities if item[2] == name]
    if len(exact) == 1 and len(id_matches) == 1 and len(name_matches) == 1:
        return exact[0][1]
    if len(exact) > 1 or len(id_matches) > 1 or len(name_matches) > 1:
        reason = "multiple_matches"
    elif id_matches or name_matches:
        reason = "identity_mismatch"
    else:
        reason = "not_found"
    message = "Created project could not be located by its exact id and name"
    raise ApiTransportError(
        message,
        details={
            "ds_version": ds_version,
            "resource": "project",
            "field": "created-and-authed",
            "reason": reason,
            "created_id": created_id,
            "project_name": name,
            "candidate_count": len(candidates),
        },
        source={
            "kind": "remote",
            "system": "dolphinscheduler",
            "layer": "response",
        },
    )


def _owner_lookup_page_matches(
    page: ProjectPageRecord,
    *,
    ds_version: str,
    code: int,
    name: str,
    project_id: int,
    user_id: int,
) -> list[ProjectPayloadRecord]:
    matches: list[ProjectPayloadRecord] = []
    for candidate in page.totalList or ():
        candidate_code = _positive_int(
            _response_field(candidate, "code", ds_version=ds_version),
            ds_version=ds_version,
            field="code",
        )
        candidate_name = _non_empty_text(
            _response_field(candidate, "name", ds_version=ds_version),
            ds_version=ds_version,
            field="name",
        )
        candidate_id = _positive_int(
            _response_field(candidate, "id", ds_version=ds_version),
            ds_version=ds_version,
            field="id",
        )
        candidate_user_id = _positive_int(
            _response_field(candidate, "userId", ds_version=ds_version),
            ds_version=ds_version,
            field="userId",
        )
        if (
            candidate_code == code
            and candidate_name == name
            and candidate_id == project_id
            and candidate_user_id == user_id
        ):
            matches.append(candidate)
    return matches


def _project_read_error(
    *,
    ds_version: str,
    resource: str,
    field: str,
    reason: str,
) -> ApiTransportError:
    del resource
    if reason == "generated payload is missing a canonical field":
        reason = "generated project payload is missing a canonical field"
    elif reason == "generated page items are not a list":
        reason = "generated project page items are not a list"
    return _projection_error(
        ds_version=ds_version,
        field=field,
        reason=reason,
    )


def _response_field(
    item: OpaqueGeneratedValue, name: str, *, ds_version: str
) -> OpaqueGeneratedValue:
    try:
        return getattr(item, name)
    except AttributeError as exc:
        raise _projection_error(
            ds_version=ds_version,
            field=name,
            reason="generated project payload is missing a canonical field",
        ) from exc


def _positive_int(
    value: OpaqueGeneratedValue,
    *,
    ds_version: str,
    field: str,
) -> int:
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return value
    raise _projection_error(
        ds_version=ds_version,
        field=field,
        reason="project identity must be a positive integer",
    )


def _non_empty_text(
    value: OpaqueGeneratedValue,
    *,
    ds_version: str,
    field: str,
) -> str:
    if isinstance(value, str) and value:
        return value
    raise _projection_error(
        ds_version=ds_version,
        field=field,
        reason="project identity must be a non-empty string",
    )


def _projection_error(
    *,
    ds_version: str,
    field: str,
    reason: str,
) -> ApiTransportError:
    return ApiTransportError(
        "DolphinScheduler response cannot be projected to the project contract.",
        details={
            "ds_version": ds_version,
            "resource": "project",
            "field": field,
            "reason": reason,
        },
        source={
            "kind": "remote",
            "system": "dolphinscheduler",
            "layer": "response",
        },
        suggestion="Verify DS_VERSION matches the server and inspect API health.",
    )


def _mutation_dispatch_error(
    cause: DsctlError,
    *,
    ds_version: str,
    operation: str,
    request_budget: int,
) -> ApiTransportError:
    details = dict(cause.details)
    details.update(
        {
            "ds_version": ds_version,
            "resource": "project",
            "operation": operation,
            "phase": "mutation_request",
            "mutation_may_have_applied": True,
            "request_budget": request_budget,
        }
    )
    return ApiTransportError(
        (
            f"Project {operation} transport failed after dispatch; the mutation "
            "may have been applied"
        ),
        details=details,
        source=getattr(cause, "source", None),
        suggestion=_reconciliation_suggestion(operation),
    )


def _mutation_verification_error(
    cause: BaseException,
    *,
    ds_version: str,
    operation: str,
    phase: str,
    request_budget: int,
    code: int | None = None,
) -> ApiTransportError:
    details = _cause_details(cause)
    details.update(
        {
            "ds_version": ds_version,
            "resource": "project",
            "operation": operation,
            "phase": phase,
            "mutation_applied": True,
            "request_budget": request_budget,
        }
    )
    if code is not None:
        details["code"] = code
    return ApiTransportError(
        f"Project {operation} succeeded, but its response could not be verified",
        details=details,
        source={
            "kind": "remote",
            "system": "dolphinscheduler",
            "layer": "response",
        },
        suggestion=_reconciliation_suggestion(operation),
    )


def _mutation_precondition_error(
    cause: BaseException,
    *,
    ds_version: str,
    operation: str,
    request_budget: int,
    code: int,
) -> ApiTransportError:
    details = _cause_details(cause)
    details.update(
        {
            "ds_version": ds_version,
            "resource": "project",
            "operation": operation,
            "phase": "precondition",
            "mutation_applied": False,
            "request_budget": request_budget,
            "code": code,
        }
    )
    message = "Project update requires a non-empty current owner name"
    return ApiTransportError(
        message,
        details=details,
        source={
            "kind": "remote",
            "system": "dolphinscheduler",
            "layer": "response",
        },
        suggestion=(
            "Inspect the project owner and repair the DolphinScheduler project "
            "record before retrying the update."
        ),
    )


def _is_mutation_response_error(error: ApiTransportError) -> bool:
    source = error.source
    return source is not None and source.get("layer") == "response"


def _owner_lookup_response_error(
    cause: ApiTransportError,
    *,
    pages_read: int,
) -> ApiTransportError:
    details = dict(cause.details)
    details["owner_lookup_pages"] = pages_read
    return ApiTransportError(
        cause.message,
        details=details,
        source=cause.source,
        suggestion=cause.suggestion,
    )


def _owner_lookup_extra_pages(error: ApiTransportError) -> int:
    pages_read = error.details.get("owner_lookup_pages")
    if isinstance(pages_read, int) and not isinstance(pages_read, bool):
        return max(pages_read - 1, 0)
    return 0


def _cause_details(cause: BaseException) -> JsonObject:
    if isinstance(cause, DsctlError):
        return dict(cause.details)
    return {}


def _reconciliation_suggestion(operation: str) -> str:
    return (
        f"Inspect the project before deciding whether to retry project {operation}; "
        "do not blindly repeat the mutation."
    )


@dataclass(frozen=True)
class _CodeProjectDomainAdapter:
    ds_version: str

    def bind(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> ProjectDomain:
        read = CodeNativeReadAdapter.for_version(self.ds_version).bind_read(
            profile,
            http_client=http_client,
        )
        operations = ProjectAdapter.for_version(self.ds_version).bind_projects(
            profile, http_client=http_client
        )
        return ProjectDomain(
            definitions=read.definitions,
            mutations=_CodeProjectMutations(operations),
        )


@dataclass(frozen=True)
class _CodeProjectMutations:
    operations: ProjectOperations

    def create(self, *, name: str, description: str | None) -> ProjectView:
        return _code_project_view(
            self.operations.create(name=name, description=description)
        )

    def update(
        self,
        *,
        native: NativeIdentity,
        name: str,
        description: str | None,
    ) -> ProjectView:
        code = _require_native_code(native)
        return _code_project_view(
            self.operations.update(
                code=code,
                name=name,
                description=description,
            )
        )

    def delete(self, *, native: NativeIdentity) -> bool:
        return self.operations.delete(code=_require_native_code(native))


class _LegacyProjectDomainAdapter:
    ds_version = "1.3.9"

    def bind(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> ProjectDomain:
        definitions = (
            IdNativeReadAdapter()
            .bind_read(
                profile,
                http_client=http_client,
            )
            .definitions
        )
        return ProjectDomain(
            definitions=definitions,
            mutations=_LegacyProjectMutations(
                programs=PROJECT_PROGRAMS.bind(
                    PROJECT_PROGRAMS.profile(self.ds_version),
                    profile,
                    http_client=http_client,
                ),
                definitions=definitions,
            ),
        )


@dataclass(frozen=True)
class _LegacyProjectMutations:
    programs: BoundCompiledPrograms[ProjectPrimitive]
    definitions: DefinitionReads

    def create(self, *, name: str, description: str | None) -> ProjectView:
        payload = mutation_call(
            lambda: self.programs.call(
                "create",
                {"projectName": name, "description": description},
            ),
            ds_version="1.3.9",
            resource="project",
            operation="create",
        )
        project_id = verify_mutation(
            lambda: positive_int(
                payload,
                ds_version="1.3.9",
                resource="project",
                field="createResult",
            ),
            ds_version="1.3.9",
            resource="project",
            operation="create",
            phase="mutation_response",
        )
        return verify_mutation(
            lambda: self._verified_readback(
                project_id,
                name=name,
                description=description,
            ),
            ds_version="1.3.9",
            resource="project",
            operation="create",
        )

    def update(
        self,
        *,
        native: NativeIdentity,
        name: str,
        description: str | None,
    ) -> ProjectView:
        project_id = _require_native_id(native)
        payload = mutation_call(
            lambda: self.programs.call(
                "update",
                {
                    "projectId": project_id,
                    "projectName": name,
                    "description": description,
                },
            ),
            ds_version="1.3.9",
            resource="project",
            operation="update",
        )
        verify_mutation(
            lambda: require_none(
                payload,
                ds_version="1.3.9",
                resource="project",
                field="updateResult",
            ),
            ds_version="1.3.9",
            resource="project",
            operation="update",
            phase="mutation_response",
        )
        return verify_mutation(
            lambda: self._verified_readback(
                project_id,
                name=name,
                description=description,
            ),
            ds_version="1.3.9",
            resource="project",
            operation="update",
        )

    def delete(self, *, native: NativeIdentity) -> bool:
        project_id = _require_native_id(native)
        payload = mutation_call(
            lambda: self.programs.call(
                "delete_legacy",
                {"projectId": project_id},
            ),
            ds_version="1.3.9",
            resource="project",
            operation="delete",
        )
        verify_mutation(
            lambda: require_none(
                payload,
                ds_version="1.3.9",
                resource="project",
                field="deleteResult",
            ),
            ds_version="1.3.9",
            resource="project",
            operation="delete",
            phase="mutation_response",
        )
        return True

    def _verified_readback(
        self,
        project_id: int,
        *,
        name: str,
        description: str | None,
    ) -> ProjectView:
        project = self.definitions.get_project(str(project_id)).view
        if project.ref.name != name or project.ref.description != description:
            raise projection_error(
                ds_version="1.3.9",
                resource="project",
                field="mutationReadback",
                reason="readback fields do not match the requested mutation",
            )
        return project


def _project_domain_adapter(ds_version: str) -> BoundDomainAdapter[ProjectDomain]:
    recipe = PROJECT_PROGRAMS.profile(ds_version).recipe_id
    if recipe == "legacy_id":
        return _LegacyProjectDomainAdapter()
    if recipe in _PROJECT_RECIPES:
        return _CodeProjectDomainAdapter(ds_version)
    message = f"DS {ds_version} has no reviewed native project lifecycle decision"
    raise WireContractError(message)


PROJECT_DOMAIN = BoundDomain[ProjectDomain](
    name="project",
    adapter_for_version=_project_domain_adapter,
)


def _code_project_view(project: ProjectPayloadRecord) -> ProjectView:
    code = project.code
    name = project.name
    if code is None or name is None:
        raise projection_error(
            ds_version="code-native",
            resource="project",
            field="identity",
            reason="project mutation response is missing code or name",
        )
    return ProjectView(
        ref=ProjectRef(
            native=NativeCode(code),
            name=name,
            description=project.description,
        ),
        id=project.id,
        user_id=project.userId,
        user_name=project.userName,
        create_time=project.createTime,
        update_time=project.updateTime,
        perm=project.perm,
        definition_count=project.defCount,
    )


def _require_native_code(native: NativeIdentity) -> int:
    if isinstance(native, NativeCode):
        return native.value
    message = "Code-native project adapter received an id identity"
    raise WireContractError(message)


def _require_native_id(native: NativeIdentity) -> int:
    if isinstance(native, NativeId):
        return native.value
    message = "Id-native project adapter received a code identity"
    raise WireContractError(message)


__all__ = [
    "PROJECT_DOMAIN",
    "NativeProjectMutations",
    "ProjectAdapter",
    "ProjectDomain",
]
