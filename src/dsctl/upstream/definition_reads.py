from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, Literal, NoReturn, Protocol, TypeVar

from dsctl.cli_surface import PROJECT_RESOURCE, SCHEDULE_RESOURCE, WORKFLOW_RESOURCE
from dsctl.errors import (
    ApiResultError,
    ApiTransportError,
    NotFoundError,
    PermissionDeniedError,
    ResolutionError,
)
from dsctl.upstream._selector_resolution import (
    collect_resolution_page_items,
    normalize_identifier,
    parse_numeric_identifier,
)
from dsctl.upstream.definition_models import (
    DefinitionPage,
    NativeCode,
    NativeId,
    NativeIdentity,
    ProjectRead,
    ProjectRef,
    ProjectView,
    ScheduleView,
    WorkflowListing,
    WorkflowListView,
    WorkflowRead,
    WorkflowRef,
    WorkflowScope,
    WorkflowView,
    same_native_identity,
)
from dsctl.upstream.pagination import requested_page_data

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence


ItemT_co = TypeVar("ItemT_co", covariant=True)
ItemT = TypeVar("ItemT")

_RESOLUTION_PAGE_SIZE = 100
_MAX_RESOLUTION_PAGES = 20
_MAX_AUTO_EXHAUST_PAGES = 100
_ATTACHED_SCHEDULE_PAGE_SIZE = 2

_PROJECT_NOT_FOUND = 10018
_PROJECT_NOT_EXIST = 10190
_USER_NO_OPERATION_PERMISSION = 30001
_USER_NO_OPERATION_PROJECT_PERMISSION = 30002
_LEGACY_PROCESS_INSTANCE_NOT_EXIST = 50001
_WORKFLOW_NOT_FOUND = 50003


@dataclass(frozen=True)
class WirePage(Generic[ItemT_co]):
    """Exact-wire page projected enough for safe pagination."""

    totalList: Sequence[ItemT_co] | None  # noqa: N815
    total: int | None
    totalPage: int | None  # noqa: N815
    pageSize: int | None  # noqa: N815
    currentPage: int | None  # noqa: N815
    pageNo: int | None  # noqa: N815


class DefinitionReadWire(Protocol):
    """Small version seam consumed by the definition-read recipe."""

    @property
    def identity_kind(self) -> Literal["id", "code"]:
        """Return the native project/workflow identity kind."""

    def project_page(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None,
    ) -> WirePage[ProjectView]:
        """Return projects visible to the authenticated user."""

    def project_detail(self, native: NativeIdentity) -> ProjectView:
        """Return one project by its native identity."""

    def workflow_refs(self, project: ProjectRef) -> Sequence[WorkflowRef]:
        """Return the profile's inexpensive workflow reference collection."""

    def workflow_page(
        self,
        project: ProjectRef,
        *,
        page_no: int,
        page_size: int,
        search: str | None,
    ) -> WirePage[WorkflowListView]:
        """Return one project-scoped workflow page."""

    def workflow_detail(
        self,
        project: ProjectRef,
        workflow: NativeIdentity,
    ) -> WorkflowView:
        """Return project-scoped workflow detail."""

    def schedule_page(
        self,
        project: ProjectRef,
        workflow: WorkflowRef,
        *,
        page_no: int,
        page_size: int,
    ) -> WirePage[ScheduleView]:
        """Return schedules filtered by one native workflow identity."""


@dataclass(frozen=True)
class _ResolvedProject:
    ref: ProjectRef
    detail: ProjectView | None


@dataclass(frozen=True)
class _ResolvedWorkflow:
    ref: WorkflowRef
    detail: WorkflowView | None


@dataclass(frozen=True)
class DefinitionReads:
    """Resolve and read project/workflow definitions across id/code dialects."""

    wire: DefinitionReadWire

    def list_projects(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None,
        all_pages: bool,
    ) -> DefinitionPage[ProjectView]:
        """Return one stable project page without exposing generated models."""
        return _requested_page(
            lambda current_page_no, current_page_size: self.wire.project_page(
                page_no=current_page_no,
                page_size=current_page_size,
                search=search,
            ),
            page_no=page_no,
            page_size=page_size,
            all_pages=all_pages,
            resource=PROJECT_RESOURCE,
        )

    def get_project(self, selector: str) -> ProjectRead:
        """Resolve and fetch one project with native-identity revalidation."""
        resolved = self._resolve_project(selector, detail_operation="get")
        detail = resolved.detail or self._project_detail(
            resolved.ref.native,
            operation="get",
        )
        _require_project_detail_identity(detail, expected=resolved.ref.native)
        return ProjectRead(project=resolved.ref, view=detail)

    def resolve_project(self, selector: str) -> ProjectRef:
        """Resolve one visible project without widening the caller's read closure."""
        return self._resolve_project(selector).ref

    def resolve_project_by_name(self, project_name: str) -> ProjectRef:
        """Resolve an explicit project name without interpreting numeric text."""
        normalized = normalize_identifier(project_name, label="Project")
        candidates = collect_resolution_page_items(
            fetch_page=lambda page_no, page_size: self.wire.project_page(
                page_no=page_no,
                page_size=page_size,
                search=normalized,
            ),
            page_size=_RESOLUTION_PAGE_SIZE,
            max_pages=_MAX_RESOLUTION_PAGES,
            safety_message=(
                f"Project name search for {normalized!r} exceeded the "
                "resolver safety limit"
            ),
            safety_details=_selector_details(
                PROJECT_RESOURCE,
                None,
                name=normalized,
            ),
        )
        matches = _unique_project_refs(
            candidate.ref
            for candidate in candidates
            if candidate.ref.name == normalized
        )
        match = _require_project_match(matches, normalized, numeric=None)
        detail = self._project_detail(match.native, operation=None)
        _require_project_detail_identity(detail, expected=match.native)
        project = _require_resolved_project_ref(detail.ref)
        if project.name != normalized:
            message = f"Project name {normalized!r} changed while it was being resolved"
            raise ResolutionError(
                message,
                details={
                    **_selector_details(
                        PROJECT_RESOURCE,
                        match.native,
                        name=normalized,
                    ),
                    "returned_name": project.name,
                    "reason": "project-name-changed-during-resolution",
                },
            )
        return project

    def visible_project_refs(self) -> tuple[ProjectRef, ...]:
        """Read the complete visible project identity inventory once."""
        project_rows = collect_resolution_page_items(
            fetch_page=lambda page_no, page_size: self.wire.project_page(
                page_no=page_no,
                page_size=page_size,
                search=None,
            ),
            page_size=_RESOLUTION_PAGE_SIZE,
            max_pages=_MAX_RESOLUTION_PAGES,
            safety_message=(
                "Project identity inventory exceeded the resolver safety limit"
            ),
            safety_details=_selector_details(PROJECT_RESOURCE, None),
        )
        return tuple(candidate.ref for candidate in project_rows)

    def workflow_refs(self, project_selector: str) -> tuple[WorkflowRef, ...]:
        """Return the profile's complete inexpensive workflow reference inventory."""
        project = self._resolve_project(project_selector).ref
        return tuple(self._workflow_refs(project))

    def visible_workflow_refs(
        self,
        project: ProjectRef,
    ) -> tuple[WorkflowRef, ...]:
        """Read one complete identity inventory for an already-proved project."""
        if self.wire.identity_kind == "code":
            return tuple(self._workflow_refs(project))
        workflow_rows = collect_resolution_page_items(
            fetch_page=lambda page_no, page_size: self._workflow_page(
                project,
                page_no=page_no,
                page_size=page_size,
                search=None,
            ),
            page_size=_RESOLUTION_PAGE_SIZE,
            max_pages=_MAX_RESOLUTION_PAGES,
            safety_message=(
                "Workflow identity inventory exceeded the resolver safety limit"
            ),
            safety_details=_workflow_selector_details(
                project,
                None,
                name=None,
            ),
        )
        return tuple(candidate.ref for candidate in workflow_rows)

    def list_workflows(
        self,
        project_selector: str,
        *,
        page_no: int,
        page_size: int,
        search: str | None,
        all_pages: bool,
    ) -> WorkflowListing:
        """Resolve one project and return its stable workflow page."""
        project = self._resolve_project(project_selector).ref
        page = _requested_page(
            lambda current_page_no, current_page_size: self._workflow_page(
                project,
                page_no=current_page_no,
                page_size=current_page_size,
                search=search,
            ),
            page_no=page_no,
            page_size=page_size,
            all_pages=all_pages,
            resource=WORKFLOW_RESOURCE,
        )
        return WorkflowListing(project=project, page=page)

    def get_workflow(
        self,
        project_selector: str,
        workflow_selector: str,
        *,
        schedule_list_supported: bool,
    ) -> WorkflowRead:
        """Resolve, fetch, scope-check, and hydrate one workflow definition."""
        project = self._resolve_project(project_selector).ref
        resolved_workflow = self._resolve_workflow(project, workflow_selector)
        detail = resolved_workflow.detail or self._workflow_detail(
            project,
            resolved_workflow.ref.native,
        )
        _require_workflow_detail_identity(
            detail,
            project=project,
            workflow=resolved_workflow.ref,
        )
        authoritative_ref = _require_resolved_workflow_ref(detail.ref)
        attached_schedule = self._load_attached_schedule(
            project,
            authoritative_ref,
            schedule_list_supported=schedule_list_supported,
        )
        return WorkflowRead(
            project=project,
            workflow=authoritative_ref,
            view=detail,
            attached_schedule=attached_schedule,
        )

    def resolve_workflow(
        self,
        project_selector: str,
        workflow_selector: str,
    ) -> WorkflowScope:
        """Resolve and scope-check workflow detail without reading schedule state."""
        project = self._resolve_project(project_selector).ref
        resolved = self._resolve_workflow(project, workflow_selector)
        detail = resolved.detail or self._workflow_detail(project, resolved.ref.native)
        _require_workflow_detail_identity(
            detail,
            project=project,
            workflow=resolved.ref,
        )
        workflow = _require_resolved_workflow_ref(detail.ref)
        return WorkflowScope(project=project, workflow=workflow, view=detail)

    def resolve_workflow_by_name(
        self,
        project: ProjectRef,
        workflow_name: str,
    ) -> WorkflowScope:
        """Resolve an explicit name without reinterpreting numeric text as an id."""
        normalized = normalize_identifier(workflow_name, label="Workflow")
        match = self._find_workflow_ref_by_normalized_name(project, normalized)
        if match is None:
            match = _require_workflow_match(
                (),
                project=project,
                selector=normalized,
                numeric=None,
            )
        detail = self._workflow_detail(project, match.native)
        _require_workflow_detail_identity(
            detail,
            project=project,
            workflow=match,
        )
        workflow = _require_resolved_workflow_ref(detail.ref)
        if workflow.name != normalized:
            message = (
                f"Workflow name {normalized!r} changed while it was being resolved"
            )
            raise ResolutionError(
                message,
                details={
                    **_workflow_selector_details(
                        project,
                        match.native,
                        name=normalized,
                    ),
                    "returned_name": workflow.name,
                    "reason": "workflow-name-changed-during-resolution",
                },
            )
        return WorkflowScope(project=project, workflow=workflow, view=detail)

    def find_workflow_ref_by_name(
        self,
        project: ProjectRef,
        workflow_name: str,
    ) -> WorkflowRef | None:
        """Return one exact-name ref from a bounded name-filtered page search."""
        normalized = normalize_identifier(workflow_name, label="Workflow")
        return self._find_workflow_ref_by_normalized_name(project, normalized)

    def resolve_workflow_by_id(
        self,
        project: ProjectRef,
        workflow_id: int,
    ) -> WorkflowScope:
        """Resolve one exact legacy database id inside an already-proved project."""
        if self.wire.identity_kind != "id":
            message = "Explicit workflow-id resolution requires an id-native profile"
            raise ResolutionError(
                message,
                details=_workflow_selector_details(project, NativeId(workflow_id)),
            )
        resolved = self._resolve_workflow(project, str(workflow_id))
        detail = resolved.detail or self._workflow_detail(
            project,
            resolved.ref.native,
        )
        _require_workflow_detail_identity(
            detail,
            project=project,
            workflow=resolved.ref,
        )
        workflow = _require_resolved_workflow_ref(detail.ref)
        return WorkflowScope(project=project, workflow=workflow, view=detail)

    def resolve_workflow_by_code(
        self,
        project: ProjectRef,
        workflow_code: int,
    ) -> WorkflowScope:
        """Resolve one exact code inside an already-proved code-native project."""
        if self.wire.identity_kind != "code":
            message = "Explicit workflow-code resolution requires a code-native profile"
            raise ResolutionError(
                message,
                details=_workflow_selector_details(
                    project,
                    NativeCode(workflow_code),
                ),
            )
        native = NativeCode(workflow_code)
        detail = self._workflow_detail(project, native)
        _require_workflow_detail_identity(
            detail,
            project=project,
            workflow=WorkflowRef(
                native=native,
                name=detail.ref.name,
                version=detail.ref.version,
            ),
        )
        workflow = _require_resolved_workflow_ref(detail.ref)
        return WorkflowScope(project=project, workflow=workflow, view=detail)

    def _resolve_project(
        self,
        selector: str,
        *,
        detail_operation: str | None = None,
    ) -> _ResolvedProject:
        normalized = normalize_identifier(selector, label="Project")
        numeric = parse_numeric_identifier(normalized)
        if numeric is not None and self.wire.identity_kind == "code":
            native = NativeCode(numeric)
            detail = self._project_detail(native, operation=detail_operation)
            _require_project_detail_identity(detail, expected=native)
            return _ResolvedProject(
                ref=_require_resolved_project_ref(detail.ref),
                detail=detail,
            )

        search = None if numeric is not None else normalized
        candidates = collect_resolution_page_items(
            fetch_page=lambda page_no, page_size: self.wire.project_page(
                page_no=page_no,
                page_size=page_size,
                search=search,
            ),
            page_size=_RESOLUTION_PAGE_SIZE,
            max_pages=_MAX_RESOLUTION_PAGES,
            safety_message=(
                f"Project search for {normalized!r} exceeded the resolver safety limit"
            ),
            safety_details=_selector_details(
                PROJECT_RESOURCE,
                NativeId(numeric) if numeric is not None else None,
                name=None if numeric is not None else normalized,
            ),
        )
        matches = _unique_project_refs(
            candidate.ref
            for candidate in candidates
            if (
                same_native_identity(candidate.ref.native, NativeId(numeric))
                if numeric is not None
                else candidate.ref.name == normalized
            )
        )
        match = _require_project_match(matches, normalized, numeric=numeric)
        return _ResolvedProject(
            ref=_require_resolved_project_ref(match),
            detail=None,
        )

    def _resolve_workflow(
        self,
        project: ProjectRef,
        selector: str,
    ) -> _ResolvedWorkflow:
        normalized = normalize_identifier(selector, label="Workflow")
        numeric = parse_numeric_identifier(normalized)
        if numeric is not None and self.wire.identity_kind == "code":
            native = NativeCode(numeric)
            detail = self._workflow_detail(project, native)
            _require_workflow_detail_identity(
                detail,
                project=project,
                workflow=WorkflowRef(
                    native=native,
                    name=detail.ref.name,
                    version=detail.ref.version,
                ),
            )
            return _ResolvedWorkflow(
                ref=_require_resolved_workflow_ref(detail.ref),
                detail=detail,
            )

        if self.wire.identity_kind == "code":
            workflow_refs = self._workflow_refs(project)
        else:
            search = None if numeric is not None else normalized
            workflow_rows = collect_resolution_page_items(
                fetch_page=lambda page_no, page_size: self._workflow_page(
                    project,
                    page_no=page_no,
                    page_size=page_size,
                    search=search,
                ),
                page_size=_RESOLUTION_PAGE_SIZE,
                max_pages=_MAX_RESOLUTION_PAGES,
                safety_message=(
                    f"Workflow search for {normalized!r} exceeded the resolver "
                    "safety limit"
                ),
                safety_details=_workflow_selector_details(
                    project,
                    NativeId(numeric) if numeric is not None else None,
                    name=None if numeric is not None else normalized,
                ),
            )
            workflow_refs = [candidate.ref for candidate in workflow_rows]

        matches = _unique_workflow_refs(
            candidate
            for candidate in workflow_refs
            if (
                same_native_identity(candidate.native, NativeId(numeric))
                if numeric is not None
                else candidate.name == normalized
            )
        )
        match = _require_workflow_match(
            matches,
            project=project,
            selector=normalized,
            numeric=numeric,
        )
        return _ResolvedWorkflow(
            ref=_require_resolved_workflow_ref(match),
            detail=None,
        )

    def _find_workflow_ref_by_normalized_name(
        self,
        project: ProjectRef,
        normalized_name: str,
    ) -> WorkflowRef | None:
        workflow_rows = collect_resolution_page_items(
            fetch_page=lambda page_no, page_size: self._workflow_page(
                project,
                page_no=page_no,
                page_size=page_size,
                search=normalized_name,
            ),
            page_size=_RESOLUTION_PAGE_SIZE,
            max_pages=_MAX_RESOLUTION_PAGES,
            safety_message=(
                f"Workflow name search for {normalized_name!r} exceeded the "
                "resolver safety limit"
            ),
            safety_details=_workflow_selector_details(
                project,
                None,
                name=normalized_name,
            ),
        )
        matches = _unique_workflow_refs(
            candidate.ref
            for candidate in workflow_rows
            if candidate.ref.name == normalized_name
        )
        if not matches:
            return None
        return _require_workflow_match(
            matches,
            project=project,
            selector=normalized_name,
            numeric=None,
        )

    def _project_detail(
        self,
        native: NativeIdentity,
        *,
        operation: str | None,
    ) -> ProjectView:
        try:
            return self.wire.project_detail(native)
        except ApiResultError as error:
            _raise_project_error(error, native=native, operation=operation)

    def _workflow_refs(self, project: ProjectRef) -> Sequence[WorkflowRef]:
        try:
            return self.wire.workflow_refs(project)
        except ApiResultError as error:
            _raise_workflow_error(error, project=project)

    def _workflow_page(
        self,
        project: ProjectRef,
        *,
        page_no: int,
        page_size: int,
        search: str | None,
    ) -> WirePage[WorkflowListView]:
        try:
            return self.wire.workflow_page(
                project,
                page_no=page_no,
                page_size=page_size,
                search=search,
            )
        except ApiResultError as error:
            _raise_workflow_error(error, project=project)

    def _workflow_detail(
        self,
        project: ProjectRef,
        workflow: NativeIdentity,
    ) -> WorkflowView:
        try:
            return self.wire.workflow_detail(project, workflow)
        except ApiResultError as error:
            _raise_workflow_error(error, project=project, workflow=workflow)

    def _load_attached_schedule(
        self,
        project: ProjectRef,
        workflow: WorkflowRef,
        *,
        schedule_list_supported: bool,
    ) -> ScheduleView | None:
        try:
            page = self.wire.schedule_page(
                project,
                workflow,
                page_no=1,
                page_size=_ATTACHED_SCHEDULE_PAGE_SIZE,
            )
        except ApiResultError as error:
            _raise_schedule_error(
                error,
                project=project,
                workflow=workflow,
                schedule_list_supported=schedule_list_supported,
            )

        schedules = list(page.totalList or ())
        reported_total = page.total
        valid_total = isinstance(reported_total, int) and not isinstance(
            reported_total,
            bool,
        )
        if valid_total and reported_total == 0 and not schedules:
            return None
        invalid_fields: list[str] = []
        if valid_total and reported_total == 1 and len(schedules) == 1:
            schedule = schedules[0]
            if not same_native_identity(
                schedule.workflow_native,
                workflow.native,
            ):
                invalid_fields.append(_schedule_workflow_field(workflow.native))
            if (
                not isinstance(schedule.id, int)
                or isinstance(schedule.id, bool)
                or schedule.id <= 0
            ):
                invalid_fields.append("id")
            if not invalid_fields:
                return schedule

        identity_details = _workflow_selector_details(
            project,
            workflow.native,
            name=workflow.name,
        )
        message = (
            "DolphinScheduler returned inconsistent attached-schedule state "
            "for the selected workflow"
        )
        raise ApiTransportError(
            message,
            details={
                **identity_details,
                "dependency_resource": SCHEDULE_RESOURCE,
                "operation": "workflow.get",
                "phase": "read",
                "mutation_applied": False,
                "reported_total": reported_total,
                "returned_count": len(schedules),
                "schedule_ids": [schedule.id for schedule in schedules],
                _returned_schedule_identity_field(workflow.native): [
                    schedule.workflow_native.value for schedule in schedules
                ],
                "invalid_fields": invalid_fields,
            },
            suggestion=_attached_schedule_suggestion(
                project,
                workflow,
                schedule_list_supported=schedule_list_supported,
            ),
        )


def _requested_page(
    fetch_page: Callable[[int, int], WirePage[ItemT]],
    *,
    page_no: int,
    page_size: int,
    all_pages: bool,
    resource: str,
) -> DefinitionPage[ItemT]:
    data = requested_page_data(
        fetch_page,
        page_no=page_no,
        page_size=page_size,
        all_pages=all_pages,
        serialize_item=lambda item: item,
        resource=resource,
        max_pages=_MAX_AUTO_EXHAUST_PAGES,
    )
    return DefinitionPage(
        totalList=tuple(data["totalList"]),
        total=data["total"],
        totalPage=data["totalPage"],
        pageSize=data["pageSize"],
        currentPage=data["currentPage"],
        pageNo=data["pageNo"],
        coverage=data.get("coverage"),
    )


def _require_project_detail_identity(
    detail: ProjectView,
    *,
    expected: NativeIdentity,
) -> None:
    if same_native_identity(detail.ref.native, expected):
        return
    message = "DolphinScheduler returned a project with an unexpected native identity"
    raise ApiTransportError(
        message,
        details={
            "resource": PROJECT_RESOURCE,
            "expected_identity": expected.value,
            "returned_identity": detail.ref.native.value,
            "expected_identity_kind": type(expected).__name__,
            "returned_identity_kind": type(detail.ref.native).__name__,
        },
    )


def _require_workflow_detail_identity(
    detail: WorkflowView,
    *,
    project: ProjectRef,
    workflow: WorkflowRef,
) -> None:
    if not same_native_identity(detail.ref.native, workflow.native):
        raise NotFoundError(
            _workflow_not_found_message(workflow.native),
            details=_workflow_selector_details(project, workflow.native),
        )
    if not same_native_identity(detail.project_native, project.native):
        raise NotFoundError(
            _workflow_not_found_message(workflow.native),
            details=_workflow_selector_details(project, workflow.native),
        )


def _require_resolved_project_ref(project: ProjectRef) -> ProjectRef:
    if project.name is not None:
        return project
    message = "Resolved project payload was missing required identity fields"
    raise ResolutionError(message, details={"resource": PROJECT_RESOURCE})


def _require_resolved_workflow_ref(workflow: WorkflowRef) -> WorkflowRef:
    if workflow.name is not None:
        return workflow
    message = "Resolved workflow payload was missing required identity fields"
    raise ResolutionError(message, details={"resource": WORKFLOW_RESOURCE})


def _require_project_match(
    matches: Sequence[ProjectRef],
    selector: str,
    *,
    numeric: int | None,
) -> ProjectRef:
    native = NativeId(numeric) if numeric is not None else None
    details = _selector_details(
        PROJECT_RESOURCE,
        native,
        name=None if numeric is not None else selector,
    )
    if not matches:
        label = f"id {numeric}" if numeric is not None else repr(selector)
        message = f"Project {label} was not found"
        raise NotFoundError(message, details=details)
    if len(matches) > 1:
        message = f"Project name {selector!r} is ambiguous"
        raise ResolutionError(
            message,
            details={
                **details,
                _identity_plural(matches[0].native): [
                    match.native.value for match in matches
                ],
            },
        )
    return matches[0]


def _require_workflow_match(
    matches: Sequence[WorkflowRef],
    *,
    project: ProjectRef,
    selector: str,
    numeric: int | None,
) -> WorkflowRef:
    native = NativeId(numeric) if numeric is not None else None
    details = _workflow_selector_details(
        project,
        native,
        name=None if numeric is not None else selector,
    )
    if not matches:
        label = f"id {numeric}" if numeric is not None else repr(selector)
        message = f"Workflow {label} was not found"
        raise NotFoundError(message, details=details)
    if len(matches) > 1:
        message = f"Workflow name {selector!r} is ambiguous"
        raise ResolutionError(
            message,
            details={
                **details,
                _identity_plural(matches[0].native): [
                    match.native.value for match in matches
                ],
            },
        )
    return matches[0]


def _unique_project_refs(candidates: Iterable[ProjectRef]) -> list[ProjectRef]:
    unique: list[ProjectRef] = []
    seen: set[tuple[str, int]] = set()
    for candidate in candidates:
        key = (_identity_label(candidate.native), candidate.native.value)
        if key in seen:
            continue
        seen.add(key)
        unique.append(candidate)
    return unique


def _unique_workflow_refs(
    candidates: Iterable[WorkflowRef],
) -> list[WorkflowRef]:
    unique: list[WorkflowRef] = []
    seen: set[tuple[str, int]] = set()
    for candidate in candidates:
        key = (_identity_label(candidate.native), candidate.native.value)
        if key in seen:
            continue
        seen.add(key)
        unique.append(candidate)
    return unique


def _raise_project_error(
    error: ApiResultError,
    *,
    native: NativeIdentity,
    operation: str | None,
) -> NoReturn:
    details = _selector_details(PROJECT_RESOURCE, native)
    if operation is not None:
        details["operation"] = operation
    if error.result_code in {_PROJECT_NOT_FOUND, _PROJECT_NOT_EXIST}:
        message = f"Project {_identity_label(native)} {native.value} was not found"
        raise NotFoundError(
            message,
            details=details,
        ) from error
    if error.result_code == _USER_NO_OPERATION_PROJECT_PERMISSION:
        message = "Current user does not have permission to access this project"
        raise PermissionDeniedError(
            message,
            details=details,
            suggestion=(
                "Ask a DolphinScheduler administrator or project owner to grant "
                "access to the project, then retry."
            ),
        ) from error
    raise error


def _raise_workflow_error(
    error: ApiResultError,
    *,
    project: ProjectRef,
    workflow: NativeIdentity | None = None,
) -> NoReturn:
    if error.result_code == _PROJECT_NOT_EXIST:
        message = (
            f"Project {_identity_label(project.native)} "
            f"{project.native.value} was not found"
        )
        raise NotFoundError(
            message,
            details=_selector_details(PROJECT_RESOURCE, project.native),
        ) from error
    details = _workflow_selector_details(project, workflow)
    if error.result_code == _USER_NO_OPERATION_PROJECT_PERMISSION:
        message = (
            "Current user does not have permission to access workflows in "
            f"the selected project {project.native.value}"
        )
        raise PermissionDeniedError(
            message,
            details=details,
            suggestion=(
                "Ask a DolphinScheduler administrator or project owner to grant "
                "workflow-definition access in the project, then retry."
            ),
        ) from error
    if (
        error.result_code == _WORKFLOW_NOT_FOUND
        or (
            error.result_code == _LEGACY_PROCESS_INSTANCE_NOT_EXIST
            and isinstance(workflow, NativeId)
        )
    ) and workflow is not None:
        message = f"Workflow {_identity_label(workflow)} {workflow.value} was not found"
        raise NotFoundError(
            message,
            details=details,
        ) from error
    raise error


def _raise_schedule_error(
    error: ApiResultError,
    *,
    project: ProjectRef,
    workflow: WorkflowRef,
    schedule_list_supported: bool,
) -> NoReturn:
    details = {
        **_workflow_selector_details(project, workflow.native, name=workflow.name),
        "dependency_resource": SCHEDULE_RESOURCE,
        "operation": "workflow.get",
        "phase": "read",
        "mutation_applied": False,
        "upstream_result_code": error.result_code,
        "upstream_result_message": error.result_message,
    }
    suggestion = _attached_schedule_suggestion(
        project,
        workflow,
        schedule_list_supported=schedule_list_supported,
    )
    if error.result_code in {_PROJECT_NOT_FOUND, _PROJECT_NOT_EXIST}:
        message = (
            f"Project {_identity_label(project.native)} {project.native.value} "
            "was not found while loading workflow state."
        )
        raise NotFoundError(
            message,
            details=details,
            suggestion=suggestion,
        ) from error
    if error.result_code == _WORKFLOW_NOT_FOUND:
        message = (
            f"Workflow {_identity_label(workflow.native)} "
            f"{workflow.native.value} was not found while loading its schedule."
        )
        raise NotFoundError(
            message,
            details=details,
            suggestion=suggestion,
        ) from error
    if error.result_code in {
        _USER_NO_OPERATION_PERMISSION,
        _USER_NO_OPERATION_PROJECT_PERMISSION,
    }:
        message = (
            "Loading the workflow's attached schedule requires project permission."
        )
        raise PermissionDeniedError(
            message,
            details=details,
            suggestion=(
                "Ask a DolphinScheduler administrator for the project permission "
                "needed to inspect schedules in this project. Then retry the "
                "workflow command."
            ),
        ) from error
    message = "DolphinScheduler could not load the workflow's attached schedule."
    raise ApiTransportError(
        message,
        details=details,
        suggestion=suggestion,
    ) from error


def _attached_schedule_suggestion(
    project: ProjectRef,
    workflow: WorkflowRef,
    *,
    schedule_list_supported: bool,
) -> str:
    workflow_verification = (
        f"`dsctl workflow get {workflow.native.value} --project {project.native.value}`"
    )
    if not schedule_list_supported:
        return (
            f"Run {workflow_verification} to retry the supported workflow read; "
            "its result includes attached-schedule state."
        )
    schedule_verification = (
        f"`dsctl schedule list --project {project.native.value} --workflow "
        f"{workflow.native.value}`"
    )
    return f"Run {schedule_verification} to verify the attached schedule, then retry."


def _selector_details(
    resource: str,
    native: NativeIdentity | None,
    *,
    name: str | None = None,
) -> dict[str, str | int]:
    details: dict[str, str | int] = {"resource": resource}
    if native is not None:
        details[_identity_label(native)] = native.value
    if name is not None:
        details["name"] = name
    return details


def _workflow_selector_details(
    project: ProjectRef,
    workflow: NativeIdentity | None,
    *,
    name: str | None = None,
) -> dict[str, str | int]:
    details: dict[str, str | int] = {
        "resource": WORKFLOW_RESOURCE,
        f"project_{_identity_label(project.native)}": project.native.value,
    }
    if workflow is not None:
        details[_identity_label(workflow)] = workflow.value
    if name is not None:
        details["workflow_name"] = name
    return details


def _workflow_not_found_message(
    workflow: NativeIdentity,
) -> str:
    return (
        f"Workflow {_identity_label(workflow)} {workflow.value} was not found in "
        "the selected project"
    )


def _identity_label(native: NativeIdentity) -> str:
    return "id" if isinstance(native, NativeId) else "code"


def _identity_plural(native: NativeIdentity) -> str:
    return "ids" if isinstance(native, NativeId) else "codes"


def _schedule_workflow_field(native: NativeIdentity) -> str:
    return (
        "processDefinitionId"
        if isinstance(native, NativeId)
        else "workflowDefinitionCode"
    )


def _returned_schedule_identity_field(native: NativeIdentity) -> str:
    return (
        "returned_workflow_ids"
        if isinstance(native, NativeId)
        else "returned_workflow_codes"
    )


__all__ = ["DefinitionReadWire", "DefinitionReads", "WirePage"]
