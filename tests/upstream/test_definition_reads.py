from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, TypeVar

import pytest

from dsctl.errors import (
    ApiResultError,
    ApiTransportError,
    NotFoundError,
    PermissionDeniedError,
    ResolutionError,
)
from dsctl.upstream.definition_models import (
    NativeCode,
    NativeId,
    NativeIdentity,
    ProjectRef,
    ProjectView,
    ScheduleView,
    WorkflowListView,
    WorkflowRef,
    WorkflowView,
)
from dsctl.upstream.definition_reads import DefinitionReads, WirePage


def test_legacy_numeric_project_must_be_visible_before_unsafe_detail() -> None:
    visible = _project(1, "visible")
    hidden = _project(9, "hidden")
    wire = FakeDefinitionWire(
        identity_kind="id",
        projects=[visible],
        project_details={9: hidden},
    )

    with pytest.raises(NotFoundError, match="Project id 9 was not found"):
        DefinitionReads(wire).get_project("9")

    assert wire.project_detail_calls == []


def test_legacy_resolve_project_by_name_keeps_numeric_text_as_a_name() -> None:
    numeric_name = _project(9, "123")
    numeric_id = _project(123, "different-project")
    wire = FakeDefinitionWire(
        identity_kind="id",
        projects=[numeric_name, numeric_id],
        project_details={9: numeric_name, 123: numeric_id},
    )

    project = DefinitionReads(wire).resolve_project_by_name("123")

    assert project == numeric_name.ref
    assert wire.project_page_calls == [(1, 100, "123")]
    assert wire.project_detail_calls == [9]


def test_legacy_resolve_project_by_name_rejects_detail_rename() -> None:
    listed = _project(9, "old-name")
    renamed = _project(9, "new-name")
    wire = FakeDefinitionWire(
        identity_kind="id",
        projects=[listed],
        project_details={9: renamed},
    )

    with pytest.raises(ResolutionError, match="changed while") as raised:
        DefinitionReads(wire).resolve_project_by_name("old-name")

    assert raised.value.details["returned_name"] == "new-name"
    assert raised.value.details["reason"] == "project-name-changed-during-resolution"


def test_legacy_resolve_project_by_name_revalidates_detail_identity() -> None:
    listed = _project(9, "project-name")
    mismatched = _project(10, "project-name")
    wire = FakeDefinitionWire(
        identity_kind="id",
        projects=[listed],
        project_details={9: mismatched},
    )

    with pytest.raises(ApiTransportError, match="unexpected native identity"):
        DefinitionReads(wire).resolve_project_by_name("project-name")

    assert wire.project_detail_calls == [9]


def test_legacy_visible_project_refs_batches_inventory_without_details() -> None:
    projects = [
        _project(project_id, f"project-{project_id}") for project_id in range(1, 151)
    ]
    wire = FakeDefinitionWire(
        identity_kind="id",
        projects=projects,
        project_details={},
    )

    refs = DefinitionReads(wire).visible_project_refs()

    assert len(refs) == 150
    assert wire.project_page_calls == [(1, 100, None), (2, 100, None)]
    assert wire.project_detail_calls == []


def test_list_projects_all_pages_preserves_source_coverage() -> None:
    projects = [_project(value, f"project-{value}") for value in range(1, 6)]
    wire = FakeDefinitionWire(
        identity_kind="id",
        projects=projects,
        project_details={},
    )

    page = DefinitionReads(wire).list_projects(
        page_no=2,
        page_size=2,
        search=None,
        all_pages=True,
    )

    assert [project.ref.name for project in page.totalList] == [
        "project-3",
        "project-4",
        "project-5",
    ]
    assert (page.total, page.totalPage, page.pageSize, page.pageNo) == (3, 1, 3, 1)
    assert page.coverage is not None
    assert page.coverage["requested_start_page"] == 2
    assert page.coverage["initial_total"] == 5
    assert page.coverage["initial_total_pages"] == 3
    assert page.coverage["pages_read"] == 2
    assert page.coverage["rows_read"] == 3
    requested_pages = [
        observation["requested_page"] for observation in page.coverage["pages"]
    ]
    assert requested_pages == [2, 3]
    assert page.coverage["scope_complete"] is True
    assert page.coverage["atomic_snapshot"] is False


def test_list_workflows_single_page_preserves_requested_page_coverage() -> None:
    project = _project(7, "etl", identity_kind="code")
    workflows = [
        _workflow_list(value, name=f"workflow-{value}") for value in range(1, 3)
    ]
    wire = FakeDefinitionWire(
        identity_kind="code",
        projects=[],
        project_details={7: project},
        workflow_pages={7: workflows},
    )

    listing = DefinitionReads(wire).list_workflows(
        "7",
        page_no=1,
        page_size=1,
        search=None,
        all_pages=False,
    )

    assert [workflow.ref.name for workflow in listing.page.totalList] == ["workflow-1"]
    assert listing.page.coverage is not None
    assert listing.page.coverage["scope"] == "requested_page"
    assert listing.page.coverage["initial_total"] == 2
    assert listing.page.coverage["initial_total_pages"] == 2
    assert listing.page.coverage["pages_read"] == 1
    assert listing.page.coverage["rows_read"] == 1


def test_legacy_numeric_workflow_must_be_visible_in_selected_project() -> None:
    project = _project(1, "visible")
    hidden_workflow = _workflow(9, project_id=2, name="hidden")
    wire = FakeDefinitionWire(
        identity_kind="id",
        projects=[project],
        project_details={1: project},
        workflow_details={(1, 9): hidden_workflow},
    )

    with pytest.raises(NotFoundError, match="Workflow id 9 was not found"):
        DefinitionReads(wire).get_workflow(
            "1",
            "9",
            schedule_list_supported=False,
        )

    assert wire.workflow_detail_calls == []


def test_legacy_resolve_workflow_by_id_requires_project_page_membership() -> None:
    project = _project(1, "visible")
    hidden_workflow = _workflow(9, project_id=1, name="hidden")
    wire = FakeDefinitionWire(
        identity_kind="id",
        projects=[project],
        project_details={1: project},
        workflow_details={(1, 9): hidden_workflow},
    )

    with pytest.raises(NotFoundError, match="Workflow id 9 was not found"):
        DefinitionReads(wire).resolve_workflow_by_id(project.ref, 9)

    assert wire.workflow_detail_calls == []


def test_code_native_resolve_workflow_by_code_revalidates_project_identity() -> None:
    project = _project(7, "etl", identity_kind="code")
    workflow = _workflow(
        9001,
        project_id=7,
        name="child-daily",
        identity_kind="code",
    )
    wire = FakeDefinitionWire(
        identity_kind="code",
        projects=[project],
        project_details={7: project},
        workflow_details={(7, 9001): workflow},
    )

    scope = DefinitionReads(wire).resolve_workflow_by_code(project.ref, 9001)

    assert scope.project == project.ref
    assert scope.workflow == workflow.ref
    assert wire.workflow_detail_calls == [(7, 9001)]


def test_legacy_visible_workflow_refs_batches_inventory_without_details() -> None:
    project = _project(1, "visible")
    workflows = [
        _workflow_list(workflow_id, name=f"workflow-{workflow_id}")
        for workflow_id in range(1, 151)
    ]
    wire = FakeDefinitionWire(
        identity_kind="id",
        projects=[project],
        project_details={1: project},
        workflow_pages={1: workflows},
    )

    refs = DefinitionReads(wire).visible_workflow_refs(project.ref)

    assert len(refs) == 150
    assert wire.workflow_page_calls == [(1, 100, None), (2, 100, None)]
    assert wire.workflow_detail_calls == []


def test_find_workflow_ref_by_name_filters_before_large_project_paging() -> None:
    project = _project(1, "visible")
    workflows = [
        _workflow_list(workflow_id, name=f"workflow-{workflow_id}")
        for workflow_id in range(1, 2002)
    ]
    wire = FakeDefinitionWire(
        identity_kind="id",
        projects=[project],
        project_details={1: project},
        workflow_pages={1: workflows},
    )

    found = DefinitionReads(wire).find_workflow_ref_by_name(project.ref, "new-name")

    assert found is None
    assert wire.workflow_page_calls == [(1, 100, "new-name")]
    assert wire.workflow_detail_calls == []


def test_find_workflow_ref_by_name_returns_exact_ref_without_detail() -> None:
    project = _project(1, "visible")
    target = _workflow_list(2001, name="target")
    workflows = [
        _workflow_list(workflow_id, name=f"workflow-{workflow_id}")
        for workflow_id in range(1, 2001)
    ]
    wire = FakeDefinitionWire(
        identity_kind="id",
        projects=[project],
        project_details={1: project},
        workflow_pages={1: [*workflows, target]},
    )

    found = DefinitionReads(wire).find_workflow_ref_by_name(project.ref, "target")

    assert found == target.ref
    assert wire.workflow_page_calls == [(1, 100, "target")]
    assert wire.workflow_detail_calls == []


def test_find_workflow_ref_by_name_rejects_fuzzy_search_rows() -> None:
    project = _project(1, "visible")
    wire = FakeDefinitionWire(
        identity_kind="id",
        projects=[project],
        project_details={1: project},
        workflow_pages={
            1: [
                _workflow_list(1, name="target-copy"),
                _workflow_list(2, name="prefix-target"),
            ]
        },
        fuzzy_workflow_search=True,
    )

    found = DefinitionReads(wire).find_workflow_ref_by_name(project.ref, "target")

    assert found is None
    assert wire.workflow_page_calls == [(1, 100, "target")]


@pytest.mark.parametrize(
    ("error", "expected_type"),
    [
        (
            ApiResultError(
                result_code=30002,
                result_message="no project permission",
            ),
            PermissionDeniedError,
        ),
        (ApiTransportError("temporary read failure"), ApiTransportError),
    ],
)
def test_find_workflow_ref_by_name_preserves_search_failures(
    error: Exception,
    expected_type: type[Exception],
) -> None:
    project = _project(1, "visible")
    wire = FakeDefinitionWire(
        identity_kind="id",
        projects=[project],
        project_details={1: project},
        workflow_page_error=error,
    )

    with pytest.raises(expected_type):
        DefinitionReads(wire).find_workflow_ref_by_name(project.ref, "target")


def test_legacy_resolve_workflow_by_name_rejects_detail_rename() -> None:
    project = _project(1, "visible")
    listed = _workflow_list(9, name="old-name")
    renamed = _workflow(9, project_id=1, name="new-name")
    wire = FakeDefinitionWire(
        identity_kind="id",
        projects=[project],
        project_details={1: project},
        workflow_pages={1: [listed]},
        workflow_details={(1, 9): renamed},
    )

    with pytest.raises(ResolutionError, match="changed while") as raised:
        DefinitionReads(wire).resolve_workflow_by_name(project.ref, "old-name")

    assert raised.value.details["returned_name"] == "new-name"
    assert raised.value.details["reason"] == ("workflow-name-changed-during-resolution")


def test_legacy_50001_detail_result_is_translated_to_workflow_not_found() -> None:
    project = _project(1, "visible")
    listed = _workflow_list(9, name="deleted")
    wire = FakeDefinitionWire(
        identity_kind="id",
        projects=[project],
        project_details={1: project},
        workflow_pages={1: [listed]},
        workflow_detail_errors={
            (1, 9): ApiResultError(
                result_code=50001,
                result_message="process instance not exist",
            )
        },
    )

    with pytest.raises(NotFoundError, match="Workflow id 9 was not found"):
        DefinitionReads(wire).resolve_workflow_by_id(project.ref, 9)

    assert wire.workflow_detail_calls == [(1, 9)]


def test_legacy_workflow_detail_revalidates_project_before_schedule_read() -> None:
    project = _project(1, "visible")
    visible_ref = _workflow_list(9, name="visible-workflow")
    leaked_detail = _workflow(9, project_id=2, name="visible-workflow")
    wire = FakeDefinitionWire(
        identity_kind="id",
        projects=[project],
        project_details={1: project},
        workflow_pages={1: [visible_ref]},
        workflow_details={(1, 9): leaked_detail},
    )

    with pytest.raises(
        NotFoundError,
        match="was not found in the selected project",
    ):
        DefinitionReads(wire).get_workflow(
            "1",
            "9",
            schedule_list_supported=False,
        )

    assert wire.workflow_detail_calls == [(1, 9)]
    assert wire.schedule_calls == []


def test_legacy_project_detail_revalidates_returned_id() -> None:
    visible = _project(1, "visible")
    mismatched = _project(2, "other")
    wire = FakeDefinitionWire(
        identity_kind="id",
        projects=[visible],
        project_details={1: mismatched},
    )

    with pytest.raises(ApiTransportError, match="unexpected native identity"):
        DefinitionReads(wire).get_project("1")

    assert wire.project_detail_calls == [1]


def test_code_recipe_keeps_numeric_direct_lookup_and_modern_output_shape() -> None:
    project = _project(7, "etl", identity_kind="code")
    workflow = _workflow(
        101,
        project_id=7,
        name="daily",
        identity_kind="code",
    )
    schedule = ScheduleView(
        id=23,
        workflow_native=NativeCode(101),
        workflow_name="daily",
        project_name="etl",
        start_time="2026-01-01 00:00:00",
        end_time="2026-12-31 23:59:59",
        timezone_id="UTC",
        crontab="0 0 0 * * ?",
        failure_strategy="CONTINUE",
        workflow_instance_priority="MEDIUM",
        release_state="ONLINE",
    )
    wire = FakeDefinitionWire(
        identity_kind="code",
        projects=[],
        project_details={7: project},
        workflow_details={(7, 101): workflow},
        schedules={(7, 101): [schedule]},
    )

    result = DefinitionReads(wire).get_workflow(
        "7",
        "101",
        schedule_list_supported=True,
    )

    assert wire.project_page_calls == []
    assert wire.project_detail_calls == [7]
    assert wire.workflow_detail_calls == [(7, 101)]
    assert result.project.to_data() == {
        "code": 7,
        "name": "etl",
        "description": None,
    }
    assert result.workflow.to_data() == {
        "code": 101,
        "name": "daily",
        "version": 1,
    }
    assert result.view.to_data(attached_schedule=result.attached_schedule) == {
        "id": 101,
        "code": 101,
        "name": "daily",
        "version": 1,
        "projectCode": 7,
        "description": None,
        "globalParams": None,
        "globalParamMap": None,
        "createTime": None,
        "updateTime": None,
        "userId": 1,
        "userName": "alice",
        "projectName": "etl",
        "timeout": 0,
        "releaseState": "ONLINE",
        "scheduleReleaseState": "ONLINE",
        "executionType": None,
        "schedule": {
            "id": 23,
            "startTime": "2026-01-01 00:00:00",
            "endTime": "2026-12-31 23:59:59",
            "timezoneId": "UTC",
            "crontab": "0 0 0 * * ?",
            "failureStrategy": "CONTINUE",
            "workflowInstancePriority": "MEDIUM",
            "releaseState": "ONLINE",
        },
    }


def test_code_workflow_detail_revalidates_requested_code() -> None:
    project = _project(7, "etl", identity_kind="code")
    mismatched = _workflow(
        102,
        project_id=7,
        name="other",
        identity_kind="code",
    )
    wire = FakeDefinitionWire(
        identity_kind="code",
        projects=[],
        project_details={7: project},
        workflow_details={(7, 101): mismatched},
    )

    with pytest.raises(
        NotFoundError,
        match="Workflow code 101 was not found in the selected project",
    ):
        DefinitionReads(wire).get_workflow(
            "7",
            "101",
            schedule_list_supported=False,
        )

    assert wire.workflow_detail_calls == [(7, 101)]
    assert wire.schedule_calls == []


def test_resolve_workflow_returns_scope_without_reading_schedule_state() -> None:
    project = _project(7, "etl", identity_kind="code")
    workflow = _workflow(
        101,
        project_id=7,
        name="daily",
        identity_kind="code",
    )
    wire = FakeDefinitionWire(
        identity_kind="code",
        projects=[],
        project_details={7: project},
        workflow_details={(7, 101): workflow},
    )

    scope = DefinitionReads(wire).resolve_workflow("7", "101")

    assert scope.project == project.ref
    assert scope.workflow == workflow.ref
    assert scope.view == workflow
    assert wire.schedule_calls == []


@dataclass
class FakeDefinitionWire:
    identity_kind: Literal["id", "code"]
    projects: list[ProjectView]
    project_details: dict[int, ProjectView]
    workflow_pages: dict[int, list[WorkflowListView]] = field(default_factory=dict)
    workflow_page_error: Exception | None = None
    fuzzy_workflow_search: bool = False
    workflow_details: dict[tuple[int, int], WorkflowView] = field(default_factory=dict)
    workflow_detail_errors: dict[tuple[int, int], ApiResultError] = field(
        default_factory=dict
    )
    schedules: dict[tuple[int, int], list[ScheduleView]] = field(default_factory=dict)
    project_page_calls: list[tuple[int, int, str | None]] = field(default_factory=list)
    project_detail_calls: list[int] = field(default_factory=list)
    workflow_page_calls: list[tuple[int, int, str | None]] = field(default_factory=list)
    workflow_detail_calls: list[tuple[int, int]] = field(default_factory=list)
    schedule_calls: list[tuple[int, int]] = field(default_factory=list)

    def project_page(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None,
    ) -> WirePage[ProjectView]:
        self.project_page_calls.append((page_no, page_size, search))
        items = self.projects
        if search is not None:
            items = [item for item in items if item.ref.name == search]
        return _page(items, page_no=page_no, page_size=page_size)

    def project_detail(self, native: NativeIdentity) -> ProjectView:
        self.project_detail_calls.append(native.value)
        return self.project_details[native.value]

    def workflow_refs(self, project: ProjectRef) -> list[WorkflowRef]:
        return [item.ref for item in self.workflow_pages.get(project.native.value, [])]

    def workflow_page(
        self,
        project: ProjectRef,
        *,
        page_no: int,
        page_size: int,
        search: str | None,
    ) -> WirePage[WorkflowListView]:
        self.workflow_page_calls.append((page_no, page_size, search))
        if self.workflow_page_error is not None:
            raise self.workflow_page_error
        items = self.workflow_pages.get(project.native.value, [])
        if search is not None:
            items = [
                item
                for item in items
                if (
                    search in (item.ref.name or "")
                    if self.fuzzy_workflow_search
                    else item.ref.name == search
                )
            ]
        return _page(items, page_no=page_no, page_size=page_size)

    def workflow_detail(
        self,
        project: ProjectRef,
        workflow: NativeIdentity,
    ) -> WorkflowView:
        key = (project.native.value, workflow.value)
        self.workflow_detail_calls.append(key)
        error = self.workflow_detail_errors.get(key)
        if error is not None:
            raise error
        return self.workflow_details[key]

    def schedule_page(
        self,
        project: ProjectRef,
        workflow: WorkflowRef,
        *,
        page_no: int,
        page_size: int,
    ) -> WirePage[ScheduleView]:
        key = (project.native.value, workflow.native.value)
        self.schedule_calls.append(key)
        return _page(
            self.schedules.get(key, []),
            page_no=page_no,
            page_size=page_size,
        )


PageItemT = TypeVar("PageItemT")


def _page(
    items: list[PageItemT],
    *,
    page_no: int,
    page_size: int,
) -> WirePage[PageItemT]:
    start = (page_no - 1) * page_size
    selected = items[start : start + page_size]
    total = len(items)
    total_pages = 0 if total == 0 else ((total - 1) // page_size) + 1
    return WirePage(
        totalList=selected,
        total=total,
        totalPage=total_pages,
        pageSize=page_size,
        currentPage=page_no,
        pageNo=page_no,
    )


def _project(
    value: int,
    name: str,
    *,
    identity_kind: Literal["id", "code"] = "id",
) -> ProjectView:
    native = NativeId(value) if identity_kind == "id" else NativeCode(value)
    return ProjectView(
        ref=ProjectRef(native=native, name=name, description=None),
        id=value,
        user_id=1,
        user_name="alice",
        create_time=None,
        update_time=None,
        perm=7,
        definition_count=1,
    )


def _workflow_list(value: int, *, name: str) -> WorkflowListView:
    return WorkflowListView(
        ref=WorkflowRef(native=NativeId(value), name=name, version=1),
        release_state="ONLINE",
        schedule_release_state=None,
        schedule_id=None,
    )


def _workflow(
    value: int,
    *,
    project_id: int,
    name: str,
    identity_kind: Literal["id", "code"] = "id",
) -> WorkflowView:
    identity = NativeId(value) if identity_kind == "id" else NativeCode(value)
    project_identity = (
        NativeId(project_id) if identity_kind == "id" else NativeCode(project_id)
    )
    return WorkflowView(
        ref=WorkflowRef(native=identity, name=name, version=1),
        project_native=project_identity,
        id=value,
        description=None,
        global_params=None,
        global_param_map=None,
        create_time=None,
        update_time=None,
        user_id=1,
        user_name="alice",
        project_name="etl",
        timeout=0,
        release_state="ONLINE",
        execution_type=None,
        include_execution_type=identity_kind == "code",
    )
