from __future__ import annotations

import hashlib
import importlib
import json
from copy import deepcopy
from urllib.parse import parse_qs

import httpx
import pytest

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import ApiTransportError
from dsctl.upstream._compiled_workflow_runtime import WORKFLOW_PROGRAMS
from dsctl.upstream.code_native_reads import CodeNativeReadAdapter
from dsctl.upstream.definition_models import (
    DefinitionPage,
    NativeCode,
    ProjectRef,
    ScheduleView,
    WorkflowListView,
)
from dsctl.upstream.wire import WireContractError
from tests.support import make_profile


def test_process_family_executes_generated_reads_and_projects_canonical_data() -> None:
    profile = make_profile(ds_version="3.2.2")
    requests_seen: list[tuple[str, str, dict[str, list[str]]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        query = parse_qs(request.url.query.decode())
        requests_seen.append((request.method, request.url.path, query))
        path = request.url.path
        if path == "/dolphinscheduler/projects":
            data: object = {
                "totalList": [_project()],
                "total": 1,
                "totalPage": 1,
                "pageSize": 20,
                "currentPage": 1,
            }
        elif path == "/dolphinscheduler/projects/7":
            data = _project()
        elif path.endswith("/process-definition/simple-list"):
            data = [
                {
                    "id": 17,
                    "code": 101,
                    "name": "daily-sync",
                    "projectCode": 7,
                }
            ]
        elif path.endswith("/process-definition/101"):
            data = {
                "processDefinition": _process_definition(),
                "processTaskRelationList": [],
                "taskDefinitionList": [],
            }
        elif path.endswith("/process-definition"):
            data = {
                "totalList": [
                    {
                        **_process_definition(),
                        "scheduleReleaseState": "OFFLINE",
                        "schedule": _process_schedule(),
                    }
                ],
                "total": 1,
                "totalPage": 1,
                "pageSize": 25,
                "currentPage": 2,
            }
        elif path.endswith("/schedules"):
            data = {
                "totalList": [_process_schedule()],
                "total": 1,
                "totalPage": 1,
                "pageSize": 2,
                "currentPage": 1,
            }
        else:  # pragma: no cover - assertion reports unexpected wire drift
            message = f"unexpected request: {request.method} {path}"
            raise AssertionError(message)
        return _success(data)

    adapter = CodeNativeReadAdapter.for_version("3.2.2")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        definitions = adapter.bind_read(
            profile,
            http_client=http_client,
        ).definitions
        projects = definitions.list_projects(
            page_no=1,
            page_size=20,
            search="etl",
            all_pages=False,
        )
        project = definitions.get_project("7")
        refs = definitions.visible_workflow_refs(project.project)
        workflows = definitions.list_workflows(
            "7",
            page_no=2,
            page_size=25,
            search="daily",
            all_pages=False,
        )
        workflow = definitions.get_workflow(
            "7",
            "101",
            schedule_list_supported=True,
        )

    assert adapter.ds_version == "3.2.2"
    assert adapter.version_slug == "ds_3_2_2"
    assert adapter._bindings.compiled.ds_version == "3.2.2"
    assert isinstance(projects, DefinitionPage)
    assert project.project.native.value == 7
    assert [(item.native.value, item.name) for item in refs] == [(101, "daily-sync")]
    workflow_rows = workflows.page.totalList
    assert isinstance(workflow_rows[0], WorkflowListView)
    assert workflow_rows[0].schedule_id == 23
    assert workflow.view.project_native.value == 7
    assert isinstance(workflow.attached_schedule, ScheduleView)
    assert workflow.attached_schedule.workflow_native.value == 101
    assert requests_seen == [
        (
            "GET",
            "/dolphinscheduler/projects",
            {"searchVal": ["etl"], "pageSize": ["20"], "pageNo": ["1"]},
        ),
        ("GET", "/dolphinscheduler/projects/7", {}),
        (
            "GET",
            "/dolphinscheduler/projects/7/process-definition/simple-list",
            {},
        ),
        ("GET", "/dolphinscheduler/projects/7", {}),
        (
            "GET",
            "/dolphinscheduler/projects/7/process-definition",
            {"searchVal": ["daily"], "pageNo": ["2"], "pageSize": ["25"]},
        ),
        ("GET", "/dolphinscheduler/projects/7", {}),
        (
            "GET",
            "/dolphinscheduler/projects/7/process-definition/101",
            {},
        ),
        (
            "GET",
            "/dolphinscheduler/projects/7/schedules",
            {
                "processDefinitionCode": ["101"],
                "pageNo": ["1"],
                "pageSize": ["2"],
            },
        ),
    ]


def test_workflow_family_executes_generated_reads_and_projects_canonical_data() -> None:
    profile = make_profile(ds_version="3.4.2")
    requests_seen: list[tuple[str, str, dict[str, list[str]]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        query = parse_qs(request.url.query.decode())
        requests_seen.append((request.method, request.url.path, query))
        path = request.url.path
        if path == "/dolphinscheduler/projects":
            data: object = {
                "totalList": [_project()],
                "total": 1,
                "totalPage": 1,
                "pageSize": 20,
                "currentPage": 1,
            }
        elif path == "/dolphinscheduler/projects/7":
            data = _project()
        elif path.endswith("/workflow-definition/simple-list"):
            data = [
                {
                    "id": 17,
                    "code": 101,
                    "name": "daily-sync",
                    "projectCode": 7,
                }
            ]
        elif path.endswith("/workflow-definition/101"):
            data = {
                "workflowDefinition": _workflow_definition(),
                "workflowTaskRelationList": [],
                "taskDefinitionList": [],
            }
        elif path.endswith("/workflow-definition"):
            data = {
                "totalList": [
                    {
                        **_workflow_definition(),
                        "scheduleReleaseState": "OFFLINE",
                        "schedule": _workflow_schedule(),
                    }
                ],
                "total": 1,
                "totalPage": 1,
                "pageSize": 25,
                "currentPage": 2,
            }
        elif path.endswith("/schedules"):
            data = {
                "totalList": [_workflow_schedule()],
                "total": 1,
                "totalPage": 1,
                "pageSize": 2,
                "currentPage": 1,
            }
        else:  # pragma: no cover - assertion reports unexpected wire drift
            message = f"unexpected request: {request.method} {path}"
            raise AssertionError(message)
        return _success(data)

    adapter = CodeNativeReadAdapter.for_version("3.4.2")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        definitions = adapter.bind_read(
            profile,
            http_client=http_client,
        ).definitions
        projects = definitions.list_projects(
            page_no=1,
            page_size=20,
            search="etl",
            all_pages=False,
        )
        project = definitions.get_project("7")
        refs = definitions.visible_workflow_refs(project.project)
        workflows = definitions.list_workflows(
            "7",
            page_no=2,
            page_size=25,
            search="daily",
            all_pages=False,
        )
        workflow = definitions.get_workflow(
            "7",
            "101",
            schedule_list_supported=True,
        )

    assert isinstance(projects, DefinitionPage)
    assert project.project.native.value == 7
    assert [(item.native.value, item.name) for item in refs] == [(101, "daily-sync")]
    workflow_rows = workflows.page.totalList
    assert isinstance(workflow_rows[0], WorkflowListView)
    assert workflow_rows[0].schedule_id == 23
    assert workflow.view.project_native.value == 7
    assert isinstance(workflow.attached_schedule, ScheduleView)
    assert workflow.attached_schedule.workflow_native.value == 101
    assert requests_seen == [
        (
            "GET",
            "/dolphinscheduler/projects",
            {"searchVal": ["etl"], "pageSize": ["20"], "pageNo": ["1"]},
        ),
        ("GET", "/dolphinscheduler/projects/7", {}),
        (
            "GET",
            "/dolphinscheduler/projects/7/workflow-definition/simple-list",
            {},
        ),
        ("GET", "/dolphinscheduler/projects/7", {}),
        (
            "GET",
            "/dolphinscheduler/projects/7/workflow-definition",
            {"searchVal": ["daily"], "pageNo": ["2"], "pageSize": ["25"]},
        ),
        ("GET", "/dolphinscheduler/projects/7", {}),
        (
            "GET",
            "/dolphinscheduler/projects/7/workflow-definition/101",
            {},
        ),
        (
            "GET",
            "/dolphinscheduler/projects/7/schedules",
            {
                "workflowDefinitionCode": ["101"],
                "pageNo": ["1"],
                "pageSize": ["2"],
            },
        ),
    ]


def test_exact_version_and_package_mismatches_fail_before_transport() -> None:
    requests_seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(request.url.path)
        return _success({})

    with pytest.raises(
        WireContractError,
        match="require the exact version slug",
    ):
        CodeNativeReadAdapter("3.2.2", version_slug="ds_3_4_2")

    with pytest.raises(
        WireContractError,
        match="no reviewed generated code-identity recipe",
    ):
        CodeNativeReadAdapter.for_version("1.3.9")

    adapter = CodeNativeReadAdapter.for_version("3.2.2")
    mismatched_profile = make_profile(ds_version="3.4.2")
    http_client = DolphinSchedulerClient(
        mismatched_profile,
        transport=httpx.MockTransport(handler),
    )
    with (
        http_client,
        pytest.raises(
            WireContractError,
            match="does not match the selected client profile",
        ),
    ):
        adapter.bind_read(mismatched_profile, http_client=http_client)

    assert requests_seen == []


def test_read_adapter_rejects_an_incomplete_compiled_program_inventory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("dsctl.generated.wire_programs.workflow_runtime")
    profiles = deepcopy(module.PROFILES)
    record = profiles["3.4.2"]
    del record["programs"]["definition_page"]
    encoded = json.dumps(
        {
            "schema_version": 2,
            **{key: value for key, value in record.items() if key != "profile_digest"},
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    record["profile_digest"] = f"sha256:{hashlib.sha256(encoded).hexdigest()}"
    monkeypatch.setattr(module, "PROFILES", profiles)
    monkeypatch.delitem(WORKFLOW_PROGRAMS.__dict__, "_profiles", raising=False)

    with pytest.raises(
        WireContractError,
        match=r"profile 3\.4\.2 programs are incomplete",
    ):
        CodeNativeReadAdapter.for_version("3.4.2")


def test_generated_response_validation_runs_before_canonical_projection() -> None:
    profile = make_profile(ds_version="3.4.2")

    def handler(_request: httpx.Request) -> httpx.Response:
        return _success(
            {
                "totalList": "not-a-list",
                "total": 1,
                "pageSize": 20,
            }
        )

    adapter = CodeNativeReadAdapter.for_version("3.4.2")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client, pytest.raises(ApiTransportError) as captured:
        adapter.bind_read(
            profile,
            http_client=http_client,
        ).definitions.list_projects(
            page_no=1,
            page_size=20,
            search=None,
            all_pages=False,
        )

    assert captured.value.message == (
        "DolphinScheduler response payload did not match the generated API contract."
    )
    assert captured.value.details["validation_error_count"] == 1


def test_canonical_projection_rejects_missing_or_mismatched_code_identity() -> None:
    profile = make_profile(ds_version="3.4.2")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/dolphinscheduler/projects/7":
            return _success({"name": "etl-prod", "perm": 7, "defCount": 1})
        if request.url.path.endswith("/workflow-definition/101"):
            return _success(
                {
                    "workflowDefinition": {
                        **_workflow_definition(),
                        "projectCode": 8,
                    },
                    "workflowTaskRelationList": [],
                    "taskDefinitionList": [],
                }
            )
        message = f"unexpected request: {request.method} {request.url.path}"
        raise AssertionError(message)

    adapter = CodeNativeReadAdapter.for_version("3.4.2")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        definitions = adapter.bind_read(profile, http_client=http_client).definitions
        with pytest.raises(ApiTransportError) as missing:
            definitions.get_project("7")
        with pytest.raises(ApiTransportError) as mismatched:
            definitions.resolve_workflow_by_code(
                ProjectRef(
                    native=NativeCode(7),
                    name="etl-prod",
                    description="daily jobs",
                ),
                101,
            )

    for error in (missing.value, mismatched.value):
        assert error.message == (
            "DolphinScheduler response cannot be projected to the canonical read "
            "contract."
        )
        assert error.suggestion == (
            "Verify DS_VERSION matches the server and retry after checking API health."
        )
        assert error.__cause__ is None

    assert missing.value.details == {
        "ds_version": "3.4.2",
        "resource": "project",
        "field": "code",
        "reason": "identity must be a positive integer",
    }
    assert mismatched.value.details == {
        "ds_version": "3.4.2",
        "resource": "workflow",
        "field": "projectCode",
        "reason": "response identity does not match the requested scope",
    }


def _success(data: object) -> httpx.Response:
    return httpx.Response(200, json={"code": 0, "msg": "success", "data": data})


def _project() -> dict[str, object]:
    return {
        "id": 7,
        "code": 7,
        "name": "etl-prod",
        "description": "daily jobs",
        "perm": 7,
        "defCount": 1,
    }


def _process_definition() -> dict[str, object]:
    return {
        "id": 17,
        "code": 101,
        "name": "daily-sync",
        "version": 4,
        "projectCode": 7,
        "releaseState": "ONLINE",
        "executionType": "PARALLEL",
    }


def _process_schedule() -> dict[str, object]:
    return {
        "id": 23,
        "processDefinitionCode": 101,
        "processDefinitionName": "daily-sync",
        "startTime": "2026-01-01 00:00:00",
        "endTime": "2026-12-31 23:59:59",
        "timezoneId": "UTC",
        "crontab": "0 0 0 * * ?",
        "releaseState": "OFFLINE",
        "processInstancePriority": "HIGH",
    }


def _workflow_definition() -> dict[str, object]:
    return {
        "id": 17,
        "code": 101,
        "name": "daily-sync",
        "version": 4,
        "projectCode": 7,
        "releaseState": "ONLINE",
        "executionType": "PARALLEL",
    }


def _workflow_schedule() -> dict[str, object]:
    return {
        "id": 23,
        "workflowDefinitionCode": 101,
        "workflowDefinitionName": "daily-sync",
        "startTime": "2026-01-01 00:00:00",
        "endTime": "2026-12-31 23:59:59",
        "timezoneId": "UTC",
        "crontab": "0 0 0 * * ?",
        "releaseState": "OFFLINE",
        "workflowInstancePriority": "HIGH",
    }
