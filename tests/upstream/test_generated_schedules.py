from __future__ import annotations

import json
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, cast
from urllib.parse import parse_qs

import httpx
import pytest

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import (
    ApiResultError,
    ApiTransportError,
    ConflictError,
    UserInputError,
)
from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.upstream.definition_models import (
    DefinitionPage,
    NativeCode,
    NativeId,
    ProjectRef,
    ProjectView,
    WorkflowRef,
    WorkflowScope,
    WorkflowView,
)
from dsctl.upstream.protocol import ScheduleCreateSpec
from dsctl.upstream.schedules import (
    ScheduleAdapter,
    ScheduleDomain,
    ScheduleOperations,
    ScheduleSnapshot,
    ScheduleState,
    ScheduleUpdatePatch,
)
from tests.support import make_profile

if TYPE_CHECKING:
    from dsctl.config import ClusterProfile
    from dsctl.upstream.definition_reads import DefinitionReads


_TENANT_VERSIONS = frozenset(
    {
        "3.2.0",
        "3.2.1",
        "3.2.2",
        "3.3.1",
        "3.3.2",
        "3.4.0",
        "3.4.1",
        "3.4.2",
        "3.4.3",
    }
)
_WORKFLOW_VOCABULARY_VERSIONS = frozenset(
    {"3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2", "3.4.3"}
)
_ENTITY_UPDATE_VERSIONS = frozenset(
    {"3.2.2", "3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2", "3.4.3"}
)
_BOOLEAN_LIFECYCLE_VERSIONS = frozenset(
    {"3.2.1", "3.2.2", "3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2", "3.4.3"}
)
_PRE_TENANT_VERSIONS = tuple(
    version
    for version in TARGET_DS_VERSIONS
    if version != "1.3.9" and version not in _TENANT_VERSIONS
)


@pytest.mark.parametrize("ds_version", TARGET_DS_VERSIONS)
def test_schedule_adapter_binds_its_exact_compiled_profile_without_io(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    adapter = ScheduleAdapter.for_version(ds_version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(_unexpected_request),
    )
    with http_client:
        domain = adapter.bind(profile, http_client=http_client)
    assert isinstance(domain, ScheduleDomain)
    assert adapter.ds_version == domain.schedules.ds_version == ds_version


def test_139_schedule_create_plan_uses_name_route_and_omits_timezone() -> None:
    profile = make_profile(ds_version="1.3.9")
    adapter = ScheduleAdapter.for_version("1.3.9")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(_unexpected_request),
    )
    with http_client:
        operations = _operations(adapter, profile, http_client)
        request = operations.plan_create(
            spec=ScheduleCreateSpec(
                project_code=7,
                workflow_code="<created_workflow_id>",
                crontab="0 0 2 * * ?",
                start_time="2026-08-06 00:00:00",
                end_time="2027-08-06 00:00:00",
                timezone_id=None,
                project_name="etl-prod",
            )
        )
        release_request = operations.plan_release(
            _Definitions.for_version("1.3.9").project,
            schedule_id="<created_schedule_id>",
            state="ONLINE",
        )

    assert request["path"] == "/projects/etl-prod/schedule/create"
    assert request["form"]["processDefinitionId"] == "<created_workflow_id>"
    assert json.loads(str(request["form"]["schedule"])) == {
        "startTime": "2026-08-06 00:00:00",
        "endTime": "2027-08-06 00:00:00",
        "crontab": "0 0 2 * * ?",
    }
    assert request["form"]["failureStrategy"] == "CONTINUE"
    assert request["form"]["warningType"] == "NONE"
    assert request["form"]["processInstancePriority"] == "MEDIUM"
    assert request["form"]["workerGroup"] == "default"
    assert release_request == {
        "method": "POST",
        "path": "/projects/etl-prod/schedule/online",
        "form": {"id": "<created_schedule_id>"},
    }


@pytest.mark.parametrize("ds_version", TARGET_DS_VERSIONS)
def test_schedule_domain_executes_the_reviewed_mutation_recipe(  # noqa: C901
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    legacy = ds_version == "1.3.9"
    project_path = "etl-prod" if legacy else "7"
    collection_path = (
        f"/dolphinscheduler/projects/{project_path}/schedule/list-paging"
        if legacy
        else f"/dolphinscheduler/projects/{project_path}/schedules"
    )
    preview_path = (
        f"/dolphinscheduler/projects/{project_path}/schedule/preview"
        if legacy
        else f"{collection_path}/preview"
    )
    current: dict[str, object] | None = None
    mutations_seen: list[
        tuple[str, str, str, dict[str, list[str]], dict[str, list[str]]]
    ] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal current
        query = parse_qs(request.url.query.decode(), keep_blank_values=True)
        form = parse_qs(request.content.decode(), keep_blank_values=True)
        path = request.url.path
        if request.method == "GET" and path.endswith("/project-preference"):
            return _success({"projectCode": 7, "state": 0})
        if request.method == "GET" and path == collection_path:
            return _success(_page([] if current is None else [current]))
        if request.method == "POST" and path == preview_path:
            return _success(["2026-08-06 02:00:00", "2026-08-07 02:00:00"])

        create_path = (
            f"/dolphinscheduler/projects/{project_path}/schedule/create"
            if legacy
            else collection_path
        )
        if request.method == "POST" and path == create_path:
            mutations_seen.append(("create", request.method, path, query, form))
            current = _schedule_from_form(ds_version, form, release_state="OFFLINE")
            return _success(None if legacy else current)

        update_path = (
            f"/dolphinscheduler/projects/{project_path}/schedule/update"
            if legacy
            else f"{collection_path}/8"
        )
        update_method = "POST" if legacy else "PUT"
        if request.method == update_method and path == update_path:
            mutations_seen.append(("update", request.method, path, query, form))
            current = _schedule_from_form(ds_version, form, release_state="OFFLINE")
            result = current if ds_version in _ENTITY_UPDATE_VERSIONS else None
            return _success(result)

        online_path = (
            f"/dolphinscheduler/projects/{project_path}/schedule/online"
            if legacy
            else f"{collection_path}/8/online"
        )
        if request.method == "POST" and path == online_path:
            mutations_seen.append(("online", request.method, path, query, form))
            assert current is not None
            current["releaseState"] = "ONLINE"
            return _success(ds_version in _BOOLEAN_LIFECYCLE_VERSIONS or None)

        offline_path = (
            f"/dolphinscheduler/projects/{project_path}/schedule/offline"
            if legacy
            else f"{collection_path}/8/offline"
        )
        if request.method == "POST" and path == offline_path:
            mutations_seen.append(("offline", request.method, path, query, form))
            assert current is not None
            current["releaseState"] = "OFFLINE"
            return _success(ds_version in _BOOLEAN_LIFECYCLE_VERSIONS or None)

        delete_path = (
            f"/dolphinscheduler/projects/{project_path}/schedule/delete"
            if legacy
            else f"{collection_path}/8"
        )
        delete_method = "GET" if legacy else "DELETE"
        if request.method == delete_method and path == delete_path:
            mutations_seen.append(("delete", request.method, path, query, form))
            current = None
            return _success(None)

        message = f"unexpected request {request.method} {path}"
        raise AssertionError(message)

    adapter = ScheduleAdapter.for_version(ds_version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        schedules = _operations(adapter, profile, http_client)
        state = _state(ds_version)
        prepared_create = schedules.prepare_create("etl-prod", "daily-etl", state)
        created = schedules.create(prepared_create)
        prepared_update = schedules.prepare_update(
            created.id,
            ScheduleUpdatePatch(crontab="0 30 2 * * ?"),
        )
        updated = schedules.update(prepared_update)
        online = schedules.online(updated.id)
        offline = schedules.offline(online.id)
        deleted_schedule, deleted = schedules.delete(offline.id)

    assert created.id == 8
    assert created.workflow_native.value == 101
    assert created.crontab == "0 0 2 * * ?"
    assert updated.crontab == "0 30 2 * * ?"
    assert online.release_state == "ONLINE"
    assert offline.release_state == "OFFLINE"
    assert deleted_schedule.id == 8
    assert deleted is True
    assert [operation for operation, *_rest in mutations_seen] == [
        "create",
        "update",
        "online",
        "offline",
        "delete",
    ]

    create_form = mutations_seen[0][4]
    update_form = mutations_seen[1][4]
    workflow_field = _workflow_field(ds_version)
    priority_field = _priority_field(ds_version)
    assert create_form[workflow_field] == ["101"]
    assert priority_field in create_form
    assert workflow_field not in update_form
    assert json.loads(create_form["schedule"][0]) == _schedule_expression(ds_version)
    assert json.loads(update_form["schedule"][0])["crontab"] == "0 30 2 * * ?"
    if legacy:
        assert update_form["id"] == ["8"]
        assert "scheduleId" not in update_form
        assert create_form["receivers"] == ["ops@example.test"]
        assert create_form["receiversCc"] == ["cc@example.test"]
        assert mutations_seen[4][1:4] == (
            "GET",
            f"/dolphinscheduler/projects/{project_path}/schedule/delete",
            {"scheduleId": ["8"]},
        )
    else:
        assert create_form["environmentCode"] == ["-1"]
    if ds_version in _TENANT_VERSIONS:
        assert create_form["tenantCode"] == ["tenant-a"]
    else:
        assert "tenantCode" not in create_form


@pytest.mark.parametrize("ds_version", TARGET_DS_VERSIONS)
def test_schedule_warning_update_preserves_calendar_after_start(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    stored = _schedule(ds_version)
    stored.update(
        startTime="2000-01-01 00:00:00",
        warningGroupId=9,
        workerGroup="etl-workers",
        failureStrategy="END",
    )
    stored[_priority_field(ds_version)] = "HIGH"
    before = dict(stored)
    sent: list[dict[str, list[str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return _success(_page([stored]))
        if request.url.path.endswith("/preview"):
            return _success(["2026-09-24 02:00:00"])
        form = parse_qs(request.content.decode(), keep_blank_values=True)
        sent.append(form)
        # Native 3.0.4-3.0.6 reject a nonempty expression with a past start;
        # all 37 controllers require the key, and empty means keep the calendar.
        assert "schedule" in form
        if form["schedule"] != [""]:
            return httpx.Response(200, json={"code": 80004, "msg": "past start"})
        stored["warningType"] = form["warningType"][0]
        return _success(stored if ds_version in _ENTITY_UPDATE_VERSIONS else None)

    client = DolphinSchedulerClient(profile, transport=httpx.MockTransport(handler))
    with client:
        operations = _operations(
            ScheduleAdapter.for_version(ds_version), profile, client
        )
        result = operations.update(
            operations.prepare_update(8, ScheduleUpdatePatch(warning_type="SUCCESS"))
        )

    assert len(sent) == 1
    for field in (
        "warningGroupId",
        "workerGroup",
        "failureStrategy",
        _priority_field(ds_version),
    ):
        assert sent[0][field] == [str(before[field])]
    if ds_version == "1.3.9":
        assert sent[0]["receivers"] == ["ops@example.test"]
        assert sent[0]["receiversCc"] == ["cc@example.test"]
    else:
        assert sent[0]["environmentCode"] == ["-1"]
    if ds_version in _TENANT_VERSIONS:
        assert sent[0]["tenantCode"] == ["tenant-a"]
    assert stored == {**before, "warningType": "SUCCESS"}
    assert result.warning_type == "SUCCESS"
    assert result.start_time == before["startTime"]


@pytest.mark.parametrize(
    ("patch", "field", "value"),
    [
        (
            ScheduleUpdatePatch(start_time="2026-09-25 00:00:00"),
            "startTime",
            "2026-09-25 00:00:00",
        ),
        (
            ScheduleUpdatePatch(end_time="2027-09-25 00:00:00"),
            "endTime",
            "2027-09-25 00:00:00",
        ),
        (ScheduleUpdatePatch(timezone_id="UTC"), "timezoneId", "UTC"),
    ],
)
def test_schedule_calendar_update_sends_merged_expression(
    patch: ScheduleUpdatePatch,
    field: str,
    value: str,
) -> None:
    profile = make_profile(ds_version="3.0.4")
    stored = _schedule("3.0.4")
    sent: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal stored
        if request.method == "GET":
            return _success(_page([stored]))
        if request.url.path.endswith("/preview"):
            return _success(["2026-09-25 02:00:00"])
        form = parse_qs(request.content.decode(), keep_blank_values=True)
        sent.append(json.loads(form["schedule"][0]))
        stored = _schedule_from_form("3.0.4", form, release_state="OFFLINE")
        return _success(None)

    client = DolphinSchedulerClient(profile, transport=httpx.MockTransport(handler))
    with client:
        operations = _operations(ScheduleAdapter.for_version("3.0.4"), profile, client)
        result = operations.update(operations.prepare_update(8, patch))
    assert sent == [{**_schedule_expression("3.0.4"), field: value}]
    assert result.to_data()[field] == value


def test_schedule_legacy_timezone_absence_is_a_zero_request_rejection() -> None:
    profile = make_profile(ds_version="1.3.9")
    requests_seen = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal requests_seen
        requests_seen += 1
        return _success(None)

    adapter = ScheduleAdapter.for_version("1.3.9")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client, pytest.raises(UserInputError) as exc_info:
        _operations(adapter, profile, http_client).prepare_create(
            "etl-prod",
            "daily-etl",
            replace(
                _state("1.3.9"),
                timezone_id="Asia/Shanghai",
                provided_fields=frozenset({"timezoneId"}),
            ),
        )

    assert requests_seen == 0
    assert exc_info.value.details == {
        "ds_version": "1.3.9",
        "resource": "schedule",
        "field": "timezoneId",
        "reason": "upstream_field_absent",
    }


@pytest.mark.parametrize("ds_version", _PRE_TENANT_VERSIONS)
def test_schedule_pre_tenant_profiles_reject_tenant_without_a_request(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    requests_seen = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal requests_seen
        requests_seen += 1
        return _success(None)

    adapter = ScheduleAdapter.for_version(ds_version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client, pytest.raises(UserInputError) as exc_info:
        _operations(adapter, profile, http_client).prepare_create(
            "etl-prod",
            "daily-etl",
            replace(
                _state(ds_version),
                tenant_code="tenant-a",
                provided_fields=frozenset({"tenantCode", "timezoneId"}),
            ),
        )

    assert requests_seen == 0
    assert exc_info.value.details["field"] == "tenantCode"
    assert exc_info.value.details["reason"] == "upstream_field_absent"


def test_schedule_legacy_get_delete_transport_failure_is_not_retried() -> None:
    profile = make_profile(ds_version="1.3.9")
    delete_requests = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal delete_requests
        if request.url.path.endswith("/schedule/list-paging"):
            return _success(_page([_schedule("1.3.9")]))
        if request.url.path.endswith("/schedule/delete"):
            delete_requests += 1
            return httpx.Response(503, json={"message": "unavailable"})
        message = f"unexpected request {request.method} {request.url.path}"
        raise AssertionError(message)

    adapter = ScheduleAdapter.for_version("1.3.9")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client, pytest.raises(ApiTransportError) as exc_info:
        _operations(adapter, profile, http_client).delete(8)

    assert delete_requests == 1
    assert exc_info.value.details["mutation_may_have_applied"] is True
    assert "do not blindly repeat" in (exc_info.value.suggestion or "")


def test_schedule_legacy_safe_get_keeps_transport_retries() -> None:
    profile = make_profile(ds_version="1.3.9").model_copy(
        update={"api_retry_attempts": 2, "api_retry_backoff_ms": 0}
    )
    list_requests = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal list_requests
        if request.url.path.endswith("/schedule/list-paging"):
            list_requests += 1
            if list_requests == 1:
                return httpx.Response(503, json={"message": "unavailable"})
            return _success(_page([_schedule("1.3.9")]))
        message = f"unexpected request {request.method} {request.url.path}"
        raise AssertionError(message)

    adapter = ScheduleAdapter.for_version("1.3.9")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        listing = _operations(adapter, profile, http_client).list(
            "etl-prod",
            workflow_selector=None,
            search=None,
            page_no=1,
            page_size=20,
            all_pages=False,
        )

    assert list_requests == 2
    assert listing.page.total == 1


@pytest.mark.parametrize("ds_version", TARGET_DS_VERSIONS)
def test_unscoped_schedule_reads_use_the_exact_controller_filter_policy(
    ds_version: str,
) -> None:
    filters = []

    def handler(request: httpx.Request) -> httpx.Response:
        workflow_id = int(request.url.params[_workflow_field(ds_version)])
        filters.append(workflow_id)
        # Source querySchedule resolves every workflow value before3.2; zero
        # therefore returns PROCESS_DEFINE_NOT_EXIST rather than a global page.
        if ds_version not in _TENANT_VERSIONS and workflow_id == 0:
            return httpx.Response(
                200,
                json={
                    "code": 50003,
                    "msg": "definition 0 does not exist",
                    "data": None,
                },
            )
        return _success(_page([_schedule(ds_version)]))

    profile = make_profile(ds_version=ds_version)
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        schedules = _operations(
            ScheduleAdapter.for_version(ds_version), profile, client
        )
        assert schedules.get(8).schedule.id == 8
        listing = schedules.list(
            "etl-prod",
            workflow_selector=None,
            search=None,
            page_no=1,
            page_size=20,
            all_pages=False,
        )
    assert listing.page.total == 1
    assert filters == ([0, 0] if ds_version in _TENANT_VERSIONS else [101, 101])


@pytest.mark.parametrize("ds_version", ["3.2.0", "3.4.1"])
def test_project_scoped_schedule_get_skips_visible_project_inventory(
    ds_version: str,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _success(_page([_schedule(ds_version)]))

    profile = make_profile(ds_version=ds_version)
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        schedules = replace(
            _operations(ScheduleAdapter.for_version(ds_version), profile, client),
            definitions=cast(
                "DefinitionReads",
                _InventoryForbiddenDefinitions.for_version(ds_version),
            ),
        )
        located = schedules.get(8, project_selector="etl-prod")

    assert located.project.native.value == 7
    assert located.schedule.id == 8
    assert [(request.method, request.url.path) for request in requests] == [
        ("GET", "/dolphinscheduler/projects/7/schedules")
    ]


@pytest.mark.parametrize("ds_version", ["1.3.9", "2.0.9", "3.1.9"])
def test_scoped_schedule_enumeration_finds_ids_and_pages_combined_results(
    ds_version: str,
) -> None:
    filters = []

    def handler(request: httpx.Request) -> httpx.Response:
        workflow_id = int(request.url.params[_workflow_field(ds_version)])
        filters.append(workflow_id)
        assert workflow_id in {101, 202}
        row = _schedule(ds_version)
        row.update(
            {
                "id": 9 if workflow_id == 101 else 8,
                _workflow_field(ds_version): workflow_id,
            }
        )
        return _success(_page([row]))

    profile = make_profile(ds_version=ds_version)
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        schedules = replace(
            _operations(ScheduleAdapter.for_version(ds_version), profile, client),
            definitions=cast(
                "DefinitionReads", _TwoWorkflowDefinitions.for_version(ds_version)
            ),
        )
        located = schedules.get(8)
        page = schedules.list(
            "etl-prod",
            workflow_selector=None,
            search=None,
            page_no=2,
            page_size=1,
            all_pages=False,
        ).page
        all_items = schedules.list(
            "etl-prod",
            workflow_selector=None,
            search=None,
            page_no=1,
            page_size=1,
            all_pages=True,
        ).page
    assert located.schedule.workflow_native.value == 202
    assert [item.id for item in page.totalList or ()] == [9]
    assert (page.total, page.totalPage, page.pageNo) == (2, 2, 2)
    assert [item.id for item in all_items.totalList or ()] == [8, 9]
    assert filters == [101, 202, 101, 202, 101, 202]


@pytest.mark.parametrize("code", [50003, 30001])
def test_scoped_schedule_lookup_preserves_real_definition_and_permission_errors(
    code: int,
) -> None:
    filters = []

    def handler(request: httpx.Request) -> httpx.Response:
        workflow_id = int(request.url.params["processDefinitionId"])
        filters.append(workflow_id)
        if workflow_id == 202:
            return httpx.Response(
                200, json={"code": code, "msg": "scoped read failed", "data": None}
            )
        return _success(_page([]))

    profile = make_profile(ds_version="1.3.9")
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        schedules = replace(
            _operations(ScheduleAdapter.for_version("1.3.9"), profile, client),
            definitions=cast(
                "DefinitionReads", _TwoWorkflowDefinitions.for_version("1.3.9")
            ),
        )
        with pytest.raises(ApiResultError) as caught:
            schedules.get(8)
    assert caught.value.result_code == code
    assert filters == [101, 202]


def test_schedule_create_readback_mismatch_requires_reconciliation() -> None:
    profile = make_profile(ds_version="3.4.2")
    created = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal created
        if request.url.path.endswith("/project-preference"):
            return _success({"projectCode": 7, "state": 0})
        if request.method == "GET" and request.url.path.endswith("/schedules"):
            return _success(
                _page([_schedule("3.4.2", crontab="wrong")] if created else [])
            )
        if request.url.path.endswith("/schedules/preview"):
            return _success(["2026-08-06 02:00:00"])
        if request.method == "POST" and request.url.path.endswith("/schedules"):
            created = True
            return _success(_schedule("3.4.2"))
        message = f"unexpected request {request.method} {request.url.path}"
        raise AssertionError(message)

    adapter = ScheduleAdapter.for_version("3.4.2")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        schedules = _operations(adapter, profile, http_client)
        prepared = schedules.prepare_create("etl-prod", "daily-etl", _state("3.4.2"))
        with pytest.raises(ApiTransportError) as exc_info:
            schedules.create(prepared)

    assert exc_info.value.details["phase"] == "readback"
    assert exc_info.value.details["mutation_applied"] is True
    assert "do not blindly repeat" in (exc_info.value.suggestion or "")


def test_139_schedule_create_readback_matches_explicit_offset_instants() -> None:
    created = _create_139_with_date_readback(
        expected_start="2026-09-20T08:15:30+08:00",
        actual_start="2026-09-20T00:15:30Z",
    )

    assert created.start_time == "2026-09-20T00:15:30Z"


@pytest.mark.parametrize(
    "actual_start",
    [
        "2026-09-20T08:15:30Z",
        "2026-09-20T00:15:30.000001Z",
        "2026-09-20 08:15:30",
        "not-a-date",
        "0001-01-01T00:00:00+23:59",
    ],
    ids=[
        "shifted-instant",
        "microsecond-shift",
        "mixed-naive-aware",
        "invalid-actual",
        "utc-conversion-underflow",
    ],
)
def test_139_schedule_create_readback_rejects_unverifiable_dates(
    actual_start: str,
) -> None:
    with pytest.raises(ApiTransportError) as exc_info:
        _create_139_with_date_readback(
            expected_start="2026-09-20T08:15:30+08:00",
            actual_start=actual_start,
        )

    error = exc_info.value
    assert error.details["mismatches"] == {
        "startTime": {
            "expected": "2026-09-20T08:15:30+08:00",
            "actual": actual_start,
        }
    }
    assert error.suggestion is not None
    assert error.suggestion.startswith(
        "Inspect the schedule before deciding whether to retry schedule create; "
        "do not blindly repeat the mutation."
    )
    assert "dsctl schedule get 8" in error.suggestion
    assert "explicit UTC offsets" in error.suggestion


def test_schedule_update_explicit_environment_uses_action_specific_preflight() -> None:
    profile = make_profile(ds_version="3.4.2")
    requests_seen: list[tuple[str, str, dict[str, list[str]]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        query = parse_qs(request.url.query.decode())
        requests_seen.append((request.method, request.url.path, query))
        if request.url.path.endswith("/schedules"):
            return _success(_page([_schedule("3.4.2")]))
        if request.url.path == "/dolphinscheduler/environment/query-by-code":
            return httpx.Response(
                200,
                json={
                    "code": 1200009,
                    "msg": "not found environment code [404]",
                    "data": None,
                },
            )
        message = f"unexpected request {request.method} {request.url.path}"
        raise AssertionError(message)

    adapter = ScheduleAdapter.for_version("3.4.2")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client, pytest.raises(ApiResultError) as exc_info:
        _operations(adapter, profile, http_client).prepare_update(
            8,
            ScheduleUpdatePatch(environment_code=404),
        )

    assert exc_info.value.result_code == 1200009
    assert requests_seen == [
        (
            "GET",
            "/dolphinscheduler/projects/7/schedules",
            {
                "workflowDefinitionCode": ["0"],
                "pageNo": ["1"],
                "pageSize": ["100"],
            },
        ),
        (
            "GET",
            "/dolphinscheduler/environment/query-by-code",
            {"environmentCode": ["404"]},
        ),
    ]


def test_schedule_create_explicit_environment_does_not_add_a_preflight() -> None:
    profile = make_profile(ds_version="3.4.2")
    paths_seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths_seen.append(request.url.path)
        if request.url.path.endswith("/project-preference"):
            return _success({"projectCode": 7, "state": 0})
        if request.method == "GET" and request.url.path.endswith("/schedules"):
            return _success(_page([]))
        if request.url.path.endswith("/schedules/preview"):
            return _success(["2026-08-06 02:00:00"])
        message = f"unexpected request {request.method} {request.url.path}"
        raise AssertionError(message)

    adapter = ScheduleAdapter.for_version("3.4.2")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        prepared = _operations(adapter, profile, http_client).prepare_create(
            "etl-prod",
            "daily-etl",
            replace(
                _state("3.4.2"),
                environment_code=404,
                provided_fields=frozenset(
                    {"timezoneId", "tenantCode", "environmentCode"}
                ),
            ),
        )

    assert prepared.state.environment_code == 404
    assert "/dolphinscheduler/environment/query-by-code" not in paths_seen


def test_schedule_create_uses_enabled_project_preference_defaults() -> None:
    profile = make_profile(ds_version="3.4.2")
    paths_seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths_seen.append(request.url.path)
        if request.url.path.endswith("/project-preference"):
            return _success(
                {
                    "projectCode": 7,
                    "state": 1,
                    "preferences": json.dumps(
                        {
                            "taskPriority": "HIGH",
                            "warningType": "ALL",
                            "workerGroup": "gpu",
                            "tenant": "tenant-pref",
                            "environmentCode": 99,
                            "alertGroups": 7,
                        }
                    ),
                }
            )
        if request.method == "GET" and request.url.path.endswith("/schedules"):
            return _success(_page([]))
        if request.url.path.endswith("/schedules/preview"):
            return _success(["2026-08-06 02:00:00"])
        message = f"unexpected request {request.method} {request.url.path}"
        raise AssertionError(message)

    adapter = ScheduleAdapter.for_version("3.4.2")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        prepared = _operations(adapter, profile, http_client).prepare_create(
            "etl-prod",
            "daily-etl",
            replace(
                _state("3.4.2"),
                tenant_code=None,
                provided_fields=frozenset({"timezoneId"}),
            ),
        )

    assert prepared.state.warning_type == "ALL"
    assert prepared.state.warning_group_id == 7
    assert prepared.state.workflow_instance_priority == "HIGH"
    assert prepared.state.worker_group == "gpu"
    assert prepared.state.tenant_code == "tenant-pref"
    assert prepared.state.environment_code == 99
    assert prepared.tenant_source == "project_preference"
    assert prepared.project_preference_used_fields == (
        "warningType",
        "warningGroupId",
        "workflowInstancePriority",
        "workerGroup",
        "environmentCode",
        "tenantCode",
    )
    assert "/dolphinscheduler/users/get-user-info" not in paths_seen


@pytest.mark.parametrize(
    ("ds_version", "definition_path"),
    [("3.2.0", "process-definition"), ("3.4.1", "workflow-definition")],
)
@pytest.mark.parametrize(
    ("preference", "worker_group", "used_fields"),
    [
        (
            {"state": 1, "preferences": '{"workerGroup":"gpu","tenant":"ignored"}'},
            "gpu",
            ("workerGroup",),
        ),
        ({"state": 0, "preferences": "invalid JSON is ignored"}, "default", ()),
        ({"state": 2, "preferences": "invalid JSON is ignored"}, "default", ()),
        ({"state": 1, "preferences": None}, "default", ()),
    ],
)
def test_bound_schedule_reads_preference_before_inventory_and_preview(
    ds_version: str,
    definition_path: str,
    preference: dict[str, object],
    worker_group: str,
    used_fields: tuple[str, ...],
) -> None:
    profile = make_profile(ds_version=ds_version)
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _schedule_preference_response(
            request, ds_version=ds_version, preference=preference
        )

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        schedules = (
            ScheduleAdapter.for_version(ds_version)
            .bind(profile, http_client=client)
            .schedules
        )
        prepared = schedules.prepare_create("7", "101", _state(ds_version))

    assert prepared.state.worker_group == worker_group
    assert prepared.state.tenant_code == "tenant-a"
    assert prepared.tenant_source == "flag"
    assert prepared.project_preference_used_fields == used_fields
    assert prepared.preview_times == ("2026-08-06 02:00:00",)
    assert [(request.method, request.url.path) for request in requests] == [
        ("GET", "/dolphinscheduler/projects/7"),
        ("GET", f"/dolphinscheduler/projects/7/{definition_path}/101"),
        ("GET", "/dolphinscheduler/projects/7/project-preference"),
        ("GET", "/dolphinscheduler/projects/7/schedules"),
        ("POST", "/dolphinscheduler/projects/7/schedules/preview"),
    ]
    assert requests[2].url.query == b""
    assert requests[2].content == b""


@pytest.mark.parametrize(
    ("ds_version", "definition_path"),
    [("3.2.0", "process-definition"), ("3.4.1", "workflow-definition")],
)
def test_bound_schedule_rejects_non_null_preference_without_state(
    ds_version: str,
    definition_path: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _schedule_preference_response(
            request, ds_version=ds_version, preference={}
        )

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        schedules = (
            ScheduleAdapter.for_version(ds_version)
            .bind(profile, http_client=client)
            .schedules
        )
        with pytest.raises(ApiTransportError) as caught:
            schedules.prepare_create("7", "101", _state(ds_version))

    assert caught.value.details["resource"] == "project_preference"
    assert caught.value.details["field"] == "state"
    assert [(request.method, request.url.path) for request in requests] == [
        ("GET", "/dolphinscheduler/projects/7"),
        ("GET", f"/dolphinscheduler/projects/7/{definition_path}/101"),
        ("GET", "/dolphinscheduler/projects/7/project-preference"),
    ]


@pytest.mark.parametrize(
    ("ds_version", "definition_path"),
    [("3.2.0", "process-definition"), ("3.4.1", "workflow-definition")],
)
def test_bound_schedule_null_preference_row_uses_schedule_defaults(
    ds_version: str,
    definition_path: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _schedule_preference_response(
            request, ds_version=ds_version, preference=None
        )

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        schedules = (
            ScheduleAdapter.for_version(ds_version)
            .bind(profile, http_client=client)
            .schedules
        )
        prepared = schedules.prepare_create("7", "101", _state(ds_version))

    assert prepared.state.worker_group == "default"
    assert prepared.state.warning_type == "NONE"
    assert prepared.project_preference_used_fields == ()
    assert [(request.method, request.url.path) for request in requests] == [
        ("GET", "/dolphinscheduler/projects/7"),
        ("GET", f"/dolphinscheduler/projects/7/{definition_path}/101"),
        ("GET", "/dolphinscheduler/projects/7/project-preference"),
        ("GET", "/dolphinscheduler/projects/7/schedules"),
        ("POST", "/dolphinscheduler/projects/7/schedules/preview"),
    ]


@pytest.mark.parametrize(
    ("ds_version", "definition_path"),
    [("3.2.0", "process-definition"), ("3.4.1", "workflow-definition")],
)
@pytest.mark.parametrize(
    ("text", "message"),
    [
        ("{", "Stored project preference must be valid JSON"),
        ("[]", "Stored project preference must be one JSON object"),
        ("null", "Stored project preference must be one JSON object"),
    ],
)
def test_bound_schedule_bad_preference_json_stops_before_inventory_or_preview(
    ds_version: str,
    definition_path: str,
    text: str,
    message: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _schedule_preference_response(
            request,
            ds_version=ds_version,
            preference={"state": 1, "preferences": text},
        )

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        schedules = (
            ScheduleAdapter.for_version(ds_version)
            .bind(profile, http_client=client)
            .schedules
        )
        with pytest.raises(ConflictError) as caught:
            schedules.prepare_create("7", "101", _state(ds_version))

    assert caught.value.message == message
    assert caught.value.details == {"projectCode": 7}
    if text == "{":
        assert (
            caught.value.suggestion
            == "Fix the remote project preference before retrying."
        )
    assert [(request.method, request.url.path) for request in requests] == [
        ("GET", "/dolphinscheduler/projects/7"),
        ("GET", f"/dolphinscheduler/projects/7/{definition_path}/101"),
        ("GET", "/dolphinscheduler/projects/7/project-preference"),
    ]


def _schedule_preference_response(
    request: httpx.Request,
    *,
    ds_version: str,
    preference: object,
) -> httpx.Response:
    if request.url.path == "/dolphinscheduler/projects/7":
        return _success({"id": 7, "code": 7, "name": "etl-prod"})
    definition_path = (
        "process-definition" if ds_version == "3.2.0" else "workflow-definition"
    )
    if request.url.path == f"/dolphinscheduler/projects/7/{definition_path}/101":
        definition_key = (
            "processDefinition" if ds_version == "3.2.0" else "workflowDefinition"
        )
        return _success(
            {
                definition_key: {
                    "id": 17,
                    "code": 101,
                    "name": "daily-etl",
                    "version": 1,
                    "projectCode": 7,
                    "releaseState": "ONLINE",
                    "executionType": "PARALLEL",
                },
                "processTaskRelationList": [],
                "workflowTaskRelationList": [],
                "taskDefinitionList": [],
            }
        )
    if request.url.path.endswith("/project-preference"):
        return _success(preference)
    if request.method == "GET" and request.url.path.endswith("/schedules"):
        return _success(_page([]))
    if request.url.path.endswith("/schedules/preview"):
        return _success(["2026-08-06 02:00:00"])
    return _unexpected_request(request)


@dataclass(frozen=True)
class _Definitions:
    project: ProjectRef
    project_view: ProjectView
    workflow: WorkflowRef
    workflow_view: WorkflowView

    @classmethod
    def for_version(cls, ds_version: str) -> _Definitions:
        native_type = NativeId if ds_version == "1.3.9" else NativeCode
        project = ProjectRef(
            native=native_type(7),
            name="etl-prod",
            description="test project",
        )
        workflow = WorkflowRef(
            native=native_type(101),
            name="daily-etl",
            version=1,
        )
        return cls(
            project=project,
            project_view=ProjectView(
                ref=project,
                id=7,
                user_id=1,
                user_name="admin",
                create_time=None,
                update_time=None,
                perm=7,
                definition_count=1,
            ),
            workflow=workflow,
            workflow_view=WorkflowView(
                ref=workflow,
                project_native=project.native,
                id=101,
                description="daily test workflow",
                global_params="[]",
                global_param_map={},
                create_time=None,
                update_time=None,
                user_id=1,
                user_name="admin",
                project_name="etl-prod",
                timeout=0,
                release_state="ONLINE",
                execution_type=None,
                include_execution_type=False,
                receivers="ops@example.test",
                receivers_cc="cc@example.test",
            ),
        )

    def resolve_project(self, selector: str) -> ProjectRef:
        assert selector in {"7", "etl-prod"}
        return self.project

    def resolve_workflow(
        self,
        project_selector: str,
        workflow_selector: str,
    ) -> WorkflowScope:
        assert project_selector in {"7", "etl-prod"}
        assert workflow_selector in {"101", "daily-etl"}
        return WorkflowScope(
            project=self.project,
            workflow=self.workflow,
            view=self.workflow_view,
        )

    def visible_workflow_refs(self, project: ProjectRef) -> tuple[WorkflowRef, ...]:
        assert project == self.project
        return (self.workflow,)

    def visible_project_refs(self) -> tuple[ProjectRef, ...]:
        return (self.project,)

    def list_projects(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None,
        all_pages: bool,
    ) -> DefinitionPage[ProjectView]:
        assert page_no == 1
        assert page_size == 100
        assert search is None
        assert all_pages is True
        return DefinitionPage(
            totalList=(self.project_view,),
            total=1,
            totalPage=1,
            pageSize=page_size,
            currentPage=page_no,
            pageNo=page_no,
        )


class _TwoWorkflowDefinitions(_Definitions):
    def visible_workflow_refs(self, project: ProjectRef) -> tuple[WorkflowRef, ...]:
        assert project == self.project
        return (
            self.workflow,
            replace(
                self.workflow, native=type(self.workflow.native)(202), name="second"
            ),
        )


class _InventoryForbiddenDefinitions(_Definitions):
    def visible_project_refs(self) -> tuple[ProjectRef, ...]:
        message = "project-scoped lookup must not read the visible project inventory"
        raise AssertionError(message)


def _operations(
    adapter: ScheduleAdapter,
    profile: ClusterProfile,
    http_client: DolphinSchedulerClient,
) -> ScheduleOperations:
    domain = adapter.bind(profile, http_client=http_client)
    assert isinstance(domain, ScheduleDomain)
    return replace(
        domain.schedules,
        definitions=cast(
            "DefinitionReads", _Definitions.for_version(adapter.ds_version)
        ),
    )


def _create_139_with_date_readback(
    *,
    expected_start: str,
    actual_start: str,
) -> ScheduleSnapshot:
    profile = make_profile(ds_version="1.3.9")
    created = False
    actual = _schedule("1.3.9")
    actual["startTime"] = actual_start
    actual["endTime"] = "2027-08-05T16:00:00Z"

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal created
        if request.method == "GET" and request.url.path.endswith(
            "/schedule/list-paging"
        ):
            return _success(_page([actual] if created else []))
        if request.url.path.endswith("/schedule/preview"):
            return _success(["2026-09-21 02:00:00"])
        if request.method == "POST" and request.url.path.endswith("/schedule/create"):
            created = True
            return _success(None)
        return _unexpected_request(request)

    adapter = ScheduleAdapter.for_version("1.3.9")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    state = replace(
        _state("1.3.9"),
        start_time=expected_start,
        end_time="2027-08-06T00:00:00+08:00",
    )
    with http_client:
        schedules = _operations(adapter, profile, http_client)
        return schedules.create(
            schedules.prepare_create("etl-prod", "daily-etl", state)
        )


def _state(ds_version: str) -> ScheduleState:
    modern = ds_version != "1.3.9"
    tenant = ds_version in _TENANT_VERSIONS
    provided = {"timezoneId"} if modern else set()
    if tenant:
        provided.add("tenantCode")
    return ScheduleState(
        crontab="0 0 2 * * ?",
        start_time="2026-08-06 00:00:00",
        end_time="2027-08-06 00:00:00",
        timezone_id="Asia/Shanghai" if modern else None,
        tenant_code="tenant-a" if tenant else None,
        provided_fields=frozenset(provided),
    )


def _workflow_field(ds_version: str) -> str:
    if ds_version == "1.3.9":
        return "processDefinitionId"
    if ds_version in _WORKFLOW_VOCABULARY_VERSIONS:
        return "workflowDefinitionCode"
    return "processDefinitionCode"


def _workflow_name_field(ds_version: str) -> str:
    if ds_version in _WORKFLOW_VOCABULARY_VERSIONS:
        return "workflowDefinitionName"
    return "processDefinitionName"


def _priority_field(ds_version: str) -> str:
    if ds_version in _WORKFLOW_VOCABULARY_VERSIONS:
        return "workflowInstancePriority"
    return "processInstancePriority"


def _schedule_expression(ds_version: str) -> dict[str, str]:
    expression = {
        "startTime": "2026-08-06 00:00:00",
        "endTime": "2027-08-06 00:00:00",
        "crontab": "0 0 2 * * ?",
    }
    if ds_version != "1.3.9":
        expression["timezoneId"] = "Asia/Shanghai"
    if ds_version == "3.4.3":
        expression["missedFirePolicy"] = "FIRE_ALL_MISSED"
    return expression


def _schedule_from_form(
    ds_version: str,
    form: dict[str, list[str]],
    *,
    release_state: str,
) -> dict[str, object]:
    expression = json.loads(form["schedule"][0])
    assert isinstance(expression, dict)
    payload = _schedule(
        ds_version,
        crontab=str(expression["crontab"]),
        release_state=release_state,
    )
    payload["startTime"] = expression["startTime"]
    payload["endTime"] = expression["endTime"]
    payload["failureStrategy"] = form["failureStrategy"][0]
    payload["warningType"] = form["warningType"][0]
    payload["warningGroupId"] = int(form["warningGroupId"][0])
    payload[_priority_field(ds_version)] = form[_priority_field(ds_version)][0]
    payload["workerGroup"] = form["workerGroup"][0]
    if ds_version != "1.3.9":
        payload["timezoneId"] = expression["timezoneId"]
        environment_code = int(form["environmentCode"][0])
        payload["environmentCode"] = environment_code
    if ds_version == "3.4.3":
        payload["missedFirePolicy"] = expression.get(
            "missedFirePolicy", "FIRE_ALL_MISSED"
        )
    if ds_version in _TENANT_VERSIONS:
        payload["tenantCode"] = form["tenantCode"][0]
    return payload


def _schedule(
    ds_version: str,
    *,
    crontab: str = "0 0 2 * * ?",
    release_state: str = "OFFLINE",
) -> dict[str, object]:
    payload: dict[str, object] = {
        "id": 8,
        _workflow_field(ds_version): 101,
        _workflow_name_field(ds_version): "daily-etl",
        "projectName": "etl-prod",
        "definitionDescription": "daily test workflow",
        "startTime": "2026-08-06 00:00:00",
        "endTime": "2027-08-06 00:00:00",
        "crontab": crontab,
        "failureStrategy": "CONTINUE",
        "warningType": "NONE",
        "createTime": "2026-08-05 10:00:00",
        "updateTime": "2026-08-05 10:00:00",
        "userId": 1,
        "userName": "admin",
        "releaseState": release_state,
        "warningGroupId": 0,
        _priority_field(ds_version): "MEDIUM",
        "workerGroup": "default",
    }
    if ds_version != "1.3.9":
        payload["timezoneId"] = "Asia/Shanghai"
        payload["environmentCode"] = -1
    if ds_version in _TENANT_VERSIONS:
        payload["tenantCode"] = "tenant-a"
        payload["environmentName"] = None
    if ds_version == "3.4.3":
        payload["missedFirePolicy"] = "FIRE_ALL_MISSED"
    return payload


def _page(items: list[dict[str, object]]) -> dict[str, object]:
    return {
        "totalList": items,
        "total": len(items),
        "totalPage": 0 if not items else 1,
        "pageSize": 100,
        "currentPage": 1,
        "pageNo": 1,
    }


def _success(data: object) -> httpx.Response:
    return httpx.Response(200, json={"code": 0, "msg": "success", "data": data})


def _unexpected_request(request: httpx.Request) -> httpx.Response:
    message = f"unexpected request {request.method} {request.url.path}"
    raise AssertionError(message)


@pytest.mark.parametrize("policy", ["SKIP_MISSED", "FIRE_ONCE_NOW", None])
def test_schedule_update_preserves_stored_missed_fire_policy(
    policy: str | None,
) -> None:
    profile = make_profile(ds_version="3.4.3")
    stored = _schedule("3.4.3")
    stored["missedFirePolicy"] = policy
    sent: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal stored
        if request.method == "GET" and request.url.path.endswith("/schedules"):
            return _success(_page([stored]))
        if request.url.path.endswith("/schedules/preview"):
            return _success(["2026-08-06 02:00:00"])
        if request.method == "PUT" and request.url.path.endswith("/schedules/8"):
            form = parse_qs(request.content.decode(), keep_blank_values=True)
            sent.append(form["schedule"][0])
            stored["warningType"] = form["warningType"][0]
            return _success(stored)
        return _unexpected_request(request)

    client = DolphinSchedulerClient(profile, transport=httpx.MockTransport(handler))
    with client:
        operations = _operations(ScheduleAdapter.for_version("3.4.3"), profile, client)
        prepared = operations.prepare_update(8, ScheduleUpdatePatch(warning_type="ALL"))
        assert prepared.state.missed_fire_policy == policy
        result = operations.update(prepared)
    assert sent == [""]
    assert result.to_data()["missedFirePolicy"] == policy


def test_schedule_update_policy_change_uses_native_json_and_verifies_readback() -> None:
    profile = make_profile(ds_version="3.4.3")
    stored = _schedule("3.4.3")
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal stored
        if request.method == "GET" and request.url.path.endswith("/schedules"):
            return _success(_page([stored]))
        if request.url.path.endswith("/schedules/preview"):
            return _success(["2026-08-06 02:00:00"])
        if request.method == "PUT" and request.url.path.endswith("/schedules/8"):
            requests.append(request)
            form = parse_qs(request.content.decode())
            stored = _schedule_from_form("3.4.3", form, release_state="OFFLINE")
            return _success(stored)
        return _unexpected_request(request)

    client = DolphinSchedulerClient(profile, transport=httpx.MockTransport(handler))
    with client:
        operations = _operations(ScheduleAdapter.for_version("3.4.3"), profile, client)
        prepared = operations.prepare_update(
            8, ScheduleUpdatePatch(missed_fire_policy="SKIP_MISSED")
        )
        result = operations.update(prepared)
    form = parse_qs(requests[0].content.decode())
    assert "missedFirePolicy" not in form
    assert json.loads(form["schedule"][0])["missedFirePolicy"] == "SKIP_MISSED"
    assert result.to_data()["missedFirePolicy"] == "SKIP_MISSED"


@pytest.mark.parametrize("policy", [None, "UNKNOWN"])
def test_schedule_update_rejects_invalid_missed_fire_policy(policy: str | None) -> None:
    profile = make_profile(ds_version="3.4.3")
    read_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal read_count
        if request.method == "GET" and request.url.path.endswith("/schedules"):
            read_count += 1
            return _success(_page([_schedule("3.4.3")]))
        return _unexpected_request(request)

    client = DolphinSchedulerClient(profile, transport=httpx.MockTransport(handler))
    with client:
        operations = _operations(ScheduleAdapter.for_version("3.4.3"), profile, client)
        with pytest.raises(UserInputError):
            operations.prepare_update(8, ScheduleUpdatePatch(missed_fire_policy=policy))
    assert read_count == 0


@pytest.mark.parametrize("version", ["1.3.9", "3.4.1", "3.4.2"])
def test_schedule_old_exact_contract_rejects_missed_fire_policy_without_requests(
    version: str,
) -> None:
    profile = make_profile(ds_version=version)
    client = DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(_unexpected_request)
    )
    with client:
        operations = _operations(ScheduleAdapter.for_version(version), profile, client)
        with pytest.raises(UserInputError) as exc:
            operations.prepare_update(
                8, ScheduleUpdatePatch(missed_fire_policy="SKIP_MISSED")
            )
    assert exc.value.details["field"] == "missedFirePolicy"


@pytest.mark.parametrize("phase", ["stale", "readback"])
def test_schedule_policy_drift_is_detected_before_or_after_mutation(phase: str) -> None:
    profile = make_profile(ds_version="3.4.3")
    stored = _schedule("3.4.3")
    reads = 0
    mutations = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal reads, mutations, stored
        if request.method == "GET" and request.url.path.endswith("/schedules"):
            reads += 1
            row = dict(stored)
            if phase == "stale" and reads > 1:
                row["missedFirePolicy"] = "FIRE_ONCE_NOW"
            return _success(_page([row]))
        if request.url.path.endswith("/schedules/preview"):
            return _success(["2026-08-06 02:00:00"])
        if request.method == "PUT" and request.url.path.endswith("/schedules/8"):
            mutations += 1
            form = parse_qs(request.content.decode(), keep_blank_values=True)
            assert form["schedule"] == [""]
            stored["warningType"] = form["warningType"][0]
            stored["missedFirePolicy"] = "FIRE_ONCE_NOW"
            return _success(stored)
        return _unexpected_request(request)

    client = DolphinSchedulerClient(profile, transport=httpx.MockTransport(handler))
    with client:
        operations = _operations(ScheduleAdapter.for_version("3.4.3"), profile, client)
        prepared = operations.prepare_update(8, ScheduleUpdatePatch(warning_type="ALL"))
        if phase == "stale":
            with pytest.raises(ConflictError):
                operations.update(prepared)
        else:
            with pytest.raises(ApiTransportError) as exc:
                operations.update(prepared)
            assert exc.value.details["mutation_applied"] is True
            mismatches = exc.value.details["mismatches"]
            assert isinstance(mismatches, dict)
            assert "missedFirePolicy" in mismatches
    assert mutations == (0 if phase == "stale" else 1)


@pytest.mark.parametrize("policy", [None, "SKIP_MISSED", "FIRE_ONCE_NOW"])
def test_schedule_create_uses_exact_native_missed_fire_default_or_explicit_value(
    policy: str | None,
) -> None:
    profile = make_profile(ds_version="3.4.3")
    sent: list[dict[str, object]] = []
    created = False
    stored = _schedule("3.4.3")

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal created, stored
        if request.url.path.endswith("/project-preference"):
            return _success({"projectCode": 7, "state": 0})
        if request.method == "GET" and request.url.path.endswith("/schedules"):
            return _success(_page([stored] if created else []))
        if request.url.path.endswith("/schedules/preview"):
            return _success(["2026-08-06 02:00:00"])
        if request.method == "POST" and request.url.path.endswith("/schedules"):
            form = parse_qs(request.content.decode())
            sent.append(json.loads(form["schedule"][0]))
            stored = _schedule_from_form("3.4.3", form, release_state="OFFLINE")
            created = True
            return _success(stored)
        return _unexpected_request(request)

    client = DolphinSchedulerClient(profile, transport=httpx.MockTransport(handler))
    with client:
        operations = _operations(ScheduleAdapter.for_version("3.4.3"), profile, client)
        state = replace(_state("3.4.3"), missed_fire_policy=policy)
        prepared = operations.prepare_create("etl-prod", "daily-etl", state)
        result = operations.create(prepared)
    expected = policy or "FIRE_ALL_MISSED"
    assert sent[0]["missedFirePolicy"] == expected
    assert result.to_data()["missedFirePolicy"] == expected
