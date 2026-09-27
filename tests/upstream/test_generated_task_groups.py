from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, cast
from urllib.parse import parse_qs

import httpx
import pytest

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import ApiResultError, ApiTransportError, UnsupportedFeatureError
from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.upstream.serialization import serialize_task_group_queue
from dsctl.upstream.task_groups import (
    TASK_GROUP_DOMAIN,
    _task_group_snapshot,
)
from tests.support import make_profile

if TYPE_CHECKING:
    from dsctl.upstream.protocol import TaskGroupOperations, TaskGroupQueueRecord

# The reviewed TaskGroupController/TaskGroupQueueController epochs retain
# numeric status through 3.2.0 and void create/update results through 3.2.1.
_ABSENT_VERSIONS = (
    "1.3.9",
    "2.0.0",
    "2.0.1",
    "2.0.2",
    "2.0.3",
    "2.0.4",
    "2.0.5",
    "2.0.6",
    "2.0.7",
    "2.0.8",
    "2.0.9",
)
_SUPPORTED_VERSIONS = tuple(
    version for version in TARGET_DS_VERSIONS if version not in _ABSENT_VERSIONS
)
_NUMERIC_STATUS_VERSIONS = frozenset(
    {
        "3.0.0",
        "3.0.1",
        "3.0.2",
        "3.0.3",
        "3.0.4",
        "3.0.5",
        "3.0.6",
        "3.1.0",
        "3.1.1",
        "3.1.2",
        "3.1.3",
        "3.1.4",
        "3.1.5",
        "3.1.6",
        "3.1.7",
        "3.1.8",
        "3.1.9",
        "3.2.0",
    }
)
_VOID_MUTATION_VERSIONS = _NUMERIC_STATUS_VERSIONS | {"3.2.1"}
_PROCESS_QUEUE_VERSIONS = _VOID_MUTATION_VERSIONS | {"3.2.2"}
_QUEUE_IN_QUEUE_ABSENT_VERSIONS = frozenset(
    {
        "3.0.0",
        "3.0.1",
        "3.0.2",
        "3.0.3",
        "3.0.4",
        "3.0.5",
        "3.0.6",
        "3.1.0",
        "3.1.1",
        "3.1.2",
        "3.1.3",
        "3.1.4",
        "3.1.5",
        "3.1.6",
        "3.1.7",
        "3.1.8",
        "3.1.9",
        "3.2.0",
    }
)


@pytest.mark.parametrize("ds_version", _SUPPORTED_VERSIONS)
def test_exact_task_group_lifecycle_and_queue_projection(ds_version: str) -> None:
    profile = make_profile(ds_version=ds_version)
    current = _group(ds_version)
    exchanges: list[tuple[str, str, dict[str, list[str]]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal current
        path = request.url.path.removeprefix("/dolphinscheduler/task-group/")
        values = parse_qs(
            request.url.query.decode()
            if request.method == "GET"
            else request.content.decode(),
            keep_blank_values=True,
        )
        exchanges.append((request.method, path, values))
        if request.method == "GET":
            if path == "query-list-by-group-id":
                return _success(_page(_queue(ds_version), values))
            assert path in {"list-paging", "query-list-by-projectCode"}
            return _success(_page(current, values))
        assert request.method == "POST"
        if path in {"create", "update"}:
            current = {
                **current,
                "name": values["name"][0],
                "description": values["description"][0],
                "groupSize": int(values["groupSize"][0]),
            }
            return _success(None if ds_version in _VOID_MUTATION_VERSIONS else current)
        if path in {"close-task-group", "start-task-group"}:
            opened = path == "start-task-group"
            if ds_version in _NUMERIC_STATUS_VERSIONS:
                current["status"] = int(opened)
            else:
                current["status"] = "YES" if opened else "NO"
        else:
            assert path in {"forceStart", "modifyPriority"}
        # These are void results, even if the server attaches successful data.
        return _success(False)

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as http_client:
        groups = TASK_GROUP_DOMAIN.bind(profile, http_client=http_client).task_groups
        page = groups.list(page_no=2, page_size=25, search="etl", status=1)
        assert page.pageNo == 2
        assert page.pageSize == 25
        assert next(iter(page.totalList or [])).status == "YES"
        assert groups.get(task_group_id=7).name == "etl"
        created = groups.create(
            project_code=11, name="new", description="", group_size=2
        )
        assert (created.name, created.description, created.groupSize) == ("new", "", 2)
        updated = groups.update(
            task_group_id=7,
            project_code=11,
            name="updated",
            description="bounded",
            group_size=3,
        )
        assert (updated.name, updated.description, updated.groupSize) == (
            "updated",
            "bounded",
            3,
        )
        assert groups.close(task_group_id=7).status == "NO"
        assert groups.start(task_group_id=7).status == "YES"
        queue_page = groups.list_queues(
            group_id=7,
            page_no=2,
            page_size=25,
            task_instance_name="extract",
            workflow_instance_name="daily",
            status=-1,
        )
        queue = next(iter(queue_page.totalList or []))
        assert (queue.workflowInstanceName, queue.workflowInstanceId) == (
            "daily-etl-1",
            501,
        )
        assert queue.status == "WAIT_QUEUE"
        assert queue.taskId == 101
        assert queue.inQueue == (
            None if ds_version in _QUEUE_IN_QUEUE_ABSENT_VERSIONS else 1
        )
        groups.force_start(queue_id=31)
        groups.set_queue_priority(queue_id=31, priority=0)

    filter_name = (
        "processInstanceName"
        if ds_version in _PROCESS_QUEUE_VERSIONS
        else "workflowInstanceName"
    )
    assert exchanges == [
        (
            "GET",
            "list-paging",
            {"name": ["etl"], "status": ["1"], "pageNo": ["2"], "pageSize": ["25"]},
        ),
        ("GET", "list-paging", {"pageNo": ["1"], "pageSize": ["100"]}),
        (
            "POST",
            "create",
            {
                "name": ["new"],
                "projectCode": ["11"],
                "description": [""],
                "groupSize": ["2"],
            },
        ),
        (
            "GET",
            "query-list-by-projectCode",
            {"projectCode": ["11"], "pageNo": ["1"], "pageSize": ["100"]},
        ),
        (
            "POST",
            "update",
            {
                "id": ["7"],
                "name": ["updated"],
                "description": ["bounded"],
                "groupSize": ["3"],
            },
        ),
        (
            "GET",
            "query-list-by-projectCode",
            {"projectCode": ["11"], "pageNo": ["1"], "pageSize": ["100"]},
        ),
        ("POST", "close-task-group", {"id": ["7"]}),
        ("GET", "list-paging", {"pageNo": ["1"], "pageSize": ["100"]}),
        ("POST", "start-task-group", {"id": ["7"]}),
        ("GET", "list-paging", {"pageNo": ["1"], "pageSize": ["100"]}),
        (
            "GET",
            "query-list-by-group-id",
            {
                "groupId": ["7"],
                "taskInstanceName": ["extract"],
                filter_name: ["daily"],
                "status": ["-1"],
                "pageNo": ["2"],
                "pageSize": ["25"],
            },
        ),
        ("POST", "forceStart", {"queueId": ["31"]}),
        ("POST", "modifyPriority", {"queueId": ["31"], "priority": ["0"]}),
    ]


@pytest.mark.parametrize("ds_version", ["3.0.0", "3.4.2"])
@pytest.mark.parametrize(
    "operation", ["create", "update", "close", "start", "force-start", "priority"]
)
def test_task_group_writes_never_retry_or_read_back_an_uncertain_write(
    ds_version: str,
    operation: str,
) -> None:
    profile = make_profile(ds_version=ds_version).model_copy(
        update={"api_retry_attempts": 4, "api_retry_backoff_ms": 0}
    )
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        message = "response lost"
        raise httpx.ReadError(message, request=request)

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        groups = TASK_GROUP_DOMAIN.bind(profile, http_client=client).task_groups
        with pytest.raises(ApiTransportError) as caught:
            _mutate(groups, operation)

    assert len(requests) == 1
    assert requests[0].method == "POST"
    assert caught.value.details["phase"] == "mutation_request"
    assert caught.value.details["mutation_may_have_applied"] is True


@pytest.mark.parametrize("ds_version", ["3.0.0", "3.4.2"])
@pytest.mark.parametrize("operation", ["create", "update", "close", "start"])
def test_task_group_readback_retries_only_the_read(
    ds_version: str,
    operation: str,
) -> None:
    profile = make_profile(ds_version=ds_version).model_copy(
        update={"api_retry_attempts": 3, "api_retry_backoff_ms": 0}
    )
    methods: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        methods.append(request.method)
        if request.method == "POST":
            return _success(
                _group(ds_version)
                if operation in {"create", "update"}
                and ds_version not in _VOID_MUTATION_VERSIONS
                else None
            )
        message = "readback unavailable"
        raise httpx.ReadError(message, request=request)

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        groups = TASK_GROUP_DOMAIN.bind(profile, http_client=client).task_groups
        with pytest.raises(ApiTransportError) as caught:
            _mutate(groups, operation)

    assert methods == ["POST", "GET", "GET", "GET"]
    assert caught.value.details["phase"] == "readback"
    assert caught.value.details["mutation_applied"] is True


@pytest.mark.parametrize("operation", ["force-start", "priority"])
def test_task_group_queue_writes_keep_definitive_upstream_errors(
    operation: str,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200, json={"code": 10010, "msg": "rejected", "data": None}
        )

    profile = make_profile()
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        groups = TASK_GROUP_DOMAIN.bind(profile, http_client=client).task_groups
        with pytest.raises(ApiResultError) as caught:
            _mutate(groups, operation)
    assert caught.value.result_code == 10010
    assert len(requests) == 1


def _mutate(groups: TaskGroupOperations, operation: str) -> None:
    if operation == "create":
        groups.create(project_code=11, name="etl", description="bounded", group_size=4)
    elif operation == "update":
        groups.update(
            task_group_id=7,
            project_code=11,
            name="etl",
            description="bounded",
            group_size=4,
        )
    elif operation == "close":
        groups.close(task_group_id=7)
    elif operation == "start":
        groups.start(task_group_id=7)
    elif operation == "force-start":
        groups.force_start(queue_id=31)
    else:
        assert operation == "priority"
        groups.set_queue_priority(queue_id=31, priority=0)


def _group(ds_version: str) -> dict[str, object]:
    return {
        "id": 7,
        "name": "etl",
        "projectCode": 11,
        "description": "bounded",
        "groupSize": 4,
        "useSize": 1,
        "userId": 3,
        "status": 1 if ds_version in _NUMERIC_STATUS_VERSIONS else "YES",
    }


def _queue(ds_version: str) -> dict[str, object]:
    row: dict[str, object] = {
        "id": 31,
        "taskId": 101,
        "taskName": "extract",
        "projectName": "etl-prod",
        "projectCode": "11",
        "groupId": 7,
        "priority": 2,
        "forceStart": 0,
        "inQueue": 1,
        "status": "WAIT_QUEUE",
    }
    if ds_version in _PROCESS_QUEUE_VERSIONS:
        row.update(processInstanceName="daily-etl-1", processId=501)
    else:
        row.update(workflowInstanceName="daily-etl-1", workflowInstanceId=501)
    return row


def _page(row: dict[str, object], query: dict[str, list[str]]) -> dict[str, object]:
    page_no, page_size = int(query["pageNo"][0]), int(query["pageSize"][0])
    return {
        "totalList": [row],
        "total": 1,
        "totalPage": 1,
        "pageSize": page_size,
        "currentPage": page_no,
        "pageNo": page_no,
    }


def _success(data: object) -> httpx.Response:
    return httpx.Response(200, json={"code": 0, "msg": "success", "data": data})


@pytest.mark.parametrize("ds_version", _SUPPORTED_VERSIONS)
@pytest.mark.parametrize(
    ("include_task_id", "task_id"),
    [(False, 0), (True, 0), (True, 101)],
    ids=["omitted", "java-default", "real-id"],
)
def test_queue_page_does_not_invent_unprojected_identity_or_flag(
    ds_version: str, *, include_task_id: bool, task_id: int
) -> None:
    # Every reviewed paging SELECT omits task_id. Early SELECTs also omit
    # in_queue; Java serializes their primitive defaults as zero.
    raw = {**_queue(ds_version), "taskId": task_id, "inQueue": 0}
    if not include_task_id:
        raw.pop("taskId")
    data = serialize_task_group_queue(_read_queue(ds_version, raw))

    assert data["id"] == 31
    assert data["taskId"] == (task_id or None)
    assert data["workflowInstanceId"] == 501
    assert data["inQueue"] == (
        None if ds_version in _QUEUE_IN_QUEUE_ABSENT_VERSIONS else 0
    )


@pytest.mark.parametrize("ds_version", ["3.0.0", "3.2.0", "3.2.1", "3.4.3"])
@pytest.mark.parametrize(
    ("field", "value"),
    [("taskId", -1), ("id", 0), ("groupId", 0)],
)
def test_queue_page_still_rejects_invalid_identities(
    ds_version: str, field: str, value: int
) -> None:
    with pytest.raises(ApiTransportError, match="cannot be projected"):
        _read_queue(ds_version, {**_queue(ds_version), field: value})


def _read_queue(ds_version: str, row: dict[str, object]) -> TaskGroupQueueRecord:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.method == "GET"
        assert request.url.path.endswith("/task-group/query-list-by-group-id")
        return _success(_page(row, parse_qs(request.url.query.decode())))

    profile = make_profile(ds_version=ds_version)
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        groups = TASK_GROUP_DOMAIN.bind(profile, http_client=client).task_groups
        page = groups.list_queues(group_id=7, page_no=1, page_size=100)
        item = next(iter(page.totalList or []))
    assert len(requests) == 1
    return item


def test_invalid_legacy_group_status_fails_closed() -> None:
    with pytest.raises(ApiTransportError, match="cannot be projected"):
        _task_group_snapshot(
            SimpleNamespace(
                id=7,
                name="etl",
                projectCode=11,
                description=None,
                groupSize=4,
                useSize=0,
                userId=3,
                status=2,
                createTime=None,
                updateTime=None,
            ),
            ds_version="3.0.0",
            numeric_status=True,
        )


@pytest.mark.parametrize("ds_version", _ABSENT_VERSIONS)
def test_pre_3_0_task_group_domain_is_terminal_without_transport(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    adapter = TASK_GROUP_DOMAIN.adapter_for_version(ds_version)

    with pytest.raises(UnsupportedFeatureError, match="do not exist") as exc_info:
        adapter.bind(
            profile,
            http_client=cast("DolphinSchedulerClient", object()),
        )

    assert exc_info.value.details["reason"] == "upstream_capability_absent"
    assert exc_info.value.details["introduced_in"] == "3.0.0"
