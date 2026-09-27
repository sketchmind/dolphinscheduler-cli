from __future__ import annotations

from urllib.parse import parse_qs

import httpx
import pytest

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import ApiTransportError, UnsupportedFeatureError
from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.upstream.queues import (
    QueueAdapter,
    QueueDomain,
    bind_queue_lookup,
)
from dsctl.upstream.wire import WireContractError
from tests.support import make_profile

_QUEUE_DELETE_VERSIONS = frozenset(
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
_QUEUE_DELETE_ABSENT_VERSIONS = tuple(
    version for version in TARGET_DS_VERSIONS if version not in _QUEUE_DELETE_VERSIONS
)
_NULL_CREATE_VERSIONS = frozenset({"1.3.9", "2.0.0", "2.0.1"})
_NULL_UPDATE_VERSIONS = frozenset(
    {
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
        "3.0.0",
        "3.0.1",
        "3.0.2",
        "3.0.3",
        "3.0.4",
        "3.0.5",
        "3.0.6",
    }
)


@pytest.mark.parametrize("ds_version", TARGET_DS_VERSIONS)
def test_queue_lookup_pages_through_its_exact_profile(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    requests_seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(request)
        return _success({"totalList": [], "total": 0, "totalPage": 0, "currentPage": 2})

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        lookup = bind_queue_lookup(profile, http_client=client)
        assert requests_seen == []
        page = lookup.list(page_no=2, page_size=17, search="queue-ops")

    assert page.currentPage == 2
    assert page.pageSize == 17
    assert len(requests_seen) == 1
    request = requests_seen[0]
    assert request.method == "GET"
    assert request.url.path == (
        "/dolphinscheduler/queue/list-paging"
        if ds_version == "1.3.9"
        else "/dolphinscheduler/queues"
    )
    assert parse_qs(request.url.query.decode()) == {
        "pageNo": ["2"],
        "pageSize": ["17"],
        "searchVal": ["queue-ops"],
    }


@pytest.mark.parametrize("ds_version", TARGET_DS_VERSIONS)
def test_queue_domain_executes_the_reviewed_crud_recipe(ds_version: str) -> None:
    profile = make_profile(ds_version=ds_version)
    legacy = ds_version == "1.3.9"
    list_path = (
        "/dolphinscheduler/queue/list-paging" if legacy else "/dolphinscheduler/queues"
    )
    current: dict[str, object] | None = None
    requests_seen: list[tuple[str, str, dict[str, list[str]]]] = []

    def queue_payload(
        form: dict[str, list[str]],
        *,
        queue_id: int,
    ) -> dict[str, object]:
        return {
            "id": queue_id,
            "queueName": form["queueName"][0],
            "queue": form["queue"][0],
            "createTime": None,
            "updateTime": None,
        }

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal current
        form = parse_qs(request.content.decode())
        requests_seen.append((request.method, request.url.path, form))
        if request.method == "GET" and request.url.path == list_path:
            items = [] if current is None else [current]
            return _success(
                {
                    "totalList": items,
                    "total": len(items),
                    "totalPage": 0 if current is None else 1,
                    "currentPage": 1,
                }
            )
        create_path = "/dolphinscheduler/queue/create" if legacy else list_path
        if request.method == "POST" and request.url.path == create_path:
            current = queue_payload(form, queue_id=8)
            result = None if ds_version in _NULL_CREATE_VERSIONS else current
            return _success(result)
        update_path = (
            "/dolphinscheduler/queue/update" if legacy else "/dolphinscheduler/queues/8"
        )
        update_method = "POST" if legacy else "PUT"
        if request.method == update_method and request.url.path == update_path:
            current = queue_payload(form, queue_id=8)
            result = None if ds_version in _NULL_UPDATE_VERSIONS else current
            return _success(result)
        if request.method == "DELETE" and request.url.path == update_path:
            current = None
            delete_result: object = None if ds_version == "3.2.0" else True
            return _success(delete_result)
        message = f"unexpected request {request.method} {request.url.path}"
        raise AssertionError(message)

    adapter = QueueAdapter.for_version(ds_version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        domain = adapter.bind(profile, http_client=http_client)
        assert isinstance(domain, QueueDomain)
        created = domain.queues.create(
            queue_name="queue-new",
            queue="root.queue-new",
        )
        assert created.id is not None
        updated = domain.queues.update(
            queue_id=created.id,
            queue_name="queue-updated",
            queue="root.queue-updated",
        )
        assert updated.id is not None
        if ds_version in _QUEUE_DELETE_VERSIONS:
            deleted = domain.queues.delete(queue_id=updated.id)
            assert deleted is True
        else:
            with pytest.raises(UnsupportedFeatureError):
                domain.queues.delete(queue_id=updated.id)

    assert created.id == 8
    assert created.queueName == "queue-new"
    assert updated.queueName == "queue-updated"
    assert updated.queue == "root.queue-updated"
    mutations = [request for request in requests_seen if request[0] != "GET"]
    expected = [
        (
            "POST",
            "/dolphinscheduler/queue/create" if legacy else "/dolphinscheduler/queues",
        ),
        (
            "POST" if legacy else "PUT",
            "/dolphinscheduler/queue/update"
            if legacy
            else "/dolphinscheduler/queues/8",
        ),
    ]
    if ds_version in _QUEUE_DELETE_VERSIONS:
        expected.append(("DELETE", "/dolphinscheduler/queues/8"))
    assert [(method, path) for method, path, _form in mutations] == expected
    assert mutations[0][2] == {
        "queue": ["root.queue-new"],
        "queueName": ["queue-new"],
    }
    expected_update_form = {
        "queue": ["root.queue-updated"],
        "queueName": ["queue-updated"],
    }
    if legacy:
        expected_update_form["id"] = ["8"]
    assert mutations[1][2] == expected_update_form


@pytest.mark.parametrize("ds_version", _QUEUE_DELETE_ABSENT_VERSIONS)
def test_queue_delete_absence_rejects_without_sending_a_request(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    request_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        request_count += 1
        message = f"unexpected request {request.method} {request.url.path}"
        raise AssertionError(message)

    adapter = QueueAdapter.for_version(ds_version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client, pytest.raises(UnsupportedFeatureError) as exc_info:
        adapter.bind(profile, http_client=http_client).queues.delete(queue_id=8)

    assert request_count == 0
    assert exc_info.value.details == {
        "action": "queue.delete",
        "selected_version": ds_version,
        "reason": "upstream_capability_absent",
    }


def test_queue_create_readback_failure_requires_reconciliation_before_retry() -> None:
    profile = make_profile(ds_version="3.4.1")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return _success(
                {
                    "id": 8,
                    "queueName": "queue-new",
                    "queue": "root.queue-new",
                }
            )
        return _success(
            {
                "totalList": [],
                "total": 0,
                "totalPage": 0,
                "currentPage": 1,
            }
        )

    adapter = QueueAdapter.for_version("3.4.1")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client, pytest.raises(ApiTransportError) as exc_info:
        adapter.bind(profile, http_client=http_client).queues.create(
            queue_name="queue-new",
            queue="root.queue-new",
        )

    assert exc_info.value.details["phase"] == "readback"
    assert exc_info.value.details["mutation_applied"] is True
    assert "do not blindly repeat" in (exc_info.value.suggestion or "")


@pytest.mark.parametrize("ds_version", TARGET_DS_VERSIONS)
def test_queue_read_retries_and_accepts_an_optional_envelope(ds_version: str) -> None:
    profile = make_profile(ds_version=ds_version).model_copy(
        update={"api_retry_attempts": 2, "api_retry_backoff_ms": 0}
    )
    attempts = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(503)
        return httpx.Response(
            200,
            json={"totalList": [], "total": 0, "totalPage": 0, "currentPage": 1},
        )

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        domain = QueueAdapter.for_version(ds_version).bind(profile, http_client=client)
        assert domain.queues.list(page_no=1, page_size=20).total == 0

    assert attempts == 2


@pytest.mark.parametrize("ds_version", TARGET_DS_VERSIONS)
def test_queue_mutation_is_never_retried(ds_version: str) -> None:
    profile = make_profile(ds_version=ds_version).model_copy(
        update={"api_retry_attempts": 3, "api_retry_backoff_ms": 0}
    )
    attempts = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(503)

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        domain = QueueAdapter.for_version(ds_version).bind(profile, http_client=client)
        with pytest.raises(ApiTransportError) as exc_info:
            domain.queues.create(queue="root.ops", queue_name="ops")

    assert attempts == 1
    assert exc_info.value.details["phase"] == "mutation_request"
    assert exc_info.value.details["mutation_may_have_applied"] is True


@pytest.mark.parametrize("ds_version", sorted(_QUEUE_DELETE_VERSIONS))
def test_queue_delete_checks_that_the_deleted_id_is_absent(ds_version: str) -> None:
    profile = make_profile(ds_version=ds_version)
    methods_seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        methods_seen.append(request.method)
        if request.method == "DELETE":
            return _success(None if ds_version == "3.2.0" else True)
        return _success(
            {
                "totalList": [{"id": 8, "queue": "root.ops", "queueName": "ops"}],
                "total": 1,
                "totalPage": 1,
                "currentPage": 1,
            }
        )

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        domain = QueueAdapter.for_version(ds_version).bind(profile, http_client=client)
        with pytest.raises(ApiTransportError) as exc_info:
            domain.queues.delete(queue_id=8)

    assert methods_seen == ["DELETE", "GET"]
    assert exc_info.value.details["phase"] == "readback"
    assert exc_info.value.details["mutation_applied"] is True
    assert "do not blindly repeat" in (exc_info.value.suggestion or "")


def test_queue_delete_false_does_not_claim_success_or_read_back() -> None:
    profile = make_profile(ds_version="3.4.1")
    methods_seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        methods_seen.append(request.method)
        return _success(False)

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        domain = QueueAdapter.for_version("3.4.1").bind(profile, http_client=client)
        with pytest.raises(ApiTransportError) as exc_info:
            domain.queues.delete(queue_id=8)

    assert methods_seen == ["DELETE"]
    assert exc_info.value.details["field"] == "deleteResult"
    assert exc_info.value.details["phase"] == "readback"
    assert exc_info.value.details["mutation_applied"] is True


@pytest.mark.parametrize("lookup_only", [False, True])
def test_queue_rejects_a_different_client_profile_before_io(
    *, lookup_only: bool
) -> None:
    profile = make_profile(ds_version="3.4.2")
    requests_seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(request)
        return _success(None)

    bind = bind_queue_lookup if lookup_only else QueueAdapter.for_version("3.4.2").bind
    with (
        DolphinSchedulerClient(
            make_profile(ds_version="3.4.1"), transport=httpx.MockTransport(handler)
        ) as client,
        pytest.raises(WireContractError, match="does not match"),
    ):
        bind(profile, http_client=client)

    assert requests_seen == []


def _success(data: object) -> httpx.Response:
    return httpx.Response(200, json={"code": 0, "msg": "success", "data": data})
