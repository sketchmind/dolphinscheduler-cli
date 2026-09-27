from __future__ import annotations

from urllib.parse import parse_qs

import httpx
import pytest

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import ApiTransportError, NotFoundError, ResolutionError
from dsctl.upstream.access_tokens import AccessTokenAdapter
from dsctl.upstream.clusters import ClusterAdapter
from dsctl.upstream.observability import AuditAdapter
from dsctl.upstream.users import UserAdapter, bind_user_identity_lookup
from tests.support import make_profile


def _success(data: object) -> httpx.Response:
    return httpx.Response(200, json={"code": 0, "msg": "success", "data": data})


def _user(
    user_id: int = 7, name: str = "alice", *, state: int = 1
) -> dict[str, object]:
    return {
        "id": user_id,
        "userName": name,
        "email": f"{name}@example.com",
        "phone": None,
        "userType": "GENERAL_USER",
        "tenantId": 11,
        "tenantCode": "tenant-prod",
        "queueName": "root.tenant",
        "queue": "",
        "state": state,
        "timeZone": "Asia/Shanghai",
        "createTime": None,
        "updateTime": None,
    }


def _page(
    *items: dict[str, object], page_no: int = 1, pages: int = 1
) -> dict[str, object]:
    return {
        "totalList": list(items),
        "total": pages,
        "totalPage": pages,
        "pageSize": 100,
        "pageNo": page_no,
        "currentPage": page_no,
    }


@pytest.mark.parametrize("selector", ["7", "alice"])
def test_identity_self_lookup_requires_only_current_user(selector: str) -> None:
    profile = make_profile(ds_version="3.4.3")
    requests: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request.url.path)
        assert request.method == "GET"
        assert request.url.path == "/dolphinscheduler/users/get-user-info"
        return _success(_user())

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        lookup = bind_user_identity_lookup(profile, http_client=client)
        assert lookup is not None
        identity = lookup.resolve(selector)

    assert identity.to_data() == {"id": 7, "userName": "alice"}
    assert requests == ["/dolphinscheduler/users/get-user-info"]


@pytest.mark.parametrize("selector", ["9", "bob"])
def test_identity_other_lookup_consumes_native_vo_without_detail_paging(
    selector: str,
) -> None:
    profile = make_profile(ds_version="3.4.3")
    requests: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request.url.path)
        assert request.method == "GET"
        if request.url.path.endswith("get-user-info"):
            return _success(_user())
        assert request.url.path == "/dolphinscheduler/users/list-all"
        return _success([{"id": 9, "userName": "bob"}])

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        lookup = bind_user_identity_lookup(profile, http_client=client)
        assert lookup is not None
        assert lookup.resolve(selector).to_data() == {"id": 9, "userName": "bob"}

    assert requests == [
        "/dolphinscheduler/users/get-user-info",
        "/dolphinscheduler/users/list-all",
    ]


def test_disabled_identity_remains_addressable_via_conditional_paged_lookup() -> None:
    profile = make_profile(ds_version="3.4.3")
    pages: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("get-user-info"):
            return _success(_user())
        if request.url.path.endswith("list-all"):
            return _success([])
        assert request.url.path == "/dolphinscheduler/users/list-paging"
        page_no = int(request.url.params["pageNo"])
        pages.append(page_no)
        assert request.url.params["pageSize"] == "100"
        return _success(
            _page(_user(8, "carol"), pages=2)
            if page_no == 1
            else _page(_user(9, "bob", state=0), page_no=2, pages=2)
        )

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        lookup = bind_user_identity_lookup(profile, http_client=client)
        assert lookup is not None
        assert lookup.resolve("9").to_data() == {"id": 9, "userName": "bob"}

    assert pages == [1, 2]


@pytest.mark.parametrize("operation", ["create", "update", "generate"])
def test_self_token_operations_do_not_require_administrator_user_lists(
    operation: str,
) -> None:
    profile = make_profile(ds_version="3.4.3")
    current: dict[str, object] = {
        "id": 11,
        "userId": 7,
        "token": "old",
        "expireTime": "2027-01-01 00:00:00",
        "createTime": None,
        "updateTime": None,
    }
    user_reads: list[str] = []
    writes: list[tuple[str, dict[str, list[str]]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.startswith("/dolphinscheduler/users/"):
            user_reads.append(request.url.path)
            assert request.method == "GET"
            assert request.url.path.endswith("get-user-info")
            return _success(_user())
        if request.method == "GET":
            assert request.url.path == "/dolphinscheduler/access-tokens"
            if operation == "create" and not writes:
                return _success(_page())
            return _success(_page(current))
        form = parse_qs(request.content.decode(), keep_blank_values=True)
        assert form["userId"] == ["7"]
        writes.append((request.method, form))
        if request.url.path.endswith("generate"):
            return _success("generated")
        assert request.url.path in {
            "/dolphinscheduler/access-tokens",
            "/dolphinscheduler/access-tokens/11",
        }
        current.update({"token": "new", "expireTime": form["expireTime"][0]})
        return _success(current)

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        domain = AccessTokenAdapter.for_version("3.4.3").bind(
            profile, http_client=client
        )
        if operation == "create":
            resolved_user = domain.create(
                user="alice", expire_time="2028-01-01 00:00:00", token="new"
            ).user
        elif operation == "update":
            resolved_user = domain.update(
                11,
                user=None,
                expire_time="2028-01-01 00:00:00",
                token="new",
                regenerate_token=False,
            ).user
        else:
            resolved_user = domain.generate(
                user="7", expire_time="2028-01-01 00:00:00"
            ).user
        assert resolved_user.to_data() == {"id": 7, "userName": "alice"}

    assert user_reads == ["/dolphinscheduler/users/get-user-info"]
    assert len(writes) == 1


@pytest.mark.parametrize("selector", ["alice", "7"])
def test_self_user_update_preserves_fields_without_administrator_lookup(
    selector: str,
) -> None:
    profile = make_profile(ds_version="3.4.3")
    current = _user()
    reads: list[str] = []
    writes: list[dict[str, list[str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            reads.append(request.url.path)
            assert request.url.path == "/dolphinscheduler/users/get-user-info"
            return _success(current)
        assert request.method == "POST"
        assert request.url.path == "/dolphinscheduler/users/update"
        form = parse_qs(request.content.decode(), keep_blank_values=True)
        writes.append(form)
        current["email"] = form["email"][0]
        return _success(current)

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        domain = UserAdapter.for_version("3.4.3").bind(profile, http_client=client)
        changed = domain.update(
            selector,
            user_name=None,
            password=None,
            email="new@example.com",
            tenant=None,
            state=None,
            phone=None,
            preserve_phone=True,
            queue=None,
            preserve_queue=True,
            time_zone=None,
        )
        assert changed.record.email == "new@example.com"
        assert changed.record.storedQueue == ""

    assert reads == ["/dolphinscheduler/users/get-user-info"] * 2
    assert len(writes) == 1
    assert writes[0]["queue"] == [""]
    assert writes[0]["tenantId"] == ["11"]
    assert writes[0]["timeZone"] == ["Asia/Shanghai"]


def _cluster(code: int) -> dict[str, object]:
    return {
        "id": code,
        "code": code,
        "name": f"cluster-{code}",
        "config": "kube",
        "description": None,
        "workflowDefinitions": ["daily-etl"],
        "operator": 1,
        "createTime": None,
        "updateTime": None,
    }


def test_cluster_detail_uses_all_native_pages_and_exact_code() -> None:
    profile = make_profile(ds_version="3.4.3")
    pages: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/dolphinscheduler/cluster/list-paging"
        assert request.url.params["pageSize"] == "100"
        page_no = int(request.url.params["pageNo"])
        pages.append(page_no)
        return _success(
            _page(_cluster(17), pages=2)
            if page_no == 1
            else _page(_cluster(7), page_no=2, pages=2)
        )

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        clusters = (
            ClusterAdapter.for_version("3.4.3")
            .bind(profile, http_client=client)
            .clusters
        )
        assert clusters.get(code=7).code == 7

    assert pages == [1, 2]


@pytest.mark.parametrize(
    ("items", "error"),
    [([], NotFoundError), ([_cluster(7), _cluster(7)], ResolutionError)],
)
def test_cluster_paged_detail_rejects_missing_or_duplicate_code(
    items: list[dict[str, object]], error: type[Exception]
) -> None:
    profile = make_profile(ds_version="3.4.3")

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/dolphinscheduler/cluster/list-paging"
        return _success(_page(*items))

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        clusters = (
            ClusterAdapter.for_version("3.4.3")
            .bind(profile, http_client=client)
            .clusters
        )
        with pytest.raises(error):
            clusters.get(code=7)


def test_cluster_create_missing_readback_requires_reconciliation() -> None:
    profile = make_profile(ds_version="3.4.3")
    writes = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal writes
        if request.method == "POST":
            assert request.url.path == "/dolphinscheduler/cluster/create"
            writes += 1
            return _success(7)
        assert request.url.path == "/dolphinscheduler/cluster/list-paging"
        return _success(_page())

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        clusters = (
            ClusterAdapter.for_version("3.4.3")
            .bind(profile, http_client=client)
            .clusters
        )
        with pytest.raises(ApiTransportError) as captured:
            clusters.create(name="cluster-7", config="kube", description=None)

    assert writes == 1
    assert captured.value.details["phase"] == "readback"
    assert captured.value.details["mutation_applied"] is True
    assert "do not blindly repeat" in (captured.value.suggestion or "")


@pytest.mark.parametrize("grant", [True, False])
def test_datasource_permission_vo_preserves_complete_grant_set(*, grant: bool) -> None:
    profile = make_profile(ds_version="3.4.3")
    identities = {41: {"id": 41, "name": "warehouse"}, 42: {"id": 42, "name": "events"}}
    authorized = {41} if grant else {41, 42}
    writes: list[dict[str, list[str]]] = []
    user_reads: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if (
            request.url.path.startswith("/dolphinscheduler/users/")
            and request.method == "GET"
        ):
            user_reads.append(request.url.path)
            if request.url.path.endswith("get-user-info"):
                return _success(_user(1, "admin") | {"userType": "ADMIN_USER"})
            assert request.url.path.endswith("list-all")
            return _success([{"id": 7, "userName": "alice"}])
        if request.method == "GET":
            assert request.url.params["userId"] == "7"
            assert request.url.path in {
                "/dolphinscheduler/datasources/authed-datasource",
                "/dolphinscheduler/datasources/unauth-datasource",
            }
            selected = (
                authorized
                if request.url.path.endswith("/authed-datasource")
                else identities.keys() - authorized
            )
            return _success([identities[key] for key in sorted(selected)])
        assert request.method == "POST"
        assert request.url.path == "/dolphinscheduler/users/grant-datasource"
        form = parse_qs(request.content.decode())
        writes.append(form)
        authorized.clear()
        authorized.update(int(value) for value in form["datasourceIds"][0].split(","))
        return _success(None)

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        domain = UserAdapter.for_version("3.4.3").bind(profile, http_client=client)
        result = domain.change_datasources(
            "alice", ["events" if grant else "warehouse"], grant=grant
        )

    assert writes == [{"userId": ["7"], "datasourceIds": ["41,42" if grant else "42"]}]
    assert {item.id for item in result.final} == ({41, 42} if grant else {42})
    assert all(
        item.note is None and item.type is None
        for item in (*result.requested, *result.final)
    )
    assert result.user.to_data() == {"id": 7, "userName": "alice"}
    assert user_reads == [
        "/dolphinscheduler/users/get-user-info",
        "/dolphinscheduler/users/list-all",
    ]


def test_audit_filter_preserves_server_visibility_without_actor_override() -> None:
    profile = make_profile(ds_version="3.4.3")
    queries: list[dict[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/dolphinscheduler/projects/audit/audit-log-list"
        queries.append(dict(request.url.params))
        return _success(_page())

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        audits = (
            AuditAdapter.for_version("3.4.3").bind(profile, http_client=client).audits
        )
        audits.list(
            page_no=2,
            page_size=25,
            user_name="other-user",
            start_date=None,
            end_date=None,
            model_types=[],
            operation_types=[],
            model_name=None,
        )

    assert queries == [{"pageNo": "2", "pageSize": "25", "userName": "other-user"}]
