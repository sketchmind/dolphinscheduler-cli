from __future__ import annotations

from dataclasses import dataclass, field
from urllib.parse import parse_qs

import httpx
import pytest

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import ApiTransportError, UserInputError
from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.upstream.access_tokens import (
    AccessTokenAdapter,
    access_token_update_requires_expire_time,
)
from dsctl.upstream.users import bind_user_lookup
from dsctl.upstream.wire import WireContractError
from tests.support import make_profile


@pytest.mark.parametrize("ds_version", TARGET_DS_VERSIONS)
def test_token_user_lookup_uses_only_reads_and_merges_matching_user(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    requests: list[tuple[str, str, dict[str, str]]] = []
    raw_user = (_legacy_user() if ds_version == "1.3.9" else _user()) | {
        "queue": "",
        "queueName": "root.tenant",
        "phone": "13800138000",
    }
    summary = raw_user | {"queue": "root.tenant", "phone": None}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        requests.append((request.method, path, dict(request.url.params)))
        assert request.method == "GET"
        if path == "/dolphinscheduler/users/get-user-info":
            return _success(
                raw_user | {"id": 1, "userName": "admin", "userType": "ADMIN_USER"}
            )
        if path == "/dolphinscheduler/users/list":
            return _success([raw_user])
        if path == "/dolphinscheduler/users/list-all":
            return _success([raw_user | {"phone": "later-duplicate"}])
        if path == "/dolphinscheduler/users/list-paging":
            return _success(
                _page(summary | {"id": 99, "userName": "alice-extra"}, summary)
            )
        message = f"unexpected request {request.method} {path}"
        raise AssertionError(message)

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        lookup = bind_user_lookup(profile, http_client=client)
        user = lookup.get(user_id=7)

    assert user.id == 7
    assert user.userName == "alice"
    assert user.phone == "13800138000"
    assert user.queue == "root.tenant"
    assert user.storedQueue == ""
    assert requests == [
        *(
            (
                [
                    ("GET", "/dolphinscheduler/users/get-user-info", {}),
                    ("GET", "/dolphinscheduler/users/list", {}),
                ]
            )
            if ds_version == "3.4.3"
            else [
                ("GET", "/dolphinscheduler/users/list", {}),
                ("GET", "/dolphinscheduler/users/list-all", {}),
            ]
        ),
        (
            "GET",
            "/dolphinscheduler/users/list-paging",
            {
                "pageNo": "1",
                "pageSize": "100",
                "searchVal": "alice",
            },
        ),
    ]


@pytest.mark.parametrize("ds_version", TARGET_DS_VERSIONS)
def test_explicit_expire_time_update_boundary_comes_from_exact_recipe(
    ds_version: str,
) -> None:
    assert access_token_update_requires_expire_time(ds_version) is (
        ds_version == "1.3.9" or ds_version.startswith("2.0.")
    )


@pytest.mark.parametrize(
    ("selected_version", "client_version"),
    [("3.4.2", "3.4.2"), ("3.4.1", "3.4.2")],
)
def test_token_binding_rejects_mismatched_exact_profile_without_io(
    selected_version: str,
    client_version: str,
) -> None:
    adapter = AccessTokenAdapter.for_version("3.4.1")
    profile = make_profile(ds_version=selected_version)
    with (
        DolphinSchedulerClient(
            make_profile(ds_version=client_version),
            transport=httpx.MockTransport(_unexpected_http),
        ) as http_client,
        pytest.raises(WireContractError, match="client profile"),
    ):
        adapter.bind(profile, http_client=http_client)


def test_user_lookup_rejects_mismatched_exact_client_without_io() -> None:
    with (
        DolphinSchedulerClient(
            make_profile(ds_version="3.4.2"),
            transport=httpx.MockTransport(_unexpected_http),
        ) as http_client,
        pytest.raises(WireContractError, match="client profile"),
    ):
        bind_user_lookup(make_profile(ds_version="3.4.1"), http_client=http_client)


@pytest.mark.parametrize("ds_version", TARGET_DS_VERSIONS[1:])
def test_modern_access_token_domain_uses_main_routes_and_verifies_lifecycle(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    current: dict[str, object] | None = None
    mutations: list[tuple[str, str, dict[str, list[str]]]] = []
    generated_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal current, generated_count
        path = request.url.path
        form = parse_qs(request.content.decode())
        if request.method == "GET" and path in {
            "/dolphinscheduler/users/list",
            "/dolphinscheduler/users/list-all",
            "/dolphinscheduler/users/get-user-info",
        }:
            return _success(_user() if path.endswith("get-user-info") else [_user()])
        if request.method == "GET" and path == "/dolphinscheduler/users/list-paging":
            return _success(_page(_user()))
        if request.method == "GET" and path == "/dolphinscheduler/access-tokens":
            return _success(_page(*([] if current is None else [current])))
        if (
            request.method == "POST"
            and path == "/dolphinscheduler/access-tokens/generate"
        ):
            mutations.append((request.method, path, form))
            generated_count += 1
            return _success(f"generated-{generated_count}")
        if request.method == "POST" and path == "/dolphinscheduler/access-tokens":
            mutations.append((request.method, path, form))
            current = _access_token(
                token_id=11,
                token=form.get("token", ["server-generated"])[0],
                expire_time=_serialized_expire_time(
                    ds_version,
                    form["expireTime"][0],
                ),
            )
            return _success(None if ds_version in {"2.0.0", "2.0.1"} else current)
        if request.method == "PUT" and path == "/dolphinscheduler/access-tokens/11":
            mutations.append((request.method, path, form))
            current = _access_token(
                token_id=11,
                token=form.get("token", ["server-regenerated"])[0],
                expire_time=_serialized_expire_time(
                    ds_version,
                    form["expireTime"][0],
                ),
            )
            return _success(None if ds_version in {"2.0.0", "2.0.1"} else current)
        if request.method == "DELETE" and path == "/dolphinscheduler/access-tokens/11":
            mutations.append((request.method, path, form))
            current = None
            return _success(
                None
                if ds_version
                not in {
                    "3.2.1",
                    "3.2.2",
                    "3.3.1",
                    "3.3.2",
                    "3.4.0",
                    "3.4.1",
                    "3.4.2",
                    "3.4.3",
                }
                else False
            )
        message = f"unexpected request {request.method} {path}"
        raise AssertionError(message)

    adapter = AccessTokenAdapter.for_version(ds_version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        domain = adapter.bind(profile, http_client=http_client)
        generated = domain.generate(
            user="alice",
            expire_time="2027-01-01 00:00:00",
        )
        created = domain.create(
            user="alice",
            expire_time="2027-01-01 00:00:00",
            token=None,
        )
        assert created.record.id is not None
        updated = domain.update(
            created.record.id,
            user=None,
            expire_time="2027-02-01 00:00:00",
            token=None,
            regenerate_token=True,
        )
        assert updated.record.id is not None
        deleted = domain.delete(updated.record.id)

    assert generated.token == "generated-1"
    assert created.record.token == (
        "generated-2" if ds_version in {"2.0.0", "2.0.1"} else "server-generated"
    )
    assert updated.record.token == (
        "generated-3" if ds_version in {"2.0.0", "2.0.1"} else "server-regenerated"
    )
    assert deleted.deleted is True
    expected_mutations = [
        ("POST", "/dolphinscheduler/access-tokens/generate"),
        ("POST", "/dolphinscheduler/access-tokens"),
        ("PUT", "/dolphinscheduler/access-tokens/11"),
        ("DELETE", "/dolphinscheduler/access-tokens/11"),
    ]
    if ds_version in {"2.0.0", "2.0.1"}:
        expected_mutations.insert(
            1, ("POST", "/dolphinscheduler/access-tokens/generate")
        )
        expected_mutations.insert(
            3, ("POST", "/dolphinscheduler/access-tokens/generate")
        )
    assert [(method, path) for method, path, _form in mutations] == expected_mutations
    assert all("access-tokens-v2" not in path for _method, path, _form in mutations)


@pytest.mark.parametrize("ds_version", TARGET_DS_VERSIONS)
def test_exact_access_token_profiles_preserve_query_and_stable_page(
    ds_version: str,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == (
            "/dolphinscheduler/access-token/list-paging"
            if ds_version == "1.3.9"
            else "/dolphinscheduler/access-tokens"
        )
        assert parse_qs(request.url.query.decode(), strict_parsing=True) == {
            "pageNo": ["2"],
            "pageSize": ["7"],
            "searchVal": ["alice"],
        }
        page = _page(
            _access_token(
                token_id=11,
                token="generated-token",
                expire_time="2027-01-01 00:00:00",
            )
        )
        page.update({"pageSize": 999, "currentPage": 3, "pageNo": 2})
        return _success(page)

    profile = make_profile(ds_version=ds_version)
    adapter = AccessTokenAdapter.for_version(ds_version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        page = adapter.bind(profile, http_client=http_client).list(
            page_no=2,
            page_size=7,
            search="alice",
        )

    assert page.totalList is not None
    assert [(item.id, item.userName) for item in page.totalList] == [(11, "alice")]
    assert page.pageSize == 7
    assert page.currentPage == 3
    assert page.pageNo == 3


def test_legacy_access_token_domain_generates_required_token_on_main_routes() -> None:
    profile = make_profile(ds_version="1.3.9")
    wire = _LegacyTokenWire()
    adapter = AccessTokenAdapter.for_version("1.3.9")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(wire),
    )

    with http_client:
        domain = adapter.bind(profile, http_client=http_client)
        created = domain.create(
            user="alice",
            expire_time="2027-01-01 00:00:00",
            token=None,
        )
        assert created.record.id is not None
        updated = domain.update(
            created.record.id,
            user=None,
            expire_time="2027-02-01 00:00:00",
            token=None,
            regenerate_token=True,
        )
        assert updated.record.id is not None
        deleted = domain.delete(updated.record.id)

    assert created.record.token == "legacy-generated-1"
    assert updated.record.token == "legacy-generated-2"
    assert deleted.deleted is True
    assert wire.mutations == [
        (
            "/dolphinscheduler/access-token/generate",
            {"userId": ["7"], "expireTime": ["2027-01-01 00:00:00"]},
        ),
        (
            "/dolphinscheduler/access-token/create",
            {
                "userId": ["7"],
                "expireTime": ["2027-01-01 00:00:00"],
                "token": ["legacy-generated-1"],
            },
        ),
        (
            "/dolphinscheduler/access-token/generate",
            {"userId": ["7"], "expireTime": ["2027-02-01 00:00:00"]},
        ),
        (
            "/dolphinscheduler/access-token/update",
            {
                "id": ["11"],
                "userId": ["7"],
                "expireTime": ["2027-02-01 00:00:00"],
                "token": ["legacy-generated-2"],
            },
        ),
        (
            "/dolphinscheduler/access-token/delete",
            {"id": ["11"]},
        ),
    ]


def test_legacy_token_readback_uses_native_date_without_timezone_guess() -> None:
    profile = make_profile(ds_version="1.3.9")
    wire = _LegacyTokenWire(
        serialized_expire_times={
            "2027-01-01 08:00:00": "2027-01-01T00:00:00.000+0000",
            "2027-02-01 08:00:00": "2027-02-01T00:00:00.000+0000",
        }
    )
    adapter = AccessTokenAdapter.for_version("1.3.9")

    with DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(wire),
    ) as http_client:
        domain = adapter.bind(profile, http_client=http_client)
        created = domain.create(
            user="alice",
            expire_time="2027-01-01 08:00:00",
            token=None,
        )
        token_id = created.record.id
        assert isinstance(token_id, int)
        updated = domain.update(
            token_id,
            user=None,
            expire_time="2027-02-01 08:00:00",
            token=None,
            regenerate_token=True,
        )

    assert created.record.expireTime == "2027-01-01T00:00:00.000+0000"
    assert created.record.token == "legacy-generated-1"
    assert updated.record.expireTime == "2027-02-01T00:00:00.000+0000"
    assert updated.record.token == "legacy-generated-2"


def test_legacy_update_accepts_same_instant_in_native_date_representation() -> None:
    profile = make_profile(ds_version="1.3.9")
    wire = _LegacyTokenWire(
        serialized_expire_times={
            "2027-01-01 08:00:00": "2027-01-01T00:00:00.000+0000",
        },
    )
    adapter = AccessTokenAdapter.for_version("1.3.9")

    with DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(wire),
    ) as http_client:
        domain = adapter.bind(profile, http_client=http_client)
        created = domain.create(
            user="alice",
            expire_time="2027-01-01 08:00:00",
            token=None,
        )
        token_id = created.record.id
        assert isinstance(token_id, int)
        updated = domain.update(
            token_id,
            user=None,
            expire_time="2027-01-01 08:00:00",
            token="replacement-token",
            regenerate_token=False,
        )

    assert updated.record.expireTime == "2027-01-01T00:00:00.000+0000"
    assert updated.record.token == "replacement-token"


def test_legacy_update_requires_explicit_native_expire_time_before_write() -> None:
    profile = make_profile(ds_version="1.3.9")
    wire = _LegacyTokenWire(
        current=_access_token(
            token_id=11,
            token="current-token",
            expire_time="2027-01-01T00:00:00.000+0000",
        )
    )
    adapter = AccessTokenAdapter.for_version("1.3.9")

    with DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(wire),
    ) as http_client:
        domain = adapter.bind(profile, http_client=http_client)
        with pytest.raises(UserInputError, match="explicit expire time") as caught:
            domain.update(
                11,
                user=None,
                expire_time=None,
                token="replacement-token",
                regenerate_token=False,
            )

    assert "--expire-time" in (caught.value.suggestion or "")
    assert wire.mutations == []


@pytest.mark.parametrize("ds_version", ["1.3.9", "2.0.0", "3.4.1"])
@pytest.mark.parametrize("operation", ["create", "update", "delete", "generate"])
def test_token_posts_and_writes_are_once_only_without_failure_readback(
    ds_version: str,
    operation: str,
) -> None:
    profile = make_profile(ds_version=ds_version).model_copy(
        update={"api_retry_attempts": 4, "api_retry_backoff_ms": 0}
    )
    requests: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append((request.method, request.url.path))
        if request.method != "GET":
            message = "response lost"
            raise httpx.ReadError(message, request=request)
        if "/users/" in request.url.path:
            user = _legacy_user() if ds_version == "1.3.9" else _user()
            return _success(
                _page(user) if request.url.path.endswith("list-paging") else [user]
            )
        return _success(
            _page(_access_token(token_id=11, token="old", expire_time="old-expiry"))
        )

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as http_client:
        domain = AccessTokenAdapter.for_version(ds_version).bind(
            profile, http_client=http_client
        )

        def dispatch() -> None:
            if operation == "create":
                domain.create(user="alice", expire_time="new-expiry", token="new")
            elif operation == "update":
                domain.update(
                    11,
                    user=None,
                    expire_time="new-expiry",
                    token="new",
                    regenerate_token=False,
                )
            elif operation == "delete":
                domain.delete(11)
            else:
                domain.generate(user="alice", expire_time="new-expiry")

        with pytest.raises(ApiTransportError) as caught:
            dispatch()

    assert len([method for method, _path in requests if method != "GET"]) == 1
    assert requests[-1][0] != "GET"
    if operation == "generate":
        assert "phase" not in caught.value.details
        assert "mutation_may_have_applied" not in caught.value.details
        assert all("access-token" not in path for _method, path in requests[:-1])
    else:
        assert caught.value.details["phase"] == "mutation_request"
        assert caught.value.details["mutation_may_have_applied"] is True


def test_delete_rejects_malformed_result_without_readback() -> None:
    profile = make_profile(ds_version="3.4.1")
    requests: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request.method)
        if request.method == "DELETE":
            return _success({"unexpected": True})
        assert request.method == "GET"
        return _success(
            _page(_access_token(token_id=11, token="old", expire_time="old-expiry"))
        )

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as http_client:
        domain = AccessTokenAdapter.for_version("3.4.1").bind(
            profile, http_client=http_client
        )
        with pytest.raises(ApiTransportError) as caught:
            domain.delete(11)

    assert requests == ["GET", "DELETE"]
    assert caught.value.details["phase"] == "mutation_response"
    assert caught.value.details["mutation_applied"] is True


def test_false_delete_result_still_requires_fresh_absence_readback() -> None:
    profile = make_profile(ds_version="3.4.1")
    requests: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request.method)
        if request.method == "DELETE":
            return _success(False)
        assert request.method == "GET"
        return _success(
            _page(_access_token(token_id=11, token="old", expire_time="old-expiry"))
        )

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as http_client:
        domain = AccessTokenAdapter.for_version("3.4.1").bind(
            profile, http_client=http_client
        )
        with pytest.raises(ApiTransportError) as caught:
            domain.delete(11)

    assert requests == ["GET", "DELETE", "GET"]
    assert caught.value.details["phase"] == "readback"
    assert caught.value.details["mutation_applied"] is True


@pytest.mark.parametrize("result", [None, "", False, {"token": "not-a-string"}])
def test_generate_rejects_invalid_token_without_persistence_or_readback(
    result: object,
) -> None:
    profile = make_profile(ds_version="3.4.1")
    requests: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append((request.method, request.url.path))
        if request.method == "POST":
            assert request.url.path == "/dolphinscheduler/access-tokens/generate"
            return _success(result)
        assert "/users/" in request.url.path
        return _success(
            _page(_user()) if request.url.path.endswith("list-paging") else [_user()]
        )

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as http_client:
        domain = AccessTokenAdapter.for_version("3.4.1").bind(
            profile, http_client=http_client
        )
        with pytest.raises(ApiTransportError) as caught:
            domain.generate(user="alice", expire_time="new-expiry")

    assert requests[-1][0] == "POST"
    assert len([method for method, _path in requests if method == "POST"]) == 1
    assert "mutation_applied" not in caught.value.details
    assert "mutation_may_have_applied" not in caught.value.details


def test_unchanged_update_resolves_user_then_rejects_without_mutation() -> None:
    profile = make_profile(ds_version="3.4.1")
    paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        paths.append(request.url.path)
        if "/users/" in request.url.path:
            return _success(
                _page(_user())
                if request.url.path.endswith("list-paging")
                else [_user()]
            )
        return _success(
            _page(_access_token(token_id=11, token="old", expire_time="old-expiry"))
        )

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as http_client:
        domain = AccessTokenAdapter.for_version("3.4.1").bind(
            profile, http_client=http_client
        )
        with pytest.raises(UserInputError, match="at least one field change"):
            domain.update(
                11, user=None, expire_time=None, token=None, regenerate_token=False
            )

    assert paths == [
        "/dolphinscheduler/access-tokens",
        "/dolphinscheduler/users/list",
        "/dolphinscheduler/users/list-all",
        "/dolphinscheduler/users/list-paging",
    ]


@dataclass
class _LegacyTokenWire:
    current: dict[str, object] | None = None
    generated_count: int = 0
    mutations: list[tuple[str, dict[str, list[str]]]] = field(default_factory=list)
    serialized_expire_times: dict[str, str] = field(default_factory=dict)

    def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        form = parse_qs(request.content.decode())
        if request.method == "GET" and path in {
            "/dolphinscheduler/users/list",
            "/dolphinscheduler/users/list-all",
        }:
            return _success([_legacy_user()])
        if request.method == "GET" and path == "/dolphinscheduler/users/list-paging":
            return _success(_page(_legacy_user()))
        if (
            request.method == "GET"
            and path == "/dolphinscheduler/access-token/list-paging"
        ):
            return _success(_page(*([] if self.current is None else [self.current])))
        if (
            request.method == "POST"
            and path == "/dolphinscheduler/access-token/generate"
        ):
            self._record(path, form)
            self.generated_count += 1
            return _success(f"legacy-generated-{self.generated_count}")
        if request.method == "POST" and path == "/dolphinscheduler/access-token/create":
            self._record(path, form)
            self.current = _access_token(
                token_id=11,
                token=form["token"][0],
                expire_time=self._serialize_expire_time(form["expireTime"][0]),
            )
            return _success(None)
        if request.method == "POST" and path == "/dolphinscheduler/access-token/update":
            self._record(path, form)
            self.current = _access_token(
                token_id=int(form["id"][0]),
                token=form["token"][0],
                expire_time=self._serialize_expire_time(form["expireTime"][0]),
            )
            return _success(None)
        if request.method == "POST" and path == "/dolphinscheduler/access-token/delete":
            self._record(path, form)
            self.current = None
            return _success(None)
        message = f"unexpected request {request.method} {path}"
        raise AssertionError(message)

    def _record(self, path: str, form: dict[str, list[str]]) -> None:
        self.mutations.append((path, form))

    def _serialize_expire_time(self, requested: str) -> str:
        return self.serialized_expire_times.get(requested, requested)


def _serialized_expire_time(ds_version: str, requested: str) -> str:
    if not ds_version.startswith("2.0."):
        return requested
    return {
        "2027-01-01 00:00:00": "2027-01-01T00:00:00.000+0000",
        "2027-02-01 00:00:00": "2027-02-01T00:00:00.000+0000",
    }[requested]


def _unexpected_http(request: httpx.Request) -> httpx.Response:
    message = f"unexpected request {request.method} {request.url.path}"
    raise AssertionError(message)


def _success(data: object) -> httpx.Response:
    return httpx.Response(200, json={"code": 0, "msg": "success", "data": data})


def _page(*items: dict[str, object]) -> dict[str, object]:
    return {
        "totalList": list(items),
        "total": len(items),
        "totalPage": 0 if not items else 1,
        "pageSize": 100,
        "currentPage": 1,
        "pageNo": 1,
    }


def _user() -> dict[str, object]:
    return {
        "id": 7,
        "userName": "alice",
        "email": "alice@example.com",
        "phone": None,
        "userType": "GENERAL_USER",
        "tenantId": 11,
        "tenantCode": "tenant-prod",
        "queueName": "default",
        "queue": "",
        "state": 1,
        "timeZone": "Asia/Shanghai",
        "createTime": None,
        "updateTime": None,
    }


def _legacy_user() -> dict[str, object]:
    user = _user()
    user.pop("state")
    user.pop("timeZone")
    return user


def _access_token(
    *,
    token_id: int,
    token: str,
    expire_time: str,
) -> dict[str, object]:
    return {
        "id": token_id,
        "userId": 7,
        "token": token,
        "expireTime": expire_time,
        "createTime": None,
        "updateTime": None,
        "userName": "alice",
    }
