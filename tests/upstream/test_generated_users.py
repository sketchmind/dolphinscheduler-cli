from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import parse_qs

import httpx
import pytest

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import (
    ApiTransportError,
    UnsupportedFeatureError,
    UserInputError,
)
from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.upstream.users import (
    UserAdapter,
)
from tests.support import make_profile

# Reviewed UsersService[Impl].grantProject semantics, not inferred from codecs.
_PROJECT_REPLACE_VERSIONS = {
    "1.3.9",
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
}
_PROJECT_INSERT_ONLY_VERSIONS = {
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
}


@pytest.mark.parametrize("ds_version", TARGET_DS_VERSIONS)
def test_user_domain_executes_crud_with_preservation_and_readback(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    has_state = ds_version != "1.3.9"
    has_time_zone = ds_version not in {
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
    }
    current: dict[str, object] | None = None
    mutations: list[tuple[str, str, dict[str, list[str]]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal current
        form = parse_qs(request.content.decode())
        path = request.url.path
        if request.method == "GET" and path == (
            "/dolphinscheduler/tenant/list-paging"
            if ds_version == "1.3.9"
            else "/dolphinscheduler/tenants"
        ):
            return _success(_page(_tenant()))
        if ds_version == "3.4.3" and path == "/dolphinscheduler/users/get-user-info":
            return _success(
                _alice() | {"id": 1, "userName": "admin", "userType": "ADMIN_USER"}
            )
        if ds_version == "3.4.3" and path == "/dolphinscheduler/users/list-all":
            return _success(
                []
                if current is None
                else [{"id": current["id"], "userName": current["userName"]}]
            )
        if request.method == "GET" and path in {
            "/dolphinscheduler/users/list",
            "/dolphinscheduler/users/list-all",
        }:
            return _success([] if current is None else [current])
        if request.method == "GET" and path == "/dolphinscheduler/users/list-paging":
            return _success(_page(*([] if current is None else [current])))
        if request.method == "POST" and path == "/dolphinscheduler/users/create":
            mutations.append((request.method, path, form))
            current = _user(
                user_id=7,
                name=form["userName"][0],
                email=form["email"][0],
                phone=form.get("phone", [None])[0],
                queue=form.get("queue", [""])[0],
                state=int(form.get("state", ["1"])[0]),
            )
            return _success(
                None if ds_version in {"1.3.9", "2.0.0", "2.0.1", "3.2.0"} else current
            )
        if request.method == "POST" and path == "/dolphinscheduler/users/update":
            mutations.append((request.method, path, form))
            current = _user(
                user_id=int(form["id"][0]),
                name=form["userName"][0],
                email=form["email"][0],
                phone=form.get("phone", [None])[0],
                queue=form.get("queue", [""])[0],
                state=int(form.get("state", ["1"])[0]),
                time_zone=form.get("timeZone", [None])[0],
            )
            return _success(
                None
                if ds_version in TARGET_DS_VERSIONS[: TARGET_DS_VERSIONS.index("3.2.1")]
                else current
            )
        if request.method == "POST" and path == "/dolphinscheduler/users/delete":
            mutations.append((request.method, path, form))
            current = None
            return _success(None)
        message = f"unexpected request {request.method} {path}"
        raise AssertionError(message)

    adapter = UserAdapter.for_version(ds_version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        domain = adapter.bind(profile, http_client=http_client)
        created = domain.create(
            user_name="alice",
            password="secret-value",
            email="alice@example.com",
            tenant="tenant-prod",
            state=1,
            phone="13800138000",
            queue="root.analytics",
        )
        updated = domain.update(
            "alice",
            user_name=None,
            password=None,
            email="alice+ops@example.com",
            tenant=None,
            state=None,
            phone=None,
            preserve_phone=True,
            queue=None,
            preserve_queue=True,
            time_zone="Asia/Shanghai" if has_time_zone else None,
        )
        cleared = domain.update(
            "alice",
            user_name=None,
            password=None,
            email=None,
            tenant=None,
            state=None,
            phone=None,
            preserve_phone=False,
            queue=None,
            preserve_queue=True,
            time_zone=None,
        )
        deleted = domain.delete("alice")

    assert created.id == 7
    assert updated.record.email == "alice+ops@example.com"
    assert updated.record.phone == "13800138000"
    assert updated.record.storedQueue == "root.analytics"
    assert updated.record.timeZone == ("Asia/Shanghai" if has_time_zone else None)
    assert cleared.record.phone is None
    assert deleted.deleted is True
    assert [path for _method, path, _form in mutations] == [
        "/dolphinscheduler/users/create",
        "/dolphinscheduler/users/update",
        "/dolphinscheduler/users/update",
        "/dolphinscheduler/users/delete",
    ]
    assert mutations[1][2]["userName"] == ["alice"]
    assert mutations[1][2]["phone"] == ["13800138000"]
    assert mutations[1][2]["queue"] == ["root.analytics"]
    assert ("state" in mutations[0][2]) is has_state
    assert ("state" in mutations[1][2]) is has_state
    assert ("timeZone" in mutations[1][2]) is has_time_zone
    assert mutations[1][2].get("userPassword", [""]) == [""]
    assert "phone" not in mutations[2][2]
    assert mutations[3][2] == {"id": ["7"]}


def test_user_delete_form_stays_once_only_with_transport_retries() -> None:
    profile = make_profile(ds_version="3.4.2").model_copy(
        update={"api_retry_attempts": 4, "api_retry_backoff_ms": 0}
    )
    delete_attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal delete_attempts
        if (
            request.method == "GET"
            and request.url.path == "/dolphinscheduler/users/list-paging"
        ):
            return _success(_page(_alice()))
        if (
            request.method == "POST"
            and request.url.path == "/dolphinscheduler/users/delete"
        ):
            delete_attempts += 1
            assert parse_qs(request.content.decode(), strict_parsing=True) == {
                "id": ["7"]
            }
            return httpx.Response(503, json={"message": "unavailable"})
        message = f"unexpected request {request.method} {request.url.path}"
        raise AssertionError(message)

    adapter = UserAdapter.for_version("3.4.2")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client, pytest.raises(ApiTransportError) as exc_info:
        adapter.bind(profile, http_client=http_client).delete("alice")

    assert delete_attempts == 1
    assert exc_info.value.details["phase"] == "mutation_request"
    assert exc_info.value.details["mutation_may_have_applied"] is True
    assert exc_info.value.details["attempts"] == 1
    assert exc_info.value.details["max_attempts"] == 1
    assert exc_info.value.details["request_replay_safe"] is False
    assert exc_info.value.details["retryable"] is True


def test_user_delete_rejects_a_blank_selector_before_http() -> None:
    requests_seen = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal requests_seen
        requests_seen += 1
        return _success(None)

    profile = make_profile(ds_version="3.4.2")
    adapter = UserAdapter.for_version("3.4.2")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client, pytest.raises(UserInputError, match="must not be empty"):
        adapter.bind(profile, http_client=http_client).delete("  ")

    assert requests_seen == 0


@pytest.mark.parametrize(
    ("ds_version", "operation", "mutation_path"),
    [
        ("3.4.2", "create", "create"),
        ("3.4.2", "update", "update"),
        ("1.3.9", "grant_project", "grant-project"),
        ("1.3.9", "revoke_project", "grant-project"),
        ("2.0.9", "revoke_project", "revoke-project"),
        ("3.4.2", "grant_project", "grant-project"),
        ("3.4.2", "revoke_project", "revoke-project-by-id"),
        ("3.4.2", "datasource", "grant-datasource"),
        ("3.0.0", "namespace", "grant-namespace"),
        ("3.0.6", "namespace", "grant-namespace"),
        ("3.0.0", "namespace_revoke", "grant-namespace"),
        ("3.0.6", "namespace_revoke", "grant-namespace"),
        ("3.4.2", "namespace", "grant-namespace"),
    ],
)
def test_user_writes_are_once_only_and_never_read_back_a_failed_request(
    ds_version: str,
    operation: str,
    mutation_path: str,
) -> None:
    profile = make_profile(ds_version=ds_version).model_copy(
        update={"api_retry_attempts": 4, "api_retry_backoff_ms": 0}
    )
    wire = _PermissionWire.make(ds_version)
    writes: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert not writes, "a failed mutation must neither replay nor read back"
        if request.method == "POST":
            writes.append(request.url.path)
            return httpx.Response(503, json={"message": "unavailable"})
        return wire(request)

    client = DolphinSchedulerClient(profile, transport=httpx.MockTransport(handler))
    domain = UserAdapter.for_version(ds_version).bind(profile, http_client=client)

    def invoke() -> None:
        if operation == "create":
            domain.create(
                user_name="bob",
                password="secret",
                email="bob@example.com",
                tenant="tenant-prod",
                state=1,
                phone=None,
                queue=None,
            )
        elif operation == "update":
            domain.update(
                "alice",
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
        elif operation == "grant_project":
            domain.grant_project("alice", "etl-prod")
        elif operation == "revoke_project":
            domain.revoke_project("alice", "existing-project")
        elif operation == "datasource":
            domain.change_datasources("alice", ["events"], grant=True)
        else:
            domain.change_namespaces(
                "alice",
                [
                    "existing-namespace"
                    if operation == "namespace_revoke"
                    else "batch-jobs"
                ],
                grant=operation != "namespace_revoke",
            )

    with client, pytest.raises(ApiTransportError) as exc_info:
        invoke()

    assert writes == [f"/dolphinscheduler/users/{mutation_path}"]
    assert exc_info.value.details["phase"] == "mutation_request"
    assert exc_info.value.details["mutation_may_have_applied"] is True
    assert exc_info.value.details["attempts"] == 1
    assert exc_info.value.details["max_attempts"] == 1
    assert exc_info.value.details["request_replay_safe"] is False
    assert exc_info.value.details["retryable"] is True


@pytest.mark.parametrize("ds_version", TARGET_DS_VERSIONS)
def test_user_permissions_replace_sets_and_verify_readback(ds_version: str) -> None:
    profile = make_profile(ds_version=ds_version)
    wire = _PermissionWire.make(ds_version)

    adapter = UserAdapter.for_version(ds_version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(wire),
    )
    with http_client:
        domain = adapter.bind(profile, http_client=http_client)
        granted_project = domain.grant_project("alice", "etl-prod")
        if ds_version not in {"2.0.0", "2.0.1"}:
            revoked_project = domain.revoke_project("alice", "etl-prod")
            assert revoked_project.project.id == 32
        granted_datasources = domain.change_datasources("alice", ["events"], grant=True)
        revoked_datasources = domain.change_datasources(
            "alice", ["warehouse"], grant=False
        )
        if ds_version not in {
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
        }:
            granted_namespaces = domain.change_namespaces(
                "alice", ["batch-jobs"], grant=True
            )
            revoked_namespaces = domain.change_namespaces(
                "alice", ["existing-namespace"], grant=False
            )
            assert [item.id for item in granted_namespaces.final] == [52, 51]
            assert [item.id for item in revoked_namespaces.final] == [52]

    assert granted_project.project.id == 32
    assert [item.id for item in granted_datasources.final] == [42, 41]
    assert [item.id for item in revoked_datasources.final] == [42]
    expected = [
        (
            "/dolphinscheduler/users/grant-project",
            {
                "userId": ["7"],
                "projectIds": [
                    "31,32" if ds_version in _PROJECT_REPLACE_VERSIONS else "32"
                ],
            },
        ),
    ]
    if ds_version == "1.3.9":
        expected.append(
            (
                "/dolphinscheduler/users/grant-project",
                {"userId": ["7"], "projectIds": ["31"]},
            )
        )
    elif ds_version in {
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
    }:
        expected.append(
            (
                "/dolphinscheduler/users/revoke-project",
                {"userId": ["7"], "projectCode": ["302"]},
            )
        )
    elif ds_version not in {"2.0.0", "2.0.1"}:
        expected.append(
            (
                "/dolphinscheduler/users/revoke-project-by-id",
                {"userId": ["7"], "projectIds": ["32"]},
            )
        )
    expected.extend(
        [
            (
                "/dolphinscheduler/users/grant-datasource",
                {"userId": ["7"], "datasourceIds": ["41,42"]},
            ),
            (
                "/dolphinscheduler/users/grant-datasource",
                {"userId": ["7"], "datasourceIds": ["42"]},
            ),
        ]
    )
    if ds_version not in {
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
    }:
        expected.extend(
            [
                (
                    "/dolphinscheduler/users/grant-namespace",
                    {"userId": ["7"], "namespaceIds": ["51,52"]},
                ),
                (
                    "/dolphinscheduler/users/grant-namespace",
                    {"userId": ["7"], "namespaceIds": ["52"]},
                ),
            ]
        )
    assert wire.mutations == expected


@pytest.mark.parametrize(
    "ds_version", ["3.0.0", "3.0.1", "3.0.2", "3.0.3", "3.0.4", "3.0.5", "3.0.6"]
)
@pytest.mark.parametrize("grant", [True, False])
def test_30_namespace_permissions_preserve_native_absent_cluster_identity(
    ds_version: str, *, grant: bool
) -> None:
    # 3.0.x DAO K8sNamespace exposes k8s, not clusterCode/clusterName.
    profile = make_profile(ds_version=ds_version)
    wire = _PermissionWire.make(ds_version)
    for namespace in wire.namespaces.values():
        namespace.pop("clusterCode")
        namespace.pop("clusterName")
        namespace["k8s"] = "native-k8s-config"
    if not grant:
        wire.authorized_namespaces = {51, 52}
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return wire(request)

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        domain = UserAdapter.for_version(ds_version).bind(profile, http_client=client)
        change = domain.change_namespaces(
            "alice", ["batch-jobs" if grant else "existing-namespace"], grant=grant
        )

    assert [item.id for item in change.requested] == ([52] if grant else [51])
    assert [item.id for item in change.final] == ([52, 51] if grant else [52])
    assert all(
        item.to_data()["clusterCode"] is None and item.to_data()["clusterName"] is None
        for item in (*change.requested, *change.final)
    )
    assert wire.mutations == [
        (
            "/dolphinscheduler/users/grant-namespace",
            {"userId": ["7"], "namespaceIds": ["51,52" if grant else "52"]},
        )
    ]
    assert [
        (request.method, request.url.path)
        for request in requests
        if "namespace" in request.url.path
    ] == [
        ("GET", "/dolphinscheduler/k8s-namespace/authed-namespace"),
        ("GET", "/dolphinscheduler/k8s-namespace/unauth-namespace"),
        ("POST", "/dolphinscheduler/users/grant-namespace"),
        ("GET", "/dolphinscheduler/k8s-namespace/authed-namespace"),
    ]


@pytest.mark.parametrize("ds_version", ["3.0.0", "3.0.6"])
@pytest.mark.parametrize("grant", [True, False])
def test_30_namespace_permission_readback_detects_lost_memberships(
    ds_version: str, *, grant: bool
) -> None:
    profile = make_profile(ds_version=ds_version)
    wire = _PermissionWire.make(ds_version)
    for namespace in wire.namespaces.values():
        namespace.pop("clusterCode")
        namespace.pop("clusterName")
    if not grant:
        wire.authorized_namespaces = {51, 52}

    def handler(request: httpx.Request) -> httpx.Response:
        response = wire(request)
        if request.method == "POST":
            # The acknowledgement must not conceal dropped preserved memberships.
            wire.authorized_namespaces = set()
        return response

    with (
        DolphinSchedulerClient(
            profile, transport=httpx.MockTransport(handler)
        ) as client,
        pytest.raises(ApiTransportError, match="could not be verified") as caught,
    ):
        UserAdapter.for_version(ds_version).bind(
            profile, http_client=client
        ).change_namespaces(
            "alice", ["batch-jobs" if grant else "existing-namespace"], grant=grant
        )

    assert len(wire.mutations) == 1
    assert caught.value.details["phase"] == "readback"
    assert caught.value.details["mutation_applied"] is True
    assert caught.value.details["expected_ids"] == ([51, 52] if grant else [52])
    assert caught.value.details["actual_ids"] == []
    operation = "grant" if grant else "revoke"
    assert caught.value.suggestion == (
        "Inspect the namespace before deciding whether to retry namespace "
        f"{operation}; "
        "do not blindly repeat the mutation."
    )


@pytest.mark.parametrize("ds_version", TARGET_DS_VERSIONS)
def test_existing_project_membership_is_not_write_permission(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    wire = _PermissionWire.make(ds_version)
    wire.authorized_projects = {31, 32}
    client = DolphinSchedulerClient(profile, transport=httpx.MockTransport(wire))
    domain = UserAdapter.for_version(ds_version).bind(profile, http_client=client)

    with client:
        if ds_version in _PROJECT_INSERT_ONLY_VERSIONS | _PROJECT_REPLACE_VERSIONS:
            result = domain.grant_project("alice", "etl-prod")
            assert result.project.id == 32
            assert wire.mutations == []
        else:
            result = domain.grant_project("alice", "etl-prod")
            assert result.project.id == 32
            assert wire.mutations == [
                (
                    "/dolphinscheduler/users/grant-project",
                    {
                        "userId": ["7"],
                        "projectIds": ["32"],
                    },
                )
            ]
    assert wire.authorized_projects == {31, 32}


@pytest.mark.parametrize("ds_version", sorted(_PROJECT_REPLACE_VERSIONS))
def test_project_grant_rejects_loss_of_an_unrelated_existing_membership(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    wire = _PermissionWire.make(ds_version)

    def handler(request: httpx.Request) -> httpx.Response:
        response = wire(request)
        if request.method == "POST":
            # Server acknowledged the target, but dropped a preserved grant.
            wire.authorized_projects = {32}
        return response

    client = DolphinSchedulerClient(profile, transport=httpx.MockTransport(handler))
    domain = UserAdapter.for_version(ds_version).bind(profile, http_client=client)
    with (
        client,
        pytest.raises(ApiTransportError, match="could not be verified") as error,
    ):
        domain.grant_project("alice", "etl-prod")
    assert error.value.details["expected_ids"] == [31, 32]
    assert error.value.details["actual_ids"] == [32]
    assert error.value.details["phase"] == "readback"


def test_project_revoke_replacement_verifies_preserved_memberships() -> None:
    profile = make_profile(ds_version="1.3.9")
    wire = _PermissionWire.make("1.3.9")
    wire.authorized_projects = {31, 32}

    def handler(request: httpx.Request) -> httpx.Response:
        response = wire(request)
        if request.method == "POST":
            wire.authorized_projects = set()
        return response

    client = DolphinSchedulerClient(profile, transport=httpx.MockTransport(handler))
    domain = UserAdapter.for_version("1.3.9").bind(profile, http_client=client)
    with (
        client,
        pytest.raises(ApiTransportError, match="could not be verified") as error,
    ):
        domain.revoke_project("alice", "etl-prod")
    assert error.value.details["expected_ids"] == [31]
    assert error.value.details["actual_ids"] == []


def test_existing_project_member_upgrade_still_reports_failed_write() -> None:
    profile = make_profile(ds_version="3.4.1")
    wire = _PermissionWire.make("3.4.1")
    writes: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert not writes, "must not verify or replay a failed upgrade"
        if request.method == "POST":
            writes.append(request.url.path)
            return httpx.Response(503)
        return wire(request)

    client = DolphinSchedulerClient(profile, transport=httpx.MockTransport(handler))
    domain = UserAdapter.for_version("3.4.1").bind(profile, http_client=client)
    with client, pytest.raises(ApiTransportError) as error:
        domain.grant_project("alice", "existing-project")
    assert writes == ["/dolphinscheduler/users/grant-project"]
    assert error.value.details["phase"] == "mutation_request"


@pytest.mark.parametrize(
    ("ds_version", "operation"),
    [
        ("2.0.0", "revoke_project"),
        ("1.3.9", "change_namespaces"),
        ("2.0.9", "change_namespaces"),
    ],
)
def test_absent_user_permission_actions_fail_before_any_lookup(
    ds_version: str,
    operation: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    http_client = DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(_unexpected_http)
    )
    domain = UserAdapter.for_version(ds_version).bind(profile, http_client=http_client)

    def invoke() -> object:
        if operation == "revoke_project":
            return domain.revoke_project("alice", "etl-prod")
        return domain.change_namespaces("alice", ["namespace-a"], grant=True)

    with http_client, pytest.raises(UnsupportedFeatureError):
        invoke()


@pytest.mark.parametrize(
    ("ds_version", "operation"),
    [
        ("1.3.9", "create-disabled"),
        ("1.3.9", "update-disabled"),
        ("2.0.9", "update-time-zone"),
    ],
)
def test_absent_user_fields_fail_before_any_lookup(
    ds_version: str,
    operation: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    http_client = DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(_unexpected_http)
    )
    domain = UserAdapter.for_version(ds_version).bind(profile, http_client=http_client)

    def invoke() -> object:
        if operation == "create-disabled":
            return domain.create(
                user_name="alice",
                password="secret-value",
                email="alice@example.com",
                tenant="tenant-prod",
                state=0,
                phone=None,
                queue=None,
            )
        return domain.update(
            "alice",
            user_name=None,
            password=None,
            email=None,
            tenant=None,
            state=0 if operation == "update-disabled" else None,
            phone=None,
            preserve_phone=True,
            queue=None,
            preserve_queue=True,
            time_zone=("Asia/Shanghai" if operation == "update-time-zone" else None),
        )

    with http_client, pytest.raises(UnsupportedFeatureError):
        invoke()


def _unexpected_http(request: httpx.Request) -> httpx.Response:
    message = f"unexpected request {request.method} {request.url.path}"
    raise AssertionError(message)


@dataclass
class _PermissionWire:
    ds_version: str
    projects: dict[int, dict[str, object]]
    datasources: dict[int, dict[str, object]]
    namespaces: dict[int, dict[str, object]]
    authorized_projects: set[int]
    authorized_datasources: set[int]
    authorized_namespaces: set[int]
    mutations: list[tuple[str, dict[str, list[str]]]]

    @classmethod
    def make(cls, ds_version: str = "3.4.1") -> _PermissionWire:
        return cls(
            ds_version=ds_version,
            projects={
                31: _project(
                    project_id=31,
                    code=301,
                    name="existing-project",
                ),
                32: _project(project_id=32, code=302, name="etl-prod"),
            },
            datasources={
                41: _datasource(datasource_id=41, name="warehouse"),
                42: _datasource(datasource_id=42, name="events"),
            },
            namespaces={
                51: _namespace(
                    namespace_id=51,
                    name="existing-namespace",
                ),
                52: _namespace(namespace_id=52, name="batch-jobs"),
            },
            authorized_projects={31},
            authorized_datasources={41},
            authorized_namespaces={51},
            mutations=[],
        )

    def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "GET":
            return self._get(path)
        if request.method == "POST":
            return self._post(
                path, parse_qs(request.content.decode(), keep_blank_values=True)
            )
        message = f"unexpected request {request.method} {path}"
        raise AssertionError(message)

    def _get(self, path: str) -> httpx.Response:
        if self.ds_version == "3.4.3":
            return self._get_simple_permissions(path)
        return self._get_shared(path)

    def _get_simple_permissions(self, path: str) -> httpx.Response:
        if path == "/dolphinscheduler/users/get-user-info":
            return _success(
                _alice() | {"id": 1, "userName": "admin", "userType": "ADMIN_USER"}
            )
        if path == "/dolphinscheduler/users/list-all":
            return _success([{"id": 7, "userName": "alice"}])
        if path in {
            "/dolphinscheduler/datasources/authed-datasource",
            "/dolphinscheduler/datasources/unauth-datasource",
        }:
            rows = (
                _selected(self.datasources, self.authorized_datasources)
                if path.endswith("/authed-datasource")
                else _unselected(self.datasources, self.authorized_datasources)
            )
            return _success([{"id": item["id"], "name": item["name"]} for item in rows])
        return self._get_shared(path)

    def _get_shared(self, path: str) -> httpx.Response:
        if path == "/dolphinscheduler/tenants":
            return _success(_page(_tenant()))
        if path in {
            "/dolphinscheduler/users/list",
            "/dolphinscheduler/users/list-all",
        }:
            return _success([_alice()])
        if path == "/dolphinscheduler/users/list-paging":
            return _success(_page(_alice()))
        if path == "/dolphinscheduler/projects/authed-project":
            return _success(_selected(self.projects, self.authorized_projects))
        if path == "/dolphinscheduler/projects/unauth-project":
            return _success(_unselected(self.projects, self.authorized_projects))
        if path == "/dolphinscheduler/datasources/authed-datasource":
            return _success(_selected(self.datasources, self.authorized_datasources))
        if path == "/dolphinscheduler/datasources/unauth-datasource":
            return _success(_unselected(self.datasources, self.authorized_datasources))
        if path == "/dolphinscheduler/k8s-namespace/authed-namespace":
            return _success(_selected(self.namespaces, self.authorized_namespaces))
        if path == "/dolphinscheduler/k8s-namespace/unauth-namespace":
            return _success(_unselected(self.namespaces, self.authorized_namespaces))
        message = f"unexpected GET {path}"
        raise AssertionError(message)

    def _post(
        self,
        path: str,
        form: dict[str, list[str]],
    ) -> httpx.Response:
        self.mutations.append((path, form))
        if path == "/dolphinscheduler/users/grant-project":
            if self.ds_version in _PROJECT_REPLACE_VERSIONS:
                self.authorized_projects.clear()
            self.authorized_projects.update(_form_ids(form, "projectIds"))
            return _success(None)
        if path == "/dolphinscheduler/users/revoke-project":
            code = int(form["projectCode"][0])
            self.authorized_projects.difference_update(
                project_id
                for project_id, record in self.projects.items()
                if record["code"] == code
            )
            return _success(None)
        if path == "/dolphinscheduler/users/revoke-project-by-id":
            self.authorized_projects.difference_update(_form_ids(form, "projectIds"))
            return _success(None)
        if path == "/dolphinscheduler/users/grant-datasource":
            _replace_ids(
                self.authorized_datasources,
                _form_ids(form, "datasourceIds"),
            )
            return _success(None)
        if path == "/dolphinscheduler/users/grant-namespace":
            _replace_ids(
                self.authorized_namespaces,
                _form_ids(form, "namespaceIds"),
            )
            return _success(None)
        message = f"unexpected POST {path}"
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


def _tenant() -> dict[str, object]:
    return {
        "id": 11,
        "tenantCode": "tenant-prod",
        "tenantName": "tenant-prod",
        "description": None,
        "queueId": 3,
        "queueName": "default",
        "queue": "root.default",
    }


def _user(
    *,
    user_id: int,
    name: str,
    email: str,
    phone: str | None,
    queue: str,
    state: int,
    time_zone: str | None = None,
) -> dict[str, object]:
    return {
        "id": user_id,
        "userName": name,
        "email": email,
        "phone": phone,
        "userType": "GENERAL_USER",
        "tenantId": 11,
        "tenantCode": "tenant-prod",
        "queueName": "default",
        "queue": queue,
        "state": state,
        "timeZone": time_zone,
        "createTime": None,
        "updateTime": None,
    }


def _alice() -> dict[str, object]:
    return _user(
        user_id=7,
        name="alice",
        email="alice@example.com",
        phone=None,
        queue="root.default",
        state=1,
    )


def _project(*, project_id: int, code: int, name: str) -> dict[str, object]:
    return {
        "id": project_id,
        "code": code,
        "name": name,
        "description": None,
    }


def _datasource(*, datasource_id: int, name: str) -> dict[str, object]:
    return {
        "id": datasource_id,
        "name": name,
        "note": None,
        "type": "MYSQL",
    }


def _namespace(*, namespace_id: int, name: str) -> dict[str, object]:
    return {
        "id": namespace_id,
        "namespace": name,
        "clusterCode": 101,
        "clusterName": "cluster-prod",
    }


def _form_ids(form: dict[str, list[str]], name: str) -> set[int]:
    return {int(item) for item in form[name][0].split(",") if item}


def _selected(
    records: dict[int, dict[str, object]],
    selected_ids: set[int],
) -> list[dict[str, object]]:
    return [records[item] for item in sorted(selected_ids)]


def _unselected(
    records: dict[int, dict[str, object]],
    selected_ids: set[int],
) -> list[dict[str, object]]:
    return _selected(records, set(records) - selected_ids)


def _replace_ids(target: set[int], replacement: set[int]) -> None:
    target.clear()
    target.update(replacement)
