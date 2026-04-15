from collections.abc import Mapping, Sequence

import pytest
from tests.fakes import (
    FakeDataSource,
    FakeDataSourceAdapter,
    FakeEnumValue,
    FakeNamespace,
    FakeNamespaceAdapter,
    FakeProject,
    FakeProjectAdapter,
    FakeTenant,
    FakeTenantAdapter,
    FakeUser,
    FakeUserAdapter,
    fake_service_runtime,
)
from tests.support import make_profile

from dsctl.errors import (
    ApiResultError,
    ApiTransportError,
    ConflictError,
    PermissionDeniedError,
    UserInputError,
)
from dsctl.services import runtime as runtime_service
from dsctl.services import user as user_service


def _install_user_service_fakes(
    monkeypatch: pytest.MonkeyPatch,
    user_adapter: FakeUserAdapter,
    tenant_adapter: FakeTenantAdapter,
    *,
    project_adapter: FakeProjectAdapter | None = None,
    datasource_adapter: FakeDataSourceAdapter | None = None,
    namespace_adapter: FakeNamespaceAdapter | None = None,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            project_adapter or FakeProjectAdapter(projects=[]),
            datasource_adapter=(
                datasource_adapter or FakeDataSourceAdapter(datasources=[])
            ),
            namespace_adapter=namespace_adapter or FakeNamespaceAdapter(namespaces=[]),
            user_adapter=user_adapter,
            tenant_adapter=tenant_adapter,
            profile=make_profile(),
        ),
    )


def _mapping(value: object) -> Mapping[str, object]:
    assert isinstance(value, Mapping)
    return value


def _sequence(value: object) -> Sequence[object]:
    assert isinstance(value, Sequence)
    assert not isinstance(value, (str, bytes, bytearray))
    return value


def _tenants() -> list[FakeTenant]:
    return [
        FakeTenant(
            id=11,
            tenant_code_value="tenant-prod",
            queue_id_value=101,
            queue_name_value="default",
        ),
        FakeTenant(
            id=12,
            tenant_code_value="tenant-analytics",
            queue_id_value=102,
            queue_name_value="analytics",
        ),
    ]


def _projects() -> list[FakeProject]:
    return [
        FakeProject(
            code=701,
            name="etl-prod",
            description="Production ETL project",
            id=17,
        )
    ]


def _datasources() -> list[FakeDataSource]:
    return [
        FakeDataSource(
            id=7,
            name="warehouse",
            note="main warehouse",
            type_value=FakeEnumValue("MYSQL"),
        ),
        FakeDataSource(
            id=9,
            name="analytics",
            note="analytics postgres",
            type_value=FakeEnumValue("POSTGRESQL"),
        ),
        FakeDataSource(
            id=12,
            name="archive",
            note="archive mysql",
            type_value=FakeEnumValue("MYSQL"),
        ),
    ]


def _namespaces() -> list[FakeNamespace]:
    return [
        FakeNamespace(
            id=21,
            namespace_value="etl-prod",
            cluster_code_value=9001,
            cluster_name_value="prod-cluster",
        ),
        FakeNamespace(
            id=22,
            namespace_value="etl-staging",
            cluster_code_value=9002,
            cluster_name_value="staging-cluster",
        ),
        FakeNamespace(
            id=23,
            namespace_value="shared",
            cluster_code_value=9001,
            cluster_name_value="prod-cluster",
        ),
    ]


def test_list_users_result_returns_first_page_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_adapter = FakeTenantAdapter(tenants=_tenants())
    user_adapter = FakeUserAdapter(
        users=[
            FakeUser(
                id=7,
                user_name_value="alice",
                email="alice@example.com",
                phone="13800138000",
                user_type_value=FakeEnumValue("GENERAL_USER"),
                tenant_id_value=11,
                tenant_code_value="tenant-prod",
                queue_name_value="default",
                queue_value="default",
                state=1,
            ),
            FakeUser(
                id=9,
                user_name_value="bob",
                email="bob@example.com",
                user_type_value=FakeEnumValue("GENERAL_USER"),
                tenant_id_value=12,
                tenant_code_value="tenant-analytics",
                queue_name_value="analytics",
                queue_value="analytics",
                state=0,
            ),
        ],
        tenants=_tenants(),
    )
    _install_user_service_fakes(monkeypatch, user_adapter, tenant_adapter)

    result = user_service.list_users_result(page_size=1)
    data = _mapping(result.data)
    items = _sequence(data["totalList"])

    assert {
        "total": data["total"],
        "totalPage": data["totalPage"],
        "pageSize": data["pageSize"],
        "currentPage": data["currentPage"],
        "pageNo": data["pageNo"],
    } == {
        "total": 2,
        "totalPage": 2,
        "pageSize": 1,
        "currentPage": 1,
        "pageNo": 1,
    }
    assert list(items) == [
        {
            "id": 7,
            "userName": "alice",
            "email": "alice@example.com",
            "phone": "13800138000",
            "userType": "GENERAL_USER",
            "tenantId": 11,
            "tenantCode": "tenant-prod",
            "queueName": "default",
            "queue": "default",
            "state": 1,
            "createTime": None,
            "updateTime": None,
        }
    ]


def test_list_users_result_translates_permission_denied(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_adapter = FakeTenantAdapter(tenants=_tenants())
    user_adapter = FakeUserAdapter(
        users=[],
        tenants=_tenants(),
    )
    _install_user_service_fakes(monkeypatch, user_adapter, tenant_adapter)

    def deny_list(*, page_no: int, page_size: int, search: str | None = None) -> object:
        del page_no, page_size, search
        raise ApiResultError(
            result_code=user_service.USER_NO_OPERATION_PERM,
            result_message="user has no operation privilege",
        )

    monkeypatch.setattr(user_adapter, "list", deny_list)

    with pytest.raises(PermissionDeniedError, match="requires additional permissions"):
        user_service.list_users_result()


def test_get_user_result_resolves_name_then_fetches_user(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenants = _tenants()
    tenant_adapter = FakeTenantAdapter(tenants=tenants)
    user_adapter = FakeUserAdapter(
        users=[
            FakeUser(
                id=7,
                user_name_value="alice",
                email="alice@example.com",
                phone="13800138000",
                user_type_value=FakeEnumValue("GENERAL_USER"),
                tenant_id_value=11,
                tenant_code_value="tenant-prod",
                queue_name_value="default",
                queue_value="default",
                state=1,
                time_zone_value="Asia/Shanghai",
                stored_queue_value="",
            )
        ],
        tenants=tenants,
    )
    _install_user_service_fakes(monkeypatch, user_adapter, tenant_adapter)

    result = user_service.get_user_result("alice")
    data = _mapping(result.data)

    assert result.resolved == {
        "user": {
            "id": 7,
            "userName": "alice",
            "email": "alice@example.com",
            "tenantId": 11,
            "tenantCode": "tenant-prod",
            "state": 1,
        }
    }
    assert data == {
        "id": 7,
        "userName": "alice",
        "email": "alice@example.com",
        "phone": "13800138000",
        "userType": "GENERAL_USER",
        "tenantId": 11,
        "tenantCode": "tenant-prod",
        "queueName": "default",
        "queue": "default",
        "state": 1,
        "createTime": None,
        "updateTime": None,
        "timeZone": "Asia/Shanghai",
    }


def test_create_user_result_resolves_tenant_and_returns_created_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    password = "super" + "secret"
    tenants = _tenants()
    tenant_adapter = FakeTenantAdapter(tenants=tenants)
    user_adapter = FakeUserAdapter(users=[], tenants=tenants)
    _install_user_service_fakes(monkeypatch, user_adapter, tenant_adapter)

    result = user_service.create_user_result(
        user_name="carol",
        password=password,
        email="carol@example.com",
        tenant="tenant-prod",
        state=1,
        queue="analytics",
    )
    data = _mapping(result.data)

    assert result.resolved == {
        "user": {
            "id": 1,
            "userName": "carol",
            "email": "carol@example.com",
            "tenantId": 11,
            "tenantCode": "tenant-prod",
            "state": 1,
        }
    }
    assert data["queue"] == "analytics"
    assert data["queueName"] == "default"


def test_create_user_result_maps_duplicate_name_to_conflict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    password = "super" + "secret"
    tenants = _tenants()
    tenant_adapter = FakeTenantAdapter(tenants=tenants)
    user_adapter = FakeUserAdapter(
        users=[
            FakeUser(
                id=7,
                user_name_value="alice",
                email="alice@example.com",
            )
        ],
        tenants=tenants,
    )
    _install_user_service_fakes(monkeypatch, user_adapter, tenant_adapter)

    with pytest.raises(ConflictError):
        user_service.create_user_result(
            user_name="alice",
            password=password,
            email="alice@example.com",
            tenant="tenant-prod",
            state=1,
        )


def test_create_user_result_rejects_invalid_state() -> None:
    with pytest.raises(UserInputError, match="User state must be 0 or 1") as exc_info:
        user_service.create_user_result(
            user_name="carol",
            password="supersecret",
            email="carol@example.com",
            tenant="tenant-prod",
            state=2,
        )

    assert exc_info.value.suggestion == (
        "Use --state 1 for enabled or --state 0 for disabled."
    )


def test_create_user_result_maps_upstream_input_rejection_with_suggestion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenants = _tenants()
    tenant_adapter = FakeTenantAdapter(tenants=tenants)
    user_adapter = FakeUserAdapter(
        users=[],
        tenants=tenants,
        create_errors_by_name={
            "carol": ApiResultError(
                result_code=10001,
                result_message="request params invalid",
            )
        },
    )
    _install_user_service_fakes(monkeypatch, user_adapter, tenant_adapter)

    with pytest.raises(
        UserInputError,
        match="rejected by the upstream API",
    ) as exc_info:
        user_service.create_user_result(
            user_name="carol",
            password="supersecret",
            email="carol@example.com",
            tenant="tenant-prod",
            state=1,
        )

    assert exc_info.value.suggestion == (
        "Verify --user-name, --password, --email, --tenant, --state, "
        "and optional --phone/--queue values, then retry."
    )


def test_create_user_result_reports_invalid_created_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenants = _tenants()
    tenant_adapter = FakeTenantAdapter(tenants=tenants)
    user_adapter = FakeUserAdapter(
        users=[],
        tenants=tenants,
    )
    _install_user_service_fakes(monkeypatch, user_adapter, tenant_adapter)

    def broken_create(
        *,
        user_name: str,
        password: str,
        email: str,
        tenant_id: int,
        phone: str | None = None,
        queue: str | None = None,
        state: int = 1,
    ) -> FakeUser:
        del user_name, password, email, tenant_id, phone, queue, state
        return FakeUser(
            id=0,
            user_name_value=None,
            email="carol@example.com",
            tenant_id_value=11,
            tenant_code_value="tenant-prod",
        )

    monkeypatch.setattr(user_adapter, "create", broken_create)

    with pytest.raises(
        ApiTransportError,
        match="missing required identity fields",
    ) as exc_info:
        user_service.create_user_result(
            user_name="carol",
            password="supersecret",
            email="carol@example.com",
            tenant="tenant-prod",
            state=1,
        )

    assert exc_info.value.details == {"resource": "user"}


def test_update_user_result_preserves_omitted_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenants = _tenants()
    tenant_adapter = FakeTenantAdapter(tenants=tenants)
    user_adapter = FakeUserAdapter(
        users=[
            FakeUser(
                id=7,
                user_name_value="alice",
                email="alice@example.com",
                phone="13800138000",
                user_type_value=FakeEnumValue("GENERAL_USER"),
                tenant_id_value=11,
                tenant_code_value="tenant-prod",
                queue_name_value="default",
                queue_value="default",
                state=1,
                time_zone_value="Asia/Shanghai",
                stored_queue_value="",
            )
        ],
        tenants=tenants,
    )
    _install_user_service_fakes(monkeypatch, user_adapter, tenant_adapter)

    result = user_service.update_user_result(
        "alice",
        email="alice+ops@example.com",
    )
    data = _mapping(result.data)

    assert result.resolved == {
        "user": {
            "id": 7,
            "userName": "alice",
            "email": "alice@example.com",
            "tenantId": 11,
            "tenantCode": "tenant-prod",
            "state": 1,
        }
    }
    assert data["email"] == "alice+ops@example.com"
    assert data["queue"] == "default"
    assert data["timeZone"] == "Asia/Shanghai"


def test_update_user_result_requires_one_change(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenants = _tenants()
    tenant_adapter = FakeTenantAdapter(tenants=tenants)
    user_adapter = FakeUserAdapter(
        users=[
            FakeUser(
                id=7,
                user_name_value="alice",
                email="alice@example.com",
                tenant_id_value=11,
                tenant_code_value="tenant-prod",
                state=1,
            )
        ],
        tenants=tenants,
    )
    _install_user_service_fakes(monkeypatch, user_adapter, tenant_adapter)

    with pytest.raises(UserInputError, match="at least one field change") as exc_info:
        user_service.update_user_result("alice")

    assert exc_info.value.suggestion == (
        "Pass at least one update flag such as --user-name, --password, --email, "
        "--tenant, --state, --phone, --clear-phone, --queue, --clear-queue, or "
        "--time-zone."
    )


def test_update_user_result_reports_invalid_current_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenants = _tenants()
    tenant_adapter = FakeTenantAdapter(tenants=tenants)
    user_adapter = FakeUserAdapter(
        users=[
            FakeUser(
                id=7,
                user_name_value="alice",
                email="alice@example.com",
                tenant_id_value=11,
                tenant_code_value="tenant-prod",
                state=1,
            )
        ],
        tenants=tenants,
    )
    _install_user_service_fakes(monkeypatch, user_adapter, tenant_adapter)

    def broken_get(*, user_id: int) -> FakeUser:
        assert user_id == 7
        return FakeUser(
            id=7,
            user_name_value=None,
            email="alice@example.com",
            tenant_id_value=11,
            tenant_code_value="tenant-prod",
            state=1,
        )

    monkeypatch.setattr(user_adapter, "get", broken_get)

    with pytest.raises(ApiTransportError) as exc_info:
        user_service.update_user_result("alice", email="alice+ops@example.com")

    assert exc_info.value.details == {"resource": "user", "id": 7}


def test_delete_user_result_returns_deleted_confirmation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenants = _tenants()
    tenant_adapter = FakeTenantAdapter(tenants=tenants)
    user_adapter = FakeUserAdapter(
        users=[
            FakeUser(
                id=7,
                user_name_value="alice",
                email="alice@example.com",
                tenant_id_value=11,
                tenant_code_value="tenant-prod",
                state=1,
            )
        ],
        tenants=tenants,
    )
    _install_user_service_fakes(monkeypatch, user_adapter, tenant_adapter)

    result = user_service.delete_user_result("alice", force=True)

    assert result.data == {
        "deleted": True,
        "user": {
            "id": 7,
            "userName": "alice",
            "email": "alice@example.com",
            "tenantId": 11,
            "tenantCode": "tenant-prod",
            "state": 1,
        },
    }


def test_grant_user_project_result_returns_grant_confirmation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenants = _tenants()
    projects = _projects()
    tenant_adapter = FakeTenantAdapter(tenants=tenants)
    user_adapter = FakeUserAdapter(
        users=[
            FakeUser(
                id=7,
                user_name_value="alice",
                email="alice@example.com",
                tenant_id_value=11,
                tenant_code_value="tenant-prod",
                state=1,
            )
        ],
        tenants=tenants,
    )
    _install_user_service_fakes(
        monkeypatch,
        user_adapter,
        tenant_adapter,
        project_adapter=FakeProjectAdapter(projects=projects),
    )

    result = user_service.grant_user_project_result("alice", "etl-prod")

    assert result.data == {
        "granted": True,
        "permission": "write",
        "user": {
            "id": 7,
            "userName": "alice",
            "email": "alice@example.com",
            "tenantId": 11,
            "tenantCode": "tenant-prod",
            "state": 1,
        },
        "project": {
            "code": 701,
            "name": "etl-prod",
            "description": "Production ETL project",
        },
    }
    assert user_adapter.granted_projects_by_user_id == {7: {701}}


def test_revoke_user_project_result_returns_revoke_confirmation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenants = _tenants()
    projects = _projects()
    tenant_adapter = FakeTenantAdapter(tenants=tenants)
    user_adapter = FakeUserAdapter(
        users=[
            FakeUser(
                id=7,
                user_name_value="alice",
                email="alice@example.com",
                tenant_id_value=11,
                tenant_code_value="tenant-prod",
                state=1,
            )
        ],
        tenants=tenants,
        granted_projects_by_user_id={7: {701}},
    )
    _install_user_service_fakes(
        monkeypatch,
        user_adapter,
        tenant_adapter,
        project_adapter=FakeProjectAdapter(projects=projects),
    )

    result = user_service.revoke_user_project_result("alice", "etl-prod")

    assert result.data == {
        "revoked": True,
        "user": {
            "id": 7,
            "userName": "alice",
            "email": "alice@example.com",
            "tenantId": 11,
            "tenantCode": "tenant-prod",
            "state": 1,
        },
        "project": {
            "code": 701,
            "name": "etl-prod",
            "description": "Production ETL project",
        },
    }
    assert user_adapter.granted_projects_by_user_id == {7: set()}


def test_grant_user_project_result_maps_permission_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenants = _tenants()
    projects = _projects()
    tenant_adapter = FakeTenantAdapter(tenants=tenants)
    user_adapter = FakeUserAdapter(
        users=[
            FakeUser(
                id=7,
                user_name_value="alice",
                email="alice@example.com",
                tenant_id_value=11,
                tenant_code_value="tenant-prod",
                state=1,
            )
        ],
        tenants=tenants,
        grant_project_errors_by_target={
            (7, 701): ApiResultError(
                result_code=1400001,
                result_message="The current user does not have this permission.",
            )
        },
    )
    _install_user_service_fakes(
        monkeypatch,
        user_adapter,
        tenant_adapter,
        project_adapter=FakeProjectAdapter(projects=projects),
    )

    with pytest.raises(PermissionDeniedError, match="requires additional permissions"):
        user_service.grant_user_project_result("alice", "etl-prod")


def test_grant_user_datasources_result_merges_with_existing_authorized_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenants = _tenants()
    datasources = _datasources()
    tenant_adapter = FakeTenantAdapter(tenants=tenants)
    datasource_adapter = FakeDataSourceAdapter(
        datasources=datasources,
        authorized_by_user_id={7: {7}},
    )
    user_adapter = FakeUserAdapter(
        users=[
            FakeUser(
                id=7,
                user_name_value="alice",
                email="alice@example.com",
                tenant_id_value=11,
                tenant_code_value="tenant-prod",
                state=1,
            )
        ],
        tenants=tenants,
        granted_datasources_by_user_id={7: {7}},
    )
    _install_user_service_fakes(
        monkeypatch,
        user_adapter,
        tenant_adapter,
        datasource_adapter=datasource_adapter,
    )

    result = user_service.grant_user_datasources_result(
        "alice",
        ["analytics", "warehouse"],
    )

    assert result.data == {
        "granted": True,
        "user": {
            "id": 7,
            "userName": "alice",
            "email": "alice@example.com",
            "tenantId": 11,
            "tenantCode": "tenant-prod",
            "state": 1,
        },
        "requested_datasources": [
            {
                "id": 7,
                "name": "warehouse",
                "note": "main warehouse",
                "type": "MYSQL",
            },
            {
                "id": 9,
                "name": "analytics",
                "note": "analytics postgres",
                "type": "POSTGRESQL",
            },
        ],
        "datasources": [
            {
                "id": 7,
                "name": "warehouse",
                "note": "main warehouse",
                "type": "MYSQL",
            },
            {
                "id": 9,
                "name": "analytics",
                "note": "analytics postgres",
                "type": "POSTGRESQL",
            },
        ],
    }
    assert user_adapter.granted_datasources_by_user_id == {7: {7, 9}}


def test_grant_user_datasources_result_requires_one_identifier() -> None:
    with pytest.raises(
        UserInputError,
        match="At least one datasource is required",
    ) as exc_info:
        user_service.grant_user_datasources_result("alice", [])

    assert exc_info.value.suggestion == "Pass at least one --datasource value."


def test_revoke_user_datasources_result_subtracts_from_existing_authorized_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenants = _tenants()
    datasources = _datasources()
    tenant_adapter = FakeTenantAdapter(tenants=tenants)
    datasource_adapter = FakeDataSourceAdapter(
        datasources=datasources,
        authorized_by_user_id={7: {7, 9, 12}},
    )
    user_adapter = FakeUserAdapter(
        users=[
            FakeUser(
                id=7,
                user_name_value="alice",
                email="alice@example.com",
                tenant_id_value=11,
                tenant_code_value="tenant-prod",
                state=1,
            )
        ],
        tenants=tenants,
        granted_datasources_by_user_id={7: {7, 9, 12}},
    )
    _install_user_service_fakes(
        monkeypatch,
        user_adapter,
        tenant_adapter,
        datasource_adapter=datasource_adapter,
    )

    result = user_service.revoke_user_datasources_result(
        "alice",
        ["analytics", "archive"],
    )

    assert result.data == {
        "revoked": True,
        "user": {
            "id": 7,
            "userName": "alice",
            "email": "alice@example.com",
            "tenantId": 11,
            "tenantCode": "tenant-prod",
            "state": 1,
        },
        "requested_datasources": [
            {
                "id": 9,
                "name": "analytics",
                "note": "analytics postgres",
                "type": "POSTGRESQL",
            },
            {
                "id": 12,
                "name": "archive",
                "note": "archive mysql",
                "type": "MYSQL",
            },
        ],
        "datasources": [
            {
                "id": 7,
                "name": "warehouse",
                "note": "main warehouse",
                "type": "MYSQL",
            }
        ],
    }
    assert user_adapter.granted_datasources_by_user_id == {7: {7}}


def test_grant_user_namespaces_result_merges_with_existing_authorized_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenants = _tenants()
    namespaces = _namespaces()
    tenant_adapter = FakeTenantAdapter(tenants=tenants)
    namespace_adapter = FakeNamespaceAdapter(
        namespaces=namespaces,
        authorized_by_user_id={7: {21}},
    )
    user_adapter = FakeUserAdapter(
        users=[
            FakeUser(
                id=7,
                user_name_value="alice",
                email="alice@example.com",
                tenant_id_value=11,
                tenant_code_value="tenant-prod",
                state=1,
            )
        ],
        tenants=tenants,
        granted_namespaces_by_user_id={7: {21}},
    )
    _install_user_service_fakes(
        monkeypatch,
        user_adapter,
        tenant_adapter,
        namespace_adapter=namespace_adapter,
    )

    result = user_service.grant_user_namespaces_result(
        "alice",
        ["etl-prod", "etl-staging"],
    )

    assert result.data == {
        "granted": True,
        "user": {
            "id": 7,
            "userName": "alice",
            "email": "alice@example.com",
            "tenantId": 11,
            "tenantCode": "tenant-prod",
            "state": 1,
        },
        "requested_namespaces": [
            {
                "id": 21,
                "namespace": "etl-prod",
                "clusterCode": 9001,
                "clusterName": "prod-cluster",
            },
            {
                "id": 22,
                "namespace": "etl-staging",
                "clusterCode": 9002,
                "clusterName": "staging-cluster",
            },
        ],
        "namespaces": [
            {
                "id": 21,
                "namespace": "etl-prod",
                "clusterCode": 9001,
                "clusterName": "prod-cluster",
            },
            {
                "id": 22,
                "namespace": "etl-staging",
                "clusterCode": 9002,
                "clusterName": "staging-cluster",
            },
        ],
    }
    assert user_adapter.granted_namespaces_by_user_id == {7: {21, 22}}


def test_revoke_user_namespaces_result_subtracts_from_existing_authorized_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenants = _tenants()
    namespaces = _namespaces()
    tenant_adapter = FakeTenantAdapter(tenants=tenants)
    namespace_adapter = FakeNamespaceAdapter(
        namespaces=namespaces,
        authorized_by_user_id={7: {21, 22, 23}},
    )
    user_adapter = FakeUserAdapter(
        users=[
            FakeUser(
                id=7,
                user_name_value="alice",
                email="alice@example.com",
                tenant_id_value=11,
                tenant_code_value="tenant-prod",
                state=1,
            )
        ],
        tenants=tenants,
        granted_namespaces_by_user_id={7: {21, 22, 23}},
    )
    _install_user_service_fakes(
        monkeypatch,
        user_adapter,
        tenant_adapter,
        namespace_adapter=namespace_adapter,
    )

    result = user_service.revoke_user_namespaces_result(
        "alice",
        ["etl-staging", "shared"],
    )

    assert result.data == {
        "revoked": True,
        "user": {
            "id": 7,
            "userName": "alice",
            "email": "alice@example.com",
            "tenantId": 11,
            "tenantCode": "tenant-prod",
            "state": 1,
        },
        "requested_namespaces": [
            {
                "id": 22,
                "namespace": "etl-staging",
                "clusterCode": 9002,
                "clusterName": "staging-cluster",
            },
            {
                "id": 23,
                "namespace": "shared",
                "clusterCode": 9001,
                "clusterName": "prod-cluster",
            },
        ],
        "namespaces": [
            {
                "id": 21,
                "namespace": "etl-prod",
                "clusterCode": 9001,
                "clusterName": "prod-cluster",
            }
        ],
    }
    assert user_adapter.granted_namespaces_by_user_id == {7: {21}}


def test_grant_user_namespaces_result_requires_one_identifier() -> None:
    with pytest.raises(
        UserInputError,
        match="At least one namespace is required",
    ) as exc_info:
        user_service.grant_user_namespaces_result("alice", [])

    assert exc_info.value.suggestion == "Pass at least one --namespace value."
