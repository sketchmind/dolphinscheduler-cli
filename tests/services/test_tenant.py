import pytest
from tests.bound_domain_fakes import patch_bound_domain_service_runtime
from tests.fakes import (
    FakeQueue,
    FakeQueueAdapter,
    FakeTenant,
    FakeTenantAdapter,
)
from tests.support import make_profile
from tests.value_shape_assertions import assert_mapping as _mapping
from tests.value_shape_assertions import assert_sequence as _sequence

from dsctl.errors import ApiResultError, ConflictError, UserInputError
from dsctl.services import tenant as tenant_service
from dsctl.upstream.tenants import TENANT_DOMAIN, TenantDomain


def _install_tenant_service_fakes(
    monkeypatch: pytest.MonkeyPatch,
    tenant_adapter: FakeTenantAdapter,
    queue_adapter: FakeQueueAdapter,
) -> None:
    domain = TenantDomain(tenants=tenant_adapter, queues=queue_adapter)

    patch_bound_domain_service_runtime(
        monkeypatch,
        tenant_service,
        expected_domain=TENANT_DOMAIN,
        runtime_domain=domain,
        profile_factory=make_profile,
    )


def _queues() -> list[FakeQueue]:
    return [
        FakeQueue(id=11, queue_name_value="default", queue="root.default"),
        FakeQueue(id=12, queue_name_value="analytics", queue="root.analytics"),
    ]


def test_list_tenants_result_returns_first_page_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queues = _queues()
    tenant_adapter = FakeTenantAdapter(
        tenants=[
            FakeTenant(
                id=7,
                tenant_code_value="tenant-prod",
                description="production tenant",
                queue_id_value=11,
                queue_name_value="default",
            ),
            FakeTenant(
                id=9,
                tenant_code_value="tenant-analytics",
                queue_id_value=12,
                queue_name_value="analytics",
            ),
        ],
        queues=queues,
    )
    queue_adapter = FakeQueueAdapter(queues=queues)
    _install_tenant_service_fakes(monkeypatch, tenant_adapter, queue_adapter)

    result = tenant_service.list_tenants_result(page_size=1)
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
            "tenantCode": "tenant-prod",
            "description": "production tenant",
            "queueId": 11,
            "queueName": "default",
            "queue": None,
            "createTime": None,
            "updateTime": None,
        }
    ]


def test_get_tenant_result_resolves_code_then_fetches_tenant(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queues = _queues()
    tenant_adapter = FakeTenantAdapter(
        tenants=[
            FakeTenant(
                id=7,
                tenant_code_value="tenant-prod",
                description="production tenant",
                queue_id_value=11,
                queue_name_value="default",
                queue_value="root.default",
            )
        ],
        queues=queues,
    )
    queue_adapter = FakeQueueAdapter(queues=queues)
    _install_tenant_service_fakes(monkeypatch, tenant_adapter, queue_adapter)

    result = tenant_service.get_tenant_result("tenant-prod")
    data = _mapping(result.data)

    assert result.resolved == {
        "tenant": {
            "id": 7,
            "tenantCode": "tenant-prod",
            "description": "production tenant",
            "queueId": 11,
            "queueName": "default",
            "queue": "root.default",
        }
    }
    assert data == {
        "id": 7,
        "tenantCode": "tenant-prod",
        "description": "production tenant",
        "queueId": 11,
        "queueName": "default",
        "queue": "root.default",
        "createTime": None,
        "updateTime": None,
    }


def test_create_tenant_result_resolves_queue_and_returns_created_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queues = _queues()
    tenant_adapter = FakeTenantAdapter(tenants=[], queues=queues)
    queue_adapter = FakeQueueAdapter(queues=queues)
    _install_tenant_service_fakes(monkeypatch, tenant_adapter, queue_adapter)

    result = tenant_service.create_tenant_result(
        tenant_code="tenant-ops",
        queue="default",
        description="ops tenant",
    )
    data = _mapping(result.data)

    assert result.resolved == {
        "tenant": {
            "id": 1,
            "tenantCode": "tenant-ops",
            "description": "ops tenant",
            "queueId": 11,
            "queueName": "default",
            "queue": "root.default",
        }
    }
    assert data["tenantCode"] == "tenant-ops"
    assert data["queueId"] == 11
    assert data["queueName"] == "default"


def test_create_tenant_result_maps_duplicate_code_to_conflict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queues = _queues()
    tenant_adapter = FakeTenantAdapter(
        tenants=[FakeTenant(id=7, tenant_code_value="tenant-prod")],
        queues=queues,
    )
    queue_adapter = FakeQueueAdapter(queues=queues)
    _install_tenant_service_fakes(monkeypatch, tenant_adapter, queue_adapter)

    with pytest.raises(ConflictError):
        tenant_service.create_tenant_result(
            tenant_code="tenant-prod",
            queue="default",
        )


def test_update_tenant_result_preserves_omitted_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queues = _queues()
    tenant_adapter = FakeTenantAdapter(
        tenants=[
            FakeTenant(
                id=7,
                tenant_code_value="tenant-prod",
                description="production tenant",
                queue_id_value=11,
                queue_name_value="default",
                queue_value="root.default",
            )
        ],
        queues=queues,
    )
    queue_adapter = FakeQueueAdapter(queues=queues)
    _install_tenant_service_fakes(monkeypatch, tenant_adapter, queue_adapter)

    result = tenant_service.update_tenant_result(
        "tenant-prod",
        queue="analytics",
    )
    data = _mapping(result.data)

    assert result.resolved == {
        "tenant": {
            "id": 7,
            "tenantCode": "tenant-prod",
            "description": "production tenant",
            "queueId": 11,
            "queueName": "default",
            "queue": "root.default",
        }
    }
    assert data["tenantCode"] == "tenant-prod"
    assert data["queueId"] == 12
    assert data["queueName"] == "analytics"


def test_update_tenant_result_requires_one_change(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queues = _queues()
    tenant_adapter = FakeTenantAdapter(
        tenants=[FakeTenant(id=7, tenant_code_value="tenant-prod")],
        queues=queues,
    )
    queue_adapter = FakeQueueAdapter(queues=queues)
    _install_tenant_service_fakes(monkeypatch, tenant_adapter, queue_adapter)

    with pytest.raises(UserInputError, match="at least one field change") as exc_info:
        tenant_service.update_tenant_result("tenant-prod")

    assert exc_info.value.suggestion == (
        "Pass at least one update flag such as --queue, --description, or "
        "--clear-description."
    )


def test_update_tenant_result_rejects_legacy_tenant_code_rename(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queues = _queues()
    tenant_adapter = FakeTenantAdapter(
        tenants=[FakeTenant(id=7, tenant_code_value="tenant-prod")],
        queues=queues,
    )
    queue_adapter = FakeQueueAdapter(queues=queues)
    _install_tenant_service_fakes(monkeypatch, tenant_adapter, queue_adapter)

    with pytest.raises(UserInputError, match="immutable") as exc_info:
        tenant_service.update_tenant_result(
            "tenant-prod",
            tenant_code="tenant-renamed",
        )

    assert exc_info.value.suggestion is not None
    assert "create a new tenant" in exc_info.value.suggestion


def test_139_duplicate_create_error_is_distinct_from_generic_invalid_input() -> None:
    duplicate = tenant_service._translate_tenant_api_error(
        ApiResultError(result_code=10001, result_message="tenant exists"),
        ds_version="1.3.9",
        operation="create",
        tenant_code="tenant-prod",
    )
    modern_invalid = tenant_service._translate_tenant_api_error(
        ApiResultError(result_code=10001, result_message="invalid params"),
        ds_version="3.4.1",
        operation="create",
        tenant_code="tenant-prod",
    )

    assert isinstance(duplicate, ConflictError)
    assert isinstance(modern_invalid, UserInputError)


def test_139_invalid_tenant_code_has_specific_input_guidance() -> None:
    translated = tenant_service._translate_tenant_api_error(
        ApiResultError(result_code=10089, result_message="verify tenant code error"),
        ds_version="1.3.9",
        operation="create",
        tenant_code="bad code",
    )

    assert isinstance(translated, UserInputError)
    assert "DolphinScheduler 1.3.9" in str(translated)
    assert translated.suggestion is not None
    assert "operating-system tenant code" in translated.suggestion


def test_delete_tenant_result_returns_deleted_confirmation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queues = _queues()
    tenant_adapter = FakeTenantAdapter(
        tenants=[
            FakeTenant(
                id=7,
                tenant_code_value="tenant-prod",
                queue_id_value=11,
                queue_name_value="default",
            )
        ],
        queues=queues,
    )
    queue_adapter = FakeQueueAdapter(queues=queues)
    _install_tenant_service_fakes(monkeypatch, tenant_adapter, queue_adapter)

    result = tenant_service.delete_tenant_result("tenant-prod", force=True)
    data = _mapping(result.data)

    assert result.resolved == {
        "tenant": {
            "id": 7,
            "tenantCode": "tenant-prod",
            "description": None,
            "queueId": 11,
            "queueName": "default",
            "queue": None,
        }
    }
    assert data == {
        "deleted": True,
        "tenant": {
            "id": 7,
            "tenantCode": "tenant-prod",
            "description": None,
            "queueId": 11,
            "queueName": "default",
            "queue": None,
        },
    }
