import pytest
from tests.bound_domain_fakes import patch_bound_domain_service_runtime
from tests.fakes import (
    FakeNamespace,
    FakeNamespaceAdapter,
)
from tests.support import make_profile
from tests.value_shape_assertions import assert_mapping as _mapping
from tests.value_shape_assertions import assert_sequence as _sequence

from dsctl.errors import (
    ApiResultError,
    ConflictError,
    PermissionDeniedError,
    UserInputError,
)
from dsctl.services import namespace as namespace_service
from dsctl.upstream.namespaces import NAMESPACE_DOMAIN, NamespaceDomain


def _install_namespace_service_fakes(
    monkeypatch: pytest.MonkeyPatch,
    adapter: FakeNamespaceAdapter,
    *,
    ds_version: str = "3.4.1",
) -> None:
    domain = NamespaceDomain(namespaces=adapter)

    patch_bound_domain_service_runtime(
        monkeypatch,
        namespace_service,
        expected_domain=NAMESPACE_DOMAIN,
        runtime_domain=domain,
        profile_factory=lambda: make_profile(ds_version=ds_version),
    )


def _namespaces() -> list[FakeNamespace]:
    return [
        FakeNamespace(
            id=21,
            code=7001,
            namespace_value="etl-prod",
            cluster_code_value=9001,
            cluster_name_value="prod-cluster",
            user_id_value=1,
            user_name_value="admin",
        ),
        FakeNamespace(
            id=22,
            code=7002,
            namespace_value="etl-staging",
            cluster_code_value=9002,
            cluster_name_value="staging-cluster",
            user_id_value=1,
            user_name_value="admin",
        ),
    ]


def test_list_namespaces_result_returns_first_page_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeNamespaceAdapter(namespaces=_namespaces())
    _install_namespace_service_fakes(monkeypatch, adapter)

    result = namespace_service.list_namespaces_result(page_size=1)
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
            "id": 21,
            "code": 7001,
            "namespace": "etl-prod",
            "clusterCode": 9001,
            "clusterName": "prod-cluster",
            "userId": 1,
            "userName": "admin",
            "createTime": None,
            "updateTime": None,
        }
    ]


def test_get_namespace_result_resolves_name_then_fetches_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeNamespaceAdapter(namespaces=_namespaces())
    _install_namespace_service_fakes(monkeypatch, adapter)

    result = namespace_service.get_namespace_result("etl-prod")
    data = _mapping(result.data)

    assert result.resolved == {
        "namespace": {
            "id": 21,
            "namespace": "etl-prod",
            "clusterCode": 9001,
            "clusterName": "prod-cluster",
        }
    }
    assert data == {
        "id": 21,
        "code": 7001,
        "namespace": "etl-prod",
        "clusterCode": 9001,
        "clusterName": "prod-cluster",
        "userId": 1,
        "userName": "admin",
        "createTime": None,
        "updateTime": None,
    }


def test_get_namespace_result_translates_resolution_permission_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeNamespaceAdapter(
        namespaces=[],
        list_error=ApiResultError(
            result_code=30001,
            result_message="no operation permission",
        ),
    )
    _install_namespace_service_fakes(monkeypatch, adapter)

    with pytest.raises(PermissionDeniedError) as exc_info:
        namespace_service.get_namespace_result("etl-prod")

    assert exc_info.value.details == {
        "operation": "get",
        "namespace": "etl-prod",
    }


def test_available_namespace_result_translates_permission_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeNamespaceAdapter(
        namespaces=[],
        available_error=ApiResultError(
            result_code=30001,
            result_message="no operation permission",
        ),
    )
    _install_namespace_service_fakes(monkeypatch, adapter)

    with pytest.raises(PermissionDeniedError) as exc_info:
        namespace_service.list_available_namespaces_result()

    assert exc_info.value.details == {"operation": "available"}


def test_list_available_namespaces_result_returns_current_user_visible_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeNamespaceAdapter(
        namespaces=_namespaces(),
        available_ids={22},
    )
    _install_namespace_service_fakes(monkeypatch, adapter)

    result = namespace_service.list_available_namespaces_result()

    assert result.resolved == {"scope": "current_user"}
    assert result.data == [
        {
            "id": 22,
            "code": 7002,
            "namespace": "etl-staging",
            "clusterCode": 9002,
            "clusterName": "staging-cluster",
            "userId": 1,
            "userName": "admin",
            "createTime": None,
            "updateTime": None,
        }
    ]


def test_create_namespace_result_returns_created_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeNamespaceAdapter(namespaces=[])
    _install_namespace_service_fakes(monkeypatch, adapter)

    result = namespace_service.create_namespace_result(
        namespace="etl-ops",
        cluster_code=9003,
    )
    data = _mapping(result.data)

    assert result.resolved == {
        "namespace": {
            "id": 1,
            "namespace": "etl-ops",
            "clusterCode": 9003,
            "clusterName": None,
        }
    }
    assert data["id"] == 1
    assert data["code"] == 1
    assert data["namespace"] == "etl-ops"
    assert data["clusterCode"] == 9003


def test_create_namespace_result_maps_duplicate_namespace_to_conflict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeNamespaceAdapter(namespaces=_namespaces())
    _install_namespace_service_fakes(monkeypatch, adapter)

    with pytest.raises(ConflictError):
        namespace_service.create_namespace_result(
            namespace="etl-prod",
            cluster_code=9001,
        )


def test_create_namespace_result_maps_upstream_input_rejection_with_suggestion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeNamespaceAdapter(
        namespaces=_namespaces(),
        create_error=ApiResultError(
            result_code=10001,
            result_message="request params invalid",
        ),
    )
    _install_namespace_service_fakes(monkeypatch, adapter)

    with pytest.raises(
        UserInputError,
        match="rejected by the upstream API",
    ) as exc_info:
        namespace_service.create_namespace_result(
            namespace="etl-new",
            cluster_code=9001,
        )

    assert exc_info.value.suggestion == (
        "Verify --namespace and --cluster-code, then retry."
    )


@pytest.mark.parametrize(
    "ds_version", ["3.0.0", "3.0.1", "3.0.2", "3.0.3", "3.0.4", "3.0.5", "3.0.6"]
)
def test_create_namespace_result_uses_legacy_k8s_input_suggestion(
    monkeypatch: pytest.MonkeyPatch,
    ds_version: str,
) -> None:
    adapter = FakeNamespaceAdapter(
        namespaces=[],
        create_error=ApiResultError(
            result_code=10001,
            result_message="request params invalid",
        ),
    )
    _install_namespace_service_fakes(
        monkeypatch,
        adapter,
        ds_version=ds_version,
    )

    with pytest.raises(UserInputError) as exc_info:
        namespace_service.create_namespace_result(
            namespace="etl-new",
            k8s="legacy-cluster",
        )

    assert exc_info.value.suggestion == "Verify --namespace and --k8s, then retry."
    assert exc_info.value.details["k8s"] == "legacy-cluster"


def test_delete_namespace_result_requires_force() -> None:
    with pytest.raises(UserInputError, match="requires --force"):
        namespace_service.delete_namespace_result("etl-prod", force=False)


def test_delete_namespace_result_returns_deleted_confirmation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeNamespaceAdapter(namespaces=_namespaces())
    _install_namespace_service_fakes(monkeypatch, adapter)

    result = namespace_service.delete_namespace_result("etl-prod", force=True)
    data = _mapping(result.data)

    assert result.resolved == {
        "deletes_kubernetes_namespace": False,
        "namespace": {
            "id": 21,
            "namespace": "etl-prod",
            "clusterCode": 9001,
            "clusterName": "prod-cluster",
        },
    }
    assert data == {
        "deleted": True,
        "deletesKubernetesNamespace": False,
        "namespace": {
            "id": 21,
            "namespace": "etl-prod",
            "clusterCode": 9001,
            "clusterName": "prod-cluster",
        },
    }
