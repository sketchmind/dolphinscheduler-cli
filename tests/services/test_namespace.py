from collections.abc import Mapping, Sequence

import pytest
from tests.fakes import (
    FakeNamespace,
    FakeNamespaceAdapter,
    FakeProjectAdapter,
    fake_service_runtime,
)
from tests.support import make_profile

from dsctl.errors import ApiResultError, ConflictError, UserInputError
from dsctl.services import namespace as namespace_service
from dsctl.services import runtime as runtime_service


def _install_namespace_service_fakes(
    monkeypatch: pytest.MonkeyPatch,
    adapter: FakeNamespaceAdapter,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            FakeProjectAdapter(projects=[]),
            namespace_adapter=adapter,
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
        "namespace": {
            "id": 21,
            "namespace": "etl-prod",
            "clusterCode": 9001,
            "clusterName": "prod-cluster",
        }
    }
    assert data == {
        "deleted": True,
        "namespace": {
            "id": 21,
            "namespace": "etl-prod",
            "clusterCode": 9001,
            "clusterName": "prod-cluster",
        },
    }
