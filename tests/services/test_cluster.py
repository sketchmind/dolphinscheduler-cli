from collections.abc import Mapping, Sequence

import pytest
from tests.fakes import (
    FakeCluster,
    FakeClusterAdapter,
    FakeProjectAdapter,
    fake_service_runtime,
)
from tests.support import make_profile

from dsctl.errors import ConflictError, UserInputError
from dsctl.services import cluster as cluster_service
from dsctl.services import runtime as runtime_service


def _install_cluster_service_fakes(
    monkeypatch: pytest.MonkeyPatch,
    adapter: FakeClusterAdapter,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            FakeProjectAdapter(projects=[]),
            cluster_adapter=adapter,
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


def test_list_clusters_result_returns_first_page_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeClusterAdapter(
        clusters=[
            FakeCluster(id=7, code=7, name="k8s-prod", config="kube-prod"),
            FakeCluster(id=9, code=9, name="k8s-dev", config="kube-dev"),
        ]
    )
    _install_cluster_service_fakes(monkeypatch, adapter)

    result = cluster_service.list_clusters_result(page_size=1)
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
            "code": 7,
            "name": "k8s-prod",
            "config": "kube-prod",
            "description": None,
            "workflowDefinitions": None,
            "operator": None,
            "createTime": None,
            "updateTime": None,
        }
    ]


def test_get_cluster_result_resolves_name_then_fetches_cluster(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeClusterAdapter(
        clusters=[
            FakeCluster(
                id=7,
                code=7,
                name="k8s-prod",
                config="kube-prod",
                description="primary",
            )
        ]
    )
    _install_cluster_service_fakes(monkeypatch, adapter)

    result = cluster_service.get_cluster_result("k8s-prod")
    data = _mapping(result.data)

    assert result.resolved == {
        "cluster": {
            "code": 7,
            "name": "k8s-prod",
            "description": "primary",
        }
    }
    assert data == {
        "id": 7,
        "code": 7,
        "name": "k8s-prod",
        "config": "kube-prod",
        "description": "primary",
        "workflowDefinitions": None,
        "operator": None,
        "createTime": None,
        "updateTime": None,
    }


def test_create_cluster_result_returns_created_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeClusterAdapter(clusters=[])
    _install_cluster_service_fakes(monkeypatch, adapter)

    result = cluster_service.create_cluster_result(
        name="analytics",
        config="kube-analytics",
        description="batch",
    )
    data = _mapping(result.data)

    assert result.resolved == {
        "cluster": {
            "code": 1,
            "name": "analytics",
            "description": "batch",
        }
    }
    assert data["code"] == 1
    assert data["name"] == "analytics"
    assert data["config"] == "kube-analytics"
    assert data["description"] == "batch"


def test_create_cluster_result_maps_duplicate_name_to_conflict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeClusterAdapter(
        clusters=[FakeCluster(id=7, code=7, name="k8s-prod", config="kube-prod")]
    )
    _install_cluster_service_fakes(monkeypatch, adapter)

    with pytest.raises(ConflictError):
        cluster_service.create_cluster_result(
            name="k8s-prod",
            config="kube-analytics",
        )


def test_update_cluster_result_preserves_omitted_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeClusterAdapter(
        clusters=[
            FakeCluster(
                id=7,
                code=7,
                name="k8s-prod",
                config="kube-prod",
                description="primary",
            )
        ]
    )
    _install_cluster_service_fakes(monkeypatch, adapter)

    result = cluster_service.update_cluster_result(
        "k8s-prod",
        config="kube-ops",
    )
    data = _mapping(result.data)

    assert result.resolved == {
        "cluster": {
            "code": 7,
            "name": "k8s-prod",
            "description": "primary",
        }
    }
    assert data["name"] == "k8s-prod"
    assert data["config"] == "kube-ops"
    assert data["description"] == "primary"


def test_update_cluster_result_can_clear_description(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeClusterAdapter(
        clusters=[
            FakeCluster(
                id=7,
                code=7,
                name="k8s-prod",
                config="kube-prod",
                description="primary",
            )
        ]
    )
    _install_cluster_service_fakes(monkeypatch, adapter)

    result = cluster_service.update_cluster_result(
        "k8s-prod",
        description=None,
    )

    assert _mapping(result.data)["description"] is None


def test_update_cluster_result_requires_one_change(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeClusterAdapter(
        clusters=[FakeCluster(id=7, code=7, name="k8s-prod", config="kube-prod")]
    )
    _install_cluster_service_fakes(monkeypatch, adapter)

    with pytest.raises(UserInputError, match="at least one field change"):
        cluster_service.update_cluster_result("k8s-prod")


def test_delete_cluster_result_returns_deleted_confirmation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeClusterAdapter(
        clusters=[
            FakeCluster(
                id=7,
                code=7,
                name="k8s-prod",
                config="kube-prod",
                description="primary",
            )
        ]
    )
    _install_cluster_service_fakes(monkeypatch, adapter)

    result = cluster_service.delete_cluster_result("k8s-prod", force=True)
    data = _mapping(result.data)

    assert result.resolved == {
        "cluster": {
            "code": 7,
            "name": "k8s-prod",
            "description": "primary",
        }
    }
    assert data == {
        "deleted": True,
        "cluster": {
            "code": 7,
            "name": "k8s-prod",
            "description": "primary",
        },
    }
