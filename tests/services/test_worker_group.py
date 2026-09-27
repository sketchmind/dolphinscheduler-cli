from collections.abc import Mapping

import pytest
from tests.bound_domain_fakes import patch_bound_domain_service_runtime
from tests.fakes import (
    FakeWorkerGroup,
    FakeWorkerGroupAdapter,
)
from tests.support import make_profile
from tests.value_shape_assertions import assert_mapping as _mapping
from tests.value_shape_assertions import assert_sequence as _sequence

from dsctl.errors import (
    ConflictError,
    InvalidStateError,
    UnsupportedFeatureError,
    UserInputError,
)
from dsctl.services import worker_group as worker_group_service
from dsctl.upstream.worker_groups import (
    WORKER_GROUP_DOMAIN,
    WorkerGroupDomain,
)


def _install_worker_group_service_fakes(
    monkeypatch: pytest.MonkeyPatch,
    adapter: FakeWorkerGroupAdapter,
    *,
    ds_version: str = "3.4.1",
) -> None:
    domain = WorkerGroupDomain(worker_groups=adapter)

    patch_bound_domain_service_runtime(
        monkeypatch,
        worker_group_service,
        expected_domain=WORKER_GROUP_DOMAIN,
        runtime_domain=domain,
        profile_factory=lambda: make_profile(ds_version=ds_version),
    )


def test_list_worker_groups_result_returns_first_page_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeWorkerGroupAdapter(
        worker_groups=[
            FakeWorkerGroup(id=7, name="default", addr_list_value="worker-a:1234"),
            FakeWorkerGroup(id=9, name="analytics", addr_list_value="worker-b:1234"),
        ]
    )
    _install_worker_group_service_fakes(monkeypatch, adapter)

    result = worker_group_service.list_worker_groups_result(page_size=1)
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
            "name": "default",
            "addrList": "worker-a:1234",
            "createTime": None,
            "updateTime": None,
            "description": None,
            "systemDefault": False,
        }
    ]


def test_list_worker_groups_result_all_pages_deduplicates_config_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_group = FakeWorkerGroup(
        id=None,
        name="config-default",
        addr_list_value="worker-a:1234",
        system_default=True,
    )
    adapter = FakeWorkerGroupAdapter(
        worker_groups=[
            config_group,
            FakeWorkerGroup(id=7, name="default", addr_list_value="worker-a:1234"),
            FakeWorkerGroup(id=9, name="analytics", addr_list_value="worker-b:1234"),
        ],
        config_worker_groups=[config_group],
    )
    _install_worker_group_service_fakes(monkeypatch, adapter)

    result = worker_group_service.list_worker_groups_result(
        page_size=1,
        all_pages=True,
    )
    data = _mapping(result.data)
    items = _sequence(data["totalList"])

    assert data["total"] == 3
    assert data["totalPage"] == 1
    assert [item["name"] for item in items if isinstance(item, Mapping)] == [
        "config-default",
        "default",
        "analytics",
    ]


def test_get_worker_group_result_resolves_name_then_returns_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeWorkerGroupAdapter(
        worker_groups=[
            FakeWorkerGroup(
                id=7,
                name="default",
                addr_list_value="worker-a:1234,worker-b:1234",
                description="primary worker group",
            )
        ]
    )
    _install_worker_group_service_fakes(monkeypatch, adapter)

    result = worker_group_service.get_worker_group_result("default")
    data = _mapping(result.data)

    assert result.resolved == {
        "workerGroup": {
            "id": 7,
            "name": "default",
            "addrList": "worker-a:1234,worker-b:1234",
            "systemDefault": False,
        }
    }
    assert data == {
        "id": 7,
        "name": "default",
        "addrList": "worker-a:1234,worker-b:1234",
        "createTime": None,
        "updateTime": None,
        "description": "primary worker group",
        "systemDefault": False,
    }


def test_create_worker_group_result_joins_addresses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeWorkerGroupAdapter(worker_groups=[])
    _install_worker_group_service_fakes(monkeypatch, adapter)

    result = worker_group_service.create_worker_group_result(
        name="analytics",
        addresses=["worker-a:1234", "worker-b:1234"],
    )
    data = _mapping(result.data)

    assert result.resolved == {
        "workerGroup": {
            "id": 1,
            "name": "analytics",
            "addrList": "worker-a:1234,worker-b:1234",
            "systemDefault": False,
        }
    }
    assert data["id"] == 1
    assert data["name"] == "analytics"
    assert data["addrList"] == "worker-a:1234,worker-b:1234"


def test_create_worker_group_result_maps_duplicate_name_to_conflict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeWorkerGroupAdapter(
        worker_groups=[FakeWorkerGroup(id=7, name="default")]
    )
    _install_worker_group_service_fakes(monkeypatch, adapter)

    with pytest.raises(ConflictError):
        worker_group_service.create_worker_group_result(name="default")


def test_update_worker_group_result_preserves_omitted_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeWorkerGroupAdapter(
        worker_groups=[
            FakeWorkerGroup(
                id=7,
                name="default",
                addr_list_value="worker-a:1234",
                description="primary worker group",
            )
        ]
    )
    _install_worker_group_service_fakes(monkeypatch, adapter)

    result = worker_group_service.update_worker_group_result(
        "default",
        addresses=["worker-b:1234"],
    )
    data = _mapping(result.data)

    assert result.resolved == {
        "workerGroup": {
            "id": 7,
            "name": "default",
            "addrList": "worker-a:1234",
            "systemDefault": False,
        }
    }
    assert data["name"] == "default"
    assert data["addrList"] == "worker-b:1234"
    assert data["description"] == "primary worker group"


@pytest.mark.parametrize("ds_version", ["3.1.0", "3.1.1"])
@pytest.mark.parametrize(
    "update_kwargs",
    [
        {"description": "new"},
        {"description": None},
        {"addresses": ["worker-b:1234"]},
    ],
)
def test_legacy_same_name_update_rejected_before_mutation(
    monkeypatch: pytest.MonkeyPatch,
    ds_version: str,
    update_kwargs: dict[str, object],
) -> None:
    adapter = FakeWorkerGroupAdapter(
        worker_groups=[
            FakeWorkerGroup(
                id=7,
                name="analytics",
                addr_list_value="worker-a:1234",
                description="old",
            )
        ]
    )
    _install_worker_group_service_fakes(monkeypatch, adapter, ds_version=ds_version)
    mutation_calls = 0

    def unexpected_mutation(**kwargs: object) -> FakeWorkerGroup:
        nonlocal mutation_calls
        mutation_calls += 1
        message = f"unexpected update: {kwargs}"
        raise AssertionError(message)

    monkeypatch.setattr(adapter, "update", unexpected_mutation)
    with pytest.raises(UnsupportedFeatureError) as exc_info:
        worker_group_service.update_worker_group_result(
            "analytics",
            **update_kwargs,  # type: ignore[arg-type]
        )
    assert mutation_calls == 0
    assert exc_info.value.details["reason"] == (
        "upstream_same_name_update_self_collision"
    )
    assert exc_info.value.details["selected_version"] == ds_version
    assert "3.1.2" in str(exc_info.value.suggestion)


@pytest.mark.parametrize("ds_version", ["3.1.0", "3.1.1", "3.1.2"])
def test_legacy_rename_with_description_remains_available(
    monkeypatch: pytest.MonkeyPatch, ds_version: str
) -> None:
    adapter = FakeWorkerGroupAdapter(
        worker_groups=[FakeWorkerGroup(id=7, name="analytics", description="old")]
    )
    _install_worker_group_service_fakes(monkeypatch, adapter, ds_version=ds_version)
    result = worker_group_service.update_worker_group_result(
        "analytics", name="analytics-renamed", description="new"
    )
    data = _mapping(result.data)
    assert data["name"] == "analytics-renamed"
    assert data["description"] == "new"


def test_312_same_name_description_update_remains_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeWorkerGroupAdapter(
        worker_groups=[FakeWorkerGroup(id=7, name="analytics", description="old")]
    )
    _install_worker_group_service_fakes(monkeypatch, adapter, ds_version="3.1.2")
    result = worker_group_service.update_worker_group_result(
        "analytics", description="new"
    )
    assert _mapping(result.data)["description"] == "new"


def test_update_worker_group_result_rejects_config_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeWorkerGroupAdapter(
        worker_groups=[
            FakeWorkerGroup(
                id=None,
                name="config-default",
                addr_list_value="worker-a:1234",
                system_default=True,
            )
        ]
    )
    _install_worker_group_service_fakes(monkeypatch, adapter)

    with pytest.raises(InvalidStateError, match="Config-derived") as exc_info:
        worker_group_service.update_worker_group_result(
            "config-default",
            description="new",
        )

    assert exc_info.value.suggestion == (
        "Run `dsctl worker-group list` to select a DB-backed worker group row. "
        "Config-derived worker groups are read-only in CRUD APIs."
    )


def test_update_worker_group_result_requires_one_change(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeWorkerGroupAdapter(
        worker_groups=[FakeWorkerGroup(id=7, name="default")]
    )
    _install_worker_group_service_fakes(monkeypatch, adapter)

    with pytest.raises(UserInputError, match="at least one field change") as exc_info:
        worker_group_service.update_worker_group_result("default")

    assert exc_info.value.suggestion == (
        "Pass at least one update flag such as --name, --addr, or --description."
    )


def test_delete_worker_group_result_returns_deleted_confirmation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeWorkerGroupAdapter(
        worker_groups=[FakeWorkerGroup(id=7, name="default")]
    )
    _install_worker_group_service_fakes(monkeypatch, adapter)

    result = worker_group_service.delete_worker_group_result("default", force=True)
    data = _mapping(result.data)

    assert result.resolved == {
        "workerGroup": {
            "id": 7,
            "name": "default",
            "addrList": None,
            "systemDefault": False,
        }
    }
    assert data == {
        "deleted": True,
        "workerGroup": {
            "id": 7,
            "name": "default",
            "addrList": None,
            "systemDefault": False,
        },
    }
