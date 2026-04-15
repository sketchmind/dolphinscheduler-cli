from collections.abc import Mapping, Sequence

import pytest
from tests.fakes import (
    FakeProjectAdapter,
    FakeWorkerGroup,
    FakeWorkerGroupAdapter,
    fake_service_runtime,
)
from tests.support import make_profile

from dsctl.errors import ConflictError, InvalidStateError, UserInputError
from dsctl.services import runtime as runtime_service
from dsctl.services import worker_group as worker_group_service


def _install_worker_group_service_fakes(
    monkeypatch: pytest.MonkeyPatch,
    adapter: FakeWorkerGroupAdapter,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            FakeProjectAdapter(projects=[]),
            worker_group_adapter=adapter,
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
