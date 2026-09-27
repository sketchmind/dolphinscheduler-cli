import pytest
from tests.bound_domain_fakes import patch_bound_domain_service_runtime
from tests.fakes import (
    FakeQueue,
    FakeQueueAdapter,
)
from tests.support import make_profile
from tests.value_shape_assertions import assert_mapping as _mapping
from tests.value_shape_assertions import assert_sequence as _sequence

from dsctl.errors import ConflictError, UserInputError
from dsctl.services import queue as queue_service
from dsctl.upstream.queues import QUEUE_DOMAIN, QueueDomain


def _install_queue_service_fakes(
    monkeypatch: pytest.MonkeyPatch,
    adapter: FakeQueueAdapter,
) -> None:
    domain = QueueDomain(queues=adapter)

    patch_bound_domain_service_runtime(
        monkeypatch,
        queue_service,
        expected_domain=QUEUE_DOMAIN,
        runtime_domain=domain,
        profile_factory=make_profile,
    )


def test_list_queues_result_returns_first_page_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeQueueAdapter(
        queues=[
            FakeQueue(id=7, queue_name_value="default", queue="root.default"),
            FakeQueue(id=9, queue_name_value="analytics", queue="root.analytics"),
        ]
    )
    _install_queue_service_fakes(monkeypatch, adapter)

    result = queue_service.list_queues_result(page_size=1)
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
            "queueName": "default",
            "queue": "root.default",
            "createTime": None,
            "updateTime": None,
        }
    ]


def test_get_queue_result_resolves_name_then_fetches_queue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeQueueAdapter(
        queues=[FakeQueue(id=7, queue_name_value="default", queue="root.default")]
    )
    _install_queue_service_fakes(monkeypatch, adapter)

    result = queue_service.get_queue_result("default")
    data = _mapping(result.data)

    assert result.resolved == {
        "queue": {
            "id": 7,
            "queueName": "default",
            "queue": "root.default",
        }
    }
    assert data == {
        "id": 7,
        "queueName": "default",
        "queue": "root.default",
        "createTime": None,
        "updateTime": None,
    }


def test_create_queue_result_returns_created_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeQueueAdapter(queues=[])
    _install_queue_service_fakes(monkeypatch, adapter)

    result = queue_service.create_queue_result(
        queue_name="analytics",
        queue="root.analytics",
    )
    data = _mapping(result.data)

    assert result.resolved == {
        "queue": {
            "id": 1,
            "queueName": "analytics",
            "queue": "root.analytics",
        }
    }
    assert data["id"] == 1
    assert data["queueName"] == "analytics"
    assert data["queue"] == "root.analytics"


def test_create_queue_result_maps_duplicate_name_to_conflict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeQueueAdapter(
        queues=[FakeQueue(id=7, queue_name_value="default", queue="root.default")]
    )
    _install_queue_service_fakes(monkeypatch, adapter)

    with pytest.raises(ConflictError):
        queue_service.create_queue_result(
            queue_name="default",
            queue="root.analytics",
        )


def test_update_queue_result_preserves_omitted_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeQueueAdapter(
        queues=[FakeQueue(id=7, queue_name_value="default", queue="root.default")]
    )
    _install_queue_service_fakes(monkeypatch, adapter)

    result = queue_service.update_queue_result(
        "default",
        queue="root.ops",
    )
    data = _mapping(result.data)

    assert result.resolved == {
        "queue": {
            "id": 7,
            "queueName": "default",
            "queue": "root.default",
        }
    }
    assert data["queueName"] == "default"
    assert data["queue"] == "root.ops"


def test_update_queue_result_requires_one_change(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeQueueAdapter(
        queues=[FakeQueue(id=7, queue_name_value="default", queue="root.default")]
    )
    _install_queue_service_fakes(monkeypatch, adapter)

    with pytest.raises(UserInputError, match="at least one field change") as exc_info:
        queue_service.update_queue_result("default")

    assert exc_info.value.suggestion == (
        "Pass at least one update flag such as --queue-name or --queue."
    )


def test_delete_queue_result_returns_deleted_confirmation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeQueueAdapter(
        queues=[FakeQueue(id=7, queue_name_value="default", queue="root.default")]
    )
    _install_queue_service_fakes(monkeypatch, adapter)

    result = queue_service.delete_queue_result("default", force=True)
    data = _mapping(result.data)

    assert result.resolved == {
        "queue": {
            "id": 7,
            "queueName": "default",
            "queue": "root.default",
        }
    }
    assert data == {
        "deleted": True,
        "queue": {
            "id": 7,
            "queueName": "default",
            "queue": "root.default",
        },
    }
