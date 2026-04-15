from collections.abc import Mapping, Sequence

import pytest
from tests.fakes import (
    FakeAudit,
    FakeAuditAdapter,
    FakeAuditModelType,
    FakeAuditOperationType,
    FakeProjectAdapter,
    fake_service_runtime,
)
from tests.support import make_profile

from dsctl.errors import UserInputError
from dsctl.services import audit as audit_service
from dsctl.services import runtime as runtime_service


def _install_audit_service_fakes(
    monkeypatch: pytest.MonkeyPatch,
    audit_adapter: FakeAuditAdapter,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            FakeProjectAdapter(projects=[]),
            audit_adapter=audit_adapter,
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


@pytest.fixture
def fake_audit_adapter() -> FakeAuditAdapter:
    return FakeAuditAdapter(
        audit_logs=[
            FakeAudit(
                user_name_value="alice",
                model_type_value="Workflow",
                model_name_value="daily-etl",
                operation_value="Create",
                create_time_value="2026-04-11 10:00:00",
                description_value="created workflow",
                detail_value="{}",
                latency_value="120",
            ),
            FakeAudit(
                user_name_value="bob",
                model_type_value="Task",
                model_name_value="extract-orders",
                operation_value="Update",
                create_time_value="2026-04-11 11:00:00",
                description_value="updated task",
                detail_value="{}",
                latency_value="80",
            ),
        ],
        model_types=[
            FakeAuditModelType(
                name="Project",
                child=[
                    FakeAuditModelType(name="Workflow"),
                    FakeAuditModelType(name="Task"),
                ],
            )
        ],
        operation_types=[
            FakeAuditOperationType(name="Create"),
            FakeAuditOperationType(name="Update"),
        ],
    )


def test_list_audit_logs_result_returns_first_page_by_default(
    monkeypatch: pytest.MonkeyPatch,
    fake_audit_adapter: FakeAuditAdapter,
) -> None:
    _install_audit_service_fakes(monkeypatch, fake_audit_adapter)

    result = audit_service.list_audit_logs_result(page_size=1)
    data = _mapping(result.data)
    items = _sequence(data["totalList"])

    assert data["total"] == 2
    assert data["pageSize"] == 1
    assert data["currentPage"] == 1
    assert list(items) == [
        {
            "userName": "alice",
            "modelType": "Workflow",
            "modelName": "daily-etl",
            "operation": "Create",
            "createTime": "2026-04-11 10:00:00",
            "description": "created workflow",
            "detail": "{}",
            "latency": "120",
        }
    ]


def test_list_audit_logs_result_applies_filters(
    monkeypatch: pytest.MonkeyPatch,
    fake_audit_adapter: FakeAuditAdapter,
) -> None:
    _install_audit_service_fakes(monkeypatch, fake_audit_adapter)

    result = audit_service.list_audit_logs_result(
        model_types=["Task"],
        operation_types=["Update"],
        user_name="bob",
        model_name="extract",
        start="2026-04-11 10:30:00",
        end="2026-04-11 11:30:00",
    )
    data = _mapping(result.data)
    items = _sequence(data["totalList"])

    assert data["total"] == 1
    assert next(iter(items)) == {
        "userName": "bob",
        "modelType": "Task",
        "modelName": "extract-orders",
        "operation": "Update",
        "createTime": "2026-04-11 11:00:00",
        "description": "updated task",
        "detail": "{}",
        "latency": "80",
    }


def test_list_audit_model_types_result_returns_tree(
    monkeypatch: pytest.MonkeyPatch,
    fake_audit_adapter: FakeAuditAdapter,
) -> None:
    _install_audit_service_fakes(monkeypatch, fake_audit_adapter)

    result = audit_service.list_audit_model_types_result()
    payload = _sequence(result.data)

    assert result.resolved == {"source": "projects/audit/audit-log-model-type"}
    assert list(payload) == [
        {
            "name": "Project",
            "child": [
                {"name": "Workflow", "child": None},
                {"name": "Task", "child": None},
            ],
        }
    ]


def test_list_audit_operation_types_result_returns_list(
    monkeypatch: pytest.MonkeyPatch,
    fake_audit_adapter: FakeAuditAdapter,
) -> None:
    _install_audit_service_fakes(monkeypatch, fake_audit_adapter)

    result = audit_service.list_audit_operation_types_result()

    assert result.resolved == {"source": "projects/audit/audit-log-operation-type"}
    assert _sequence(result.data) == [
        {"name": "Create"},
        {"name": "Update"},
    ]


def test_list_audit_logs_result_rejects_invalid_datetime() -> None:
    with pytest.raises(
        UserInputError,
        match="must match DS datetime format",
    ) as exc_info:
        audit_service.list_audit_logs_result(start="2026-04-11T10:00:00")

    assert exc_info.value.suggestion == (
        "Pass --start in '%Y-%m-%d %H:%M:%S' format, for example '2026-04-11 10:00:00'."
    )


def test_list_audit_logs_result_rejects_inverted_range() -> None:
    with pytest.raises(UserInputError, match="end must be greater than") as exc_info:
        audit_service.list_audit_logs_result(
            start="2026-04-11 11:00:00",
            end="2026-04-11 10:00:00",
        )

    assert exc_info.value.suggestion == (
        "Pass an --end value that is later than or equal to --start."
    )


def test_list_audit_logs_result_rejects_empty_model_type() -> None:
    with pytest.raises(
        UserInputError,
        match="model type must not be empty",
    ) as exc_info:
        audit_service.list_audit_logs_result(model_types=[" "])

    assert exc_info.value.suggestion == "Pass a non-empty --model-type value."
