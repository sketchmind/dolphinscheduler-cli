from collections.abc import Mapping, Sequence

import pytest
from tests.fakes import (
    FakeProjectAdapter,
    FakeTaskType,
    FakeTaskTypeAdapter,
    fake_service_runtime,
)
from tests.support import make_profile

from dsctl.errors import ApiTransportError
from dsctl.services import runtime as runtime_service
from dsctl.services import task_type as task_type_service


def _install_task_type_service_fakes(
    monkeypatch: pytest.MonkeyPatch,
    adapter: FakeTaskTypeAdapter,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            FakeProjectAdapter(projects=[]),
            profile=make_profile(),
            task_type_adapter=adapter,
        ),
    )


def _mapping(value: object) -> Mapping[str, object]:
    assert isinstance(value, Mapping)
    return value


def _sequence(value: object) -> Sequence[object]:
    assert isinstance(value, Sequence)
    assert not isinstance(value, (str, bytes, bytearray))
    return value


def test_list_task_types_result_returns_remote_payload_and_cli_coverage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeTaskTypeAdapter(
        task_types=[
            FakeTaskType(
                task_type_value="SHELL",
                is_collection_value=True,
                task_category_value="Universal",
            ),
            FakeTaskType(
                task_type_value="REMOTE_SHELL",
                is_collection_value=False,
                task_category_value="Universal",
            ),
            FakeTaskType(
                task_type_value="CUSTOM_PLUGIN",
                is_collection_value=False,
                task_category_value="Universal",
            ),
            FakeTaskType(
                task_type_value="SUB_WORKFLOW",
                is_collection_value=True,
                task_category_value="Logic",
            ),
        ]
    )
    _install_task_type_service_fakes(monkeypatch, adapter)

    result = task_type_service.list_task_types_result()
    data = _mapping(result.data)
    task_types = _sequence(data["taskTypes"])
    coverage = _mapping(data["cliCoverage"])

    assert result.resolved == {"source": "favourite/taskTypes"}
    assert data["count"] == 4
    assert list(task_types) == [
        {
            "taskType": "SHELL",
            "isCollection": True,
            "taskCategory": "Universal",
        },
        {
            "taskType": "REMOTE_SHELL",
            "isCollection": False,
            "taskCategory": "Universal",
        },
        {
            "taskType": "CUSTOM_PLUGIN",
            "isCollection": False,
            "taskCategory": "Universal",
        },
        {
            "taskType": "SUB_WORKFLOW",
            "isCollection": True,
            "taskCategory": "Logic",
        },
    ]
    assert data["taskTypesByCategory"] == {
        "Universal": ["SHELL", "REMOTE_SHELL", "CUSTOM_PLUGIN"],
        "Logic": ["SUB_WORKFLOW"],
    }
    assert "REMOTESHELL" in _sequence(coverage["taskTemplateTypes"])
    assert "SPARK" in _sequence(coverage["genericTaskTemplateTypes"])
    assert "CUSTOM_PLUGIN" in _sequence(coverage["untemplatedTaskTypes"])
    assert "REMOTE_SHELL" not in _sequence(coverage["untemplatedTaskTypes"])


def test_list_task_types_result_rejects_missing_required_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeTaskTypeAdapter(
        task_types=[
            FakeTaskType(
                task_type_value=None,
                task_category_value="Universal",
            )
        ]
    )
    _install_task_type_service_fakes(monkeypatch, adapter)

    with pytest.raises(ApiTransportError, match="missing required field 'taskType'"):
        task_type_service.list_task_types_result()
