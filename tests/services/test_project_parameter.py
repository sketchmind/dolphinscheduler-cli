from collections.abc import Mapping, Sequence

import pytest
from tests.fakes import (
    FakeProject,
    FakeProjectAdapter,
    FakeProjectParameter,
    FakeProjectParameterAdapter,
    fake_service_runtime,
)
from tests.support import make_profile

from dsctl.context import SessionContext
from dsctl.errors import ConflictError, UserInputError
from dsctl.services import project_parameter as project_parameter_service
from dsctl.services import runtime as runtime_service


def _install_project_parameter_service_fakes(
    monkeypatch: pytest.MonkeyPatch,
    *,
    project_adapter: FakeProjectAdapter,
    project_parameter_adapter: FakeProjectParameterAdapter,
    context: SessionContext | None = None,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            project_adapter,
            project_parameter_adapter=project_parameter_adapter,
            profile=make_profile(),
            context=context,
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
def fake_project_adapter() -> FakeProjectAdapter:
    return FakeProjectAdapter(
        projects=[
            FakeProject(code=7, name="etl-prod"),
            FakeProject(code=8, name="other"),
        ]
    )


@pytest.fixture
def fake_project_parameter_adapter() -> FakeProjectParameterAdapter:
    return FakeProjectParameterAdapter(
        project_parameters=[
            FakeProjectParameter(
                code=101,
                project_code_value=7,
                param_name_value="warehouse_db",
                param_value_value="jdbc:mysql://warehouse",
                param_data_type_value="VARCHAR",
            ),
            FakeProjectParameter(
                code=102,
                project_code_value=7,
                param_name_value="parallelism",
                param_value_value="4",
                param_data_type_value="INT",
            ),
            FakeProjectParameter(
                code=201,
                project_code_value=8,
                param_name_value="warehouse_db",
                param_value_value="jdbc:mysql://other",
                param_data_type_value="VARCHAR",
            ),
        ]
    )


def test_list_project_parameters_result_uses_selected_project(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_project_parameter_adapter: FakeProjectParameterAdapter,
) -> None:
    _install_project_parameter_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        project_parameter_adapter=fake_project_parameter_adapter,
        context=SessionContext(project="etl-prod"),
    )

    result = project_parameter_service.list_project_parameters_result(page_size=1)
    data = _mapping(result.data)
    items = _sequence(data["totalList"])

    assert data["total"] == 2
    assert data["pageSize"] == 1
    assert result.resolved == {
        "project": {
            "code": 7,
            "name": "etl-prod",
            "description": None,
            "source": "context",
        },
        "search": None,
        "data_type": None,
        "page_no": 1,
        "page_size": 1,
        "all": False,
    }
    assert list(items) == [
        {
            "id": None,
            "userId": None,
            "operator": None,
            "code": 101,
            "projectCode": 7,
            "paramName": "warehouse_db",
            "paramValue": "jdbc:mysql://warehouse",
            "paramDataType": "VARCHAR",
            "createTime": None,
            "updateTime": None,
            "createUser": None,
            "modifyUser": None,
        }
    ]


def test_get_project_parameter_result_resolves_name_within_project(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_project_parameter_adapter: FakeProjectParameterAdapter,
) -> None:
    _install_project_parameter_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        project_parameter_adapter=fake_project_parameter_adapter,
    )

    result = project_parameter_service.get_project_parameter_result(
        "warehouse_db",
        project="etl-prod",
    )
    data = _mapping(result.data)

    assert result.resolved == {
        "project": {
            "code": 7,
            "name": "etl-prod",
            "description": None,
            "source": "flag",
        },
        "projectParameter": {
            "code": 101,
            "paramName": "warehouse_db",
            "paramDataType": "VARCHAR",
        },
    }
    assert data["code"] == 101
    assert data["projectCode"] == 7
    assert data["paramName"] == "warehouse_db"


def test_create_project_parameter_result_returns_created_parameter(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
) -> None:
    adapter = FakeProjectParameterAdapter(project_parameters=[])
    _install_project_parameter_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        project_parameter_adapter=adapter,
    )

    result = project_parameter_service.create_project_parameter_result(
        project="etl-prod",
        name="warehouse_db",
        value="jdbc:mysql://warehouse",
        data_type="VARCHAR",
    )
    data = _mapping(result.data)

    assert data["code"] == 1
    assert data["projectCode"] == 7
    assert data["paramName"] == "warehouse_db"
    assert data["paramValue"] == "jdbc:mysql://warehouse"


def test_update_project_parameter_result_preserves_omitted_fields_and_empty_value(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_project_parameter_adapter: FakeProjectParameterAdapter,
) -> None:
    _install_project_parameter_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        project_parameter_adapter=fake_project_parameter_adapter,
    )

    result = project_parameter_service.update_project_parameter_result(
        "warehouse_db",
        project="etl-prod",
        value="",
    )
    data = _mapping(result.data)

    assert data["code"] == 101
    assert data["paramName"] == "warehouse_db"
    assert data["paramValue"] == ""
    assert data["paramDataType"] == "VARCHAR"


def test_update_project_parameter_result_requires_change(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_project_parameter_adapter: FakeProjectParameterAdapter,
) -> None:
    _install_project_parameter_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        project_parameter_adapter=fake_project_parameter_adapter,
    )

    with pytest.raises(UserInputError, match="requires at least one field change"):
        project_parameter_service.update_project_parameter_result(
            "warehouse_db",
            project="etl-prod",
        )


def test_create_project_parameter_result_translates_duplicate_name(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_project_parameter_adapter: FakeProjectParameterAdapter,
) -> None:
    _install_project_parameter_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        project_parameter_adapter=fake_project_parameter_adapter,
    )

    with pytest.raises(ConflictError, match="already exists"):
        project_parameter_service.create_project_parameter_result(
            project="etl-prod",
            name="warehouse_db",
            value="jdbc:mysql://duplicate",
            data_type="VARCHAR",
        )


def test_delete_project_parameter_result_requires_force(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_project_parameter_adapter: FakeProjectParameterAdapter,
) -> None:
    _install_project_parameter_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        project_parameter_adapter=fake_project_parameter_adapter,
    )

    with pytest.raises(UserInputError, match="requires --force"):
        project_parameter_service.delete_project_parameter_result(
            "warehouse_db",
            project="etl-prod",
            force=False,
        )
