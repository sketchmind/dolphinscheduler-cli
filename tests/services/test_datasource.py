import json
from collections.abc import Mapping, Sequence
from pathlib import Path

import pytest
from tests.fakes import (
    FakeDataSource,
    FakeDataSourceAdapter,
    FakeEnumValue,
    FakeProjectAdapter,
    fake_service_runtime,
)
from tests.support import make_profile

from dsctl.errors import ConflictError, UserInputError
from dsctl.services import datasource as datasource_service
from dsctl.services import runtime as runtime_service


def _install_datasource_service_fakes(
    monkeypatch: pytest.MonkeyPatch,
    adapter: FakeDataSourceAdapter,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            FakeProjectAdapter(projects=[]),
            datasource_adapter=adapter,
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


def _write_json(path: Path, payload: Mapping[str, object]) -> Path:
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_list_datasources_result_returns_first_page_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeDataSourceAdapter(
        datasources=[
            FakeDataSource(id=7, name="warehouse", type_value=FakeEnumValue("MYSQL")),
            FakeDataSource(
                id=9,
                name="analytics",
                type_value=FakeEnumValue("POSTGRESQL"),
            ),
        ]
    )
    _install_datasource_service_fakes(monkeypatch, adapter)

    result = datasource_service.list_datasources_result(page_size=1)
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
            "name": "warehouse",
            "note": None,
            "type": "MYSQL",
            "userId": 0,
            "userName": None,
            "createTime": None,
            "updateTime": None,
        }
    ]


def test_get_datasource_result_resolves_name_then_fetches_detail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeDataSourceAdapter(
        datasources=[
            FakeDataSource(
                id=7,
                name="warehouse",
                note="main warehouse",
                type_value=FakeEnumValue("MYSQL"),
                detail_payload_value={
                    "host": "db.example",
                    "port": 3306,
                    "password": "******",
                },
            )
        ]
    )
    _install_datasource_service_fakes(monkeypatch, adapter)

    result = datasource_service.get_datasource_result("warehouse")
    data = _mapping(result.data)

    assert result.resolved == {
        "datasource": {
            "id": 7,
            "name": "warehouse",
            "note": "main warehouse",
            "type": "MYSQL",
        }
    }
    assert data["host"] == "db.example"
    assert data["type"] == "MYSQL"


def test_create_datasource_result_returns_created_detail(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    payload = {
        "name": "warehouse",
        "type": "MYSQL",
        "host": "db.example",
        "port": 3306,
        "database": "warehouse",
        "userName": "etl",
        "password": "secret",
    }
    adapter = FakeDataSourceAdapter(datasources=[])
    _install_datasource_service_fakes(monkeypatch, adapter)
    file = _write_json(tmp_path / "warehouse.json", payload)

    result = datasource_service.create_datasource_result(file=file)
    data = _mapping(result.data)

    assert result.resolved == {
        "datasource": {
            "id": 1,
            "name": "warehouse",
            "note": None,
            "type": "MYSQL",
        }
    }
    assert data["id"] == 1
    assert data["name"] == "warehouse"
    assert data["password"] == payload["password"]


def test_create_datasource_result_maps_duplicate_name_to_conflict(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    adapter = FakeDataSourceAdapter(
        datasources=[
            FakeDataSource(id=7, name="warehouse", type_value=FakeEnumValue("MYSQL"))
        ]
    )
    _install_datasource_service_fakes(monkeypatch, adapter)
    file = _write_json(
        tmp_path / "duplicate.json",
        {
            "name": "warehouse",
            "type": "MYSQL",
            "password": "secret",
        },
    )

    with pytest.raises(ConflictError):
        datasource_service.create_datasource_result(file=file)


def test_create_datasource_result_rejects_payload_with_id(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    adapter = FakeDataSourceAdapter(datasources=[])
    _install_datasource_service_fakes(monkeypatch, adapter)
    file = _write_json(
        tmp_path / "with-id.json",
        {
            "id": 7,
            "name": "warehouse",
            "type": "MYSQL",
            "password": "secret",
        },
    )

    with pytest.raises(
        UserInputError,
        match="Datasource create payload must not include id",
    ) as exc_info:
        datasource_service.create_datasource_result(file=file)

    assert exc_info.value.suggestion == (
        "Remove `id` from the create payload; DS assigns it."
    )


def test_update_datasource_result_preserves_existing_password_when_masked(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    adapter = FakeDataSourceAdapter(
        datasources=[
            FakeDataSource(
                id=7,
                name="warehouse",
                note="main warehouse",
                type_value=FakeEnumValue("MYSQL"),
                detail_payload_value={"password": "******"},
            )
        ]
    )
    _install_datasource_service_fakes(monkeypatch, adapter)
    file = _write_json(
        tmp_path / "update.json",
        {
            "id": 7,
            "name": "warehouse",
            "type": "MYSQL",
            "note": "new note",
            "password": "******",
        },
    )

    result = datasource_service.update_datasource_result("warehouse", file=file)
    data = _mapping(result.data)

    assert result.warnings == [
        "datasource update: masked password placeholder detected; "
        "preserving the existing password"
    ]
    assert result.warning_details == [
        {
            "code": "datasource_update_preserved_existing_password",
            "message": (
                "datasource update: masked password placeholder detected; "
                "preserving the existing password"
            ),
            "field": "password",
            "reason": "masked_placeholder",
            "preserved_existing": True,
        }
    ]
    assert data["password"] == ""
    assert adapter.get(datasource_id=7)["password"] == ""


def test_update_datasource_result_rejects_mismatched_payload_id(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    adapter = FakeDataSourceAdapter(
        datasources=[
            FakeDataSource(id=7, name="warehouse", type_value=FakeEnumValue("MYSQL"))
        ]
    )
    _install_datasource_service_fakes(monkeypatch, adapter)
    file = _write_json(
        tmp_path / "mismatch.json",
        {
            "id": 9,
            "name": "warehouse",
            "type": "MYSQL",
        },
    )

    with pytest.raises(UserInputError, match="did not match") as exc_info:
        datasource_service.update_datasource_result("warehouse", file=file)

    assert exc_info.value.suggestion == (
        "Update the payload `id` to match the selected datasource, or remove "
        "`id` and let the CLI target control the update."
    )


def test_create_datasource_result_rejects_invalid_json_payload(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    adapter = FakeDataSourceAdapter(datasources=[])
    _install_datasource_service_fakes(monkeypatch, adapter)
    file = tmp_path / "invalid.json"
    file.write_text("{invalid", encoding="utf-8")

    with pytest.raises(UserInputError, match="not valid JSON") as exc_info:
        datasource_service.create_datasource_result(file=file)

    assert exc_info.value.suggestion == (
        "Fix the JSON syntax, or regenerate a DS-native payload with `dsctl "
        "datasource get DATASOURCE`."
    )


def test_delete_datasource_result_returns_deleted_confirmation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeDataSourceAdapter(
        datasources=[
            FakeDataSource(id=7, name="warehouse", type_value=FakeEnumValue("MYSQL"))
        ]
    )
    _install_datasource_service_fakes(monkeypatch, adapter)

    result = datasource_service.delete_datasource_result("warehouse", force=True)
    data = _mapping(result.data)

    assert result.resolved == {
        "datasource": {
            "id": 7,
            "name": "warehouse",
            "note": None,
            "type": "MYSQL",
        }
    }
    assert data["deleted"] is True


def test_connection_test_datasource_result_returns_boolean_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeDataSourceAdapter(
        datasources=[
            FakeDataSource(id=7, name="warehouse", type_value=FakeEnumValue("MYSQL"))
        ],
        connection_test_results={7: False},
    )
    _install_datasource_service_fakes(monkeypatch, adapter)

    result = datasource_service.connection_test_datasource_result("warehouse")
    data = _mapping(result.data)

    assert data == {
        "connected": False,
        "datasource": {
            "id": 7,
            "name": "warehouse",
            "note": None,
            "type": "MYSQL",
        },
    }
