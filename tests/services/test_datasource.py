import json
from collections.abc import Mapping
from pathlib import Path

import pytest
from tests.bound_domain_fakes import patch_bound_domain_service_runtime
from tests.fakes import (
    FakeDataSource,
    FakeDataSourceAdapter,
    FakeEnumValue,
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
from dsctl.services import datasource as datasource_service
from dsctl.upstream.datasources import DATASOURCE_DOMAIN, DataSourceDomain


def _install_datasource_service_fakes(
    monkeypatch: pytest.MonkeyPatch,
    adapter: FakeDataSourceAdapter,
    *,
    ds_version: str = "3.4.1",
) -> None:
    domain = DataSourceDomain(datasources=adapter)

    patch_bound_domain_service_runtime(
        monkeypatch,
        datasource_service,
        expected_domain=DATASOURCE_DOMAIN,
        runtime_domain=domain,
        profile_factory=lambda: make_profile(ds_version=ds_version),
    )


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


def test_get_datasource_result_translates_resolution_permission_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeDataSourceAdapter(
        datasources=[],
        list_error=ApiResultError(
            result_code=30001,
            result_message="no operation permission",
        ),
    )
    _install_datasource_service_fakes(monkeypatch, adapter)

    with pytest.raises(PermissionDeniedError) as exc_info:
        datasource_service.get_datasource_result("warehouse")

    assert exc_info.value.details == {
        "operation": "get",
        "name": "warehouse",
    }


def test_get_datasource_result_recursively_redacts_all_supported_secrets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeDataSourceAdapter(
        datasources=[
            FakeDataSource(
                id=7,
                name="governance",
                type_value=FakeEnumValue("K8S"),
                detail_payload_value={
                    "password": "database-secret",
                    "accessKeySecret": "cloud-secret",
                    "kubeConfig": "kube-secret",
                    "privateKey": "ssh-secret",
                    "nested": {
                        "password": "nested-secret",
                        "items": [{"accessKeySecret": "nested-cloud-secret"}],
                    },
                },
            )
        ]
    )
    _install_datasource_service_fakes(monkeypatch, adapter)

    result = datasource_service.get_datasource_result("governance")
    data = _mapping(result.data)

    assert data["password"] == "******"
    assert data["accessKeySecret"] == "******"
    assert data["kubeConfig"] == "******"
    assert data["privateKey"] == "******"
    nested = _mapping(data["nested"])
    assert nested["password"] == "******"
    items = _sequence(nested["items"])
    assert _mapping(items[0])["accessKeySecret"] == "******"


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
    assert data["password"] == "******"


def test_create_datasource_result_normalizes_datasource_type(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    payload = {
        "name": "warehouse",
        "type": "mysql",
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

    resolved_datasource = _mapping(result.resolved["datasource"])
    assert resolved_datasource["type"] == "MYSQL"


def test_create_datasource_result_rejects_unknown_datasource_type(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    adapter = FakeDataSourceAdapter(datasources=[])
    _install_datasource_service_fakes(monkeypatch, adapter)
    file = _write_json(
        tmp_path / "unknown.json",
        {
            "name": "warehouse",
            "type": "UNKNOWN",
            "password": "secret",
        },
    )

    with pytest.raises(UserInputError, match="Unsupported datasource type") as exc_info:
        datasource_service.create_datasource_result(file=file)

    assert exc_info.value.suggestion == (
        "Run `dsctl template datasource --ds-version 3.4.1` to choose a "
        "supported datasource type, then add `--type TYPE`."
    )


def test_create_datasource_result_rejects_masked_non_password_secret(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    adapter = FakeDataSourceAdapter(datasources=[])
    _install_datasource_service_fakes(monkeypatch, adapter)
    file = _write_json(
        tmp_path / "masked-key.json",
        {
            "name": "ssh-prod",
            "type": "SSH",
            "host": "ssh.example",
            "port": 22,
            "privateKey": "******",
        },
    )

    with pytest.raises(
        UserInputError,
        match="must include real secret values",
    ) as exc_info:
        datasource_service.create_datasource_result(file=file)

    assert exc_info.value.details["masked_fields"] == ["privateKey"]


def test_create_datasource_result_rejects_fields_from_another_plugin(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    adapter = FakeDataSourceAdapter(datasources=[])
    _install_datasource_service_fakes(monkeypatch, adapter)
    file = _write_json(
        tmp_path / "wrong-plugin-field.json",
        {
            "name": "warehouse",
            "type": "MYSQL",
            "accessKeySecret": "secret",
        },
    )

    with pytest.raises(UserInputError, match="fields not accepted") as exc_info:
        datasource_service.create_datasource_result(file=file)

    assert exc_info.value.details["unexpected_fields"] == ["accessKeySecret"]


def test_create_datasource_result_uses_configured_exact_version_catalog(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    adapter = FakeDataSourceAdapter(datasources=[])
    _install_datasource_service_fakes(monkeypatch, adapter, ds_version="1.3.9")
    file = _write_json(
        tmp_path / "presto.json",
        {
            "name": "presto",
            "type": "PRESTO",
        },
    )

    with pytest.raises(UserInputError, match="Unsupported datasource type"):
        datasource_service.create_datasource_result(file=file)


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
    assert data["password"] == "******"
    assert adapter.get(datasource_id=7)["password"] == ""


def test_update_datasource_result_fails_closed_without_blank_password_contract(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    adapter = FakeDataSourceAdapter(
        datasources=[
            FakeDataSource(
                id=7,
                name="warehouse",
                type_value=FakeEnumValue("MYSQL"),
                detail_payload_value={"password": "******"},
            )
        ],
        blank_password_preserves_existing=False,
    )
    _install_datasource_service_fakes(
        monkeypatch,
        adapter,
        ds_version="1.3.9",
    )
    file = _write_json(
        tmp_path / "update.json",
        {
            "name": "warehouse",
            "type": "MYSQL",
            "password": "******",
        },
    )

    with pytest.raises(
        UserInputError,
        match="cannot safely recover masked 'password'",
    ) as exc_info:
        datasource_service.update_datasource_result("warehouse", file=file)

    assert exc_info.value.suggestion == (
        "Set 'password' to its real value in the update payload, then retry."
    )


def test_update_datasource_result_preserves_masked_plugin_secret(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    adapter = FakeDataSourceAdapter(
        datasources=[
            FakeDataSource(
                id=7,
                name="ssh-prod",
                type_value=FakeEnumValue("SSH"),
                detail_payload_value={
                    "host": "ssh.example",
                    "port": 22,
                    "privateKey": "actual-private-key",
                },
            )
        ]
    )
    _install_datasource_service_fakes(monkeypatch, adapter)
    file = _write_json(
        tmp_path / "update-ssh.json",
        {
            "name": "ssh-prod",
            "type": "SSH",
            "host": "ssh.example",
            "port": 22,
            "privateKey": "******",
        },
    )

    result = datasource_service.update_datasource_result("ssh-prod", file=file)

    assert result.warning_details[0]["field"] == "privateKey"
    assert result.warning_details[0]["code"] == (
        "datasource_update_preserved_existing_private_key"
    )
    assert result.warning_details[0]["preserved_existing"] is True
    assert adapter.get(datasource_id=7)["privateKey"] == "actual-private-key"
    assert _mapping(result.data)["privateKey"] == "******"


def test_update_datasource_result_preserves_omitted_plugin_secret_silently(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    adapter = FakeDataSourceAdapter(
        datasources=[
            FakeDataSource(
                id=7,
                name="ssh-prod",
                type_value=FakeEnumValue("SSH"),
                detail_payload_value={
                    "host": "ssh.example",
                    "port": 22,
                    "privateKey": "actual-private-key",
                },
            )
        ]
    )
    _install_datasource_service_fakes(monkeypatch, adapter)
    file = _write_json(
        tmp_path / "update-ssh.json",
        {
            "name": "ssh-prod",
            "type": "SSH",
            "host": "ssh.example",
            "port": 22,
        },
    )

    result = datasource_service.update_datasource_result("ssh-prod", file=file)

    assert result.warnings == []
    assert adapter.get(datasource_id=7)["privateKey"] == "actual-private-key"


def test_update_datasource_result_rejects_type_change(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    adapter = FakeDataSourceAdapter(
        datasources=[
            FakeDataSource(
                id=7,
                name="warehouse",
                type_value=FakeEnumValue("MYSQL"),
            )
        ]
    )
    _install_datasource_service_fakes(monkeypatch, adapter)
    file = _write_json(
        tmp_path / "change-type.json",
        {
            "name": "warehouse",
            "type": "POSTGRESQL",
        },
    )

    with pytest.raises(UserInputError, match="cannot change"):
        datasource_service.update_datasource_result("warehouse", file=file)


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
        "Fix the JSON syntax, or run `dsctl template datasource --ds-version "
        "VERSION` to choose a type and add `--type TYPE` to generate a "
        "skeleton for the target cluster version."
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
