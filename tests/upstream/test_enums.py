from dsctl.upstream import (
    datasource_base_payload_fields,
    datasource_type_names,
    get_enum_spec,
    normalize_datasource_type,
    supported_enum_names,
)


def test_supported_enum_names_are_sorted_and_stable() -> None:
    names = supported_enum_names("3.4.1")

    assert names == tuple(sorted(names))
    assert "priority" in names
    assert "resource-type" in names
    assert "workflow-execution-status" in names


def test_get_enum_spec_resolves_aliases_and_member_attributes() -> None:
    spec = get_enum_spec("3.4.1", "DbType")

    assert spec is not None
    assert spec.name == "db-type"
    assert spec.module == "spi.enums.db_type"
    assert spec.class_name == "DbType"
    assert spec.value_type == "string"
    assert spec.members[0].name == "MYSQL"
    assert spec.members[0].attributes == {
        "code": 0,
        "descp": "mysql",
        "name_field": "mysql",
    }


def test_datasource_contract_exposes_db_type_and_payload_fields() -> None:
    assert datasource_type_names("3.4.1")[:3] == (
        "MYSQL",
        "POSTGRESQL",
        "HIVE",
    )
    assert normalize_datasource_type("3.4.1", "mysql") == "MYSQL"
    assert (
        normalize_datasource_type("3.4.1", "aliyun-serverless-spark")
        == "ALIYUN_SERVERLESS_SPARK"
    )

    fields = datasource_base_payload_fields("3.4.1")
    assert [field.name for field in fields] == [
        "id",
        "name",
        "note",
        "host",
        "port",
        "database",
        "userName",
        "password",
        "other",
        "type",
    ]
    type_field = fields[-1]
    assert type_field.name == "type"
    assert type_field.cli_required is True
    assert "MYSQL" in type_field.choices
