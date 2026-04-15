from dsctl.upstream import get_enum_spec, supported_enum_names


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
