from __future__ import annotations

from dataclasses import dataclass
from functools import cache
from typing import TYPE_CHECKING

from dsctl.errors import ConfigError
from dsctl.generated.versions.ds_3_4_1.plugin.datasource_api.datasource import (
    BaseDataSourceParamDTO,
)
from dsctl.generated.versions.ds_3_4_1.spi.enums.db_type import DbType
from dsctl.upstream.registry import normalize_version

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject

DATASOURCE_CONTRACT_VERSION = "3.4.1"

_FIELD_VALUE_TYPES: dict[str, str] = {
    "id": "integer",
    "name": "string",
    "note": "string",
    "host": "string",
    "port": "integer",
    "database": "string",
    "userName": "string",
    "password": "string",
    "other": "object<string,string>",
    "type": "enum",
}
_FIELD_DESCRIPTIONS: dict[str, str] = {
    "id": "Datasource id. Omit on create; update may include the selected id.",
    "name": "Datasource display name.",
    "note": "Optional datasource description.",
    "host": "Datasource host or DS plugin address field.",
    "port": "Datasource port.",
    "database": "Database, schema, service, or catalog value used by the plugin.",
    "userName": "Datasource login user where the plugin requires one.",
    "password": "Datasource password or secret where the plugin requires one.",
    "other": "JDBC or plugin-specific key/value options.",
    "type": "Datasource type. Values come from DS DbType.",
}
_CLI_REQUIRED_FIELDS = frozenset({"name", "type"})


@dataclass(frozen=True)
class DataSourcePayloadFieldSpec:
    """One generated datasource payload field exposed above upstream."""

    name: str
    value_type: str
    cli_required: bool
    description: str
    choices: tuple[str, ...] = ()

    def to_data(self) -> JsonObject:
        """Return a JSON-safe field schema payload."""
        data: JsonObject = {
            "name": self.name,
            "value_type": self.value_type,
            "required_by_cli": self.cli_required,
            "description": self.description,
        }
        if self.choices:
            data["choices"] = list(self.choices)
        return data


def datasource_type_names(version: str) -> tuple[str, ...]:
    """Return DS datasource type wire values for one contract version."""
    _require_datasource_contract_version(version)
    return tuple(member.value for member in DbType)


def normalize_datasource_type(version: str, datasource_type: str) -> str | None:
    """Return the canonical DS datasource type, accepting common enum aliases."""
    _require_datasource_contract_version(version)
    requested = _normalize_datasource_type_key(datasource_type)
    if not requested:
        return None
    for member in DbType:
        aliases = (
            member.name,
            member.value,
            member.name_field,
            member.descp,
        )
        if requested in {_normalize_datasource_type_key(alias) for alias in aliases}:
            return member.value
    return None


@cache
def datasource_base_payload_fields(
    version: str,
) -> tuple[DataSourcePayloadFieldSpec, ...]:
    """Return generated BaseDataSourceParamDTO field metadata."""
    _require_datasource_contract_version(version)
    fields = BaseDataSourceParamDTO.model_fields
    type_names = datasource_type_names(version)
    return tuple(
        DataSourcePayloadFieldSpec(
            name=field_name,
            value_type=_FIELD_VALUE_TYPES.get(field_name, "json"),
            cli_required=field_name in _CLI_REQUIRED_FIELDS,
            description=_FIELD_DESCRIPTIONS.get(
                field_name,
                "Datasource payload field.",
            ),
            choices=type_names if field_name == "type" else (),
        )
        for field_name in fields
    )


def _require_datasource_contract_version(version: str) -> None:
    normalized = normalize_version(version)
    if normalized == DATASOURCE_CONTRACT_VERSION:
        return
    supported_versions = [DATASOURCE_CONTRACT_VERSION]
    message = f"Unsupported datasource contract version {version!r}"
    raise ConfigError(
        message,
        details={
            "version": version,
            "supported_versions": supported_versions,
        },
    )


def _normalize_datasource_type_key(value: str) -> str:
    return value.strip().upper().replace("-", "_")


__all__ = [
    "DATASOURCE_CONTRACT_VERSION",
    "DataSourcePayloadFieldSpec",
    "datasource_base_payload_fields",
    "datasource_type_names",
    "normalize_datasource_type",
]
