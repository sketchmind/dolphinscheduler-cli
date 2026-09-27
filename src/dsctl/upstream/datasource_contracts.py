from __future__ import annotations

from dataclasses import dataclass
from functools import cache
from typing import TYPE_CHECKING

from dsctl.errors import ConfigError
from dsctl.generated.datasource_profiles import (
    DATASOURCE_PROFILES,
    TARGET_DATASOURCE_VERSIONS,
)
from dsctl.upstream.registry import normalize_version

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dsctl.support.json_types import JsonObject


_FIELD_VALUE_TYPES: dict[str, str] = {
    "id": "integer",
    "name": "string",
    "note": "string",
    "host": "string",
    "port": "integer",
    "database": "string",
    "principal": "string",
    "userName": "string",
    "password": "string",
    "connectType": "enum",
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
    "principal": "Kerberos principal for a legacy or HDFS datasource.",
    "userName": "Datasource login user where the plugin requires one.",
    "password": "Datasource password or secret where the plugin requires one.",
    "connectType": "Oracle SID or service-name connection mode.",
    "other": "JDBC or plugin-specific key/value options.",
    "type": "Datasource type. Values come from the exact DS DbType.",
}
_CLI_REQUIRED_FIELDS = frozenset({"name", "type"})


@dataclass(frozen=True)
class DataSourcePayloadFieldSpec:
    """One source-reviewed datasource payload field exposed above upstream."""

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


@dataclass(frozen=True)
class DataSourcePayloadContract:
    """Exact-version datasource authoring facts reviewed from DS plugins."""

    ds_version: str
    type_names: tuple[str, ...]
    type_aliases: Mapping[str, str]
    base_field_names: tuple[str, ...]
    plugin_fields_by_type: Mapping[str, tuple[str, ...]]
    sensitive_field_names: tuple[str, ...]

    def fields_for_type(self, datasource_type: str) -> tuple[str, ...]:
        """Return the exact accepted field names for one datasource type."""
        normalized = normalize_datasource_type(self.ds_version, datasource_type)
        if normalized is None:
            return ()
        extras = self.plugin_fields_by_type.get(normalized, ())
        return tuple(dict.fromkeys((*self.base_field_names, *extras)))


def datasource_type_names(version: str) -> tuple[str, ...]:
    """Return exact DS datasource type wire values for one reviewed version."""
    return datasource_payload_contract(version).type_names


def normalize_datasource_type(version: str, datasource_type: str) -> str | None:
    """Return one canonical datasource type for an exact reviewed version."""
    requested = _normalize_datasource_type_key(datasource_type)
    if not requested:
        return None
    return datasource_payload_contract(version).type_aliases.get(requested)


@cache
def datasource_base_payload_fields(
    version: str,
) -> tuple[DataSourcePayloadFieldSpec, ...]:
    """Return exact-version base datasource payload metadata."""
    contract = datasource_payload_contract(version)
    return tuple(
        DataSourcePayloadFieldSpec(
            name=field_name,
            value_type=_FIELD_VALUE_TYPES.get(field_name, "json"),
            cli_required=field_name in _CLI_REQUIRED_FIELDS,
            description=_FIELD_DESCRIPTIONS.get(
                field_name,
                "Datasource payload field.",
            ),
            choices=contract.type_names if field_name == "type" else (),
        )
        for field_name in contract.base_field_names
    )


def datasource_payload_field_names(
    version: str,
    datasource_type: str,
) -> tuple[str, ...]:
    """Return exact base plus plugin DTO field names for one datasource type."""
    return datasource_payload_contract(version).fields_for_type(datasource_type)


def datasource_sensitive_payload_fields(version: str) -> tuple[str, ...]:
    """Return fields that must never cross a CLI output boundary unredacted."""
    return datasource_payload_contract(version).sensitive_field_names


@cache
def datasource_payload_contract(version: str) -> DataSourcePayloadContract:
    """Load one fail-closed, source-reviewed datasource authoring contract."""
    normalized = normalize_version(version)
    profile = DATASOURCE_PROFILES.get(normalized)
    if profile is None:
        message = f"Unsupported datasource contract version {version!r}"
        raise ConfigError(
            message,
            details={
                "version": version,
                "supported_versions": list(TARGET_DATASOURCE_VERSIONS),
            },
        )
    return DataSourcePayloadContract(
        ds_version=normalized,
        type_names=tuple(profile["type_names"]),
        type_aliases=dict(profile["type_aliases"]),
        base_field_names=tuple(profile["base_field_names"]),
        plugin_fields_by_type={
            datasource_type: tuple(field_names)
            for datasource_type, field_names in profile["plugin_fields_by_type"].items()
        },
        sensitive_field_names=tuple(profile["sensitive_field_names"]),
    )


def _normalize_datasource_type_key(value: str) -> str:
    return "_".join(value.strip().upper().replace("-", " ").split())


__all__ = [
    "DataSourcePayloadContract",
    "DataSourcePayloadFieldSpec",
    "datasource_base_payload_fields",
    "datasource_payload_contract",
    "datasource_payload_field_names",
    "datasource_sensitive_payload_fields",
    "datasource_type_names",
    "normalize_datasource_type",
]
