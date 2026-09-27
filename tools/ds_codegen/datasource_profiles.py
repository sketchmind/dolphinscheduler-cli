"""Compile exact-version datasource authoring profiles from DS source contracts."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from ds_codegen.contract_visibility import is_client_supplied_parameter

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ds_codegen.ir import ContractSnapshot, DtoSpec, ModelSpec


DATASOURCE_PROFILE_SCHEMA_VERSION = 1
GENERATED_DATASOURCE_PROFILE_PATH = Path("generated/datasource_profiles.py")
DEFAULT_DATASOURCE_PROFILE_REVIEWS = Path(__file__).with_name(
    "datasource_profile_reviews.json"
)
_UPSTREAM_PACKAGE_PREFIX = "org.apache.dolphinscheduler."
_SENSITIVE_FIELD_NAMES = frozenset(
    {
        "accessKeySecret",
        "kubeConfig",
        "password",
        "privateKey",
        "publicKey",
    }
)


@dataclass(frozen=True)
class DataSourceProfile:
    """Generated datasource fields and enum values for one exact release."""

    version: str
    type_names: tuple[str, ...]
    type_aliases: dict[str, str]
    base_field_names: tuple[str, ...]
    plugin_fields_by_type: dict[str, tuple[str, ...]]
    sensitive_field_names: tuple[str, ...]


def compile_datasource_profile(
    snapshot: ContractSnapshot,
    *,
    plugin_fields_by_type: Mapping[str, tuple[str, ...]],
) -> DataSourceProfile:
    """Compile one profile from an exact closure plus reviewed plugin facets."""
    type_names, type_aliases = _db_type_profile(snapshot)
    base_field_names = _base_field_names(snapshot)
    plugin_fields = _validate_plugin_fields(
        snapshot.ds_version,
        type_names=type_names,
        base_field_names=base_field_names,
        plugin_fields_by_type=plugin_fields_by_type,
    )
    accepted_fields = list(base_field_names)
    for datasource_type in type_names:
        accepted_fields.extend(plugin_fields.get(datasource_type, ()))
    sensitive_fields = tuple(
        dict.fromkeys(
            field_name
            for field_name in accepted_fields
            if field_name in _SENSITIVE_FIELD_NAMES
        )
    )
    return DataSourceProfile(
        version=snapshot.ds_version,
        type_names=type_names,
        type_aliases=type_aliases,
        base_field_names=base_field_names,
        plugin_fields_by_type=plugin_fields,
        sensitive_field_names=sensitive_fields,
    )


def load_datasource_profile_reviews(
    path: Path = DEFAULT_DATASOURCE_PROFILE_REVIEWS,
) -> dict[str, dict[str, tuple[str, ...]]]:
    """Load source-reviewed plugin DTO facets used by the profile compiler."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        message = "datasource profile reviews must be a JSON object"
        raise TypeError(message)
    if payload.get("schema_version") != DATASOURCE_PROFILE_SCHEMA_VERSION:
        message = (
            "datasource profile reviews schema_version must be "
            f"{DATASOURCE_PROFILE_SCHEMA_VERSION}"
        )
        raise ValueError(message)
    raw_profiles = payload.get("profiles")
    raw_versions = payload.get("versions")
    if not isinstance(raw_profiles, dict) or not isinstance(raw_versions, dict):
        message = "datasource profile reviews require profiles and versions objects"
        raise TypeError(message)

    profiles: dict[str, dict[str, tuple[str, ...]]] = {}
    for profile_name, raw_profile in raw_profiles.items():
        if not isinstance(profile_name, str) or not isinstance(raw_profile, dict):
            message = "datasource plugin profiles must map names to objects"
            raise TypeError(message)
        fields_by_type: dict[str, tuple[str, ...]] = {}
        for datasource_type, raw_fields in raw_profile.items():
            if not isinstance(datasource_type, str) or not isinstance(raw_fields, list):
                message = f"datasource plugin profile {profile_name!r} is invalid"
                raise TypeError(message)
            if not all(isinstance(field_name, str) for field_name in raw_fields):
                message = (
                    f"datasource plugin profile {profile_name!r} field names "
                    "must be strings"
                )
                raise TypeError(message)
            fields = tuple(raw_fields)
            if not fields or len(fields) != len(set(fields)):
                message = (
                    f"datasource plugin profile {profile_name!r}/{datasource_type} "
                    "must contain unique fields"
                )
                raise ValueError(message)
            fields_by_type[datasource_type] = fields
        profiles[profile_name] = fields_by_type

    reviews: dict[str, dict[str, tuple[str, ...]]] = {}
    for version, profile_name in raw_versions.items():
        if not isinstance(version, str) or not isinstance(profile_name, str):
            message = "datasource review versions must map strings to strings"
            raise TypeError(message)
        profile = profiles.get(profile_name)
        if profile is None:
            message = (
                f"datasource review version {version} selects unknown profile "
                f"{profile_name!r}"
            )
            raise ValueError(message)
        reviews[version] = dict(profile)
    return reviews


def datasource_profile_data(
    profiles: tuple[DataSourceProfile, ...],
) -> dict[str, object]:
    """Project compiled profiles into the generated runtime data shape."""
    ordered = tuple(sorted(profiles, key=lambda profile: _version_key(profile.version)))
    versions = [profile.version for profile in ordered]
    if len(versions) != len(set(versions)):
        duplicates = sorted(
            version for version in set(versions) if versions.count(version) > 1
        )
        message = f"duplicate datasource profile versions: {', '.join(duplicates)}"
        raise ValueError(message)
    return {
        "schema_version": DATASOURCE_PROFILE_SCHEMA_VERSION,
        "target_versions": versions,
        "profiles": {
            profile.version: {
                "type_names": list(profile.type_names),
                "type_aliases": dict(profile.type_aliases),
                "base_field_names": list(profile.base_field_names),
                "plugin_fields_by_type": {
                    datasource_type: list(field_names)
                    for datasource_type, field_names in (
                        profile.plugin_fields_by_type.items()
                    )
                },
                "sensitive_field_names": list(profile.sensitive_field_names),
            }
            for profile in ordered
        },
    }


def render_datasource_profiles(profiles: tuple[DataSourceProfile, ...]) -> str:
    """Render canonical datasource profiles beside exact wire packages."""
    payload = json.dumps(
        datasource_profile_data(profiles),
        ensure_ascii=True,
        indent=2,
        separators=(",", ": "),
    )
    return "\n".join(
        (
            "from __future__ import annotations",
            "",
            "import json as _json",
            "from typing import TypedDict, cast",
            "",
            "# Generated by tools/generate_ds_runtime_bundles.py; do not edit.",
            "",
            "",
            "class DataSourceProfileData(TypedDict):",
            '    """Generated exact-version datasource profile payload."""',
            "",
            "    type_names: list[str]",
            "    type_aliases: dict[str, str]",
            "    base_field_names: list[str]",
            "    plugin_fields_by_type: dict[str, list[str]]",
            "    sensitive_field_names: list[str]",
            "",
            "",
            '_DATASOURCE_PROFILE_JSON = r"""',
            payload,
            '"""',
            "_DATASOURCE_PROFILE_DOCUMENT = cast(",
            '    "dict[str, object]",',
            "    _json.loads(_DATASOURCE_PROFILE_JSON),",
            ")",
            "DATASOURCE_PROFILE_SCHEMA_VERSION = cast(",
            '    "int", _DATASOURCE_PROFILE_DOCUMENT["schema_version"]',
            ")",
            "TARGET_DATASOURCE_VERSIONS = tuple(",
            '    cast("list[str]", _DATASOURCE_PROFILE_DOCUMENT["target_versions"])',
            ")",
            "DATASOURCE_PROFILES = cast(",
            '    "dict[str, DataSourceProfileData]",',
            '    _DATASOURCE_PROFILE_DOCUMENT["profiles"],',
            ")",
            "",
            "__all__ = [",
            '    "DATASOURCE_PROFILES",',
            '    "DATASOURCE_PROFILE_SCHEMA_VERSION",',
            '    "TARGET_DATASOURCE_VERSIONS",',
            '    "DataSourceProfileData",',
            "]",
            "",
        )
    )


def write_datasource_profiles(
    output_root: Path,
    profiles: tuple[DataSourceProfile, ...],
) -> Path:
    """Write the generated datasource profile module."""
    output_path = output_root / GENERATED_DATASOURCE_PROFILE_PATH
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_datasource_profiles(profiles), encoding="utf-8")
    return output_path


def _db_type_profile(
    snapshot: ContractSnapshot,
) -> tuple[tuple[str, ...], dict[str, str]]:
    matches = [
        spec
        for spec in snapshot.enums
        if spec.name == "DbType"
        and spec.import_path.startswith(_UPSTREAM_PACKAGE_PREFIX)
    ]
    if len(matches) != 1:
        message = (
            f"DS {snapshot.ds_version} must expose exactly one DS DbType; "
            f"found {len(matches)}"
        )
        raise ValueError(message)
    type_names = tuple(value.name for value in matches[0].values)
    aliases: dict[str, str] = {}
    for value in matches[0].values:
        for raw_alias in (value.name, *value.arguments):
            alias = _normalize_type_alias(raw_alias)
            if not alias or alias.isdecimal():
                continue
            existing = aliases.get(alias)
            if existing is not None and existing != value.name:
                message = (
                    f"DS {snapshot.ds_version} DbType alias {alias!r} maps to "
                    f"both {existing} and {value.name}"
                )
                raise ValueError(message)
            aliases[alias] = value.name
    return type_names, aliases


def _base_field_names(snapshot: ContractSnapshot) -> tuple[str, ...]:
    base_models: list[DtoSpec | ModelSpec] = []
    base_models.extend(
        spec
        for spec in snapshot.dtos
        if spec.name == "BaseDataSourceParamDTO"
        and spec.import_path.startswith(_UPSTREAM_PACKAGE_PREFIX)
    )
    base_models.extend(
        spec
        for spec in snapshot.models
        if spec.name == "BaseDataSourceParamDTO"
        and spec.import_path.startswith(_UPSTREAM_PACKAGE_PREFIX)
    )
    if len(base_models) == 1:
        return tuple(field.wire_name for field in base_models[0].fields)
    if base_models:
        message = (
            f"DS {snapshot.ds_version} exposes ambiguous BaseDataSourceParamDTO "
            f"contracts: {len(base_models)}"
        )
        raise ValueError(message)

    update_operations = [
        operation
        for operation in snapshot.operations
        if operation.controller.endswith("DataSourceController")
        and operation.method_name == "updateDataSource"
    ]
    if len(update_operations) != 1:
        message = f"DS {snapshot.ds_version} has no unambiguous datasource base payload"
        raise ValueError(message)
    fields = tuple(
        parameter.wire_name or parameter.name
        for parameter in update_operations[0].parameters
        if parameter.binding == "request_param"
        and is_client_supplied_parameter(parameter)
    )
    if not fields:
        message = f"DS {snapshot.ds_version} datasource base payload is empty"
        raise ValueError(message)
    return fields


def _validate_plugin_fields(
    version: str,
    *,
    type_names: tuple[str, ...],
    base_field_names: tuple[str, ...],
    plugin_fields_by_type: Mapping[str, tuple[str, ...]],
) -> dict[str, tuple[str, ...]]:
    unknown_types = set(plugin_fields_by_type).difference(type_names)
    if unknown_types:
        message = (
            f"DS {version} datasource plugin reviews reference unknown DbType "
            f"values: {sorted(unknown_types)!r}"
        )
        raise ValueError(message)
    base_fields = set(base_field_names)
    validated: dict[str, tuple[str, ...]] = {}
    for datasource_type in type_names:
        fields = plugin_fields_by_type.get(datasource_type)
        if fields is None:
            continue
        overlap = base_fields.intersection(fields)
        if overlap:
            message = (
                f"DS {version} datasource plugin {datasource_type} repeats base "
                f"fields: {sorted(overlap)!r}"
            )
            raise ValueError(message)
        validated[datasource_type] = fields
    return validated


def _version_key(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in version.split("."))


def _normalize_type_alias(value: str) -> str:
    return "_".join(value.strip().upper().replace("-", " ").split())


__all__ = [
    "DATASOURCE_PROFILE_SCHEMA_VERSION",
    "DEFAULT_DATASOURCE_PROFILE_REVIEWS",
    "GENERATED_DATASOURCE_PROFILE_PATH",
    "DataSourceProfile",
    "compile_datasource_profile",
    "datasource_profile_data",
    "load_datasource_profile_reviews",
    "render_datasource_profiles",
    "write_datasource_profiles",
]
