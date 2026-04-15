from __future__ import annotations

from typing import TYPE_CHECKING, TypedDict

from dsctl.config import load_selected_ds_version
from dsctl.errors import UserInputError
from dsctl.output import CommandResult, require_json_object
from dsctl.upstream import (
    SUPPORTED_VERSIONS,
    get_enum_spec,
    get_version_support,
    supported_enum_names,
)

if TYPE_CHECKING:
    from dsctl.upstream.enums import EnumAttributeValue, EnumMemberSpec, EnumSpec


class EnumMemberData(TypedDict):
    """One enum member emitted by `dsctl enum list`."""

    name: str
    value: str | int
    attributes: dict[str, EnumAttributeValue]


class EnumData(TypedDict):
    """Stable enum discovery payload."""

    name: str
    module: str
    class_name: str
    ds_version: str
    value_type: str
    member_count: int
    members: list[EnumMemberData]


class ResolvedEnumData(TypedDict):
    """Resolved enum selector metadata."""

    requested: str
    name: str
    ds_version: str


def list_enum_result(enum_name: str, *, env_file: str | None = None) -> CommandResult:
    """Return one generated enum and its members."""
    requested_name = enum_name.strip()
    support = get_version_support(load_selected_ds_version(env_file))
    spec = get_enum_spec(support.contract_version, requested_name)
    if spec is None:
        supported = list(supported_enum_names(support.contract_version))
        message = f"Unsupported enum {enum_name!r}"
        raise UserInputError(
            message,
            details={
                "enum": enum_name,
                "supported_enums": supported,
            },
            suggestion=(
                "Run `capabilities` and inspect `data.enums.names` to choose "
                "a supported enum name."
            ),
        )

    return CommandResult(
        data=require_json_object(
            _enum_data(spec, ds_version=support.server_version),
            label="enum data",
        ),
        resolved={
            "enum": require_json_object(
                ResolvedEnumData(
                    requested=enum_name,
                    name=spec.name,
                    ds_version=support.server_version,
                ),
                label="resolved enum",
            )
        },
    )


def supported_enum_choices(*, ds_version: str | None = None) -> tuple[str, ...]:
    """Return the stable enum names exposed to schema and capabilities."""
    catalog_version = (
        SUPPORTED_VERSIONS[-1]
        if ds_version is None
        else get_version_support(ds_version).contract_version
    )
    return supported_enum_names(catalog_version)


def enum_capabilities_data(*, ds_version: str | None = None) -> dict[str, object]:
    """Return machine-readable enum discovery metadata."""
    return {
        "discovery": True,
        "names": list(supported_enum_choices(ds_version=ds_version)),
    }


def _enum_data(spec: EnumSpec, *, ds_version: str) -> EnumData:
    return {
        "name": spec.name,
        "module": spec.module,
        "class_name": spec.class_name,
        "ds_version": ds_version,
        "value_type": spec.value_type,
        "member_count": len(spec.members),
        "members": [_enum_member_data(member) for member in spec.members],
    }


def _enum_member_data(member: EnumMemberSpec) -> EnumMemberData:
    return {
        "name": member.name,
        "value": member.value,
        "attributes": dict(member.attributes),
    }


__all__ = ["enum_capabilities_data", "list_enum_result", "supported_enum_choices"]
