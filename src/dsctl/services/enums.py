from __future__ import annotations

from typing import TYPE_CHECKING, TypedDict

from dsctl.errors import ConfigError, UserInputError
from dsctl.output import (
    CommandResult,
    JsonObject,
    require_json_object,
    require_json_value,
)
from dsctl.services._discovery_commands import render_discovery_command
from dsctl.services.version_resolution import (
    CompatibilityResolution,
    compatibility_details,
    resolve_target,
)
from dsctl.upstream import (
    get_default_version_support,
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


class EnumNameData(TypedDict):
    """One supported enum discovery name."""

    name: str
    list_command: str


class ResolvedEnumData(TypedDict):
    """Resolved enum selector metadata."""

    requested: str
    name: str
    ds_version: str


def list_enum_names_result(*, env_file: str | None = None) -> CommandResult:
    """Return supported generated enum discovery names."""
    resolution = resolve_target(env_file, mode="local")
    if isinstance(resolution, CompatibilityResolution):
        return _compatible_enum_names_result(resolution, env_file=env_file)
    support = get_version_support(resolution.version)
    names = supported_enum_names(support.server_version)
    rows = [
        EnumNameData(
            name=name,
            list_command=render_discovery_command(
                "enum.list", values={"enum": name}, env_file=env_file
            ),
        )
        for name in names
    ]
    return CommandResult(
        data=require_json_value(rows, label="enum names data"),
        resolved={
            "enum": require_json_object(
                {
                    "ds_version": support.server_version,
                    "count": len(rows),
                },
                label="resolved enum names",
            )
        },
    )


def list_enum_result(enum_name: str, *, env_file: str | None = None) -> CommandResult:
    """Return one generated enum and its members."""
    requested_name = enum_name.strip()
    resolution = resolve_target(env_file, mode="local")
    if isinstance(resolution, CompatibilityResolution):
        return _compatible_enum_result(enum_name, resolution)
    support = get_version_support(resolution.version)
    spec = get_enum_spec(support.server_version, requested_name)
    if spec is None:
        supported = list(supported_enum_names(support.server_version))
        message = f"Unsupported enum {enum_name!r}"
        raise UserInputError(
            message,
            details={
                "enum": enum_name,
                "supported_enums": supported,
            },
            suggestion="Run `dsctl enum names` to choose a supported enum name.",
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
    selected_version = (
        get_default_version_support().server_version
        if ds_version is None
        else get_version_support(ds_version).server_version
    )
    return supported_enum_names(selected_version)


def supported_enum_member_values(
    enum_name: str,
    *,
    ds_version: str,
) -> tuple[str, ...]:
    """Return exact-profile string values for one required generated enum."""
    selected_version = get_version_support(ds_version).server_version
    spec = get_enum_spec(selected_version, enum_name)
    if spec is None:
        message = f"Generated enum {enum_name!r} is absent from DS {selected_version}"
        raise UserInputError(
            message,
            details={
                "enum": enum_name,
                "ds_version": selected_version,
                "reason": "upstream_capability_absent",
            },
            suggestion="Run `dsctl enum names` for this selected DS version.",
        )
    return tuple(str(member.value) for member in spec.members)


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


__all__ = [
    "candidate_enum_contract_metadata",
    "enum_capabilities_data",
    "list_enum_names_result",
    "list_enum_result",
    "supported_enum_choices",
    "supported_enum_member_values",
]


def compatible_enum_names(candidate_versions: tuple[str, ...]) -> tuple[str, ...]:
    """List only enums with identical complete member contracts in every candidate."""
    if not candidate_versions:
        return ()
    return tuple(
        name
        for name in supported_enum_names(candidate_versions[0])
        if _compatible_enum_specs(candidate_versions, name) is not None
    )


def _compatible_enum_specs(
    candidate_versions: tuple[str, ...], name: str
) -> tuple[EnumSpec, ...] | None:
    specs = tuple(get_enum_spec(version, name) for version in candidate_versions)
    if not specs or any(spec is None for spec in specs):
        return None
    present = tuple(spec for spec in specs if spec is not None)
    first = present[0]
    if any(
        (spec.name, spec.class_name, spec.value_type, spec.members)
        != (first.name, first.class_name, first.value_type, first.members)
        for spec in present[1:]
    ):
        return None
    return present


def candidate_enum_contract_metadata() -> JsonObject:
    """Distinguish generated candidate members from observed server membership."""
    return {
        "membership": "identical_across_candidates",
        "contract_scope": "candidate_generated_contracts",
        "server_membership": "unverified",
        "note": (
            "Values describe identical generated candidate contracts; "
            "observed server values may differ."
        ),
    }


def _compatible_enum_names_result(
    resolution: CompatibilityResolution, *, env_file: str | None = None
) -> CommandResult:
    names = compatible_enum_names(resolution.candidate_versions)
    return CommandResult(
        data=[
            {
                "name": name,
                "list_command": render_discovery_command(
                    "enum.list", values={"enum": name}, env_file=env_file
                ),
            }
            for name in names
        ],
        resolved={
            "enum": {
                **compatibility_details(resolution),
                "count": len(names),
                **candidate_enum_contract_metadata(),
            }
        },
    )


def _compatible_enum_result(
    enum_name: str, resolution: CompatibilityResolution
) -> CommandResult:
    specs = _compatible_enum_specs(resolution.candidate_versions, enum_name.strip())
    if specs is None:
        if resolution.candidate_versions and all(
            get_enum_spec(version, enum_name.strip()) is None
            for version in resolution.candidate_versions
        ):
            message = f"Unsupported enum {enum_name!r}"
            raise UserInputError(
                message,
                details={
                    "enum": enum_name,
                    "supported_enums": list(
                        compatible_enum_names(resolution.candidate_versions)
                    ),
                },
                suggestion="Run `dsctl enum names` to choose a supported enum name.",
            )
        message = f"Enum {enum_name!r} has no identical contract across all candidates"
        raise ConfigError(
            message,
            details={
                **compatibility_details(resolution),
                "enum": enum_name,
                "reason": "exact_version_required",
            },
            suggestion=(
                "Set DS_VERSION to the deployment's actual release for this enum."
            ),
        )
    spec = specs[0]
    data: JsonObject = {
        "name": spec.name,
        "module": None,
        "class_name": spec.class_name,
        "value_type": spec.value_type,
        "member_count": len(spec.members),
        "members": [
            require_json_object(_enum_member_data(member), label="enum member data")
            for member in spec.members
        ],
        "ds_version": None,
        "candidate_versions": list(resolution.candidate_versions),
        **candidate_enum_contract_metadata(),
        "source_modules": {
            version: candidate.module
            for version, candidate in zip(
                resolution.candidate_versions, specs, strict=True
            )
        },
    }
    return CommandResult(
        data=require_json_object(data, label="compatible enum data"),
        resolved={
            "enum": {
                **compatibility_details(resolution),
                "requested": enum_name,
                "name": spec.name,
                **candidate_enum_contract_metadata(),
            }
        },
    )
