from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from functools import cache
from importlib import import_module
from pkgutil import iter_modules
from typing import TYPE_CHECKING, TypeGuard

from dsctl.errors import ConfigError
from dsctl.upstream.registry import normalize_version

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping
    from types import ModuleType


EnumScalar = str | int
EnumAttributeValue = str | int | float | bool | None

_ENUM_PACKAGE_ROOTS_BY_VERSION: dict[str, tuple[str, ...]] = {
    "3.4.1": (
        "dsctl.generated.versions.ds_3_4_1.api.enums",
        "dsctl.generated.versions.ds_3_4_1.common.enums",
        "dsctl.generated.versions.ds_3_4_1.plugin.task_api.enums",
        "dsctl.generated.versions.ds_3_4_1.registry.api.enums",
        "dsctl.generated.versions.ds_3_4_1.spi.enums",
    )
}


@dataclass(frozen=True)
class EnumMemberSpec:
    """One generated enum member exposed through CLI enum discovery."""

    name: str
    value: EnumScalar
    attributes: Mapping[str, EnumAttributeValue]


@dataclass(frozen=True)
class EnumSpec:
    """Stable upstream enum descriptor resolved from generated code."""

    name: str
    module: str
    class_name: str
    value_type: str
    members: tuple[EnumMemberSpec, ...]


@dataclass(frozen=True)
class _EnumCatalog:
    names: tuple[str, ...]
    specs_by_name: Mapping[str, EnumSpec]
    canonical_names_by_alias: Mapping[str, str]


def supported_enum_names(version: str) -> tuple[str, ...]:
    """Return the stable CLI enum names available for one DS version."""
    return _catalog_for_version(version).names


def get_enum_spec(version: str, enum_name: str) -> EnumSpec | None:
    """Resolve one enum by canonical name or supported alias."""
    catalog = _catalog_for_version(version)
    canonical_name = catalog.canonical_names_by_alias.get(
        _normalize_enum_name(enum_name)
    )
    if canonical_name is None:
        return None
    return catalog.specs_by_name.get(canonical_name)


@cache
def _catalog_for_version(version: str) -> _EnumCatalog:
    normalized_version = normalize_version(version)
    package_roots = _ENUM_PACKAGE_ROOTS_BY_VERSION.get(normalized_version)
    if package_roots is None:
        supported_versions = ", ".join(sorted(_ENUM_PACKAGE_ROOTS_BY_VERSION))
        message = f"Unsupported DS version {version!r}"
        raise ConfigError(
            message,
            details={
                "version": version,
                "supported_versions": supported_versions,
            },
        )

    specs = sorted(
        (
            _spec_from_module(module_name)
            for module_name in _enum_module_names(package_roots)
        ),
        key=lambda spec: spec.name,
    )
    specs_by_name = {spec.name: spec for spec in specs}
    canonical_names_by_alias: dict[str, str] = {}
    for spec in specs:
        for alias in _enum_aliases(spec):
            canonical_names_by_alias.setdefault(alias, spec.name)
    return _EnumCatalog(
        names=tuple(specs_by_name),
        specs_by_name=specs_by_name,
        canonical_names_by_alias=canonical_names_by_alias,
    )


def _enum_module_names(package_roots: tuple[str, ...]) -> Iterator[str]:
    for package_root in package_roots:
        package = import_module(package_root)
        for module_info in iter_modules(package.__path__, package.__name__ + "."):
            if module_info.ispkg:
                continue
            yield module_info.name


def _spec_from_module(module_name: str) -> EnumSpec:
    module = import_module(module_name)
    enum_type = _enum_type_from_module(module)
    canonical_name = module_name.rsplit(".", 1)[-1].replace("_", "-")
    return EnumSpec(
        name=canonical_name,
        module=_relative_generated_module_name(module_name),
        class_name=enum_type.__name__,
        value_type=_enum_value_type(enum_type),
        members=tuple(_member_spec(member) for member in enum_type),
    )


def _enum_type_from_module(module: ModuleType) -> type[Enum]:
    export_names = getattr(module, "__all__", ())
    for export_name in export_names:
        candidate = getattr(module, export_name, None)
        if _is_generated_enum_type(candidate, module_name=module.__name__):
            return candidate

    for candidate in module.__dict__.values():
        if _is_generated_enum_type(candidate, module_name=module.__name__):
            return candidate

    message = f"Module {module.__name__!r} did not expose a generated enum type"
    raise ConfigError(message)


def _is_generated_enum_type(
    candidate: object,
    *,
    module_name: str,
) -> TypeGuard[type[Enum]]:
    return (
        isinstance(candidate, type)
        and issubclass(candidate, Enum)
        and candidate.__module__ == module_name
    )


def _enum_value_type(enum_type: type[Enum]) -> str:
    members = tuple(enum_type)
    if not members:
        return "unknown"
    first_value = members[0].value
    if isinstance(first_value, bool):
        return "boolean"
    if isinstance(first_value, int):
        return "integer"
    if isinstance(first_value, float):
        return "number"
    if isinstance(first_value, str):
        return "string"
    return "unknown"


def _member_spec(member: Enum) -> EnumMemberSpec:
    value = member.value
    if not isinstance(value, (str, int)):
        message = (
            "Enum member "
            f"{member.__class__.__name__}.{member.name} used unsupported value type"
        )
        raise ConfigError(message)
    return EnumMemberSpec(
        name=member.name,
        value=value,
        attributes=dict(_member_attributes(member)),
    )


def _member_attributes(
    member: Enum,
) -> Iterator[tuple[str, EnumAttributeValue]]:
    attributes = member.__dict__
    for key in sorted(attributes):
        if key.startswith("_"):
            continue
        value = attributes[key]
        if isinstance(value, (str, int, float, bool)) or value is None:
            yield key, value


def _relative_generated_module_name(module_name: str) -> str:
    prefix = "dsctl.generated.versions.ds_3_4_1."
    return module_name.removeprefix(prefix)


def _enum_aliases(spec: EnumSpec) -> Iterator[str]:
    yield _normalize_enum_name(spec.name)
    module_basename = spec.module.rsplit(".", 1)[-1]
    yield _normalize_enum_name(module_basename)
    yield _normalize_enum_name(spec.class_name)
    yield _normalize_enum_name(spec.class_name.lower())
    yield _normalize_enum_name(_camel_to_snake(spec.class_name))
    yield _normalize_enum_name(_camel_to_kebab(spec.class_name))


def _normalize_enum_name(value: str) -> str:
    return value.strip().lower().replace("_", "-")


def _camel_to_snake(value: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", value).lower()


def _camel_to_kebab(value: str) -> str:
    return _camel_to_snake(value).replace("_", "-")


__all__ = ["EnumMemberSpec", "EnumSpec", "get_enum_spec", "supported_enum_names"]
