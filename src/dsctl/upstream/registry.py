from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from functools import cache, cached_property
from typing import TYPE_CHECKING, Literal, TypedDict, cast

from dsctl.errors import ConfigError
from dsctl.generated.version_profiles import TARGET_DS_VERSIONS, VERSION_PROFILES
from dsctl.upstream.capability_catalog import (
    ActionCapability,
    Availability,
    CapabilityCatalog,
    Verification,
)
from dsctl.versioning import DEFAULT_DS_VERSION, normalize_version

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonValue
    from dsctl.upstream.protocol import (
        IdentityUpstreamAdapter,
        ReadUpstreamAdapter,
        TaskDefinitionUpstreamAdapter,
    )

SupportLevel = Literal["full", "legacy_core", "experimental"]


class VersionSupportData(TypedDict):
    """JSON-serializable support metadata for discovery commands."""

    server_version: str
    contract_version: str
    family: str
    support_level: SupportLevel
    tested: bool


@dataclass(frozen=True)
class VersionSupport:
    """Support metadata for one selectable DolphinScheduler server version."""

    server_version: str
    contract_version: str
    family: str
    support_level: SupportLevel
    tested: bool
    catalog: CapabilityCatalog

    def __post_init__(self) -> None:
        """Reject stable support claims that lack live-test evidence."""
        if self.catalog.server_version != self.server_version:
            message = "capability catalog version must match server_version"
            raise ValueError(message)
        if self.support_level in {"full", "legacy_core"} and not self.tested:
            message = f"support level {self.support_level!r} requires tested=True"
            raise ValueError(message)

    @cached_property
    def read_adapter(self) -> ReadUpstreamAdapter | None:
        """Load this profile's exact read adapter only when execution needs it."""
        return _read_adapter_or_none(self.server_version)

    @cached_property
    def identity_adapter(self) -> IdentityUpstreamAdapter | None:
        """Load this profile's exact identity adapter only when requested."""
        return _identity_adapter_or_none(self.server_version)

    @cached_property
    def task_definition_adapter(self) -> TaskDefinitionUpstreamAdapter | None:
        """Load this profile's exact task adapter only when requested."""
        return _task_definition_adapter_or_none(self.server_version)

    def as_dict(self) -> VersionSupportData:
        """Return a JSON-serializable representation for discovery commands."""
        return {
            "server_version": self.server_version,
            "contract_version": self.contract_version,
            "family": self.family,
            "support_level": self.support_level,
            "tested": self.tested,
        }


_UNIVERSAL_LOCAL_ACTIONS = frozenset(
    {
        "capabilities",
        "context",
        "schema",
        "template.datasource",  # The service honors its explicit --ds-version.
        "context.list",
        "context.get",
        "context.create",
        "context.update",
        "context.delete",
        "config.get",
        "config.set",
        "config.unset",
        "version",
    }
)
_DIAGNOSTIC_ACTIONS = frozenset({"doctor"})
_PREFLIGHT_EXEMPT_ACTIONS = _UNIVERSAL_LOCAL_ACTIONS | _DIAGNOSTIC_ACTIONS
_READ_ACTIONS = frozenset(
    {"project.get", "project.list", "workflow.get", "workflow.list"}
)


def _mapping(value: JsonValue, *, label: str) -> Mapping[str, JsonValue]:
    if not isinstance(value, Mapping):
        message = f"generated version profile {label} must be an object"
        raise TypeError(message)
    if not all(isinstance(key, str) for key in value):
        message = f"generated version profile {label} keys must be text"
        raise TypeError(message)
    return value


def _text(value: JsonValue, *, label: str) -> str:
    if not isinstance(value, str) or not value:
        message = f"generated version profile {label} must be non-empty text"
        raise TypeError(message)
    return value


def _boolean(value: JsonValue, *, label: str) -> bool:
    if not isinstance(value, bool):
        message = f"generated version profile {label} must be a boolean"
        raise TypeError(message)
    return value


def _catalog_from_profile(
    version: str,
    profile: Mapping[str, JsonValue],
) -> CapabilityCatalog:
    raw_actions = _mapping(profile.get("actions"), label=f"{version}.actions")
    entries: dict[str, ActionCapability] = {}
    for action, raw_capability in raw_actions.items():
        capability = _mapping(
            raw_capability,
            label=f"{version}.actions.{action}",
        )
        constraint_value = capability.get("constraint")
        if constraint_value is not None and not isinstance(constraint_value, str):
            message = (
                f"generated version profile {version}.actions.{action}.constraint "
                "must be text"
            )
            raise TypeError(message)
        entries[action] = ActionCapability(
            availability=Availability(
                _text(
                    capability.get("availability"),
                    label=f"{version}.actions.{action}.availability",
                )
            ),
            verification=Verification(
                _text(
                    capability.get("verification"),
                    label=f"{version}.actions.{action}.verification",
                )
            ),
            constraint=constraint_value,
        )
    return CapabilityCatalog(server_version=version, entries=entries)


def _execution_mode_for_supported_actions(
    *,
    version: str,
    profile: Mapping[str, JsonValue],
    catalog: CapabilityCatalog,
    actions: frozenset[str],
) -> str | None:
    raw_actions = _mapping(profile.get("actions"), label=f"{version}.actions")
    modes = {
        _text(
            _mapping(
                raw_actions[action],
                label=f"{version}.actions.{action}",
            ).get("execution_mode"),
            label=f"{version}.actions.{action}.execution_mode",
        )
        for action in actions
        if catalog.entries[action].availability is Availability.SUPPORTED
    }
    if not modes:
        return None
    if len(modes) != 1:
        message = (
            f"DS {version} uses multiple execution modes for one adapter closure: "
            f"{', '.join(sorted(modes))}"
        )
        raise ValueError(message)
    return next(iter(modes))


def _read_adapter_from_profile(
    *,
    version: str,
    profile: Mapping[str, JsonValue],
    catalog: CapabilityCatalog,
) -> ReadUpstreamAdapter | None:
    mode = _execution_mode_for_supported_actions(
        version=version,
        profile=profile,
        catalog=catalog,
        actions=_READ_ACTIONS,
    )
    if mode is None:
        return None
    if mode == "generated_adapter":
        if version == "1.3.9":
            from dsctl.upstream.id_native_reads import (  # noqa: PLC0415 - keep exact packages lazy
                IdNativeReadAdapter,
            )

            return IdNativeReadAdapter()
        from dsctl.upstream.code_native_reads import (  # noqa: PLC0415 - keep exact packages lazy
            CodeNativeReadAdapter,
        )

        return CodeNativeReadAdapter.for_version(version)
    message = f"DS {version} read execution mode {mode!r} has no adapter binding"
    raise ValueError(message)


def _support_from_profile(version: str) -> VersionSupport:
    profile = _mapping(VERSION_PROFILES[version], label=version)
    server_version = _text(
        profile.get("server_version"),
        label=f"{version}.server_version",
    )
    if server_version != version:
        message = (
            f"generated version profile key {version!r} does not match "
            f"server_version {server_version!r}"
        )
        raise ValueError(message)
    support_level_value = _text(
        profile.get("support_level"),
        label=f"{version}.support_level",
    )
    if support_level_value not in {"full", "legacy_core", "experimental"}:
        message = f"unsupported generated support level {support_level_value!r}"
        raise ValueError(message)
    support_level = cast("SupportLevel", support_level_value)
    catalog = _catalog_from_profile(version, profile)
    return VersionSupport(
        server_version=version,
        contract_version=_text(
            profile.get("contract_version"),
            label=f"{version}.contract_version",
        ),
        family=_text(profile.get("family"), label=f"{version}.family"),
        support_level=support_level,
        tested=_boolean(profile.get("tested"), label=f"{version}.tested"),
        catalog=catalog,
    )


SUPPORTED_VERSIONS = tuple(TARGET_DS_VERSIONS)
_SUPPORT_BY_VERSION = {
    version: _support_from_profile(version) for version in SUPPORTED_VERSIONS
}


@cache
def _read_adapter_or_none(
    version: str,
) -> ReadUpstreamAdapter | None:
    support = _SUPPORT_BY_VERSION[version]
    profile = _mapping(VERSION_PROFILES[version], label=version)
    return _read_adapter_from_profile(
        version=version,
        profile=profile,
        catalog=support.catalog,
    )


@cache
def _identity_adapter_or_none(version: str) -> IdentityUpstreamAdapter | None:
    if version not in TARGET_DS_VERSIONS:
        return None
    from dsctl.upstream.identity import (  # noqa: PLC0415 - keep profiles lazy
        IdentityAdapter,
    )

    return cast(
        "IdentityUpstreamAdapter",
        IdentityAdapter.for_version(version),
    )


@cache
def _task_definition_adapter_or_none(
    version: str,
) -> TaskDefinitionUpstreamAdapter | None:
    if version == "1.3.9" or version not in TARGET_DS_VERSIONS:
        return None
    from dsctl.upstream.task_definition_wire import (  # noqa: PLC0415 - keep exact packages lazy
        TaskDefinitionAdapter,
    )

    return cast(
        "TaskDefinitionUpstreamAdapter",
        TaskDefinitionAdapter.for_version(version),
    )


def get_read_adapter(version: str) -> ReadUpstreamAdapter:
    """Return the exact-version adapter for supported project/workflow reads."""
    support = get_version_support(version)
    if support.read_adapter is not None:
        return support.read_adapter
    message = f"DS {support.server_version} has no project/workflow read adapter"
    raise ConfigError(
        message,
        details={
            "version": support.server_version,
            "capability_discovery": "dsctl capabilities --action project.list",
        },
        suggestion=(
            "Run `dsctl capabilities --action project.list` to inspect the "
            "selected profile's support."
        ),
    )


def get_identity_adapter(version: str) -> IdentityUpstreamAdapter:
    """Return the exact-version authenticated-user adapter when available."""
    support = get_version_support(version)
    if support.identity_adapter is not None:
        return support.identity_adapter
    message = f"DS {support.server_version} has no current-user adapter"
    raise ConfigError(message, details={"version": support.server_version})


def get_task_definition_adapter(version: str) -> TaskDefinitionUpstreamAdapter:
    """Return the exact-version task-definition adapter when available."""
    support = get_version_support(version)
    if support.task_definition_adapter is not None:
        return support.task_definition_adapter
    message = f"DS {support.server_version} has no task-definition adapter"
    raise ConfigError(
        message,
        details={
            "version": support.server_version,
            "capability_discovery": "dsctl capabilities --action task.get",
        },
        suggestion=(
            "Run `dsctl capabilities --action task.get` to inspect the selected "
            "profile's support."
        ),
    )


def get_default_version_support() -> VersionSupport:
    """Return support metadata for the default target DS version."""
    return get_version_support(DEFAULT_DS_VERSION)


def get_action_capability(version: str, action: str) -> ActionCapability:
    """Return one exact-version capability fact without executing it."""
    return get_version_support(version).catalog.entries[action]


def is_action_preflight_exempt(action: str) -> bool:
    """Return whether a local or diagnostic builder owns version validation."""
    return action in _PREFLIGHT_EXEMPT_ACTIONS


def preflight_action(
    version: str,
    action: str,
) -> ActionCapability:
    """Reject an unsupported exact-version intent before transport."""
    return get_version_support(version).catalog.preflight(action)


def get_version_support(version: str) -> VersionSupport:
    """Return support metadata for a selectable DolphinScheduler version."""
    normalized = normalize_version(version)
    support = _SUPPORT_BY_VERSION.get(normalized)
    if support is not None:
        return support

    supported = ", ".join(SUPPORTED_VERSIONS)
    message = f"Unsupported DS version {version!r}"
    raise ConfigError(
        message,
        details={"version": version, "supported_versions": supported},
    )


def supported_version_metadata() -> tuple[VersionSupportData, ...]:
    """Return all selectable DS versions and their support metadata."""
    return tuple(
        _SUPPORT_BY_VERSION[version].as_dict() for version in SUPPORTED_VERSIONS
    )


__all__ = [
    "SUPPORTED_VERSIONS",
    "VersionSupport",
    "VersionSupportData",
    "get_action_capability",
    "get_default_version_support",
    "get_identity_adapter",
    "get_read_adapter",
    "get_task_definition_adapter",
    "get_version_support",
    "is_action_preflight_exempt",
    "normalize_version",
    "preflight_action",
    "supported_version_metadata",
]


@cache
def is_local_action(action: str) -> bool:
    """Derive connection needs from compiled execution plans across exact profiles."""
    modes: set[bool] = set()
    for version in SUPPORTED_VERSIONS:
        profile = _mapping(VERSION_PROFILES[version], label=version)
        actions = _mapping(profile.get("actions"), label=f"{version}.actions")
        entry = _mapping(actions.get(action), label=f"{version}.actions.{action}")
        if entry.get("availability") == "supported":
            modes.add(entry.get("execution_mode") == "local")
    if len(modes) != 1:
        message = f"Action {action!r} does not have a consistent connection policy"
        raise ValueError(message)
    return modes.pop()
