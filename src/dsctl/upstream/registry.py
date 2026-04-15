from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, TypedDict, cast

from dsctl.errors import ConfigError
from dsctl.upstream.adapters.ds_3_4_1 import DS341Adapter
from dsctl.versioning import DEFAULT_DS_VERSION, normalize_version

if TYPE_CHECKING:
    from dsctl.upstream.protocol import UpstreamAdapter

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
    adapter: UpstreamAdapter[object]
    contract_version: str
    family: str
    support_level: SupportLevel
    tested: bool

    def as_dict(self) -> VersionSupportData:
        """Return a JSON-serializable representation for discovery commands."""
        return {
            "server_version": self.server_version,
            "contract_version": self.contract_version,
            "family": self.family,
            "support_level": self.support_level,
            "tested": self.tested,
        }


_DS_3_4_1 = DS341Adapter()

_SUPPORT_BY_VERSION: dict[str, VersionSupport] = {
    "3.3.2": VersionSupport(
        server_version="3.3.2",
        adapter=cast("UpstreamAdapter[object]", _DS_3_4_1),
        contract_version="3.4.1",
        family="workflow-3.3-plus",
        support_level="full",
        tested=False,
    ),
    "3.4.0": VersionSupport(
        server_version="3.4.0",
        adapter=cast("UpstreamAdapter[object]", _DS_3_4_1),
        contract_version="3.4.1",
        family="workflow-3.3-plus",
        support_level="full",
        tested=False,
    ),
    "3.4.1": VersionSupport(
        server_version="3.4.1",
        adapter=cast("UpstreamAdapter[object]", _DS_3_4_1),
        contract_version="3.4.1",
        family="workflow-3.3-plus",
        support_level="full",
        tested=True,
    ),
}

SUPPORTED_VERSIONS = tuple(sorted(_SUPPORT_BY_VERSION))


def get_adapter(version: str) -> UpstreamAdapter[object]:
    """Return the adapter for a supported DolphinScheduler version."""
    return get_version_support(version).adapter


def get_default_version_support() -> VersionSupport:
    """Return support metadata for the default target DS version."""
    return get_version_support(DEFAULT_DS_VERSION)


def get_version_support(version: str) -> VersionSupport:
    """Return support metadata for a supported DolphinScheduler version."""
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
    "get_adapter",
    "get_default_version_support",
    "get_version_support",
    "normalize_version",
    "supported_version_metadata",
]
