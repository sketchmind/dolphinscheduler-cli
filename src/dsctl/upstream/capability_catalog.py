from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import TYPE_CHECKING

from dsctl.cli_surface import stable_leaf_actions
from dsctl.errors import UnsupportedFeatureError

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dsctl.support.json_types import JsonObject, JsonValue

_MAX_SERVER_VERSION_LENGTH = 64
_MAX_CONSTRAINT_LENGTH = 256


class Availability(StrEnum):
    """Stable availability states for one CLI action."""

    SUPPORTED = "supported"
    LIMITED = "limited"
    UNSUPPORTED = "unsupported"


class Verification(StrEnum):
    """Strongest evidence currently backing one capability claim."""

    STATIC = "static"
    CONTRACT_TESTED = "contract_tested"
    LIVE_SMOKE = "live_smoke"
    LIVE_FULL = "live_full"


@dataclass(frozen=True)
class ActionCapability:
    """One exact-version capability claim for a stable CLI action."""

    availability: Availability
    verification: Verification
    constraint: str | None = None

    def __post_init__(self) -> None:
        """Keep capability claims internally consistent and output-bounded."""
        _require_availability(self.availability)
        _require_verification(self.verification)
        _validate_constraint(self.constraint)
        if self.availability is not Availability.SUPPORTED and self.constraint is None:
            message = (
                f"{self.availability.value} capability requires an explicit constraint"
            )
            raise ValueError(message)
        if self.availability is Availability.SUPPORTED and self.constraint is not None:
            message = "supported capability cannot define a constraint"
            raise ValueError(message)


class CapabilityCatalog:
    """Complete exact-version capability facts for the stable CLI surface."""

    def __init__(
        self,
        *,
        server_version: str,
        entries: Mapping[str, ActionCapability],
    ) -> None:
        """Validate and freeze one complete exact-version capability map."""
        _validate_server_version(server_version)
        stable_actions = stable_leaf_actions()
        provided_actions = frozenset(entries)
        missing_actions = sorted(stable_actions - provided_actions)
        extra_actions = sorted(provided_actions - stable_actions)
        if missing_actions or extra_actions:
            parts = []
            if missing_actions:
                parts.append(f"missing actions: {', '.join(missing_actions)}")
            if extra_actions:
                parts.append(f"unknown actions: {', '.join(extra_actions)}")
            raise ValueError("; ".join(parts))
        invalid_entry = next(
            (
                action
                for action, capability in entries.items()
                if not isinstance(capability, ActionCapability)
            ),
            None,
        )
        if invalid_entry is not None:
            message = f"capability for {invalid_entry} must be an ActionCapability"
            raise TypeError(message)
        self._server_version = server_version
        self._entries = MappingProxyType(dict(entries))

    @property
    def server_version(self) -> str:
        """Return the exact DolphinScheduler server version."""
        return self._server_version

    @property
    def entries(self) -> Mapping[str, ActionCapability]:
        """Return immutable action capability facts."""
        return self._entries

    def summary_metadata(self) -> JsonObject:
        """Return bounded exact-version counts without expanding every action."""
        availability_counts = {availability.value: 0 for availability in Availability}
        verification_counts = {verification.value: 0 for verification in Verification}
        for capability in self.entries.values():
            availability_counts[capability.availability.value] += 1
            verification_counts[capability.verification.value] += 1
        return {
            "selected_version": self.server_version,
            "action_count": len(self.entries),
            "availability_counts": availability_counts,
            "verification_counts": verification_counts,
        }

    def action_metadata(self, action: str) -> JsonObject:
        """Return one bounded machine-readable action capability."""
        capability = self.entries[action]
        metadata: JsonObject = {
            "action": action,
            "availability": capability.availability.value,
            "verification": capability.verification.value,
        }
        if capability.constraint is not None:
            metadata["constraint"] = capability.constraint
        return metadata

    def available_actions(self) -> frozenset[str]:
        """Return actions that may be attempted for this exact version."""
        return frozenset(
            action
            for action, capability in self.entries.items()
            if capability.availability is Availability.SUPPORTED
        )

    def preflight(self, action: str) -> ActionCapability:
        """Require one fully supported stable action before transport."""
        capability = self._entries[action]
        if capability.availability is not Availability.SUPPORTED:
            action_details: dict[str, JsonValue] = {
                "action": action,
                "selected_version": self.server_version,
                "availability": capability.availability.value,
            }
            if capability.constraint is not None:
                action_details["constraint"] = capability.constraint
            message = (
                f"{action} is {capability.availability.value} on "
                f"DolphinScheduler {self.server_version}."
            )
            raise UnsupportedFeatureError(
                message,
                details=action_details,
                suggestion=(
                    f"Run `dsctl capabilities --action {action}` to inspect the "
                    "constraint. This operation requires a server version that "
                    "supports it; the selected profile must match that server."
                ),
            )
        return capability


def _require_availability(value: Availability | None) -> None:
    if not isinstance(value, Availability):
        message = "availability must be an Availability value"
        raise TypeError(message)


def _require_verification(value: Verification | None) -> None:
    if not isinstance(value, Verification):
        message = "verification must be a Verification value"
        raise TypeError(message)


def _validate_constraint(value: str | bytes | None) -> None:
    if value is None:
        return
    if not isinstance(value, str):
        message = "constraint must be text"
        raise TypeError(message)
    if not value.strip():
        message = "constraint must not be blank"
        raise ValueError(message)
    if len(value) > _MAX_CONSTRAINT_LENGTH:
        message = f"constraint must be at most {_MAX_CONSTRAINT_LENGTH} characters"
        raise ValueError(message)


def _validate_server_version(value: str | None) -> None:
    if not isinstance(value, str):
        message = "server_version must be text"
        raise TypeError(message)
    if not value.strip():
        message = "server_version must not be blank"
        raise ValueError(message)
    if len(value) > _MAX_SERVER_VERSION_LENGTH:
        message = (
            f"server_version must be at most {_MAX_SERVER_VERSION_LENGTH} characters"
        )
        raise ValueError(message)
