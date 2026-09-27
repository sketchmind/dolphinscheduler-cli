from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, Protocol, TypeVar

if TYPE_CHECKING:
    from collections.abc import Callable

    from dsctl.client import DolphinSchedulerClient
    from dsctl.config import ClusterProfile


DomainT = TypeVar("DomainT")
DomainT_co = TypeVar("DomainT_co", covariant=True)


class BoundDomainAdapter(Protocol[DomainT_co]):
    """Bind one selected exact-version adapter to a caller-owned domain."""

    def bind(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> DomainT_co:
        """Return the domain surface backed by this exact profile."""


@dataclass(frozen=True)
class BoundDomain(Generic[DomainT]):
    """Late-bound domain recipe independent of the central version registry."""

    name: str
    adapter_for_version: Callable[[str], BoundDomainAdapter[DomainT]]

    def bind(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> DomainT:
        """Resolve and bind only the adapter required by the caller."""
        return self.adapter_for_version(profile.ds_version).bind(
            profile,
            http_client=http_client,
        )


__all__ = ["BoundDomain", "BoundDomainAdapter"]
