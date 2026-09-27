"""Shared runtime injection for bound-domain command and service tests."""

from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar

from dsctl.services.runtime import BoundDomainServiceRuntime
from dsctl.services.selection import ResourceDefaults
from tests.fakes import FakeHttpClient

if TYPE_CHECKING:
    from collections.abc import Callable

    import pytest

    from dsctl.config import ClusterProfile
    from dsctl.output import CommandResult
    from dsctl.upstream.bound_domain import BoundDomain


DomainT = TypeVar("DomainT")


def patch_bound_domain_service_runtime(
    monkeypatch: pytest.MonkeyPatch,
    service_module: object,
    *,
    expected_domain: BoundDomain[DomainT],
    runtime_domain: DomainT,
    profile_factory: Callable[[], ClusterProfile],
) -> None:
    """Route one service module through an exact in-memory bound domain."""

    def run_with_fakes(
        env_file: str | None,
        requested_domain: object,
        operation: Callable[..., CommandResult],
        /,
        *args: object,
        **kwargs: object,
    ) -> CommandResult:
        del env_file
        assert requested_domain is expected_domain
        runtime = BoundDomainServiceRuntime(
            profile=profile_factory(),
            context=ResourceDefaults(),
            http_client=FakeHttpClient(),
            domain=runtime_domain,
        )
        return operation(runtime, *args, **kwargs)

    monkeypatch.setattr(
        service_module,
        "run_with_bound_domain_service_runtime",
        run_with_fakes,
    )
