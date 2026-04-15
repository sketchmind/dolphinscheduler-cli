from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING, Concatenate, ParamSpec, Protocol, TypeVar

from dsctl.client import DolphinSchedulerClient
from dsctl.config import ClusterProfile, load_profile
from dsctl.context import SessionContext, load_context
from dsctl.services._serialization import optional_text
from dsctl.upstream import get_adapter

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping
    from pathlib import Path

    from dsctl.upstream.protocol import UpstreamSession, UserRecord


P = ParamSpec("P")
ResultT = TypeVar("ResultT")


class HealthcheckClient(Protocol):
    """Minimal client interface needed by service-level health commands."""

    def healthcheck(self) -> Mapping[str, object]:
        """Return the API server health payload."""


@dataclass(frozen=True)
class ServiceRuntime:
    """Shared service-layer runtime for one CLI command execution."""

    profile: ClusterProfile
    context: SessionContext
    http_client: HealthcheckClient
    upstream: UpstreamSession

    @cached_property
    def current_user_defaults(self) -> CurrentUserDefaults:
        """Return defaults derived from the current authenticated DS user."""
        return _current_user_defaults(self.upstream.users.current())


@dataclass(frozen=True)
class CurrentUserDefaults:
    """User-scoped defaults resolved from DolphinScheduler itself."""

    user_name: str | None
    tenant_code: str | None
    queue: str | None
    queue_name: str | None
    time_zone: str | None


@contextmanager
def open_service_runtime(
    *,
    env_file: str | None = None,
    cwd: Path | None = None,
) -> Iterator[ServiceRuntime]:
    """Open one profile-bound service runtime with a shared HTTP client."""
    profile = load_profile(env_file)
    session_context = load_context(cwd=cwd)
    adapter = get_adapter(profile.ds_version)
    with DolphinSchedulerClient(profile) as http_client:
        yield ServiceRuntime(
            profile=profile,
            context=session_context,
            http_client=http_client,
            upstream=adapter.bind(profile, http_client=http_client),
        )


def run_with_service_runtime(
    env_file: str | None,
    operation: Callable[Concatenate[ServiceRuntime, P], ResultT],
    /,
    *args: P.args,
    **kwargs: P.kwargs,
) -> ResultT:
    """Open one runtime, invoke one service operation, and return its result."""
    with open_service_runtime(env_file=env_file) as runtime:
        return operation(runtime, *args, **kwargs)


def _current_user_defaults(user: UserRecord) -> CurrentUserDefaults:
    return CurrentUserDefaults(
        user_name=optional_text(user.userName),
        tenant_code=optional_text(user.tenantCode),
        queue=optional_text(user.queue),
        queue_name=optional_text(user.queueName),
        time_zone=optional_text(user.timeZone),
    )
