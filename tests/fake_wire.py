from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, TypeVar

if TYPE_CHECKING:
    from dsctl.upstream.wire import WireRequest


ArgsT = TypeVar("ArgsT")


@dataclass(frozen=True)
class FakePreparedWireCall(Generic[ArgsT]):
    """Prepared-call token carrying fake-owned arguments for service tests."""

    ds_version: str
    program_fingerprint: str
    args: ArgsT
    request: WireRequest
