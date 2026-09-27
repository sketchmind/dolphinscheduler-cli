from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from dsctl.generated.version_profiles import TARGET_DS_VERSIONS

BlockingStandbyTransition = Literal["KILL", "PAUSE"]
BlockingPauseKillBehavior = Literal["task-local-state", "warn-no-op"]


@dataclass(frozen=True, slots=True)
class BlockingAuthoringSurface:
    """BLOCKING availability and master-local lifecycle epochs."""

    available: bool
    standby_transition: BlockingStandbyTransition | None
    pause_kill_behavior: BlockingPauseKillBehavior | None


_BLOCKING_ABSENT = BlockingAuthoringSurface(
    available=False,
    standby_transition=None,
    pause_kill_behavior=None,
)
_BLOCKING_KILL_LOCAL = BlockingAuthoringSurface(
    available=True,
    standby_transition="KILL",
    pause_kill_behavior="task-local-state",
)
_BLOCKING_PAUSE_LOCAL = BlockingAuthoringSurface(
    available=True,
    standby_transition="PAUSE",
    pause_kill_behavior="task-local-state",
)
_BLOCKING_PAUSE_NOOP = BlockingAuthoringSurface(
    available=True,
    standby_transition="PAUSE",
    pause_kill_behavior="warn-no-op",
)


def _blocking_surface(version: str) -> BlockingAuthoringSurface:
    if version in {"3.0.0", "3.0.1", "3.0.2", "3.0.3", "3.0.4", "3.0.5", "3.0.6"}:
        return _BLOCKING_KILL_LOCAL
    if version in {
        "3.1.0",
        "3.1.1",
        "3.1.2",
        "3.1.3",
        "3.1.4",
        "3.1.5",
        "3.1.6",
        "3.1.7",
        "3.1.8",
        "3.1.9",
    }:
        return _BLOCKING_PAUSE_LOCAL
    if version in {"3.2.0", "3.2.1", "3.2.2"}:
        return _BLOCKING_PAUSE_NOOP
    if version in TARGET_DS_VERSIONS:
        return _BLOCKING_ABSENT
    message = f"No exact BLOCKING authoring surface for DolphinScheduler {version}"
    raise ValueError(message)
