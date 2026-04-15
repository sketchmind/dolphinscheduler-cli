from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import yaml

from dsctl.errors import ConfigError

ContextScope = Literal["project", "user"]
PROJECT_CONTEXT_FILENAME = ".dsctl-context.yaml"


class _UnsetContextValue:
    """Sentinel for fields that should keep their current stored value."""


_UNSET = _UnsetContextValue()
ContextUpdateValue = str | None | _UnsetContextValue


@dataclass(frozen=True)
class SessionContext:
    """Persisted session context used by `dsctl use` and command defaults."""

    project: str | None = None
    workflow: str | None = None
    set_at: str | None = None

    def to_data(self) -> dict[str, str]:
        """Serialize the context for YAML storage."""
        return {
            key: value
            for key, value in {
                "project": self.project,
                "workflow": self.workflow,
                "set_at": self.set_at,
            }.items()
            if value is not None
        }


def load_context(*, cwd: Path | None = None) -> SessionContext:
    """Load merged user-level and project-level context state."""
    merged: dict[str, str] = {}
    for path in (user_context_path(), project_context_path(cwd=cwd)):
        merged.update(_read_context_file(path))
    return SessionContext(
        project=merged.get("project"),
        workflow=merged.get("workflow"),
        set_at=merged.get("set_at"),
    )


def write_context(
    context: SessionContext,
    *,
    scope: ContextScope = "project",
    cwd: Path | None = None,
) -> Path:
    """Persist a context layer to disk."""
    path = _context_path(scope=scope, cwd=cwd)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = context.to_data()
    path.write_text(
        yaml.safe_dump(payload, sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )
    return path


def update_context(
    *,
    project: ContextUpdateValue = _UNSET,
    workflow: ContextUpdateValue = _UNSET,
    scope: ContextScope = "project",
    cwd: Path | None = None,
) -> SessionContext:
    """Update a context layer.

    Omitted fields preserve the current stored value. Passing ``None`` clears
    that field from the selected context scope.
    """
    current = read_context_layer(scope=scope, cwd=cwd)
    updated = SessionContext(
        project=_resolve_context_update(current.project, project),
        workflow=_resolve_context_update(current.workflow, workflow),
        set_at=_utc_now(),
    )
    write_context(updated, scope=scope, cwd=cwd)
    return updated


def clear_context(
    *,
    scope: ContextScope = "project",
    cwd: Path | None = None,
) -> None:
    """Remove a stored context layer if it exists."""
    path = _context_path(scope=scope, cwd=cwd)
    if path.exists():
        path.unlink()


def read_context_layer(
    *, scope: ContextScope = "project", cwd: Path | None = None
) -> SessionContext:
    """Read a single context layer without merging it with other scopes."""
    data = _read_context_file(_context_path(scope=scope, cwd=cwd))
    return SessionContext(
        project=data.get("project"),
        workflow=data.get("workflow"),
        set_at=data.get("set_at"),
    )


def user_context_path() -> Path:
    """Return the user-level context file path."""
    config_home = os.environ.get("XDG_CONFIG_HOME")
    base_dir = Path(config_home) if config_home else Path.home() / ".config"
    return base_dir / "dsctl" / "context.yaml"


def project_context_path(*, cwd: Path | None = None) -> Path:
    """Return the project-level context file path."""
    return (cwd or Path.cwd()) / PROJECT_CONTEXT_FILENAME


def _context_path(*, scope: ContextScope, cwd: Path | None = None) -> Path:
    if scope == "user":
        return user_context_path()
    return project_context_path(cwd=cwd)


def _read_context_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}

    try:
        loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        message = f"Invalid YAML in context file {path}"
        raise ConfigError(
            message,
            details={"path": str(path)},
        ) from exc

    if loaded is None:
        return {}
    if not isinstance(loaded, dict):
        message = f"Context file {path} must contain a mapping"
        raise ConfigError(
            message,
            details={"path": str(path)},
        )
    data = {str(key): str(value) for key, value in loaded.items() if value is not None}
    allowed_keys = {"project", "workflow", "set_at"}
    unexpected = sorted(set(data) - allowed_keys)
    if unexpected:
        message = f"Context file {path} contains unsupported keys"
        raise ConfigError(
            message,
            details={"path": str(path), "keys": unexpected},
        )
    return data


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _resolve_context_update(
    current_value: str | None,
    update: ContextUpdateValue,
) -> str | None:
    if isinstance(update, _UnsetContextValue):
        return current_value
    return update
