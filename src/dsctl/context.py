from __future__ import annotations

import os
import tempfile
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, cast

import yaml

from dsctl.errors import ConfigError
from dsctl.support.json_types import JsonObject, JsonValue, is_json_value

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

    def __post_init__(self) -> None:
        """Keep project and workflow selection as one valid scope tuple."""
        for field_name, value in (
            ("project", self.project),
            ("workflow", self.workflow),
        ):
            if value is not None and not value.strip():
                message = f"{field_name} context must not be blank"
                raise ValueError(message)
        if self.workflow is not None and self.project is None:
            message = "workflow context requires project context"
            raise ValueError(message)

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


@dataclass(frozen=True)
class EffectiveContext:
    """Resolved session context and the persisted scope that supplied it."""

    session: SessionContext
    scope: ContextScope | None


def resolve_context(*, cwd: Path | None = None) -> EffectiveContext:
    """Resolve the highest-priority context tuple and its source scope."""
    context_layers: tuple[tuple[ContextScope, Path], ...] = (
        ("project", project_context_path(cwd=cwd)),
        ("user", user_context_path()),
    )
    for scope, path in context_layers:
        layer = _read_context_file(path, scope=scope)
        if "project" in layer:
            return EffectiveContext(
                session=SessionContext(
                    project=layer["project"],
                    workflow=layer.get("workflow"),
                    set_at=layer.get("set_at"),
                ),
                scope=scope,
            )
    return EffectiveContext(session=SessionContext(), scope=None)


def load_context(*, cwd: Path | None = None) -> SessionContext:
    """Load the highest-priority context tuple that selects a project."""
    return resolve_context(cwd=cwd).session


def write_context(
    context: SessionContext,
    *,
    scope: ContextScope = "project",
    cwd: Path | None = None,
) -> Path:
    """Persist a context layer to disk."""
    path = _context_path(scope=scope, cwd=cwd)
    payload = context.to_data()
    document = yaml.safe_dump(payload, sort_keys=False, allow_unicode=False)
    temporary_path: Path | None = None
    write_path = path
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.is_symlink():
            write_path = path.resolve(strict=False)
        write_path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=write_path.parent,
            prefix=f".{write_path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary_file:
            temporary_path = Path(temporary_file.name)
            temporary_file.write(document)
            temporary_file.flush()
            os.fsync(temporary_file.fileno())
        os.replace(temporary_path, write_path)  # noqa: PTH105
    except (OSError, RuntimeError) as exc:
        if temporary_path is not None:
            with suppress(OSError):
                temporary_path.unlink(missing_ok=True)
        message = f"Could not write context file {path}"
        raise ConfigError(
            message,
            details={"operation": "write", "path": str(path)},
            suggestion=(
                f"Make sure {write_path.parent} is a writable directory, then retry "
                "the command."
            ),
        ) from exc
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
    that field from the selected context scope. Updating project clears an
    omitted workflow so the two values cannot cross project boundaries.
    """
    can_discard_unbound_workflow = (
        not isinstance(project, _UnsetContextValue) or workflow is None
    )
    if not can_discard_unbound_workflow:
        current = read_context_layer(scope=scope, cwd=cwd)
    else:
        current_data = _read_context_file(
            _context_path(scope=scope, cwd=cwd),
            scope=scope,
            discard_unbound_workflow=True,
        )
        current = SessionContext(
            project=current_data.get("project"),
            workflow=current_data.get("workflow"),
            set_at=current_data.get("set_at"),
        )
    updated_project = _resolve_context_update(current.project, project)
    updated_workflow = (
        None
        if not isinstance(project, _UnsetContextValue)
        and isinstance(workflow, _UnsetContextValue)
        else _resolve_context_update(current.workflow, workflow)
    )
    updated = SessionContext(
        project=updated_project,
        workflow=updated_workflow,
        set_at=_utc_now(),
    )
    if updated.project is None:
        clear_context(scope=scope, cwd=cwd)
        return SessionContext()
    write_context(updated, scope=scope, cwd=cwd)
    return updated


def clear_context(
    *,
    scope: ContextScope = "project",
    cwd: Path | None = None,
) -> None:
    """Remove a stored context layer if it exists."""
    path = _context_path(scope=scope, cwd=cwd)
    try:
        path.unlink()
    except FileNotFoundError:
        return
    except OSError as exc:
        message = f"Could not clear context file {path}"
        raise ConfigError(
            message,
            details={"operation": "clear", "path": str(path)},
            suggestion=(
                f"Make sure {path} is a removable regular file, then retry the command."
            ),
        ) from exc


def read_context_layer(
    *, scope: ContextScope = "project", cwd: Path | None = None
) -> SessionContext:
    """Read a single context layer without merging it with other scopes."""
    data = _read_context_file(
        _context_path(scope=scope, cwd=cwd),
        scope=scope,
    )
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


def _read_context_file(
    path: Path,
    *,
    scope: ContextScope,
    discard_unbound_workflow: bool = False,
) -> dict[str, str]:
    data = _validated_context_data(
        _load_context_mapping(path),
        path=path,
        scope=scope,
    )
    if "workflow" not in data or "project" in data:
        return data
    if discard_unbound_workflow:
        data.pop("workflow")
        return data

    message = f"Context file {path} workflow requires project in the same layer"
    raise ConfigError(
        message,
        details={
            "path": str(path),
            "scope": scope,
            "key": "workflow",
            "required_key": "project",
        },
        suggestion=(
            f"Run `dsctl use project NAME --scope {scope}` to bind a project "
            "and discard the unbound workflow, or run `dsctl use --clear "
            f"--scope {scope}` to clear that context layer."
        ),
    )


def _load_context_mapping(path: Path) -> JsonObject:
    """Load one context YAML mapping without applying context invariants."""
    try:
        document = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return {}
    except UnicodeError as exc:
        message = f"Context file {path} must be UTF-8"
        raise ConfigError(
            message,
            details={
                "operation": "read",
                "path": str(path),
                "encoding": "utf-8",
            },
            suggestion=f"Rewrite {path} as UTF-8 YAML, then retry the command.",
        ) from exc
    except OSError as exc:
        message = f"Could not read context file {path}"
        raise ConfigError(
            message,
            details={"operation": "read", "path": str(path)},
            suggestion=(
                f"Make sure {path} is a readable regular file, then retry the command."
            ),
        ) from exc

    try:
        loaded = yaml.safe_load(document)
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
    invalid_keys = sorted(str(key) for key in loaded if not isinstance(key, str))
    if invalid_keys:
        message = f"Context file {path} contains unsupported keys"
        raise ConfigError(
            message,
            details={"path": str(path), "keys": invalid_keys},
        )

    data: JsonObject = {}
    for raw_key, raw_value in loaded.items():
        key = cast("str", raw_key)
        if not is_json_value(raw_value):
            message = f"Context value {key!r} in {path} must be a string"
            raise ConfigError(
                message,
                details={
                    "path": str(path),
                    "key": key,
                    "expected_type": "string",
                    "actual_type": type(raw_value).__name__,
                },
            )
        data[key] = raw_value
    return data


def _validated_context_data(
    loaded: JsonObject,
    *,
    path: Path,
    scope: ContextScope,
) -> dict[str, str]:
    """Validate supported context keys and their scalar values."""
    allowed_keys = {"project", "workflow", "set_at"}
    unexpected = sorted(key for key in loaded if key not in allowed_keys)
    if unexpected:
        message = f"Context file {path} contains unsupported keys"
        raise ConfigError(
            message,
            details={"path": str(path), "keys": unexpected},
        )

    data: dict[str, str] = {}
    for key, value in loaded.items():
        validated_value = _validated_context_value(
            value,
            key=key,
            path=path,
            scope=scope,
        )
        if validated_value is not None:
            data[key] = validated_value
    return data


def _validated_context_value(
    value: JsonValue,
    *,
    key: str,
    path: Path,
    scope: ContextScope,
) -> str | None:
    """Validate one optional scalar context value."""
    if value is None:
        return None
    if not isinstance(value, str):
        message = f"Context value {key!r} in {path} must be a string"
        raise ConfigError(
            message,
            details={
                "path": str(path),
                "key": key,
                "expected_type": "string",
                "actual_type": type(value).__name__,
            },
        )
    if key in {"project", "workflow"} and not value.strip():
        message = f"Context value {key!r} in {path} must not be blank"
        raise ConfigError(
            message,
            details={
                "path": str(path),
                "scope": scope,
                "key": key,
            },
            suggestion=(
                f"Run `dsctl use --clear --scope {scope}` to clear the "
                "invalid context layer, then set project context again."
            ),
        )
    return value


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _resolve_context_update(
    current_value: str | None,
    update: ContextUpdateValue,
) -> str | None:
    if isinstance(update, _UnsetContextValue):
        return current_value
    return update
