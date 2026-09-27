from __future__ import annotations

import os
import shlex
import sys
import tempfile
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import TYPE_CHECKING, cast
from urllib.parse import urlsplit, urlunsplit

import yaml

from dsctl.errors import ConfigError

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping
    from typing import BinaryIO

if sys.platform == "win32":
    import msvcrt
else:
    import fcntl


@dataclass(frozen=True)
class NamedContext:
    """A named connection-file reference bound to one target URL."""

    name: str
    env_file: Path
    api_url: str
    project: str | None = None


@dataclass(frozen=True)
class ContextRegistry:
    """User-wide context references and an optional explicit default."""

    contexts: dict[str, NamedContext] = field(default_factory=dict)
    default_context: str | None = None

    def context(self, name: str) -> NamedContext:
        """Resolve one named entry within this registry snapshot."""
        return _get_context(self, name)


def registry_path(*, environment: Mapping[str, str] | None = None) -> Path:
    """Return the sole registry path without searching working directories."""
    env = os.environ if environment is None else environment
    config_home = env.get("XDG_CONFIG_HOME")
    if config_home:
        base = absolute_config_path(config_home, label="configuration directory")
    else:
        try:
            home = Path(env["HOME"]) if env.get("HOME") else Path.home()
        except (OSError, RuntimeError) as exc:
            message = "Could not resolve the user configuration directory"
            raise ConfigError(
                message,
                details={"operation": "resolve", "key": "HOME"},
                suggestion="Set XDG_CONFIG_HOME to an accessible absolute directory.",
            ) from exc
        base = absolute_config_path(home / ".config", label="configuration directory")
    return base / "dsctl" / "config.yaml"


def absolute_config_path(value: str | Path, *, label: str) -> Path:
    """Normalize a local configuration path with stable expansion failures."""
    if "\0" in str(value):
        raise _configuration_path_error(value, label=label)
    try:
        return Path(value).expanduser().absolute()
    except (OSError, RuntimeError, ValueError) as exc:
        raise _configuration_path_error(value, label=label) from exc


def _configuration_path_error(value: str | Path, *, label: str) -> ConfigError:
    return ConfigError(
        f"Could not resolve {label} path {value}",
        details={"operation": "resolve", "path": str(value)},
        suggestion="Use an absolute path or a home-directory reference that exists.",
    )


def normalize_api_url(value: str) -> str:
    """Validate an HTTP API base URL and normalize its target identity."""
    try:
        parsed = urlsplit(value.strip())
        port = parsed.port
        hostname = parsed.hostname
    except ValueError as exc:
        raise _invalid_api_url() from exc
    if (
        parsed.scheme not in {"http", "https"}
        or not hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or any(character.isspace() for character in value.strip())
    ):
        raise _invalid_api_url()
    host = f"[{hostname}]" if ":" in hostname else hostname
    if port is not None and (parsed.scheme, port) not in {("http", 80), ("https", 443)}:
        host = f"{host}:{port}"
    return urlunsplit((parsed.scheme, host, parsed.path.rstrip("/"), "", ""))


def _invalid_api_url() -> ConfigError:
    return ConfigError(
        "DS_API_URL must be an absolute HTTP or HTTPS API URL",
        details={"key": "DS_API_URL"},
        suggestion=(
            "Set DS_API_URL to the API base URL without credentials, query parameters, "
            "or a fragment, for example https://example.test/dolphinscheduler."
        ),
    )


def load_registry(*, path: Path | None = None) -> ContextRegistry:
    """Read the registry atomically; a missing file represents no configuration."""
    selected = registry_path() if path is None else path
    try:
        document = selected.read_text(encoding="utf-8")
    except FileNotFoundError:
        return ContextRegistry()
    except (OSError, UnicodeError) as exc:
        raise _file_error(selected, "read") from exc
    try:
        loaded: object = yaml.load(document, Loader=_RegistryLoader)  # noqa: S506
    except yaml.YAMLError as exc:
        raise _invalid_registry(selected, "must contain valid YAML") from exc
    return _parse_registry(loaded, selected)


def list_contexts() -> tuple[NamedContext, ...]:
    """List stored references without opening their connection files."""
    return tuple(sorted(load_registry().contexts.values(), key=lambda item: item.name))


def get_context(name: str, *, path: Path | None = None) -> NamedContext:
    """Read a named context; a missing name never falls back to the default."""
    return load_registry(path=path).context(name)


def get_default_context() -> str | None:
    """Return the saved default name without resolving its connection file."""
    return load_registry().default_context


def create_context(
    name: str,
    *,
    env_file: str | Path,
    api_url: str,
    project: str | None = None,
) -> NamedContext:
    """Store a validated connection-file reference without copying its token."""
    context = _validated_context(name, env_file, api_url, project)
    path = registry_path()
    with _registry_lock(path):
        current = load_registry(path=path)
        if name in current.contexts:
            message = f"Context {name!r} already exists"
            raise ConfigError(
                message,
                details={"context_name": name},
                suggestion=(
                    f"Use `dsctl context update {shlex.quote(name)}` to edit it."
                ),
            )
        _write_registry(
            replace(current, contexts={**current.contexts, name: context}), path
        )
    return context


def update_context(
    name: str,
    *,
    env_file: str | Path | None = None,
    api_url: str | None = None,
    project: str | None = None,
    clear_project: bool = False,
) -> NamedContext:
    """Update a reference, clearing its previous project whenever a file is supplied."""
    if clear_project and project is not None:
        message = "project and clear_project are mutually exclusive"
        raise ConfigError(
            message, suggestion="Use either --project or --clear-project."
        )
    if (env_file is None) != (api_url is None):
        message = "Updating a context file requires its validated API URL"
        raise ConfigError(message)
    path = registry_path()
    with _registry_lock(path):
        current = load_registry(path=path)
        previous = _get_context(current, name)
        selected_project = previous.project
        if env_file is not None or clear_project:
            selected_project = None
        if project is not None:
            selected_project = project
        context = _validated_context(
            name,
            previous.env_file if env_file is None else env_file,
            previous.api_url if api_url is None else api_url,
            selected_project,
        )
        _write_registry(
            replace(current, contexts={**current.contexts, name: context}), path
        )
    return context


def delete_context(name: str) -> NamedContext:
    """Delete one reference after its default selection has been removed."""
    path = registry_path()
    with _registry_lock(path):
        current = load_registry(path=path)
        context = _get_context(current, name)
        if current.default_context == name:
            message = f"Context {name!r} is the saved default"
            raise ConfigError(
                message,
                details={"context_name": name, "reason": "default_context_in_use"},
                suggestion=(
                    "Run `dsctl config unset default-context` before deleting "
                    "this context."
                ),
            )
        contexts = dict(current.contexts)
        del contexts[name]
        _write_registry(replace(current, contexts=contexts), path)
    return context


def set_default_context(name: str) -> ContextRegistry:
    """Select an existing reference as the user-wide default."""
    path = registry_path()
    with _registry_lock(path):
        current = load_registry(path=path)
        _get_context(current, name)
        updated = replace(current, default_context=name)
        _write_registry(updated, path)
    return updated


def unset_default_context() -> ContextRegistry:
    """Remove the saved default while retaining all named references."""
    path = registry_path()
    with _registry_lock(path):
        updated = replace(load_registry(path=path), default_context=None)
        _write_registry(updated, path)
    return updated


def _validated_context(
    name: str, env_file: str | Path, api_url: str, project: str | None
) -> NamedContext:
    _required_string(name, "context name")
    if project is not None:
        _required_string(project, "project")
    _required_string(str(env_file), "env_file")
    return NamedContext(
        name=name,
        env_file=absolute_config_path(env_file, label="connection file"),
        api_url=normalize_api_url(api_url),
        project=project,
    )


def _get_context(registry: ContextRegistry, name: str) -> NamedContext:
    _required_string(name, "context name")
    try:
        return registry.contexts[name]
    except KeyError as exc:
        message = f"Context {name!r} was not found"
        raise ConfigError(
            message,
            details={"context_name": name},
            suggestion="Run `dsctl context list` to inspect registered names.",
        ) from exc


def _parse_registry(loaded: object, path: Path) -> ContextRegistry:
    if loaded is None:
        return ContextRegistry()
    data = _mapping(loaded, path, "registry")
    _check_keys(data, {"contexts", "default_context"}, path)
    context_data = _mapping(data.get("contexts", {}), path, "contexts")
    contexts: dict[str, NamedContext] = {}
    for name, raw in context_data.items():
        _required_string(name, "context name")
        entry = _mapping(raw, path, f"context {name!r}")
        _check_keys(entry, {"env_file", "api_url", "project"}, path)
        env_file = _required_string(entry.get("env_file"), "env_file")
        api_url = _required_string(entry.get("api_url"), "api_url")
        raw_project = entry.get("project")
        project = (
            None if raw_project is None else _required_string(raw_project, "project")
        )
        if not Path(env_file).is_absolute():
            raise _invalid_registry(path, "context env_file must be an absolute path")
        contexts[name] = _validated_context(name, env_file, api_url, project)
    raw_default = data.get("default_context")
    default = (
        None
        if raw_default is None
        else _required_string(raw_default, "default_context")
    )
    if default is not None and default not in contexts:
        raise _invalid_registry(path, "default_context must name an existing context")
    return ContextRegistry(contexts=contexts, default_context=default)


def _mapping(value: object, path: Path, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise _invalid_registry(path, f"{label} must be a mapping")
    mapping = cast("dict[object, object]", value)
    if any(not isinstance(key, str) for key in mapping):
        raise _invalid_registry(path, f"{label} keys must be strings")
    return cast("dict[str, object]", mapping)


def _check_keys(data: dict[str, object], allowed: set[str], path: Path) -> None:
    if set(data) - allowed:
        raise _invalid_registry(path, "contains unsupported keys")


def _required_string(value: object, key: str) -> str:
    if not isinstance(value, str) or not value.strip():
        message = f"{key} must be a nonblank string"
        raise ConfigError(message, details={"key": key})
    return value


def _invalid_registry(path: Path, reason: str) -> ConfigError:
    return ConfigError(
        f"Context registry {path} {reason}",
        details={"path": str(path)},
        suggestion=f"Repair the named context registry at {path}, then retry.",
    )


class _RegistryLoader(yaml.SafeLoader):
    """Reject duplicate keys instead of silently discarding registry data."""

    def construct_mapping(
        self,
        node: yaml.MappingNode,
        deep: bool = False,  # noqa: FBT001, FBT002
    ) -> dict[object, object]:
        """Validate mapping keys before SafeLoader constructs their values."""
        keys: set[str] = set()
        for key_node, _ in node.value:
            key: object = self.construct_object(key_node, deep=deep)
            if not isinstance(key, str) or key in keys:
                message = "Registry mapping keys must be unique strings"
                raise yaml.YAMLError(message)
            keys.add(key)
        return cast("dict[object, object]", super().construct_mapping(node, deep=deep))


@contextmanager
def _registry_lock(path: Path) -> Iterator[None]:
    """Serialize the entire read/modify/write operation across processes."""
    try:
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        path.parent.chmod(0o700)
        lock_path = path.with_suffix(".lock")
        with lock_path.open("a+b") as lock_file:
            lock_path.chmod(0o600)
            _lock_file(lock_file)
            try:
                yield
            finally:
                _unlock_file(lock_file)
    except OSError as exc:
        raise _file_error(path, "write") from exc


def _lock_file(lock_file: BinaryIO) -> None:
    if sys.platform == "win32":
        lock_file.seek(0, os.SEEK_END)
        if lock_file.tell() == 0:
            lock_file.write(b"\0")
            lock_file.flush()
        lock_file.seek(0)
        msvcrt.locking(lock_file.fileno(), msvcrt.LK_LOCK, 1)
    else:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)


def _unlock_file(lock_file: BinaryIO) -> None:
    if sys.platform == "win32":
        lock_file.seek(0)
        msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
    else:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _write_registry(registry: ContextRegistry, path: Path) -> None:
    payload = {
        "contexts": {
            name: {
                "env_file": str(context.env_file),
                "api_url": context.api_url,
                **({"project": context.project} if context.project is not None else {}),
            }
            for name, context in registry.contexts.items()
        },
        "default_context": registry.default_context,
    }
    document = yaml.safe_dump(payload, sort_keys=False, allow_unicode=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary_file:
            temporary_path = Path(temporary_file.name)
            temporary_file.write(document)
            temporary_file.flush()
            os.fsync(temporary_file.fileno())
        temporary_path.replace(path)
    except OSError as exc:
        if temporary_path is not None:
            with suppress(OSError):
                temporary_path.unlink(missing_ok=True)
        raise _file_error(path, "write") from exc


def _file_error(path: Path, operation: str) -> ConfigError:
    return ConfigError(
        f"Could not {operation} context registry {path}",
        details={"operation": operation, "path": str(path)},
        suggestion=f"Make sure {path} and its directory are accessible, then retry.",
    )
