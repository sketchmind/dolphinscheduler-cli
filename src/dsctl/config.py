from __future__ import annotations

import math
import os
import shlex
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, ConfigDict, field_validator

from dsctl.context import (
    absolute_config_path,
    get_context,
    load_registry,
    normalize_api_url,
    registry_path,
)
from dsctl.errors import ConfigError
from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.versioning import DEFAULT_DS_VERSION, normalize_version

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping
    from pathlib import Path

    from dsctl.context import NamedContext

ConfigurationSource = Literal[
    "flag", "environment_selector", "environment", "default", "unconfigured"
]

_SUPPORTED_PROFILE_KEYS = (
    "DS_VERSION",
    "DS_API_URL",
    "DS_API_TOKEN",
    "DS_API_RETRY_ATTEMPTS",
    "DS_API_RETRY_BACKOFF_MS",
    "DS_API_TIMEOUT_SECONDS",
)
_TARGET_PROFILE_KEYS = ("DS_VERSION", "DS_API_URL", "DS_API_TOKEN")
_EXECUTION_POLICY_KEYS = (
    "DS_API_RETRY_ATTEMPTS",
    "DS_API_RETRY_BACKOFF_MS",
    "DS_API_TIMEOUT_SECONDS",
)


class ConnectionSettings(BaseModel):
    """Connection inputs that do not presume a server version."""

    model_config = ConfigDict(frozen=True)

    api_url: str
    api_token: str
    api_retry_attempts: int = 3
    api_retry_backoff_ms: int = 200
    api_timeout_seconds: float = 10.0

    @field_validator("api_url")
    @classmethod
    def _normalize_api_url(cls, value: str) -> str:
        return normalize_api_url(value)

    @field_validator("api_timeout_seconds")
    @classmethod
    def _validate_api_timeout_seconds(cls, value: float) -> float:
        if not math.isfinite(value) or value <= 0:
            message = "api_timeout_seconds must be a finite number greater than 0"
            raise ValueError(message)
        return value

    @property
    def health_url(self) -> str:
        """Return the standard actuator health endpoint for the configured API."""
        return f"{self.api_url.rstrip('/')}/actuator/health"


class ClusterProfile(ConnectionSettings):
    """Immutable runtime configuration with a resolved exact DS version."""

    ds_version: str = DEFAULT_DS_VERSION

    @field_validator("ds_version")
    @classmethod
    def _normalize_ds_version(cls, value: str) -> str:
        return normalize_version(value)

    def redacted(self) -> dict[str, str | int | float]:
        """Return a display-safe view of the profile with secrets masked."""
        return {
            "api_url": self.api_url,
            "api_token": _mask_secret(self.api_token),
            "ds_version": self.ds_version,
            "api_retry_attempts": self.api_retry_attempts,
            "api_retry_backoff_ms": self.api_retry_backoff_ms,
            "api_timeout_seconds": self.api_timeout_seconds,
            "health_url": self.health_url,
        }


@dataclass(frozen=True)
class TargetSettings:
    """One atomic configuration source, before remote version resolution."""

    requested_version: str | None = None
    api_url: str | None = None
    api_token: str | None = field(default=None, repr=False)
    api_retry_attempts: int = 3
    api_retry_backoff_ms: int = 200
    api_timeout_seconds: float = 10.0
    env_file: str | Path | None = None
    context_name: str | None = None
    project: str | None = None
    source: ConfigurationSource = "unconfigured"
    configured_keys: tuple[str, ...] = ()

    def connection(self) -> ConnectionSettings:
        """Require connection fields only when a command needs the target."""
        values = {
            "DS_API_URL": self.api_url or "",
            "DS_API_TOKEN": self.api_token or "",
        }
        try:
            return ConnectionSettings(
                api_url=_require(values, "DS_API_URL", env_file=self.env_file),
                api_token=_require(values, "DS_API_TOKEN", env_file=self.env_file),
                api_retry_attempts=self.api_retry_attempts,
                api_retry_backoff_ms=self.api_retry_backoff_ms,
                api_timeout_seconds=self.api_timeout_seconds,
            )
        except ConfigError as exc:
            details = {
                **exc.details,
                "reason": "incomplete_selected_source",
                "source": self.source,
                "context_name": self.context_name,
                "configured_keys": list(self.configured_keys),
            }
            suggestion = exc.suggestion
            if self.source == "environment":
                key = exc.details.get("key")
                suggestion = (
                    f"Set {key} in the selected process environment, or unset all "
                    "DS connection/version settings to use the saved default context."
                )
            raise ConfigError(str(exc), details=details, suggestion=suggestion) from exc


@dataclass
class _ConfigurationScope:
    environment: dict[str, str]
    context_name: str | None
    env_file: str | Path | None
    targets: dict[tuple[str | None, str | None], TargetSettings | ConfigError] = field(
        default_factory=dict
    )


_CONFIGURATION_SCOPE: ContextVar[_ConfigurationScope | None] = ContextVar(
    "dsctl_configuration_scope", default=None
)


@contextmanager
def configuration_scope(
    *, context_name: str | None = None, env_file: str | Path | None = None
) -> Iterator[None]:
    """Freeze source inputs and cache each selected target for one invocation."""
    if _CONFIGURATION_SCOPE.get() is not None:
        yield
        return
    token = _CONFIGURATION_SCOPE.set(
        _ConfigurationScope(
            environment=dict(os.environ), context_name=context_name, env_file=env_file
        )
    )
    try:
        yield
    finally:
        _CONFIGURATION_SCOPE.reset(token)


def load_target_settings(
    env_file: str | Path | None = None, *, context_name: str | None = None
) -> TargetSettings:
    """Select one complete source before reading or validating any lower source."""
    scope = _CONFIGURATION_SCOPE.get()
    selected_context = (
        context_name
        if context_name is not None or scope is None
        else scope.context_name
    )
    selected_file = (
        env_file if env_file is not None or scope is None else scope.env_file
    )
    key = (None if selected_file is None else str(selected_file), selected_context)
    if scope is not None and key in scope.targets:
        cached = scope.targets[key]
        if isinstance(cached, ConfigError):
            raise cached
        return cached
    environment = dict(os.environ) if scope is None else scope.environment
    try:
        settings = _select_target(selected_file, selected_context, environment)
    except ConfigError as exc:
        if scope is not None:
            scope.targets[key] = exc
        raise
    if scope is not None:
        scope.targets[key] = settings
    return settings


def validate_context_file(path: str | Path) -> ConnectionSettings:
    """Validate a complete file locally, independently of ambient selectors."""
    selected = absolute_config_path(path, label="selected env file")
    settings = _settings_from_values(
        _load_profile_values(selected), env_file=selected, source="flag"
    )
    requested = settings.requested_version
    if (
        requested is not None
        and requested != "auto"
        and requested not in TARGET_DS_VERSIONS
    ):
        message = f"Unsupported DS version {requested!r}"
        raise ConfigError(
            message,
            details={
                "key": "DS_VERSION",
                "supported_versions": list(TARGET_DS_VERSIONS),
            },
            suggestion="Set DS_VERSION to a supported exact release or auto.",
        )
    return settings.connection()


def _select_target(
    env_file: str | Path | None,
    context_name: str | None,
    environment: Mapping[str, str],
) -> TargetSettings:
    selected: TargetSettings
    if env_file is not None or context_name is not None:
        _validate_selectors(env_file, context_name, source="flag")
        selected = _selected_target(env_file, context_name, "flag", environment)
    else:
        environment_file = environment.get("DSCTL_ENV_FILE")
        environment_context = environment.get("DSCTL_CONTEXT")
        if environment_file is not None or environment_context is not None:
            _validate_selectors(
                environment_file, environment_context, source="environment_selector"
            )
            selected = _selected_target(
                environment_file,
                environment_context,
                "environment_selector",
                environment,
            )
        elif any(key in environment for key in _TARGET_PROFILE_KEYS):
            selected = _settings_from_values(
                {
                    key: environment[key]
                    for key in _SUPPORTED_PROFILE_KEYS
                    if key in environment
                },
                source="environment",
            )
        else:
            registry = load_registry(path=registry_path(environment=environment))
            selected = (
                _context_target(
                    registry.contexts[registry.default_context],
                    "default",
                    policy_overrides=environment,
                )
                if registry.default_context is not None
                else _settings_from_values(
                    {}, source="unconfigured", policy_overrides=environment
                )
            )
    return selected


def _validate_selectors(
    env_file: str | Path | None,
    context_name: str | None,
    *,
    source: ConfigurationSource,
) -> None:
    if env_file is not None and context_name is not None:
        message = "Context and env-file selectors are mutually exclusive"
        suggestion = (
            "Pass either --context or --env-file."
            if source == "flag"
            else "Set either DSCTL_CONTEXT or DSCTL_ENV_FILE, and unset the other."
        )
        raise ConfigError(message, details={"source": source}, suggestion=suggestion)
    selected = str(env_file) if env_file is not None else context_name
    if selected is None or not selected.strip():
        message = "The selected context or env-file must not be blank"
        raise ConfigError(
            message,
            details={"source": source},
            suggestion="Provide a context name or file path, or unset the selector.",
        )


def _selected_target(
    env_file: str | Path | None,
    context_name: str | None,
    source: ConfigurationSource,
    environment: Mapping[str, str],
) -> TargetSettings:
    if context_name is not None:
        context = get_context(context_name, path=registry_path(environment=environment))
        return _context_target(context, source, policy_overrides=environment)
    if env_file is None:
        message = "A target selector is required"
        raise ConfigError(message)
    selected = absolute_config_path(env_file, label="selected env file")
    return _settings_from_values(
        _load_profile_values(selected),
        env_file=selected,
        source=source,
        policy_overrides=environment,
    )


def _context_target(
    context: NamedContext,
    source: ConfigurationSource,
    *,
    policy_overrides: Mapping[str, str] | None = None,
) -> TargetSettings:
    settings = _settings_from_values(
        _load_profile_values(context.env_file),
        env_file=context.env_file,
        context_name=context.name,
        project=context.project,
        source=source,
        policy_overrides=policy_overrides,
    )
    connection = settings.connection()
    if connection.api_url != context.api_url:
        command = shlex.join(
            [
                "dsctl",
                "context",
                "update",
                context.name,
                "--file",
                str(context.env_file),
            ]
        )
        message = (
            f"Context {context.name!r} connection file now targets a different API URL"
        )
        raise ConfigError(
            message,
            details={
                "context_name": context.name,
                "reason": "context_api_url_changed",
                "expected_api_url": context.api_url,
                "actual_api_url": connection.api_url,
            },
            suggestion=(
                f"Restore DS_API_URL in the file, or run `{command}` to rebind the "
                "context. Rebinding clears its project unless --project is supplied."
            ),
        )
    return settings


def _settings_from_values(
    values: dict[str, str],
    *,
    env_file: Path | None = None,
    context_name: str | None = None,
    project: str | None = None,
    source: ConfigurationSource,
    policy_overrides: Mapping[str, str] | None = None,
) -> TargetSettings:
    effective_values = dict(values)
    if policy_overrides is not None:
        effective_values.update(
            {
                key: policy_overrides[key]
                for key in _EXECUTION_POLICY_KEYS
                if key in policy_overrides
            }
        )
    requested = values.get("DS_VERSION", "").strip()
    api_url = values.get("DS_API_URL")
    return TargetSettings(
        requested_version=normalize_version(requested) if requested else None,
        api_url=normalize_api_url(api_url) if api_url and api_url.strip() else None,
        api_token=values.get("DS_API_TOKEN") or None,
        api_retry_attempts=_int_with_default(
            effective_values.get("DS_API_RETRY_ATTEMPTS"),
            "DS_API_RETRY_ATTEMPTS",
            default=3,
            minimum=1,
        ),
        api_retry_backoff_ms=_int_with_default(
            effective_values.get("DS_API_RETRY_BACKOFF_MS"),
            "DS_API_RETRY_BACKOFF_MS",
            default=200,
            minimum=0,
        ),
        api_timeout_seconds=_float_with_default(
            effective_values.get("DS_API_TIMEOUT_SECONDS"),
            "DS_API_TIMEOUT_SECONDS",
            default=10.0,
        ),
        env_file=env_file,
        context_name=context_name,
        project=project,
        source=source,
        configured_keys=tuple(key for key in _SUPPORTED_PROFILE_KEYS if key in values),
    )


def load_profile(env_file: str | Path | None = None) -> ClusterProfile:
    """Load an explicitly versioned profile without remote discovery.

    Runtime callers use services.version_resolution.resolve_profile for automatic
    selection. This pure loader never guesses a configured server's version.
    """
    settings = load_target_settings(env_file)
    connection = settings.connection()
    return ClusterProfile(
        **connection.model_dump(), ds_version=_local_version(settings)
    )


def load_selected_ds_version(env_file: str | Path | None = None) -> str:
    """Read an explicit selection or the untargeted offline authoring baseline."""
    return _local_version(load_target_settings(env_file))


def _local_version(settings: TargetSettings) -> str:
    requested = settings.requested_version
    if requested is not None and requested != "auto":
        return requested
    if requested is None and settings.api_url is None and settings.api_token is None:
        return DEFAULT_DS_VERSION
    message = "The configured target requires DS version discovery"
    raise ConfigError(
        message,
        details={"reason": "version_discovery_required"},
        suggestion=(
            "Run `dsctl doctor` with the selected profile to discover the server "
            "version, or set DS_VERSION to its exact DolphinScheduler version."
        ),
    )


def _load_profile_values(env_file: Path) -> dict[str, str]:
    values = _read_env_file(env_file)
    _reject_unknown_profile_keys(values)
    return values


def _reject_unknown_profile_keys(values: dict[str, str]) -> None:
    unknown_keys = sorted(key for key in values if key not in _SUPPORTED_PROFILE_KEYS)
    if not unknown_keys:
        return
    message = "Unsupported DS profile settings"
    raise ConfigError(
        message,
        details={"keys": unknown_keys, "supported_keys": list(_SUPPORTED_PROFILE_KEYS)},
        suggestion=(
            "Cluster profile only accepts DS connection and version settings. "
            "Remove those keys from the profile. Use "
            "`dsctl context update NAME --project PROJECT` for "
            "local project selection and `dsctl project-preference update` for "
            "project-level runtime defaults."
        ),
    )


def _read_env_file(path: Path) -> dict[str, str]:
    data: dict[str, str] = {}
    try:
        document = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        message = f"Could not read selected env file {path}"
        raise ConfigError(
            message,
            details={"operation": "read", "path": str(path)},
            suggestion=f"Make sure {path} is a readable UTF-8 dotenv file, then retry.",
        ) from exc
    for line_number, raw_line in enumerate(document.splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            message = (
                f"Invalid setting in selected env file {path} at line {line_number}"
            )
            raise ConfigError(
                message,
                details={"path": str(path), "line": line_number},
                suggestion=(
                    "Use one KEY=VALUE setting per line, or begin comments with #."
                ),
            )
        key, value = line.split("=", 1)
        data[key.strip()] = _strip_optional_quotes(value.strip())
    return data


def _require(
    values: dict[str, str],
    key: str,
    *,
    env_file: str | Path | None,
) -> str:
    value = values.get(key)
    if value is None or not value.strip():
        message = f"Missing required setting: {key}"
        raise ConfigError(
            message,
            details={"key": key},
            suggestion=_missing_setting_suggestion(key, env_file=env_file),
        )
    return value


def _int_with_default(
    value: str | None, key: str, *, default: int, minimum: int
) -> int:
    if value is None or value == "":
        return default
    parsed = _parse_int(value, key)
    if parsed < minimum:
        message = f"Setting {key} must be greater than or equal to {minimum}"
        raise ConfigError(
            message,
            details={"key": key, "value": value, "minimum": minimum},
            suggestion=_minimum_setting_suggestion(key, minimum=minimum),
        )
    return parsed


def _parse_int(value: str, key: str) -> int:
    try:
        return int(value)
    except ValueError as exc:
        message = f"Setting {key} must be an integer"
        raise ConfigError(
            message,
            details={"key": key, "value": value},
            suggestion=_integer_setting_suggestion(key),
        ) from exc


def _float_with_default(value: str | None, key: str, *, default: float) -> float:
    if value is None or value == "":
        return default
    try:
        parsed = float(value)
    except ValueError as exc:
        message = f"Setting {key} must be a number"
        raise ConfigError(
            message,
            details={"key": key, "value": value},
            suggestion=f"Set {key} to a finite number greater than 0.",
        ) from exc
    if not math.isfinite(parsed) or parsed <= 0:
        message = f"Setting {key} must be a finite number greater than 0"
        raise ConfigError(
            message,
            details={"key": key, "value": value, "minimum_exclusive": 0},
            suggestion=f"Set {key} to a finite number greater than 0.",
        )
    return parsed


def _mask_secret(value: str) -> str:
    if value == "":
        return value
    if len(value) <= 8:
        return "*" * len(value)
    return f"{value[:4]}...{value[-4:]}"


def _strip_optional_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def _missing_setting_suggestion(
    key: str,
    *,
    env_file: str | Path | None,
) -> str:
    if env_file is not None:
        return f"Add {key} to the selected env file."
    return f"Set {key} in the environment or provide it through --env-file."


def _integer_setting_suggestion(key: str) -> str:
    return f"Set {key} to a valid integer in the environment or env file."


def _minimum_setting_suggestion(key: str, *, minimum: int) -> str:
    return f"Set {key} to an integer greater than or equal to {minimum}."
