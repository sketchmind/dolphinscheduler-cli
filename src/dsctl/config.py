from __future__ import annotations

import os
from pathlib import Path

from pydantic import BaseModel, ConfigDict, field_validator

from dsctl.errors import ConfigError
from dsctl.versioning import DEFAULT_DS_VERSION, normalize_version

_SUPPORTED_PROFILE_KEYS = (
    "DS_VERSION",
    "DS_API_URL",
    "DS_API_TOKEN",
    "DS_API_RETRY_ATTEMPTS",
    "DS_API_RETRY_BACKOFF_MS",
)


class ClusterProfile(BaseModel):
    """Immutable runtime configuration for a DolphinScheduler cluster."""

    model_config = ConfigDict(frozen=True)

    api_url: str
    api_token: str
    ds_version: str = DEFAULT_DS_VERSION
    api_retry_attempts: int = 3
    api_retry_backoff_ms: int = 200

    @field_validator("api_url")
    @classmethod
    def _normalize_api_url(cls, value: str) -> str:
        return value.rstrip("/")

    @field_validator("ds_version")
    @classmethod
    def _normalize_ds_version(cls, value: str) -> str:
        return normalize_version(value)

    @property
    def health_url(self) -> str:
        """Return the standard actuator health endpoint for the configured API."""
        return f"{self.api_url.rstrip('/')}/actuator/health"

    def redacted(self) -> dict[str, str | int]:
        """Return a display-safe view of the profile with secrets masked."""
        return {
            "api_url": self.api_url,
            "api_token": _mask_secret(self.api_token),
            "ds_version": self.ds_version,
            "api_retry_attempts": self.api_retry_attempts,
            "api_retry_backoff_ms": self.api_retry_backoff_ms,
            "health_url": self.health_url,
        }


def load_profile(env_file: str | Path | None = None) -> ClusterProfile:
    """Load a cluster profile from an optional env file plus process env."""
    values: dict[str, str] = {}
    if env_file is not None:
        values.update(_read_env_file(Path(env_file)))
    values.update(_read_profile_env())
    _reject_unknown_profile_keys(values)

    return ClusterProfile(
        api_url=_require(values, "DS_API_URL"),
        api_token=_require(values, "DS_API_TOKEN"),
        ds_version=_string_with_default(
            values.get("DS_VERSION"),
            default=DEFAULT_DS_VERSION,
        ),
        api_retry_attempts=_int_with_default(
            values.get("DS_API_RETRY_ATTEMPTS"),
            "DS_API_RETRY_ATTEMPTS",
            default=3,
            minimum=1,
        ),
        api_retry_backoff_ms=_int_with_default(
            values.get("DS_API_RETRY_BACKOFF_MS"),
            "DS_API_RETRY_BACKOFF_MS",
            default=200,
            minimum=0,
        ),
    )


def load_selected_ds_version(env_file: str | Path | None = None) -> str:
    """Load only the target DS version from profile inputs.

    This is intentionally lighter than ``load_profile`` so static discovery
    commands such as ``version`` and ``capabilities`` can honor ``DS_VERSION``
    without requiring a live cluster URL and token.
    """
    values: dict[str, str] = {}
    if env_file is not None:
        values.update(_read_env_file(Path(env_file)))
    values.update(_read_profile_env())
    _reject_unknown_profile_keys(values)
    return normalize_version(
        _string_with_default(values.get("DS_VERSION"), default=DEFAULT_DS_VERSION)
    )


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
            "Remove those keys from the profile. Use `dsctl use project` for "
            "local project selection and `dsctl project-preference update` for "
            "project-level runtime defaults."
        ),
    )


def _read_env_file(path: Path) -> dict[str, str]:
    data: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        data[key.strip()] = _strip_optional_quotes(value.strip())
    return data


def _read_profile_env() -> dict[str, str]:
    data: dict[str, str] = {}
    for key in _SUPPORTED_PROFILE_KEYS:
        value = os.environ.get(key)
        if value is not None:
            data[key] = value
    return data


def _require(values: dict[str, str], key: str) -> str:
    value = values.get(key)
    if value is None or value == "":
        message = f"Missing required setting: {key}"
        raise ConfigError(
            message,
            details={"key": key},
            suggestion=_missing_setting_suggestion(key),
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


def _string_with_default(value: str | None, *, default: str) -> str:
    if value is None or value == "":
        return default
    return value


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


def _missing_setting_suggestion(key: str) -> str:
    return f"Set {key} in the environment or provide it through --env-file."


def _integer_setting_suggestion(key: str) -> str:
    return f"Set {key} to a valid integer in the environment or env file."


def _minimum_setting_suggestion(key: str, *, minimum: int) -> str:
    return f"Set {key} to an integer greater than or equal to {minimum}."
