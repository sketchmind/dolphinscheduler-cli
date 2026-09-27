"""Resolve exact metadata or candidate evidence before selecting runtime contracts."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import time
from contextlib import contextmanager, suppress
from contextvars import ContextVar
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import TYPE_CHECKING, Literal
from urllib.parse import urlsplit, urlunsplit

from pydantic import BaseModel, ConfigDict

from dsctl.config import (
    ClusterProfile,
    TargetSettings,
    configuration_scope,
    load_target_settings,
)
from dsctl.context import absolute_config_path
from dsctl.errors import ConfigError, DsctlError
from dsctl.upstream import SUPPORTED_VERSIONS
from dsctl.upstream.read_compatibility import build_read_compatibility_plan
from dsctl.upstream.version_discovery import (
    VersionDiscoveryError,
    discover_target,
    discovery_contract_digest,
)
from dsctl.versioning import DEFAULT_DS_VERSION, normalize_version

if TYPE_CHECKING:
    from collections.abc import Iterator

    from dsctl.config import ConnectionSettings
    from dsctl.output import CommandResult, JsonObject
    from dsctl.upstream.read_compatibility import ReadCompatibilityPlan

ResolutionMode = Literal["local", "runtime", "refresh"]
ResolutionSource = Literal["explicit", "probe", "cache", "offline_default"]
_CACHE_TTL_SECONDS = 300


@dataclass(frozen=True)
class VersionResolution:
    """An exact selection together with its observation or configuration origin."""

    version: str
    source: ResolutionSource
    checked_at: float | None = None
    evidence: str | None = None


@dataclass(frozen=True)
class CompatibilityResolution:
    """Observed contracts, without asserting an exact server release."""

    candidate_versions: tuple[str, ...]
    source: str
    checked_at: float
    evidence: str
    reported_version: str | None = None
    probes: tuple[dict[str, str | int], ...] = ()
    compatible_operations: tuple[str, ...] = ()


DiscoveryResolution = VersionResolution | CompatibilityResolution


@dataclass(frozen=True)
class RuntimeSelection:
    """Separate the selected executable contract from server identification."""

    execution_profile: ClusterProfile
    read_plan: ReadCompatibilityPlan | None = None
    project: str | None = None


class _CachedVersion(BaseModel):
    """Validate persisted discovery data at its filesystem boundary."""

    model_config = ConfigDict(strict=True, extra="forbid", allow_inf_nan=False)

    schema_version: int
    target_key: str
    version: str | None
    checked_at: float
    evidence: str
    candidate_versions: tuple[str, ...] = ()
    reported_version: str | None = None
    contract_digest: str | None = None
    probes: tuple[dict[str, str | int], ...] = ()
    compatible_operations: tuple[str, ...] = ()


@dataclass
class _TargetResolution:
    settings: TargetSettings | None = None
    settings_error: DsctlError | None = None
    local: DiscoveryResolution | DsctlError | None = None
    online: DiscoveryResolution | DsctlError | None = None
    runtime: RuntimeSelection | None = None


@dataclass
class _Invocation:
    action: str | None = None
    env_file: str | Path | None = None
    targets: dict[Path | None, _TargetResolution] = field(default_factory=dict)


_INVOCATION: ContextVar[_Invocation | None] = ContextVar(
    "dsctl_version_resolution_invocation", default=None
)


@contextmanager
def invocation_scope(
    action: str | None = None,
    *,
    context_name: str | None = None,
    env_file: str | Path | None = None,
) -> Iterator[None]:
    """Share settings, successful probes, and failures for one emitted command."""
    if _INVOCATION.get() is not None:
        yield
        return
    token = _INVOCATION.set(_Invocation(action=action, env_file=env_file))
    try:
        with configuration_scope(context_name=context_name, env_file=env_file):
            yield
    finally:
        _INVOCATION.reset(token)


def set_invocation_action(action: str) -> None:
    """Retain the CLI action before version policy or service execution."""
    scope = _INVOCATION.get()
    if scope is not None:
        scope.action = action


def resolve_version(
    env_file: str | Path | None = None,
    *,
    mode: ResolutionMode = "local",
) -> VersionResolution:
    """Select an explicit version, a fresh observation, or local discovery data."""
    resolved = resolve_target(env_file, mode=mode)
    if isinstance(resolved, CompatibilityResolution):
        raise _exact_version_required(resolved)
    return resolved


def resolve_target(
    env_file: str | Path | None = None,
    *,
    mode: ResolutionMode = "local",
) -> DiscoveryResolution:
    """Return exact metadata or contract evidence without inventing a version."""
    return _resolve(_target(env_file), mode=mode)


def resolve_profile(
    env_file: str | Path | None = None,
    *,
    mode: Literal["runtime", "refresh"] = "runtime",
) -> ClusterProfile:
    """Construct an exact runtime profile only after version resolution succeeds."""
    target = _target(env_file)
    resolution = resolve_version(env_file, mode=mode)
    connection = _settings(target).connection()
    return ClusterProfile(
        api_url=connection.api_url,
        api_token=connection.api_token,
        api_retry_attempts=connection.api_retry_attempts,
        api_retry_backoff_ms=connection.api_retry_backoff_ms,
        api_timeout_seconds=connection.api_timeout_seconds,
        ds_version=resolution.version,
    )


def resolve_settings(env_file: str | Path | None = None) -> TargetSettings:
    """Return the same immutable inputs used for this invocation's discovery."""
    return _settings(_target(env_file))


def resolve_runtime_selection(
    env_file: str | Path | None = None,
    *,
    action: str | None = None,
) -> RuntimeSelection:
    """Admit only a reviewed read action when the exact release is unknown."""
    target = _target(env_file)
    observed = _resolve(target, mode="runtime")
    if isinstance(observed, VersionResolution):
        return RuntimeSelection(
            ClusterProfile(
                **_settings(target).connection().model_dump(),
                ds_version=observed.version,
            ),
            project=_settings(target).project,
        )
    scope = _INVOCATION.get()
    if (
        action is not None
        and scope is not None
        and scope.action is not None
        and action != scope.action
    ):
        message = "The read compatibility action differs from the active command"
        raise ConfigError(
            message,
            details={"reason": "invocation_action_mismatch", "action": scope.action},
        )
    selected_action = action or (scope.action if scope is not None else None)
    if selected_action is None or not observed.candidate_versions:
        raise _exact_version_required(observed, action=selected_action)
    if (
        target.runtime is not None
        and target.runtime.read_plan is not None
        and target.runtime.read_plan.action == selected_action
    ):
        return target.runtime
    plan = build_read_compatibility_plan(
        observed.candidate_versions,
        action=selected_action,
        compatible_operations=observed.compatible_operations,
    )
    selection = RuntimeSelection(
        execution_profile=ClusterProfile(
            **_settings(target).connection().model_dump(),
            ds_version=plan.execution_version,
        ),
        read_plan=plan,
        project=_settings(target).project,
    )
    target.runtime = selection
    return selection


def compatibility_details(resolution: CompatibilityResolution) -> JsonObject:
    """Project bounded discovery evidence into the public output boundary."""
    return {
        "ds_version": None,
        "reported_version": resolution.reported_version,
        "candidate_versions": list(resolution.candidate_versions),
        "version_source": resolution.source,
        "version_checked_at": resolution.checked_at,
        "version_evidence": resolution.evidence,
        "matched_operation_count": len(resolution.compatible_operations),
        "identification": (
            "contract_candidates" if resolution.candidate_versions else "unresolved"
        ),
    }


def _exact_version_required(
    observed: CompatibilityResolution, *, action: str | None = None
) -> ConfigError:
    message = "The exact DolphinScheduler release could not be established"
    return ConfigError(
        message,
        details={
            **compatibility_details(observed),
            "reason": "exact_version_required",
            "action": action,
            "probes": list(observed.probes),
        },
        suggestion=(
            "Configure DS_VERSION once from the deployment's actual release "
            "for this operation. Contract candidates do not establish that release; "
            "reviewed read commands can run without an exact version."
        ),
    )


def selected_target_globals(env_file: str | Path | None = None) -> dict[str, str]:
    """Preserve the selected target in locally generated command hints."""
    settings = resolve_settings(env_file)
    if settings.context_name is not None:
        return {"context": settings.context_name}
    if settings.env_file is not None:
        return {"env-file": str(settings.env_file)}
    return {}


def selection_details(settings: TargetSettings) -> JsonObject:
    """Expose connection provenance without credentials or duplicated settings."""
    return {
        "source": settings.source,
        "context": settings.context_name,
        "env_file": None if settings.env_file is None else str(settings.env_file),
        "api_url": settings.api_url,
    }


def annotate_target_result(
    result: CommandResult, env_file: Path | None
) -> CommandResult:
    """Report the selected connection and any bounded compatibility execution."""
    scope = _INVOCATION.get()
    if scope is None:
        return result
    key = _target_key(env_file)
    target = scope.targets.get(key)
    if target is None:
        return result
    if target.settings is not None:
        result = replace(
            result,
            resolved={
                **result.resolved,
                "selection": selection_details(target.settings),
            },
        )
    if target.runtime is None or target.runtime.read_plan is None:
        return result
    observed = target.online
    if not isinstance(observed, CompatibilityResolution):
        return result
    message = (
        "Exact DS release is unknown; this read uses a reviewed compatible contract."
    )
    return replace(
        result,
        resolved={
            **result.resolved,
            "target": {**compatibility_details(observed), "execution": "read_only"},
        },
        warnings=[*result.warnings, message],
        warning_details=[
            *result.warning_details,
            {"code": "read_compatibility", "message": message},
        ],
    )


def annotate_target_error(payload: JsonObject, env_file: Path | None) -> JsonObject:
    """Retain connection provenance and exact identity limits on failed actions."""
    scope = _INVOCATION.get()
    if scope is None:
        return payload
    try:
        key = _target_key(env_file)
    except (DsctlError, OSError, RuntimeError):
        # Diagnostics must not hide the original failure if the path changed.
        return payload
    target = scope.targets.get(key)
    if target is None:
        return payload
    if target.settings is not None:
        resolved = payload.get("resolved")
        payload = {
            **payload,
            "resolved": {
                **(resolved if isinstance(resolved, dict) else {}),
                "selection": selection_details(target.settings),
            },
        }
    if not isinstance(target.online, CompatibilityResolution):
        return payload
    error = payload.get("error")
    if not isinstance(error, dict):
        return payload
    projected_error = dict(error)
    if target.runtime is not None:
        projected_error = _contract_error_projection(
            projected_error, target.runtime.execution_profile.ds_version
        )
    resolved = payload.get("resolved")
    return {
        **payload,
        "resolved": {
            **(resolved if isinstance(resolved, dict) else {}),
            "target": compatibility_details(target.online),
        },
        "error": projected_error,
    }


def _contract_error_projection(error: JsonObject, version: str) -> JsonObject:
    details = error.get("details")
    if not isinstance(details, dict):
        return error
    message = error.get("message")
    unsupported = (
        error.get("type") == "unsupported_feature"
        and details.get("reason")
        in ("upstream_capability_absent", "upstream_capability_limited")
        and details.get("selected_version") == version
    )
    projection = (
        error.get("type") == "api_transport_error"
        and message
        == "DolphinScheduler returned an incompatible runtime-instance payload"
        and details.get("ds_version") == version
    )
    if not unsupported and not projection:
        return error
    field_name = "selected_version" if unsupported else "ds_version"
    projected_details = {
        **details,
        field_name: None,
        "execution_contract_version": version,
    }
    # Only these complete, locally authored templates make a server-version
    # assertion. Substring matching could corrupt user names or upstream prose.
    if unsupported and error.get("source") is None:
        action = details.get("action")
        flag = details.get("flag")
        prefix = f"{action}" if flag is None else f"{flag} for {action}"
        original = (
            f"{action} is not available on DolphinScheduler {version}."
            if flag is None
            else f"{flag} is not available for {action} on DolphinScheduler {version}."
        )
        if isinstance(action, str) and message == original:
            message = f"{prefix} is not available under the selected read contract."
    return {**error, "message": message, "details": projected_details}


def _target_key(env_file: str | Path | None) -> Path | None:
    """Share one target for explicit and ambient references to the same file."""
    scope = _INVOCATION.get()
    selected_file = (
        env_file
        if env_file is not None
        else (None if scope is None else scope.env_file)
    )
    return (
        None
        if selected_file is None
        else absolute_config_path(selected_file, label="selected env file")
    )


def _target(env_file: str | Path | None) -> _TargetResolution:
    scope = _INVOCATION.get()
    key = _target_key(env_file)
    target = (
        _TargetResolution()
        if scope is None
        else scope.targets.setdefault(key, _TargetResolution())
    )
    if target.settings is None and target.settings_error is None:
        try:
            target.settings = load_target_settings(env_file)
        except DsctlError as exc:
            target.settings_error = exc
    return target


def _settings(target: _TargetResolution) -> TargetSettings:
    if target.settings_error is not None:
        raise target.settings_error
    if target.settings is None:
        message = "Target settings have not been loaded"
        raise RuntimeError(message)
    return target.settings


def _resolve(target: _TargetResolution, *, mode: ResolutionMode) -> DiscoveryResolution:
    settings = _settings(target)
    if target.online is not None:
        return _result(target.online)
    if mode == "local" and target.local is not None:
        return _result(target.local)
    try:
        resolved: DiscoveryResolution
        requested = settings.requested_version
        if requested is not None and requested != "auto":
            resolved = VersionResolution(_exact_version(requested), "explicit")
        elif (
            requested is None
            and settings.api_url is None
            and settings.api_token is None
        ):
            resolved = VersionResolution(DEFAULT_DS_VERSION, "offline_default")
        elif mode == "local":
            resolved = _resolve_local(settings)
        else:
            resolved = _resolve_online(settings)
            prior = target.local
            if isinstance(
                prior, (VersionResolution, CompatibilityResolution)
            ) and _identity(prior) != _identity(resolved):
                message = "DS version changed after local discovery in this command"
                raise ConfigError(
                    message,
                    details={
                        "reason": "version_changed_during_invocation",
                        "local_version": prior.version
                        if isinstance(prior, VersionResolution)
                        else None,
                        "detected_version": resolved.version
                        if isinstance(resolved, VersionResolution)
                        else None,
                    },
                    suggestion="Rerun the command using the newly detected version.",
                )
    except DsctlError as exc:
        if mode == "local":
            target.local = exc
        else:
            target.online = exc
        raise
    if mode == "local":
        target.local = resolved
    else:
        target.online = resolved
    return resolved


def _identity(
    value: DiscoveryResolution,
) -> tuple[str] | tuple[tuple[str, ...], tuple[str, ...], str | None]:
    if isinstance(value, VersionResolution):
        return (value.version,)
    return (
        value.candidate_versions,
        value.compatible_operations,
        value.reported_version,
    )


def _result(value: DiscoveryResolution | DsctlError) -> DiscoveryResolution:
    if isinstance(value, DsctlError):
        raise value
    return value


def _exact_version(version: str) -> str:
    normalized = normalize_version(version)
    if normalized not in SUPPORTED_VERSIONS:
        message = f"Unsupported DS version {version!r}"
        raise ConfigError(
            message,
            details={
                "version": version,
                "supported_versions": list(SUPPORTED_VERSIONS),
            },
            suggestion="Set DS_VERSION to a reviewed exact DolphinScheduler version.",
        )
    return normalized


def _resolve_local(settings: TargetSettings) -> DiscoveryResolution:
    if settings.api_url and settings.api_token:
        cached = _read_cache(_cache_key(settings.connection()))
        if cached is not None:
            return cached
    message = "The target DolphinScheduler version has not been resolved locally"
    raise ConfigError(
        message,
        details={"reason": "version_not_resolved", "requested_version": "auto"},
        suggestion="Run `dsctl doctor` to detect the target, or set DS_VERSION.",
    )


def _resolve_online(settings: TargetSettings) -> DiscoveryResolution:
    connection = settings.connection()
    try:
        observed = discover_target(connection)
    except VersionDiscoveryError as exc:
        with suppress(OSError, ConfigError):
            _cache_path(_cache_key(connection)).unlink(missing_ok=True)
        message = "Could not detect the target DolphinScheduler version"
        suggestion = (
            "Check that DS_API_TOKEN is valid and unexpired, and that its account "
            "is enabled."
            if exc.reason == "authentication_failed"
            else "Check the target connection, or set DS_VERSION explicitly."
        )
        raise ConfigError(
            message,
            details={"reason": exc.reason, "probes": list(exc.probes)},
            suggestion=suggestion,
        ) from exc
    resolved: DiscoveryResolution
    if observed.version is not None:
        resolved = VersionResolution(
            _exact_version(observed.version), "probe", time.time(), observed.source
        )
    else:
        resolved = CompatibilityResolution(
            observed.candidate_versions,
            observed.source,
            time.time(),
            observed.evidence_summary,
            observed.reported_version,
            observed.probes,
            observed.compatible_operations,
        )
    _write_cache(_cache_key(connection), resolved)
    return resolved


def _cache_key(connection: ConnectionSettings) -> str:
    try:
        parts = urlsplit(connection.api_url)
        port = parts.port
    except ValueError as exc:
        message = "DS_API_URL is not a valid API base URL"
        raise ConfigError(
            message,
            suggestion="Set DS_API_URL to the DolphinScheduler API base URL.",
        ) from exc
    hostname = parts.hostname or ""
    host = f"[{hostname}]" if ":" in hostname else hostname.lower()
    if port is not None and (parts.scheme.lower(), port) not in {
        ("http", 80),
        ("https", 443),
    }:
        host = f"{host}:{port}"
    if "@" in parts.netloc:
        host = f"{parts.netloc.rsplit('@', 1)[0]}@{host}"
    normalized_url = urlunsplit(
        (parts.scheme.lower(), host, parts.path.rstrip("/"), parts.query, "")
    )
    credential_scope = hashlib.sha256(connection.api_token.encode()).hexdigest()
    return hashlib.sha256(f"{normalized_url}\0{credential_scope}".encode()).hexdigest()


def _cache_path(key: str) -> Path:
    configured = os.environ.get("XDG_CACHE_HOME")
    root = Path(configured) if configured else Path.home() / ".cache"
    return root / "dsctl" / "version-discovery" / f"{key}.json"


def _read_cache(key: str) -> DiscoveryResolution | None:
    try:
        payload = _CachedVersion.model_validate_json(_cache_path(key).read_bytes())
    except (OSError, ValueError):
        return None
    if (
        payload.target_key != key
        or not 0 <= time.time() - payload.checked_at < _CACHE_TTL_SECONDS
    ):
        return None
    if payload.schema_version == 2:
        if (
            payload.version is not None
            or payload.contract_digest != discovery_contract_digest()
            or not set(payload.candidate_versions) <= set(SUPPORTED_VERSIONS)
        ):
            return None
        return CompatibilityResolution(
            payload.candidate_versions,
            "cache",
            payload.checked_at,
            payload.evidence,
            payload.reported_version,
            payload.probes,
            payload.compatible_operations,
        )
    if (
        payload.schema_version != 1
        or payload.version is None
        or payload.version not in SUPPORTED_VERSIONS
    ):
        return None
    return VersionResolution(
        payload.version, "cache", payload.checked_at, payload.evidence
    )


def _write_cache(key: str, resolution: DiscoveryResolution) -> None:
    data: JsonObject = {
        "schema_version": 1,
        "target_key": key,
        "version": resolution.version
        if isinstance(resolution, VersionResolution)
        else None,
        "checked_at": resolution.checked_at,
        "evidence": resolution.evidence,
    }
    if isinstance(resolution, CompatibilityResolution):
        data.update(
            schema_version=2,
            candidate_versions=list(resolution.candidate_versions),
            reported_version=resolution.reported_version,
            contract_digest=discovery_contract_digest(),
            probes=[dict(probe) for probe in resolution.probes],
            compatible_operations=list(resolution.compatible_operations),
        )
    path = _cache_path(key)
    temporary: Path | None = None
    try:
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, delete=False
        ) as stream:
            temporary = Path(stream.name)
            os.fchmod(stream.fileno(), 0o600)
            json.dump(
                data,
                stream,
                sort_keys=True,
            )
            stream.flush()
            os.fsync(stream.fileno())
        Path(temporary).replace(path)
    except OSError:
        # A discovery cache cannot make a successfully resolved runtime unusable.
        return
    finally:
        if temporary is not None:
            with suppress(OSError):
                temporary.unlink(missing_ok=True)


__all__ = [
    "VersionResolution",
    "invocation_scope",
    "resolve_profile",
    "resolve_version",
    "selected_target_globals",
]
