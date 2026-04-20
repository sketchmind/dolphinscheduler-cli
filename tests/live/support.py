from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

LIVE_TESTS_ENABLED_ENV: Final = "DSCTL_RUN_LIVE_TESTS"
LIVE_ADMIN_TESTS_ENABLED_ENV: Final = "DSCTL_RUN_LIVE_ADMIN_TESTS"
LIVE_API_URL_ENV: Final = "DS_LIVE_API_URL"
LIVE_ADMIN_TOKEN_ENV: Final = "DS_LIVE_ADMIN_TOKEN"
LIVE_ADMIN_ENV_FILE_ENV: Final = "DS_LIVE_ADMIN_ENV_FILE"
LIVE_ETL_ENV_FILE_ENV: Final = "DS_LIVE_ETL_ENV_FILE"
LIVE_TENANT_CODE_ENV: Final = "DS_LIVE_TENANT_CODE"
LIVE_QUEUE_ENV: Final = "DS_LIVE_QUEUE"
LIVE_KEEP_RESOURCES_ENV: Final = "DS_LIVE_KEEP_RESOURCES"
DEFAULT_LIVE_QUEUE: Final = "default"
PROFILE_ENV_NAMES: Final = frozenset(
    {
        "DS_API_URL",
        "DS_API_TOKEN",
        "DS_API_RETRY_ATTEMPTS",
        "DS_API_RETRY_BACKOFF_MS",
    }
)
LIVE_HARNESS_ENV_NAMES: Final = frozenset(
    {
        LIVE_TESTS_ENABLED_ENV,
        LIVE_ADMIN_TESTS_ENABLED_ENV,
        LIVE_API_URL_ENV,
        LIVE_ADMIN_TOKEN_ENV,
        LIVE_ADMIN_ENV_FILE_ENV,
        LIVE_ETL_ENV_FILE_ENV,
        LIVE_TENANT_CODE_ENV,
        LIVE_QUEUE_ENV,
        LIVE_KEEP_RESOURCES_ENV,
    }
)


@dataclass(frozen=True)
class LiveProfileConfig:
    """Minimal DS profile data needed by the live harness."""

    api_url: str
    api_token: str
    tenant_code: str | None = None


@dataclass(frozen=True)
class LiveSettings:
    """Process-environment settings used by the live test harness."""

    admin: LiveProfileConfig | None
    etl: LiveProfileConfig | None
    queue: str
    keep_resources: bool


@dataclass(frozen=True)
class DsctlCommandResult:
    """Completed black-box CLI invocation plus parsed JSON payload."""

    argv: tuple[str, ...]
    exit_code: int
    stdout: str
    stderr: str
    payload: dict[str, object]


@dataclass(frozen=True)
class LiveBootstrapState:
    """Resolved live identity used by non-admin live suites."""

    admin_env_file: Path | None
    etl_env_file: Path
    tenant_code: str | None
    user_name: str | None
    password: str | None
    access_token_id: int | None
    token: str | None
    used_existing_etl_profile: bool


def live_tests_enabled() -> bool:
    """Return whether the live test suite is explicitly enabled."""
    return _truthy_env(LIVE_TESTS_ENABLED_ENV)


def live_admin_tests_enabled() -> bool:
    """Return whether admin-scoped live tests are explicitly enabled."""
    return _truthy_env(LIVE_ADMIN_TESTS_ENABLED_ENV)


def load_live_settings() -> LiveSettings:
    """Read the live harness settings from the current process environment."""
    return LiveSettings(
        admin=_load_profile_from_process_env_or_file(
            env_file_var=LIVE_ADMIN_ENV_FILE_ENV,
            token_var=LIVE_ADMIN_TOKEN_ENV,
        ),
        etl=_load_profile_from_process_env_or_file(
            env_file_var=LIVE_ETL_ENV_FILE_ENV,
            token_var=None,
        ),
        queue=_optional_process_text(LIVE_QUEUE_ENV) or DEFAULT_LIVE_QUEUE,
        keep_resources=_truthy_env(LIVE_KEEP_RESOURCES_ENV),
    )


def create_live_run_prefix() -> str:
    """Return one DS-safe run prefix for remote resource names."""
    timestamp = datetime.now(tz=UTC).strftime("%Y%m%d-%H%M%S")
    return f"dsctl-live-{timestamp}-{os.urandom(2).hex()}"


def future_expire_time(*, days: int = 30) -> str:
    """Return one DS-formatted expiration timestamp in the future."""
    return (datetime.now(tz=UTC) + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")


def write_profile_env(path: Path, profile: LiveProfileConfig) -> Path:
    """Materialize one DS profile as a dotenv-style file."""
    lines = [
        f"DS_API_URL={profile.api_url}",
        f"DS_API_TOKEN={profile.api_token}",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def run_dsctl(
    repo_root: Path,
    argv: list[str],
    *,
    env_file: Path | None = None,
    extra_env: dict[str, str] | None = None,
    timeout_seconds: float = 60.0,
) -> DsctlCommandResult:
    """Run `python -m dsctl` as a black-box subprocess and parse its JSON."""
    command = [sys.executable, "-m", "dsctl"]
    if env_file is not None:
        command.extend(["--env-file", str(env_file)])
    command.extend(argv)
    env = _clean_dsctl_subprocess_env(os.environ, env_file=env_file)
    env["PYTHONPATH"] = _pythonpath(repo_root, env.get("PYTHONPATH"))
    if extra_env is not None:
        env.update(extra_env)
    completed = subprocess.run(
        command,
        capture_output=True,
        check=False,
        cwd=repo_root,
        env=env,
        text=True,
        timeout=timeout_seconds,
    )
    payload = _parse_command_payload(
        command=command,
        stdout=completed.stdout,
        stderr=completed.stderr,
    )
    return DsctlCommandResult(
        argv=tuple(command),
        exit_code=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
        payload=payload,
    )


def run_dsctl_raw(
    repo_root: Path,
    argv: list[str],
    *,
    env_file: Path | None = None,
    extra_env: dict[str, str] | None = None,
    timeout_seconds: float = 60.0,
) -> DsctlCommandResult:
    """Run `python -m dsctl` as a black-box subprocess without JSON parsing."""
    command = [sys.executable, "-m", "dsctl"]
    if env_file is not None:
        command.extend(["--env-file", str(env_file)])
    command.extend(argv)
    env = _clean_dsctl_subprocess_env(os.environ, env_file=env_file)
    env["PYTHONPATH"] = _pythonpath(repo_root, env.get("PYTHONPATH"))
    if extra_env is not None:
        env.update(extra_env)
    completed = subprocess.run(
        command,
        capture_output=True,
        check=False,
        cwd=repo_root,
        env=env,
        text=True,
        timeout=timeout_seconds,
    )
    return DsctlCommandResult(
        argv=tuple(command),
        exit_code=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
        payload={},
    )


def require_mapping(
    value: object,
    *,
    label: str,
) -> dict[str, object]:
    """Require one plain JSON object from a parsed command payload."""
    if not isinstance(value, dict):
        message = f"{label} must be a JSON object, got {type(value).__name__}"
        raise TypeError(message)
    return value


def require_list(
    value: object,
    *,
    label: str,
) -> list[object]:
    """Require one JSON array from a parsed command payload."""
    if not isinstance(value, list):
        message = f"{label} must be a JSON array, got {type(value).__name__}"
        raise TypeError(message)
    return value


def require_int_value(
    value: object,
    *,
    label: str,
) -> int:
    """Require one integer value from a parsed JSON payload."""
    if not isinstance(value, int):
        message = f"{label} must be an integer, got {type(value).__name__}"
        raise TypeError(message)
    return value


def require_text_value(
    value: object,
    *,
    label: str,
) -> str:
    """Require one non-empty string value from a parsed JSON payload."""
    if not isinstance(value, str) or value == "":
        message = f"{label} must be a non-empty string"
        raise TypeError(message)
    return value


def require_ok_payload(
    result: DsctlCommandResult,
    *,
    expected_action: str,
    label: str,
) -> dict[str, object]:
    """Require one successful CLI result with the expected action."""
    if result.exit_code != 0 or result.payload.get("ok") is not True:
        message = (
            f"{label} failed with exit code {result.exit_code}: "
            f"{json.dumps(result.payload, ensure_ascii=False)}"
        )
        raise AssertionError(message)
    action = result.payload.get("action")
    if action != expected_action:
        message = f"{label} returned action {action!r}, expected {expected_action!r}"
        raise AssertionError(message)
    return result.payload


def require_error_payload(
    result: DsctlCommandResult,
    *,
    expected_action: str,
    expected_type: str | None = None,
    label: str,
) -> dict[str, object]:
    """Require one structured CLI error payload with the expected action."""
    if result.exit_code == 0 or result.payload.get("ok") is not False:
        message = (
            f"{label} unexpectedly succeeded: "
            f"{json.dumps(result.payload, ensure_ascii=False)}"
        )
        raise AssertionError(message)
    action = result.payload.get("action")
    if action != expected_action:
        message = f"{label} returned action {action!r}, expected {expected_action!r}"
        raise AssertionError(message)
    error = require_mapping(result.payload.get("error"), label=f"{label} error")
    if expected_type is not None and error.get("type") != expected_type:
        message = (
            f"{label} returned error type {error.get('type')!r}, "
            f"expected {expected_type!r}"
        )
        raise AssertionError(message)
    return error


def result_error_code(result: DsctlCommandResult) -> int | None:
    """Extract one upstream/result code from a structured CLI error payload."""
    error = result.payload.get("error")
    if not isinstance(error, dict):
        return None
    source = error.get("source")
    if isinstance(source, dict):
        source_result_code = source.get("result_code")
        if isinstance(source_result_code, int):
            return source_result_code
    details = error.get("details")
    if isinstance(details, dict):
        details_result_code = details.get("result_code")
        if isinstance(details_result_code, int):
            return details_result_code
    return None


def wait_for_result(
    repo_root: Path,
    argv: list[str],
    *,
    accept: Callable[[DsctlCommandResult], bool],
    env_file: Path | None = None,
    extra_env: dict[str, str] | None = None,
    timeout_seconds: float = 30.0,
    interval_seconds: float = 1.0,
    command_timeout_seconds: float = 60.0,
) -> DsctlCommandResult:
    """Poll one black-box CLI command until the accept predicate returns true."""
    if timeout_seconds < 0:
        message = "timeout_seconds must be non-negative"
        raise ValueError(message)
    if interval_seconds <= 0:
        message = "interval_seconds must be positive"
        raise ValueError(message)

    started_at = time.monotonic()
    while True:
        result = run_dsctl(
            repo_root,
            argv,
            env_file=env_file,
            extra_env=extra_env,
            timeout_seconds=command_timeout_seconds,
        )
        if accept(result):
            return result
        if (time.monotonic() - started_at) >= timeout_seconds:
            return result
        time.sleep(interval_seconds)


def _clean_dsctl_subprocess_env(
    source_env: Mapping[str, str],
    *,
    env_file: Path | None,
) -> dict[str, str]:
    env: dict[str, str] = {}
    for key, value in source_env.items():
        if key in LIVE_HARNESS_ENV_NAMES or key.startswith("DS_LIVE_"):
            continue
        if env_file is not None and key in PROFILE_ENV_NAMES:
            continue
        env[key] = value
    return env


def _truthy_env(name: str) -> bool:
    value = os.environ.get(name)
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _optional_process_text(name: str) -> str | None:
    value = os.environ.get(name)
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def _load_profile_from_process_env_or_file(
    *,
    env_file_var: str,
    token_var: str | None,
) -> LiveProfileConfig | None:
    env_file_value = _optional_process_text(env_file_var)
    if env_file_value is not None:
        return _load_profile_from_env_file(Path(env_file_value))

    api_url = _optional_process_text(LIVE_API_URL_ENV)
    api_token = None if token_var is None else _optional_process_text(token_var)
    if api_url is None or api_token is None:
        return None
    return LiveProfileConfig(
        api_url=api_url,
        api_token=api_token,
        tenant_code=_optional_process_text(LIVE_TENANT_CODE_ENV),
    )


def _load_profile_from_env_file(path: Path) -> LiveProfileConfig:
    values = _parse_env_file(path)
    api_url = values.get("DS_API_URL")
    api_token = values.get("DS_API_TOKEN")
    if api_url is None or api_token is None:
        message = (
            f"Live env file {path} must define DS_API_URL and DS_API_TOKEN "
            "for the live harness"
        )
        raise AssertionError(message)
    return LiveProfileConfig(
        api_url=api_url.rstrip("/"),
        api_token=api_token,
        tenant_code=values.get("DS_LIVE_TENANT_CODE"),
    )


def _parse_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", maxsplit=1)
        values[key.strip()] = _strip_optional_quotes(value.strip())
    return values


def _strip_optional_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def _pythonpath(repo_root: Path, existing_pythonpath: str | None) -> str:
    repo_src = str(repo_root / "src")
    if existing_pythonpath is None or existing_pythonpath == "":
        return repo_src
    return os.pathsep.join([repo_src, existing_pythonpath])


def _parse_command_payload(
    *,
    command: list[str],
    stdout: str,
    stderr: str,
) -> dict[str, object]:
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError as error:
        message = (
            f"Command {' '.join(command)} did not emit valid JSON.\n"
            f"stdout:\n{stdout}\n"
            f"stderr:\n{stderr}"
        )
        raise AssertionError(message) from error
    if not isinstance(payload, dict):
        message = (
            f"Command {' '.join(command)} emitted a non-object JSON payload: "
            f"{type(payload).__name__}"
        )
        raise TypeError(message)
    return payload
