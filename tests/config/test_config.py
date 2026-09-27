from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from dsctl.config import (
    configuration_scope,
    load_profile,
    load_selected_ds_version,
    load_target_settings,
    validate_context_file,
)
from dsctl.context import (
    NamedContext,
    create_context,
    get_default_context,
    registry_path,
    set_default_context,
)
from dsctl.errors import ConfigError
from dsctl.services.configuration import set_config_result

if TYPE_CHECKING:
    from collections.abc import Callable


def test_load_profile_from_env_file(tmp_path: Path) -> None:
    env_file = tmp_path / "cluster.env"
    env_file.write_text(
        (
            "DS_VERSION=3.4.1\n"
            "DS_API_URL=http://example.test/dolphinscheduler\n"
            "DS_API_TOKEN=token-value\n"
            "DS_API_RETRY_ATTEMPTS=5\n"
            "DS_API_RETRY_BACKOFF_MS=50\n"
            "DS_API_TIMEOUT_SECONDS=2.5"
        ),
        encoding="utf-8",
    )

    profile = load_profile(env_file)

    assert profile.api_url == "http://example.test/dolphinscheduler"
    expected_token = "-".join(["token", "value"])
    assert profile.api_token == expected_token
    assert profile.ds_version == "3.4.1"
    assert profile.health_url == "http://example.test/dolphinscheduler/actuator/health"
    assert profile.api_retry_attempts == 5
    assert profile.api_retry_backoff_ms == 50
    assert profile.api_timeout_seconds == 2.5


def test_profile_redaction_masks_secrets(tmp_path: Path) -> None:
    env_file = tmp_path / "cluster.env"
    env_file.write_text(
        (
            "DS_VERSION=3.4.1\n"
            "DS_API_URL=http://example.test/dolphinscheduler\n"
            "DS_API_TOKEN=1234567890abcdef"
        ),
        encoding="utf-8",
    )

    redacted = load_profile(env_file).redacted()

    expected_api_token = "".join(["1234", "...", "cdef"])
    assert redacted["api_token"] == expected_api_token


def test_load_profile_supports_export_and_quoted_values(tmp_path: Path) -> None:
    env_file = tmp_path / "cluster.env"
    env_file.write_text(
        (
            "DS_VERSION=3.4.1\n"
            'export DS_API_URL="http://example.test/dolphinscheduler"\n'
            "export DS_API_TOKEN='quoted-token'"
        ),
        encoding="utf-8",
    )

    profile = load_profile(env_file)

    assert profile.api_url == "http://example.test/dolphinscheduler"
    expected_token = "-".join(["quoted", "token"])
    assert profile.api_token == expected_token


def test_explicit_env_file_keeps_identity_but_process_policy_takes_precedence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env_file = tmp_path / "cluster.env"
    env_file.write_text(
        (
            "DS_VERSION=3.3.2\n"
            "DS_API_URL=http://file.test/dolphinscheduler\n"
            "DS_API_TOKEN=file-token\n"
            "DS_API_RETRY_ATTEMPTS=7\n"
            "DS_API_RETRY_BACKOFF_MS=75\n"
            "DS_API_TIMEOUT_SECONDS=12\n"
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("DS_VERSION", "3.4.2")
    monkeypatch.setenv("DS_API_URL", "http://env.test/dolphinscheduler/")
    monkeypatch.setenv("DS_API_TOKEN", "env-token")
    monkeypatch.setenv("DS_API_RETRY_ATTEMPTS", "2")
    monkeypatch.setenv("DS_API_RETRY_BACKOFF_MS", "25")
    monkeypatch.setenv("DS_API_TIMEOUT_SECONDS", "1.25")

    profile = load_profile(env_file)

    assert profile.api_url == "http://file.test/dolphinscheduler"
    expected_token = "-".join(["file", "token"])
    assert profile.api_token == expected_token
    assert profile.ds_version == "3.3.2"
    assert profile.api_retry_attempts == 2
    assert profile.api_retry_backoff_ms == 25
    assert profile.api_timeout_seconds == 1.25
    assert profile.health_url == "http://file.test/dolphinscheduler/actuator/health"


def test_explicit_env_file_does_not_fall_back_to_process_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env_file = tmp_path / "cluster.env"
    env_file.write_text(
        "DS_API_URL=http://file.test/dolphinscheduler\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("DS_API_URL", "http://env.test/dolphinscheduler")
    monkeypatch.setenv("DS_API_TOKEN", "env-token")

    with pytest.raises(ConfigError) as exc_info:
        load_profile(env_file)

    assert exc_info.value.details["key"] == "DS_API_TOKEN"
    assert exc_info.value.suggestion == "Add DS_API_TOKEN to the selected env file."


def test_load_profile_accepts_and_normalizes_ds_version(tmp_path: Path) -> None:
    env_file = tmp_path / "cluster.env"
    env_file.write_text(
        (
            "DS_VERSION=ds_3_4_1\n"
            "DS_VERSION=3.4.1\n"
            "DS_API_URL=http://example.test/dolphinscheduler\n"
            "DS_API_TOKEN=token-value"
        ),
        encoding="utf-8",
    )

    profile = load_profile(env_file)

    assert profile.ds_version == "3.4.1"


def test_load_selected_ds_version_does_not_require_connection_settings(
    tmp_path: Path,
) -> None:
    env_file = tmp_path / "cluster.env"
    env_file.write_text("DS_VERSION=v3.4.1", encoding="utf-8")

    assert load_selected_ds_version(env_file) == "3.4.1"


def test_load_profile_ignores_live_harness_process_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env_file = tmp_path / "cluster.env"
    env_file.write_text(
        (
            "DS_VERSION=3.4.1\nDS_API_URL=http://file.test/dolphinscheduler\nDS_API_TOKEN=file-token\n"
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("DS_LIVE_TENANT_CODE", "live-tenant")
    monkeypatch.setenv("DS_LIVE_ADMIN_ENV_FILE", str(tmp_path / "admin.env"))

    profile = load_profile(env_file)

    assert profile.api_url == "http://file.test/dolphinscheduler"
    assert profile.api_token == "file-token"


def test_load_profile_rejects_invalid_integer_values(tmp_path: Path) -> None:
    env_file = tmp_path / "cluster.env"
    env_file.write_text(
        (
            "DS_API_URL=http://example.test/dolphinscheduler\n"
            "DS_API_TOKEN=token-value\n"
            "DS_API_RETRY_ATTEMPTS=not-an-integer"
        ),
        encoding="utf-8",
    )

    with pytest.raises(ConfigError) as exc_info:
        load_profile(env_file)

    assert exc_info.value.details["key"] == "DS_API_RETRY_ATTEMPTS"
    assert exc_info.value.suggestion == (
        "Set DS_API_RETRY_ATTEMPTS to a valid integer in the environment or env file."
    )


def test_load_profile_rejects_retry_attempts_below_one(tmp_path: Path) -> None:
    env_file = tmp_path / "cluster.env"
    env_file.write_text(
        "DS_API_URL=http://example.test/dolphinscheduler\nDS_API_TOKEN=token-value\nDS_API_RETRY_ATTEMPTS=0",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError) as exc_info:
        load_profile(env_file)

    assert exc_info.value.details["key"] == "DS_API_RETRY_ATTEMPTS"
    assert exc_info.value.suggestion == (
        "Set DS_API_RETRY_ATTEMPTS to an integer greater than or equal to 1."
    )


def test_load_profile_requires_missing_api_url_with_suggestion(
    tmp_path: Path,
) -> None:
    env_file = tmp_path / "cluster.env"
    env_file.write_text(
        "DS_API_TOKEN=token-value",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError) as exc_info:
        load_profile(env_file)

    assert exc_info.value.details["key"] == "DS_API_URL"
    assert exc_info.value.suggestion == "Add DS_API_URL to the selected env file."


def test_load_profile_rejects_unsupported_non_profile_keys(
    tmp_path: Path,
) -> None:
    env_file = tmp_path / "cluster.env"
    env_file.write_text(
        "\n".join(
            [
                "DS_API_URL=http://example.test/dolphinscheduler",
                "DS_API_TOKEN=token-value",
                "DS_DEFAULT_PROJECT=etl-prod",
                "DS_DEFAULT_USER=alice",
                "DS_DEFAULT_TENANT=tenant-prod",
                "DS_DEFAULT_WORKER_GROUP=analytics",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ConfigError) as exc_info:
        load_profile(env_file)

    assert exc_info.value.details["keys"] == [
        "DS_DEFAULT_PROJECT",
        "DS_DEFAULT_TENANT",
        "DS_DEFAULT_USER",
        "DS_DEFAULT_WORKER_GROUP",
    ]
    supported_keys = exc_info.value.details["supported_keys"]
    assert isinstance(supported_keys, list)
    assert "DS_VERSION" in supported_keys
    assert "DS_API_URL" in supported_keys
    assert str(exc_info.value) == "Unsupported DS profile settings"
    assert exc_info.value.suggestion == (
        "Cluster profile only accepts DS connection and version settings. "
        "Remove those keys from the profile. Use "
        "`dsctl context update NAME --project PROJECT` for local "
        "project selection and `dsctl project-preference update` for "
        "project-level runtime defaults."
    )


def test_load_profile_rejects_cluster_metadata_keys(tmp_path: Path) -> None:
    env_file = tmp_path / "cluster.env"
    env_file.write_text(
        "\n".join(
            [
                "DS_API_URL=http://example.test/dolphinscheduler",
                "DS_API_TOKEN=token-value",
                "DS_WEB_UI=http://example.test/dolphinscheduler/ui",
                "DS_DB_HOST=127.0.0.1",
                "DS_PY_GATEWAY_ADDRESS=127.0.0.1",
                "DS_DEPLOY_VERSION=3.4.1",
                "DS_LIVE_TENANT_CODE=dsctl-live",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ConfigError) as exc_info:
        load_profile(env_file)

    assert exc_info.value.details["keys"] == [
        "DS_DB_HOST",
        "DS_DEPLOY_VERSION",
        "DS_LIVE_TENANT_CODE",
        "DS_PY_GATEWAY_ADDRESS",
        "DS_WEB_UI",
    ]


@pytest.mark.parametrize("version", [None, "", "auto", "AUTO"])
def test_target_config_defers_automatic_version_without_network(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, version: str | None
) -> None:
    env_file = tmp_path / "cluster.env"
    lines = ["DS_API_URL=http://file.test/dolphinscheduler/", "DS_API_TOKEN=file-token"]
    if version is not None:
        lines.append(f"DS_VERSION={version}")
    env_file.write_text("\n".join(lines), encoding="utf-8")
    monkeypatch.setenv("DS_VERSION", "3.4.2")

    settings = load_target_settings(env_file)

    assert settings.requested_version == ("auto" if version else None)
    assert settings.connection().api_url == "http://file.test/dolphinscheduler"
    assert settings.connection().api_token == "file-token"
    assert "file-token" not in repr(settings)
    with pytest.raises(ConfigError, match="requires DS version discovery"):
        load_profile(env_file)
    with pytest.raises(ConfigError, match="requires DS version discovery"):
        load_selected_ds_version(env_file)


def test_unconfigured_local_selection_keeps_explicit_offline_baseline() -> None:
    settings = load_target_settings()
    assert settings.requested_version is None
    assert load_selected_ds_version() == "3.4.1"
    with pytest.raises(ConfigError, match="DS_API_URL"):
        settings.connection()


@pytest.fixture(autouse=True)
def isolated_registry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.delenv("DSCTL_CONTEXT", raising=False)
    monkeypatch.delenv("DSCTL_ENV_FILE", raising=False)


def _named_target(
    tmp_path: Path, name: str = "production", *, default: bool = False
) -> NamedContext:
    env_file = tmp_path / f"{name}.env"
    env_file.write_text(
        f"DS_API_URL=https://{name}.test/dolphinscheduler/\nDS_API_TOKEN={name}-token\n",
        encoding="utf-8",
    )
    context = create_context(
        name,
        env_file=env_file,
        api_url=f"https://{name}.test/dolphinscheduler",
        project=f"{name}-project",
    )
    if default:
        set_default_context(name)
    return context


def test_named_context_resolves_file_and_bound_project(tmp_path: Path) -> None:
    context = _named_target(tmp_path)
    target = load_target_settings(context_name=context.name)
    assert target.source == "flag"
    assert target.context_name == "production"
    assert target.project == "production-project"
    assert target.env_file == context.env_file
    assert target.connection().api_url == context.api_url
    assert target.connection().api_token == "production-token"


def test_saved_default_is_used_when_no_higher_source_exists(tmp_path: Path) -> None:
    context = _named_target(tmp_path, default=True)
    target = load_target_settings()
    assert target.source == "default"
    assert target.context_name == context.name
    assert target.project == context.project


@pytest.mark.parametrize(
    "key",
    [
        "DS_VERSION",
        "DS_API_URL",
        "DS_API_TOKEN",
    ],
)
@pytest.mark.parametrize("value", ["", " "])
def test_any_present_raw_setting_shadows_default_without_merging(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, key: str, value: str
) -> None:
    _named_target(tmp_path, default=True)
    monkeypatch.setenv(key, value)
    target = load_target_settings()
    assert target.source == "environment"
    assert target.project is None
    assert target.context_name is None
    with pytest.raises(ConfigError) as exc:
        target.connection()
    assert exc.value.details["source"] == "environment"
    configured_keys = exc.value.details["configured_keys"]
    assert isinstance(configured_keys, list)
    assert key in configured_keys
    assert "unset all" in str(exc.value.suggestion)


@pytest.mark.parametrize(
    ("key", "value", "attribute", "expected"),
    [
        ("DS_API_RETRY_ATTEMPTS", "8", "api_retry_attempts", 8),
        ("DS_API_RETRY_BACKOFF_MS", "125", "api_retry_backoff_ms", 125),
        ("DS_API_TIMEOUT_SECONDS", "3.75", "api_timeout_seconds", 3.75),
    ],
)
def test_process_policy_overrides_default_without_selecting_a_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    key: str,
    value: str,
    attribute: str,
    expected: int | float,
) -> None:
    context = _named_target(tmp_path, default=True)
    monkeypatch.setenv(key, value)

    target = load_target_settings()

    assert target.source == "default"
    assert target.context_name == context.name
    assert target.api_url == context.api_url
    assert getattr(target, attribute) == expected


def test_process_policy_masks_lower_priority_invalid_profile_policy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    context = _named_target(tmp_path, default=True)
    context.env_file.write_text(
        "DS_API_URL=https://production.test/dolphinscheduler\n"
        "DS_API_TOKEN=production-token\n"
        "DS_API_TIMEOUT_SECONDS=invalid\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("DS_API_TIMEOUT_SECONDS", "4")

    target = load_target_settings()

    assert target.context_name == context.name
    assert target.api_timeout_seconds == 4


@pytest.mark.parametrize("value", ["0", "-1", "nan", "inf", "-inf", "bad"])
def test_timeout_policy_requires_a_finite_positive_number(
    tmp_path: Path, value: str
) -> None:
    env_file = tmp_path / "invalid-timeout.env"
    env_file.write_text(
        "DS_API_URL=https://target.test\n"
        "DS_API_TOKEN=token\n"
        f"DS_API_TIMEOUT_SECONDS={value}\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError) as exc_info:
        validate_context_file(env_file)

    assert exc_info.value.details["key"] == "DS_API_TIMEOUT_SECONDS"
    assert exc_info.value.suggestion == (
        "Set DS_API_TIMEOUT_SECONDS to a finite number greater than 0."
    )


@pytest.mark.parametrize("selector", ["DSCTL_CONTEXT", "DSCTL_ENV_FILE"])
def test_environment_selector_beats_raw_settings_and_saved_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, selector: str
) -> None:
    _named_target(tmp_path, default=True)
    selected = _named_target(tmp_path, "selected")
    monkeypatch.setenv(
        selector,
        selected.name if selector == "DSCTL_CONTEXT" else str(selected.env_file),
    )
    monkeypatch.setenv("DS_API_URL", "invalid")
    monkeypatch.setenv("DS_VERSION", "invalid")
    target = load_target_settings()
    assert target.source == "environment_selector"
    assert target.api_url == selected.api_url
    assert target.project == (selected.project if selector == "DSCTL_CONTEXT" else None)


def test_explicit_file_ignores_ambiguous_environment_and_corrupt_registry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    selected = _named_target(tmp_path)
    registry_path().write_text("invalid: [", encoding="utf-8")
    monkeypatch.setenv("DSCTL_CONTEXT", "missing")
    monkeypatch.setenv("DSCTL_ENV_FILE", "/missing")
    monkeypatch.setenv("DS_VERSION", "invalid")
    assert load_target_settings(selected.env_file).api_url == selected.api_url


def test_explicit_context_ignores_ambiguous_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    selected = _named_target(tmp_path)
    monkeypatch.setenv("DSCTL_CONTEXT", "missing")
    monkeypatch.setenv("DSCTL_ENV_FILE", "/missing")
    monkeypatch.setenv("DS_VERSION", "invalid")
    assert load_target_settings(context_name=selected.name).api_url == selected.api_url


def test_raw_environment_ignores_corrupt_registry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _named_target(tmp_path, default=True)
    registry_path().write_text("invalid: [", encoding="utf-8")
    monkeypatch.setenv("DS_API_URL", "https://raw.test")
    monkeypatch.setenv("DS_API_TOKEN", "raw-token")
    assert load_target_settings().source == "environment"


def test_explicit_selector_conflict_fails_before_reading_any_file(
    tmp_path: Path,
) -> None:
    with pytest.raises(ConfigError, match="mutually exclusive"):
        load_target_settings(tmp_path / "missing", context_name="missing")


def test_environment_selector_conflict_fails_before_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _named_target(tmp_path, default=True)
    monkeypatch.setenv("DSCTL_CONTEXT", "production")
    monkeypatch.setenv("DSCTL_ENV_FILE", "missing")
    with pytest.raises(ConfigError, match="mutually exclusive"):
        load_target_settings()


@pytest.mark.parametrize("selector", ["DSCTL_CONTEXT", "DSCTL_ENV_FILE"])
def test_blank_environment_selector_never_falls_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, selector: str
) -> None:
    _named_target(tmp_path, default=True)
    monkeypatch.setenv(selector, "")
    with pytest.raises(ConfigError, match="must not be blank"):
        load_target_settings()


def test_missing_selected_file_never_uses_default(tmp_path: Path) -> None:
    _named_target(tmp_path, default=True)
    with pytest.raises(ConfigError, match="Could not read selected env file"):
        load_target_settings(tmp_path / "missing.env")
    with pytest.raises(ConfigError, match="was not found"):
        load_target_settings(context_name="missing")


def test_token_rotation_works_but_url_change_requires_rebinding(tmp_path: Path) -> None:
    context = _named_target(tmp_path)
    context.env_file.write_text(
        "DS_API_URL=https://PRODUCTION.test:443/dolphinscheduler/\nDS_API_TOKEN=rotated\n",
        encoding="utf-8",
    )
    assert (
        load_target_settings(context_name=context.name).connection().api_token
        == "rotated"
    )
    context.env_file.write_text(
        "DS_API_URL=https://other.test/dolphinscheduler/\nDS_API_TOKEN=rotated\n",
        encoding="utf-8",
    )
    with pytest.raises(ConfigError, match="different API URL") as exc:
        load_target_settings(context_name=context.name)
    assert exc.value.details["reason"] == "context_api_url_changed"
    assert "dsctl context update production --file" in str(exc.value.suggestion)


def test_scope_captures_process_environment_once_before_first_resolution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DS_API_URL", "https://original.test")
    monkeypatch.setenv("DS_API_TOKEN", "original-token")
    monkeypatch.setenv("DS_API_TIMEOUT_SECONDS", "2")
    with configuration_scope():
        monkeypatch.setenv("DS_API_URL", "https://changed.test")
        monkeypatch.setenv("DS_API_TIMEOUT_SECONDS", "8")
        selected = load_target_settings()
        assert selected.api_url == "https://original.test"
        assert selected.api_timeout_seconds == 2
    assert load_target_settings().api_url == "https://changed.test"
    assert load_target_settings().api_timeout_seconds == 8


def test_scope_freezes_named_target_and_token_for_retries_and_readback(
    tmp_path: Path,
) -> None:
    context = _named_target(tmp_path)
    with configuration_scope(context_name=context.name):
        original = load_target_settings()
        context.env_file.write_text(
            "DS_API_URL=https://other.test\nDS_API_TOKEN=other-token", encoding="utf-8"
        )
        assert load_target_settings() is original
        assert load_target_settings().connection().api_token == "production-token"
        assert load_target_settings().project == "production-project"
    with pytest.raises(ConfigError, match="different API URL"):
        load_target_settings(context_name=context.name)


def test_scope_caches_selected_source_errors_until_next_invocation(
    tmp_path: Path,
) -> None:
    env_file = tmp_path / "missing.env"
    with configuration_scope():
        with pytest.raises(ConfigError) as first:
            load_target_settings(env_file)
        env_file.write_text("DS_VERSION=3.4.1", encoding="utf-8")
        with pytest.raises(ConfigError) as second:
            load_target_settings(env_file)
        assert first.value is second.value
    assert load_target_settings(env_file).requested_version == "3.4.1"


def test_scope_is_lazy_so_config_write_can_read_back_new_default(
    tmp_path: Path,
) -> None:
    context = _named_target(tmp_path)
    with configuration_scope():
        set_default_context(context.name)
        assert load_target_settings().context_name == context.name


def test_scoped_context_and_explicit_file_conflict(tmp_path: Path) -> None:
    context = _named_target(tmp_path)
    with (
        configuration_scope(context_name=context.name),
        pytest.raises(ConfigError, match="mutually exclusive"),
    ):
        load_target_settings(context.env_file)


def test_registration_validation_is_independent_and_complete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    context = _named_target(tmp_path)
    monkeypatch.setenv("DSCTL_CONTEXT", "missing")
    with configuration_scope(context_name="missing"):
        assert validate_context_file(context.env_file).api_url == context.api_url
    context.env_file.write_text("DS_API_URL=https://production.test", encoding="utf-8")
    with pytest.raises(ConfigError, match="DS_API_TOKEN"):
        validate_context_file(context.env_file)


@pytest.mark.parametrize(
    "contents",
    [
        "DS_API_URL=relative\nDS_API_TOKEN=token",
        "DS_API_URL=https://valid.test\nDS_API_TOKEN=  ",
        "DS_API_URL=https://valid.test\nDS_API_TOKEN=token\nDS_VERSION=bad",
        "DS_API_URL=https://valid.test\nDS_API_TOKEN=token\nDS_API_RETRY_ATTEMPTS=0",
        "DS_API_URL=https://valid.test\nDS_API_TOKEN=token\nDS_API_RETRY_BACKOFF_MS=bad",
        "DS_API_URL=https://valid.test\nDS_API_TOKEN=token\nINVALID_LINE",
        "DS_API_URL=https://valid.test\nDS_API_TOKEN=token\nworkflow=unsupported",
    ],
)
def test_registration_rejects_invalid_connection_settings(
    tmp_path: Path, contents: str
) -> None:
    env_file = tmp_path / "invalid.env"
    env_file.write_text(contents, encoding="utf-8")
    with pytest.raises(ConfigError):
        validate_context_file(env_file)


def test_scope_supplies_explicit_file_to_consumers_without_file_argument(
    tmp_path: Path,
) -> None:
    context = _named_target(tmp_path, default=True)
    with configuration_scope(env_file=context.env_file):
        selected = load_target_settings()
        assert selected.env_file == context.env_file
        assert selected.context_name is None
        assert selected.project is None
        assert load_target_settings(context.env_file) is selected
        context.env_file.write_text("DS_VERSION=1.3.9", encoding="utf-8")
        assert load_target_settings() is selected
        assert load_target_settings().api_token == "production-token"


def test_explicit_function_file_takes_precedence_over_ambient_file(
    tmp_path: Path,
) -> None:
    first = _named_target(tmp_path)
    second = _named_target(tmp_path, "second")
    with configuration_scope(env_file=first.env_file):
        assert load_target_settings(second.env_file).api_url == second.api_url
        assert load_target_settings().api_url == first.api_url


@pytest.mark.parametrize("source", ["flag", "environment", "registration"])
def test_unknown_home_user_is_a_selected_source_error_without_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, source: str
) -> None:
    _named_target(tmp_path, default=True)
    invalid = "~__dsctl_no_such_account_92718__/cluster.env"
    operation: Callable[[], object]
    if source == "environment":
        monkeypatch.setenv("DSCTL_ENV_FILE", invalid)
        operation = load_target_settings
    elif source == "registration":
        operation = partial(validate_context_file, invalid)
    else:
        operation = partial(load_target_settings, invalid)
    with pytest.raises(ConfigError, match="Could not resolve") as error:
        operation()
    assert error.value.details == {"operation": "resolve", "path": invalid}
    assert "absolute path" in str(error.value.suggestion)


@pytest.mark.parametrize("exception_type", [OSError, RuntimeError])
def test_selected_path_absolute_failure_is_structured(
    monkeypatch: pytest.MonkeyPatch, exception_type: type[Exception]
) -> None:
    def fail_absolute(self: Path) -> Path:
        raise exception_type

    monkeypatch.setattr(Path, "absolute", fail_absolute)
    with pytest.raises(ConfigError, match="Could not resolve"):
        load_target_settings("cluster.env")
    with pytest.raises(ConfigError, match="Could not resolve"):
        validate_context_file("cluster.env")


def test_default_write_survives_unresolvable_environment_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    context = _named_target(tmp_path)
    monkeypatch.setenv("DSCTL_ENV_FILE", "~__dsctl_no_such_account_92718__/cluster.env")
    result = set_config_result("default-context", context.name)
    assert result.data == {"key": "default-context", "value": context.name}
    assert result.resolved["saved"] is True
    assert result.resolved["effective"] is None
    assert result.warnings
    error = result.warning_details[0]["error"]
    assert isinstance(error, dict)
    assert error["type"] == "config_error"
    assert get_default_context() == context.name
