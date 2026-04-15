from pathlib import Path

import pytest

from dsctl.config import load_profile, load_selected_ds_version
from dsctl.errors import ConfigError


def test_load_profile_from_env_file(tmp_path: Path) -> None:
    env_file = tmp_path / "cluster.env"
    env_file.write_text(
        (
            "DS_API_URL=http://example.test/dolphinscheduler\n"
            "DS_API_TOKEN=token-value\n"
            "DS_API_RETRY_ATTEMPTS=5\n"
            "DS_API_RETRY_BACKOFF_MS=50"
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


def test_profile_redaction_masks_secrets(tmp_path: Path) -> None:
    env_file = tmp_path / "cluster.env"
    env_file.write_text(
        (
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
            'export DS_API_URL="http://example.test/dolphinscheduler"\n'
            "export DS_API_TOKEN='quoted-token'"
        ),
        encoding="utf-8",
    )

    profile = load_profile(env_file)

    assert profile.api_url == "http://example.test/dolphinscheduler"
    expected_token = "-".join(["quoted", "token"])
    assert profile.api_token == expected_token


def test_load_profile_prefers_process_environment_over_env_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env_file = tmp_path / "cluster.env"
    env_file.write_text(
        ("DS_API_URL=http://file.test/dolphinscheduler\nDS_API_TOKEN=file-token\n"),
        encoding="utf-8",
    )
    monkeypatch.setenv("DS_API_URL", "http://env.test/dolphinscheduler/")
    monkeypatch.setenv("DS_API_TOKEN", "env-token")

    profile = load_profile(env_file)

    assert profile.api_url == "http://env.test/dolphinscheduler"
    expected_token = "-".join(["env", "token"])
    assert profile.api_token == expected_token
    assert profile.health_url == "http://env.test/dolphinscheduler/actuator/health"


def test_load_profile_accepts_and_normalizes_ds_version(tmp_path: Path) -> None:
    env_file = tmp_path / "cluster.env"
    env_file.write_text(
        (
            "DS_VERSION=ds_3_4_1\n"
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
        ("DS_API_URL=http://file.test/dolphinscheduler\nDS_API_TOKEN=file-token\n"),
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
    assert exc_info.value.suggestion == (
        "Set DS_API_URL in the environment or provide it through --env-file."
    )


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
        "Remove those keys from the profile. Use `dsctl use project` for local "
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
