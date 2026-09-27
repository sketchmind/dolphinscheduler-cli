from __future__ import annotations

import json
import os
import time
from typing import TYPE_CHECKING

import pytest

from dsctl.errors import ConfigError
from dsctl.services import version_resolution as resolution
from dsctl.upstream.version_discovery import DiscoveredVersion, VersionDiscoveryError

if TYPE_CHECKING:
    from pathlib import Path

    from dsctl.config import ConnectionSettings


@pytest.fixture(autouse=True)
def isolated_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.setattr(time, "time", lambda: 1_000.0)


def _configure(monkeypatch: pytest.MonkeyPatch, *, version: str | None = None) -> None:
    monkeypatch.setenv("DS_API_URL", "https://ds.example.test/dolphinscheduler")
    monkeypatch.setenv("DS_API_TOKEN", "private-test-credential")
    if version is not None:
        monkeypatch.setenv("DS_VERSION", version)


def _probe(
    monkeypatch: pytest.MonkeyPatch, *, version: str = "3.4.2"
) -> list[ConnectionSettings]:
    calls: list[ConnectionSettings] = []

    def discover(connection: ConnectionSettings) -> DiscoveredVersion:
        calls.append(connection)
        return DiscoveredVersion(version=version, source="product_info")

    monkeypatch.setattr(resolution, "discover_target", discover)
    return calls


def _forbid_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    def discover(_connection: ConnectionSettings) -> DiscoveredVersion:
        pytest.fail("This resolution must not contact the target")

    monkeypatch.setattr(resolution, "discover_target", discover)


def _cache_file(tmp_path: Path) -> Path:
    paths = list((tmp_path / "cache" / "dsctl" / "version-discovery").glob("*.json"))
    assert len(paths) == 1
    return paths[0]


@pytest.mark.parametrize("mode", ["local", "runtime", "refresh"])
def test_explicit_exact_version_needs_no_connection_or_probe(
    monkeypatch: pytest.MonkeyPatch, mode: resolution.ResolutionMode
) -> None:
    monkeypatch.setenv("DS_VERSION", "v3.2.1")
    _forbid_probe(monkeypatch)

    selected = resolution.resolve_version(mode=mode)

    assert selected == resolution.VersionResolution("3.2.1", "explicit")


def test_explicit_unknown_version_fails_without_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DS_VERSION", "3.4.99")
    _forbid_probe(monkeypatch)
    with pytest.raises(ConfigError, match="Unsupported DS version") as error:
        resolution.resolve_version(mode="runtime")
    assert error.value.suggestion


@pytest.mark.parametrize("mode", ["local", "runtime", "refresh"])
def test_only_a_completely_unconfigured_target_uses_offline_default(
    monkeypatch: pytest.MonkeyPatch,
    mode: resolution.ResolutionMode,
) -> None:
    _forbid_probe(monkeypatch)
    assert resolution.resolve_version(mode=mode) == resolution.VersionResolution(
        "3.4.1", "offline_default"
    )


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("DS_VERSION", "auto"),
        ("DS_API_URL", "https://ds.example.test"),
        ("DS_API_TOKEN", "credential"),
    ],
)
def test_partial_or_auto_target_never_falls_back_to_stable(
    monkeypatch: pytest.MonkeyPatch, key: str, value: str
) -> None:
    monkeypatch.setenv(key, value)
    _forbid_probe(monkeypatch)
    with pytest.raises(ConfigError) as error:
        resolution.resolve_version()
    assert error.value.details["reason"] == "version_not_resolved"
    assert error.value.suggestion is not None
    assert "doctor" in error.value.suggestion
    assert "DS_VERSION" in error.value.suggestion


def test_configured_local_target_without_cache_does_not_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure(monkeypatch)
    _forbid_probe(monkeypatch)
    with pytest.raises(ConfigError, match="not been resolved locally"):
        resolution.resolve_version()


def test_unconfigured_remote_target_does_not_use_offline_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _forbid_probe(monkeypatch)
    with pytest.raises(ConfigError):
        resolution.resolve_profile()


def test_runtime_builds_exact_profile_and_preserves_connection_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure(monkeypatch, version="auto")
    monkeypatch.setenv("DS_API_RETRY_ATTEMPTS", "2")
    monkeypatch.setenv("DS_API_RETRY_BACKOFF_MS", "17")
    monkeypatch.setenv("DS_API_TIMEOUT_SECONDS", "6.5")
    calls = _probe(monkeypatch, version="1.3.9")

    profile = resolution.resolve_profile()

    assert profile.ds_version == "1.3.9"
    assert profile.api_retry_attempts == 2
    assert profile.api_retry_backoff_ms == 17
    assert profile.api_timeout_seconds == 6.5
    assert calls[0].api_timeout_seconds == 6.5
    assert len(calls) == 1
    assert not hasattr(calls[0], "ds_version")


def test_observation_cache_is_private_and_preserves_provenance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure(monkeypatch)
    calls = _probe(monkeypatch)
    observed = resolution.resolve_version(mode="runtime")
    path = _cache_file(tmp_path)
    content = path.read_text()
    assert "private-test-credential" not in content
    assert "ds.example.test" not in content
    assert path.stat().st_mode & 0o777 == 0o600
    assert observed == resolution.VersionResolution(
        "3.4.2", "probe", 1_000.0, "product_info"
    )
    _forbid_probe(monkeypatch)

    cached = resolution.resolve_version()

    assert cached == resolution.VersionResolution(
        "3.4.2", "cache", 1_000.0, "product_info"
    )
    assert len(calls) == 1


@pytest.mark.parametrize("now", [1_300.0, 1_301.0, 999.0])
def test_expired_and_future_cache_observations_are_not_used(
    monkeypatch: pytest.MonkeyPatch,
    now: float,
) -> None:
    _configure(monkeypatch)
    _probe(monkeypatch)
    resolution.resolve_version(mode="runtime")
    monkeypatch.setattr(time, "time", lambda: now)
    _forbid_probe(monkeypatch)
    with pytest.raises(ConfigError, match="not been resolved locally"):
        resolution.resolve_version()


def test_cache_remains_valid_until_the_exact_ttl_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure(monkeypatch)
    _probe(monkeypatch)
    resolution.resolve_version(mode="runtime")
    monkeypatch.setattr(time, "time", lambda: 1_299.999)
    _forbid_probe(monkeypatch)
    assert resolution.resolve_version().source == "cache"


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("version", "3.4.99"),
        ("checked_at", None),
        ("checked_at", True),
        ("checked_at", "1000"),
        ("checked_at", float("nan")),
        ("checked_at", float("inf")),
        ("checked_at", 10**400),
        ("schema_version", True),
        ("schema_version", 2),
        ("target_key", "wrong"),
        ("evidence", None),
    ],
)
def test_invalid_cache_records_cannot_choose_a_profile(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    key: str,
    value: object,
) -> None:
    _configure(monkeypatch)
    _probe(monkeypatch)
    resolution.resolve_version(mode="runtime")
    path = _cache_file(tmp_path)
    payload = json.loads(path.read_text())
    payload[key] = value
    path.write_text(json.dumps(payload))
    _forbid_probe(monkeypatch)
    with pytest.raises(ConfigError, match="not been resolved locally"):
        resolution.resolve_version()


@pytest.mark.parametrize("content", ["{", "[]", "null"])
def test_malformed_cache_is_a_local_miss(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    content: str,
) -> None:
    _configure(monkeypatch)
    _probe(monkeypatch)
    resolution.resolve_version(mode="runtime")
    _cache_file(tmp_path).write_text(content)
    _forbid_probe(monkeypatch)
    with pytest.raises(ConfigError):
        resolution.resolve_version()


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("DS_API_URL", "https://other.example.test/dolphinscheduler"),
        ("DS_API_URL", "https://ds.example.test/other-context"),
        ("DS_API_TOKEN", "different-credential"),
    ],
)
def test_cache_is_scoped_to_url_context_and_credentials(
    monkeypatch: pytest.MonkeyPatch,
    key: str,
    value: str,
) -> None:
    _configure(monkeypatch)
    _probe(monkeypatch)
    resolution.resolve_version(mode="runtime")
    monkeypatch.setenv(key, value)
    _forbid_probe(monkeypatch)
    with pytest.raises(ConfigError):
        resolution.resolve_version()


def test_equivalent_urls_share_the_same_discovery_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure(monkeypatch)
    _probe(monkeypatch)
    resolution.resolve_version(mode="runtime")
    monkeypatch.setenv("DS_API_URL", "https://DS.EXAMPLE.TEST:443/dolphinscheduler/")
    _forbid_probe(monkeypatch)
    assert resolution.resolve_version().version == "3.4.2"


def test_each_remote_invocation_ignores_a_fresh_persistent_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure(monkeypatch)
    calls = _probe(monkeypatch)
    for mode in ("runtime", "refresh", "runtime"):
        with resolution.invocation_scope():
            assert resolution.resolve_version(mode=mode).source == "probe"
    assert len(calls) == 3


def test_one_invocation_shares_the_probe_with_local_and_runtime_consumers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure(monkeypatch)
    calls = _probe(monkeypatch)
    with resolution.invocation_scope():
        selected = resolution.resolve_version(mode="runtime")
        assert resolution.resolve_version(mode="refresh") is selected
        assert resolution.resolve_version() is selected
        assert resolution.resolve_profile().ds_version == selected.version
    assert len(calls) == 1


def test_one_invocation_freezes_profile_inputs_and_releases_them_afterward(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DS_VERSION", "3.4.1")
    _forbid_probe(monkeypatch)
    with resolution.invocation_scope():
        assert resolution.resolve_version().version == "3.4.1"
        monkeypatch.setenv("DS_VERSION", "1.3.9")
        assert resolution.resolve_version(mode="runtime").version == "3.4.1"
    assert resolution.resolve_version().version == "1.3.9"


def test_authoritative_env_file_does_not_merge_inherited_version(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure(monkeypatch, version="3.4.1")
    selected = tmp_path / "cluster.env"
    selected.write_text(
        "DS_API_URL=https://file.example.test\nDS_API_TOKEN=file-token\n"
    )
    calls = _probe(monkeypatch, version="2.0.9")
    profile = resolution.resolve_profile(selected)
    assert profile.ds_version == "2.0.9"
    assert calls[0].api_url == "https://file.example.test"
    assert calls[0].api_token == "file-token"


def test_cache_then_conflicting_probe_cannot_mix_versions_in_one_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure(monkeypatch)
    _probe(monkeypatch, version="3.4.1")
    resolution.resolve_version(mode="runtime")
    calls = _probe(monkeypatch, version="3.4.2")
    with resolution.invocation_scope():
        assert resolution.resolve_version().version == "3.4.1"
        with pytest.raises(ConfigError) as error:
            resolution.resolve_profile()
        assert error.value.details["reason"] == "version_changed_during_invocation"
        with pytest.raises(ConfigError) as local_error:
            resolution.resolve_version()
        assert local_error.value is error.value
    assert len(calls) == 1


def test_atomic_cache_write_failure_does_not_break_successful_resolution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure(monkeypatch)
    _probe(monkeypatch, version="3.4.1")
    resolution.resolve_version(mode="runtime")
    path = _cache_file(tmp_path)
    previous = path.read_bytes()
    _probe(monkeypatch, version="3.4.2")

    def cannot_replace(_source: Path, _target: Path) -> None:
        msg = "simulated filesystem failure"
        raise OSError(msg)

    monkeypatch.setattr(os, "replace", cannot_replace)
    assert resolution.resolve_version(mode="runtime").version == "3.4.2"
    assert path.read_bytes() == previous
    assert list(path.parent.iterdir()) == [path]


def test_atomic_replacement_sees_complete_private_cache_contents(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure(monkeypatch)
    _probe(monkeypatch)
    replace = os.replace
    observed: list[str] = []

    def check_replace(source: Path, target: Path) -> None:
        assert source.parent == target.parent
        assert source.stat().st_mode & 0o777 == 0o600
        observed.append(json.loads(source.read_text())["version"])
        replace(source, target)

    monkeypatch.setattr(os, "replace", check_replace)
    resolution.resolve_version(mode="runtime")
    assert observed == ["3.4.2"]


@pytest.mark.parametrize(
    ("reason", "suggestion"),
    [
        (
            "authentication_failed",
            "Check that DS_API_TOKEN is valid and unexpired, and that its account "
            "is enabled.",
        ),
        (
            "transport_failed",
            "Check the target connection, or set DS_VERSION explicitly.",
        ),
        (
            "unavailable",
            "Check the target connection, or set DS_VERSION explicitly.",
        ),
        (
            "unsupported_version",
            "Check the target connection, or set DS_VERSION explicitly.",
        ),
    ],
)
def test_failed_remote_probe_never_uses_the_old_cache_in_that_invocation(
    monkeypatch: pytest.MonkeyPatch,
    reason: str,
    suggestion: str,
) -> None:
    _configure(monkeypatch)
    _probe(monkeypatch)
    resolution.resolve_version(mode="runtime")
    calls: list[ConnectionSettings] = []

    def fail(connection: ConnectionSettings) -> DiscoveredVersion:
        calls.append(connection)
        raise VersionDiscoveryError(
            reason, probes=({"source": "product_info", "reason": reason},)
        )

    monkeypatch.setattr(resolution, "discover_target", fail)
    with resolution.invocation_scope():
        with pytest.raises(ConfigError) as error:
            resolution.resolve_version(mode="runtime")
        assert error.value.details == {
            "reason": reason,
            "probes": [{"source": "product_info", "reason": reason}],
        }
        assert error.value.suggestion == suggestion
        for mode in ("local", "runtime", "refresh"):
            with pytest.raises(ConfigError) as repeated:
                resolution.resolve_version(mode=mode)
            assert repeated.value is error.value
    assert len(calls) == 1


def test_unsupported_observation_is_not_cached_or_bound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure(monkeypatch)
    calls = _probe(monkeypatch, version="3.4.99")
    with resolution.invocation_scope():
        for _ in range(2):
            with pytest.raises(ConfigError, match="Unsupported DS version"):
                resolution.resolve_profile()
    assert len(calls) == 1
    assert not list((tmp_path / "cache").rglob("*.json"))


def test_local_cache_miss_does_not_prevent_a_later_fresh_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure(monkeypatch)
    calls = _probe(monkeypatch)
    with resolution.invocation_scope():
        with pytest.raises(ConfigError):
            resolution.resolve_version()
        selected = resolution.resolve_version(mode="runtime")
        assert resolution.resolve_version() is selected
    assert len(calls) == 1


def test_configuration_failure_is_read_only_once_in_one_invocation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DS_API_RETRY_ATTEMPTS", "invalid")
    with resolution.invocation_scope():
        with pytest.raises(ConfigError) as original:
            resolution.resolve_version()
        monkeypatch.setenv("DS_API_RETRY_ATTEMPTS", "2")
        with pytest.raises(ConfigError) as repeated:
            resolution.resolve_version(mode="runtime")
        assert repeated.value is original.value
    assert resolution.resolve_version().source == "offline_default"


def test_distinct_env_files_do_not_share_an_invocation_resolution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _forbid_probe(monkeypatch)
    first = tmp_path / "first.env"
    second = tmp_path / "second.env"
    first.write_text("DS_VERSION=1.3.9\n")
    second.write_text("DS_VERSION=3.4.2\n")
    with resolution.invocation_scope():
        assert resolution.resolve_version(first).version == "1.3.9"
        assert resolution.resolve_version(second).version == "3.4.2"


def test_nested_scope_reuses_outer_result_and_cleans_up_after_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure(monkeypatch)
    calls = _probe(monkeypatch)

    def abort() -> None:
        with resolution.invocation_scope():
            selected = resolution.resolve_version(mode="runtime")
            with resolution.invocation_scope():
                assert resolution.resolve_version(mode="runtime") is selected
            message = "abort the command"
            raise RuntimeError(message)

    with pytest.raises(RuntimeError):
        abort()
    resolution.resolve_version(mode="runtime")
    assert len(calls) == 2
