from __future__ import annotations

import os
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
import yaml

from dsctl.context import (
    ContextRegistry,
    create_context,
    delete_context,
    get_context,
    get_default_context,
    list_contexts,
    load_registry,
    normalize_api_url,
    registry_path,
    set_default_context,
    unset_default_context,
    update_context,
)
from dsctl.errors import ConfigError

if TYPE_CHECKING:
    from dsctl.context import NamedContext


@pytest.fixture(autouse=True)
def registry_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))


def _register(
    tmp_path: Path, name: str = "production", project: str | None = "etl"
) -> NamedContext:
    return create_context(
        name,
        env_file=tmp_path / f"{name}.env",
        api_url="https://production.test/dolphinscheduler/",
        project=project,
    )


def test_registry_location_uses_only_user_configuration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert registry_path() == tmp_path / "config" / "dsctl" / "config.yaml"
    monkeypatch.delenv("XDG_CONFIG_HOME")
    assert registry_path() == tmp_path / "home" / ".config" / "dsctl" / "config.yaml"


def test_legacy_and_cwd_files_never_supply_registry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".dsctl-context.yaml").write_text("invalid: [", encoding="utf-8")
    legacy = registry_path().with_name("context.yaml")
    legacy.parent.mkdir(parents=True)
    legacy.write_text("project: old\nworkflow: old", encoding="utf-8")
    assert load_registry() == ContextRegistry()


def test_create_read_list_and_delete_references(tmp_path: Path) -> None:
    created = _register(tmp_path)
    assert created.env_file.is_absolute()
    assert created.api_url == "https://production.test/dolphinscheduler"
    assert created.project == "etl"
    assert get_context("production") == created
    assert list_contexts() == (created,)
    assert get_default_context() is None
    assert delete_context("production") == created
    assert list_contexts() == ()


def test_store_does_not_open_or_copy_connection_secrets(tmp_path: Path) -> None:
    env_file = tmp_path / "production.env"
    env_file.write_text("DS_API_TOKEN=secret-token", encoding="utf-8")
    _register(tmp_path)
    document = registry_path().read_text(encoding="utf-8")
    assert "secret-token" not in document
    assert "token" not in document
    env_file.unlink()
    assert get_context("production").env_file == env_file


def test_registry_restricts_file_and_directory_permissions(tmp_path: Path) -> None:
    _register(tmp_path)
    path = registry_path()
    assert path.stat().st_mode & 0o777 == 0o600
    assert path.with_suffix(".lock").stat().st_mode & 0o777 == 0o600
    assert path.parent.stat().st_mode & 0o777 == 0o700


def test_relative_context_file_is_saved_absolute(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    created = create_context(
        "relative", env_file=Path("cluster.env"), api_url="https://production.test"
    )
    assert created.env_file == tmp_path / "cluster.env"


def test_duplicate_create_fails_without_replacing_existing(tmp_path: Path) -> None:
    created = _register(tmp_path)
    with pytest.raises(ConfigError, match="already exists"):
        _register(tmp_path, project="other")
    assert get_context("production") == created


def test_unknown_name_never_falls_back_to_default(tmp_path: Path) -> None:
    _register(tmp_path)
    set_default_context("production")
    with pytest.raises(ConfigError, match="was not found"):
        get_context("missing")
    with pytest.raises(ConfigError, match="was not found"):
        set_default_context("missing")
    assert get_default_context() == "production"


def test_default_delete_requires_explicit_unset(tmp_path: Path) -> None:
    _register(tmp_path)
    set_default_context("production")
    with pytest.raises(ConfigError, match="saved default") as exc:
        delete_context("production")
    assert "dsctl config unset default-context" in str(exc.value.suggestion)
    assert get_default_context() == "production"
    unset_default_context()
    delete_context("production")
    assert load_registry() == ContextRegistry()


def test_project_updates_preserve_file_and_clear_explicitly(tmp_path: Path) -> None:
    created = _register(tmp_path)
    updated = update_context("production", project="new-project")
    assert updated.env_file == created.env_file
    assert updated.api_url == created.api_url
    assert updated.project == "new-project"
    assert update_context("production").project == "new-project"
    assert update_context("production", clear_project=True).project is None


def test_file_update_clears_previous_project_even_for_same_url(tmp_path: Path) -> None:
    created = _register(tmp_path)
    updated = update_context(
        "production", env_file=created.env_file, api_url=created.api_url
    )
    assert updated.project is None
    assert (
        update_context(
            "production",
            env_file=tmp_path / "other.env",
            api_url="https://other.test",
            project="new-project",
        ).project
        == "new-project"
    )


def test_invalid_update_leaves_registry_unchanged(tmp_path: Path) -> None:
    created = _register(tmp_path)
    with pytest.raises(ConfigError, match="mutually exclusive"):
        update_context("production", project="new", clear_project=True)
    with pytest.raises(ConfigError, match="validated API URL"):
        update_context("production", env_file=tmp_path / "other.env")
    with pytest.raises(ConfigError, match="nonblank"):
        update_context("production", project=" ")
    assert get_context("production") == created


@pytest.mark.parametrize(
    "document",
    [
        "[",
        "[]",
        "contexts: []",
        "contexts: {}\nextra: secret",
        "contexts: {}\ncontexts: {}",
        "contexts: {123: {}}",
        "contexts: {production: {env_file: relative.env, api_url: 'https://test'}}",
        "contexts: {production: {env_file: /env, "
        "api_url: 'https://test', workflow: bad}}",
        "contexts: {production: {env_file: /env, "
        "api_url: 'https://test', token: secret}}",
        "contexts: {production: {env_file: /env, api_url: false}}",
        "contexts: {production: {env_file: /env, "
        "api_url: 'https://test', project: []}}",
        "contexts: {}\ndefault_context: missing",
        "contexts: {}\ndefault_context: false",
        "contexts: {'': {env_file: /env, api_url: 'https://test'}}",
    ],
)
def test_malformed_registry_fails_without_rewriting(
    tmp_path: Path, document: str
) -> None:
    path = registry_path()
    path.parent.mkdir(parents=True)
    path.write_text(document, encoding="utf-8")
    with pytest.raises(ConfigError):
        load_registry()
    with pytest.raises(ConfigError):
        _register(tmp_path)
    assert path.read_text(encoding="utf-8") == document


def test_atomic_replace_failure_preserves_old_registry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    created = _register(tmp_path)
    before = registry_path().read_bytes()

    def fail_replace(self: Path, target: Path) -> Path:
        raise OSError

    monkeypatch.setattr(Path, "replace", fail_replace)
    with pytest.raises(ConfigError, match="Could not write"):
        update_context("production", project="new-project")
    assert registry_path().read_bytes() == before
    assert get_context("production") == created
    assert not list(registry_path().parent.glob("*.tmp"))


def _concurrent_create(item: tuple[str, int]) -> str:
    config_home, worker = item
    os.environ["XDG_CONFIG_HOME"] = config_home
    for count in range(6):
        name = f"worker-{worker}-{count}"
        create_context(
            name, env_file=Path(config_home) / "cluster.env", api_url="https://test"
        )
    return str(worker)


def test_concurrent_process_updates_do_not_lose_contexts(tmp_path: Path) -> None:
    config_home = str(tmp_path / "config")
    with ProcessPoolExecutor(max_workers=4) as executor:
        assert (
            len(
                list(
                    executor.map(
                        _concurrent_create, [(config_home, n) for n in range(4)]
                    )
                )
            )
            == 4
        )
    assert len(load_registry().contexts) == 24
    assert isinstance(yaml.safe_load(registry_path().read_text()), dict)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (
            "HTTPS://EXAMPLE.TEST:443/dolphinscheduler/",
            "https://example.test/dolphinscheduler",
        ),
        ("http://EXAMPLE.test:80/", "http://example.test"),
        ("https://[::1]:8443/dolphinscheduler/", "https://[::1]:8443/dolphinscheduler"),
    ],
)
def test_url_binding_normalizes_equivalent_targets(value: str, expected: str) -> None:
    assert normalize_api_url(value) == expected


@pytest.mark.parametrize(
    "value",
    [
        "",
        "relative/path",
        "ftp://host",
        "https://host:bad",
        "https://user:secret@host",
        "https://host?token=secret",
        "https://host/#fragment",
    ],
)
def test_url_binding_rejects_invalid_or_secret_bearing_urls(value: str) -> None:
    with pytest.raises(ConfigError, match="absolute HTTP") as exc:
        normalize_api_url(value)
    assert "secret" not in str(exc.value)
    assert "secret" not in str(exc.value.details)


def test_unknown_home_user_in_context_file_path_is_structured(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="Could not resolve"):
        create_context(
            "production",
            env_file="~__dsctl_no_such_account_92718__/cluster.env",
            api_url="https://production.test",
        )
    assert not registry_path().exists()


def test_unknown_home_user_in_config_directory_is_structured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", "~__dsctl_no_such_account_92718__/config")
    with pytest.raises(ConfigError, match="Could not resolve") as error:
        load_registry()
    assert error.value.details["operation"] == "resolve"


def test_unavailable_home_directory_is_structured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("HOME")
    monkeypatch.delenv("XDG_CONFIG_HOME")

    def fail_home(cls: type[Path]) -> Path:
        raise RuntimeError

    monkeypatch.setattr(Path, "home", classmethod(fail_home))
    with pytest.raises(ConfigError, match="Could not resolve") as error:
        registry_path()
    assert error.value.details == {"operation": "resolve", "key": "HOME"}


def test_explicit_config_directory_does_not_require_home(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("HOME")

    def fail_home(cls: type[Path]) -> Path:
        raise RuntimeError

    monkeypatch.setattr(Path, "home", classmethod(fail_home))
    assert registry_path() == tmp_path / "config" / "dsctl" / "config.yaml"


def test_null_byte_in_saved_connection_file_path_is_structured() -> None:
    path = registry_path()
    path.parent.mkdir(parents=True)
    path.write_text(
        yaml.safe_dump(
            {
                "contexts": {
                    "production": {
                        "env_file": "/invalid\0path.env",
                        "api_url": "https://test",
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ConfigError, match="Could not resolve"):
        load_registry()
