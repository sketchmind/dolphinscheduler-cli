from __future__ import annotations

from itertools import count
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from tests.live.support import (
    LiveBootstrapState,
    LiveProfileConfig,
    LiveSettings,
    create_live_run_prefix,
    future_expire_time,
    live_admin_tests_enabled,
    live_tests_enabled,
    load_live_settings,
    require_mapping,
    require_ok_payload,
    run_dsctl,
    write_profile_env,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator


def pytest_runtest_setup(item: pytest.Item) -> None:
    """Skip live tests unless the explicit enable flags are present."""
    if "live" in item.keywords and not live_tests_enabled():
        pytest.skip("Set DSCTL_RUN_LIVE_TESTS=1 to enable real-cluster live tests.")
    if "live_admin" in item.keywords and not live_admin_tests_enabled():
        pytest.skip(
            "Set DSCTL_RUN_LIVE_ADMIN_TESTS=1 to enable admin-scoped live tests."
        )


def pytest_report_header(config: pytest.Config) -> list[str]:
    """Report the live-test opt-in state in pytest headers."""
    return [
        f"live tests enabled: {live_tests_enabled()}",
        f"live admin tests enabled: {live_admin_tests_enabled()}",
    ]


@pytest.fixture(scope="session")
def live_repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


@pytest.fixture(scope="session")
def live_workspace(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("live")


@pytest.fixture(scope="session")
def live_run_prefix() -> str:
    return create_live_run_prefix()


@pytest.fixture(scope="session")
def live_settings() -> LiveSettings:
    return load_live_settings()


@pytest.fixture(scope="session")
def live_admin_profile(live_settings: LiveSettings) -> LiveProfileConfig:
    if live_settings.admin is None:
        pytest.skip(
            "Configure DS_LIVE_API_URL and DS_LIVE_ADMIN_TOKEN, or "
            "DS_LIVE_ADMIN_ENV_FILE, to run admin-backed live tests."
        )
    return live_settings.admin


@pytest.fixture(scope="session")
def live_admin_env_file(
    live_workspace: Path,
    live_admin_profile: LiveProfileConfig,
) -> Path:
    return write_profile_env(live_workspace / "admin.env", live_admin_profile)


@pytest.fixture(scope="session")
def live_existing_etl_profile(
    live_settings: LiveSettings,
) -> LiveProfileConfig | None:
    return live_settings.etl


@pytest.fixture(scope="session")
def live_existing_etl_env_file(
    live_workspace: Path,
    live_existing_etl_profile: LiveProfileConfig | None,
) -> Path | None:
    if live_existing_etl_profile is None:
        return None
    return write_profile_env(
        live_workspace / "etl-existing.env",
        live_existing_etl_profile,
    )


@pytest.fixture(scope="session")
def live_bootstrap_state(
    live_repo_root: Path,
    live_workspace: Path,
    live_run_prefix: str,
    live_settings: LiveSettings,
    live_admin_env_file: Path,
    live_admin_profile: LiveProfileConfig,
    live_existing_etl_env_file: Path | None,
    live_existing_etl_profile: LiveProfileConfig | None,
) -> Iterator[LiveBootstrapState]:
    if live_existing_etl_env_file is not None and live_existing_etl_profile is not None:
        yield LiveBootstrapState(
            admin_env_file=None,
            etl_env_file=live_existing_etl_env_file,
            tenant_code=live_existing_etl_profile.tenant_code,
            user_name=None,
            password=None,
            access_token_id=None,
            token=None,
            used_existing_etl_profile=True,
        )
        return

    safe_suffix = live_run_prefix.removeprefix("dsctl-live-").replace("-", "")[-12:]
    tenant_code = f"dslvt{safe_suffix}"
    user_name = f"dslvu{safe_suffix}"
    password = f"Dslv{safe_suffix}P1"
    email = f"{user_name}@example.com"
    tenant_created = False
    user_created = False
    access_token_id: int | None = None

    try:
        tenant_result = run_dsctl(
            live_repo_root,
            [
                "tenant",
                "create",
                "--tenant-code",
                tenant_code,
                "--queue",
                live_settings.queue,
            ],
            env_file=live_admin_env_file,
        )
        require_ok_payload(
            tenant_result,
            expected_action="tenant.create",
            label="tenant bootstrap",
        )
        tenant_created = True

        user_result = run_dsctl(
            live_repo_root,
            [
                "user",
                "create",
                "--user-name",
                user_name,
                "--password",
                password,
                "--email",
                email,
                "--tenant",
                tenant_code,
                "--state",
                "1",
            ],
            env_file=live_admin_env_file,
        )
        require_ok_payload(
            user_result,
            expected_action="user.create",
            label="user bootstrap",
        )
        user_created = True

        access_token_result = run_dsctl(
            live_repo_root,
            [
                "access-token",
                "create",
                "--user",
                user_name,
                "--expire-time",
                future_expire_time(),
            ],
            env_file=live_admin_env_file,
        )
        access_token_payload = require_ok_payload(
            access_token_result,
            expected_action="access-token.create",
            label="access-token bootstrap",
        )
        access_token_data = require_mapping(
            access_token_payload["data"],
            label="access-token bootstrap data",
        )
        access_token_id_value = access_token_data.get("id")
        if not isinstance(access_token_id_value, int):
            message = "Bootstrapped access-token payload did not include an integer id"
            raise TypeError(message)
        access_token_id = access_token_id_value
        token = access_token_data.get("token")
        if not isinstance(token, str) or token == "":
            message = (
                "Bootstrapped access-token payload did not include the generated "
                "token string"
            )
            raise AssertionError(message)

        etl_env_file = write_profile_env(
            live_workspace / "etl-generated.env",
            LiveProfileConfig(
                api_url=live_admin_profile.api_url,
                api_token=token,
                tenant_code=tenant_code,
            ),
        )
        bootstrap_state = LiveBootstrapState(
            admin_env_file=live_admin_env_file,
            etl_env_file=etl_env_file,
            tenant_code=tenant_code,
            user_name=user_name,
            password=password,
            access_token_id=access_token_id,
            token=token,
            used_existing_etl_profile=False,
        )

        yield bootstrap_state
    finally:
        if not live_settings.keep_resources:
            if access_token_id is not None:
                run_dsctl(
                    live_repo_root,
                    ["access-token", "delete", str(access_token_id), "--force"],
                    env_file=live_admin_env_file,
                )
            if user_created:
                run_dsctl(
                    live_repo_root,
                    ["user", "delete", user_name, "--force"],
                    env_file=live_admin_env_file,
                )
            if tenant_created:
                run_dsctl(
                    live_repo_root,
                    ["tenant", "delete", tenant_code, "--force"],
                    env_file=live_admin_env_file,
                )


@pytest.fixture(scope="session")
def live_etl_env_file(live_bootstrap_state: LiveBootstrapState) -> Path:
    return live_bootstrap_state.etl_env_file


@pytest.fixture
def live_name_factory(live_run_prefix: str) -> Callable[[str], str]:
    sequence = count(1)

    def build(stem: str) -> str:
        return f"{live_run_prefix}-{stem}-{next(sequence)}"

    return build
