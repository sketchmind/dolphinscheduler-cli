from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests.live.support import (
    cleanup_live_resources,
    require_error_payload,
    require_mapping,
    require_ok_payload,
    run_dsctl,
)
from tests.live.test_governance_surfaces import (
    _create_tenant,
    _create_user,
    _safe_suffix,
    _worker_address,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from pathlib import Path

    from tests.live.support import LiveProfileConfig, LiveSettings


pytestmark = [pytest.mark.live, pytest.mark.live_admin, pytest.mark.destructive]

_USER_TIMEZONE_VERSIONS = frozenset(
    {
        *(f"3.0.{patch}" for patch in range(7)),
        *(f"3.1.{patch}" for patch in range(10)),
        *(f"3.2.{patch}" for patch in range(3)),
        "3.3.1",
        "3.3.2",
        *(f"3.4.{patch}" for patch in range(4)),
    }
)
_WORKER_GROUP_DESCRIPTION_VERSIONS = _USER_TIMEZONE_VERSIONS - {
    *(f"3.0.{patch}" for patch in range(7)),
    "3.1.0",
    "3.1.1",
}


def _require_absent(
    repo_root: Path,
    admin_env_file: Path,
    *,
    command: str,
    selector: str,
    label: str,
) -> None:
    require_error_payload(
        run_dsctl(
            repo_root,
            [command, "get", selector],
            env_file=admin_env_file,
        ),
        expected_action=f"{command}.get",
        expected_type="not_found",
        label=label,
    )


def _cleanup_owned(
    repo_root: Path,
    admin_env_file: Path,
    *,
    command: str,
    selector: str,
    identity: Mapping[str, object],
) -> None:
    result = run_dsctl(
        repo_root,
        [command, "get", selector],
        env_file=admin_env_file,
    )
    if result.exit_code != 0:
        require_error_payload(
            result,
            expected_action=f"{command}.get",
            expected_type="not_found",
            label=f"{command} already absent",
        )
        return

    payload = require_ok_payload(
        result,
        expected_action=f"{command}.get",
        label=f"{command} cleanup readback",
    )
    data = require_mapping(payload["data"], label=f"{command} cleanup identity")
    for field, expected in identity.items():
        assert data.get(field) == expected, (
            f"{command} cleanup refused unproven {field} ownership"
        )

    delete_payload = require_ok_payload(
        run_dsctl(
            repo_root,
            [command, "delete", selector, "--force"],
            env_file=admin_env_file,
        ),
        expected_action=f"{command}.delete",
        label=f"{command} owned cleanup",
    )
    delete_data = require_mapping(
        delete_payload["data"], label=f"{command} delete data"
    )
    assert delete_data["deleted"] is True
    _require_absent(
        repo_root,
        admin_env_file,
        command=command,
        selector=selector,
        label=f"{command} final absence",
    )


def _native_id(value: object, *, label: str) -> int:
    if type(value) is not int or value <= 0:
        message = f"{label} did not return a positive integer id"
        raise TypeError(message)
    return value


def test_admin_user_timezone_update_round_trips(
    live_repo_root: Path,
    live_admin_env_file: Path,
    live_admin_profile: LiveProfileConfig,
    live_settings: LiveSettings,
    live_name_factory: Callable[[str], str],
) -> None:
    if live_admin_profile.ds_version not in _USER_TIMEZONE_VERSIONS:
        pytest.skip("User timeZone update requires an exact DS 3.0.0+ profile")

    suffix = _safe_suffix(live_name_factory, "user-timezone")
    tenant_code = f"dsltz{suffix}"
    user_name = f"dsutz{suffix}"
    email = f"{user_name}@example.com"
    time_zone = "America/New_York"
    _require_absent(
        live_repo_root,
        live_admin_env_file,
        command="tenant",
        selector=tenant_code,
        label="tenant timezone preflight",
    )
    _require_absent(
        live_repo_root,
        live_admin_env_file,
        command="user",
        selector=user_name,
        label="user timezone preflight",
    )

    tenant_id: int | None = None
    user_id: int | None = None
    try:
        tenant_id = _create_tenant(
            live_repo_root,
            live_admin_env_file,
            tenant_code=tenant_code,
            queue=live_settings.queue,
        )
        user_id = _create_user(
            live_repo_root,
            live_admin_env_file,
            user_name=user_name,
            password=f"Dslv{suffix}P1",
            email=email,
            tenant=tenant_code,
        )
        update_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["user", "update", user_name, "--time-zone", time_zone],
                env_file=live_admin_env_file,
            ),
            expected_action="user.update",
            label="user timeZone update",
        )
        update_data = require_mapping(
            update_payload["data"], label="user timeZone update data"
        )
        assert update_data["id"] == user_id
        assert update_data["timeZone"] == time_zone

        get_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["user", "get", user_name],
                env_file=live_admin_env_file,
            ),
            expected_action="user.get",
            label="user timeZone get",
        )
        get_data = require_mapping(get_payload["data"], label="user timeZone get data")
        assert get_data["id"] == user_id
        assert get_data["tenantId"] == tenant_id
        assert get_data["timeZone"] == time_zone
    finally:
        cleanup_live_resources(
            [
                lambda: _cleanup_owned(
                    live_repo_root,
                    live_admin_env_file,
                    command="user",
                    selector=user_name,
                    identity={
                        "userName": user_name,
                        "email": email,
                        **({"id": user_id} if user_id is not None else {}),
                    },
                ),
                lambda: _cleanup_owned(
                    live_repo_root,
                    live_admin_env_file,
                    command="tenant",
                    selector=tenant_code,
                    identity={
                        "tenantCode": tenant_code,
                        **({"id": tenant_id} if tenant_id is not None else {}),
                    },
                ),
            ]
        )


def test_admin_worker_group_description_update_round_trips(
    live_repo_root: Path,
    live_admin_env_file: Path,
    live_admin_profile: LiveProfileConfig,
    live_name_factory: Callable[[str], str],
) -> None:
    if live_admin_profile.ds_version in {"3.1.0", "3.1.1"}:
        pytest.skip(
            "Native DS 3.1.0/3.1.1 rejects a worker-group update that keeps "
            "its name, including description-only updates"
        )
    if live_admin_profile.ds_version not in _WORKER_GROUP_DESCRIPTION_VERSIONS:
        pytest.skip("Worker-group description update requires DS 3.1.2+")

    worker_address = _worker_address(live_repo_root, live_admin_env_file)
    suffix = _safe_suffix(live_name_factory, "worker-description")
    name = f"dsctl-worker-desc-{suffix}"
    description = f"owned worker description {suffix}"
    _require_absent(
        live_repo_root,
        live_admin_env_file,
        command="worker-group",
        selector=name,
        label="worker-group description preflight",
    )

    worker_group_id: int | None = None
    try:
        create_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["worker-group", "create", "--name", name, "--addr", worker_address],
                env_file=live_admin_env_file,
            ),
            expected_action="worker-group.create",
            label="worker-group description create",
        )
        create_data = require_mapping(
            create_payload["data"], label="worker-group description create data"
        )
        assert create_data["name"] == name
        worker_group_id = _native_id(create_data.get("id"), label="worker-group create")

        update_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["worker-group", "update", name, "--description", description],
                env_file=live_admin_env_file,
            ),
            expected_action="worker-group.update",
            label="worker-group description update",
        )
        update_data = require_mapping(
            update_payload["data"], label="worker-group description update data"
        )
        assert update_data["id"] == worker_group_id
        assert update_data["name"] == name
        assert update_data["description"] == description

        get_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["worker-group", "get", name],
                env_file=live_admin_env_file,
            ),
            expected_action="worker-group.get",
            label="worker-group description get",
        )
        get_data = require_mapping(
            get_payload["data"], label="worker-group description get data"
        )
        assert get_data["id"] == worker_group_id
        assert get_data["name"] == name
        assert get_data["description"] == description
    finally:
        _cleanup_owned(
            live_repo_root,
            live_admin_env_file,
            command="worker-group",
            selector=name,
            identity={
                "name": name,
                **({"id": worker_group_id} if worker_group_id is not None else {}),
            },
        )
