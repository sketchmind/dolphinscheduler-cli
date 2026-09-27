from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

import pytest

from tests.live.support import (
    DsctlCommandResult,
    LiveProfileConfig,
    cleanup_live_resources,
    future_expire_time,
    require_error_payload,
    require_list,
    require_mapping,
    require_ok_payload,
    run_dsctl,
    write_profile_env,
)
from tests.live.task_group_support import (
    require_task_group_get_absent,
    require_task_group_list_absent,
    require_task_group_project_cleanup_version,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path


pytestmark = [pytest.mark.live, pytest.mark.live_admin, pytest.mark.destructive]


def _require_int_value(value: object, *, label: str) -> int:
    if not isinstance(value, int):
        message = f"{label} must be an integer, got {type(value).__name__}"
        raise TypeError(message)
    return value


def _require_text_value(value: object, *, label: str) -> str:
    if not isinstance(value, str) or value == "":
        message = f"{label} must be a non-empty string"
        raise TypeError(message)
    return value


def _worker_address(repo_root: Path, admin_env_file: Path) -> str:
    payload = require_ok_payload(
        run_dsctl(repo_root, ["monitor", "server", "worker"], env_file=admin_env_file),
        expected_action="monitor.server",
        label="discover live worker address",
    )
    rows = require_list(payload["data"], label="worker monitor rows")
    assert rows, "A live worker is required for the worker-group lifecycle"
    row = require_mapping(rows[0], label="worker monitor row")
    host = _require_text_value(row.get("host"), label="worker host")
    port = _require_int_value(row.get("port"), label="worker port")
    assert type(port) is int
    assert 0 < port <= 65535
    return f"{host}:{port}"


def _tenant_create_denial_from_queue_visibility(
    result: DsctlCommandResult, *, queue_id: int
) -> str:
    if result.exit_code != 0:
        require_error_payload(
            result,
            expected_action="queue.list",
            expected_type="permission_denied",
            label="ETL queue visibility",
        )
        return "permission_denied"
    payload = require_ok_payload(
        result, expected_action="queue.list", label="ETL queue visibility"
    )
    data = require_mapping(payload["data"], label="ETL queue list data")
    rows = require_list(data["totalList"], label="ETL queue rows")
    coverage = require_mapping(data["coverage"], label="ETL queue coverage")
    assert coverage["scope_complete"] is True
    assert coverage["totals_changed"] is False
    assert data["total"] == len(rows)
    visible = any(
        require_mapping(row, label="queue row").get("id") == queue_id for row in rows
    )
    return "permission_denied" if visible else "not_found"


def _user_create_denial_from_tenant_visibility(
    result: DsctlCommandResult, *, tenant_code: str
) -> str:
    if result.exit_code != 0:
        require_error_payload(
            result,
            expected_action="tenant.list",
            expected_type="permission_denied",
            label="ETL tenant visibility",
        )
        return "permission_denied"
    payload = require_ok_payload(
        result, expected_action="tenant.list", label="ETL tenant visibility"
    )
    data = require_mapping(payload["data"], label="ETL tenant list data")
    rows = require_list(data["totalList"], label="ETL tenant rows")
    coverage = require_mapping(data["coverage"], label="ETL tenant coverage")
    assert coverage["scope_complete"] is True
    assert coverage["totals_changed"] is False
    assert data["total"] == len(rows)
    visible = any(
        require_mapping(row, label="tenant row").get("tenantCode") == tenant_code
        for row in rows
    )
    return "permission_denied" if visible else "not_found"


def _require_user_create_denial(
    result: DsctlCommandResult, *, expected_type: str, tenant_code: str
) -> None:
    error = require_error_payload(
        result,
        expected_action="user.create",
        expected_type=expected_type,
        label="etl user create",
    )
    assert error["type"] == expected_type
    if expected_type == "not_found":
        details = require_mapping(error["details"], label="tenant visibility denial")
        assert details["resource"] == "tenant"
        assert details["tenantCode"] == tenant_code


def _safe_suffix(name_factory: Callable[[str], str], stem: str) -> str:
    raw = name_factory(stem)
    if raw == "":
        message = "live name factory returned an empty identifier"
        raise AssertionError(message)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]


def _create_queue(
    repo_root: Path,
    admin_env_file: Path,
    *,
    queue_name: str,
    queue_value: str,
) -> int:
    payload = require_ok_payload(
        run_dsctl(
            repo_root,
            [
                "queue",
                "create",
                "--queue-name",
                queue_name,
                "--queue",
                queue_value,
            ],
            env_file=admin_env_file,
        ),
        expected_action="queue.create",
        label="queue create",
    )
    data = require_mapping(payload["data"], label="queue create data")
    return _require_int_value(data.get("id"), label="queue id")


def _delete_queue(
    repo_root: Path,
    admin_env_file: Path,
    *,
    queue: str,
) -> None:
    payload = require_ok_payload(
        run_dsctl(
            repo_root,
            ["queue", "delete", queue, "--force"],
            env_file=admin_env_file,
        ),
        expected_action="queue.delete",
        label="queue delete",
    )
    data = require_mapping(payload["data"], label="queue delete data")
    assert data["deleted"] is True


def _create_tenant(
    repo_root: Path,
    admin_env_file: Path,
    *,
    tenant_code: str,
    queue: str,
    description: str | None = None,
    on_created: Callable[[], None] | None = None,
) -> int:
    argv = [
        "tenant",
        "create",
        "--tenant-code",
        tenant_code,
        "--queue",
        queue,
    ]
    if description is not None:
        argv.extend(["--description", description])
    payload = require_ok_payload(
        run_dsctl(repo_root, argv, env_file=admin_env_file),
        expected_action="tenant.create",
        label="tenant create",
    )
    if on_created is not None:
        on_created()
    data = require_mapping(payload["data"], label="tenant create data")
    return _require_int_value(data.get("id"), label="tenant id")


def _delete_tenant(
    repo_root: Path,
    admin_env_file: Path,
    *,
    tenant: str,
) -> None:
    payload = require_ok_payload(
        run_dsctl(
            repo_root,
            ["tenant", "delete", tenant, "--force"],
            env_file=admin_env_file,
        ),
        expected_action="tenant.delete",
        label="tenant delete",
    )
    data = require_mapping(payload["data"], label="tenant delete data")
    assert data["deleted"] is True


def _create_user(
    repo_root: Path,
    admin_env_file: Path,
    *,
    user_name: str,
    password: str,
    email: str,
    tenant: str,
    phone: str | None = None,
    on_created: Callable[[], None] | None = None,
) -> int:
    argv = [
        "user",
        "create",
        "--user-name",
        user_name,
        "--password",
        password,
        "--email",
        email,
        "--tenant",
        tenant,
        "--state",
        "1",
    ]
    if phone is not None:
        argv.extend(["--phone", phone])
    payload = require_ok_payload(
        run_dsctl(repo_root, argv, env_file=admin_env_file),
        expected_action="user.create",
        label="user create",
    )
    if on_created is not None:
        on_created()
    data = require_mapping(payload["data"], label="user create data")
    return _require_int_value(data.get("id"), label="user id")


def _delete_user(
    repo_root: Path,
    admin_env_file: Path,
    *,
    user: str,
) -> None:
    payload = require_ok_payload(
        run_dsctl(
            repo_root,
            ["user", "delete", user, "--force"],
            env_file=admin_env_file,
        ),
        expected_action="user.delete",
        label="user delete",
    )
    data = require_mapping(payload["data"], label="user delete data")
    assert data["deleted"] is True


def _create_access_token(
    repo_root: Path,
    admin_env_file: Path,
    *,
    user_name: str,
    on_created: Callable[[int], None] | None = None,
) -> tuple[int, str]:
    payload = require_ok_payload(
        run_dsctl(
            repo_root,
            [
                "access-token",
                "create",
                "--user",
                user_name,
                "--expire-time",
                future_expire_time(days=30),
            ],
            env_file=admin_env_file,
        ),
        expected_action="access-token.create",
        label="access-token create",
    )
    data = require_mapping(payload["data"], label="access-token create data")
    token_id = _require_int_value(data.get("id"), label="access-token id")
    if on_created is not None:
        on_created(token_id)
    token = _require_text_value(data.get("token"), label="access-token token")
    return token_id, token


def _delete_access_token(
    repo_root: Path,
    admin_env_file: Path,
    *,
    token_id: int,
) -> None:
    payload = require_ok_payload(
        run_dsctl(
            repo_root,
            ["access-token", "delete", str(token_id), "--force"],
            env_file=admin_env_file,
        ),
        expected_action="access-token.delete",
        label="access-token delete",
    )
    data = require_mapping(payload["data"], label="access-token delete data")
    assert data["deleted"] is True


def _create_project(
    repo_root: Path,
    admin_env_file: Path,
    *,
    name: str,
    description: str,
    on_created: Callable[[], None] | None = None,
) -> None:
    payload = require_ok_payload(
        run_dsctl(
            repo_root,
            [
                "project",
                "create",
                "--name",
                name,
                "--description",
                description,
            ],
            env_file=admin_env_file,
        ),
        expected_action="project.create",
        label="project create",
    )
    if on_created is not None:
        on_created()
    data = require_mapping(payload["data"], label="project create data")
    assert _require_text_value(data.get("name"), label="project name") == name
    assert data.get("description") == description


def _delete_project(
    repo_root: Path,
    admin_env_file: Path,
    *,
    project: str,
) -> None:
    payload = require_ok_payload(
        run_dsctl(
            repo_root,
            ["project", "delete", project, "--force"],
            env_file=admin_env_file,
        ),
        expected_action="project.delete",
        label="project delete",
    )
    data = require_mapping(payload["data"], label="project delete data")
    assert data["deleted"] is True


def _cleanup_user_project_resources(
    *,
    token: Callable[[], None] | None,
    user: Callable[[], None] | None,
    project: Callable[[], None] | None,
    tenant: Callable[[], None] | None,
) -> None:
    cleanup_live_resources(
        [task for task in (token, user, project, tenant) if task is not None]
    )


def test_admin_queue_lifecycle_round_trips_and_etl_is_denied(
    live_repo_root: Path,
    live_admin_env_file: Path,
    live_etl_env_file: Path,
    live_name_factory: Callable[[str], str],
) -> None:
    suffix = _safe_suffix(live_name_factory, "queue")
    queue_name = f"dsctl-queue-{suffix}"
    updated_queue_name = f"dsctl-queue-upd-{suffix}"
    queue_value = f"q{suffix}"
    updated_queue_value = f"uq{suffix}"

    permission_error = require_error_payload(
        run_dsctl(
            live_repo_root,
            [
                "queue",
                "create",
                "--queue-name",
                queue_name,
                "--queue",
                queue_value,
            ],
            env_file=live_etl_env_file,
        ),
        expected_action="queue.create",
        expected_type="permission_denied",
        label="etl queue create",
    )
    assert permission_error["type"] == "permission_denied"

    queue_id = _create_queue(
        live_repo_root,
        live_admin_env_file,
        queue_name=queue_name,
        queue_value=queue_value,
    )
    current_queue = queue_name

    try:
        get_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["queue", "get", current_queue],
                env_file=live_admin_env_file,
            ),
            expected_action="queue.get",
            label="queue get",
        )
        get_data = require_mapping(get_payload["data"], label="queue get data")
        assert get_data["id"] == queue_id
        assert get_data["queueName"] == queue_name
        assert get_data["queue"] == queue_value

        list_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["queue", "list", "--search", queue_name, "--page-size", "20"],
                env_file=live_admin_env_file,
            ),
            expected_action="queue.list",
            label="queue list",
        )
        list_data = require_mapping(list_payload["data"], label="queue list data")
        rows = require_list(list_data["totalList"], label="queue rows")
        assert any(
            require_mapping(item, label="queue row").get("id") == queue_id
            for item in rows
        )

        update_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "queue",
                    "update",
                    current_queue,
                    "--queue-name",
                    updated_queue_name,
                    "--queue",
                    updated_queue_value,
                ],
                env_file=live_admin_env_file,
            ),
            expected_action="queue.update",
            label="queue update",
        )
        update_data = require_mapping(update_payload["data"], label="queue update data")
        assert update_data["id"] == queue_id
        assert update_data["queueName"] == updated_queue_name
        assert update_data["queue"] == updated_queue_value
        current_queue = updated_queue_name
    finally:
        _delete_queue(live_repo_root, live_admin_env_file, queue=current_queue)

    not_found_error = require_error_payload(
        run_dsctl(
            live_repo_root,
            ["queue", "get", current_queue],
            env_file=live_admin_env_file,
        ),
        expected_action="queue.get",
        expected_type="not_found",
        label="queue get after delete",
    )
    assert not_found_error["type"] == "not_found"


def test_admin_worker_group_lifecycle_round_trips(
    live_repo_root: Path,
    live_admin_env_file: Path,
    live_name_factory: Callable[[str], str],
) -> None:
    worker_address = _worker_address(live_repo_root, live_admin_env_file)
    suffix = _safe_suffix(live_name_factory, "worker-group")
    worker_group_name = f"dsctl-worker-{suffix}"
    updated_worker_group_name = f"dsctl-worker-upd-{suffix}"

    create_payload = require_ok_payload(
        run_dsctl(
            live_repo_root,
            [
                "worker-group",
                "create",
                "--name",
                worker_group_name,
                "--addr",
                worker_address,
            ],
            env_file=live_admin_env_file,
        ),
        expected_action="worker-group.create",
        label="worker-group create",
    )
    current_worker_group = worker_group_name
    try:
        create_data = require_mapping(
            create_payload["data"],
            label="worker-group create data",
        )
        assert create_data["name"] == worker_group_name
        get_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["worker-group", "get", current_worker_group],
                env_file=live_admin_env_file,
            ),
            expected_action="worker-group.get",
            label="worker-group get",
        )
        get_data = require_mapping(get_payload["data"], label="worker-group get data")
        assert get_data["name"] == worker_group_name

        list_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "worker-group",
                    "list",
                    "--search",
                    worker_group_name,
                    "--page-size",
                    "20",
                ],
                env_file=live_admin_env_file,
            ),
            expected_action="worker-group.list",
            label="worker-group list",
        )
        list_data = require_mapping(
            list_payload["data"],
            label="worker-group list data",
        )
        rows = require_list(list_data["totalList"], label="worker-group rows")
        assert any(
            require_mapping(item, label="worker-group row").get("name")
            == worker_group_name
            for item in rows
        )

        update_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "worker-group",
                    "update",
                    current_worker_group,
                    "--name",
                    updated_worker_group_name,
                    "--addr",
                    worker_address,
                ],
                env_file=live_admin_env_file,
            ),
            expected_action="worker-group.update",
            label="worker-group update",
        )
        current_worker_group = updated_worker_group_name
        update_data = require_mapping(
            update_payload["data"],
            label="worker-group update data",
        )
        assert update_data["name"] == updated_worker_group_name
    finally:
        delete_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["worker-group", "delete", current_worker_group, "--force"],
                env_file=live_admin_env_file,
            ),
            expected_action="worker-group.delete",
            label="worker-group delete",
        )
        delete_data = require_mapping(
            delete_payload["data"],
            label="worker-group delete data",
        )
        assert delete_data["deleted"] is True

    not_found_error = require_error_payload(
        run_dsctl(
            live_repo_root,
            ["worker-group", "get", current_worker_group],
            env_file=live_admin_env_file,
        ),
        expected_action="worker-group.get",
        expected_type="not_found",
        label="worker-group get after delete",
    )
    assert not_found_error["type"] == "not_found"


def test_admin_tenant_lifecycle_round_trips_and_etl_is_denied(
    live_repo_root: Path,
    live_admin_env_file: Path,
    live_etl_env_file: Path,
    live_name_factory: Callable[[str], str],
) -> None:
    suffix = _safe_suffix(live_name_factory, "tenant")
    tenant_code = f"dsltt{suffix}"
    updated_description = "live tenant update path"

    queue_payload = require_ok_payload(
        run_dsctl(
            live_repo_root, ["queue", "get", "default"], env_file=live_admin_env_file
        ),
        expected_action="queue.get",
        label="admin default queue",
    )
    queue_data = require_mapping(queue_payload["data"], label="default queue data")
    queue_id = _require_int_value(queue_data.get("id"), label="default queue id")
    expected_denial = _tenant_create_denial_from_queue_visibility(
        run_dsctl(
            live_repo_root, ["queue", "list", "--all"], env_file=live_etl_env_file
        ),
        queue_id=queue_id,
    )

    permission_error = require_error_payload(
        run_dsctl(
            live_repo_root,
            [
                "tenant",
                "create",
                "--tenant-code",
                tenant_code,
                "--queue",
                str(queue_id),
            ],
            env_file=live_etl_env_file,
        ),
        expected_action="tenant.create",
        expected_type=expected_denial,
        label="etl tenant create",
    )
    assert permission_error["type"] == expected_denial
    if expected_denial == "not_found":
        details = require_mapping(
            permission_error["details"], label="queue visibility denial"
        )
        assert details["resource"] == "queue"
        assert details["id"] == queue_id

    tenant_id = _create_tenant(
        live_repo_root,
        live_admin_env_file,
        tenant_code=tenant_code,
        queue="default",
        description="live tenant create path",
    )

    try:
        get_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["tenant", "get", tenant_code],
                env_file=live_admin_env_file,
            ),
            expected_action="tenant.get",
            label="tenant get",
        )
        get_data = require_mapping(get_payload["data"], label="tenant get data")
        assert get_data["id"] == tenant_id
        assert get_data["tenantCode"] == tenant_code
        assert get_data["queueName"] == "default"

        list_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["tenant", "list", "--search", tenant_code, "--page-size", "20"],
                env_file=live_admin_env_file,
            ),
            expected_action="tenant.list",
            label="tenant list",
        )
        list_data = require_mapping(list_payload["data"], label="tenant list data")
        rows = require_list(list_data["totalList"], label="tenant rows")
        assert any(
            require_mapping(item, label="tenant row").get("tenantCode") == tenant_code
            for item in rows
        )

        update_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "tenant",
                    "update",
                    tenant_code,
                    "--description",
                    updated_description,
                ],
                env_file=live_admin_env_file,
            ),
            expected_action="tenant.update",
            label="tenant update",
        )
        update_data = require_mapping(
            update_payload["data"],
            label="tenant update data",
        )
        assert update_data["id"] == tenant_id
        assert update_data["description"] == updated_description
    finally:
        _delete_tenant(live_repo_root, live_admin_env_file, tenant=tenant_code)

    not_found_error = require_error_payload(
        run_dsctl(
            live_repo_root,
            ["tenant", "get", tenant_code],
            env_file=live_admin_env_file,
        ),
        expected_action="tenant.get",
        expected_type="not_found",
        label="tenant get after delete",
    )
    assert not_found_error["type"] == "not_found"


def test_admin_user_lifecycle_and_project_grant_effect(
    live_repo_root: Path,
    live_admin_env_file: Path,
    live_admin_profile: LiveProfileConfig,
    live_etl_env_file: Path,
    live_bootstrap_state: object,
    live_name_factory: Callable[[str], str],
    tmp_path: Path,
) -> None:
    suffix = _safe_suffix(live_name_factory, "user")
    tenant_code = f"dsltu{suffix}"
    user_name = f"dslvu{suffix}"
    password = f"Dslv{suffix}P1"
    email = f"{user_name}@example.com"
    updated_phone = "13800000000"
    project_name = f"dsctl-admin-project-{suffix}"
    etl_tenant_code = _require_text_value(
        getattr(live_bootstrap_state, "tenant_code", None),
        label="managed ETL tenant code",
    )
    token_cleanup: Callable[[], None] | None = None
    user_cleanup: Callable[[], None] | None = None
    project_cleanup: Callable[[], None] | None = None
    tenant_cleanup: Callable[[], None] | None = None
    user_env_file: Path | None = None

    def cleanup_tenant() -> None:
        _delete_tenant(
            live_repo_root,
            live_admin_env_file,
            tenant=tenant_code,
        )

    def register_tenant_cleanup() -> None:
        nonlocal tenant_cleanup
        tenant_cleanup = cleanup_tenant

    def cleanup_user() -> None:
        _delete_user(
            live_repo_root,
            live_admin_env_file,
            user=user_name,
        )

    def register_user_cleanup() -> None:
        nonlocal user_cleanup
        user_cleanup = cleanup_user

    def register_token_cleanup(token_id: int) -> None:
        nonlocal token_cleanup

        def cleanup_token() -> None:
            _delete_access_token(
                live_repo_root,
                live_admin_env_file,
                token_id=token_id,
            )

        token_cleanup = cleanup_token

    def cleanup_project() -> None:
        _delete_project(
            live_repo_root,
            live_admin_env_file,
            project=project_name,
        )

    def register_project_cleanup() -> None:
        nonlocal project_cleanup
        project_cleanup = cleanup_project

    try:
        tenant_id = _create_tenant(
            live_repo_root,
            live_admin_env_file,
            tenant_code=tenant_code,
            queue="default",
            on_created=register_tenant_cleanup,
        )

        tenant_visibility = run_dsctl(
            live_repo_root,
            ["tenant", "list", "--search", etl_tenant_code, "--all"],
            env_file=live_etl_env_file,
        )
        expected_denial = _user_create_denial_from_tenant_visibility(
            tenant_visibility,
            tenant_code=etl_tenant_code,
        )
        permission_result = run_dsctl(
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
                etl_tenant_code,
                "--state",
                "1",
            ],
            env_file=live_etl_env_file,
        )
        if (
            permission_result.exit_code == 0
            and permission_result.payload.get("ok") is True
        ):
            register_user_cleanup()
        _require_user_create_denial(
            permission_result,
            expected_type=expected_denial,
            tenant_code=etl_tenant_code,
        )

        user_id = _create_user(
            live_repo_root,
            live_admin_env_file,
            user_name=user_name,
            password=password,
            email=email,
            tenant=tenant_code,
            on_created=register_user_cleanup,
        )

        get_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["user", "get", user_name],
                env_file=live_admin_env_file,
            ),
            expected_action="user.get",
            label="user get",
        )
        get_data = require_mapping(get_payload["data"], label="user get data")
        assert get_data["id"] == user_id
        assert get_data["userName"] == user_name
        assert get_data["tenantId"] == tenant_id
        assert get_data.get("tenantCode") in (None, tenant_code)

        list_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["user", "list", "--search", user_name, "--page-size", "20"],
                env_file=live_admin_env_file,
            ),
            expected_action="user.list",
            label="user list",
        )
        list_data = require_mapping(list_payload["data"], label="user list data")
        rows = require_list(list_data["totalList"], label="user rows")
        assert any(
            require_mapping(item, label="user row").get("id") == user_id
            for item in rows
        )

        update_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "user",
                    "update",
                    user_name,
                    "--phone",
                    updated_phone,
                ],
                env_file=live_admin_env_file,
            ),
            expected_action="user.update",
            label="user update",
        )
        update_data = require_mapping(update_payload["data"], label="user update data")
        assert update_data["id"] == user_id
        assert update_data["phone"] == updated_phone

        token_id, token = _create_access_token(
            live_repo_root,
            live_admin_env_file,
            user_name=user_name,
            on_created=register_token_cleanup,
        )
        user_env_file = write_profile_env(
            tmp_path / f"{user_name}.env",
            live_admin_profile.with_credentials(
                api_token=token,
                tenant_code=tenant_code,
            ),
        )

        _create_project(
            live_repo_root,
            live_admin_env_file,
            name=project_name,
            description="live admin grant project path",
            on_created=register_project_cleanup,
        )

        before_grant_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["project", "list", "--search", project_name, "--page-size", "20"],
                env_file=user_env_file,
            ),
            expected_action="project.list",
            label="user project list before grant",
        )
        before_grant_data = require_mapping(
            before_grant_payload["data"],
            label="user project list before grant data",
        )
        before_grant_rows = require_list(
            before_grant_data["totalList"],
            label="user project list before grant rows",
        )
        assert not any(
            require_mapping(item, label="project row").get("name") == project_name
            for item in before_grant_rows
        )

        grant_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["user", "grant", "project", user_name, project_name],
                env_file=live_admin_env_file,
            ),
            expected_action="user.grant.project",
            label="user grant project",
        )
        grant_data = require_mapping(grant_payload["data"], label="user grant data")
        assert grant_data["granted"] is True
        assert grant_data["permission"] == "write"

        after_grant_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["project", "get", project_name],
                env_file=user_env_file,
            ),
            expected_action="project.get",
            label="user project get after grant",
        )
        after_grant_data = require_mapping(
            after_grant_payload["data"],
            label="user project get after grant data",
        )
        assert after_grant_data["name"] == project_name

        revoke_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["user", "revoke", "project", user_name, project_name],
                env_file=live_admin_env_file,
            ),
            expected_action="user.revoke.project",
            label="user revoke project",
        )
        revoke_data = require_mapping(
            revoke_payload["data"],
            label="user revoke data",
        )
        assert revoke_data["revoked"] is True

        after_revoke_error = require_error_payload(
            run_dsctl(
                live_repo_root,
                ["project", "get", project_name],
                env_file=user_env_file,
            ),
            expected_action="project.get",
            expected_type="not_found",
            label="user project get after revoke",
        )
        assert after_revoke_error["type"] == "not_found"

        # A failed response cannot prove the delete was not applied.
        token_cleanup = None
        _delete_access_token(
            live_repo_root,
            live_admin_env_file,
            token_id=token_id,
        )

        user_cleanup = None
        delete_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["user", "delete", user_name, "--force"],
                env_file=live_admin_env_file,
            ),
            expected_action="user.delete",
            label="user delete",
        )
        delete_data = require_mapping(delete_payload["data"], label="user delete data")
        assert delete_data["deleted"] is True

        not_found_error = require_error_payload(
            run_dsctl(
                live_repo_root,
                ["user", "get", user_name],
                env_file=live_admin_env_file,
            ),
            expected_action="user.get",
            expected_type="not_found",
            label="user get after delete",
        )
        assert not_found_error["type"] == "not_found"
    finally:
        _cleanup_user_project_resources(
            token=token_cleanup,
            user=user_cleanup,
            project=project_cleanup,
            tenant=tenant_cleanup,
        )


def test_etl_task_group_lifecycle_round_trips_with_project_grant(
    live_repo_root: Path,
    live_admin_env_file: Path,
    live_admin_profile: LiveProfileConfig,
    live_bootstrap_state: object,
    live_etl_env_file: Path,
    live_run_prefix: str,
) -> None:
    user_name = getattr(live_bootstrap_state, "user_name", None)
    if not isinstance(user_name, str) or user_name == "":
        pytest.skip(
            "Task-group live test requires a managed ETL bootstrap user for "
            "project grant."
        )

    require_task_group_project_cleanup_version(
        live_repo_root,
        live_admin_env_file,
        live_admin_profile.ds_version,
        execute=run_dsctl,
    )

    suffix = _safe_suffix(lambda stem: f"{live_run_prefix}-{stem}", "task-group")
    project_name = f"live-task-group-project-{suffix}"
    task_group_name = f"live-task-group-{suffix}"

    project_cleanup: Callable[[], None] | None = None

    def cleanup_project() -> None:
        _delete_project(
            live_repo_root,
            live_admin_env_file,
            project=project_name,
        )

    def register_project_cleanup() -> None:
        nonlocal project_cleanup
        project_cleanup = cleanup_project

    try:
        _create_project(
            live_repo_root,
            live_admin_env_file,
            name=project_name,
            description="live task-group lifecycle",
            on_created=register_project_cleanup,
        )

        grant_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["user", "grant", "project", user_name, project_name],
                env_file=live_admin_env_file,
            ),
            expected_action="user.grant.project",
            label="task-group project grant",
        )
        grant_data = require_mapping(
            grant_payload["data"],
            label="task-group grant data",
        )
        assert grant_data["granted"] is True

        create_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "task-group",
                    "create",
                    "--project",
                    project_name,
                    "--name",
                    task_group_name,
                    "--group-size",
                    "2",
                    "--description",
                    "initial task-group",
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="task-group.create",
            label="task-group create",
        )
        create_data = require_mapping(create_payload["data"], label="task-group data")
        assert create_data["name"] == task_group_name
        assert create_data["groupSize"] == 2
        assert create_data["status"] == "YES"

        get_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["task-group", "get", task_group_name],
                env_file=live_etl_env_file,
            ),
            expected_action="task-group.get",
            label="task-group get",
        )
        get_data = require_mapping(get_payload["data"], label="task-group get data")
        project_code = _require_int_value(
            get_data.get("projectCode"),
            label="task-group projectCode",
        )
        assert get_data["name"] == task_group_name
        assert get_data["groupSize"] == 2
        assert get_data["status"] == "YES"

        list_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["task-group", "list", "--project", project_name],
                env_file=live_etl_env_file,
            ),
            expected_action="task-group.list",
            label="task-group list by project",
        )
        list_data = require_mapping(list_payload["data"], label="task-group list data")
        list_rows = require_list(list_data["totalList"], label="task-group list rows")
        assert any(
            require_mapping(item, label="task-group row").get("name") == task_group_name
            for item in list_rows
        )

        update_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "task-group",
                    "update",
                    task_group_name,
                    "--group-size",
                    "3",
                    "--description",
                    "updated task-group",
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="task-group.update",
            label="task-group update",
        )
        update_data = require_mapping(
            update_payload["data"],
            label="task-group update data",
        )
        assert update_data["groupSize"] == 3
        assert update_data["description"] == "updated task-group"

        close_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["task-group", "close", task_group_name],
                env_file=live_etl_env_file,
            ),
            expected_action="task-group.close",
            label="task-group close",
        )
        close_data = require_mapping(
            close_payload["data"],
            label="task-group close data",
        )
        assert close_data["status"] == "NO"

        closed_list_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "task-group",
                    "list",
                    "--search",
                    task_group_name,
                    "--status",
                    "closed",
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="task-group.list",
            label="task-group list closed",
        )
        closed_list_data = require_mapping(
            closed_list_payload["data"],
            label="task-group list closed data",
        )
        closed_rows = require_list(
            closed_list_data["totalList"],
            label="task-group closed rows",
        )
        assert any(
            require_mapping(item, label="task-group closed row").get("projectCode")
            == project_code
            for item in closed_rows
        )

        start_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["task-group", "start", task_group_name],
                env_file=live_etl_env_file,
            ),
            expected_action="task-group.start",
            label="task-group start",
        )
        start_data = require_mapping(
            start_payload["data"],
            label="task-group start data",
        )
        assert start_data["status"] == "YES"

        queue_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["task-group", "queue", "list", task_group_name],
                env_file=live_etl_env_file,
            ),
            expected_action="task-group.queue.list",
            label="task-group queue list",
        )
        queue_data = require_mapping(
            queue_payload["data"],
            label="task-group queue data",
        )
        queue_rows = require_list(
            queue_data["totalList"],
            label="task-group queue rows",
        )
        assert isinstance(queue_rows, list)
    finally:
        if project_cleanup is not None:
            cleanup_live_resources(
                [
                    project_cleanup,
                    lambda: require_task_group_get_absent(
                        live_repo_root,
                        live_etl_env_file,
                        task_group=task_group_name,
                        execute=run_dsctl,
                    ),
                    lambda: require_task_group_list_absent(
                        live_repo_root,
                        live_etl_env_file,
                        task_group=task_group_name,
                        execute=run_dsctl,
                    ),
                ]
            )
