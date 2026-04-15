import pytest
from tests.fakes import (
    FakeAlertGroup,
    FakeAlertGroupAdapter,
    FakeDataSource,
    FakeDataSourceAdapter,
    FakeEnumValue,
    FakeEnvironment,
    FakeEnvironmentAdapter,
    FakeProject,
    FakeProjectAdapter,
    FakeQueue,
    FakeQueueAdapter,
    FakeTenant,
    FakeTenantAdapter,
    FakeUser,
    FakeUserAdapter,
    FakeWorkerGroup,
    FakeWorkerGroupAdapter,
)

from dsctl.errors import NotFoundError, ResolutionError, UserInputError
from dsctl.services import resolver as resolver_service
from dsctl.services.resolver import (
    alert_group,
    datasource,
    project,
    queue,
    tenant,
    user,
    worker_group,
)


def test_project_resolver_matches_by_exact_name() -> None:
    adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, name="etl-prod", description="daily jobs")]
    )
    resolved = project(
        "etl-prod",
        adapter=adapter,
    )

    assert resolved.code == 7
    assert resolved.name == "etl-prod"
    assert resolved.description == "daily jobs"


def test_project_resolver_fetches_numeric_codes_directly() -> None:
    adapter = FakeProjectAdapter(projects=[FakeProject(code=9, name="batch")])
    resolved = project(
        "9",
        adapter=adapter,
    )

    assert resolved.code == 9
    assert resolved.name == "batch"


def test_project_resolver_reports_missing_names() -> None:
    adapter = FakeProjectAdapter(projects=[])
    with pytest.raises(NotFoundError, match="was not found") as exc_info:
        project(
            "missing",
            adapter=adapter,
        )
    assert exc_info.value.suggestion == (
        "Retry with `project list` to inspect available values, or pass the "
        "numeric code if known."
    )


def test_project_resolver_rejects_empty_identifier() -> None:
    adapter = FakeProjectAdapter(projects=[])

    with pytest.raises(
        UserInputError,
        match="Project identifier must not be empty",
    ) as exc_info:
        project(
            "   ",
            adapter=adapter,
        )

    assert exc_info.value.suggestion == "Pass one non-empty Project name or numeric id."


def test_project_resolver_reports_ambiguous_names() -> None:
    adapter = FakeProjectAdapter(
        projects=[
            FakeProject(code=7, name="etl-prod"),
            FakeProject(code=8, name="etl-prod"),
        ]
    )
    with pytest.raises(ResolutionError, match="ambiguous") as exc_info:
        project(
            "etl-prod",
            adapter=adapter,
        )
    assert exc_info.value.suggestion == (
        "Retry with one explicit numeric code from the matching results: 7, 8."
    )


def test_project_resolver_uses_search_pages_instead_of_full_project_scan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[int, int, str | None]] = []
    adapter = FakeProjectAdapter(
        projects=[
            FakeProject(code=7, name="etl-prod"),
            FakeProject(code=8, name="streaming"),
        ]
    )

    original_list = adapter.list

    def tracked_list(
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> object:
        calls.append((page_no, page_size, search))
        return original_list(
            page_no=page_no,
            page_size=page_size,
            search=search,
        )

    monkeypatch.setattr(adapter, "list", tracked_list)

    resolved = project("etl-prod", adapter=adapter)

    assert resolved.code == 7
    assert calls == [(1, resolver_service.DEFAULT_RESOLUTION_PAGE_SIZE, "etl-prod")]


def test_project_resolver_reports_search_safety_limit_exhaustion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(resolver_service, "DEFAULT_RESOLUTION_PAGE_SIZE", 1)
    monkeypatch.setattr(resolver_service, "MAX_RESOLUTION_PAGES", 2)
    adapter = FakeProjectAdapter(
        projects=[
            FakeProject(code=index, name=f"etl-prod-{index}") for index in range(3)
        ]
    )

    with pytest.raises(ResolutionError, match="safety limit") as exc_info:
        project(
            "etl-prod",
            adapter=adapter,
        )
    assert exc_info.value.suggestion == (
        "Retry with a more specific selector or use the numeric code if known."
    )


def test_environment_resolver_uses_cli_resource_slug_in_not_found_details() -> None:
    adapter = FakeEnvironmentAdapter(
        environments=[FakeEnvironment(code=7, name="prod")]
    )

    with pytest.raises(NotFoundError) as exc_info:
        resolver_service.environment("missing", adapter=adapter)

    assert exc_info.value.details["resource"] == "env"


def test_environment_resolver_uses_search_pages_instead_of_full_scan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[int, int, str | None]] = []
    adapter = FakeEnvironmentAdapter(
        environments=[
            FakeEnvironment(code=7, name="prod"),
            FakeEnvironment(code=8, name="stage"),
        ]
    )

    original_list = adapter.list

    def tracked_list(
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> object:
        calls.append((page_no, page_size, search))
        return original_list(
            page_no=page_no,
            page_size=page_size,
            search=search,
        )

    monkeypatch.setattr(adapter, "list", tracked_list)

    resolved = resolver_service.environment("prod", adapter=adapter)

    assert resolved.code == 7
    assert calls == [(1, resolver_service.DEFAULT_RESOLUTION_PAGE_SIZE, "prod")]


def test_datasource_resolver_matches_by_exact_name() -> None:
    adapter = FakeDataSourceAdapter(
        datasources=[
            FakeDataSource(
                id=7,
                name="warehouse",
                note="main warehouse",
                type_value=FakeEnumValue("MYSQL"),
            )
        ]
    )
    resolved = datasource("warehouse", adapter=adapter)

    assert resolved.id == 7
    assert resolved.name == "warehouse"
    assert resolved.note == "main warehouse"
    assert resolved.type == "MYSQL"


def test_datasource_resolver_fetches_numeric_ids_directly() -> None:
    adapter = FakeDataSourceAdapter(
        datasources=[FakeDataSource(id=9, name="analytics")]
    )
    resolved = datasource("9", adapter=adapter)

    assert resolved.id == 9
    assert resolved.name == "analytics"


def test_datasource_resolver_reports_missing_names() -> None:
    adapter = FakeDataSourceAdapter(datasources=[])
    with pytest.raises(NotFoundError, match="was not found"):
        datasource("missing", adapter=adapter)


def test_datasource_resolver_reports_ambiguous_names() -> None:
    adapter = FakeDataSourceAdapter(
        datasources=[
            FakeDataSource(id=7, name="warehouse"),
            FakeDataSource(id=8, name="warehouse"),
        ]
    )
    with pytest.raises(ResolutionError, match="ambiguous"):
        datasource("warehouse", adapter=adapter)


def test_alert_group_resolver_matches_by_exact_name() -> None:
    adapter = FakeAlertGroupAdapter(
        alert_groups=[
            FakeAlertGroup(
                id=7,
                group_name_value="ops",
                description="ops alerts",
                create_user_id_value=1,
            )
        ]
    )

    resolved = alert_group("ops", adapter=adapter)

    assert resolved.id == 7
    assert resolved.group_name == "ops"
    assert resolved.description == "ops alerts"


def test_alert_group_resolver_fetches_numeric_ids_directly() -> None:
    adapter = FakeAlertGroupAdapter(
        alert_groups=[FakeAlertGroup(id=9, group_name_value="etl")]
    )

    resolved = alert_group("9", adapter=adapter)

    assert resolved.id == 9
    assert resolved.group_name == "etl"


def test_alert_group_resolver_reports_missing_names() -> None:
    adapter = FakeAlertGroupAdapter(alert_groups=[])

    with pytest.raises(NotFoundError, match="was not found") as exc_info:
        alert_group("missing", adapter=adapter)

    assert exc_info.value.details["resource"] == "alert-group"


def test_alert_group_resolver_reports_ambiguous_names() -> None:
    adapter = FakeAlertGroupAdapter(
        alert_groups=[
            FakeAlertGroup(id=7, group_name_value="ops"),
            FakeAlertGroup(id=8, group_name_value="ops"),
        ]
    )

    with pytest.raises(ResolutionError, match="ambiguous"):
        alert_group("ops", adapter=adapter)


def test_queue_resolver_matches_by_exact_name() -> None:
    adapter = FakeQueueAdapter(
        queues=[FakeQueue(id=7, queue_name_value="default", queue="root.default")]
    )

    resolved = queue("default", adapter=adapter)

    assert resolved.id == 7
    assert resolved.queue_name == "default"
    assert resolved.queue == "root.default"


def test_queue_resolver_fetches_numeric_ids_directly() -> None:
    adapter = FakeQueueAdapter(
        queues=[FakeQueue(id=9, queue_name_value="analytics", queue="root.analytics")]
    )

    resolved = queue("9", adapter=adapter)

    assert resolved.id == 9
    assert resolved.queue_name == "analytics"


def test_queue_resolver_reports_missing_names() -> None:
    adapter = FakeQueueAdapter(queues=[])

    with pytest.raises(NotFoundError, match="was not found"):
        queue("missing", adapter=adapter)


def test_queue_resolver_uses_search_pages_instead_of_full_scan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[int, int, str | None]] = []
    adapter = FakeQueueAdapter(
        queues=[
            FakeQueue(id=7, queue_name_value="default", queue="root.default"),
            FakeQueue(id=8, queue_name_value="analytics", queue="root.analytics"),
        ]
    )

    original_list = adapter.list

    def tracked_list(
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> object:
        calls.append((page_no, page_size, search))
        return original_list(
            page_no=page_no,
            page_size=page_size,
            search=search,
        )

    monkeypatch.setattr(adapter, "list", tracked_list)

    resolved = queue("default", adapter=adapter)

    assert resolved.id == 7
    assert calls == [(1, resolver_service.DEFAULT_RESOLUTION_PAGE_SIZE, "default")]


def test_alert_group_resolver_uses_search_pages_instead_of_full_scan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[int, int, str | None]] = []
    adapter = FakeAlertGroupAdapter(
        alert_groups=[
            FakeAlertGroup(id=7, group_name_value="ops"),
            FakeAlertGroup(id=8, group_name_value="etl"),
        ]
    )

    original_list = adapter.list

    def tracked_list(
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> object:
        calls.append((page_no, page_size, search))
        return original_list(
            page_no=page_no,
            page_size=page_size,
            search=search,
        )

    monkeypatch.setattr(adapter, "list", tracked_list)

    resolved = alert_group("ops", adapter=adapter)

    assert resolved.id == 7
    assert calls == [(1, resolver_service.DEFAULT_RESOLUTION_PAGE_SIZE, "ops")]


def test_worker_group_resolver_uses_cli_resource_slug_in_not_found_details() -> None:
    adapter = FakeWorkerGroupAdapter(
        worker_groups=[FakeWorkerGroup(id=7, name="default")]
    )

    with pytest.raises(NotFoundError) as exc_info:
        worker_group("missing", adapter=adapter)

    assert exc_info.value.details["resource"] == "worker-group"


def test_worker_group_resolver_uses_search_pages_with_deduplication(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[int, int, str | None]] = []
    monkeypatch.setattr(resolver_service, "DEFAULT_RESOLUTION_PAGE_SIZE", 1)
    adapter = FakeWorkerGroupAdapter(
        worker_groups=[
            FakeWorkerGroup(id=7, name="default-a"),
            FakeWorkerGroup(id=8, name="default-b"),
        ],
        config_worker_groups=[
            FakeWorkerGroup(id=None, name="default", system_default=True),
        ],
    )

    original_list = adapter.list

    def tracked_list(
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> object:
        calls.append((page_no, page_size, search))
        return original_list(
            page_no=page_no,
            page_size=page_size,
            search=search,
        )

    monkeypatch.setattr(adapter, "list", tracked_list)

    resolved = worker_group("default", adapter=adapter)

    assert resolved.name == "default"
    assert resolved.system_default is True
    assert calls == [
        (1, resolver_service.DEFAULT_RESOLUTION_PAGE_SIZE, "default"),
        (2, resolver_service.DEFAULT_RESOLUTION_PAGE_SIZE, "default"),
    ]


def test_tenant_resolver_matches_by_exact_code() -> None:
    adapter = FakeTenantAdapter(
        tenants=[
            FakeTenant(
                id=7,
                tenant_code_value="tenant-prod",
                description="production tenant",
                queue_id_value=11,
                queue_name_value="default",
                queue_value="root.default",
            )
        ]
    )

    resolved = tenant("tenant-prod", adapter=adapter)

    assert resolved.id == 7
    assert resolved.tenant_code == "tenant-prod"
    assert resolved.queue_id == 11
    assert resolved.queue_name == "default"


def test_tenant_resolver_fetches_numeric_ids_directly() -> None:
    adapter = FakeTenantAdapter(
        tenants=[FakeTenant(id=9, tenant_code_value="tenant-analytics")]
    )

    resolved = tenant("9", adapter=adapter)

    assert resolved.id == 9
    assert resolved.tenant_code == "tenant-analytics"


def test_tenant_resolver_reports_missing_codes() -> None:
    adapter = FakeTenantAdapter(tenants=[])

    with pytest.raises(NotFoundError, match="was not found"):
        tenant("missing", adapter=adapter)


def test_user_resolver_matches_by_exact_name() -> None:
    adapter = FakeUserAdapter(
        users=[
            FakeUser(
                id=7,
                user_name_value="alice",
                email="alice@example.com",
                tenant_id_value=11,
                tenant_code_value="tenant-prod",
                state=1,
            )
        ]
    )

    resolved = user("alice", adapter=adapter)

    assert resolved.id == 7
    assert resolved.user_name == "alice"
    assert resolved.tenant_id == 11
    assert resolved.tenant_code == "tenant-prod"


def test_user_resolver_fetches_numeric_ids_directly() -> None:
    adapter = FakeUserAdapter(
        users=[
            FakeUser(
                id=9,
                user_name_value="bob",
                email="bob@example.com",
                tenant_id_value=12,
                tenant_code_value="tenant-analytics",
                state=0,
            )
        ]
    )

    resolved = user("9", adapter=adapter)

    assert resolved.id == 9
    assert resolved.user_name == "bob"


def test_user_resolver_reports_missing_names() -> None:
    adapter = FakeUserAdapter(users=[])

    with pytest.raises(NotFoundError, match="was not found"):
        user("missing", adapter=adapter)


def test_tenant_resolver_uses_search_pages_instead_of_full_tenant_scan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[int, int, str | None]] = []
    adapter = FakeTenantAdapter(
        tenants=[
            FakeTenant(id=7, tenant_code_value="tenant-prod"),
            FakeTenant(id=8, tenant_code_value="tenant-dev"),
        ]
    )

    original_list = adapter.list

    def tracked_list(
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> object:
        calls.append((page_no, page_size, search))
        return original_list(
            page_no=page_no,
            page_size=page_size,
            search=search,
        )

    monkeypatch.setattr(adapter, "list", tracked_list)

    resolved = tenant("tenant-prod", adapter=adapter)

    assert resolved.id == 7
    assert calls == [(1, resolver_service.DEFAULT_RESOLUTION_PAGE_SIZE, "tenant-prod")]


def test_user_resolver_uses_search_pages_instead_of_full_user_scan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[int, int, str | None]] = []
    adapter = FakeUserAdapter(
        users=[
            FakeUser(
                id=7,
                user_name_value="alice",
                email="alice@example.com",
                tenant_id_value=11,
                tenant_code_value="tenant-prod",
                state=1,
            ),
            FakeUser(
                id=8,
                user_name_value="bob",
                email="bob@example.com",
                tenant_id_value=12,
                tenant_code_value="tenant-analytics",
                state=1,
            ),
        ]
    )

    original_list = adapter.list

    def tracked_list(
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> object:
        calls.append((page_no, page_size, search))
        return original_list(
            page_no=page_no,
            page_size=page_size,
            search=search,
        )

    monkeypatch.setattr(adapter, "list", tracked_list)

    resolved = user("alice", adapter=adapter)

    assert resolved.id == 7
    assert calls == [(1, resolver_service.DEFAULT_RESOLUTION_PAGE_SIZE, "alice")]
